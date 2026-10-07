"""Normal execution paths and explicit, non-retryable failures."""

import logging

import pytest

from durable_execution.database import event_types
from durable_execution.errors import UnknownActionTypeError
from durable_execution.execution import ExecutionRequest, RunOutcome
from durable_execution.external import FailureMode, FakeExternalService
from durable_execution.models import EventType, ExecutionStatus, can_transition
from durable_execution.security import REDACTED, ExecutionContext

from .conftest import RuntimeFactory, invoice_request, submit


async def test_normal_success_pending_running_succeeded(
    make_runtime: RuntimeFactory, ctx: ExecutionContext, service: FakeExternalService
) -> None:
    """Test 1: PENDING → CLAIMED → RUNNING → SUCCEEDED with a full audit trail."""
    rt = make_runtime("worker-a")
    record = await submit(rt, ctx, invoice_request())
    assert record.status is ExecutionStatus.PENDING

    result = await rt.worker.run(ctx, record.id)

    assert result.outcome is RunOutcome.SUCCEEDED
    final = await rt.repository.get(workspace_id=ctx.workspace_id, execution_id=record.id)
    assert final.status is ExecutionStatus.SUCCEEDED
    assert final.attempt == 1
    assert final.external_reference == "ext-000001"
    assert final.result_payload == {
        "external_reference": "ext-000001",
        "status": "SUCCEEDED",
        "action": "create_invoice",
    }
    assert final.lease_owner is None
    assert final.lease_token is None
    assert final.started_at is not None
    assert final.completed_at is not None
    assert service.execution_count == 1

    events = await rt.repository.list_events(workspace_id=ctx.workspace_id, execution_id=record.id)
    assert event_types(events) == [
        EventType.CREATED,
        EventType.CLAIMED,
        EventType.LEASE_ACQUIRED,
        EventType.STARTED,
        EventType.EXTERNAL_CALL_STARTED,
        EventType.EXTERNAL_CALL_SUCCEEDED,
        EventType.FINAL_SUCCESS,
    ]
    assert [(e.from_status, e.to_status) for e in events if e.from_status != e.to_status] == [
        (None, ExecutionStatus.PENDING),
        (ExecutionStatus.PENDING, ExecutionStatus.CLAIMED),
        (ExecutionStatus.CLAIMED, ExecutionStatus.RUNNING),
        (ExecutionStatus.RUNNING, ExecutionStatus.SUCCEEDED),
    ]
    assert {e.worker_id for e in events[1:]} == {"worker-a"}


async def test_rerunning_settled_execution_returns_stored_result(
    make_runtime: RuntimeFactory, ctx: ExecutionContext, service: FakeExternalService
) -> None:
    rt = make_runtime("worker-a")
    record = await submit(rt, ctx, invoice_request())
    await rt.worker.run(ctx, record.id)

    again = await make_runtime("worker-b").worker.run(ctx, record.id)

    assert again.outcome is RunOutcome.ALREADY_SETTLED
    assert again.status is ExecutionStatus.SUCCEEDED
    assert service.perform_calls == 1


@pytest.mark.parametrize(
    ("mode", "error_code"),
    [
        (FailureMode.PERMANENT, "VALIDATION_ERROR"),
        (FailureMode.AUTHENTICATION, "UNAUTHENTICATED"),
    ],
)
async def test_permanent_and_auth_errors_fail_without_retry(
    make_runtime: RuntimeFactory,
    ctx: ExecutionContext,
    service: FakeExternalService,
    mode: FailureMode,
    error_code: str,
) -> None:
    rt = make_runtime("worker-a")
    record = await submit(rt, ctx, invoice_request())
    service.inject(mode)

    result = await rt.worker.run_until_settled(ctx, record.id)

    assert result.outcome is RunOutcome.FAILED
    assert result.status is ExecutionStatus.FAILED
    assert result.record is not None
    assert result.record.error_code == error_code
    assert service.perform_calls == 1, "non-retryable error must not be retried"
    assert service.execution_count == 0


async def test_unknown_action_type_is_refused(
    make_runtime: RuntimeFactory, ctx: ExecutionContext
) -> None:
    rt = make_runtime("worker-a")
    request = ExecutionRequest(
        action_type="drop_production_db", idempotency_key="k-unknown-01", payload={}
    )
    with pytest.raises(UnknownActionTypeError):
        await rt.idempotency.submit(ctx, request)


def test_state_machine_has_no_blind_reexecution_edges() -> None:
    """Once a call may have started, the only way back to execution is RECONCILING."""
    assert not can_transition(ExecutionStatus.RUNNING, ExecutionStatus.PENDING)
    assert not can_transition(ExecutionStatus.RUNNING, ExecutionStatus.CLAIMED)
    assert not can_transition(ExecutionStatus.UNKNOWN, ExecutionStatus.CLAIMED)
    assert not can_transition(ExecutionStatus.UNKNOWN, ExecutionStatus.RETRY_SCHEDULED)
    assert not can_transition(ExecutionStatus.SUCCEEDED, ExecutionStatus.RUNNING)
    assert can_transition(ExecutionStatus.UNKNOWN, ExecutionStatus.RECONCILING)


async def test_structured_logs_carry_execution_context_and_redact_secrets(
    make_runtime: RuntimeFactory, ctx: ExecutionContext, caplog: pytest.LogCaptureFixture
) -> None:
    rt = make_runtime("worker-a")
    record = await submit(rt, ctx, invoice_request())
    with caplog.at_level(logging.INFO, logger="durable_execution"):
        await rt.worker.run(ctx, record.id)

    started = [r for r in caplog.records if r.getMessage() == "external_call_started"]
    assert started, "worker must log the external call start"
    fields = started[0].structured_fields  # type: ignore[attr-defined]
    for key in ("execution_id", "workspace_id", "task_id", "agent_id", "attempt", "status"):
        assert key in fields
    assert fields["workspace_id"] == str(ctx.workspace_id)
    assert fields["lease_owner"] == "worker-a"
    finished = [r for r in caplog.records if r.getMessage() == "external_call_finished"]
    assert "duration_ms" in finished[0].structured_fields  # type: ignore[attr-defined]

    from durable_execution.logging import get_logger, log_event

    with caplog.at_level(logging.INFO, logger="durable_execution"):
        log_event(get_logger("test"), "probe", api_key="should-not-appear", nested={"token": "x"})
    probe = next(r for r in caplog.records if r.getMessage() == "probe")
    assert probe.structured_fields["api_key"] == REDACTED  # type: ignore[attr-defined]
    assert probe.structured_fields["nested"]["token"] == REDACTED  # type: ignore[attr-defined]
    assert "should-not-appear" not in caplog.text
