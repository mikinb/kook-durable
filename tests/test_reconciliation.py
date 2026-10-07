"""Uncertain outcomes: lost responses are reconciled, never blindly retried."""

import pytest

from durable_execution.database import event_types
from durable_execution.execution import RunOutcome
from durable_execution.external import FailureMode, FakeExternalService, FakeServiceClient
from durable_execution.models import EventType, ExecutionStatus
from durable_execution.security import ExecutionContext

from .conftest import RuntimeFactory, invoice_request, order_request, submit


async def test_external_success_with_lost_response_is_reconciled_exactly_once(
    make_runtime: RuntimeFactory, ctx: ExecutionContext, service: FakeExternalService
) -> None:
    """Test 8 (mandatory critical scenario).

    1-2. Worker A claims the execution and obtains the lease.
    3-5. A calls the external service; the action commits, then the connection drops.
    6.   A records UNKNOWN (not FAILED).
    7.   A terminates.
    8-10. Worker B starts, finds the existing execution, sees it is uncertain.
    11-13. B reconciles; the external system reports SUCCESS; B marks SUCCEEDED.
    14.  No second external side effect is created.
    """
    worker_a = make_runtime("worker-a")
    record = await submit(worker_a, ctx, invoice_request())
    service.inject(FailureMode.AFTER_SIDE_EFFECT)

    result_a = await worker_a.worker.run(ctx, record.id)

    assert result_a.outcome is RunOutcome.UNKNOWN
    assert result_a.status is ExecutionStatus.UNKNOWN
    assert service.execution_count == 1, "the side effect did happen"
    del worker_a  # worker A terminates; nothing it remembers survives

    worker_b = make_runtime("worker-b")
    existing = await worker_b.repository.get(workspace_id=ctx.workspace_id, execution_id=record.id)
    assert existing.status is ExecutionStatus.UNKNOWN
    assert existing.error_code == "CONNECTION_LOST"

    result_b = await worker_b.worker.run(ctx, record.id)

    assert result_b.outcome is RunOutcome.SUCCEEDED
    final = await worker_b.repository.get(workspace_id=ctx.workspace_id, execution_id=record.id)
    assert final.status is ExecutionStatus.SUCCEEDED
    assert final.external_reference == "ext-000001"
    assert final.last_reconciled_at is not None
    assert final.attempt == 1, "no second attempt was started"

    assert service.execution_count == 1
    assert service.perform_calls == 1
    assert service.status_calls == 1

    events = await worker_b.repository.list_events(
        workspace_id=ctx.workspace_id, execution_id=record.id
    )
    assert event_types(events) == [
        EventType.CREATED,
        EventType.CLAIMED,
        EventType.LEASE_ACQUIRED,
        EventType.STARTED,
        EventType.EXTERNAL_CALL_STARTED,
        EventType.CONNECTION_LOST,
        EventType.UNKNOWN,
        EventType.LEASE_ACQUIRED,
        EventType.RECONCILIATION_STARTED,
        EventType.RECONCILIATION_SUCCEEDED,
        EventType.FINAL_SUCCESS,
    ]
    assert {e.worker_id for e in events[1:7]} == {"worker-a"}
    assert {e.worker_id for e in events[7:]} == {"worker-b"}


@pytest.mark.parametrize("mode", [FailureMode.AFTER_SIDE_EFFECT, FailureMode.TIMEOUT])
async def test_non_idempotent_uncertain_but_executed_is_not_repeated(
    make_runtime: RuntimeFactory,
    ctx: ExecutionContext,
    service: FakeExternalService,
    mode: FailureMode,
) -> None:
    """Test 9a: physical order shipped, response lost → reconcile finds it → SUCCEEDED."""
    rt = make_runtime("worker-a")
    record = await submit(rt, ctx, order_request())
    service.inject(mode)

    final = await rt.worker.run_until_settled(ctx, record.id)

    assert final.outcome is RunOutcome.SUCCEEDED
    assert service.execution_count == 1
    assert service.perform_calls == 1


async def test_non_idempotent_undeterminable_outcome_requires_manual_review(
    make_runtime: RuntimeFactory, ctx: ExecutionContext, service: FakeExternalService
) -> None:
    """Test 9b: not found by a non-authoritative lookup ≠ not shipped. Stop and escalate."""
    rt = make_runtime("worker-a")
    record = await submit(rt, ctx, order_request())
    service.inject(FailureMode.BEFORE_EXECUTION)  # looks identical to a lost response

    final = await rt.worker.run_until_settled(ctx, record.id)

    assert final.outcome is RunOutcome.MANUAL_REVIEW_REQUIRED
    assert final.status is ExecutionStatus.MANUAL_REVIEW_REQUIRED
    assert final.record is not None
    assert final.record.error_code == "OUTCOME_UNDETERMINED"
    assert service.perform_calls == 1, "must NOT blindly re-send a non-idempotent action"

    again = await rt.worker.run(ctx, record.id)
    assert again.outcome is RunOutcome.ALREADY_SETTLED
    assert service.perform_calls == 1
    events = await rt.repository.list_events(workspace_id=ctx.workspace_id, execution_id=record.id)
    assert EventType.RECONCILIATION_INCONCLUSIVE in event_types(events)
    assert event_types(events)[-1] is EventType.MANUAL_REVIEW_REQUIRED


async def test_non_idempotent_confirmed_not_executed_is_retried_safely(
    make_runtime: RuntimeFactory, ctx: ExecutionContext, service: FakeExternalService
) -> None:
    """Test 9c: an authoritative 'not found' proves it never happened → retry is safe."""
    rt = make_runtime("worker-a")
    record = await submit(rt, ctx, order_request(action_type="send_notification"))
    service.inject(FailureMode.BEFORE_EXECUTION)

    first = await rt.worker.run(ctx, record.id)
    assert first.outcome is RunOutcome.UNKNOWN
    reconciled = await rt.worker.run(ctx, record.id)
    assert reconciled.outcome is RunOutcome.RETRY_SCHEDULED
    final = await rt.worker.run_until_settled(ctx, record.id)

    assert final.outcome is RunOutcome.SUCCEEDED
    assert service.execution_count == 1
    assert service.perform_calls == 2
    events = await rt.repository.list_events(workspace_id=ctx.workspace_id, execution_id=record.id)
    assert EventType.RECONCILIATION_NOT_EXECUTED in event_types(events)


async def test_idempotent_action_inconclusive_retry_is_deduplicated_externally(
    make_runtime: RuntimeFactory, ctx: ExecutionContext, service: FakeExternalService
) -> None:
    """Lookup down repeatedly; idempotent action re-sent with the SAME key → still one effect."""
    rt = make_runtime("worker-a", max_reconcile_attempts=2)
    record = await submit(rt, ctx, invoice_request())
    service.inject(FailureMode.AFTER_SIDE_EFFECT)
    service.inject_lookup_failures(2)

    assert (await rt.worker.run(ctx, record.id)).outcome is RunOutcome.UNKNOWN
    assert (await rt.worker.run(ctx, record.id)).outcome is RunOutcome.UNKNOWN  # lookup down
    assert (await rt.worker.run(ctx, record.id)).outcome is RunOutcome.RETRY_SCHEDULED
    final = await rt.worker.run_until_settled(ctx, record.id)

    assert final.outcome is RunOutcome.SUCCEEDED
    assert service.perform_calls == 2
    assert service.execution_count == 1, "external idempotency key deduplicated the repeat"


async def test_lookup_outage_on_non_idempotent_action_escalates_after_bound(
    make_runtime: RuntimeFactory, ctx: ExecutionContext, service: FakeExternalService
) -> None:
    rt = make_runtime("worker-a", max_reconcile_attempts=3)
    record = await submit(rt, ctx, order_request())
    service.inject(FailureMode.AFTER_SIDE_EFFECT)
    service.inject_lookup_failures(10)

    final = await rt.worker.run_until_settled(ctx, record.id)

    assert final.outcome is RunOutcome.MANUAL_REVIEW_REQUIRED
    assert final.record is not None
    assert final.record.reconcile_attempts == 3
    assert service.status_calls == 3
    assert service.perform_calls == 1


async def test_blind_retry_would_have_duplicated_the_side_effect(
    service: FakeExternalService, client: FakeServiceClient
) -> None:
    """Control experiment: why reconciliation matters for non-idempotent actions."""
    service.inject(FailureMode.AFTER_SIDE_EFFECT)
    payload = {"sku": "SKU-DEMO-1", "quantity": 1}
    with pytest.raises(Exception, match="CONNECTION_LOST"):
        await client.perform_action(
            action_type="send_physical_order", idempotency_key="order-1234", payload=payload
        )
    await client.perform_action(
        action_type="send_physical_order", idempotency_key="order-1234", payload=payload
    )
    assert service.execution_count == 2, "naive retry shipped the order twice"
