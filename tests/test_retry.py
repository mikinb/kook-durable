"""Retry policy (pure) and retry behaviour end to end."""

import random

import pytest

from durable_execution.database import event_types
from durable_execution.execution import (
    ExternalConfirmation,
    RetryDecisionEngine,
    RetryPolicy,
    RunOutcome,
)
from durable_execution.external import FailureMode, FakeExternalService
from durable_execution.models import ErrorCategory, EventType, ExecutionStatus
from durable_execution.security import ExecutionContext

from .conftest import RuntimeFactory, invoice_request, submit

# ------------------------------------------------------------------- policy


def test_exponential_backoff_1_2_4_and_cap() -> None:
    policy = RetryPolicy(initial_delay=1.0, backoff_multiplier=2.0, max_delay=5.0, jitter=0.0)
    assert [policy.compute_delay(n) for n in (1, 2, 3, 4, 5)] == [1.0, 2.0, 4.0, 5.0, 5.0]


def test_jitter_is_bounded_and_deterministic_with_seed() -> None:
    policy = RetryPolicy(initial_delay=4.0, jitter=0.5, max_delay=60)
    a = [policy.compute_delay(2, random.Random(7)) for _ in range(3)]
    b = [policy.compute_delay(2, random.Random(7)) for _ in range(3)]
    assert a == b
    rng = random.Random(1)
    for _ in range(200):
        assert 4.0 <= policy.compute_delay(2, rng) <= 8.0


def test_policy_rejects_invalid_config() -> None:
    with pytest.raises(ValueError, match="max_attempts"):
        RetryPolicy(max_attempts=0)
    with pytest.raises(ValueError, match="jitter"):
        RetryPolicy(jitter=1.5)


# ------------------------------------------------------------ decision engine


def _decide(**overrides: object) -> object:
    engine = RetryDecisionEngine(RetryPolicy(initial_delay=1.0, jitter=0.0))
    params: dict[str, object] = {
        "category": ErrorCategory.TRANSIENT,
        "status": ExecutionStatus.RUNNING,
        "attempt": 1,
        "max_attempts": 3,
        "action_idempotent": True,
        "external_confirmation": ExternalConfirmation.CONFIRMED_NOT_EXECUTED,
        "holds_lease": True,
    }
    params.update(overrides)
    return engine.should_retry(**params)  # type: ignore[arg-type]


def test_transient_is_retried_with_backoff() -> None:
    decision = _decide(attempt=2)
    assert decision.retry
    assert decision.delay_seconds == 2.0


def test_transient_stops_at_max_attempts() -> None:
    decision = _decide(attempt=3)
    assert not decision.retry  # type: ignore[attr-defined]
    assert "max attempts" in decision.reason  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    "category",
    [
        ErrorCategory.VALIDATION,
        ErrorCategory.AUTHENTICATION,
        ErrorCategory.PERMANENT,
        ErrorCategory.IDEMPOTENCY_CONFLICT,
        ErrorCategory.LEASE_LOST,
    ],
)
def test_non_retryable_categories(category: ErrorCategory) -> None:
    decision = _decide(category=category)
    assert not decision.retry  # type: ignore[attr-defined]


def test_no_retry_without_lease() -> None:
    decision = _decide(holds_lease=False)
    assert not decision.retry
    assert "lease" in decision.reason


def test_no_retry_when_terminal() -> None:
    assert not _decide(status=ExecutionStatus.SUCCEEDED).retry  # type: ignore[attr-defined]


def test_unknown_outcome_requires_reconciliation_first() -> None:
    decision = _decide(
        category=ErrorCategory.OUTCOME_UNKNOWN,
        external_confirmation=ExternalConfirmation.NOT_CHECKED,
    )
    assert not decision.retry  # type: ignore[attr-defined]
    assert decision.requires_reconciliation  # type: ignore[attr-defined]


def test_unknown_confirmed_executed_is_not_retried() -> None:
    decision = _decide(
        category=ErrorCategory.OUTCOME_UNKNOWN,
        external_confirmation=ExternalConfirmation.CONFIRMED_EXECUTED,
    )
    assert not decision.retry  # type: ignore[attr-defined]


def test_unknown_inconclusive_non_idempotent_goes_to_manual_review() -> None:
    decision = _decide(
        category=ErrorCategory.OUTCOME_UNKNOWN,
        external_confirmation=ExternalConfirmation.INCONCLUSIVE,
        action_idempotent=False,
    )
    assert not decision.retry  # type: ignore[attr-defined]
    assert decision.requires_manual_review  # type: ignore[attr-defined]


def test_unknown_inconclusive_idempotent_may_retry_with_same_key() -> None:
    decision = _decide(
        category=ErrorCategory.OUTCOME_UNKNOWN,
        external_confirmation=ExternalConfirmation.INCONCLUSIVE,
        action_idempotent=True,
    )
    assert decision.retry  # type: ignore[attr-defined]


def test_unknown_confirmed_not_executed_retries_even_if_not_idempotent() -> None:
    decision = _decide(
        category=ErrorCategory.OUTCOME_UNKNOWN,
        external_confirmation=ExternalConfirmation.CONFIRMED_NOT_EXECUTED,
        action_idempotent=False,
    )
    assert decision.retry  # type: ignore[attr-defined]


# ---------------------------------------------------------------- end to end


async def test_transient_failure_then_success(
    make_runtime: RuntimeFactory, ctx: ExecutionContext, service: FakeExternalService
) -> None:
    """Test 2: RUNNING → RETRY_SCHEDULED → ... → SUCCEEDED."""
    rt = make_runtime("worker-a")
    record = await submit(rt, ctx, invoice_request())
    service.inject(FailureMode.TRANSIENT)

    first = await rt.worker.run(ctx, record.id)
    assert first.outcome is RunOutcome.RETRY_SCHEDULED
    assert first.record is not None
    assert first.record.error_code == "SERVICE_UNAVAILABLE"

    final = await rt.worker.run_until_settled(ctx, record.id)
    assert final.outcome is RunOutcome.SUCCEEDED
    assert final.record is not None
    assert final.record.attempt == 2
    assert service.perform_calls == 2
    assert service.execution_count == 1

    events = await rt.repository.list_events(workspace_id=ctx.workspace_id, execution_id=record.id)
    types = event_types(events)
    assert types.count(EventType.EXTERNAL_CALL_STARTED) == 2
    assert types.count(EventType.RETRY_SCHEDULED) == 1
    assert types[-1] is EventType.FINAL_SUCCESS


async def test_max_retries_exhausted_marks_failed(
    make_runtime: RuntimeFactory, ctx: ExecutionContext, service: FakeExternalService
) -> None:
    """Test 3: RUNNING → RETRY → RETRY → FAILED after max_attempts."""
    rt = make_runtime("worker-a")
    record = await submit(rt, ctx, invoice_request())
    service.inject(FailureMode.TRANSIENT, FailureMode.TRANSIENT, FailureMode.TRANSIENT)

    final = await rt.worker.run_until_settled(ctx, record.id)

    assert final.outcome is RunOutcome.FAILED
    assert final.record is not None
    assert final.record.attempt == 3 == final.record.max_attempts
    assert service.perform_calls == 3
    assert service.execution_count == 0
    events = await rt.repository.list_events(workspace_id=ctx.workspace_id, execution_id=record.id)
    types = event_types(events)
    assert types.count(EventType.RETRY_SCHEDULED) == 2
    assert types[-1] is EventType.FINAL_FAILURE
    assert "max attempts exhausted" in events[-1].details["reason"]


async def test_retry_is_not_run_before_it_is_due(
    make_runtime: RuntimeFactory, ctx: ExecutionContext, service: FakeExternalService
) -> None:
    rt = make_runtime(
        "worker-a", retry_policy=RetryPolicy(max_attempts=3, initial_delay=60.0, jitter=0.0)
    )
    record = await submit(rt, ctx, invoice_request())
    service.inject(FailureMode.TRANSIENT)
    await rt.worker.run(ctx, record.id)

    second = await rt.worker.run(ctx, record.id)

    assert second.outcome is RunOutcome.NOT_DUE
    assert service.perform_calls == 1
