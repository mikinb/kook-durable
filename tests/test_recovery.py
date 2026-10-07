"""Crash recovery: dead workers, expired leases, stale RUNNING records."""

import asyncio

import pytest

from durable_execution.database import ExecutionRepository, SessionFactory, event_types
from durable_execution.execution import RunOutcome
from durable_execution.external import FakeExternalService
from durable_execution.models import EventType, ExecutionStatus
from durable_execution.security import ExecutionContext

from .conftest import (
    CrashAfterCallClient,
    CrashBeforeCallClient,
    RuntimeFactory,
    SimulatedWorkerCrash,
    expire_lease,
    invoice_request,
    order_request,
    status_of,
    submit,
)


async def test_worker_dies_lease_expires_second_worker_recovers(
    make_runtime: RuntimeFactory,
    ctx: ExecutionContext,
    service: FakeExternalService,
    client: object,
    repository: ExecutionRepository,
    session_factory: SessionFactory,
) -> None:
    """Test 7: A dies mid-call (after the side effect, before recording anything)."""
    worker_a = make_runtime("worker-a", client=CrashAfterCallClient(client))  # type: ignore[arg-type]
    record = await submit(worker_a, ctx, order_request())

    with pytest.raises(SimulatedWorkerCrash):
        await worker_a.worker.run(ctx, record.id)

    stuck = await repository.get(workspace_id=ctx.workspace_id, execution_id=record.id)
    assert stuck.status is ExecutionStatus.RUNNING
    assert stuck.lease_owner == "worker-a"
    assert service.execution_count == 1

    worker_b = make_runtime("worker-b")
    early = await worker_b.recovery.recover_stale_executions()
    assert early == [], "a live lease must not be considered stale"
    blocked = await worker_b.worker.run(ctx, record.id)
    assert blocked.outcome is RunOutcome.SKIPPED_LEASE_HELD

    await expire_lease(session_factory, record.id)
    results = await worker_b.recovery.recover_stale_executions()

    assert [r.outcome for r in results] == [RunOutcome.SUCCEEDED]
    assert await status_of(repository, record) is ExecutionStatus.SUCCEEDED
    assert service.execution_count == 1
    assert service.perform_calls == 1

    events = await repository.list_events(workspace_id=ctx.workspace_id, execution_id=record.id)
    recovered = next(e for e in events if e.event_type is EventType.RECOVERED)
    assert recovered.worker_id == "worker-b"
    assert recovered.details["previous_owner"] == "worker-a"
    assert recovered.details["previous_status"] == "RUNNING"
    # history preserved: A's attempt is still visible
    assert EventType.EXTERNAL_CALL_STARTED in event_types(events)


async def test_crash_before_external_call_is_retried_after_reconciliation(
    make_runtime: RuntimeFactory,
    ctx: ExecutionContext,
    service: FakeExternalService,
    client: object,
    session_factory: SessionFactory,
) -> None:
    """RUNNING was committed but the call never left: reconcile → NOT_EXECUTED → retry."""
    worker_a = make_runtime("worker-a", client=CrashBeforeCallClient(client))  # type: ignore[arg-type]
    record = await submit(worker_a, ctx, invoice_request())
    with pytest.raises(SimulatedWorkerCrash):
        await worker_a.worker.run(ctx, record.id)
    assert service.execution_count == 0
    await expire_lease(session_factory, record.id)

    worker_b = make_runtime("worker-b")
    recovered = await worker_b.recovery.recover_stale_executions()
    assert [r.outcome for r in recovered] == [RunOutcome.RETRY_SCHEDULED]

    final = await worker_b.worker.run_until_settled(ctx, record.id)
    assert final.outcome is RunOutcome.SUCCEEDED
    assert final.record is not None
    assert final.record.attempt == 2
    assert service.execution_count == 1


async def test_two_recoverers_never_reconcile_the_same_execution_concurrently(
    make_runtime: RuntimeFactory,
    ctx: ExecutionContext,
    service: FakeExternalService,
    client: object,
    session_factory: SessionFactory,
) -> None:
    worker_a = make_runtime("worker-a", client=CrashAfterCallClient(client))  # type: ignore[arg-type]
    record = await submit(worker_a, ctx, invoice_request())
    with pytest.raises(SimulatedWorkerCrash):
        await worker_a.worker.run(ctx, record.id)
    await expire_lease(session_factory, record.id)

    recoverers = [make_runtime(f"recoverer-{i}") for i in range(5)]
    results = await asyncio.gather(
        *(r.recovery.recover_stale_execution(ctx, execution_id=record.id) for r in recoverers)
    )

    outcomes = sorted(r.outcome for r in results)
    assert outcomes.count(RunOutcome.SUCCEEDED) == 1
    assert outcomes.count(RunOutcome.SKIPPED_LEASE_HELD) == 4
    assert service.status_calls == 1, "exactly one reconciliation happened"
    assert service.execution_count == 1


async def test_dead_reconciler_is_taken_over(
    make_runtime: RuntimeFactory,
    ctx: ExecutionContext,
    service: FakeExternalService,
    repository: ExecutionRepository,
    session_factory: SessionFactory,
) -> None:
    rt = make_runtime("worker-a")
    record = await submit(rt, ctx, invoice_request())
    from durable_execution.external import FailureMode

    service.inject(FailureMode.AFTER_SIDE_EFFECT)
    await rt.worker.run(ctx, record.id)  # → UNKNOWN

    # Reconciler R1 takes the recovery lease and then dies silently.
    r1 = make_runtime("reconciler-1")
    assert (
        await r1.lease_manager.acquire_recovery_lease(
            workspace_id=ctx.workspace_id, execution_id=record.id
        )
        is not None
    )
    assert await status_of(repository, record) is ExecutionStatus.RECONCILING

    r2 = make_runtime("reconciler-2")
    assert (await r2.worker.run(ctx, record.id)).outcome is RunOutcome.SKIPPED_LEASE_HELD
    await expire_lease(session_factory, record.id)
    assert (await r2.worker.run(ctx, record.id)).outcome is RunOutcome.SUCCEEDED
    assert service.execution_count == 1
