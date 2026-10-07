"""Competing workers on the same execution (separate engines = separate 'processes')."""

import asyncio

from durable_execution.database import ExecutionRepository, SessionFactory
from durable_execution.execution import RunOutcome
from durable_execution.external import FakeExternalService
from durable_execution.models import ExecutionStatus
from durable_execution.security import ExecutionContext

from .conftest import RuntimeFactory, expire_lease, invoice_request, order_request, submit


async def test_two_workers_race_only_one_executes(
    make_runtime: RuntimeFactory, ctx: ExecutionContext, service: FakeExternalService
) -> None:
    """Test 6: Worker A and B start simultaneously; exactly one acquires the lease."""
    service.latency_seconds = 0.2  # keep the winner inside the call while the loser tries
    worker_a, worker_b = make_runtime("worker-a"), make_runtime("worker-b")
    record = await submit(worker_a, ctx, order_request())

    result_a, result_b = await asyncio.gather(
        worker_a.worker.run(ctx, record.id), worker_b.worker.run(ctx, record.id)
    )

    outcomes = {result_a.outcome, result_b.outcome}
    assert outcomes == {RunOutcome.SUCCEEDED, RunOutcome.SKIPPED_LEASE_HELD}
    assert service.perform_calls == 1
    assert service.execution_count == 1


async def test_many_workers_race_no_double_execution(
    make_runtime: RuntimeFactory, ctx: ExecutionContext, service: FakeExternalService
) -> None:
    service.latency_seconds = 0.1
    workers = [make_runtime(f"worker-{i}") for i in range(10)]
    record = await submit(workers[0], ctx, order_request())

    results = await asyncio.gather(*(w.worker.run(ctx, record.id) for w in workers))

    assert sum(r.outcome is RunOutcome.SUCCEEDED for r in results) == 1
    assert all(r.outcome in (RunOutcome.SUCCEEDED, RunOutcome.SKIPPED_LEASE_HELD) for r in results)
    assert service.perform_calls == 1


async def test_many_executions_many_workers_each_executed_once(
    make_runtime: RuntimeFactory, ctx: ExecutionContext, service: FakeExternalService
) -> None:
    workers = [make_runtime(f"worker-{i}") for i in range(4)]
    records = [
        await submit(workers[0], ctx, invoice_request(key=f"invoice-key-{n:04d}"))
        for n in range(12)
    ]

    await asyncio.gather(*(w.worker.run(ctx, r.id) for r in records for w in workers))

    assert service.execution_count == 12
    assert service.perform_calls == 12


async def test_slow_worker_is_fenced_after_lease_takeover(
    make_runtime: RuntimeFactory,
    ctx: ExecutionContext,
    service: FakeExternalService,
    repository: ExecutionRepository,
    session_factory: SessionFactory,
) -> None:
    """§21: A's lease expires mid-call, B takes over and finishes, A's late write is rejected."""
    gate = asyncio.Event()
    service.response_gate = gate
    worker_a, worker_b = make_runtime("worker-a"), make_runtime("worker-b")
    record = await submit(worker_a, ctx, invoice_request())

    task_a = asyncio.create_task(worker_a.worker.run(ctx, record.id))
    await asyncio.wait_for(service.side_effect_committed.wait(), timeout=5)

    # A is "paused" (GC pause / network stall) past its lease expiry.
    await expire_lease(session_factory, record.id)
    service.response_gate = None  # B's reconciliation lookups are not held
    result_b = await worker_b.worker.run(ctx, record.id)
    assert result_b.outcome is RunOutcome.SUCCEEDED

    gate.set()  # A wakes up and tries to write SUCCESS with its old token
    result_a = await asyncio.wait_for(task_a, timeout=5)

    assert result_a.outcome is RunOutcome.LEASE_LOST
    final = await repository.get(workspace_id=ctx.workspace_id, execution_id=record.id)
    assert final.status is ExecutionStatus.SUCCEEDED
    events = await repository.list_events(workspace_id=ctx.workspace_id, execution_id=record.id)
    final_success = [e for e in events if e.event_type == "FINAL_SUCCESS"]
    assert len(final_success) == 1
    assert final_success[0].worker_id == "worker-b"
    assert service.execution_count == 1
