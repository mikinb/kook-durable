"""Deterministic walkthrough of the critical failure scenario.

    external action succeeded → response lost → worker died →
    new worker started → reconciliation found success → no duplicate action

Run (PostgreSQL must be up: ``docker compose up -d --wait``)::

    python examples/failure_recovery_demo.py            # narrative output
    python examples/failure_recovery_demo.py --verbose  # + structured logs

Every narrative line below is derived from the durable event history written
to PostgreSQL, not from in-memory state, so what you see is what an auditor
would see after a real incident.
"""

import argparse
import asyncio
import logging
import uuid

from pydantic import SecretStr

from durable_execution.database import (
    create_engine,
    create_session_factory,
    database_url_from_env,
    init_schema,
)
from durable_execution.execution import ExecutionRequest, WorkerRuntime, build_worker_runtime
from durable_execution.external import FailureMode, FakeExternalService, FakeServiceClient
from durable_execution.logging import configure_logging
from durable_execution.models import EventType, ExecutionEvent
from durable_execution.security import ExecutionContext

API_KEY = SecretStr("demo-placeholder-not-a-real-key")
_NAMESPACE = uuid.UUID("8f0c7c52-0000-4000-8000-00000000d3e0")

NARRATIVE = {
    EventType.CLAIMED: ["claimed execution"],
    EventType.LEASE_ACQUIRED: ["lease acquired"],
    EventType.EXTERNAL_CALL_STARTED: ["starting external action"],
    EventType.UNKNOWN: ["execution outcome = UNKNOWN"],
    EventType.RECONCILIATION_STARTED: ["outcome is uncertain", "reconciling external state"],
    EventType.RECONCILIATION_SUCCEEDED: ["external state = SUCCESS"],
    EventType.FINAL_SUCCESS: ["marking execution SUCCESS"],
    EventType.RECONCILIATION_INCONCLUSIVE: ["external state = UNDETERMINED"],
    EventType.MANUAL_REVIEW_REQUIRED: ["NOT retrying a non-idempotent action blindly"],
}


def narrate(label: str, events: list[ExecutionEvent], service: FakeExternalService) -> None:
    for event in events:
        if event.event_type is EventType.CONNECTION_LOST:
            if service.execution_count:
                print("[External] action performed successfully")
            print("[Network] response lost")
            continue
        for line in NARRATIVE.get(event.event_type, []):
            print(f"[{label}] {line}")


async def scenario_lost_response(
    service: FakeExternalService, runtime: WorkerRuntime, other: WorkerRuntime
) -> None:
    run_id = uuid.uuid4().hex[:8]
    ctx = ExecutionContext(
        workspace_id=uuid.uuid5(_NAMESPACE, "demo-workspace"),
        task_id=uuid.uuid5(_NAMESPACE, f"demo-task-{run_id}"),
        agent_id=uuid.uuid5(_NAMESPACE, "demo-agent"),
    )
    request = ExecutionRequest(
        action_type="create_invoice",
        idempotency_key=f"demo-invoice-{run_id}",
        payload={"customer_ref": "customer-demo", "amount_cents": 4200, "currency": "USD"},
    )
    submission = await runtime.idempotency.submit(ctx, request)
    execution_id = submission.record.id
    print(f"execution_id={execution_id} workspace=demo-workspace action={request.action_type}\n")

    service.inject(FailureMode.AFTER_SIDE_EFFECT)
    await runtime.worker.run(ctx, execution_id)
    seen = await runtime.repository.list_events(
        workspace_id=ctx.workspace_id, execution_id=execution_id
    )
    narrate("Worker A", seen, service)

    print("\n--- worker restart ---\n")

    existing = await other.repository.get(workspace_id=ctx.workspace_id, execution_id=execution_id)
    print(f"[Worker B] found existing execution (status={existing.status})")
    calls_before = service.perform_calls
    await other.worker.run(ctx, execution_id)
    events = await other.repository.list_events(
        workspace_id=ctx.workspace_id, execution_id=execution_id
    )
    narrate("Worker B", events[len(seen) :], service)
    if service.perform_calls == calls_before:
        print("[Worker B] no duplicate external action created")

    final = await other.repository.get(workspace_id=ctx.workspace_id, execution_id=execution_id)
    print()
    print(f"External side effects: {service.execution_count}")
    print(f"Final execution status: {final.status}")
    print(f"External reference: {final.external_reference}")
    print("\nDurable event history:")
    for event in events:
        print(
            f"  {event.id:>4} {event.event_type:<26} {event.from_status or '-':>12} -> "
            f"{event.to_status or '-':<12} worker={event.worker_id or '-'}"
        )
    assert service.execution_count == 1
    assert str(final.status) == "SUCCEEDED"


async def scenario_non_idempotent(service: FakeExternalService, runtime: WorkerRuntime) -> None:
    run_id = uuid.uuid4().hex[:8]
    ctx = ExecutionContext(
        workspace_id=uuid.uuid5(_NAMESPACE, "demo-workspace"),
        task_id=uuid.uuid5(_NAMESPACE, f"demo-task-order-{run_id}"),
        agent_id=uuid.uuid5(_NAMESPACE, "demo-agent"),
    )
    request = ExecutionRequest(
        action_type="send_physical_order",
        idempotency_key=f"demo-order-{run_id}",
        payload={"sku": "SKU-DEMO-1", "quantity": 1, "ship_to_ref": "address-ref-demo"},
    )
    record = (await runtime.idempotency.submit(ctx, request)).record
    service.inject(FailureMode.BEFORE_EXECUTION)  # indistinguishable from a lost response
    result = await runtime.worker.run_until_settled(ctx, record.id)
    events = await runtime.repository.list_events(
        workspace_id=ctx.workspace_id, execution_id=record.id
    )
    narrate("Worker C", events, service)
    print(f"\nExternal calls made: {service.perform_calls} (original attempt only; no blind retry)")
    print(f"Final execution status: {result.status}")
    print(f"Reason: {result.detail}")
    assert str(result.status) == "MANUAL_REVIEW_REQUIRED"


async def main(verbose: bool) -> None:
    configure_logging(logging.INFO if verbose else logging.WARNING)
    url = database_url_from_env()
    engine_a, engine_b = create_engine(url, pool_size=2), create_engine(url, pool_size=2)
    try:
        await init_schema(engine_a)
        print("=" * 72)
        print("Scenario 1: external action succeeded, response lost, worker restarted")
        print("=" * 72)
        service = FakeExternalService()
        client = FakeServiceClient(service, api_key=API_KEY)
        worker_a = build_worker_runtime(
            worker_id="worker-a", session_factory=create_session_factory(engine_a), client=client
        )
        worker_b = build_worker_runtime(
            worker_id="worker-b", session_factory=create_session_factory(engine_b), client=client
        )
        await scenario_lost_response(service, worker_a, worker_b)

        print()
        print("=" * 72)
        print("Scenario 2: non-idempotent action, outcome cannot be determined")
        print("=" * 72)
        order_service = FakeExternalService()
        worker_c = build_worker_runtime(
            worker_id="worker-c",
            session_factory=create_session_factory(engine_a),
            client=FakeServiceClient(order_service, api_key=API_KEY),
        )
        await scenario_non_idempotent(order_service, worker_c)
    finally:
        await engine_a.dispose()
        await engine_b.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--verbose", action="store_true", help="also print structured logs")
    asyncio.run(main(parser.parse_args().verbose))
