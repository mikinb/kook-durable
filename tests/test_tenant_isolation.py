"""Tenant/workspace isolation is enforced by the execution layer itself."""

import dataclasses
import uuid

import pytest

from durable_execution.errors import ExecutionContextMismatchError, ExecutionNotFoundError
from durable_execution.execution import RunOutcome
from durable_execution.external import FakeExternalService
from durable_execution.security import ExecutionContext

from .conftest import RuntimeFactory, invoice_request, submit


def _workspace() -> ExecutionContext:
    return ExecutionContext(uuid.uuid4(), uuid.uuid4(), uuid.uuid4())


async def test_workspace_a_cannot_read_workspace_b_execution(make_runtime: RuntimeFactory) -> None:
    """Test 10."""
    rt = make_runtime("worker-a")
    ws_a, ws_b = _workspace(), _workspace()
    record_b = await submit(rt, ws_b, invoice_request())

    with pytest.raises(ExecutionNotFoundError):
        await rt.repository.get(workspace_id=ws_a.workspace_id, execution_id=record_b.id)
    assert (
        await rt.repository.get_by_idempotency_key(
            workspace_id=ws_a.workspace_id, idempotency_key=record_b.idempotency_key
        )
        is None
    )
    assert (
        await rt.repository.list_events(workspace_id=ws_a.workspace_id, execution_id=record_b.id)
        == []
    )


async def test_workspace_a_cannot_execute_or_mutate_workspace_b_execution(
    make_runtime: RuntimeFactory, service: FakeExternalService
) -> None:
    rt = make_runtime("worker-a")
    ws_a, ws_b = _workspace(), _workspace()
    record_b = await submit(rt, ws_b, invoice_request())

    with pytest.raises(ExecutionNotFoundError):
        await rt.worker.run(ws_a, record_b.id)
    with pytest.raises(ExecutionNotFoundError):
        await rt.lease_manager.acquire_lease(
            workspace_id=ws_a.workspace_id, execution_id=record_b.id
        )
    with pytest.raises(ExecutionNotFoundError):
        await rt.repository.mark_succeeded(
            workspace_id=ws_a.workspace_id,
            execution_id=record_b.id,
            lease_token=uuid.uuid4(),
            external_reference="x",
            result_payload={},
        )
    assert service.perform_calls == 0


async def test_same_idempotency_key_in_two_workspaces_is_independent(
    make_runtime: RuntimeFactory, service: FakeExternalService
) -> None:
    rt = make_runtime("worker-a")
    ws_a, ws_b = _workspace(), _workspace()
    # Different payloads under the same key string are fine across tenants.
    a = await rt.idempotency.submit(ws_a, invoice_request(amount=100))
    b = await rt.idempotency.submit(ws_b, invoice_request(amount=200))

    assert a.created
    assert b.created
    assert a.record.id != b.record.id
    assert a.record.workspace_id == ws_a.workspace_id
    assert b.record.workspace_id == ws_b.workspace_id


async def test_wrong_task_in_same_workspace_is_rejected(make_runtime: RuntimeFactory) -> None:
    rt = make_runtime("worker-a")
    ctx = _workspace()
    record = await submit(rt, ctx, invoice_request())
    other_task = dataclasses.replace(ctx, task_id=uuid.uuid4())

    with pytest.raises(ExecutionContextMismatchError):
        await rt.worker.run(other_task, record.id)
    assert (await rt.worker.run(ctx, record.id)).outcome is RunOutcome.SUCCEEDED


async def test_execution_records_carry_full_context(make_runtime: RuntimeFactory) -> None:
    rt = make_runtime("worker-a")
    ctx = _workspace()
    record = await submit(rt, ctx, invoice_request())
    assert (record.workspace_id, record.task_id, record.agent_id) == (
        ctx.workspace_id,
        ctx.task_id,
        ctx.agent_id,
    )
    events = await rt.repository.list_events(workspace_id=ctx.workspace_id, execution_id=record.id)
    assert all(e.workspace_id == ctx.workspace_id for e in events)
