"""Idempotency keys: one key, one logical action; conflicting reuse is rejected."""

import asyncio

import pytest
from pydantic import ValidationError

from durable_execution.errors import IdempotencyConflictError, SensitivePayloadError
from durable_execution.execution import ExecutionRequest, RunOutcome, request_fingerprint
from durable_execution.external import FakeExternalService
from durable_execution.models import ExecutionStatus
from durable_execution.security import ExecutionContext

from .conftest import RuntimeFactory, invoice_request, submit


def test_fingerprint_is_order_independent() -> None:
    a = request_fingerprint("create_invoice", {"amount_cents": 1, "customer_ref": "c"})
    b = request_fingerprint("create_invoice", {"customer_ref": "c", "amount_cents": 1})
    assert a == b
    assert a != request_fingerprint("create_invoice", {"customer_ref": "c", "amount_cents": 2})


async def test_duplicate_request_produces_one_external_operation(
    make_runtime: RuntimeFactory, ctx: ExecutionContext, service: FakeExternalService
) -> None:
    """Test 4: same key twice → one record, one external side effect, same result."""
    rt = make_runtime("worker-a")
    first = await rt.idempotency.submit(ctx, invoice_request())
    assert first.created
    await rt.worker.run(ctx, first.record.id)

    second = await rt.idempotency.submit(ctx, invoice_request())
    assert not second.created
    assert second.record.id == first.record.id
    assert second.record.status is ExecutionStatus.SUCCEEDED
    assert second.record.external_reference == "ext-000001"

    again = await rt.worker.run(ctx, second.record.id)
    assert again.outcome is RunOutcome.ALREADY_SETTLED
    assert service.execution_count == 1
    assert service.perform_calls == 1


@pytest.mark.parametrize(
    "conflicting",
    [
        invoice_request(amount=9999),
        invoice_request(customer="customer-b"),
        ExecutionRequest(
            action_type="send_notification",
            idempotency_key="invoice-key-0001",
            payload={"customer_ref": "customer-a", "amount_cents": 1250, "currency": "USD"},
        ),
    ],
    ids=["different-amount", "different-customer", "different-action"],
)
async def test_same_key_with_different_request_is_rejected(
    make_runtime: RuntimeFactory, ctx: ExecutionContext, conflicting: ExecutionRequest
) -> None:
    """Test 5: reusing a key for a materially different request must fail loudly."""
    rt = make_runtime("worker-a")
    original = await submit(rt, ctx, invoice_request())

    with pytest.raises(IdempotencyConflictError):
        await rt.idempotency.submit(ctx, conflicting)

    unchanged = await rt.repository.get(workspace_id=ctx.workspace_id, execution_id=original.id)
    assert unchanged.request_payload["amount_cents"] == 1250
    assert unchanged.status is ExecutionStatus.PENDING


async def test_concurrent_submissions_of_same_key_create_one_record(
    make_runtime: RuntimeFactory, ctx: ExecutionContext
) -> None:
    """The unique constraint, not a check-then-insert, resolves the race."""
    runtimes = [make_runtime(f"api-{i}") for i in range(8)]
    results = await asyncio.gather(
        *(rt.idempotency.submit(ctx, invoice_request()) for rt in runtimes)
    )

    assert len({r.record.id for r in results}) == 1
    assert sum(r.created for r in results) == 1


async def test_concurrent_conflicting_submission_is_rejected(
    make_runtime: RuntimeFactory, ctx: ExecutionContext
) -> None:
    a, b = make_runtime("api-a"), make_runtime("api-b")
    outcomes = await asyncio.gather(
        a.idempotency.submit(ctx, invoice_request(amount=100)),
        b.idempotency.submit(ctx, invoice_request(amount=200)),
        return_exceptions=True,
    )
    assert sum(isinstance(o, IdempotencyConflictError) for o in outcomes) == 1


async def test_payload_with_credentials_is_refused(
    make_runtime: RuntimeFactory, ctx: ExecutionContext
) -> None:
    rt = make_runtime("worker-a")
    request = ExecutionRequest(
        action_type="create_invoice",
        idempotency_key="invoice-key-0002",
        payload={"amount_cents": 1, "auth": {"api_key": "do-not-store"}},
    )
    with pytest.raises(SensitivePayloadError, match=r"auth\.api_key"):
        await rt.idempotency.submit(ctx, request)
    assert (
        await rt.repository.get_by_idempotency_key(
            workspace_id=ctx.workspace_id, idempotency_key="invoice-key-0002"
        )
        is None
    )


def test_idempotency_key_format_is_validated() -> None:
    with pytest.raises(ValidationError):
        invoice_request(key="short")
    with pytest.raises(ValidationError):
        invoice_request(key="has spaces in it")
