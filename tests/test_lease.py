"""Lease ownership: acquisition, renewal, release, expiry and fencing."""

import uuid

import pytest

from durable_execution.database import ExecutionRepository, SessionFactory
from durable_execution.errors import InvalidStateTransitionError, StaleLeaseError
from durable_execution.models import ExecutionStatus
from durable_execution.security import ExecutionContext

from .conftest import RuntimeFactory, expire_lease, invoice_request, status_of, submit


async def test_live_lease_cannot_be_claimed_by_another_worker(
    make_runtime: RuntimeFactory, ctx: ExecutionContext
) -> None:
    a, b = make_runtime("worker-a"), make_runtime("worker-b")
    record = await submit(a, ctx, invoice_request())

    acquired_a = await a.lease_manager.acquire_lease(
        workspace_id=ctx.workspace_id, execution_id=record.id
    )
    acquired_b = await b.lease_manager.acquire_lease(
        workspace_id=ctx.workspace_id, execution_id=record.id
    )

    assert acquired_a is not None
    assert acquired_b is None
    lease, claimed = acquired_a
    assert claimed.status is ExecutionStatus.CLAIMED
    assert lease.owner == "worker-a"
    assert claimed.lease_expires_at is not None
    assert claimed.lease_expires_at > claimed.updated_at


async def test_expired_claim_can_be_reclaimed_with_new_token(
    make_runtime: RuntimeFactory, ctx: ExecutionContext, session_factory: SessionFactory
) -> None:
    """A CLAIMED-but-never-started execution is safe to re-claim once its lease expires."""
    a, b = make_runtime("worker-a"), make_runtime("worker-b")
    record = await submit(a, ctx, invoice_request())
    acquired_a = await a.lease_manager.acquire_lease(
        workspace_id=ctx.workspace_id, execution_id=record.id
    )
    assert acquired_a is not None
    await expire_lease(session_factory, record.id)

    acquired_b = await b.lease_manager.acquire_lease(
        workspace_id=ctx.workspace_id, execution_id=record.id
    )

    assert acquired_b is not None
    assert acquired_b[0].token != acquired_a[0].token
    assert acquired_b[1].lease_owner == "worker-b"


async def test_lease_renewal_only_by_valid_owner(
    make_runtime: RuntimeFactory, ctx: ExecutionContext
) -> None:
    """Test 11: the owner can renew; a forged/old token cannot."""
    a = make_runtime("worker-a")
    record = await submit(a, ctx, invoice_request())
    acquired = await a.lease_manager.acquire_lease(
        workspace_id=ctx.workspace_id, execution_id=record.id
    )
    assert acquired is not None
    lease, _ = acquired

    renewed = await a.lease_manager.renew_lease(lease)
    assert renewed is not None
    assert renewed.expires_at > lease.expires_at
    assert renewed.token == lease.token

    impostor = type(lease)(
        execution_id=lease.execution_id,
        workspace_id=lease.workspace_id,
        owner="worker-b",
        token=uuid.uuid4(),
        expires_at=lease.expires_at,
    )
    assert await a.lease_manager.renew_lease(impostor) is None


async def test_lease_renewal_fails_after_takeover(
    make_runtime: RuntimeFactory, ctx: ExecutionContext, session_factory: SessionFactory
) -> None:
    a, b = make_runtime("worker-a"), make_runtime("worker-b")
    record = await submit(a, ctx, invoice_request())
    acquired_a = await a.lease_manager.acquire_lease(
        workspace_id=ctx.workspace_id, execution_id=record.id
    )
    assert acquired_a is not None
    await expire_lease(session_factory, record.id)
    assert (
        await b.lease_manager.acquire_lease(workspace_id=ctx.workspace_id, execution_id=record.id)
        is not None
    )

    assert await a.lease_manager.renew_lease(acquired_a[0]) is None


async def test_release_requires_ownership(
    make_runtime: RuntimeFactory, ctx: ExecutionContext, repository: ExecutionRepository
) -> None:
    a = make_runtime("worker-a")
    record = await submit(a, ctx, invoice_request())
    acquired = await a.lease_manager.acquire_lease(
        workspace_id=ctx.workspace_id, execution_id=record.id
    )
    assert acquired is not None
    lease, _ = acquired

    forged = await repository.release_lease(
        workspace_id=ctx.workspace_id, execution_id=record.id, lease_token=uuid.uuid4()
    )
    assert forged is False
    assert await status_of(repository, record) is ExecutionStatus.CLAIMED

    assert await a.lease_manager.release_lease(lease) is True
    released = await repository.get(workspace_id=ctx.workspace_id, execution_id=record.id)
    assert released.status is ExecutionStatus.PENDING
    assert released.lease_owner is None
    assert released.lease_token is None


async def test_running_lease_cannot_be_released_silently(
    make_runtime: RuntimeFactory, ctx: ExecutionContext, repository: ExecutionRepository
) -> None:
    a = make_runtime("worker-a")
    record = await submit(a, ctx, invoice_request())
    acquired = await a.lease_manager.acquire_lease(
        workspace_id=ctx.workspace_id, execution_id=record.id
    )
    assert acquired is not None
    lease, _ = acquired
    await repository.mark_running(
        workspace_id=ctx.workspace_id, execution_id=record.id, lease_token=lease.token
    )
    with pytest.raises(InvalidStateTransitionError):
        await a.lease_manager.release_lease(lease)


async def test_cannot_start_external_call_on_expired_lease(
    make_runtime: RuntimeFactory,
    ctx: ExecutionContext,
    repository: ExecutionRepository,
    session_factory: SessionFactory,
) -> None:
    a = make_runtime("worker-a")
    record = await submit(a, ctx, invoice_request())
    acquired = await a.lease_manager.acquire_lease(
        workspace_id=ctx.workspace_id, execution_id=record.id
    )
    assert acquired is not None
    await expire_lease(session_factory, record.id)

    with pytest.raises(StaleLeaseError, match="expired"):
        await repository.mark_running(
            workspace_id=ctx.workspace_id, execution_id=record.id, lease_token=acquired[0].token
        )


async def test_stale_owner_cannot_overwrite_new_owner(
    make_runtime: RuntimeFactory,
    ctx: ExecutionContext,
    repository: ExecutionRepository,
    session_factory: SessionFactory,
) -> None:
    """Test 12: A's lease expires, B takes over, A's late SUCCESS write is rejected."""
    a, b = make_runtime("worker-a"), make_runtime("worker-b")
    record = await submit(a, ctx, invoice_request())
    acquired_a = await a.lease_manager.acquire_lease(
        workspace_id=ctx.workspace_id, execution_id=record.id
    )
    assert acquired_a is not None
    lease_a, _ = acquired_a
    await repository.mark_running(
        workspace_id=ctx.workspace_id, execution_id=record.id, lease_token=lease_a.token
    )

    await expire_lease(session_factory, record.id)
    acquired_b = await b.lease_manager.acquire_recovery_lease(
        workspace_id=ctx.workspace_id, execution_id=record.id
    )
    assert acquired_b is not None
    lease_b, _ = acquired_b

    with pytest.raises(StaleLeaseError):
        await repository.mark_succeeded(
            workspace_id=ctx.workspace_id,
            execution_id=record.id,
            lease_token=lease_a.token,
            external_reference="forged-by-stale-worker",
            result_payload={},
        )
    with pytest.raises(StaleLeaseError):
        await repository.mark_failed(
            workspace_id=ctx.workspace_id,
            execution_id=record.id,
            lease_token=lease_a.token,
            error_code="X",
            error_message="stale",
            reason="stale",
        )

    current = await repository.get(workspace_id=ctx.workspace_id, execution_id=record.id)
    assert current.status is ExecutionStatus.RECONCILING
    assert current.lease_owner == "worker-b"
    assert current.lease_token == lease_b.token
    assert current.external_reference is None
