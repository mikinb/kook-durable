"""Database-backed worker leases.

Why not ``asyncio.Lock``? A process-local lock protects nothing once there is
more than one worker process, host or restart. The lease lives in the execution
row, so PostgreSQL arbitrates between *all* workers everywhere.

A lease has an **owner** (worker id), a unique **token** (fencing token) and an
**expiry** (database time). Properties:

* Acquisition is a conditional, row-locked update: a live lease cannot be taken.
* An expired lease can be taken over; takeover always issues a new token.
* Every write by a worker presents its token; a stale token is rejected
  (:class:`StaleLeaseError`). This is what stops a paused/slow worker from
  overwriting the state written by the worker that replaced it.
* Renewal and release verify the token.
"""

import asyncio
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, replace
from datetime import datetime

from sqlalchemy.exc import SQLAlchemyError

from durable_execution.database.repository import ExecutionRepository
from durable_execution.logging.setup import get_logger, log_event
from durable_execution.models.execution import ExecutionRecord

logger = get_logger("lease")


@dataclass(frozen=True, slots=True)
class Lease:
    execution_id: uuid.UUID
    workspace_id: uuid.UUID
    owner: str
    token: uuid.UUID
    expires_at: datetime

    @classmethod
    def from_record(cls, record: ExecutionRecord) -> "Lease":
        if record.lease_owner is None or record.lease_token is None or not record.lease_expires_at:
            raise ValueError("record does not carry a lease")
        return cls(
            execution_id=record.id,
            workspace_id=record.workspace_id,
            owner=record.lease_owner,
            token=record.lease_token,
            expires_at=record.lease_expires_at,
        )


class LeaseHeartbeat:
    """Background renewal while a long external call is in flight.

    If a renewal is refused, ``lost`` becomes True. The worker still cannot do
    damage — its later writes would be fenced off — but it can stop early and
    log clearly instead of discovering the loss at write time.
    """

    def __init__(self, manager: "LeaseManager", lease: Lease, interval: float) -> None:
        self.lease = lease
        self.lost = False
        self._manager = manager
        self._interval = interval

    async def run(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            try:
                renewed = await self._manager.renew_lease(self.lease)
            except (SQLAlchemyError, OSError) as exc:
                # Transient DB trouble: keep trying until expiry; fencing protects us.
                log_event(logger, "lease_renewal_error", error=type(exc).__name__)
                continue
            if renewed is None:
                self.lost = True
                return
            self.lease = renewed


class LeaseManager:
    def __init__(
        self,
        repository: ExecutionRepository,
        *,
        worker_id: str,
        lease_seconds: float = 30.0,
        renew_interval: float | None = None,
    ) -> None:
        self._repository = repository
        self.worker_id = worker_id
        self.lease_seconds = lease_seconds
        self.renew_interval = renew_interval or lease_seconds / 3

    async def acquire_lease(
        self, *, workspace_id: uuid.UUID, execution_id: uuid.UUID
    ) -> tuple[Lease, ExecutionRecord] | None:
        """Claim a runnable execution (``PENDING``/due ``RETRY_SCHEDULED``/stale ``CLAIMED``).

        Why: guarantees a single active executor per execution across all
        processes. Delegates to :meth:`ExecutionRepository.claim_execution`,
        which performs check-and-set under a row lock with database time.

        Returns ``None`` when another worker holds a live lease or the
        execution is not runnable — callers treat that as "skip", not an error.
        Transition: ``→ CLAIMED``. Safe to retry.
        """
        record = await self._repository.claim_execution(
            workspace_id=workspace_id,
            execution_id=execution_id,
            worker_id=self.worker_id,
            lease_seconds=self.lease_seconds,
        )
        if record is None:
            return None
        log_event(logger, "lease_acquired", record=record)
        return Lease.from_record(record), record

    async def acquire_recovery_lease(
        self, *, workspace_id: uuid.UUID, execution_id: uuid.UUID
    ) -> tuple[Lease, ExecutionRecord] | None:
        """Take over an uncertain execution (``UNKNOWN`` or stale ``RUNNING``/``RECONCILING``).

        Never steals a live lease. Transition: ``→ RECONCILING``. Returns
        ``None`` if not eligible (e.g. its owner is still alive).
        """
        record = await self._repository.acquire_recovery_lease(
            workspace_id=workspace_id,
            execution_id=execution_id,
            worker_id=self.worker_id,
            lease_seconds=self.lease_seconds,
        )
        if record is None:
            return None
        log_event(logger, "recovery_lease_acquired", record=record)
        return Lease.from_record(record), record

    async def renew_lease(self, lease: Lease) -> Lease | None:
        """Extend ``lease``; returns the renewed lease or ``None`` if ownership was lost.

        Verifies the token under a row lock; never extends someone else's
        lease. No state transition. Safe to retry.
        """
        expires_at = await self._repository.renew_lease(
            workspace_id=lease.workspace_id,
            execution_id=lease.execution_id,
            lease_token=lease.token,
            lease_seconds=self.lease_seconds,
        )
        if expires_at is None:
            log_event(logger, "lease_renewal_refused", execution_id=str(lease.execution_id))
            return None
        return replace(lease, expires_at=expires_at)

    async def release_lease(self, lease: Lease) -> bool:
        """Give up ``lease`` without an outcome (``CLAIMED → PENDING``,
        ``RECONCILING → UNKNOWN``). Returns False if the token is no longer current.
        """
        released = await self._repository.release_lease(
            workspace_id=lease.workspace_id,
            execution_id=lease.execution_id,
            lease_token=lease.token,
        )
        log_event(logger, "lease_released", execution_id=str(lease.execution_id), released=released)
        return released

    @asynccontextmanager
    async def keep_alive(self, lease: Lease) -> AsyncIterator[LeaseHeartbeat]:
        """Renew ``lease`` periodically for the duration of the ``async with`` block."""
        heartbeat = LeaseHeartbeat(self, lease, self.renew_interval)
        task = asyncio.create_task(heartbeat.run())
        try:
            yield heartbeat
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
