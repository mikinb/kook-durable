"""Execution repository: every state change, atomically, with its audit events.

Transaction pattern
-------------------

Every mutating method follows the same shape inside **one short transaction**:

1. ``SELECT ... FOR UPDATE`` the row, filtered by ``workspace_id`` *and* ``id``.
   The row lock serialises concurrent workers on that execution: a second
   worker blocks until the first commits, then re-reads the committed state.
2. Read the database clock (``clock_timestamp()``) *after* acquiring the lock.
   All lease comparisons use database time so worker clock skew is irrelevant.
3. Validate preconditions in Python against the locked row: allowed state
   transition (:func:`validate_transition`), fencing token match, lease expiry.
4. Apply the change and append ``ExecutionEvent`` rows.
5. Commit. Either the state change *and* its history are durable, or neither.

This replaces the unsafe "``if status == PENDING: status = RUNNING``" pattern:
the check and the write happen under the same row lock, so two workers can
never both observe ``PENDING`` and both proceed.

External calls are **never** made inside these transactions. Holding a row
lock across a network call would block every other worker (including recovery)
for an unbounded time, and a DB rollback cannot undo an external side effect
anyway. The durable intent (``RUNNING`` + ``EXTERNAL_CALL_STARTED``) is committed
*before* the call; the outcome is committed *after*.

Tenant isolation
----------------

Every method that reads or writes a single execution requires ``workspace_id``
and includes it in the ``WHERE`` clause. A record from another workspace is
reported as not found. The only cross-workspace query is
:meth:`find_recoverable`, a system-level scan that returns identifiers only.
"""

import uuid
from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import DateTime, and_, func, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from durable_execution.database.session import SessionFactory
from durable_execution.errors import (
    ExecutionNotFoundError,
    InvalidStateTransitionError,
    StaleLeaseError,
)
from durable_execution.models.enums import (
    LEASED_STATUSES,
    EventType,
    ExecutionStatus,
    validate_transition,
)
from durable_execution.models.execution import ExecutionEvent, ExecutionRecord
from durable_execution.security.context import ExecutionContext, redact

S = ExecutionStatus
_MAX_ERROR_MESSAGE = 500


class ExecutionRepository:
    def __init__(self, session_factory: SessionFactory) -> None:
        self._session_factory = session_factory

    # ------------------------------------------------------------------ create

    async def create_execution(
        self,
        ctx: ExecutionContext,
        *,
        action_type: str,
        idempotency_key: str,
        request_fingerprint: str,
        request_payload: dict[str, Any],
        max_attempts: int,
    ) -> tuple[ExecutionRecord, bool]:
        """Insert a ``PENDING`` execution, or return the one that already owns the key.

        Why: this is the durable registration of intent. Once it commits, a
        crash anywhere later cannot cause the action to be forgotten or created
        twice under the same key.

        Invariants: ``(workspace_id, idempotency_key)`` is unique (DB constraint).
        Uses ``INSERT ... ON CONFLICT DO NOTHING`` so concurrent submissions of
        the same key race safely: exactly one insert wins, the others read the
        winner's row. The caller (:class:`IdempotencyService`) must then verify
        the fingerprint matches.

        Returns ``(record, created)``. ``created`` is False when the key existed.

        Failure: if the transaction fails nothing is written; safe to retry.
        Transitions: ``∅ → PENDING`` (event ``CREATED``). No lease involved.
        """
        async with self._session_factory.begin() as session:
            now = await _db_now(session)
            stmt = (
                pg_insert(ExecutionRecord)
                .values(
                    id=uuid.uuid4(),
                    workspace_id=ctx.workspace_id,
                    task_id=ctx.task_id,
                    agent_id=ctx.agent_id,
                    action_type=action_type,
                    idempotency_key=idempotency_key,
                    request_fingerprint=request_fingerprint,
                    request_payload=request_payload,
                    status=S.PENDING,
                    attempt=0,
                    max_attempts=max_attempts,
                    reconcile_attempts=0,
                    created_at=now,
                    updated_at=now,
                )
                .on_conflict_do_nothing(constraint="uq_executions_workspace_idempotency_key")
                .returning(ExecutionRecord.id)
            )
            inserted_id = (await session.execute(stmt)).scalar_one_or_none()
            if inserted_id is not None:
                session.add(
                    ExecutionEvent(
                        execution_id=inserted_id,
                        workspace_id=ctx.workspace_id,
                        event_type=EventType.CREATED,
                        from_status=None,
                        to_status=S.PENDING,
                        attempt=0,
                        worker_id=None,
                        details={"action_type": action_type},
                        created_at=now,
                    )
                )
            record = (
                await session.execute(
                    select(ExecutionRecord).where(
                        ExecutionRecord.workspace_id == ctx.workspace_id,
                        ExecutionRecord.idempotency_key == idempotency_key,
                    )
                )
            ).scalar_one()
            return record, inserted_id is not None

    # ------------------------------------------------------------------- reads

    async def get(self, *, workspace_id: uuid.UUID, execution_id: uuid.UUID) -> ExecutionRecord:
        """Return a snapshot of the execution, scoped to ``workspace_id``.

        Raises :class:`ExecutionNotFoundError` if it does not exist in this
        workspace, including when it exists in a different workspace.
        Read-only; safe to retry; no lease required.
        """
        async with self._session_factory() as session:
            record = (
                await session.execute(
                    select(ExecutionRecord).where(
                        ExecutionRecord.id == execution_id,
                        ExecutionRecord.workspace_id == workspace_id,
                    )
                )
            ).scalar_one_or_none()
        if record is None:
            raise ExecutionNotFoundError(f"execution {execution_id} not found")
        return record

    async def get_by_idempotency_key(
        self, *, workspace_id: uuid.UUID, idempotency_key: str
    ) -> ExecutionRecord | None:
        """Look up the execution registered under ``idempotency_key`` in this workspace.

        Why: the idempotency key is the caller-facing identity of a logical
        action. Keys are scoped per workspace, so two tenants may use the same
        key string without colliding or seeing each other's records.
        Read-only; safe to retry; no lease required.
        """
        async with self._session_factory() as session:
            return (
                await session.execute(
                    select(ExecutionRecord).where(
                        ExecutionRecord.workspace_id == workspace_id,
                        ExecutionRecord.idempotency_key == idempotency_key,
                    )
                )
            ).scalar_one_or_none()

    async def list_events(
        self, *, workspace_id: uuid.UUID, execution_id: uuid.UUID
    ) -> list[ExecutionEvent]:
        """Return the append-only event history in commit order (workspace-scoped)."""
        async with self._session_factory() as session:
            rows = await session.execute(
                select(ExecutionEvent)
                .where(
                    ExecutionEvent.execution_id == execution_id,
                    ExecutionEvent.workspace_id == workspace_id,
                )
                .order_by(ExecutionEvent.id)
            )
            return list(rows.scalars())

    async def find_recoverable(self, *, limit: int = 100) -> list[tuple[uuid.UUID, uuid.UUID]]:
        """Return ``(workspace_id, execution_id)`` pairs that need recovery.

        Candidates are ``UNKNOWN`` executions, and ``RUNNING``/``RECONCILING``
        executions whose lease has expired according to the database clock.

        This is the one deliberately cross-tenant query: it is used by the
        system-level recovery sweep, returns identifiers only, and every
        subsequent read/write goes back through workspace-scoped methods.

        It is only a *hint*: the actual takeover in :meth:`acquire_recovery_lease`
        re-checks expiry under a row lock, so a stale candidate list is harmless.
        """
        async with self._session_factory() as session:
            now = func.clock_timestamp()
            rows = await session.execute(
                select(ExecutionRecord.workspace_id, ExecutionRecord.id)
                .where(
                    or_(
                        ExecutionRecord.status == S.UNKNOWN,
                        and_(
                            ExecutionRecord.status.in_([S.RUNNING, S.RECONCILING]),
                            ExecutionRecord.lease_expires_at < now,
                        ),
                    )
                )
                .order_by(ExecutionRecord.updated_at)
                .limit(limit)
            )
            return [(row.workspace_id, row.id) for row in rows]

    # ------------------------------------------------------------------ leases

    async def claim_execution(
        self,
        *,
        workspace_id: uuid.UUID,
        execution_id: uuid.UUID,
        worker_id: str,
        lease_seconds: float,
    ) -> ExecutionRecord | None:
        """Atomically claim a runnable execution and grant a fresh lease.

        Claimable when (evaluated under the row lock, with DB time):

        * ``PENDING``;
        * ``RETRY_SCHEDULED`` and ``next_attempt_at`` has passed;
        * ``CLAIMED`` with an expired lease: the previous claimant died before
          ``mark_running`` committed, so **no external call can have started**
          and re-claiming is safe.

        ``RUNNING``/``UNKNOWN``/``RECONCILING`` are *not* claimable here: an
        external call may have happened, so they go through
        :meth:`acquire_recovery_lease` and reconciliation instead.

        Returns the claimed record, or ``None`` if another worker holds it or it
        is not runnable. Concurrency: two workers calling this simultaneously
        serialise on the row lock; the second sees ``CLAIMED`` with a live lease
        and gets ``None``. Exactly one wins.

        Transitions: ``PENDING|RETRY_SCHEDULED|CLAIMED(stale) → CLAIMED``.
        Events: ``CLAIMED``, ``LEASE_ACQUIRED``. Each claim issues a new token.
        Safe to retry: a repeated call either claims or returns ``None``.
        """
        async with self._session_factory.begin() as session:
            record = await _lock(session, workspace_id, execution_id)
            now = await _db_now(session)
            claimable = (
                record.status == S.PENDING
                or (
                    record.status == S.RETRY_SCHEDULED
                    and (record.next_attempt_at is None or record.next_attempt_at <= now)
                )
                or (record.status == S.CLAIMED and _lease_expired(record, now))
            )
            if not claimable:
                return None
            previous_owner = record.lease_owner if record.status == S.CLAIMED else None
            _transition(
                session,
                record,
                S.CLAIMED,
                EventType.CLAIMED,
                now,
                worker_id,
                {"reclaimed_from": previous_owner} if previous_owner else {},
            )
            _grant_lease(record, worker_id, now, lease_seconds)
            _event(
                session, record, EventType.LEASE_ACQUIRED, now, worker_id, _lease_details(record)
            )
            return record

    async def acquire_recovery_lease(
        self,
        *,
        workspace_id: uuid.UUID,
        execution_id: uuid.UUID,
        worker_id: str,
        lease_seconds: float,
    ) -> ExecutionRecord | None:
        """Take ownership of an execution whose outcome is uncertain, for reconciliation.

        Eligible (checked under the row lock with DB time):

        * ``UNKNOWN`` — a worker recorded that it lost the response;
        * ``RUNNING`` with an **expired** lease — the worker died mid-call;
        * ``RECONCILING`` with an expired lease — a reconciler died.

        A live lease is never stolen, so two recoverers cannot both proceed:
        the second blocks on the row lock, then sees ``RECONCILING`` with a live
        lease held by the first and returns ``None``.

        Why not reset to ``PENDING``? Because the previous attempt may already
        have produced the external side effect. The execution moves to
        ``RECONCILING`` and must be resolved by asking the external system.

        Transitions: ``UNKNOWN|RUNNING(stale)|RECONCILING(stale) → RECONCILING``.
        Events: ``RECOVERED`` (for stale leases, preserving the previous owner),
        ``LEASE_ACQUIRED``, ``RECONCILIATION_STARTED``. Increments
        ``reconcile_attempts``. Issues a new fencing token, which invalidates
        any write the previous (presumed dead) owner might still attempt.
        """
        async with self._session_factory.begin() as session:
            record = await _lock(session, workspace_id, execution_id)
            now = await _db_now(session)
            stale_leased = record.status in (S.RUNNING, S.RECONCILING) and _lease_expired(
                record, now
            )
            if not (record.status == S.UNKNOWN or stale_leased):
                return None
            if stale_leased:
                _event(
                    session,
                    record,
                    EventType.RECOVERED,
                    now,
                    worker_id,
                    {
                        "previous_status": str(record.status),
                        "previous_owner": record.lease_owner,
                        "previous_lease_expired_at": _iso(record.lease_expires_at),
                        "reason": "lease expired; previous attempt may have produced a "
                        "side effect, so reconciling instead of re-executing",
                    },
                )
            _transition(session, record, S.RECONCILING, EventType.LEASE_ACQUIRED, now, worker_id)
            _grant_lease(record, worker_id, now, lease_seconds)
            record.reconcile_attempts += 1
            _event(
                session,
                record,
                EventType.RECONCILIATION_STARTED,
                now,
                worker_id,
                {"reconcile_attempt": record.reconcile_attempts},
            )
            return record

    async def renew_lease(
        self,
        *,
        workspace_id: uuid.UUID,
        execution_id: uuid.UUID,
        lease_token: uuid.UUID,
        lease_seconds: float,
    ) -> datetime | None:
        """Extend the lease if, and only if, ``lease_token`` is still the current token.

        Returns the new expiry, or ``None`` if the lease was lost (token changed
        or execution left a leased status). A worker that gets ``None`` must
        stop making decisions about this execution.

        Renewal is allowed even if the lease *just* expired, provided nobody
        took over: any takeover issues a new token, so a token match proves no
        one else owns it. Safe to retry. No state transition, no event (to keep
        the history readable); renewals are logged instead.
        """
        async with self._session_factory.begin() as session:
            record = await _lock(session, workspace_id, execution_id)
            if record.lease_token != lease_token or record.status not in LEASED_STATUSES:
                return None
            now = await _db_now(session)
            record.lease_expires_at = now + timedelta(seconds=lease_seconds)
            record.updated_at = now
            return record.lease_expires_at

    async def release_lease(
        self, *, workspace_id: uuid.UUID, execution_id: uuid.UUID, lease_token: uuid.UUID
    ) -> bool:
        """Voluntarily give up a lease without recording an outcome.

        Only valid where giving up is provably side-effect free:

        * ``CLAIMED → PENDING`` (no external call has started);
        * ``RECONCILING → UNKNOWN`` (still uncertain; someone must reconcile).

        A ``RUNNING`` lease cannot be released — the worker must record an
        outcome (success/failure/unknown) instead, otherwise the uncertainty
        would be hidden. Returns False if the token is not current.
        Event: ``LEASE_RELEASED``.
        """
        async with self._session_factory.begin() as session:
            record = await _lock(session, workspace_id, execution_id)
            if record.lease_token != lease_token:
                return False
            if record.status == S.RUNNING:
                raise InvalidStateTransitionError(
                    "a RUNNING lease cannot be released; record the outcome (or UNKNOWN) instead"
                )
            now = await _db_now(session)
            worker_id = record.lease_owner
            target = S.PENDING if record.status == S.CLAIMED else S.UNKNOWN
            _transition(session, record, target, EventType.LEASE_RELEASED, now, worker_id)
            _clear_lease(record)
            return True

    # -------------------------------------------------------------- execution

    async def mark_running(
        self, *, workspace_id: uuid.UUID, execution_id: uuid.UUID, lease_token: uuid.UUID
    ) -> ExecutionRecord:
        """Durably record that an external call is about to start (write-ahead intent).

        This commit is what makes crash recovery correct: if the worker dies
        after this point, the record is ``RUNNING`` and recovery knows a side
        effect *may* exist and must reconcile. If it dies before this point,
        the record is ``CLAIMED`` and can be re-claimed safely.

        Requires a current **and unexpired** lease: a worker must never start a
        side effect on a lease that could already be handed to someone else.
        Increments ``attempt`` (attempt = number of external calls started).

        Transitions: ``CLAIMED → RUNNING``. Events: ``STARTED``,
        ``EXTERNAL_CALL_STARTED``. Raises :class:`StaleLeaseError` if the lease
        is not current/valid. Not retryable after success (status changed).
        """
        async with self._session_factory.begin() as session:
            record = await _lock(session, workspace_id, execution_id)
            now = await _db_now(session)
            _require_lease(record, lease_token, now, require_unexpired=True)
            record.attempt += 1
            worker_id = record.lease_owner
            _transition(session, record, S.RUNNING, EventType.STARTED, now, worker_id)
            record.started_at = now
            record.error_code = None
            record.error_message = None
            _event(
                session,
                record,
                EventType.EXTERNAL_CALL_STARTED,
                now,
                worker_id,
                {"action_type": record.action_type},
            )
            return record

    async def mark_succeeded(
        self,
        *,
        workspace_id: uuid.UUID,
        execution_id: uuid.UUID,
        lease_token: uuid.UUID,
        external_reference: str | None,
        result_payload: dict[str, Any],
        via_reconciliation: bool = False,
    ) -> ExecutionRecord:
        """Record the confirmed successful outcome and release the lease.

        ``via_reconciliation`` distinguishes "we received the response" from
        "the response was lost but the external system confirms success".

        Fencing: requires the current token (not necessarily unexpired — if no
        one has taken over, the information is still correct and valuable). A
        stale worker whose lease was taken over gets :class:`StaleLeaseError`
        and cannot overwrite the new owner's state.

        Transitions: ``RUNNING|RECONCILING → SUCCEEDED``. Events:
        ``EXTERNAL_CALL_SUCCEEDED`` or ``RECONCILIATION_SUCCEEDED``, then
        ``FINAL_SUCCESS``. ``result_payload`` is redacted before storage.
        """
        async with self._session_factory.begin() as session:
            record = await _lock(session, workspace_id, execution_id)
            now = await _db_now(session)
            _require_lease(record, lease_token, now, require_unexpired=False)
            worker_id = record.lease_owner
            first_event = (
                EventType.RECONCILIATION_SUCCEEDED
                if via_reconciliation
                else EventType.EXTERNAL_CALL_SUCCEEDED
            )
            _transition(
                session,
                record,
                S.SUCCEEDED,
                first_event,
                now,
                worker_id,
                {"external_reference": external_reference},
            )
            record.result_payload = redact(result_payload)
            record.external_reference = external_reference
            record.completed_at = now
            record.error_code = None
            record.error_message = None
            if via_reconciliation:
                record.last_reconciled_at = now
            _clear_lease(record)
            _event(session, record, EventType.FINAL_SUCCESS, now, worker_id)
            return record

    async def mark_failed(
        self,
        *,
        workspace_id: uuid.UUID,
        execution_id: uuid.UUID,
        lease_token: uuid.UUID,
        error_code: str,
        error_message: str,
        reason: str,
    ) -> ExecutionRecord:
        """Record a final failure. Only used when the action is known *not* to have
        taken effect (explicit rejection, or reconciliation confirmed it absent)
        and no further attempt is allowed.

        Fencing: current token required. Transitions: ``RUNNING|RECONCILING →
        FAILED``. Event: ``FINAL_FAILURE`` with the retry-engine ``reason``.
        """
        async with self._session_factory.begin() as session:
            record = await _lock(session, workspace_id, execution_id)
            now = await _db_now(session)
            _require_lease(record, lease_token, now, require_unexpired=False)
            worker_id = record.lease_owner
            if record.status == S.RECONCILING:
                record.last_reconciled_at = now
            _transition(
                session,
                record,
                S.FAILED,
                EventType.FINAL_FAILURE,
                now,
                worker_id,
                {"error_code": error_code, "reason": reason},
            )
            record.error_code = error_code
            record.error_message = error_message[:_MAX_ERROR_MESSAGE]
            record.completed_at = now
            _clear_lease(record)
            return record

    async def mark_unknown(
        self,
        *,
        workspace_id: uuid.UUID,
        execution_id: uuid.UUID,
        lease_token: uuid.UUID,
        error_code: str,
        error_message: str,
    ) -> ExecutionRecord:
        """Record that the external outcome is uncertain (response lost / timed out).

        ``UNKNOWN`` does **not** mean failed. The external system may have
        committed the action. The lease is released so any worker (or the
        recovery sweep) can pick it up, but the only legal next step is
        ``RECONCILING`` — never a blind re-execution.

        Fencing: current token required. Transitions: ``RUNNING → UNKNOWN``.
        Events: ``CONNECTION_LOST``, ``UNKNOWN``.
        """
        async with self._session_factory.begin() as session:
            record = await _lock(session, workspace_id, execution_id)
            now = await _db_now(session)
            _require_lease(record, lease_token, now, require_unexpired=False)
            worker_id = record.lease_owner
            _event(
                session,
                record,
                EventType.CONNECTION_LOST,
                now,
                worker_id,
                {"error_code": error_code},
            )
            _transition(session, record, S.UNKNOWN, EventType.UNKNOWN, now, worker_id)
            record.error_code = error_code
            record.error_message = error_message[:_MAX_ERROR_MESSAGE]
            _clear_lease(record)
            return record

    async def schedule_retry(
        self,
        *,
        workspace_id: uuid.UUID,
        execution_id: uuid.UUID,
        lease_token: uuid.UUID,
        delay_seconds: float,
        error_code: str,
        error_message: str,
        reason: str,
    ) -> ExecutionRecord:
        """Schedule another attempt after ``delay_seconds`` and release the lease.

        Only called after the retry engine has decided a repeat is safe: the
        failure was an explicit transient rejection, or reconciliation proved
        the action did not happen, or the action is idempotent by key.

        Fencing: current token required. Transitions: ``RUNNING|RECONCILING →
        RETRY_SCHEDULED``. Event: ``RETRY_SCHEDULED`` with delay and reason.
        """
        async with self._session_factory.begin() as session:
            record = await _lock(session, workspace_id, execution_id)
            now = await _db_now(session)
            _require_lease(record, lease_token, now, require_unexpired=False)
            worker_id = record.lease_owner
            if record.status == S.RECONCILING:
                record.last_reconciled_at = now
            _transition(
                session,
                record,
                S.RETRY_SCHEDULED,
                EventType.RETRY_SCHEDULED,
                now,
                worker_id,
                {"delay_seconds": delay_seconds, "error_code": error_code, "reason": reason},
            )
            record.next_attempt_at = now + timedelta(seconds=delay_seconds)
            record.error_code = error_code
            record.error_message = error_message[:_MAX_ERROR_MESSAGE]
            _clear_lease(record)
            return record

    async def mark_manual_review(
        self,
        *,
        workspace_id: uuid.UUID,
        execution_id: uuid.UUID,
        lease_token: uuid.UUID,
        reason: str,
    ) -> ExecutionRecord:
        """Park an execution whose outcome cannot be determined safely.

        Used for non-idempotent actions when reconciliation cannot prove whether
        the side effect happened. Automation stops here on purpose: guessing
        "failed" could cause a duplicate physical action; guessing "succeeded"
        could silently drop one. A human (or a separate process) must decide.

        Fencing: current token required. Transitions: ``RECONCILING →
        MANUAL_REVIEW_REQUIRED``. Event: ``MANUAL_REVIEW_REQUIRED``.
        """
        async with self._session_factory.begin() as session:
            record = await _lock(session, workspace_id, execution_id)
            now = await _db_now(session)
            _require_lease(record, lease_token, now, require_unexpired=False)
            worker_id = record.lease_owner
            _transition(
                session,
                record,
                S.MANUAL_REVIEW_REQUIRED,
                EventType.MANUAL_REVIEW_REQUIRED,
                now,
                worker_id,
                {"reason": reason},
            )
            record.last_reconciled_at = now
            record.error_code = "OUTCOME_UNDETERMINED"
            record.error_message = reason[:_MAX_ERROR_MESSAGE]
            _clear_lease(record)
            return record

    async def return_to_unknown(
        self,
        *,
        workspace_id: uuid.UUID,
        execution_id: uuid.UUID,
        lease_token: uuid.UUID,
        reason: str,
    ) -> ExecutionRecord:
        """Reconciliation could not reach the external system; try again later.

        Fencing: current token required. Transitions: ``RECONCILING → UNKNOWN``.
        Event: ``RECONCILIATION_INCONCLUSIVE``. ``reconcile_attempts`` already
        counts this attempt, which bounds how often this can repeat.
        """
        async with self._session_factory.begin() as session:
            record = await _lock(session, workspace_id, execution_id)
            now = await _db_now(session)
            _require_lease(record, lease_token, now, require_unexpired=False)
            worker_id = record.lease_owner
            _transition(
                session,
                record,
                S.UNKNOWN,
                EventType.RECONCILIATION_INCONCLUSIVE,
                now,
                worker_id,
                {"reason": reason},
            )
            record.last_reconciled_at = now
            _clear_lease(record)
            return record

    async def record_event(
        self,
        *,
        workspace_id: uuid.UUID,
        execution_id: uuid.UUID,
        lease_token: uuid.UUID,
        event_type: EventType,
        details: dict[str, Any],
    ) -> None:
        """Append an informational event (no transition) as the current lease owner."""
        async with self._session_factory.begin() as session:
            record = await _lock(session, workspace_id, execution_id)
            now = await _db_now(session)
            _require_lease(record, lease_token, now, require_unexpired=False)
            _event(session, record, event_type, now, record.lease_owner, details)


# ---------------------------------------------------------------------- helpers


async def _lock(
    session: AsyncSession, workspace_id: uuid.UUID, execution_id: uuid.UUID
) -> ExecutionRecord:
    record = (
        await session.execute(
            select(ExecutionRecord)
            .where(
                ExecutionRecord.id == execution_id,
                ExecutionRecord.workspace_id == workspace_id,
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    if record is None:
        raise ExecutionNotFoundError(f"execution {execution_id} not found")
    return record


async def _db_now(session: AsyncSession) -> datetime:
    # clock_timestamp() (not now()) so the value reflects time *after* any wait
    # for the row lock, not the start of the transaction.
    value: datetime = (
        await session.execute(select(func.clock_timestamp(type_=DateTime(timezone=True))))
    ).scalar_one()
    return value


def _lease_expired(record: ExecutionRecord, now: datetime) -> bool:
    return record.lease_expires_at is None or record.lease_expires_at <= now


def _require_lease(
    record: ExecutionRecord, token: uuid.UUID, now: datetime, *, require_unexpired: bool
) -> None:
    if record.lease_token is None or record.lease_token != token:
        raise StaleLeaseError(
            f"lease token for execution {record.id} is not current "
            f"(status={record.status}, current_owner={record.lease_owner})"
        )
    if require_unexpired and _lease_expired(record, now):
        raise StaleLeaseError(f"lease for execution {record.id} has expired")


def _grant_lease(
    record: ExecutionRecord, worker_id: str, now: datetime, lease_seconds: float
) -> None:
    record.lease_owner = worker_id
    record.lease_token = uuid.uuid4()
    record.lease_expires_at = now + timedelta(seconds=lease_seconds)


def _clear_lease(record: ExecutionRecord) -> None:
    record.lease_owner = None
    record.lease_token = None
    record.lease_expires_at = None


def _lease_details(record: ExecutionRecord) -> dict[str, Any]:
    return {"lease_owner": record.lease_owner, "lease_expires_at": _iso(record.lease_expires_at)}


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _transition(
    session: AsyncSession,
    record: ExecutionRecord,
    target: ExecutionStatus,
    event_type: EventType,
    now: datetime,
    worker_id: str | None,
    details: dict[str, Any] | None = None,
) -> None:
    validate_transition(record.status, target)
    from_status = record.status
    record.status = target
    record.updated_at = now
    session.add(
        ExecutionEvent(
            execution_id=record.id,
            workspace_id=record.workspace_id,
            event_type=event_type,
            from_status=from_status,
            to_status=target,
            attempt=record.attempt,
            worker_id=worker_id,
            details=redact(details or {}),
            created_at=now,
        )
    )


def _event(
    session: AsyncSession,
    record: ExecutionRecord,
    event_type: EventType,
    now: datetime,
    worker_id: str | None,
    details: dict[str, Any] | None = None,
) -> None:
    session.add(
        ExecutionEvent(
            execution_id=record.id,
            workspace_id=record.workspace_id,
            event_type=event_type,
            from_status=record.status,
            to_status=record.status,
            attempt=record.attempt,
            worker_id=worker_id,
            details=redact(details or {}),
            created_at=now,
        )
    )


def event_types(events: Sequence[ExecutionEvent]) -> list[EventType]:
    """Convenience for tests and the demo: the ordered list of event types."""
    return [event.event_type for event in events]
