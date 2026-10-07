"""Worker: drives one execution forward by exactly one durable step.

``Worker.run`` is deliberately small. It routes the execution by its *durable*
status (never by anything remembered in memory):

* settled (``SUCCEEDED``/``FAILED``/``CANCELLED``/``MANUAL_REVIEW_REQUIRED``)
  → return the stored result; do nothing;
* uncertain (``UNKNOWN``, or ``RUNNING``/``RECONCILING``) → delegate to
  :class:`RecoveryService` (reconcile first, never re-execute blindly);
* runnable (``PENDING``, due ``RETRY_SCHEDULED``, stale ``CLAIMED``) → claim a
  lease, commit ``RUNNING``, call the external system once, record the outcome.

Each run performs at most **one** external call. Retries are not loops inside
the worker; they are durable ``RETRY_SCHEDULED`` states that any worker can
pick up later, so a crash between attempts loses nothing.
"""

import asyncio
import uuid

from durable_execution.database.repository import ExecutionRepository
from durable_execution.errors import ExecutionContextMismatchError, StaleLeaseError
from durable_execution.execution.executor import ActionExecutor, ExecutionOutcome, OutcomeKind
from durable_execution.execution.lease import Lease, LeaseManager
from durable_execution.execution.outcomes import RunOutcome, RunResult
from durable_execution.execution.recovery import RecoveryService
from durable_execution.execution.retry import ExternalConfirmation, RetryDecisionEngine
from durable_execution.external.client import ActionRegistry
from durable_execution.logging.setup import get_logger, log_event
from durable_execution.models.enums import (
    NEEDS_RECONCILIATION_STATUSES,
    ErrorCategory,
    ExecutionStatus,
)
from durable_execution.models.execution import ExecutionRecord
from durable_execution.security.context import ExecutionContext, bind_context

logger = get_logger("worker")


class Worker:
    def __init__(
        self,
        *,
        repository: ExecutionRepository,
        lease_manager: LeaseManager,
        executor: ActionExecutor,
        recovery: RecoveryService,
        retry_engine: RetryDecisionEngine,
        registry: ActionRegistry,
    ) -> None:
        self._repository = repository
        self._leases = lease_manager
        self._executor = executor
        self._recovery = recovery
        self._retry = retry_engine
        self._registry = registry

    @property
    def worker_id(self) -> str:
        return self._leases.worker_id

    async def run(self, ctx: ExecutionContext, execution_id: uuid.UUID) -> RunResult:
        """Advance ``execution_id`` by one durable step.

        Why: this is the single entry point a scheduler/queue consumer calls.
        It is safe to call any number of times, concurrently, from any number
        of processes: the lease guarantees at most one active executor, and the
        durable status decides what the step is.

        Tenant isolation: the record is loaded with ``ctx.workspace_id``; a
        record from another workspace is "not found". A record from the same
        workspace but another task/agent raises
        :class:`ExecutionContextMismatchError`.

        Returns a :class:`RunResult`. Never raises for expected contention
        (lease held, not due, fenced) — those are outcomes, not errors.
        """
        with bind_context(ctx):
            record = await self._repository.get(
                workspace_id=ctx.workspace_id, execution_id=execution_id
            )
            if record.task_id != ctx.task_id or record.agent_id != ctx.agent_id:
                raise ExecutionContextMismatchError(
                    f"execution {execution_id} belongs to a different task/agent"
                )
            if record.status.is_settled:
                log_event(logger, "already_settled", record=record)
                return RunResult.from_record(RunOutcome.ALREADY_SETTLED, record, self.worker_id)
            if record.status in NEEDS_RECONCILIATION_STATUSES:
                log_event(logger, "uncertain_outcome_detected", record=record)
                return await self._recovery.recover_stale_execution(ctx, execution_id=execution_id)

            acquired = await self._leases.acquire_lease(
                workspace_id=ctx.workspace_id, execution_id=execution_id
            )
            if acquired is None:
                return await self._not_claimed(ctx, execution_id)
            lease, claimed = acquired
            return await self._attempt(lease, claimed)

    async def run_until_settled(
        self,
        ctx: ExecutionContext,
        execution_id: uuid.UUID,
        *,
        max_steps: int = 20,
        poll_interval: float = 0.05,
    ) -> RunResult:
        """Convenience driver: call :meth:`run` until the execution settles.

        Stops early when another worker owns the execution or this worker was
        fenced off. Waits ``poll_interval`` while a retry is not yet due. In
        production a scheduler would play this role; here it keeps tests and
        the demo readable.
        """
        result: RunResult | None = None
        for _ in range(max_steps):
            result = await self.run(ctx, execution_id)
            if result.status.is_settled or result.outcome in (
                RunOutcome.SKIPPED_LEASE_HELD,
                RunOutcome.LEASE_LOST,
            ):
                return result
            if result.outcome is RunOutcome.NOT_DUE:
                await asyncio.sleep(poll_interval)
        if result is None:
            raise ValueError("max_steps must be >= 1")
        return result

    # ------------------------------------------------------------- internals

    async def _attempt(self, lease: Lease, record: ExecutionRecord) -> RunResult:
        try:
            running = await self._repository.mark_running(
                workspace_id=lease.workspace_id,
                execution_id=lease.execution_id,
                lease_token=lease.token,
            )
        except StaleLeaseError:
            return await self._fenced(lease, "lease lost before the external call started")

        log_event(logger, "external_call_started", record=running)
        async with self._leases.keep_alive(lease) as heartbeat:
            outcome = await self._executor.execute(running)
        log_event(
            logger,
            "external_call_finished",
            record=running,
            outcome=str(outcome.kind),
            error_code=outcome.error_code,
            duration_ms=outcome.duration_ms,
        )
        try:
            return await self._record_outcome(heartbeat.lease, running, outcome, heartbeat.lost)
        except StaleLeaseError:
            return await self._fenced(lease, "lease was taken over; outcome write rejected")

    async def _record_outcome(
        self, lease: Lease, record: ExecutionRecord, outcome: ExecutionOutcome, lease_lost: bool
    ) -> RunResult:
        ids = {
            "workspace_id": lease.workspace_id,
            "execution_id": lease.execution_id,
            "lease_token": lease.token,
        }
        if outcome.kind is OutcomeKind.SUCCEEDED:
            updated = await self._repository.mark_succeeded(
                **ids,
                external_reference=outcome.external_reference,
                result_payload=outcome.result_payload,
                via_reconciliation=False,
            )
            log_event(
                logger, "execution_succeeded", record=updated, duration_ms=outcome.duration_ms
            )
            return RunResult.from_record(RunOutcome.SUCCEEDED, updated, self.worker_id)

        error_code = outcome.error_code or "UNKNOWN_ERROR"
        error_message = outcome.error_message or ""

        if outcome.kind is OutcomeKind.OUTCOME_UNKNOWN:
            # Do NOT treat as failure and do NOT retry: the side effect may exist.
            updated = await self._repository.mark_unknown(
                **ids, error_code=error_code, error_message=error_message
            )
            log_event(logger, "execution_outcome_unknown", record=updated, error_code=error_code)
            return RunResult.from_record(
                RunOutcome.UNKNOWN,
                updated,
                self.worker_id,
                "response lost; reconciliation required before any retry",
            )

        # REJECTED: the external system answered explicitly, so it did not apply
        # the action. Whether to try again is the retry engine's call.
        spec = self._registry.get(record.action_type)
        decision = self._retry.should_retry(
            category=outcome.category or ErrorCategory.PERMANENT,
            status=record.status,
            attempt=record.attempt,
            max_attempts=record.max_attempts,
            action_idempotent=spec.idempotent_by_key,
            external_confirmation=ExternalConfirmation.CONFIRMED_NOT_EXECUTED,
            holds_lease=not lease_lost,
        )
        log_event(
            logger,
            "retry_decision",
            record=record,
            retry=decision.retry,
            reason=decision.reason,
            delay_seconds=decision.delay_seconds,
            error_code=error_code,
        )
        if decision.retry:
            updated = await self._repository.schedule_retry(
                **ids,
                delay_seconds=decision.delay_seconds,
                error_code=error_code,
                error_message=error_message,
                reason=decision.reason,
            )
            return RunResult.from_record(
                RunOutcome.RETRY_SCHEDULED, updated, self.worker_id, decision.reason
            )
        updated = await self._repository.mark_failed(
            **ids, error_code=error_code, error_message=error_message, reason=decision.reason
        )
        log_event(logger, "execution_failed", record=updated, error_code=error_code)
        return RunResult.from_record(RunOutcome.FAILED, updated, self.worker_id, decision.reason)

    async def _not_claimed(self, ctx: ExecutionContext, execution_id: uuid.UUID) -> RunResult:
        current = await self._repository.get(
            workspace_id=ctx.workspace_id, execution_id=execution_id
        )
        if current.status is ExecutionStatus.RETRY_SCHEDULED:
            return RunResult.from_record(RunOutcome.NOT_DUE, current, self.worker_id)
        if current.status.is_settled:
            return RunResult.from_record(RunOutcome.ALREADY_SETTLED, current, self.worker_id)
        log_event(logger, "claim_rejected_lease_held", record=current)
        return RunResult.from_record(
            RunOutcome.SKIPPED_LEASE_HELD,
            current,
            self.worker_id,
            f"lease held by {current.lease_owner}",
        )

    async def _fenced(self, lease: Lease, detail: str) -> RunResult:
        current = await self._repository.get(
            workspace_id=lease.workspace_id, execution_id=lease.execution_id
        )
        log_event(logger, "stale_write_rejected", record=current, stale_owner=lease.owner)
        return RunResult.from_record(RunOutcome.LEASE_LOST, current, self.worker_id, detail)
