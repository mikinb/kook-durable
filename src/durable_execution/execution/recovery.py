"""Recovery of executions whose outcome is uncertain.

Handles three situations with one code path:

* ``UNKNOWN`` — a worker lost the response and said so;
* ``RUNNING`` with an expired lease — a worker died mid-call and said nothing;
* ``RECONCILING`` with an expired lease — a reconciler died.

In all three, the previous attempt *may* have produced an external side effect,
so recovery never resets to ``PENDING``. It (1) atomically takes a recovery
lease (only if the old lease is genuinely expired), (2) reconciles with the
external system, and (3) applies the retry engine's decision:

======================  =============================================================
Reconciliation          Resulting transition
======================  =============================================================
EXECUTED_SUCCEEDED      → SUCCEEDED (existing external result recorded, no new call)
EXECUTED_FAILED         → FAILED
NOT_EXECUTED            → RETRY_SCHEDULED, or FAILED if attempts are exhausted
INDETERMINATE           → RETRY_SCHEDULED only if idempotent by key, else MANUAL_REVIEW
LOOKUP_UNAVAILABLE      → UNKNOWN (try later); after ``max_reconcile_attempts``
                          treated as INDETERMINATE
======================  =============================================================

History is preserved: every step is appended to ``execution_events``.
"""

import uuid

from durable_execution.database.repository import ExecutionRepository
from durable_execution.errors import ExecutionNotFoundError, StaleLeaseError
from durable_execution.execution.lease import Lease, LeaseManager
from durable_execution.execution.outcomes import RunOutcome, RunResult
from durable_execution.execution.retry import ExternalConfirmation, RetryDecisionEngine
from durable_execution.external.client import ActionRegistry
from durable_execution.external.reconciliation import (
    Reconciler,
    ReconciliationResult,
    ReconciliationStatus,
)
from durable_execution.logging.setup import get_logger, log_event
from durable_execution.models.enums import ErrorCategory, EventType
from durable_execution.models.execution import ExecutionRecord
from durable_execution.security.context import ExecutionContext, bind_context

logger = get_logger("recovery")


class RecoveryService:
    def __init__(
        self,
        *,
        repository: ExecutionRepository,
        lease_manager: LeaseManager,
        reconciler: Reconciler,
        retry_engine: RetryDecisionEngine,
        registry: ActionRegistry,
        max_reconcile_attempts: int = 3,
    ) -> None:
        self._repository = repository
        self._leases = lease_manager
        self._reconciler = reconciler
        self._retry = retry_engine
        self._registry = registry
        self._max_reconcile_attempts = max_reconcile_attempts

    @property
    def worker_id(self) -> str:
        return self._leases.worker_id

    async def recover_stale_executions(self, *, limit: int = 100) -> list[RunResult]:
        """Sweep for uncertain/stale executions and recover each one.

        Why: a worker that crashes cannot report its own death. A periodic
        sweep (run by any worker) is what guarantees that every execution
        eventually leaves ``RUNNING``/``UNKNOWN``.

        Concurrency: many sweepers may run at once. The candidate query is
        only a hint; each takeover is a row-locked conditional update, so a
        given execution is recovered by at most one sweeper at a time and the
        others get ``SKIPPED_LEASE_HELD``. Safe to run repeatedly.
        """
        results: list[RunResult] = []
        for workspace_id, execution_id in await self._repository.find_recoverable(limit=limit):
            try:
                record = await self._repository.get(
                    workspace_id=workspace_id, execution_id=execution_id
                )
            except ExecutionNotFoundError:
                continue
            ctx = ExecutionContext(record.workspace_id, record.task_id, record.agent_id)
            results.append(await self.recover_stale_execution(ctx, execution_id=record.id))
        return results

    async def recover_stale_execution(
        self, ctx: ExecutionContext, *, execution_id: uuid.UUID
    ) -> RunResult:
        """Recover one uncertain execution: take lease → reconcile → decide → record.

        Steps and guarantees:

        1. **Verify the old lease is expired and take over atomically**
           (:meth:`LeaseManager.acquire_recovery_lease`). A live lease is never
           stolen; returns ``SKIPPED_LEASE_HELD`` instead.
        2. **Reconcile**: query the external system by idempotency key. No
           ``perform_action`` call is ever made here.
        3. **Decide** with :class:`RetryDecisionEngine` and record the result
           with the recovery lease's fencing token.

        Transitions: ``UNKNOWN|RUNNING|RECONCILING → RECONCILING → {SUCCEEDED,
        FAILED, RETRY_SCHEDULED, UNKNOWN, MANUAL_REVIEW_REQUIRED}``.

        Failure: if this process crashes mid-way, the execution stays
        ``RECONCILING`` with an expiring lease and the next sweep takes it over.
        Safe to retry.
        """
        with bind_context(ctx):
            acquired = await self._leases.acquire_recovery_lease(
                workspace_id=ctx.workspace_id, execution_id=execution_id
            )
            if acquired is None:
                record = await self._repository.get(
                    workspace_id=ctx.workspace_id, execution_id=execution_id
                )
                log_event(logger, "recovery_skipped_lease_held", record=record)
                return RunResult.from_record(
                    RunOutcome.SKIPPED_LEASE_HELD,
                    record,
                    self.worker_id,
                    "execution is not eligible for recovery (live lease or settled)",
                )
            lease, record = acquired
            log_event(logger, "reconciliation_started", record=record)
            result = await self._reconciler.reconcile(record)
            try:
                return await self._apply(lease, record, result)
            except StaleLeaseError:
                current = await self._repository.get(
                    workspace_id=ctx.workspace_id, execution_id=execution_id
                )
                log_event(logger, "stale_write_rejected", record=current)
                return RunResult.from_record(
                    RunOutcome.LEASE_LOST, current, self.worker_id, "recovery lease lost"
                )

    async def _apply(
        self, lease: Lease, record: ExecutionRecord, result: ReconciliationResult
    ) -> RunResult:
        ids = {
            "workspace_id": lease.workspace_id,
            "execution_id": lease.execution_id,
            "lease_token": lease.token,
        }
        status = result.status

        if status is ReconciliationStatus.EXECUTED_SUCCEEDED:
            updated = await self._repository.mark_succeeded(
                **ids,
                external_reference=result.external_reference,
                result_payload=result.result_payload,
                via_reconciliation=True,
            )
            log_event(logger, "recovered_existing_success", record=updated)
            return RunResult.from_record(
                RunOutcome.SUCCEEDED, updated, self.worker_id, "external system confirmed success"
            )

        if status is ReconciliationStatus.EXECUTED_FAILED:
            updated = await self._repository.mark_failed(
                **ids,
                error_code="EXTERNAL_ACTION_FAILED",
                error_message=result.detail,
                reason="external system holds the action as failed",
            )
            return RunResult.from_record(RunOutcome.FAILED, updated, self.worker_id, result.detail)

        if (
            status is ReconciliationStatus.LOOKUP_UNAVAILABLE
            and record.reconcile_attempts < self._max_reconcile_attempts
        ):
            updated = await self._repository.return_to_unknown(
                **ids,
                reason=f"status lookup unavailable (reconcile attempt "
                f"{record.reconcile_attempts}/{self._max_reconcile_attempts})",
            )
            log_event(logger, "reconciliation_deferred", record=updated)
            return RunResult.from_record(RunOutcome.UNKNOWN, updated, self.worker_id, result.detail)

        confirmation = (
            ExternalConfirmation.CONFIRMED_NOT_EXECUTED
            if status is ReconciliationStatus.NOT_EXECUTED
            else ExternalConfirmation.INCONCLUSIVE
        )
        if status is ReconciliationStatus.NOT_EXECUTED:
            await self._repository.record_event(
                **ids, event_type=EventType.RECONCILIATION_NOT_EXECUTED, details={}
            )
        else:
            await self._repository.record_event(
                **ids,
                event_type=EventType.RECONCILIATION_INCONCLUSIVE,
                details={"reason": result.detail},
            )
        spec = self._registry.get(record.action_type)
        decision = self._retry.should_retry(
            category=ErrorCategory.OUTCOME_UNKNOWN,
            status=record.status,
            attempt=record.attempt,
            max_attempts=record.max_attempts,
            action_idempotent=spec.idempotent_by_key,
            external_confirmation=confirmation,
            holds_lease=True,
        )
        log_event(
            logger, "retry_decision", record=record, retry=decision.retry, reason=decision.reason
        )
        if decision.retry:
            updated = await self._repository.schedule_retry(
                **ids,
                delay_seconds=decision.delay_seconds,
                error_code=record.error_code or "OUTCOME_UNKNOWN",
                error_message=result.detail,
                reason=decision.reason,
            )
            return RunResult.from_record(
                RunOutcome.RETRY_SCHEDULED, updated, self.worker_id, decision.reason
            )
        if decision.requires_manual_review:
            updated = await self._repository.mark_manual_review(**ids, reason=decision.reason)
            log_event(logger, "manual_review_required", record=updated, reason=decision.reason)
            return RunResult.from_record(
                RunOutcome.MANUAL_REVIEW_REQUIRED, updated, self.worker_id, decision.reason
            )
        updated = await self._repository.mark_failed(
            **ids,
            error_code="MAX_ATTEMPTS_EXHAUSTED",
            error_message=result.detail,
            reason=decision.reason,
        )
        return RunResult.from_record(RunOutcome.FAILED, updated, self.worker_id, decision.reason)
