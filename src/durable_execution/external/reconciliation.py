"""Reconciliation: ask the external system what actually happened.

A missing response is not a failure. Before anything is retried after an
uncertain outcome, the reconciler queries the external system by the same
idempotency key and classifies what it learns:

========================  =====================================================
ReconciliationStatus      Meaning
========================  =====================================================
EXECUTED_SUCCEEDED        The side effect exists. Record success; never repeat.
EXECUTED_FAILED           The external system holds the action as failed.
NOT_EXECUTED              Authoritative lookup proves the action never happened.
INDETERMINATE             Lookup answered "not found" but is not authoritative.
LOOKUP_UNAVAILABLE        Could not ask (outage). Nothing learned; ask again.
========================  =====================================================

The reconciler only *observes*; it never changes state. The recovery service
turns its answer into a state transition via the retry decision engine.
"""

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from durable_execution.external.client import (
    ActionRegistry,
    ExternalActionClient,
    ExternalError,
    StatusLookupUnavailableError,
)
from durable_execution.external.fake_service import ExternalActionStatus
from durable_execution.logging.setup import get_logger, log_event
from durable_execution.models.execution import ExecutionRecord

logger = get_logger("reconciliation")


class ReconciliationStatus(StrEnum):
    EXECUTED_SUCCEEDED = "EXECUTED_SUCCEEDED"
    EXECUTED_FAILED = "EXECUTED_FAILED"
    NOT_EXECUTED = "NOT_EXECUTED"
    INDETERMINATE = "INDETERMINATE"
    LOOKUP_UNAVAILABLE = "LOOKUP_UNAVAILABLE"


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    status: ReconciliationStatus
    detail: str
    external_reference: str | None = None
    result_payload: dict[str, Any] = field(default_factory=dict)


class Reconciler:
    def __init__(self, client: ExternalActionClient, registry: ActionRegistry) -> None:
        self._client = client
        self._registry = registry

    async def reconcile(self, record: ExecutionRecord) -> ReconciliationResult:
        """Determine the true external outcome of ``record`` without side effects.

        Why: after a lost response the database cannot know whether the
        external system committed the action. Only the external system can.

        Invariants: performs read-only status lookups using the execution's own
        idempotency key; never calls ``perform_action``. Requires that the caller
        holds the execution's (recovery) lease so only one reconciler acts on
        the answer; the lease itself is not touched here.

        Failure: lookup outages become ``LOOKUP_UNAVAILABLE`` (never an
        exception), so the caller can return the execution to ``UNKNOWN``.
        Safe to retry: yes, it is a pure read.
        """
        spec = self._registry.get(record.action_type)
        try:
            found = await self._client.get_action_status(
                action_type=record.action_type, idempotency_key=record.idempotency_key
            )
        except StatusLookupUnavailableError as exc:
            log_event(logger, "reconciliation_lookup_unavailable", record=record, error=exc.code)
            return ReconciliationResult(ReconciliationStatus.LOOKUP_UNAVAILABLE, str(exc))
        except ExternalError as exc:
            # e.g. credentials rejected on the status endpoint: we learned nothing.
            log_event(logger, "reconciliation_lookup_failed", record=record, error=exc.code)
            return ReconciliationResult(ReconciliationStatus.LOOKUP_UNAVAILABLE, str(exc))

        if found is None:
            if spec.authoritative_status_lookup:
                result = ReconciliationResult(
                    ReconciliationStatus.NOT_EXECUTED,
                    "authoritative lookup found no action for this idempotency key",
                )
            else:
                result = ReconciliationResult(
                    ReconciliationStatus.INDETERMINATE,
                    "lookup found nothing, but this action's lookup is not authoritative",
                )
        elif found.status is ExternalActionStatus.SUCCEEDED:
            result = ReconciliationResult(
                ReconciliationStatus.EXECUTED_SUCCEEDED,
                "external system confirms the action succeeded",
                external_reference=found.external_reference,
                result_payload=found.summary,
            )
        else:
            result = ReconciliationResult(
                ReconciliationStatus.EXECUTED_FAILED,
                "external system holds the action as failed",
                external_reference=found.external_reference,
                result_payload=found.summary,
            )
        log_event(logger, "reconciliation_result", record=record, result=str(result.status))
        return result
