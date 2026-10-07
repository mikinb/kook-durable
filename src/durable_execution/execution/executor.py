"""Executor: performs one external call and classifies what came back.

The executor has no database access and makes no state decisions. It converts
"what happened on the wire" into an :class:`ExecutionOutcome` the worker can
reason about. Keeping this separate keeps the worker small and makes the
classification rules easy to read in one place.
"""

import time
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from durable_execution.external.client import (
    AuthenticationExternalError,
    ExternalActionClient,
    OutcomeUnknownError,
    PermanentExternalError,
    TransientExternalError,
    ValidationExternalError,
)
from durable_execution.logging.setup import get_logger, log_event
from durable_execution.models.enums import ErrorCategory
from durable_execution.models.execution import ExecutionRecord

logger = get_logger("executor")


class OutcomeKind(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    OUTCOME_UNKNOWN = "OUTCOME_UNKNOWN"
    REJECTED = "REJECTED"  # explicit response saying the action was not applied


@dataclass(frozen=True, slots=True)
class ExecutionOutcome:
    kind: OutcomeKind
    duration_ms: float
    category: ErrorCategory | None = None
    external_reference: str | None = None
    result_payload: dict[str, Any] = field(default_factory=dict)
    error_code: str | None = None
    error_message: str | None = None


class ActionExecutor:
    def __init__(self, client: ExternalActionClient) -> None:
        self._client = client

    async def execute(self, record: ExecutionRecord) -> ExecutionOutcome:
        """Call the external system once for ``record`` and classify the result.

        Why: isolates the single side-effecting operation. The caller must
        already have committed ``RUNNING`` (write-ahead intent) under a valid
        lease before calling this, so a crash during the call is recoverable.

        Invariants: always sends the execution's own idempotency key; never
        retries internally (retries are a durable, decided, recorded event,
        not a hidden loop); never touches the database.

        Classification:

        * response received → ``SUCCEEDED``;
        * connection lost / timeout → ``OUTCOME_UNKNOWN`` (side effect may exist);
        * explicit 5xx/429 → ``REJECTED`` + ``TRANSIENT``;
        * explicit 4xx → ``REJECTED`` + ``VALIDATION``/``AUTHENTICATION``/``PERMANENT``;
        * any *unexpected* exception after the call began → ``OUTCOME_UNKNOWN``,
          logged with its type. We cannot prove the side effect did not happen,
          so the only safe classification is "unknown".

        Not safe to call twice for the same attempt; that is the worker's job
        to prevent via the lease and state machine.
        """
        start = time.perf_counter()
        try:
            result = await self._client.perform_action(
                action_type=record.action_type,
                idempotency_key=record.idempotency_key,
                payload=record.request_payload,
            )
        except OutcomeUnknownError as exc:
            return self._failure(start, OutcomeKind.OUTCOME_UNKNOWN, exc.code, exc.message)
        except AuthenticationExternalError as exc:
            return self._failure(
                start, OutcomeKind.REJECTED, exc.code, exc.message, ErrorCategory.AUTHENTICATION
            )
        except ValidationExternalError as exc:
            return self._failure(
                start, OutcomeKind.REJECTED, exc.code, exc.message, ErrorCategory.VALIDATION
            )
        except PermanentExternalError as exc:
            return self._failure(
                start, OutcomeKind.REJECTED, exc.code, exc.message, ErrorCategory.PERMANENT
            )
        except TransientExternalError as exc:
            return self._failure(
                start, OutcomeKind.REJECTED, exc.code, exc.message, ErrorCategory.TRANSIENT
            )
        except Exception as exc:  # noqa: BLE001 - logged and translated to UNKNOWN on purpose
            log_event(
                logger,
                "external_call_unexpected_error",
                record=record,
                error_type=type(exc).__name__,
            )
            return self._failure(
                start,
                OutcomeKind.OUTCOME_UNKNOWN,
                "UNEXPECTED_CLIENT_ERROR",
                f"unexpected {type(exc).__name__} during external call",
            )
        return ExecutionOutcome(
            kind=OutcomeKind.SUCCEEDED,
            duration_ms=_elapsed_ms(start),
            external_reference=result.external_reference,
            result_payload=result.summary,
        )

    @staticmethod
    def _failure(
        start: float,
        kind: OutcomeKind,
        code: str,
        message: str,
        category: ErrorCategory = ErrorCategory.OUTCOME_UNKNOWN,
    ) -> ExecutionOutcome:
        return ExecutionOutcome(
            kind=kind,
            duration_ms=_elapsed_ms(start),
            category=category,
            error_code=code,
            error_message=message,
        )


def _elapsed_ms(start: float) -> float:
    return round((time.perf_counter() - start) * 1000, 2)
