"""Idempotency: one key ⇒ one logical action, forever.

An idempotency key identifies a *logical* action chosen by the caller (e.g. an
agent planning step). Submitting the same key again must return the existing
execution and never create a second external side effect.

The service also guards against the subtle bug of reusing a key for a
*different* request (different amount, customer, parameters or action type).
That is rejected with :class:`IdempotencyConflictError` rather than silently
returning the old result for a request the caller did not actually make.

What idempotency does *not* give you on its own: exactly-once external effects.
It guarantees one durable *record*; the lease, write-ahead ``RUNNING`` state and
reconciliation are what prevent a second *external call* after a crash.
"""

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from durable_execution.database.repository import ExecutionRepository
from durable_execution.errors import IdempotencyConflictError, SensitivePayloadError
from durable_execution.external.client import ActionRegistry
from durable_execution.logging.setup import get_logger, log_event
from durable_execution.models.execution import ExecutionRecord
from durable_execution.security.context import ExecutionContext, find_sensitive_keys

logger = get_logger("idempotency")

_KEY_PATTERN = re.compile(r"^[A-Za-z0-9._:\-]{8,128}$")


class ExecutionRequest(BaseModel):
    """A request to perform one external action."""

    model_config = ConfigDict(frozen=True)

    action_type: str = Field(min_length=1, max_length=64)
    idempotency_key: str
    payload: dict[str, Any]
    max_attempts: int | None = Field(default=None, ge=1, le=20)

    @field_validator("idempotency_key")
    @classmethod
    def _validate_key(cls, value: str) -> str:
        if not _KEY_PATTERN.match(value):
            raise ValueError("idempotency_key must be 8-128 chars of [A-Za-z0-9._:-]")
        return value


@dataclass(frozen=True, slots=True)
class SubmissionResult:
    record: ExecutionRecord
    created: bool


def request_fingerprint(action_type: str, payload: dict[str, Any]) -> str:
    """SHA-256 over the canonical JSON of ``(action_type, payload)``.

    Canonical = sorted keys, no insignificant whitespace, so logically equal
    payloads hash equally regardless of key order.
    """
    canonical = json.dumps(
        {"action_type": action_type, "payload": payload},
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


class IdempotencyService:
    def __init__(
        self,
        repository: ExecutionRepository,
        registry: ActionRegistry,
        *,
        default_max_attempts: int = 3,
    ) -> None:
        self._repository = repository
        self._registry = registry
        self._default_max_attempts = default_max_attempts

    async def get_existing_execution(
        self, ctx: ExecutionContext, idempotency_key: str
    ) -> ExecutionRecord | None:
        """Return the execution registered under ``idempotency_key`` in ``ctx``'s workspace.

        Read-only, workspace-scoped, safe to retry. No lease.
        """
        return await self._repository.get_by_idempotency_key(
            workspace_id=ctx.workspace_id, idempotency_key=idempotency_key
        )

    @staticmethod
    def ensure_same_request(record: ExecutionRecord, *, action_type: str, fingerprint: str) -> None:
        """Raise :class:`IdempotencyConflictError` if ``record`` was created for a
        different request than the one now presented under the same key.

        Why: returning a stored result for a materially different request (other
        amount, other customer) would tell the caller "done" for an action that
        was never performed. Pure check; no I/O.
        """
        if record.action_type != action_type or record.request_fingerprint != fingerprint:
            raise IdempotencyConflictError(
                f"idempotency key {record.idempotency_key!r} was already used for a different "
                f"request (existing execution {record.id}, action {record.action_type})"
            )

    async def check_idempotency(
        self, ctx: ExecutionContext, request: ExecutionRequest
    ) -> ExecutionRecord | None:
        """Return the existing execution for this request's key, or ``None`` if new.

        Raises :class:`IdempotencyConflictError` if the key exists with a
        different fingerprint. Read-only; safe to retry; no lease.

        This is a fast path only. It is *not* sufficient on its own because two
        concurrent submissions may both see ``None``; :meth:`register_key`
        resolves that race at the database.
        """
        existing = await self.get_existing_execution(ctx, request.idempotency_key)
        if existing is not None:
            self.ensure_same_request(
                existing,
                action_type=request.action_type,
                fingerprint=request_fingerprint(request.action_type, request.payload),
            )
        return existing

    async def register_key(
        self, ctx: ExecutionContext, request: ExecutionRequest
    ) -> SubmissionResult:
        """Durably register the key → execution mapping (race-safe).

        Uses the unique constraint ``(workspace_id, idempotency_key)`` with
        ``ON CONFLICT DO NOTHING``: of N concurrent registrations, exactly one
        creates the record; the others receive it and are fingerprint-checked.

        Transitions: ``∅ → PENDING`` for the winner only. Safe to retry: a
        retried registration finds its own earlier record (same fingerprint).
        """
        fingerprint = request_fingerprint(request.action_type, request.payload)
        record, created = await self._repository.create_execution(
            ctx,
            action_type=request.action_type,
            idempotency_key=request.idempotency_key,
            request_fingerprint=fingerprint,
            request_payload=request.payload,
            max_attempts=request.max_attempts or self._default_max_attempts,
        )
        if not created:
            self.ensure_same_request(
                record, action_type=request.action_type, fingerprint=fingerprint
            )
        return SubmissionResult(record=record, created=created)

    async def submit(self, ctx: ExecutionContext, request: ExecutionRequest) -> SubmissionResult:
        """Validate a request and return its (new or existing) durable execution.

        Order: validate action type → refuse credential-like payload fields →
        fast-path lookup → race-safe registration. A duplicate submission
        returns the existing record (whatever its status, including the stored
        result if it already succeeded) and never schedules new work.
        """
        self._registry.get(request.action_type)  # raises UnknownActionTypeError
        sensitive = find_sensitive_keys(request.payload)
        if sensitive:
            raise SensitivePayloadError(
                f"payload contains credential-like fields {sensitive}; credentials must be "
                "resolved by the client at call time, never stored in an execution"
            )
        existing = await self.check_idempotency(ctx, request)
        if existing is not None:
            log_event(logger, "idempotent_replay", record=existing)
            return SubmissionResult(record=existing, created=False)
        result = await self.register_key(ctx, request)
        log_event(
            logger,
            "execution_registered" if result.created else "idempotent_replay",
            record=result.record,
        )
        return result
