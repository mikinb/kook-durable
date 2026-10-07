"""A sanitized fake external system with deterministic failure injection.

The fake keeps **its own state**, separate from the execution database, exactly
like a real third-party API would. That separation is the whole point: after a
lost response, the worker's database says "UNKNOWN" while this service already
holds evidence that the action succeeded. Reconciliation bridges the two.

Failure modes (mapped to the walkthrough's scenario names):

=====================  =====================================  ==============
FailureMode            Scenario                               Side effect?
=====================  =====================================  ==============
NORMAL                 normal success                         yes
BEFORE_EXECUTION       NETWORK_FAILURE_BEFORE_EXECUTION       no
AFTER_SIDE_EFFECT      NETWORK_FAILURE_AFTER_EXECUTION        **yes**
TIMEOUT                response too slow, client gives up     **yes**
TRANSIENT              explicit 503 response                  no
PERMANENT              explicit 422 validation rejection      no
AUTHENTICATION         explicit 401 response                  no
=====================  =====================================  ==============

Note that ``BEFORE_EXECUTION``, ``AFTER_SIDE_EFFECT`` and ``TIMEOUT`` look
*identical* to the caller (no response). Only reconciliation can tell them
apart. ``TRANSIENT``/``PERMANENT``/``AUTHENTICATION`` are definite answers.

Failures are injected as a FIFO script (``inject(...)``) so tests and the demo
are fully reproducible; nothing here is random.
"""

import asyncio
import itertools
from collections import deque
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any


class FailureMode(StrEnum):
    NORMAL = "NORMAL"
    BEFORE_EXECUTION = "BEFORE_EXECUTION"
    AFTER_SIDE_EFFECT = "AFTER_SIDE_EFFECT"
    TIMEOUT = "TIMEOUT"
    TRANSIENT = "TRANSIENT"
    PERMANENT = "PERMANENT"
    AUTHENTICATION = "AUTHENTICATION"


class ExternalActionStatus(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


# --- simulated transport-level errors (what an HTTP library would raise) -------


class SimulatedConnectionError(ConnectionError):
    """Connection reset / dropped. Says nothing about whether the action ran."""


class SimulatedTimeoutError(TimeoutError):
    """The client stopped waiting. The server may still have committed the action."""


class SimulatedHTTPError(Exception):
    def __init__(self, status_code: int, code: str, message: str) -> None:
        super().__init__(f"HTTP {status_code} {code}: {message}")
        self.status_code = status_code
        self.code = code
        self.message = message


@dataclass
class ExternalActionRecord:
    external_reference: str
    action_type: str
    idempotency_key: str
    status: ExternalActionStatus
    payload: dict[str, Any]
    created_at: datetime


@dataclass
class FakeExternalService:
    """In-memory stand-in for a third-party API.

    ``idempotent_action_types``: action types for which the service honours the
    ``Idempotency-Key`` (a repeated key returns the original record and creates
    no new side effect). For every other type, each call creates a **new** side
    effect — that is what makes blind retries dangerous.
    """

    idempotent_action_types: frozenset[str] = frozenset({"create_invoice"})
    latency_seconds: float = 0.0

    records: dict[str, ExternalActionRecord] = field(default_factory=dict)
    perform_calls: int = 0
    status_calls: int = 0
    #: Set every time a side effect is committed (lets tests synchronise).
    side_effect_committed: asyncio.Event = field(default_factory=asyncio.Event)
    #: When set by a test, responses are held after the side effect commits
    #: until the event is set — used to create a deterministic "slow worker".
    response_gate: asyncio.Event | None = None

    _failures: deque[FailureMode] = field(default_factory=deque)
    _lookup_failures: int = 0
    _ref_counter: Iterator[int] = field(default_factory=lambda: itertools.count(1))

    # ------------------------------------------------------------- injection

    def inject(self, *modes: FailureMode) -> None:
        """Queue failure modes for the next ``perform_action`` calls (FIFO)."""
        self._failures.extend(modes)

    def inject_lookup_failures(self, count: int) -> None:
        """Make the next ``count`` status lookups fail with a connection error."""
        self._lookup_failures += count

    @property
    def execution_count(self) -> int:
        """Number of real side effects committed (the number that must stay 1)."""
        return len(self.records)

    # ------------------------------------------------------------------ API

    async def perform_action(
        self, *, action_type: str, idempotency_key: str, payload: dict[str, Any], api_key: str
    ) -> ExternalActionRecord:
        """Perform the action, subject to the next injected failure mode."""
        self.perform_calls += 1
        mode = self._failures.popleft() if self._failures else FailureMode.NORMAL
        if self.latency_seconds:
            await asyncio.sleep(self.latency_seconds)
        if not api_key:
            raise SimulatedHTTPError(401, "UNAUTHENTICATED", "missing credentials")

        if mode is FailureMode.BEFORE_EXECUTION:
            raise SimulatedConnectionError("connection reset before request was processed")
        if mode is FailureMode.TRANSIENT:
            raise SimulatedHTTPError(503, "SERVICE_UNAVAILABLE", "temporarily unavailable")
        if mode is FailureMode.PERMANENT:
            raise SimulatedHTTPError(422, "VALIDATION_ERROR", "request rejected by validation")
        if mode is FailureMode.AUTHENTICATION:
            raise SimulatedHTTPError(401, "UNAUTHENTICATED", "credentials rejected")

        record = self._commit(action_type, idempotency_key, payload)

        if self.response_gate is not None:
            await self.response_gate.wait()
        if mode is FailureMode.AFTER_SIDE_EFFECT:
            raise SimulatedConnectionError("connection reset after request was processed")
        if mode is FailureMode.TIMEOUT:
            raise SimulatedTimeoutError("response not received within client timeout")
        return record

    async def get_action_status(
        self, *, action_type: str, idempotency_key: str, api_key: str
    ) -> ExternalActionRecord | None:
        """Look up an action by the client-supplied key. ``None`` if never received."""
        self.status_calls += 1
        if self._lookup_failures > 0:
            self._lookup_failures -= 1
            raise SimulatedConnectionError("status endpoint unreachable")
        if not api_key:
            raise SimulatedHTTPError(401, "UNAUTHENTICATED", "missing credentials")
        matches = [
            r
            for r in self.records.values()
            if r.action_type == action_type and r.idempotency_key == idempotency_key
        ]
        return matches[0] if matches else None

    # ------------------------------------------------------------- internals

    def _commit(
        self, action_type: str, idempotency_key: str, payload: dict[str, Any]
    ) -> ExternalActionRecord:
        # No await between lookup and insert: atomic under asyncio.
        if action_type in self.idempotent_action_types:
            for existing in self.records.values():
                if (
                    existing.action_type == action_type
                    and existing.idempotency_key == idempotency_key
                ):
                    return existing
        reference = f"ext-{next(self._ref_counter):06d}"
        record = ExternalActionRecord(
            external_reference=reference,
            action_type=action_type,
            idempotency_key=idempotency_key,
            status=ExternalActionStatus.SUCCEEDED,
            payload=dict(payload),
            created_at=datetime.now(UTC),
        )
        self.records[reference] = record
        self.side_effect_committed.set()
        return record
