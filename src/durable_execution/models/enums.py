"""Execution state machine, event vocabulary and error categories.

State machine
-------------

::

    PENDING ──► CLAIMED ──► RUNNING ──┬──► SUCCEEDED
       ▲          │  ▲               ├──► FAILED
       │          │  │               ├──► RETRY_SCHEDULED ──► CLAIMED
       └──────────┘  └ (reclaim when ├──► UNKNOWN ──► RECONCILING
       (release)        lease stale) └──► RECONCILING (stale RUNNING recovered)

    RECONCILING ──┬──► SUCCEEDED               (external system confirms success)
                  ├──► FAILED                  (confirmed rejected / confirmed not
                  │                             executed with attempts exhausted)
                  ├──► RETRY_SCHEDULED         (only when a repeat is provably safe)
                  ├──► UNKNOWN                 (lookup temporarily unavailable)
                  ├──► RECONCILING             (stale reconciler taken over)
                  └──► MANUAL_REVIEW_REQUIRED  (outcome cannot be safely determined)

    MANUAL_REVIEW_REQUIRED ──► SUCCEEDED | FAILED | CANCELLED   (operator decision)

``SUCCEEDED``, ``FAILED`` and ``CANCELLED`` are terminal. ``MANUAL_REVIEW_REQUIRED``
is terminal for automation: no worker will touch it again.

Key rule: there is no ``RUNNING → PENDING`` or ``UNKNOWN → CLAIMED`` edge. Once an
external call may have started, the only way back to execution is through
``RECONCILING``, which inspects the external system first.
"""

from collections.abc import Mapping
from enum import StrEnum

from durable_execution.errors import InvalidStateTransitionError


class ExecutionStatus(StrEnum):
    PENDING = "PENDING"
    CLAIMED = "CLAIMED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    RETRY_SCHEDULED = "RETRY_SCHEDULED"
    UNKNOWN = "UNKNOWN"
    RECONCILING = "RECONCILING"
    MANUAL_REVIEW_REQUIRED = "MANUAL_REVIEW_REQUIRED"
    CANCELLED = "CANCELLED"

    @property
    def is_terminal(self) -> bool:
        return self in TERMINAL_STATUSES

    @property
    def is_settled(self) -> bool:
        """True when no automated worker will act on the execution again."""
        return self in SETTLED_STATUSES


TERMINAL_STATUSES: frozenset[ExecutionStatus] = frozenset(
    {ExecutionStatus.SUCCEEDED, ExecutionStatus.FAILED, ExecutionStatus.CANCELLED}
)

SETTLED_STATUSES: frozenset[ExecutionStatus] = TERMINAL_STATUSES | {
    ExecutionStatus.MANUAL_REVIEW_REQUIRED
}

#: Statuses in which a worker holds (or held) a lease over the execution.
LEASED_STATUSES: frozenset[ExecutionStatus] = frozenset(
    {ExecutionStatus.CLAIMED, ExecutionStatus.RUNNING, ExecutionStatus.RECONCILING}
)

#: Statuses where a previous attempt may have produced an external side effect,
#: so the next actor must reconcile before doing anything else.
NEEDS_RECONCILIATION_STATUSES: frozenset[ExecutionStatus] = frozenset(
    {ExecutionStatus.UNKNOWN, ExecutionStatus.RUNNING, ExecutionStatus.RECONCILING}
)

_S = ExecutionStatus

ALLOWED_TRANSITIONS: Mapping[ExecutionStatus, frozenset[ExecutionStatus]] = {
    _S.PENDING: frozenset({_S.CLAIMED, _S.CANCELLED}),
    _S.CLAIMED: frozenset({_S.RUNNING, _S.CLAIMED, _S.PENDING, _S.CANCELLED}),
    _S.RUNNING: frozenset(
        {_S.SUCCEEDED, _S.FAILED, _S.UNKNOWN, _S.RETRY_SCHEDULED, _S.RECONCILING}
    ),
    _S.RETRY_SCHEDULED: frozenset({_S.CLAIMED, _S.CANCELLED}),
    _S.UNKNOWN: frozenset({_S.RECONCILING}),
    _S.RECONCILING: frozenset(
        {
            _S.SUCCEEDED,
            _S.FAILED,
            _S.RETRY_SCHEDULED,
            _S.UNKNOWN,
            _S.RECONCILING,
            _S.MANUAL_REVIEW_REQUIRED,
        }
    ),
    _S.MANUAL_REVIEW_REQUIRED: frozenset({_S.SUCCEEDED, _S.FAILED, _S.CANCELLED}),
    _S.SUCCEEDED: frozenset(),
    _S.FAILED: frozenset(),
    _S.CANCELLED: frozenset(),
}


def can_transition(current: ExecutionStatus, target: ExecutionStatus) -> bool:
    """Return whether ``current → target`` is an edge of the state machine."""
    return target in ALLOWED_TRANSITIONS[current]


def validate_transition(current: ExecutionStatus, target: ExecutionStatus) -> None:
    """Raise :class:`InvalidStateTransitionError` unless ``current → target`` is allowed.

    Called by the repository while it holds the row lock, so the check and the
    write happen in the same transaction and cannot be interleaved by another
    worker.
    """
    if not can_transition(current, target):
        raise InvalidStateTransitionError(f"transition {current} -> {target} is not allowed")


class EventType(StrEnum):
    """Append-only audit events written alongside every state change."""

    CREATED = "CREATED"
    CLAIMED = "CLAIMED"
    LEASE_ACQUIRED = "LEASE_ACQUIRED"
    LEASE_RELEASED = "LEASE_RELEASED"
    STARTED = "STARTED"
    EXTERNAL_CALL_STARTED = "EXTERNAL_CALL_STARTED"
    EXTERNAL_CALL_SUCCEEDED = "EXTERNAL_CALL_SUCCEEDED"
    EXTERNAL_CALL_FAILED = "EXTERNAL_CALL_FAILED"
    CONNECTION_LOST = "CONNECTION_LOST"
    UNKNOWN = "UNKNOWN"
    RECOVERED = "RECOVERED"
    RECONCILIATION_STARTED = "RECONCILIATION_STARTED"
    RECONCILIATION_SUCCEEDED = "RECONCILIATION_SUCCEEDED"
    RECONCILIATION_NOT_EXECUTED = "RECONCILIATION_NOT_EXECUTED"
    RECONCILIATION_INCONCLUSIVE = "RECONCILIATION_INCONCLUSIVE"
    RETRY_SCHEDULED = "RETRY_SCHEDULED"
    MANUAL_REVIEW_REQUIRED = "MANUAL_REVIEW_REQUIRED"
    FINAL_SUCCESS = "FINAL_SUCCESS"
    FINAL_FAILURE = "FINAL_FAILURE"


class ErrorCategory(StrEnum):
    """Classification used by the retry decision engine.

    The category describes *what we know about the external side effect*, which
    is what actually determines whether a retry is safe.
    """

    #: The external system explicitly answered "temporarily unavailable"; the
    #: action was not applied and may be repeated.
    TRANSIENT = "TRANSIENT"
    #: The external system explicitly rejected the action permanently.
    PERMANENT = "PERMANENT"
    #: The request itself is invalid; repeating it cannot succeed.
    VALIDATION = "VALIDATION"
    #: Credentials were rejected; repeating will not help until fixed.
    AUTHENTICATION = "AUTHENTICATION"
    #: Same idempotency key, different request: a caller bug, never retried.
    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"
    #: No response received (connection lost, timeout). The side effect may or
    #: may not have happened. Requires reconciliation before any retry.
    OUTCOME_UNKNOWN = "OUTCOME_UNKNOWN"
    #: The worker lost its lease; only the current owner may decide.
    LEASE_LOST = "LEASE_LOST"
