"""Durable execution reference implementation.

A sanitized walkthrough of how an agent/tool runtime can execute external
actions reliably: durable execution records, database-backed worker leases,
idempotency keys, bounded retries, reconciliation of uncertain outcomes and
crash recovery. PostgreSQL is the source of truth; worker memory never is.
"""

from durable_execution.errors import (
    DurableExecutionError,
    ExecutionContextMismatchError,
    ExecutionNotFoundError,
    IdempotencyConflictError,
    InvalidStateTransitionError,
    SensitivePayloadError,
    StaleLeaseError,
    UnknownActionTypeError,
)

__all__ = [
    "DurableExecutionError",
    "ExecutionContextMismatchError",
    "ExecutionNotFoundError",
    "IdempotencyConflictError",
    "InvalidStateTransitionError",
    "SensitivePayloadError",
    "StaleLeaseError",
    "UnknownActionTypeError",
]
