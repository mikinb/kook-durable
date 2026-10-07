"""Domain errors.

Each error maps to a distinct operational meaning. Callers branch on the type,
never on message text, so the retry engine and worker can make explicit
decisions instead of catching everything and hoping for the best.
"""


class DurableExecutionError(Exception):
    """Base class for all errors raised by this package."""


class ExecutionNotFoundError(DurableExecutionError):
    """The execution does not exist *within the caller's workspace*.

    Deliberately indistinguishable from "exists in another workspace" so that
    tenant boundaries do not leak the existence of foreign records.
    """


class ExecutionContextMismatchError(DurableExecutionError):
    """The execution belongs to the workspace but to a different task/agent."""


class InvalidStateTransitionError(DurableExecutionError):
    """A requested status change is not permitted by the state machine."""


class StaleLeaseError(DurableExecutionError):
    """The caller's lease token is no longer the current one (fencing failure).

    Raised when a worker whose lease expired, or was taken over, tries to write
    to the execution. The write is rejected so the current owner's view wins.
    Never retry the same write: the worker has lost the right to decide.
    """


class IdempotencyConflictError(DurableExecutionError):
    """An idempotency key was reused for a materially different request."""


class SensitivePayloadError(DurableExecutionError, ValueError):
    """A request payload contains credential-like fields and was refused.

    Credentials must be resolved by the client at call time and never persisted
    in durable execution records or logs.
    """


class UnknownActionTypeError(DurableExecutionError, ValueError):
    """The action type is not registered, so its safety properties are unknown."""
