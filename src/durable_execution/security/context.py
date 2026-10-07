"""Tenant/workspace execution context and credential redaction.

Every execution carries ``workspace_id``, ``task_id`` and ``agent_id``. The
context is passed explicitly to repository calls (tenant scoping is enforced in
SQL, not only at an API layer) and is also bound to a ``ContextVar`` so that
structured logs emitted deep in the call stack carry it automatically.
"""

import re
import uuid
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

REDACTED = "***REDACTED***"

# Field names that must never be persisted or logged. Matching is by key name
# because values (tokens, keys) have no reliable shape.
_SENSITIVE_KEY = re.compile(
    r"(passw(or)?d|secret|token|api[_-]?key|authorization|credential|private[_-]?key|cookie)",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class ExecutionContext:
    """Identifies who an execution belongs to. Immutable by design.

    ``workspace_id`` is the tenant boundary. ``task_id`` and ``agent_id``
    identify the logical task and the agent acting on it; they are recorded on
    the execution so that audits can answer "which agent did this".
    """

    workspace_id: uuid.UUID
    task_id: uuid.UUID
    agent_id: uuid.UUID

    def as_log_fields(self) -> dict[str, str]:
        return {
            "workspace_id": str(self.workspace_id),
            "task_id": str(self.task_id),
            "agent_id": str(self.agent_id),
        }


_current_context: ContextVar[ExecutionContext | None] = ContextVar(
    "durable_execution_context", default=None
)


@contextmanager
def bind_context(ctx: ExecutionContext) -> Iterator[ExecutionContext]:
    """Bind ``ctx`` for the current asyncio task (used by structured logging)."""
    token = _current_context.set(ctx)
    try:
        yield ctx
    finally:
        _current_context.reset(token)


def current_context() -> ExecutionContext | None:
    return _current_context.get()


def is_sensitive_key(key: str) -> bool:
    return bool(_SENSITIVE_KEY.search(key))


def find_sensitive_keys(value: Any, path: str = "") -> list[str]:
    """Return dotted paths of credential-like keys anywhere inside ``value``."""
    found: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            key_path = f"{path}.{key}" if path else str(key)
            if is_sensitive_key(str(key)):
                found.append(key_path)
            found.extend(find_sensitive_keys(item, key_path))
    elif isinstance(value, list | tuple):
        for index, item in enumerate(value):
            found.extend(find_sensitive_keys(item, f"{path}[{index}]"))
    return found


def redact(value: Any) -> Any:
    """Return a deep copy of ``value`` with credential-like keys masked."""
    if isinstance(value, Mapping):
        return {
            key: REDACTED if is_sensitive_key(str(key)) else redact(item)
            for key, item in value.items()
        }
    if isinstance(value, list | tuple):
        return [redact(item) for item in value]
    return value
