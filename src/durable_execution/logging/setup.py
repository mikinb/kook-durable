"""Structured logging with execution context and credential redaction.

Every important worker event is emitted through :func:`log_event`, which attaches
``execution_id, workspace_id, task_id, agent_id, attempt, status, lease_owner``
plus any extra fields (``duration_ms``, ``error_code`` ...). Values are passed
through :func:`redact` so credential-like keys can never reach a log sink.

Output is ``key=value`` by default (easy to read in a terminal) or one JSON
object per line with ``fmt="json"`` (easy to ingest).
"""

import json
import logging
import os
from datetime import UTC, datetime
from typing import Any, Literal

from durable_execution.models.execution import ExecutionRecord
from durable_execution.security.context import current_context, redact

LOGGER_NAME = "durable_execution"
_STRUCTURED_ATTR = "structured_fields"


class StructuredFormatter(logging.Formatter):
    def __init__(self, fmt: Literal["kv", "json"] = "kv") -> None:
        super().__init__()
        self._fmt = fmt

    def format(self, record: logging.LogRecord) -> str:
        fields: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
        }
        fields.update(getattr(record, _STRUCTURED_ATTR, {}))
        if record.exc_info:
            fields["exc_type"] = record.exc_info[0].__name__ if record.exc_info[0] else None
        if self._fmt == "json":
            return json.dumps(fields, default=str, sort_keys=False)
        return " ".join(f"{key}={_kv(value)}" for key, value in fields.items() if value is not None)


def _kv(value: Any) -> str:
    text = str(value)
    return f'"{text}"' if " " in text else text


def configure_logging(
    level: str | int | None = None, fmt: Literal["kv", "json"] = "kv"
) -> logging.Logger:
    """Configure the package logger once. Idempotent; safe to call from tests."""
    logger = logging.getLogger(LOGGER_NAME)
    resolved = level if level is not None else os.environ.get("LOG_LEVEL", "INFO")
    logger.setLevel(resolved)
    if not any(getattr(h, "_durable_handler", False) for h in logger.handlers):
        handler = logging.StreamHandler()
        handler._durable_handler = True  # type: ignore[attr-defined]
        handler.setFormatter(StructuredFormatter(fmt))
        logger.addHandler(handler)
    return logger


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(f"{LOGGER_NAME}.{name}")


def execution_fields(record: ExecutionRecord) -> dict[str, Any]:
    """Context fields for a record. Payloads and lease tokens are never included."""
    return {
        "execution_id": str(record.id),
        "workspace_id": str(record.workspace_id),
        "task_id": str(record.task_id),
        "agent_id": str(record.agent_id),
        "action_type": record.action_type,
        "attempt": record.attempt,
        "status": str(record.status),
        "lease_owner": record.lease_owner,
    }


def log_event(
    logger: logging.Logger,
    event: str,
    *,
    record: ExecutionRecord | None = None,
    level: int = logging.INFO,
    **fields: Any,
) -> None:
    """Emit one structured, redacted event with full execution context."""
    if not logger.isEnabledFor(level):
        return
    structured: dict[str, Any] = {}
    ctx = current_context()
    if ctx is not None:
        structured.update(ctx.as_log_fields())
    if record is not None:
        structured.update(execution_fields(record))
    structured.update(fields)
    logger.log(level, event, extra={_STRUCTURED_ATTR: redact(structured)})
