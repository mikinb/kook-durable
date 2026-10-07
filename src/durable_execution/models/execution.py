"""SQLAlchemy models: the durable execution record and its append-only event log.

The execution record is the single source of truth for "what is the state of
this external action". Workers are disposable; anything they know that is not
written here is assumed lost when they crash.
"""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    Uuid,
)
from sqlalchemy import Enum as SAEnum
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from durable_execution.models.enums import EventType, ExecutionStatus


class Base(DeclarativeBase):
    pass


def _status_column() -> Any:
    # Stored as VARCHAR + CHECK rather than a native PG enum so adding a status
    # is a plain code change rather than an ALTER TYPE migration.
    return SAEnum(
        ExecutionStatus,
        name="execution_status",
        native_enum=False,
        length=32,
        validate_strings=True,
    )


class ExecutionRecord(Base):
    """One logical external action, identified by (workspace_id, idempotency_key).

    Invariants:

    * ``(workspace_id, idempotency_key)`` is unique: the same key can never
      produce two logical executions inside one workspace.
    * ``request_fingerprint`` is the SHA-256 of the canonical request; reusing a
      key with a different fingerprint is rejected.
    * ``lease_token`` is a fencing token. Every write by a worker must present
      the current token; a new owner always gets a new token.
    * ``request_payload`` and ``result_payload`` never contain credentials.
    """

    __tablename__ = "executions"
    __table_args__ = (
        UniqueConstraint(
            "workspace_id", "idempotency_key", name="uq_executions_workspace_idempotency_key"
        ),
        Index("ix_executions_status_lease", "status", "lease_expires_at"),
        Index("ix_executions_workspace_task", "workspace_id", "task_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)

    workspace_id: Mapped[uuid.UUID] = mapped_column(Uuid, nullable=False)
    task_id: Mapped[uuid.UUID] = mapped_column(Uuid, nullable=False)
    agent_id: Mapped[uuid.UUID] = mapped_column(Uuid, nullable=False)

    action_type: Mapped[str] = mapped_column(String(64), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    request_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    request_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)

    status: Mapped[ExecutionStatus] = mapped_column(_status_column(), nullable=False)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False)
    reconcile_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    lease_owner: Mapped[str | None] = mapped_column(String(128))
    lease_token: Mapped[uuid.UUID | None] = mapped_column(Uuid)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    result_payload: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    error_code: Mapped[str | None] = mapped_column(String(64))
    error_message: Mapped[str | None] = mapped_column(Text)

    external_reference: Mapped[str | None] = mapped_column(String(128))
    last_reconciled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    def __repr__(self) -> str:  # never include payloads in reprs
        return (
            f"ExecutionRecord(id={self.id}, status={self.status}, attempt={self.attempt}, "
            f"lease_owner={self.lease_owner})"
        )


class ExecutionEvent(Base):
    """Append-only history. Rows are never updated or deleted by the runtime.

    The record holds the *current* state; the events explain *how* it got
    there (who claimed it, when the connection was lost, what reconciliation
    found). This is what makes an incident auditable after the fact.
    """

    __tablename__ = "execution_events"
    __table_args__ = (Index("ix_execution_events_execution", "execution_id", "id"),)

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    execution_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("executions.id", ondelete="CASCADE"), nullable=False
    )
    workspace_id: Mapped[uuid.UUID] = mapped_column(Uuid, nullable=False)
    event_type: Mapped[EventType] = mapped_column(
        SAEnum(EventType, name="execution_event_type", native_enum=False, length=48),
        nullable=False,
    )
    from_status: Mapped[ExecutionStatus | None] = mapped_column(_status_column())
    to_status: Mapped[ExecutionStatus | None] = mapped_column(_status_column())
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    worker_id: Mapped[str | None] = mapped_column(String(128))
    details: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
