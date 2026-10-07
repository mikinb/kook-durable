"""Result type returned by worker and recovery runs."""

import uuid
from dataclasses import dataclass
from enum import StrEnum

from durable_execution.models.enums import ExecutionStatus
from durable_execution.models.execution import ExecutionRecord


class RunOutcome(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    RETRY_SCHEDULED = "RETRY_SCHEDULED"
    UNKNOWN = "UNKNOWN"
    MANUAL_REVIEW_REQUIRED = "MANUAL_REVIEW_REQUIRED"
    #: Execution was already settled; its stored result is returned unchanged.
    ALREADY_SETTLED = "ALREADY_SETTLED"
    #: Another worker holds a live lease; this worker did nothing.
    SKIPPED_LEASE_HELD = "SKIPPED_LEASE_HELD"
    #: Scheduled retry is not due yet.
    NOT_DUE = "NOT_DUE"
    #: This worker's lease was taken over; its write was rejected (fenced).
    LEASE_LOST = "LEASE_LOST"


@dataclass(frozen=True, slots=True)
class RunResult:
    outcome: RunOutcome
    execution_id: uuid.UUID
    status: ExecutionStatus
    worker_id: str
    detail: str = ""
    record: ExecutionRecord | None = None

    @classmethod
    def from_record(
        cls, outcome: RunOutcome, record: ExecutionRecord, worker_id: str, detail: str = ""
    ) -> "RunResult":
        return cls(outcome, record.id, record.status, worker_id, detail, record)
