"""Retry policy and the retry decision engine.

The engine is a pure function of its inputs (no I/O, no clock), so every retry
rule is unit-testable in isolation and the worker stays small. It answers one
question: *given what we know about the external side effect, is another
attempt safe and allowed?*

Rules, in evaluation order (first match wins):

1. Execution already terminal → no retry.
2. Caller does not hold the lease → no retry. Only the current owner decides.
3. ``VALIDATION`` / ``AUTHENTICATION`` / ``PERMANENT`` / ``IDEMPOTENCY_CONFLICT`` /
   ``LEASE_LOST`` → no retry. Repeating cannot change the answer.
4. ``OUTCOME_UNKNOWN``:

   * not yet reconciled → no retry, **reconcile first**;
   * reconciliation confirmed it executed → no retry (record existing result);
   * reconciliation inconclusive → retry *only* if the action is idempotent by
     key (the external system will deduplicate) and attempts remain; otherwise
     **manual review** — never guess with a non-idempotent action;
   * reconciliation confirmed not executed → treated like a transient failure.

5. ``TRANSIENT`` (or confirmed-not-executed) → retry with exponential backoff
   while ``attempt < max_attempts``; then stop.
"""

import random
from dataclasses import dataclass
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

from durable_execution.models.enums import ErrorCategory, ExecutionStatus

_NEVER_RETRY: frozenset[ErrorCategory] = frozenset(
    {
        ErrorCategory.VALIDATION,
        ErrorCategory.AUTHENTICATION,
        ErrorCategory.PERMANENT,
        ErrorCategory.IDEMPOTENCY_CONFLICT,
        ErrorCategory.LEASE_LOST,
    }
)


class RetryPolicy(BaseModel):
    """Bounded exponential backoff.

    ``delay(n) = min(max_delay, initial_delay * backoff_multiplier ** (n - 1))``
    for attempt ``n`` (1-based), then reduced by up to ``jitter`` (a fraction in
    ``[0, 1]``) to de-synchronise retries from many workers. Jitter only ever
    *reduces* the delay, so ``max_delay`` remains a hard upper bound.

    Defaults give 1s, 2s, 4s ... as in the walkthrough.
    """

    model_config = ConfigDict(frozen=True)

    max_attempts: int = Field(default=3, ge=1)
    initial_delay: float = Field(default=1.0, ge=0)
    max_delay: float = Field(default=30.0, ge=0)
    backoff_multiplier: float = Field(default=2.0, ge=1)
    jitter: float = Field(default=0.0, ge=0, le=1)

    def compute_delay(self, attempt: int, rng: random.Random | None = None) -> float:
        """Delay to wait after failed attempt number ``attempt`` (1-based)."""
        if attempt < 1:
            raise ValueError("attempt is 1-based")
        base = min(self.max_delay, self.initial_delay * self.backoff_multiplier ** (attempt - 1))
        if self.jitter and base:
            factor = (rng or random.Random()).random()
            base -= base * self.jitter * factor
        return base


class ExternalConfirmation(StrEnum):
    """What, if anything, we have verified about the external side effect."""

    NOT_CHECKED = "NOT_CHECKED"
    CONFIRMED_EXECUTED = "CONFIRMED_EXECUTED"
    CONFIRMED_NOT_EXECUTED = "CONFIRMED_NOT_EXECUTED"
    INCONCLUSIVE = "INCONCLUSIVE"


@dataclass(frozen=True, slots=True)
class RetryDecision:
    retry: bool
    reason: str
    delay_seconds: float = 0.0
    requires_reconciliation: bool = False
    requires_manual_review: bool = False


class RetryDecisionEngine:
    def __init__(self, policy: RetryPolicy, rng: random.Random | None = None) -> None:
        self.policy = policy
        self._rng = rng or random.Random()

    def should_retry(
        self,
        *,
        category: ErrorCategory,
        status: ExecutionStatus,
        attempt: int,
        max_attempts: int,
        action_idempotent: bool,
        external_confirmation: ExternalConfirmation,
        holds_lease: bool,
    ) -> RetryDecision:
        """Decide whether another attempt is safe and permitted.

        Why: retry safety depends on *what we know about the side effect*, not
        just on the exception type. Centralising the rules keeps them auditable
        and keeps the worker free of ad-hoc ``except: retry()`` logic.

        Inputs: ``attempt`` is the number of external calls already started
        for this execution; ``max_attempts`` is the per-execution budget.

        Pure function: no I/O, no state transitions, no lease use (it only
        consumes ``holds_lease``). Safe to call any number of times. The caller
        applies the decision (``schedule_retry`` / ``mark_failed`` /
        ``mark_manual_review``) under its lease.
        """
        if status.is_terminal:
            return RetryDecision(False, f"execution already terminal ({status})")
        if not holds_lease:
            return RetryDecision(False, "lease not held; only the current lease owner may decide")
        if category in _NEVER_RETRY:
            return RetryDecision(False, f"{category} errors are not retryable")

        if category is ErrorCategory.OUTCOME_UNKNOWN:
            if external_confirmation is ExternalConfirmation.NOT_CHECKED:
                return RetryDecision(
                    False,
                    "outcome unknown: reconcile with the external system before any retry",
                    requires_reconciliation=True,
                )
            if external_confirmation is ExternalConfirmation.CONFIRMED_EXECUTED:
                return RetryDecision(False, "external system confirms the action already executed")
            if external_confirmation is ExternalConfirmation.INCONCLUSIVE:
                if not action_idempotent:
                    return RetryDecision(
                        False,
                        "outcome cannot be determined and the action is not idempotent; "
                        "a retry could duplicate the side effect",
                        requires_manual_review=True,
                    )
                if attempt >= max_attempts:
                    return RetryDecision(
                        False,
                        f"outcome undetermined and attempts exhausted ({attempt}/{max_attempts})",
                        requires_manual_review=True,
                    )
                return RetryDecision(
                    True,
                    "outcome inconclusive but action is idempotent by key; "
                    "external system will deduplicate a repeat",
                    delay_seconds=self.policy.compute_delay(attempt, self._rng),
                )
            # CONFIRMED_NOT_EXECUTED falls through: equivalent to a transient failure.

        if attempt >= max_attempts:
            return RetryDecision(False, f"max attempts exhausted ({attempt}/{max_attempts})")
        reason = (
            "reconciliation confirmed the action did not execute"
            if external_confirmation is ExternalConfirmation.CONFIRMED_NOT_EXECUTED
            and category is ErrorCategory.OUTCOME_UNKNOWN
            else "transient failure; the external system did not apply the action"
        )
        return RetryDecision(
            True,
            f"{reason}; attempt {attempt}/{max_attempts}",
            delay_seconds=self.policy.compute_delay(attempt, self._rng),
        )
