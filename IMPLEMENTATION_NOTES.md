# Implementation Notes

The engineering decisions behind this walkthrough, with the trade-offs each one
accepts.

## 1. PostgreSQL is the only coordinator

Leases, idempotency and state all live in the `executions` row. No Redis and no
in-process locks. This removes a whole class of split-brain bugs: the store
that holds the state is the one that arbitrates access to it, inside the same
transaction.

*Trade-off:* every claim and transition is a DB round trip, and very hot
executions contend on one row. For external-action execution, where each step
is dominated by a network call to a third party, that cost is negligible. A
high-throughput queue would add `FOR UPDATE SKIP LOCKED` batch polling on top
of the same row model.

## 2. Lock → validate → write → commit, every time

Each mutation does `SELECT … FOR UPDATE` → read DB clock → validate transition,
fencing token and expiry in Python → mutate → append events → commit, all in
one short transaction. The alternative, a single conditional `UPDATE … WHERE …
RETURNING`, is equally atomic. The lock-then-validate form was chosen because:

* the transition table is enforced in one place (`validate_transition`), not
  duplicated as SQL predicates;
* failures are precise (`StaleLeaseError` vs `InvalidStateTransitionError`)
  instead of "0 rows updated";
* the previous status is available for the event row without a CTE.

Row locks are held for milliseconds and **never across an external call**.

## 3. Write-ahead intent: commit `RUNNING` before calling out

`mark_running` commits *before* `perform_action`. This one commit is what
makes crash recovery sound:

* crash before it → record is `CLAIMED`, so no call can have started and a
  re-claim is safe;
* crash after it → record is `RUNNING`, so a side effect *may* exist and
  recovery must reconcile.

Without it, recovery could not tell "never called" from "called and died".

## 4. Fencing tokens, and which writes require an unexpired lease

* Starting a side effect (`mark_running`) requires a token match **and** an
  unexpired lease.
* Recording an outcome (`mark_succeeded/failed/unknown`, `schedule_retry`)
  requires a token match only. If the lease expired but nobody took over, the
  outcome is still true and worth keeping. If someone did take over, the token
  changed and the write is rejected.

Expiry is compared with `clock_timestamp()` read *after* the row lock is
acquired, so worker clock skew and lock wait time do not distort it.

## 5. `UNKNOWN` is a first-class state, not an error

An exception during an external call is not evidence of failure. The executor
classifies:

* connection lost / timeout / unexpected client exception → `OUTCOME_UNKNOWN`
* explicit 429/5xx → `TRANSIENT`; explicit 4xx → `VALIDATION`/`AUTHENTICATION`/`PERMANENT`

An unexpected exception counts as *unknown* rather than *failed*, because we
cannot prove the request never left. That is the only broad `except` in the
codebase, and it is logged and translated.

## 6. Reconciliation is observation; the retry engine is policy

The `Reconciler` only reads from the external system. The `RetryDecisionEngine`
is a pure function of `(category, status, attempt, max_attempts, idempotency,
confirmation, lease)`. Keeping them apart means every retry rule is
unit-testable without a database, and the worker/recovery code just applies a
decision.

## 7. Action safety is declared, not guessed

`ActionSpec(idempotent_by_key, authoritative_status_lookup)` makes the external
contract explicit per action type. Unregistered action types are rejected at
submission. The distinction that matters most: a "not found" from an
eventually-consistent lookup proves nothing, so for a non-idempotent action
that ends in `MANUAL_REVIEW_REQUIRED`, never a guess.

## 8. Bounded everything

* `max_attempts` bounds external calls (`attempt` counts calls *started*).
* `max_reconcile_attempts` bounds how long a lookup outage can keep an
  execution in `UNKNOWN` before it escalates (or, for idempotent actions,
  re-sends with the same key).
* Backoff is capped by `max_delay`, and jitter only shortens delays.

## 9. History is append-only

`execution_events` is never updated. The record holds *current* state; the
events hold *how it got there*, including who held the lease, when the
connection dropped, what reconciliation found and who recovered whom. Events
are written in the same transaction as the state change, so the history can
never disagree with the state.

## 10. Tenant isolation lives in the data layer

`workspace_id` sits in every single-execution `WHERE` clause and is part of the
idempotency uniqueness constraint. Foreign records look like "not found". The
one cross-tenant query (the recovery candidate scan) returns identifiers only.

## 11. Secrets

Credentials live in the client as `SecretStr` and are resolved at call time.
Payloads with credential-like keys are refused at submission. Result payloads,
event details and log fields are passed through `redact()`. Lease tokens and
payloads are never logged.

## 12. Testing approach

* Real PostgreSQL, because the guarantees *are* database behaviour.
* One engine per simulated worker, which stands in for separate processes.
* Deterministic failure scripts, `BaseException` crashes, and SQL-forced lease
  expiry, so there are no sleeps or randomness (aside from 100–200 ms latency
  that keeps race windows open).
* A response gate in the fake service creates a deterministic "slow worker" to
  prove fencing end to end.
* Mutation-checked: weakening the claim condition makes the race tests fail.

## Known limitations / what production would add

* **Schema migrations:** `create_all` stands in for Alembic.
* **Scheduling:** `Worker.run_until_settled` and the recovery sweep are invoked
  explicitly. Production would add a poller (`FOR UPDATE SKIP LOCKED` over
  runnable rows) or a queue that delivers execution ids, with the sweep running
  on an interval.
* **Lease heartbeat** is implemented, but tests use leases long enough that it
  never fires. Takeover is exercised by forcing expiry instead.
* **Fake external service** is in-process. Its separation from the database is
  logical, not physical.
* **Manual review** is a terminal state with the transitions an operator would
  use, but no operator tooling or alerting is included.
* **Event table growth**: needs retention/archival in a long-running system.
* **Exactly-once** is not claimed. Effectively-once requires the external
  system to deduplicate by key or to offer an authoritative status lookup. When
  neither holds, the system escalates instead of guessing.
