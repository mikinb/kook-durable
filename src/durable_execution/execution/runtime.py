"""Composition root: wires one worker's components together.

Kept explicit (no DI framework) so every dependency is visible. Each simulated
worker builds its own runtime — its own lease manager identity and, ideally,
its own engine — sharing only the database and the external system.
"""

import random
from dataclasses import dataclass

from durable_execution.database.repository import ExecutionRepository
from durable_execution.database.session import SessionFactory
from durable_execution.execution.executor import ActionExecutor
from durable_execution.execution.idempotency import IdempotencyService
from durable_execution.execution.lease import LeaseManager
from durable_execution.execution.recovery import RecoveryService
from durable_execution.execution.retry import RetryDecisionEngine, RetryPolicy
from durable_execution.execution.worker import Worker
from durable_execution.external.client import (
    DEFAULT_ACTION_REGISTRY,
    ActionRegistry,
    ExternalActionClient,
)
from durable_execution.external.reconciliation import Reconciler


@dataclass(frozen=True)
class WorkerRuntime:
    repository: ExecutionRepository
    idempotency: IdempotencyService
    lease_manager: LeaseManager
    recovery: RecoveryService
    worker: Worker


def build_worker_runtime(
    *,
    worker_id: str,
    session_factory: SessionFactory,
    client: ExternalActionClient,
    retry_policy: RetryPolicy | None = None,
    registry: ActionRegistry = DEFAULT_ACTION_REGISTRY,
    lease_seconds: float = 30.0,
    max_reconcile_attempts: int = 3,
    rng: random.Random | None = None,
) -> WorkerRuntime:
    policy = retry_policy or RetryPolicy()
    repository = ExecutionRepository(session_factory)
    lease_manager = LeaseManager(repository, worker_id=worker_id, lease_seconds=lease_seconds)
    retry_engine = RetryDecisionEngine(policy, rng=rng)
    recovery = RecoveryService(
        repository=repository,
        lease_manager=lease_manager,
        reconciler=Reconciler(client, registry),
        retry_engine=retry_engine,
        registry=registry,
        max_reconcile_attempts=max_reconcile_attempts,
    )
    worker = Worker(
        repository=repository,
        lease_manager=lease_manager,
        executor=ActionExecutor(client),
        recovery=recovery,
        retry_engine=retry_engine,
        registry=registry,
    )
    return WorkerRuntime(
        repository=repository,
        idempotency=IdempotencyService(
            repository, registry, default_max_attempts=policy.max_attempts
        ),
        lease_manager=lease_manager,
        recovery=recovery,
        worker=worker,
    )
