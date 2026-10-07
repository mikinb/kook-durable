"""Shared fixtures.

Tests run against a real PostgreSQL (docker compose) because the guarantees
under test — row locks, unique constraints, conditional updates, DB time — are
database behaviours that an in-memory fake would not faithfully reproduce.

Each simulated worker gets its *own* engine/connection pool, mirroring separate
processes that share nothing but the database and the external system.
"""

import asyncio
import os
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import pytest
from pydantic import SecretStr
from sqlalchemy import make_url, text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from durable_execution.database import (
    ExecutionRepository,
    SessionFactory,
    create_engine,
    create_session_factory,
)
from durable_execution.execution import (
    ExecutionRequest,
    RetryPolicy,
    WorkerRuntime,
    build_worker_runtime,
)
from durable_execution.external import (
    ExternalActionClient,
    ExternalActionResult,
    FakeExternalService,
    FakeServiceClient,
)
from durable_execution.models import Base, ExecutionRecord, ExecutionStatus
from durable_execution.security import ExecutionContext

TEST_DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql+asyncpg://durable:durable_local_only@localhost:54329/durable_test",
)
DEMO_API_KEY = SecretStr("demo-placeholder-not-a-real-key")


async def _ensure_test_database() -> None:
    """Create the test database if missing.

    docker/postgres/init normally creates it, but init scripts only run on a
    fresh volume (and some Docker VMs cannot mount every host path), so the
    suite does not depend on them.
    """
    url = make_url(TEST_DATABASE_URL)
    admin = create_async_engine(url.set(database="postgres"), isolation_level="AUTOCOMMIT")
    try:
        async with admin.connect() as conn:
            exists = await conn.scalar(
                text("SELECT 1 FROM pg_database WHERE datname = :name"), {"name": url.database}
            )
            if not exists:
                await conn.execute(text(f'CREATE DATABASE "{url.database}"'))
    finally:
        await admin.dispose()


def pytest_sessionstart(session: pytest.Session) -> None:
    asyncio.run(_ensure_test_database())


class SimulatedWorkerCrash(BaseException):
    """Models abrupt process death (SIGKILL, OOM, node loss).

    A ``BaseException`` so that no ``except Exception`` in the worker can
    intercept it: nothing after the crash point gets recorded, exactly like a
    real crash.
    """


class CrashAfterCallClient:
    """Wraps a client; the worker 'dies' right after the external call returns."""

    def __init__(self, inner: ExternalActionClient) -> None:
        self._inner = inner

    async def perform_action(
        self, *, action_type: str, idempotency_key: str, payload: dict[str, Any]
    ) -> ExternalActionResult:
        await self._inner.perform_action(
            action_type=action_type, idempotency_key=idempotency_key, payload=payload
        )
        raise SimulatedWorkerCrash("worker process died after the external call")

    async def get_action_status(
        self, *, action_type: str, idempotency_key: str
    ) -> ExternalActionResult | None:
        return await self._inner.get_action_status(
            action_type=action_type, idempotency_key=idempotency_key
        )


class CrashBeforeCallClient(CrashAfterCallClient):
    """The worker 'dies' after committing RUNNING but before reaching the network."""

    async def perform_action(
        self, *, action_type: str, idempotency_key: str, payload: dict[str, Any]
    ) -> ExternalActionResult:
        raise SimulatedWorkerCrash("worker process died before the external call")


@pytest.fixture
async def _schema() -> None:
    engine = create_engine(TEST_DATABASE_URL, pool_size=2)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.execute(text("TRUNCATE execution_events, executions RESTART IDENTITY CASCADE"))
    await engine.dispose()


@pytest.fixture
async def engine_factory(_schema: None) -> AsyncIterator[Callable[[], AsyncEngine]]:
    """Create independent engines (one per simulated worker process)."""
    engines: list[AsyncEngine] = []

    def make() -> AsyncEngine:
        engine = create_engine(TEST_DATABASE_URL, pool_size=5)
        engines.append(engine)
        return engine

    yield make
    for engine in engines:
        await engine.dispose()


@pytest.fixture
def session_factory(engine_factory: Callable[[], AsyncEngine]) -> SessionFactory:
    return create_session_factory(engine_factory())


@pytest.fixture
def repository(session_factory: SessionFactory) -> ExecutionRepository:
    return ExecutionRepository(session_factory)


@pytest.fixture
def service() -> FakeExternalService:
    return FakeExternalService()


@pytest.fixture
def client(service: FakeExternalService) -> FakeServiceClient:
    return FakeServiceClient(service, api_key=DEMO_API_KEY)


@pytest.fixture
def fast_policy() -> RetryPolicy:
    """Zero delays so retry tests run instantly; delays are unit-tested separately."""
    return RetryPolicy(max_attempts=3, initial_delay=0.0, max_delay=0.0, jitter=0.0)


RuntimeFactory = Callable[..., WorkerRuntime]


@pytest.fixture
def make_runtime(
    engine_factory: Callable[[], AsyncEngine],
    client: FakeServiceClient,
    fast_policy: RetryPolicy,
) -> RuntimeFactory:
    """Build an isolated worker runtime (own engine, own worker id)."""

    def make(worker_id: str, **overrides: Any) -> WorkerRuntime:
        params: dict[str, Any] = {
            "worker_id": worker_id,
            "session_factory": create_session_factory(engine_factory()),
            "client": client,
            "retry_policy": fast_policy,
        }
        params.update(overrides)
        return build_worker_runtime(**params)

    return make


@pytest.fixture
def ctx() -> ExecutionContext:
    return ExecutionContext(workspace_id=uuid.uuid4(), task_id=uuid.uuid4(), agent_id=uuid.uuid4())


def invoice_request(
    key: str = "invoice-key-0001", amount: int = 1250, customer: str = "customer-a", **extra: Any
) -> ExecutionRequest:
    return ExecutionRequest(
        action_type="create_invoice",
        idempotency_key=key,
        payload={"customer_ref": customer, "amount_cents": amount, "currency": "USD"},
        **extra,
    )


def order_request(
    key: str = "order-key-0001", action_type: str = "send_physical_order"
) -> ExecutionRequest:
    return ExecutionRequest(
        action_type=action_type,
        idempotency_key=key,
        payload={"sku": "SKU-DEMO-1", "quantity": 1, "ship_to_ref": "address-ref-1"},
    )


Submit = Callable[[WorkerRuntime, ExecutionContext, ExecutionRequest], Awaitable[ExecutionRecord]]


async def submit(
    runtime: WorkerRuntime, ctx: ExecutionContext, request: ExecutionRequest
) -> ExecutionRecord:
    result = await runtime.idempotency.submit(ctx, request)
    return result.record


async def expire_lease(session_factory: SessionFactory, execution_id: uuid.UUID) -> None:
    """Deterministically simulate the passage of time past the lease expiry."""
    async with session_factory.begin() as session:
        await session.execute(
            text(
                "UPDATE executions SET lease_expires_at = clock_timestamp() - interval '1 second' "
                "WHERE id = :id"
            ),
            {"id": execution_id},
        )


async def status_of(repository: ExecutionRepository, record: ExecutionRecord) -> ExecutionStatus:
    current = await repository.get(workspace_id=record.workspace_id, execution_id=record.id)
    return current.status
