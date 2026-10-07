"""Engine construction and schema initialisation."""

import os

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from durable_execution.models.execution import Base

DEFAULT_DATABASE_URL = (
    "postgresql+asyncpg://durable:durable_local_only@localhost:54329/durable_demo"
)


def database_url_from_env(var: str = "DATABASE_URL", default: str = DEFAULT_DATABASE_URL) -> str:
    return os.environ.get(var, default)


def create_engine(url: str | None = None, *, pool_size: int = 10) -> AsyncEngine:
    """Create an async engine.

    Each worker process should own its own engine (and therefore its own
    connection pool). Tests create one engine per simulated worker to mirror a
    multi-process deployment where workers share nothing but the database.
    """
    return create_async_engine(
        url or database_url_from_env(),
        pool_size=pool_size,
        max_overflow=pool_size,
        pool_pre_ping=True,
    )


async def init_schema(engine: AsyncEngine) -> None:
    """Create tables if they do not exist.

    A production deployment would use migrations (e.g. Alembic); ``create_all``
    keeps this walkthrough self-contained.
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
