"""Session factory.

``expire_on_commit=False`` lets the repository return records after the
transaction commits without triggering lazy reloads (which are not allowed in
async SQLAlchemy). Returned records are snapshots: callers must go back through
the repository to observe or change current state.
"""

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

SessionFactory = async_sessionmaker[AsyncSession]


def create_session_factory(engine: AsyncEngine) -> SessionFactory:
    return async_sessionmaker(engine, expire_on_commit=False)
