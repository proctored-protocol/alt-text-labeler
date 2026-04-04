from __future__ import annotations

from contextlib import contextmanager
from functools import lru_cache
from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from app.config import get_settings


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""
    pass


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    """
    Build and cache the SQLAlchemy engine for the current process.

    Notes:
    - Uses the DATABASE_URL from app.config.
    - pool_pre_ping helps long-running workers recover from stale DB connections.
    - We keep engine creation centralized so tests and runtime code use the same path.
    """
    settings = get_settings()

    return create_engine(
        settings.database_url,
        future=True,
        pool_pre_ping=True,
    )


SessionLocal = sessionmaker(
    bind=get_engine(),
    autoflush=False,
    autocommit=False,
    expire_on_commit=False,
    class_=Session,
)


@contextmanager
def session_scope() -> Iterator[Session]:
    """
    Provide a transactional session scope.

    Behavior:
    - yields a Session
    - commits on normal exit
    - rolls back on exception
    - always closes the session

    Use this in worker code when the whole block should be one transaction.
    """
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def init_db() -> None:
    """
    Create all ORM-managed tables.

    Intended use:
    - local development
    - tests
    - empty-db bootstrap checks

    Production migrations should eventually be handled through Alembic,
    but keeping this function is useful during the rebuild while we are
    iterating on the canonical schema.
    """
    import app.models  # noqa: F401

    Base.metadata.create_all(bind=get_engine())


def dispose_engine() -> None:
    """
    Dispose the cached engine and clear the engine cache.

    Useful for:
    - tests that need a fresh engine
    - process teardown
    - rare cases where config/connection state must be reset in-process
    """
    engine = get_engine()
    engine.dispose()
    get_engine.cache_clear()