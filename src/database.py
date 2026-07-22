import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.models import Base

# Load environment variables before reading DATABASE_URL
load_dotenv()

# Database configuration from environment variables
# PostgreSQL is required - no fallback to SQLite
DATABASE_URL = os.getenv("DATABASE_URL")


def get_engine(db_url: str | None = None):
    """Create a SQLAlchemy engine.

    ``db_url=None`` (the default) reads ``DATABASE_URL`` from the
    environment at call time — the historical single-database behavior.
    Sidecar collectors (e.g. SFMTA) pass an explicit URL so one process
    can target a different database than the rest of the codebase.

    Connection pooling is enabled for PostgreSQL for production performance:
    - pool_pre_ping: Verify connections before using (handle stale connections)
    - pool_size: Number of connections to maintain in pool
    - max_overflow: Additional connections allowed when pool is full
    - pool_recycle: Recycle connections after 1 hour
    """
    url = db_url or DATABASE_URL
    engine_kwargs = {"echo": False}

    # Apply pool parameters only for non-SQLite databases (PostgreSQL, etc.)
    if url and not url.startswith("sqlite"):
        engine_kwargs.update(
            {
                "pool_pre_ping": True,
                "pool_size": 10,
                "max_overflow": 20,
                "pool_recycle": 3600,
            }
        )

    engine = create_engine(url, **engine_kwargs)
    return engine


def init_db(engine=None, db_url: str | None = None):
    """Create all tables on the given engine (or one built from db_url/env)."""
    if engine is None:
        engine = get_engine(db_url)
    Base.metadata.create_all(bind=engine)
    print(f"Database initialized at: {engine.url}")


def get_session(db_url: str | None = None) -> Session:
    """Get a new database session, optionally against an explicit URL."""
    engine = get_engine(db_url)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal()


def get_db():
    """Dependency for getting database sessions (useful for FastAPI later)"""
    db = get_session()
    try:
        yield db
    finally:
        db.close()
