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


def get_engine():
    """
    Create and return a SQLAlchemy engine for PostgreSQL

    Connection pooling is enabled for production performance:
    - pool_pre_ping: Verify connections before using (handle stale connections)
    - pool_size: Number of connections to maintain in pool
    - max_overflow: Additional connections allowed when pool is full
    - pool_recycle: Recycle connections after 1 hour
    """
    engine = create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20,
        pool_recycle=3600,
        echo=False,  # Set to True for SQL debugging
    )
    return engine


def init_db(engine=None):
    """Initialize the database by creating all tables"""
    if engine is None:
        engine = get_engine()

    # Create all tables
    Base.metadata.create_all(bind=engine)
    print(f"Database initialized at: {DATABASE_URL}")


def get_session() -> Session:
    """Get a new database session"""
    engine = get_engine()
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal()


def get_db():
    """Dependency for getting database sessions (useful for FastAPI later)"""
    db = get_session()
    try:
        yield db
    finally:
        db.close()
