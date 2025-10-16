import os

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.models import Base

# Database configuration from environment variables
# Default to SQLite if no PostgreSQL connection string provided
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./wmata_dashboard.db")


def get_engine():
    """Create and return a SQLAlchemy engine"""
    if DATABASE_URL.startswith("sqlite"):
        # SQLite-specific settings
        engine = create_engine(
            DATABASE_URL,
            connect_args={"check_same_thread": False},
            echo=False,  # Set to True for SQL debugging
        )
    else:
        # PostgreSQL settings
        engine = create_engine(
            DATABASE_URL,
            pool_pre_ping=True,  # Verify connections before using
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
