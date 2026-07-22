"""The session factory must accept an explicit URL for sidecar databases."""

from src.database import get_engine


def test_get_engine_honors_explicit_url():
    """An explicit db_url wins over the DATABASE_URL environment default."""
    engine = get_engine(db_url="sqlite:///:memory:")
    assert str(engine.url) == "sqlite:///:memory:"


def test_get_engine_default_unchanged():
    """No argument -> engine built from DATABASE_URL env (conftest sets sqlite)."""
    engine = get_engine()
    assert engine.url is not None
