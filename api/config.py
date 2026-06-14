"""Application configuration sourced from environment variables.

Centralizes the env reads that differ between dev and a future public
deployment (spec 2026-06-13-dev-deploy-environments-design §4.5). Dev keeps
permissive defaults; a deployed instance overrides via the environment with
no code change.
"""

import os


class Settings:
    """Typed view over the process environment.

    Attributes:
        database_url: The SQLAlchemy URL (already consumed by src.database;
            mirrored here so config lives in one place).
        cors_allow_origins: Allowed CORS origins. ``CORS_ALLOW_ORIGINS`` is a
            comma-separated list; unset means the dev wildcard ``["*"]``.
    """

    def __init__(self) -> None:
        """Read settings from the current environment."""
        self.database_url = os.environ.get("DATABASE_URL", "")
        raw_origins = os.environ.get("CORS_ALLOW_ORIGINS", "*")
        self.cors_allow_origins = [o.strip() for o in raw_origins.split(",") if o.strip()]


settings = Settings()
"""Module-level singleton; import as ``from api.config import settings``."""
