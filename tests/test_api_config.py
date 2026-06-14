"""Tests for api.config environment-driven settings."""

import importlib

import api.config


def _reload(monkeypatch, **env):
    """Reload api.config with a patched environment and return the module."""
    for key in ("CORS_ALLOW_ORIGINS", "DATABASE_URL"):
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    return importlib.reload(api.config)


def test_cors_defaults_to_wildcard(monkeypatch):
    """With no CORS_ALLOW_ORIGINS set, dev keeps the permissive wildcard."""
    cfg = _reload(monkeypatch)
    assert cfg.settings.cors_allow_origins == ["*"]


def test_cors_parses_comma_separated_origins(monkeypatch):
    """A comma-separated CORS_ALLOW_ORIGINS becomes a trimmed list."""
    cfg = _reload(monkeypatch, CORS_ALLOW_ORIGINS="https://a.example, https://b.example")
    assert cfg.settings.cors_allow_origins == ["https://a.example", "https://b.example"]
