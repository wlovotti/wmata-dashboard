"""Tests for src/deadman.py — the dead-man's-switch ping helper."""

import requests

from src.deadman import ping_healthcheck


def test_none_url_is_noop_and_false(monkeypatch):
    """A missing/empty URL disables pinging entirely — no HTTP call is made."""
    calls = []
    monkeypatch.setattr(requests, "post", lambda *a, **k: calls.append(a))
    assert ping_healthcheck(None) is False
    assert ping_healthcheck("") is False
    assert calls == []


def test_successful_ping_returns_true(monkeypatch):
    """Any completed HTTP exchange counts as a delivered ping."""
    seen = {}

    def fake_post(url, timeout):
        seen["url"], seen["timeout"] = url, timeout

    monkeypatch.setattr(requests, "post", fake_post)
    assert ping_healthcheck("https://hc-ping.example/uuid") is True
    assert seen == {"url": "https://hc-ping.example/uuid", "timeout": 5.0}


def test_network_failure_swallowed_returns_false(monkeypatch):
    """Network failures must never propagate into the collector tick."""

    def fake_post(url, timeout):
        raise requests.ConnectionError("refused")

    monkeypatch.setattr(requests, "post", fake_post)
    assert ping_healthcheck("https://hc-ping.example/uuid") is False
