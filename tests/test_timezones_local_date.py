"""Tests for timezone-parameterized service-date derivation (SFMTA spec §2)."""

from datetime import date, datetime

from src.timezones import local_date_from_naive_utc


def test_local_date_pacific_vs_eastern_split():
    """05:30 UTC is the next day in Eastern but still 'yesterday' in Pacific.

    2026-07-22 05:30 UTC = 01:30 EDT Jul 22 = 22:30 PDT Jul 21.
    """
    ts = datetime(2026, 7, 22, 5, 30, 0)
    assert local_date_from_naive_utc(ts, "America/New_York") == date(2026, 7, 22)
    assert local_date_from_naive_utc(ts, "America/Los_Angeles") == date(2026, 7, 21)


def test_service_date_fallback_respects_tz():
    """_service_date_for_row uses the agency timezone when start_date is absent."""
    from src.wmata_collector import _service_date_for_row

    row = {"trip_start_date": None, "snapshot_ts": datetime(2026, 7, 22, 5, 30, 0)}
    assert _service_date_for_row(row) == date(2026, 7, 22)  # Eastern default
    assert _service_date_for_row(row, tz_name="America/Los_Angeles") == date(2026, 7, 21)


def test_wmata_collector_importable_without_key(monkeypatch):
    """Importing the module must not require WMATA_API_KEY (SFMTA-only hosts)."""
    import importlib
    import sys

    monkeypatch.delenv("WMATA_API_KEY", raising=False)
    sys.modules.pop("src.wmata_collector", None)
    mod = importlib.import_module("src.wmata_collector")
    assert hasattr(mod, "WMATADataCollector")
