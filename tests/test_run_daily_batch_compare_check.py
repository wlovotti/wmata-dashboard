"""Tests for the comparison-output threshold check used by run_daily_batch."""

from pipelines.run_daily_batch import _check_comparison_thresholds


def test_clean_line_returns_none():
    """A clean 100% line with no diverging routes returns None (no WARN)."""
    line = "2026-05-22: 100.0% agreement (497,730/497,730), 5,058 v2-only rows, 0 routes with >1% disagreement"
    assert _check_comparison_thresholds(line) is None


def test_below_995_returns_reason():
    """Agreement below 99.5% returns a reason string."""
    line = "2026-05-22: 99.4% agreement (490,000/493,000), 5,058 v2-only rows, 0 routes with >1% disagreement"
    reason = _check_comparison_thresholds(line)
    assert reason is not None
    assert "99.4" in reason


def test_v2_only_above_2pct_returns_reason():
    """v2-only fraction above 2% of total returns a reason."""
    line = "2026-05-22: 100.0% agreement (490,000/490,000), 12,000 v2-only rows, 0 routes with >1% disagreement"
    reason = _check_comparison_thresholds(line)
    assert reason is not None
    assert "v2-only" in reason


def test_diverging_routes_returns_reason():
    """Any diverging route returns a reason."""
    line = "2026-05-22: 99.9% agreement (497,000/497,500), 5,000 v2-only rows, 1 routes with >1% disagreement"
    reason = _check_comparison_thresholds(line)
    assert reason is not None
    assert "diverging" in reason or "1" in reason


def test_unparseable_line_returns_reason():
    """A line that doesn't match the expected format returns a reason."""
    line = "something went wrong"
    reason = _check_comparison_thresholds(line)
    assert reason is not None
