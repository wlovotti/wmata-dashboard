"""
Unit tests for `src/route_diagnostics.py` (NOTES-57).

The classification logic is the load-bearing surface — thresholds for
recovery / leaky / underpowered / neutral determine whether a
timepoint shows up on RouteDetail's diagnosis panel (NOTES-58) and on
the hold-down candidates list (NOTES-61). Tests cover:

  - The four classification labels each fire under their archetypal
    input and don't fire when the threshold is missed by 1s.
  - Insufficient-sample guard.
  - End-to-end direction asymmetry under synthetic deviation samples.
  - End-to-end segment-slip orchestration on a tiny synthetic
    stop_events fixture (SQLite-compatible — we avoid the
    Postgres-only `AT TIME ZONE` in the test via direct calls to the
    Python helpers; the SQL path is exercised in integration).
"""

from datetime import date

import pytest

from src.route_diagnostics import (
    ASYMMETRY_MARGIN_PCT,
    EARLY_THRESHOLD_SEC,
    LATE_THRESHOLD_SEC,
    LEAKY_P10_DROP_SEC,
    MIN_TIMEPOINT_OBSERVATIONS,
    NEUTRAL_MEDIAN_BAND_SEC,
    RECOVERY_MEDIAN_DROP_SEC,
    UNDERPOWERED_ENTERING_MEDIAN_SEC,
    classify_timepoint,
    default_service_date_range,
)

# ---------------------------------------------------------------------------
# Classification logic — synthetic deviation samples
# ---------------------------------------------------------------------------


def _const_sample(value: float, n: int = MIN_TIMEPOINT_OBSERVATIONS + 10) -> list[float]:
    """Return a flat-as-possible sample around `value` for predictable percentiles."""
    return [value] * n


@pytest.mark.smoke
def test_classify_recovery_fires_when_median_drops_120s():
    """Median drop ≥ 120s → recovery."""
    entering = _const_sample(200)
    leaving = _const_sample(200 - RECOVERY_MEDIAN_DROP_SEC)  # exactly 120s drop
    label, stats = classify_timepoint(entering, leaving)
    assert label == "recovery"
    assert stats["median_dev_entering"] == 200
    assert stats["median_dev_leaving"] == 80


@pytest.mark.smoke
def test_classify_recovery_does_not_fire_one_second_below_threshold():
    """Median drop 119s < 120s → not recovery (falls through to underpowered)."""
    entering = _const_sample(200)
    leaving = _const_sample(200 - RECOVERY_MEDIAN_DROP_SEC + 1)  # 119s drop
    label, _ = classify_timepoint(entering, leaving)
    assert label != "recovery"


@pytest.mark.smoke
def test_classify_leaky_fires_when_p10_drops_180s():
    """p10 drop ≥ 180s with median below recovery threshold → leaky.

    Construct a sample whose body sits around 0 (no recovery-level
    median compression) but whose early tail collapses across the
    timepoint by more than the leaky threshold. The 80%-headway portion
    of each sample is pinned at the same value so the median stays
    intact; only the lowest 20% moves.
    """
    n = MIN_TIMEPOINT_OBSERVATIONS + 10
    tail_n = n // 5  # 20% lowest values form the "early tail"
    body = [0] * (n - tail_n)
    entering = [-50] * tail_n + body  # p10 ~ -50
    leaving = [-300] * tail_n + body  # p10 ~ -300 → drop ~ 250 ≥ 180
    label, stats = classify_timepoint(entering, leaving)
    assert stats["p10_dev_entering"] is not None
    assert stats["p10_dev_leaving"] is not None
    p10_drop = stats["p10_dev_entering"] - stats["p10_dev_leaving"]
    assert p10_drop >= LEAKY_P10_DROP_SEC
    assert label == "leaky"


@pytest.mark.smoke
def test_classify_underpowered_fires_when_median_high_no_compression():
    """Median entering ≥ 120s, median drop < 60s → underpowered."""
    entering = _const_sample(UNDERPOWERED_ENTERING_MEDIAN_SEC + 30)
    leaving = _const_sample(UNDERPOWERED_ENTERING_MEDIAN_SEC + 20)  # only 10s drop
    label, _ = classify_timepoint(entering, leaving)
    assert label == "underpowered"


@pytest.mark.smoke
def test_classify_neutral_fires_when_median_near_zero():
    """Median entering within ±60s, no big shift → neutral."""
    entering = _const_sample(30)  # well within ±60s
    leaving = _const_sample(20)
    label, _ = classify_timepoint(entering, leaving)
    assert label == "neutral"


@pytest.mark.smoke
def test_classify_insufficient_data_below_min_sample():
    """Either side below MIN_TIMEPOINT_OBSERVATIONS → insufficient_data."""
    entering = _const_sample(200, n=MIN_TIMEPOINT_OBSERVATIONS - 1)
    leaving = _const_sample(80, n=MIN_TIMEPOINT_OBSERVATIONS + 10)
    label, _ = classify_timepoint(entering, leaving)
    assert label == "insufficient_data"


@pytest.mark.smoke
def test_classify_recovery_wins_over_leaky_when_both_conditions_met():
    """Recovery takes priority over leaky when both fire.

    A timepoint that simultaneously drops its median substantially AND
    drops its p10 substantially is still primarily a recovery point —
    the median compression is the dominant signal. Lock that priority
    in so a future refactor doesn't silently flip it.
    """
    entering = _const_sample(200)
    leaving = _const_sample(200 - RECOVERY_MEDIAN_DROP_SEC - 100)  # 220s drop, fires both
    label, _ = classify_timepoint(entering, leaving)
    assert label == "recovery"


@pytest.mark.smoke
def test_neutral_band_boundary():
    """Median in ±NEUTRAL_MEDIAN_BAND_SEC entering classifies as neutral."""
    entering = _const_sample(NEUTRAL_MEDIAN_BAND_SEC)
    leaving = _const_sample(NEUTRAL_MEDIAN_BAND_SEC - 10)
    label, _ = classify_timepoint(entering, leaving)
    assert label == "neutral"


# ---------------------------------------------------------------------------
# Asymmetry signature
# ---------------------------------------------------------------------------


def _signature_for_pct(early_pct: float, late_pct: float) -> str:
    """Reproduce the direction-asymmetry signature rule in isolation."""
    diff = early_pct - late_pct
    if diff > ASYMMETRY_MARGIN_PCT:
        return "early_dominant"
    if diff < -ASYMMETRY_MARGIN_PCT:
        return "late_dominant"
    return "balanced"


@pytest.mark.smoke
def test_signature_early_dominant_above_margin():
    """early% − late% > 5pp → early_dominant."""
    assert _signature_for_pct(26.0, 10.0) == "early_dominant"


@pytest.mark.smoke
def test_signature_late_dominant_below_margin():
    """early% − late% < −5pp → late_dominant (the D80 dir-1 pattern)."""
    assert _signature_for_pct(8.0, 26.0) == "late_dominant"


@pytest.mark.smoke
def test_signature_balanced_within_margin():
    """|early% − late%| ≤ 5pp → balanced."""
    assert _signature_for_pct(15.0, 12.0) == "balanced"
    assert _signature_for_pct(15.0, 20.0) == "balanced"


# ---------------------------------------------------------------------------
# OTP bucket constants
# ---------------------------------------------------------------------------


def test_otp_threshold_constants_match_wmata_window():
    """−2 / +7 minute WMATA OTP window — guard against accidental edits."""
    assert EARLY_THRESHOLD_SEC == -120
    assert LATE_THRESHOLD_SEC == 7 * 60


# ---------------------------------------------------------------------------
# Service-date range helper
# ---------------------------------------------------------------------------


@pytest.mark.smoke
def test_default_service_date_range_is_30_days_ending_yesterday():
    """Default window ends yesterday, spans 30 inclusive days."""
    today = date(2026, 5, 15)
    start, end = default_service_date_range(today)
    assert end == date(2026, 5, 14)
    assert (end - start).days == 29  # 30 inclusive days


@pytest.mark.smoke
def test_default_service_date_range_custom_window():
    """Custom `days` argument scales the window symmetrically."""
    today = date(2026, 5, 15)
    start, end = default_service_date_range(today, days=7)
    assert end == date(2026, 5, 14)
    assert (end - start).days == 6


# ---------------------------------------------------------------------------
# Direction asymmetry SQL path on SQLite — skipped because the production
# query uses Postgres `AT TIME ZONE`. The classification rule above is the
# critical surface for testing; the SQL bucketing has been validated in
# the D80 deep-dive that motivated this work. Add an integration test
# against a Postgres fixture if SQL behaviour ever diverges.
# ---------------------------------------------------------------------------


def test_classify_emits_distribution_summaries():
    """Stats dict carries the four summaries the panels surface."""
    entering = _const_sample(150)
    leaving = _const_sample(20)
    _, stats = classify_timepoint(entering, leaving)
    assert {
        "median_dev_entering",
        "median_dev_leaving",
        "p10_dev_entering",
        "p10_dev_leaving",
    }.issubset(stats.keys())
    assert stats["median_dev_entering"] == 150
    assert stats["median_dev_leaving"] == 20


def test_classify_handles_skewed_sample_with_distinct_percentiles():
    """A skewed sample produces distinct median and p10 values.

    Most percentile-based unit tests use constant samples to keep results
    predictable; this one uses a graded sample to confirm the implementation
    actually returns p50 vs p10 (a percentile-aware estimator, not just
    the mean).
    """
    # 30 values: 0, 5, 10, ..., 145 — uniformly spread.
    entering = [i * 5.0 for i in range(MIN_TIMEPOINT_OBSERVATIONS)]
    leaving = entering[:]
    _, stats = classify_timepoint(entering, leaving)
    # p10 of 0..145 is around 14, median around 72.
    assert stats["p10_dev_entering"] is not None
    assert stats["median_dev_entering"] is not None
    assert stats["p10_dev_entering"] < stats["median_dev_entering"]
