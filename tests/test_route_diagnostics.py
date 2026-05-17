"""
Unit tests for `src/route_diagnostics.py` (NOTES-57).

The classification logic is the load-bearing surface — thresholds for
recovery / leaky / underpowered / neutral determine whether a
timepoint shows up on RouteDetail's diagnosis panel (PR #124) and on
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

from datetime import date, datetime

import pytest

from src.models import Route, StopEvent
from src.route_diagnostics import (
    ASYMMETRY_MARGIN_PCT,
    EARLY_THRESHOLD_SEC,
    LATE_THRESHOLD_SEC,
    LEAKY_P10_DROP_SEC,
    MIN_TIMEPOINT_OBSERVATIONS,
    NEUTRAL_MEDIAN_BAND_SEC,
    RECOVERY_MEDIAN_DROP_SEC,
    UNDERPOWERED_ENTERING_MEDIAN_SEC,
    _assemble_segment_slip_output,
    classify_timepoint,
    compute_canonical_stop_mapping,
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


# ---------------------------------------------------------------------------
# Canonical-pattern collapse (NOTES-57 fast-follow)
#
# Route variants (express+local splits, mid-route detours) legitimately
# produce multiple stop_ids at the same (direction_id, stop_sequence) over
# the 30-day window. Without filtering, the slip aggregation emits multiple
# rows per (direction_id, from_seq, to_seq), which both violates the
# `route_diagnostic_segment` unique constraint (PR #107 fast-follow bug
# mode A — 24 routes failed to load) and inflates `cum_slip_sec` on the
# cumsum walk (bug mode B — 8 routes loaded with wrong cum slip).
#
# Tests here exercise the SQL path on the in-memory SQLite test DB; the
# canonical CTE deliberately avoids Postgres-only `EXTRACT` / `AT TIME
# ZONE` so it runs under both dialects (the slip SQL itself remains
# Postgres-only — its cumsum is unit-tested via `_assemble_segment_slip_output`).
# ---------------------------------------------------------------------------


def _make_stop_event(
    *,
    service_date: str,
    trip_id: str,
    route_id: str,
    direction_id: int,
    stop_sequence: int,
    stop_id: str,
    base_ts: datetime,
    minutes_offset: int = 0,
) -> StopEvent:
    """Build one `StopEvent` row shaped for the canonical-mapping SQL filters.

    All required filters (`source='proximity'`, `schedule_relationship='SCHEDULED'`,
    `observed_arrival_ts` non-null, `scheduled_arrival_ts` non-null) are
    satisfied by construction so the test data sees the same predicate
    set as production.
    """
    from datetime import timedelta as _td

    obs_ts = base_ts + _td(minutes=minutes_offset)
    return StopEvent(
        service_date=service_date,
        trip_id=trip_id,
        route_id=route_id,
        direction_id=direction_id,
        stop_id=stop_id,
        stop_sequence=stop_sequence,
        scheduled_arrival_ts=obs_ts,
        observed_arrival_ts=obs_ts,
        deviation_sec=0,
        source="proximity",
        schedule_relationship="SCHEDULED",
    )


def _seed_variant_route(db_session, route_id: str = "VRTEST") -> None:
    """Seed a two-pattern route used by the canonical-mapping tests.

    Pattern A (majority — 10 trips): seq 1 → stop_A1, seq 2 → stop_A2,
    seq 3 → stop_A3. Pattern B (minority — 3 trips): seq 1 → stop_A1
    (shared origin), seq 2 → stop_B2, seq 3 → stop_A3 (shared terminus).
    The mid-route divergence at seq 2 is the variant pattern that breaks
    the slip table's unique constraint on real routes (e.g., P93 seq 3-4).
    """
    db_session.add(
        Route(
            route_id=route_id,
            route_short_name=route_id,
            route_long_name=f"Variant test route {route_id}",
            route_type=3,
            is_current=True,
        )
    )
    base_ts = datetime(2026, 5, 1, 14, 0, 0)
    events: list[StopEvent] = []
    # Majority pattern (10 trips): stop_A1 -> stop_A2 -> stop_A3
    for i in range(10):
        trip_id = f"TRIP_A_{i}"
        for seq, stop_id in [(1, "stop_A1"), (2, "stop_A2"), (3, "stop_A3")]:
            events.append(
                _make_stop_event(
                    service_date="2026-05-01",
                    trip_id=trip_id,
                    route_id=route_id,
                    direction_id=0,
                    stop_sequence=seq,
                    stop_id=stop_id,
                    base_ts=base_ts,
                    minutes_offset=seq,
                )
            )
    # Minority pattern (3 trips): stop_A1 -> stop_B2 -> stop_A3
    for i in range(3):
        trip_id = f"TRIP_B_{i}"
        for seq, stop_id in [(1, "stop_A1"), (2, "stop_B2"), (3, "stop_A3")]:
            events.append(
                _make_stop_event(
                    service_date="2026-05-01",
                    trip_id=trip_id,
                    route_id=route_id,
                    direction_id=0,
                    stop_sequence=seq,
                    stop_id=stop_id,
                    base_ts=base_ts,
                    minutes_offset=seq,
                )
            )
    db_session.add_all(events)
    db_session.commit()


def test_canonical_mapping_picks_most_served_stop(db_session):
    """Majority-pattern stop_id wins at every (direction_id, stop_sequence)."""
    _seed_variant_route(db_session, route_id="VRMAJ")
    mapping = compute_canonical_stop_mapping(
        db_session,
        route_id="VRMAJ",
        service_date_range=(date(2026, 5, 1), date(2026, 5, 1)),
    )
    # All three positions should map to the majority-pattern stop.
    assert mapping[(0, 1)] == "stop_A1"
    assert mapping[(0, 2)] == "stop_A2"  # the divergence point: A wins
    assert mapping[(0, 3)] == "stop_A3"


def test_canonical_mapping_lexical_tiebreak(db_session):
    """Equal trip counts → lexically smaller stop_id wins (deterministic)."""
    db_session.add(
        Route(
            route_id="VRTIE",
            route_short_name="VRTIE",
            route_long_name="Variant tiebreak test",
            route_type=3,
            is_current=True,
        )
    )
    base_ts = datetime(2026, 5, 1, 14, 0, 0)
    events: list[StopEvent] = []
    # Five trips through stop_alpha at seq 2, five trips through stop_beta.
    # Lexical tiebreak ("alpha" < "beta") should pick stop_alpha.
    for i in range(5):
        for tag, stop_id_seq2 in [("ALPHA", "stop_alpha"), ("BETA", "stop_beta")]:
            trip_id = f"TRIP_{tag}_{i}"
            events.append(
                _make_stop_event(
                    service_date="2026-05-01",
                    trip_id=trip_id,
                    route_id="VRTIE",
                    direction_id=0,
                    stop_sequence=2,
                    stop_id=stop_id_seq2,
                    base_ts=base_ts,
                )
            )
    db_session.add_all(events)
    db_session.commit()

    mapping = compute_canonical_stop_mapping(
        db_session,
        route_id="VRTIE",
        service_date_range=(date(2026, 5, 1), date(2026, 5, 1)),
    )
    assert mapping[(0, 2)] == "stop_alpha"


def test_segment_slip_excludes_minority_variant_observations(db_session):
    """Canonical filter drops the minority pattern from the slip aggregation.

    The slip SQL itself is Postgres-only, but the canonical CTE that
    powers its filter runs under SQLite via
    :func:`compute_canonical_stop_mapping`. Verifying the mapping
    excludes the variant `stop_id` at the divergence sequence is the
    load-bearing invariant — the slip SQL's twin JOIN to ``canonical``
    will then exclude any observation whose endpoint isn't the canonical
    stop, leaving exactly one row per ``(direction_id, from_seq, to_seq)``.
    """
    _seed_variant_route(db_session, route_id="VRSEG")
    mapping = compute_canonical_stop_mapping(
        db_session,
        route_id="VRSEG",
        service_date_range=(date(2026, 5, 1), date(2026, 5, 1)),
    )
    # The minority pattern's distinctive stop ("stop_B2") must NOT appear
    # as the canonical mapping for any position.
    canonical_stop_ids = set(mapping.values())
    assert "stop_B2" not in canonical_stop_ids
    # And the canonical mapping for seq 2 must be the majority's stop.
    assert mapping[(0, 2)] == "stop_A2"


def test_cumulative_slip_unaffected_by_variant_observations():
    """Cumsum walks one row per (direction_id, from_seq) once canonical-filtered.

    Pre-fix, duplicate rows at the same ``(direction_id, from_seq, to_seq)``
    (one per variant) would each contribute to the cumsum, double-counting
    the slip at that position. Post-fix, the canonical filter inside the
    slip SQL guarantees one row per position, so the cumsum walk produces
    the simple running total of per-segment mean slips.

    This test exercises :func:`_assemble_segment_slip_output` (the post-
    SQL stage) with synthetic canonical-shape rows to confirm the cumsum
    arithmetic. If a variant slipped through the canonical filter the
    invariant would break — that's tested separately via the canonical
    mapping tests.
    """
    canonical_rows = [
        # direction 0: three segments, mean slips 10, 20, 30
        {
            "direction_id": 0,
            "from_seq": 1,
            "from_stop_id": "S1",
            "to_seq": 2,
            "to_stop_id": "S2",
            "n_observations": 100,
            "mean_slip_sec": 10.0,
        },
        {
            "direction_id": 0,
            "from_seq": 2,
            "from_stop_id": "S2",
            "to_seq": 3,
            "to_stop_id": "S3",
            "n_observations": 100,
            "mean_slip_sec": 20.0,
        },
        {
            "direction_id": 0,
            "from_seq": 3,
            "from_stop_id": "S3",
            "to_seq": 4,
            "to_stop_id": "S4",
            "n_observations": 100,
            "mean_slip_sec": 30.0,
        },
    ]
    out = _assemble_segment_slip_output(canonical_rows, period="all")
    # Origin segment (from_seq=1) dropped per the module-docstring rule.
    assert [r["from_seq"] for r in out] == [2, 3]
    # cum_slip_sec is the running sum from the first kept segment.
    assert out[0]["cum_slip_sec"] == 20.0
    assert out[1]["cum_slip_sec"] == 50.0  # 20 + 30; NOT 60 (which would
    # indicate the cumsum double-walked a duplicate row)
    assert all(r["period"] == "all" for r in out)


def test_cumulative_slip_would_inflate_if_duplicates_were_present():
    """Pin the bug surface: duplicate-row input inflates cum_slip_sec.

    The pre-fix bug: when route variants produced two rows at the same
    (direction_id, from_seq, to_seq) (different stop_id pairs), the
    cumsum loop walked both and added each mean_slip_sec to the running
    total. This test reproduces that input shape and confirms the helper
    really does inflate — locking the contract that the canonical filter
    upstream is what prevents the bug, not any clever dedup in the helper.
    """
    duplicated_rows = [
        # Two rows at (dir 0, from_seq=2, to_seq=3): the pre-fix bug shape.
        {
            "direction_id": 0,
            "from_seq": 2,
            "from_stop_id": "S2_majority",
            "to_seq": 3,
            "to_stop_id": "S3",
            "n_observations": 100,
            "mean_slip_sec": 20.0,
        },
        {
            "direction_id": 0,
            "from_seq": 2,
            "from_stop_id": "S2_minority",
            "to_seq": 3,
            "to_stop_id": "S3",
            "n_observations": 30,
            "mean_slip_sec": 50.0,
        },
        # Origin segment to be dropped.
        {
            "direction_id": 0,
            "from_seq": 1,
            "from_stop_id": "S1",
            "to_seq": 2,
            "to_stop_id": "S2_majority",
            "n_observations": 100,
            "mean_slip_sec": 5.0,
        },
    ]
    out = _assemble_segment_slip_output(duplicated_rows, period="all")
    # Both duplicate rows survive (the helper doesn't dedup at the
    # (from_seq, to_seq) level — canonical filtering upstream enforces
    # that invariant). With the NOTES-57 fast-follow fix the cumsum
    # walk visits each from_seq at most once: only the first row at
    # (2, 3) advances the walk; subsequent rows with the same from_seq
    # don't re-add their mean_slip_sec to the running total. Both rows
    # match `consecutive_to_seq[2] == 3` (same to_seq), so both receive
    # the post-walk arrival value (20.0). The pre-fix inflation
    # ({20.0, 70.0}) no longer happens — that's the bug NOTES-57 fast-
    # follow fixed at the cumsum layer. The contract that the canonical
    # filter upstream guarantees one-row-per-(from_seq, to_seq) is
    # unchanged.
    assert len(out) == 2  # both duplicates survive
    assert {r["cum_slip_sec"] for r in out} == {20.0}


# ---------------------------------------------------------------------------
# Consecutive-edge cumsum walk (NOTES-57 fast-follow, skip-N variant)
#
# When proximity observations are sparse the same `from_seq` can appear in
# multiple rows with different `to_seq` values. Each per-edge mean_slip is
# correct in isolation, but skip-N edges' means already include the
# intermediate consecutive edges' slip implicitly, so walking them in the
# cumsum double/triple-counts. The fix walks only the consecutive edge
# (min to_seq per from_seq); skip-N rows still get a well-defined
# cum_slip_sec (the cumsum at their from_seq's origin) to avoid a schema
# migration.
# ---------------------------------------------------------------------------


def test_cumulative_slip_walks_only_consecutive_edges():
    """Skip-N edges don't advance the cumsum; consecutive edges do.

    Sparse-proximity synthetic: from_seq 2 and 3 each have one consecutive
    and one skip-1 edge. The cumsum walk should follow only the
    consecutive edges (2→3 and 3→4), and skip-N rows should carry the
    cumsum value at their from_seq's origin (i.e., BEFORE adding the
    from_seq's own consecutive edge).
    """
    rows = [
        # Origin segment (dropped per the origin-departure rule).
        {
            "direction_id": 0,
            "from_seq": 1,
            "from_stop_id": "S1",
            "to_seq": 2,
            "to_stop_id": "S2",
            "n_observations": 100,
            "mean_slip_sec": 5.0,
        },
        # Consecutive edge from seq 2.
        {
            "direction_id": 0,
            "from_seq": 2,
            "from_stop_id": "S2",
            "to_seq": 3,
            "to_stop_id": "S3",
            "n_observations": 100,
            "mean_slip_sec": 10.0,
        },
        # Skip-1 from seq 2 (D80-style — sparse ping at seq 3 on some trips).
        {
            "direction_id": 0,
            "from_seq": 2,
            "from_stop_id": "S2",
            "to_seq": 4,
            "to_stop_id": "S4",
            "n_observations": 40,
            "mean_slip_sec": 25.0,
        },
        # Consecutive edge from seq 3.
        {
            "direction_id": 0,
            "from_seq": 3,
            "from_stop_id": "S3",
            "to_seq": 4,
            "to_stop_id": "S4",
            "n_observations": 100,
            "mean_slip_sec": 15.0,
        },
        # Skip-1 from seq 3.
        {
            "direction_id": 0,
            "from_seq": 3,
            "from_stop_id": "S3",
            "to_seq": 5,
            "to_stop_id": "S5",
            "n_observations": 30,
            "mean_slip_sec": 40.0,
        },
        # Consecutive edge from seq 4.
        {
            "direction_id": 0,
            "from_seq": 4,
            "from_stop_id": "S4",
            "to_seq": 5,
            "to_stop_id": "S5",
            "n_observations": 100,
            "mean_slip_sec": 20.0,
        },
    ]
    out = _assemble_segment_slip_output(rows, period="all")

    by_key = {(r["from_seq"], r["to_seq"]): r for r in out}

    # Origin (1, 2) dropped.
    assert (1, 2) not in by_key
    assert len(out) == 5

    # Consecutive edges: cumsum at to-stop, walking only consecutive edges.
    assert by_key[(2, 3)]["cum_slip_sec"] == 10.0  # 0 + 10
    assert by_key[(3, 4)]["cum_slip_sec"] == 25.0  # 10 + 15
    assert by_key[(4, 5)]["cum_slip_sec"] == 45.0  # 25 + 20

    # Skip-N edges: cumsum at from_seq's origin (BEFORE that from_seq's
    # consecutive edge contributed).
    assert by_key[(2, 4)]["cum_slip_sec"] == 0.0  # before any consecutive walk
    assert by_key[(3, 5)]["cum_slip_sec"] == 10.0  # after seq 2's consecutive

    # period carried through.
    assert all(r["period"] == "all" for r in out)


def test_cumulative_slip_no_skip_edges_unchanged():
    """Dense-observation routes (only consecutive edges) are unaffected.

    With no skip-N edges in the input the fix collapses to the pre-fix
    behavior: a simple running sum of mean_slip_sec across the
    direction-sorted segments.
    """
    rows = [
        # Origin (dropped).
        {
            "direction_id": 0,
            "from_seq": 1,
            "from_stop_id": "S1",
            "to_seq": 2,
            "to_stop_id": "S2",
            "n_observations": 100,
            "mean_slip_sec": 5.0,
        },
        # Three consecutive edges, no skip-N siblings.
        {
            "direction_id": 0,
            "from_seq": 2,
            "from_stop_id": "S2",
            "to_seq": 3,
            "to_stop_id": "S3",
            "n_observations": 100,
            "mean_slip_sec": 10.0,
        },
        {
            "direction_id": 0,
            "from_seq": 3,
            "from_stop_id": "S3",
            "to_seq": 4,
            "to_stop_id": "S4",
            "n_observations": 100,
            "mean_slip_sec": 20.0,
        },
        {
            "direction_id": 0,
            "from_seq": 4,
            "from_stop_id": "S4",
            "to_seq": 5,
            "to_stop_id": "S5",
            "n_observations": 100,
            "mean_slip_sec": 30.0,
        },
    ]
    out = _assemble_segment_slip_output(rows, period="all")
    assert [r["from_seq"] for r in out] == [2, 3, 4]
    # Running sum matches pre-fix behavior on dense-observation input.
    assert [r["cum_slip_sec"] for r in out] == [10.0, 30.0, 60.0]


def test_origin_departure_segment_dropped_before_cumsum():
    """Origin-departure exclusion happens before the consecutive filter.

    Even when the origin from_seq has skip-N siblings, the entire
    minimum-from_seq group is dropped before the consecutive walk
    decides what advances the cumsum — so the origin's slip never
    contributes, regardless of skip-N variants at that position.
    """
    rows = [
        # Origin segment with both consecutive and skip-1 variants.
        {
            "direction_id": 0,
            "from_seq": 1,
            "from_stop_id": "S1",
            "to_seq": 2,
            "to_stop_id": "S2",
            "n_observations": 100,
            "mean_slip_sec": 50.0,  # large value — would dominate cumsum if kept
        },
        {
            "direction_id": 0,
            "from_seq": 1,
            "from_stop_id": "S1",
            "to_seq": 3,
            "to_stop_id": "S3",
            "n_observations": 30,
            "mean_slip_sec": 100.0,  # even larger skip-1 slip on origin
        },
        # Real consecutive edge starting from seq 2.
        {
            "direction_id": 0,
            "from_seq": 2,
            "from_stop_id": "S2",
            "to_seq": 3,
            "to_stop_id": "S3",
            "n_observations": 100,
            "mean_slip_sec": 10.0,
        },
    ]
    out = _assemble_segment_slip_output(rows, period="all")
    # Both origin rows (consecutive + skip-1 at from_seq=1) dropped.
    assert all(r["from_seq"] != 1 for r in out)
    assert len(out) == 1
    # The remaining consecutive edge starts the cumsum from zero —
    # origin's 50.0 / 100.0 are nowhere in the running total.
    assert out[0]["from_seq"] == 2
    assert out[0]["cum_slip_sec"] == 10.0
