"""Unit tests for `src.service_profile` classification helpers."""

from src.service_profile import (
    FREQUENCY_CLASS_LIMITED,
    FREQUENCY_CLASS_LIMITED_STOP,
    classify_route_frequency,
)


class TestClassifyRouteFrequency:
    """Boundary and edge-case checks on the P90 frequency classifier."""

    def test_x_suffix_overrides_frequency(self):
        """Routes ending in 'X' classify as limited-stop regardless of headways."""
        assert classify_route_frequency([5.0, 6.0, 7.0], "D4X") == FREQUENCY_CLASS_LIMITED_STOP
        assert classify_route_frequency([], "A1X") == FREQUENCY_CLASS_LIMITED_STOP

    def test_no_data_returns_none(self):
        """Empty headway list (and non-X route) yields None."""
        assert classify_route_frequency([], "C21") is None

    def test_high_frequency(self):
        """All hours ≤12 min → high."""
        assert classify_route_frequency([8.0, 9.0, 10.0, 11.0, 12.0], "D20") == "high"

    def test_medium_frequency(self):
        """P90 between 12 and 20 → medium."""
        assert classify_route_frequency([10.0, 12.0, 15.0, 18.0, 20.0], "C21") == "medium"

    def test_low_frequency(self):
        """P90 between 20 and 30 → low."""
        assert classify_route_frequency([15.0, 20.0, 25.0, 28.0, 30.0], "C27") == "low"

    def test_limited_frequency(self):
        """P90 above 30 → limited."""
        assert (
            classify_route_frequency([20.0, 30.0, 35.0, 40.0, 45.0], "C43")
            == FREQUENCY_CLASS_LIMITED
        )

    def test_p90_absorbs_single_hour_artifact(self):
        """A lone 700-min hour shouldn't drag a frequent route to limited.

        Reproduces the D50 hr=1=710-min late-night artifact: most hours under
        20 min, one extreme outlier. With max-rule this would classify as
        limited; P90 keeps it at medium.
        """
        # 23 hours under 20 min, 1 hour at 710 → P90 should land in the high-teens.
        headways = [10.0] * 11 + [15.0] * 11 + [20.0, 710.0]
        assert classify_route_frequency(headways, "D50") == "medium"

    def test_threshold_boundaries_inclusive(self):
        """Exactly 12 / 20 / 30 fall into high / medium / low respectively."""
        # All values ≤12 → high.
        assert classify_route_frequency([12.0] * 5, "R1") == "high"
        # All ≤20 with at least one 20 → medium.
        assert classify_route_frequency([10.0, 15.0, 20.0, 20.0, 20.0], "R2") == "medium"
        # All ≤30 with at least one 30 → low.
        assert classify_route_frequency([20.0, 25.0, 30.0, 30.0, 30.0], "R3") == "low"
