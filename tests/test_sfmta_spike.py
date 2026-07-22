"""Pure-function tests for the day-1 trip_id validation spike."""

from scripts.sfmta_feed_spike import trip_id_match_rate


def test_match_rate_full_overlap():
    assert trip_id_match_rate({"a", "b"}, {"a", "b", "c"}) == 1.0


def test_match_rate_partial():
    assert trip_id_match_rate({"a", "b", "x", "y"}, {"a", "b"}) == 0.5


def test_match_rate_empty_rt_is_zero():
    """No RT trips (outage / 3am) must read as 0.0, not a crash."""
    assert trip_id_match_rate(set(), {"a"}) == 0.0
