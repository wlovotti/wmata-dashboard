"""
Deterministic SHA-256 hash of the route_diagnostic_* profile rows for one
(route_id, period). Used by ``scripts/generate_route_diagnosis.py`` (writer)
and ``api/aggregations.py`` (staleness checker) so both compute the identical
hash from the same DB state.

The hash is order-independent: rows are sorted by a stable key before
serialization, so inserting or re-computing rows in a different order does
not change the hash.
"""

import hashlib
import json
from typing import Any


def _canonical_segment_row(row: dict[str, Any]) -> dict[str, Any]:
    """Return only the fields that matter for change detection on a segment row.

    Excludes ``id``, ``computed_at``, and ``route_id`` / ``period`` (they are
    the same for every row in a given computation and do not carry content
    information relevant to staleness).
    """
    return {
        "direction_id": row["direction_id"],
        "from_seq": row["from_seq"],
        "from_stop_id": row["from_stop_id"],
        "to_seq": row["to_seq"],
        "to_stop_id": row["to_stop_id"],
        "mean_slip_sec": row["mean_slip_sec"],
        "cum_slip_sec": row["cum_slip_sec"],
        "n_observations": row["n_observations"],
        "is_timepoint": row["is_timepoint"],
    }


def _canonical_timepoint_row(row: dict[str, Any]) -> dict[str, Any]:
    """Return only the fields that matter for change detection on a timepoint row."""
    return {
        "direction_id": row["direction_id"],
        "timepoint_stop_id": row["timepoint_stop_id"],
        "classification": row["classification"],
        "median_dev_entering": row["median_dev_entering"],
        "median_dev_leaving": row["median_dev_leaving"],
        "p10_dev_entering": row["p10_dev_entering"],
        "p10_dev_leaving": row["p10_dev_leaving"],
        "n_observations": row["n_observations"],
    }


def compute_profile_hash(
    segment_rows: list[dict[str, Any]],
    timepoint_rows: list[dict[str, Any]],
) -> str:
    """Compute a deterministic SHA-256 hex digest over the diagnostic profile rows.

    Args:
        segment_rows: List of dicts from ``route_diagnostic_segment`` for a
            single ``(route_id, period)``. Each dict must include at least the
            keys returned by ``_canonical_segment_row``.
        timepoint_rows: List of dicts from ``route_diagnostic_timepoint`` for
            the same ``(route_id, period)``. Each dict must include at least
            the keys returned by ``_canonical_timepoint_row``.

    Returns:
        64-character lowercase SHA-256 hex string.
    """
    canonical_segments = sorted(
        [_canonical_segment_row(r) for r in segment_rows],
        key=lambda r: (r["direction_id"], r["from_seq"], r["to_seq"]),
    )
    canonical_timepoints = sorted(
        [_canonical_timepoint_row(r) for r in timepoint_rows],
        key=lambda r: (r["direction_id"], r["timepoint_stop_id"]),
    )
    payload = {
        "segments": canonical_segments,
        "timepoints": canonical_timepoints,
    }
    # sort_keys=True for reproducibility across Python versions / dict orderings.
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()
