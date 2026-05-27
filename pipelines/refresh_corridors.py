"""Pipeline: rebuild ``corridors`` + ``corridor_route_membership`` from GTFS shapes.

Called from ``scripts/reload_gtfs_complete.py`` (and any subsequent
GTFS-snapshot refresh). The implementation is the Python-side counterpart
to the shape-matching algorithm in ``src/corridor_identity.py``.

Algorithm overview (see ``docs/superpowers/specs/2026-05-25-cross-route-corridor-design.md``,
Section 1):

  1. Pick a canonical shape per (route_id, direction_id) by trip count.
  2. Augment each canonical shape's points with local bearings.
  3. Grid-bucket all canonical points; for each point, find OTHER
     (route, direction) shapes that pass within 15m and 30 deg of
     bearing.
  4. Walk each canonical shape; emit runs at every change in the
     colocated route set. Discard runs where the set has fewer than 2
     routes or fewer than ``MIN_RUN_POINTS`` points.
  5. For each run that clears ``MIN_CORRIDOR_LENGTH_M``, snap endpoints
     to stops shared by every route in the set (within
     ``ENDPOINT_STOP_SNAP_M``), then dedupe by the resulting
     ``(cardinal, start_stop_id, end_stop_id, route_set)`` identity —
     this is the same tuple as the DB's ``uq_corridor_identity``
     constraint, so collisions are resolved here rather than at INSERT.
  6. Persist in a single transaction: ``DELETE`` then ``INSERT`` both
     tables; ``corridor_route_membership.corridor_id`` is resolved by
     flushing each new ``Corridor`` to get its serial PK.

Usage:
    uv run python pipelines/refresh_corridors.py
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from sqlalchemy import delete, text
from sqlalchemy.orm import Session

from src.corridor_identity import (
    MIN_CORRIDOR_LENGTH_M,
    Run,
    ShapeKey,
    StopInfo,
    augment_shape_with_bearings,
    bearing_to_cardinal,
    build_display_name,
    compute_colocated_route_sets,
    extract_runs,
    pick_canonical_shapes,
    snap_run_to_stops,
)
from src.models import Corridor, CorridorRouteMembership


@dataclass
class RefreshCounts:
    """Per-run counters returned for logging."""

    canonical_shapes_picked: int = 0
    points_examined: int = 0
    runs_extracted: int = 0
    corridors_inserted: int = 0
    memberships_inserted: int = 0


def _load_trip_shape_counts(session: Session) -> list[tuple[str, int, str, int]]:
    """Return ``(route_id, direction_id, shape_id, n_trips)`` for current trips."""
    rows = session.execute(
        text(
            """
            SELECT t.route_id, t.direction_id, t.shape_id, COUNT(*) AS n_trips
            FROM trips t
            WHERE t.is_current = TRUE
              AND t.shape_id IS NOT NULL
            GROUP BY t.route_id, t.direction_id, t.shape_id
            """
        )
    ).all()
    return [(r.route_id, r.direction_id, r.shape_id, r.n_trips) for r in rows]


def _load_canonical_shape_points(
    session: Session,
    canonical: dict[ShapeKey, str],
) -> dict[ShapeKey, tuple[str, list[tuple[float, float, int, float]]]]:
    """Load bearing-augmented points for every canonical shape.

    Returns ``{(route_id, direction_id): (shape_id, [(lat, lon, seq, bearing), ...])}``.
    Shapes with fewer than 2 points are silently skipped — they cannot
    contribute bearing information.
    """
    if not canonical:
        return {}

    shape_ids = sorted(set(canonical.values()))
    rows = session.execute(
        text(
            """
            SELECT shape_id, shape_pt_lat AS lat, shape_pt_lon AS lon,
                   shape_pt_sequence AS seq
            FROM shapes
            WHERE shape_id = ANY(:shape_ids)
            ORDER BY shape_id, shape_pt_sequence
            """
        ),
        {"shape_ids": shape_ids},
    ).all()

    points_by_shape: dict[str, list[tuple[float, float, int]]] = defaultdict(list)
    for row in rows:
        points_by_shape[row.shape_id].append((row.lat, row.lon, row.seq))

    out: dict[ShapeKey, tuple[str, list[tuple[float, float, int, float]]]] = {}
    for key, shape_id in canonical.items():
        pts = points_by_shape.get(shape_id, [])
        if len(pts) < 2:
            continue
        out[key] = (shape_id, augment_shape_with_bearings(pts))
    return out


def _load_stops(session: Session) -> dict[str, StopInfo]:
    """Load all current stops into a ``stop_id -> StopInfo`` map."""
    rows = session.execute(
        text(
            """
            SELECT stop_id, stop_name, stop_lat AS lat, stop_lon AS lon
            FROM stops
            WHERE is_current = TRUE
            """
        )
    ).all()
    return {
        r.stop_id: StopInfo(stop_id=r.stop_id, stop_name=r.stop_name, lat=r.lat, lon=r.lon)
        for r in rows
    }


def _load_route_stops(
    session: Session,
    canonical: dict[ShapeKey, str],
) -> dict[ShapeKey, list[tuple[str, int]]]:
    """Return the canonical-trip stop pattern per ``(route_id, direction_id)``.

    For each canonical shape we pick a single representative trip (lowest
    ``trip_id``) and return its ``[(stop_id, stop_sequence), ...]`` list
    ordered by ``stop_sequence``. The membership table records ranges
    against this canonical stop pattern.
    """
    if not canonical:
        return {}

    # Build a (route_id, direction_id, shape_id) -> [(stop_id, seq), ...] map.
    rows = session.execute(
        text(
            """
            WITH ranked AS (
                SELECT t.route_id, t.direction_id, t.shape_id, t.trip_id,
                       ROW_NUMBER() OVER (
                           PARTITION BY t.route_id, t.direction_id, t.shape_id
                           ORDER BY t.trip_id
                       ) AS rn
                FROM trips t
                WHERE t.is_current = TRUE
                  AND t.shape_id IS NOT NULL
            ),
            picked AS (
                SELECT route_id, direction_id, shape_id, trip_id
                FROM ranked
                WHERE rn = 1
            )
            SELECT p.route_id, p.direction_id, p.shape_id,
                   st.stop_id, st.stop_sequence
            FROM picked p
            JOIN stop_times st
              ON st.trip_id = p.trip_id AND st.is_current = TRUE
            ORDER BY p.route_id, p.direction_id, p.shape_id, st.stop_sequence
            """
        )
    ).all()

    by_rds: dict[tuple[str, int, str], list[tuple[str, int]]] = defaultdict(list)
    for row in rows:
        by_rds[(row.route_id, row.direction_id, row.shape_id)].append(
            (row.stop_id, row.stop_sequence)
        )

    out: dict[ShapeKey, list[tuple[str, int]]] = {}
    for key, shape_id in canonical.items():
        out[key] = by_rds.get((key[0], key[1], shape_id), [])
    return out


def _build_geometry_wkt(points: tuple[tuple[float, float, int, float], ...]) -> str:
    """Render a run's points as a WKT LINESTRING (lon lat ordering)."""
    coords = ", ".join(f"{lon} {lat}" for lat, lon, _, _ in points)
    return f"LINESTRING({coords})"


@dataclass
class _SnappedCandidate:
    """A run whose endpoints have been resolved to shared stops.

    Carries everything needed to assemble both a ``corridors`` row and
    its ``corridor_route_membership`` children, plus the dedup key
    (``identity``) used to collapse duplicates that the database's
    ``uq_corridor_identity`` constraint would otherwise reject.
    """

    run: Run
    cardinal: str
    start_stop_id: str
    start_stop_name: str
    end_stop_id: str
    end_stop_name: str
    per_route_range: dict[ShapeKey, tuple[int, int]]
    route_ids_sorted: list[str]
    length_m: float

    @property
    def identity(self) -> tuple[str, str, str, str]:
        """Identity tuple matching the DB's ``uq_corridor_identity`` constraint."""
        return (
            self.cardinal,
            self.start_stop_id,
            self.end_stop_id,
            ",".join(self.route_ids_sorted),
        )


def refresh_corridors(session: Session, gtfs_snapshot_id: int) -> dict[str, int]:
    """Rebuild the two corridor identity tables from current GTFS shapes.

    Truncates ``corridor_route_membership`` and ``corridors`` (child
    first to satisfy the FK) and re-inserts from scratch. The caller is
    responsible for committing the surrounding transaction.

    Args:
        session: SQLAlchemy session bound to the target database.
        gtfs_snapshot_id: Snapshot ID stamped on every new ``corridors`` row.

    Returns:
        Dict with per-step counters suitable for logging.
    """
    counts = RefreshCounts()

    # Step 1: pick canonical shape per (route_id, direction_id).
    trip_shape_counts = _load_trip_shape_counts(session)
    canonical = pick_canonical_shapes(trip_shape_counts)
    counts.canonical_shapes_picked = len(canonical)

    # Always wipe both tables first — failure modes below short-circuit
    # to "empty corridors" rather than leaving stale rows in place.
    session.execute(delete(CorridorRouteMembership))
    session.execute(delete(Corridor))

    if not canonical:
        return _counts_to_dict(counts)

    # Step 2: load + augment canonical shape points.
    augmented_shapes = _load_canonical_shape_points(session, canonical)
    counts.points_examined = sum(len(points) for _shape_id, points in augmented_shapes.values())

    # Step 3: compute colocation across all canonical shapes.
    colocated = compute_colocated_route_sets(augmented_shapes)

    # Step 4: extract runs from each canonical shape.
    all_runs: list[Run] = []
    for (route_id, direction_id), (shape_id, points) in augmented_shapes.items():
        all_runs.extend(
            extract_runs(
                route_id=route_id,
                direction_id=direction_id,
                canonical_shape_id=shape_id,
                points=points,
                colocated=colocated,
            )
        )
    counts.runs_extracted = len(all_runs)

    # Step 5: snap endpoints to shared stops and dedupe by the final
    # stop-anchored identity. Different runs (from different routes'
    # canonical shapes, or from non-overlapping segments of one shape)
    # can land on the same (cardinal, start_stop, end_stop, route_set)
    # tuple — the DB unique constraint demands we collapse them here
    # rather than letting the INSERT collide.
    stops = _load_stops(session)
    route_stops = _load_route_stops(session, canonical)

    candidates_by_identity: dict[tuple[str, str, str, str], _SnappedCandidate] = {}
    for run in all_runs:
        length_m = run.length_m
        if length_m < MIN_CORRIDOR_LENGTH_M:
            continue

        snap = snap_run_to_stops(
            route_set=run.route_set,
            run_points=list(run.points),
            stops=stops,
            route_stops=route_stops,
        )
        if snap is None:
            continue
        start_ref, end_ref, per_route_range = snap
        cardinal = bearing_to_cardinal(run.mean_bearing_deg)
        route_ids = sorted({rid for (rid, _) in run.route_set})
        # Skip self-overlap runs: when a single route's two directions
        # both fall inside the bearing+proximity window (one-way loops,
        # U-turns), the ShapeKey-based route_set has |set| >= 2 and
        # passes the extract_runs filter, but collapsing to route_id
        # leaves only one route — not a cross-route corridor.
        if len(route_ids) < 2:
            continue

        candidate = _SnappedCandidate(
            run=run,
            cardinal=cardinal,
            start_stop_id=start_ref.stop_id,
            start_stop_name=start_ref.stop_name,
            end_stop_id=end_ref.stop_id,
            end_stop_name=end_ref.stop_name,
            per_route_range=per_route_range,
            route_ids_sorted=route_ids,
            length_m=length_m,
        )
        prior = candidates_by_identity.get(candidate.identity)
        if prior is None or candidate.length_m > prior.length_m:
            candidates_by_identity[candidate.identity] = candidate

    # Step 6: assemble corridor + membership rows from the deduped set.
    corridor_rows: list[dict] = []
    membership_rows: list[dict] = []
    for cand in candidates_by_identity.values():
        display_name = build_display_name(
            cardinal=cand.cardinal,
            start_stop_name=cand.start_stop_name,
            end_stop_name=cand.end_stop_name,
        )
        # route_set is stored as comma-separated sorted route_ids (TEXT)
        # — Postgres rejects JSON columns in btree unique constraints,
        # so we serialize to TEXT here and at every query site.
        corridor_rows.append(
            {
                "direction_bearing_deg": cand.run.mean_bearing_deg,
                "direction_cardinal": cand.cardinal,
                "start_stop_id": cand.start_stop_id,
                "end_stop_id": cand.end_stop_id,
                "length_m": cand.length_m,
                "n_routes": len(cand.route_ids_sorted),
                "route_set": ",".join(cand.route_ids_sorted),
                "display_name": display_name,
                "geometry_wkt": _build_geometry_wkt(cand.run.points),
                "gtfs_snapshot_id": gtfs_snapshot_id,
            }
        )

        corridor_index = len(corridor_rows) - 1
        for (route_id, direction_id), (s_seq, e_seq) in cand.per_route_range.items():
            membership_rows.append(
                {
                    "_corridor_index": corridor_index,
                    "route_id": route_id,
                    "direction_id": direction_id,
                    "canonical_shape_id": canonical[(route_id, direction_id)],
                    "start_stop_sequence": s_seq,
                    "end_stop_sequence": e_seq,
                }
            )

    # Step 7: persist. We add Corridors first, flush to materialize
    # their serial PKs, then add memberships keyed by index.
    inserted_corridor_ids: list[int] = []
    for row in corridor_rows:
        c = Corridor(**row)
        session.add(c)
        session.flush()
        inserted_corridor_ids.append(c.corridor_id)

    for mrow in membership_rows:
        idx = mrow.pop("_corridor_index")
        mrow["corridor_id"] = inserted_corridor_ids[idx]
        session.add(CorridorRouteMembership(**mrow))

    counts.corridors_inserted = len(corridor_rows)
    counts.memberships_inserted = len(membership_rows)
    session.flush()
    return _counts_to_dict(counts)


def _counts_to_dict(counts: RefreshCounts) -> dict[str, int]:
    """Flatten ``RefreshCounts`` to a plain dict for logging."""
    return {
        "canonical_shapes_picked": counts.canonical_shapes_picked,
        "points_examined": counts.points_examined,
        "runs_extracted": counts.runs_extracted,
        "corridors_inserted": counts.corridors_inserted,
        "memberships_inserted": counts.memberships_inserted,
    }


def main() -> None:
    """CLI entrypoint: refresh corridors against the configured DB."""
    from dotenv import load_dotenv

    from src.database import get_session

    load_dotenv()
    session = get_session()
    try:
        snap_id = session.execute(
            text("SELECT MAX(snapshot_id) FROM routes WHERE is_current = TRUE")
        ).scalar_one()
        counts = refresh_corridors(session=session, gtfs_snapshot_id=snap_id)
        session.commit()
        print(f"[refresh_corridors] {counts}")
    finally:
        session.close()


if __name__ == "__main__":
    main()
