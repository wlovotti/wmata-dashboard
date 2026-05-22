"""
Offline CLI: generate LLM narrative summaries for route diagnostic profiles.

Reads the materialized ``route_diagnostic_*`` tables (PR #107), calls Claude
with a structured prompt and the profile as context, and writes the result to
``route_diagnosis_narrative`` keyed by ``(route_id, period)``.

**This script calls the Anthropic API and costs money. It is a build-time
tool; the public-facing API never calls Claude.**

Requirements:
  - ANTHROPIC_API_KEY in environment or ``.env``
  - The ``anthropic`` package (``uv sync --extra llm``)
  - The ``route_diagnostic_*`` tables must be populated by
    ``pipelines/refresh_route_diagnostic_profile.py``

Usage::

    # Single route, default period (all)
    uv run python scripts/generate_route_diagnosis.py --route D80

    # Single route, specific period
    uv run python scripts/generate_route_diagnosis.py --route D80 --period am_peak

    # All routes with materialized profiles (default period only)
    uv run python scripts/generate_route_diagnosis.py --all

    # All routes, all periods
    uv run python scripts/generate_route_diagnosis.py --all --all-periods

    # Dry-run: print the prompt without calling Claude
    uv run python scripts/generate_route_diagnosis.py --route D80 --dry-run

    # Force regeneration even if the hash matches (narrative already up-to-date)
    uv run python scripts/generate_route_diagnosis.py --route D80 --force
"""

import argparse
import os
import sys

from dotenv import load_dotenv

# Load .env before importing anything that reads env vars.
load_dotenv()

from sqlalchemy.orm import Session  # noqa: E402

from src.database import get_engine  # noqa: E402
from src.diagnosis_hash import compute_profile_hash  # noqa: E402
from src.models import (  # noqa: E402
    Base,
    Route,
    RouteDiagnosisNarrative,
    RouteDiagnosticDirection,
    RouteDiagnosticSegment,
    RouteDiagnosticTimepoint,
)
from src.timezones import utcnow_naive  # noqa: E402

# ---------------------------------------------------------------------------
# Prompt versioning — bump when the prompt text changes so callers can detect
# that cached narratives were generated with an older prompt.
# ---------------------------------------------------------------------------
PROMPT_VERSION = "v1"

# ---------------------------------------------------------------------------
# Model selection — sonnet for cost, latest available version.
# ---------------------------------------------------------------------------
MODEL_ID = "claude-sonnet-4-6"

# The system prompt is the cacheable block: identical for every route call.
SYSTEM_PROMPT = """\
You are a transit operations analyst writing concise diagnostic narratives \
for a WMATA bus route performance dashboard. Your output is read by transit \
planners and operations managers who are familiar with scheduling, on-time \
performance, and bus bunching concepts.

Write a 200–300 word narrative interpreting the route's diagnostic profile. \
Structure your analysis as flowing prose (no bullet lists, no headers). Cover:
1. Direction asymmetry — does one direction run significantly earlier or later \
than the other? Is performance balanced or skewed?
2. Key delay zones — which segments accumulate the most slip (positive mean \
slip = bus runs slower than scheduled)? Name them by stop sequence range.
3. Timepoint behavior — summarize the timepoints: which are functioning as \
intended recovery points, which are leaking early, which are underpowered?
4. 2–3 ranked hypotheses explaining the observed pattern, with the evidence \
from the profile that supports each.
5. Suggested intervention class: schedule revision (adjust running times or \
recovery time), hold-down policy (enforce departure discipline at leaky \
timepoints), or infrastructure (TSP / queue jumps at high-slip segments).

Be specific and grounded in the numbers. Avoid generic transit jargon. \
If data is thin (few observations) note that confidence is limited. \
Do not speculate beyond what the data supports.\
"""


def _fmt_min(sec: float | None) -> str:
    """Format seconds as ±X.Xmin for prompt context."""
    if sec is None:
        return "N/A"
    return f"{sec / 60:+.1f}min"


def _build_user_prompt(
    route_id: str,
    period: str,
    seg_rows: list,
    tp_rows: list,
    dir_rows: list,
) -> str:
    """Build the per-route user prompt from the diagnostic profile rows.

    Args:
        route_id: Route identifier.
        period: Time-of-day period key.
        seg_rows: ``RouteDiagnosticSegment`` ORM objects for this route+period.
        tp_rows: ``RouteDiagnosticTimepoint`` ORM objects for this route+period.
        dir_rows: ``RouteDiagnosticDirection`` ORM objects for this route+period.

    Returns:
        User-turn text string ready for the API call.
    """
    lines: list[str] = []
    lines.append(f"Route: {route_id}")
    lines.append(f"Period: {period}")
    lines.append("")

    # Direction asymmetry
    if dir_rows:
        lines.append("=== Direction asymmetry ===")
        for r in sorted(dir_rows, key=lambda x: x.direction_id):
            dir_label = "Outbound (dir 0)" if r.direction_id == 0 else "Inbound (dir 1)"
            lines.append(
                f"  {dir_label}: early={r.early_pct:.1f}%, late={r.late_pct:.1f}%, "
                f"on-time={100 - r.early_pct - r.late_pct:.1f}%, "
                f"signature={r.signature}, n={r.n_observations}"
            )
        lines.append("")
    else:
        lines.append("=== Direction asymmetry ===")
        lines.append("  No direction data available.")
        lines.append("")

    # Segment slip — group by direction
    if seg_rows:
        lines.append("=== Segment slip (positive = bus slower than scheduled) ===")
        by_dir: dict[int, list] = {}
        for r in seg_rows:
            by_dir.setdefault(r.direction_id, []).append(r)
        for dir_id in sorted(by_dir):
            dir_label = "Outbound (dir 0)" if dir_id == 0 else "Inbound (dir 1)"
            lines.append(f"  {dir_label}:")
            # Only consecutive edges (min to_seq per from_seq) for the trajectory.
            segs = sorted(by_dir[dir_id], key=lambda x: (x.from_seq, x.to_seq))
            for r in segs:
                tp_flag = " [timepoint]" if r.is_timepoint else ""
                lines.append(
                    f"    seq {r.from_seq} → {r.to_seq}: "
                    f"mean_slip={_fmt_min(r.mean_slip_sec)}, "
                    f"cum_slip={_fmt_min(r.cum_slip_sec)}, "
                    f"n={r.n_observations}{tp_flag}"
                )
        lines.append("")
    else:
        lines.append("=== Segment slip ===")
        lines.append("  No segment data available.")
        lines.append("")

    # Timepoint behavior
    if tp_rows:
        lines.append("=== Timepoint behavior ===")
        for r in sorted(tp_rows, key=lambda x: (x.direction_id, x.timepoint_stop_id)):
            dir_label = "dir0" if r.direction_id == 0 else "dir1"
            lines.append(
                f"  [{dir_label}] {r.timepoint_stop_id}: classification={r.classification}, "
                f"median_dev_entering={_fmt_min(r.median_dev_entering)}, "
                f"median_dev_leaving={_fmt_min(r.median_dev_leaving)}, "
                f"p10_entering={_fmt_min(r.p10_dev_entering)}, "
                f"p10_leaving={_fmt_min(r.p10_dev_leaving)}, "
                f"n={r.n_observations}"
            )
        lines.append("")
    else:
        lines.append("=== Timepoint behavior ===")
        lines.append("  No timepoint data available.")
        lines.append("")

    lines.append("Write the 200–300 word diagnostic narrative as described in the system prompt.")
    return "\n".join(lines)


def _generate_narrative(
    client,
    route_id: str,
    period: str,
    seg_rows: list,
    tp_rows: list,
    dir_rows: list,
) -> tuple[str, str]:
    """Call Claude and return ``(narrative_text, model_id_used)``.

    Uses prompt caching: the system prompt is marked as cacheable so repeated
    calls across routes share the cached system-prompt tokens.

    Args:
        client: Anthropic client instance.
        route_id: Route identifier.
        period: Time-of-day period key.
        seg_rows: Segment ORM rows for this route+period.
        tp_rows: Timepoint ORM rows for this route+period.
        dir_rows: Direction ORM rows for this route+period.

    Returns:
        Tuple of ``(narrative_text, model_id_used)``.
    """
    user_text = _build_user_prompt(route_id, period, seg_rows, tp_rows, dir_rows)

    response = client.messages.create(
        model=MODEL_ID,
        max_tokens=512,
        system=[
            {
                "type": "text",
                "text": SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        messages=[{"role": "user", "content": user_text}],
    )

    narrative = response.content[0].text.strip()
    return narrative, response.model


def _upsert_narrative(
    db: Session,
    route_id: str,
    period: str,
    narrative: str,
    model_id: str,
    profile_hash: str,
) -> None:
    """Write or overwrite the narrative row for ``(route_id, period)``.

    Args:
        db: Active SQLAlchemy session.
        route_id: Route identifier.
        period: Time-of-day period key.
        narrative: Generated narrative text.
        model_id: Model identifier returned by the API.
        profile_hash: SHA-256 hex of the profile snapshot at generation time.
    """
    existing = (
        db.query(RouteDiagnosisNarrative)
        .filter(
            RouteDiagnosisNarrative.route_id == route_id,
            RouteDiagnosisNarrative.period == period,
        )
        .first()
    )
    if existing is not None:
        existing.narrative = narrative
        existing.generated_at = utcnow_naive()
        existing.model_id = model_id
        existing.prompt_version = PROMPT_VERSION
        existing.profile_snapshot_hash = profile_hash
    else:
        db.add(
            RouteDiagnosisNarrative(
                route_id=route_id,
                period=period,
                narrative=narrative,
                generated_at=utcnow_naive(),
                model_id=model_id,
                prompt_version=PROMPT_VERSION,
                profile_snapshot_hash=profile_hash,
            )
        )
    db.commit()


def _process_route_period(
    db: Session,
    client,
    route_id: str,
    period: str,
    *,
    force: bool,
    dry_run: bool,
) -> None:
    """Generate or skip the narrative for one ``(route_id, period)``.

    Args:
        db: Active SQLAlchemy session.
        client: Anthropic client (or ``None`` in dry-run mode).
        route_id: Route identifier.
        period: Time-of-day period key.
        force: If ``True``, regenerate even when the hash matches.
        dry_run: If ``True``, print the prompt and exit without calling Claude.
    """
    # Load profile rows.
    seg_rows = (
        db.query(RouteDiagnosticSegment)
        .filter(
            RouteDiagnosticSegment.route_id == route_id,
            RouteDiagnosticSegment.period == period,
        )
        .all()
    )
    tp_rows = (
        db.query(RouteDiagnosticTimepoint)
        .filter(
            RouteDiagnosticTimepoint.route_id == route_id,
            RouteDiagnosticTimepoint.period == period,
        )
        .all()
    )
    dir_rows = (
        db.query(RouteDiagnosticDirection)
        .filter(
            RouteDiagnosticDirection.route_id == route_id,
            RouteDiagnosticDirection.period == period,
        )
        .all()
    )

    if not seg_rows and not tp_rows:
        print(f"  {route_id}/{period}: no diagnostic data — skipping.")
        return

    # Compute hash over the raw ORM-row dicts.
    seg_dicts = [
        {
            "direction_id": r.direction_id,
            "from_seq": r.from_seq,
            "from_stop_id": r.from_stop_id,
            "to_seq": r.to_seq,
            "to_stop_id": r.to_stop_id,
            "mean_slip_sec": r.mean_slip_sec,
            "cum_slip_sec": r.cum_slip_sec,
            "n_observations": r.n_observations,
            "is_timepoint": r.is_timepoint,
        }
        for r in seg_rows
    ]
    tp_dicts = [
        {
            "direction_id": r.direction_id,
            "timepoint_stop_id": r.timepoint_stop_id,
            "classification": r.classification,
            "median_dev_entering": r.median_dev_entering,
            "median_dev_leaving": r.median_dev_leaving,
            "p10_dev_entering": r.p10_dev_entering,
            "p10_dev_leaving": r.p10_dev_leaving,
            "n_observations": r.n_observations,
        }
        for r in tp_rows
    ]
    current_hash = compute_profile_hash(seg_dicts, tp_dicts)

    # Check for an existing up-to-date narrative.
    if not force and not dry_run:
        existing = (
            db.query(RouteDiagnosisNarrative)
            .filter(
                RouteDiagnosisNarrative.route_id == route_id,
                RouteDiagnosisNarrative.period == period,
            )
            .first()
        )
        if existing is not None and existing.profile_snapshot_hash == current_hash:
            print(f"  {route_id}/{period}: hash matches — already up-to-date, skipping.")
            return

    if dry_run:
        print(f"\n=== DRY RUN prompt for {route_id}/{period} ===")
        print("--- SYSTEM ---")
        print(SYSTEM_PROMPT)
        print("--- USER ---")
        print(_build_user_prompt(route_id, period, seg_rows, tp_rows, dir_rows))
        return

    print(f"  {route_id}/{period}: generating narrative...", end="", flush=True)
    narrative, model_id_used = _generate_narrative(
        client, route_id, period, seg_rows, tp_rows, dir_rows
    )
    _upsert_narrative(db, route_id, period, narrative, model_id_used, current_hash)
    print(f" done ({len(narrative)} chars, model={model_id_used})")


def main(argv: list[str] | None = None) -> int:
    """Entry point.

    Returns:
        Exit code: 0 on success, 1 on any fatal error.
    """
    parser = argparse.ArgumentParser(
        description="Generate LLM narrative summaries for WMATA route diagnostic profiles."
    )
    route_group = parser.add_mutually_exclusive_group(required=True)
    route_group.add_argument(
        "--route",
        metavar="ROUTE_ID",
        help="Generate narrative for a single route (e.g. D80).",
    )
    route_group.add_argument(
        "--all",
        action="store_true",
        help="Generate narratives for all routes that have materialized diagnostic data.",
    )
    parser.add_argument(
        "--period",
        default="all",
        help=(
            "Time-of-day period (default: all). "
            "One of: all, am_peak, midday, pm_peak, evening, late."
        ),
    )
    parser.add_argument(
        "--all-periods",
        action="store_true",
        help="Generate narratives for all six periods. Overrides --period.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Regenerate even when the stored hash matches the current profile.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the prompt without calling Claude. Useful for inspection.",
    )

    args = parser.parse_args(argv)

    all_periods = ["all", "am_peak", "midday", "pm_peak", "evening", "late"]
    periods_to_run = all_periods if args.all_periods else [args.period]
    if args.period not in all_periods:
        print(
            f"ERROR: --period must be one of: {', '.join(all_periods)}",
            file=sys.stderr,
        )
        return 1

    # Anthropic API key check (skip in dry-run — prompt inspection needs no key).
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key and not args.dry_run:
        print(
            "ERROR: ANTHROPIC_API_KEY is not set. "
            "Set it in the environment or in .env and re-run.\n"
            "Tip: use --dry-run to inspect the prompt without calling Claude.",
            file=sys.stderr,
        )
        return 1

    client = None
    if not args.dry_run:
        try:
            import anthropic  # noqa: PLC0415

            client = anthropic.Anthropic(api_key=api_key)
        except ImportError:
            print(
                "ERROR: The 'anthropic' package is not installed.\nRun: uv sync --extra llm",
                file=sys.stderr,
            )
            return 1

    engine = get_engine()
    # Ensure the narrative table exists (idempotent).
    Base.metadata.create_all(bind=engine, tables=[RouteDiagnosisNarrative.__table__])

    from sqlalchemy.orm import sessionmaker  # noqa: PLC0415

    SessionLocal = sessionmaker(bind=engine)
    db: Session = SessionLocal()

    try:
        # Determine routes to process.
        if args.all:
            # Any route that has at least one row in route_diagnostic_segment.
            route_ids = [
                r
                for (r,) in db.query(RouteDiagnosticSegment.route_id)
                .distinct()
                .order_by(RouteDiagnosticSegment.route_id)
                .all()
            ]
            if not route_ids:
                print(
                    "No routes have materialized diagnostic data. "
                    "Run pipelines/refresh_route_diagnostic_profile.py first.",
                    file=sys.stderr,
                )
                return 1
            print(f"Found {len(route_ids)} route(s) with diagnostic data: {', '.join(route_ids)}")
        else:
            # Validate single route exists in DB.
            route_exists = (
                db.query(Route).filter(Route.route_id == args.route, Route.is_current).first()
            )
            if route_exists is None:
                # Allow proceeding if diagnostic rows exist even if route is no
                # longer current (handles GTFS reload edge cases).
                seg_exists = (
                    db.query(RouteDiagnosticSegment)
                    .filter(RouteDiagnosticSegment.route_id == args.route)
                    .first()
                )
                if seg_exists is None:
                    print(
                        f"ERROR: Route {args.route!r} not found and has no "
                        "diagnostic data. Check the route_id.",
                        file=sys.stderr,
                    )
                    return 1
            route_ids = [args.route]

        for route_id in route_ids:
            for period in periods_to_run:
                _process_route_period(
                    db,
                    client,
                    route_id,
                    period,
                    force=args.force,
                    dry_run=args.dry_run,
                )

    finally:
        db.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
