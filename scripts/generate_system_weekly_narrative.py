"""
Offline CLI: generate an LLM narrative summarizing one week of system-wide
bus performance (system weekly narrative, NOTES-86).

Sibling to ``scripts/generate_route_diagnosis.py`` (PR #141) — same offline
generation / cache-table / read-only-serve pattern, applied to the whole
network's week-over-week trend instead of one route's diagnostic profile.

Reads ``system_metrics_daily`` for the trailing 7-day window ending at
``--as-of`` (default: the latest date with a row) plus the 7-day window
immediately before it for week-over-week deltas, pulls the top OTP- and
EWT-dragging routes from the existing "contributors" view
(``api.aggregations.get_route_contributors``) for concrete grounding, calls
Claude with a structured prompt, and writes the result to
``system_weekly_narrative`` keyed by ``as_of_date``.

**This script calls Claude (via Claude Code) and consumes Max-subscription
quota. It is a build-time tool; the public-facing API never calls Claude.**

Requirements:
  - The ``claude`` CLI on PATH (your Claude Code install). Uses your
    existing Claude Code auth — no separate API key needed.
  - ``system_metrics_daily`` must be populated by
    ``pipelines/run_daily_batch.py`` (via
    ``pipelines/upsert_system_metrics_daily.py``) for the target window.
  - ``system_weekly_narrative`` must exist —
    run ``scripts/migrate_create_system_weekly_narrative.py`` first (also
    run automatically, idempotently, by this script).

Usage::

    # Latest available week
    uv run python scripts/generate_system_weekly_narrative.py

    # A specific week, identified by its last day
    uv run python scripts/generate_system_weekly_narrative.py --as-of 2026-08-16

    # Dry-run: print the prompt without calling Claude
    uv run python scripts/generate_system_weekly_narrative.py --dry-run

    # Force regeneration even if the hash matches (narrative already up-to-date)
    uv run python scripts/generate_system_weekly_narrative.py --force
"""

import argparse
import shutil
import subprocess
import sys
from datetime import date as date_type
from datetime import timedelta

from sqlalchemy import func
from sqlalchemy.orm import Session

from src.database import get_engine
from src.diagnosis_hash import compute_system_snapshot_hash
from src.models import Base, SystemMetricsDaily, SystemWeeklyNarrative
from src.timezones import utcnow_naive

# ---------------------------------------------------------------------------
# Prompt versioning — bump when the prompt text changes so callers can detect
# that cached narratives were generated with an older prompt.
# ---------------------------------------------------------------------------
PROMPT_VERSION = "v1"

# ---------------------------------------------------------------------------
# Model selection — sonnet for cost, latest available version. Mirrors
# scripts/generate_route_diagnosis.py.
# ---------------------------------------------------------------------------
MODEL_ID = "claude-sonnet-4-6"

WEEK_DAYS = 7

# The system prompt is the cacheable block: identical for every call.
SYSTEM_PROMPT = """\
You are a transit analyst writing a short weekly recap for a WMATA bus \
performance dashboard read by a transit-interested member of the public — \
not an operations manager. Your job is to translate metric acronyms (OTP, \
EWT, bunching, service delivered) into what riders actually experienced.

Write a 150-250 word narrative as flowing prose (no bullet lists, no \
headers). Cover:
1. The headline: how did the network perform this week, described in terms \
of rider consequences ("riders on frequent routes waited about a minute \
longer than scheduled") rather than bare metric values.
2. Week-over-week change: is each metric better or worse than the prior \
7-day window, and by how much (use the percentage or absolute deltas \
given)? Call out the most notable move.
3. Where it's concentrated: if specific routes are named as top drags on a \
metric, mention 1-2 of them by their short name as concrete examples — \
riders on THOSE routes felt it most.
4. A one-sentence closing takeaway — is the trend improving, worsening, or \
flat, and should a rider expect this week to look like last week?

Be specific and grounded in the numbers provided. If a day in the window is \
flagged as a partial-collection day, note that the week's figures may be \
thinner for that reason, but do not dwell on it. Do not speculate beyond \
what the data supports. Avoid generic transit jargon; write for a reader \
who knows what "on-time" and "bus bunching" mean intuitively but has never \
heard of "excess wait time" as a term of art — define it in passing if you \
use it.\
"""


def _fmt_pct(val: float | None) -> str:
    """Format a 0-100 percentage value, or 'N/A' when missing."""
    if val is None:
        return "N/A"
    return f"{val:.1f}%"


def _fmt_ratio_pct(val: float | None) -> str:
    """Format a 0-1 fraction as a percentage, or 'N/A' when missing."""
    if val is None:
        return "N/A"
    return f"{val * 100:.1f}%"


def _fmt_sec(val: float | None) -> str:
    """Format a seconds value as Xs, or 'N/A' when missing."""
    if val is None:
        return "N/A"
    return f"{val:.0f}s"


def _row_to_dict(row: SystemMetricsDaily) -> dict:
    """Canonicalize one ``system_metrics_daily`` ORM row to a plain dict.

    Field set matches ``src.diagnosis_hash._canonical_system_metrics_row``
    (which reads a subset of these keys) plus the full set the prompt
    builder needs for per-day detail.
    """
    return {
        "service_date": row.service_date,
        "otp_percentage": row.otp_percentage,
        "service_delivered_ratio": row.service_delivered_ratio,
        "ewt_seconds": row.ewt_seconds,
        "swt_seconds": row.swt_seconds,
        "bunching_rate": row.bunching_rate,
        "data_quality": row.data_quality,
    }


def _fetch_week_rows(db: Session, end_date: date_type, days: int = WEEK_DAYS) -> list[dict]:
    """Return canonicalized ``system_metrics_daily`` rows for the `days`-day
    window ending at (and including) ``end_date``, ordered by service_date.

    Args:
        db: Active SQLAlchemy session.
        end_date: Last date (inclusive) of the window.
        days: Window length in days (default 7).

    Returns:
        List of dicts (one per date with a row present — missing dates are
        simply absent, not padded with nulls).
    """
    start_iso = (end_date - timedelta(days=days - 1)).isoformat()
    end_iso = end_date.isoformat()
    rows = (
        db.query(SystemMetricsDaily)
        .filter(
            SystemMetricsDaily.service_date >= start_iso,
            SystemMetricsDaily.service_date <= end_iso,
        )
        .order_by(SystemMetricsDaily.service_date)
        .all()
    )
    return [_row_to_dict(r) for r in rows]


def _window_mean(rows: list[dict], field: str) -> float | None:
    """Mean of ``field`` across ``rows``, skipping nulls and partial-quality days.

    Mirrors the averaging convention in ``api.aggregations._system_baseline_for_window``
    (mean of complete-quality, non-null daily values) without importing that
    private helper across the scripts/api boundary.

    Args:
        rows: Canonicalized system_metrics_daily dicts.
        field: One of ``otp_percentage``, ``service_delivered_ratio``,
            ``ewt_seconds``, ``bunching_rate``.

    Returns:
        Mean value, or ``None`` when no complete-quality row has a non-null value.
    """
    values = [r[field] for r in rows if r["data_quality"] == "complete" and r[field] is not None]
    if not values:
        return None
    return sum(values) / len(values)


def _pct_delta(current: float | None, prior: float | None) -> str:
    """Format the percentage change from ``prior`` to ``current``, or 'N/A'."""
    if current is None or prior is None or prior == 0:
        return "N/A"
    return f"{(current - prior) / abs(prior) * 100:+.1f}%"


def _format_contributors(contributors_payload: dict, metric_label: str, top_n: int = 3) -> str:
    """Render the top-N contributor routes for one metric as prompt text.

    Args:
        contributors_payload: Return value of
            ``api.aggregations.get_route_contributors`` for one metric.
        metric_label: Human label used in the header line (e.g. ``"OTP"``).
        top_n: How many top-ranked (biggest-drag) routes to include.

    Returns:
        Multi-line string, or a "no data" line when the contributors list is empty.
    """
    contributors = (contributors_payload or {}).get("contributors") or []
    if not contributors:
        return f"  No {metric_label} contributor data available this week."
    lines = []
    for c in contributors[:top_n]:
        name = c.get("route_short_name") or c["route_id"]
        lines.append(
            f"  {name}: route_value={c['route_value']:.2f}, "
            f"reference={c['reference_value']:.2f} ({c['reference_source']})"
        )
    return "\n".join(lines)


def _build_user_prompt(
    as_of_date: date_type,
    current_week_rows: list[dict],
    prior_week_rows: list[dict],
    otp_contributors: dict,
    ewt_contributors: dict,
) -> str:
    """Build the user-turn prompt from the weekly system snapshot.

    Args:
        as_of_date: Last day (inclusive) of the current 7-day window.
        current_week_rows: Canonicalized system_metrics_daily rows for the
            current week.
        prior_week_rows: Canonicalized system_metrics_daily rows for the
            week immediately before.
        otp_contributors: ``get_route_contributors(db, metric="otp", days=7)`` result.
        ewt_contributors: ``get_route_contributors(db, metric="ewt", days=7)`` result.

    Returns:
        User-turn text string ready for the API call.
    """
    lines: list[str] = []
    lines.append(f"Week ending: {as_of_date.isoformat()}")
    lines.append("")

    cur_otp = _window_mean(current_week_rows, "otp_percentage")
    cur_sd = _window_mean(current_week_rows, "service_delivered_ratio")
    cur_ewt = _window_mean(current_week_rows, "ewt_seconds")
    cur_bun = _window_mean(current_week_rows, "bunching_rate")

    prior_otp = _window_mean(prior_week_rows, "otp_percentage")
    prior_sd = _window_mean(prior_week_rows, "service_delivered_ratio")
    prior_ewt = _window_mean(prior_week_rows, "ewt_seconds")
    prior_bun = _window_mean(prior_week_rows, "bunching_rate")

    lines.append("=== Current week (7-day mean) vs prior week ===")
    lines.append(
        f"  On-time performance: {_fmt_pct(cur_otp)} "
        f"(prior week {_fmt_pct(prior_otp)}, change {_pct_delta(cur_otp, prior_otp)})"
    )
    lines.append(
        f"  Service delivered: {_fmt_ratio_pct(cur_sd)} "
        f"(prior week {_fmt_ratio_pct(prior_sd)}, change {_pct_delta(cur_sd, prior_sd)})"
    )
    lines.append(
        f"  Excess wait time (extra time riders on frequent routes wait beyond "
        f"scheduled headway): {_fmt_sec(cur_ewt)} "
        f"(prior week {_fmt_sec(prior_ewt)}, change {_pct_delta(cur_ewt, prior_ewt)})"
    )
    lines.append(
        f"  Bunching rate (share of consecutive-bus pairs arriving abnormally "
        f"close together): {_fmt_ratio_pct(cur_bun)} "
        f"(prior week {_fmt_ratio_pct(prior_bun)}, change {_pct_delta(cur_bun, prior_bun)})"
    )
    lines.append("")

    lines.append("=== Daily detail, current week ===")
    for r in current_week_rows:
        flag = " [partial data collection]" if r["data_quality"] == "partial" else ""
        lines.append(
            f"  {r['service_date']}: OTP={_fmt_pct(r['otp_percentage'])}, "
            f"service_delivered={_fmt_ratio_pct(r['service_delivered_ratio'])}, "
            f"EWT={_fmt_sec(r['ewt_seconds'])}, "
            f"bunching={_fmt_ratio_pct(r['bunching_rate'])}{flag}"
        )
    if not current_week_rows:
        lines.append("  No data.")
    lines.append("")

    lines.append(
        "=== Biggest OTP-dragging routes this week (system baseline or per-route target) ==="
    )
    lines.append(_format_contributors(otp_contributors, "OTP"))
    lines.append("")
    lines.append("=== Biggest EWT-dragging routes this week ===")
    lines.append(_format_contributors(ewt_contributors, "EWT"))
    lines.append("")

    lines.append("Write the 150-250 word weekly narrative as described in the system prompt.")
    return "\n".join(lines)


def _generate_narrative(
    as_of_date: date_type,
    current_week_rows: list[dict],
    prior_week_rows: list[dict],
    otp_contributors: dict,
    ewt_contributors: dict,
) -> tuple[str, str]:
    """Call Claude via ``claude -p`` subprocess and return ``(narrative_text, model_id_used)``.

    Invokes the ``claude`` CLI so the script reuses the existing Claude Code
    OAuth / keychain auth rather than requiring a separate ANTHROPIC_API_KEY.
    Mirrors ``scripts/generate_route_diagnosis.py:_generate_narrative`` exactly.

    Returns:
        Tuple of ``(narrative_text, model_id_used)``.

    Raises:
        SystemExit: If the ``claude`` subprocess exits with a non-zero status.
    """
    user_text = _build_user_prompt(
        as_of_date, current_week_rows, prior_week_rows, otp_contributors, ewt_contributors
    )

    result = subprocess.run(
        [
            "claude",
            "-p",
            user_text,
            "--system-prompt",
            SYSTEM_PROMPT,
            "--model",
            MODEL_ID,
            "--tools",
            "",
            "--disable-slash-commands",
            "--output-format",
            "text",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    if result.returncode != 0:
        error_detail = result.stderr.strip() if result.stderr.strip() else "(no stderr output)"
        print(
            f"\nERROR: `claude -p` exited with code {result.returncode}.\n{error_detail}",
            file=sys.stderr,
        )
        sys.exit(result.returncode)

    narrative = result.stdout.strip()
    return narrative, MODEL_ID


def _upsert_narrative(
    db: Session,
    as_of_date: date_type,
    narrative: str,
    model_id: str,
    snapshot_hash: str,
) -> None:
    """Write or overwrite the narrative row for ``as_of_date``.

    Args:
        db: Active SQLAlchemy session.
        as_of_date: Last day of the narrative's 7-day window.
        narrative: Generated narrative text.
        model_id: Model identifier returned by the API.
        snapshot_hash: SHA-256 hex of the system_metrics_daily snapshot at
            generation time.
    """
    as_of_iso = as_of_date.isoformat()
    existing = (
        db.query(SystemWeeklyNarrative)
        .filter(SystemWeeklyNarrative.as_of_date == as_of_iso)
        .first()
    )
    if existing is not None:
        existing.narrative = narrative
        existing.generated_at = utcnow_naive()
        existing.model_id = model_id
        existing.prompt_version = PROMPT_VERSION
        existing.metrics_snapshot_hash = snapshot_hash
    else:
        db.add(
            SystemWeeklyNarrative(
                as_of_date=as_of_iso,
                narrative=narrative,
                generated_at=utcnow_naive(),
                model_id=model_id,
                prompt_version=PROMPT_VERSION,
                metrics_snapshot_hash=snapshot_hash,
            )
        )
    db.commit()


def _process_week(
    db: Session,
    as_of_date: date_type,
    *,
    force: bool,
    dry_run: bool,
) -> None:
    """Generate or skip the narrative for the 7-day window ending at ``as_of_date``.

    Args:
        db: Active SQLAlchemy session.
        as_of_date: Last day of the window.
        force: If ``True``, regenerate even when the hash matches.
        dry_run: If ``True``, print the prompt and exit without calling Claude.
    """
    from api.aggregations import get_route_contributors  # noqa: PLC0415

    current_week_rows = _fetch_week_rows(db, as_of_date, WEEK_DAYS)
    prior_end = as_of_date - timedelta(days=WEEK_DAYS)
    prior_week_rows = _fetch_week_rows(db, prior_end, WEEK_DAYS)

    if not current_week_rows:
        print(f"  {as_of_date.isoformat()}: no system_metrics_daily rows in window — skipping.")
        return

    current_hash = compute_system_snapshot_hash(current_week_rows, prior_week_rows)

    if not force and not dry_run:
        existing = (
            db.query(SystemWeeklyNarrative)
            .filter(SystemWeeklyNarrative.as_of_date == as_of_date.isoformat())
            .first()
        )
        if existing is not None and existing.metrics_snapshot_hash == current_hash:
            print(f"  {as_of_date.isoformat()}: hash matches — already up-to-date, skipping.")
            return

    otp_contributors = get_route_contributors(db, metric="otp", days=WEEK_DAYS)
    ewt_contributors = get_route_contributors(db, metric="ewt", days=WEEK_DAYS)

    if dry_run:
        print(f"\n=== DRY RUN prompt for week ending {as_of_date.isoformat()} ===")
        print("--- SYSTEM ---")
        print(SYSTEM_PROMPT)
        print("--- USER ---")
        print(
            _build_user_prompt(
                as_of_date, current_week_rows, prior_week_rows, otp_contributors, ewt_contributors
            )
        )
        return

    print(f"  {as_of_date.isoformat()}: generating narrative...", end="", flush=True)
    narrative, model_id_used = _generate_narrative(
        as_of_date, current_week_rows, prior_week_rows, otp_contributors, ewt_contributors
    )
    _upsert_narrative(db, as_of_date, narrative, model_id_used, current_hash)
    print(f" done ({len(narrative)} chars, model={model_id_used})")


def main(argv: list[str] | None = None) -> int:
    """Entry point.

    Returns:
        Exit code: 0 on success, 1 on any fatal error.
    """
    parser = argparse.ArgumentParser(
        description="Generate an LLM weekly narrative for WMATA system-wide performance."
    )
    parser.add_argument(
        "--as-of",
        metavar="YYYY-MM-DD",
        default=None,
        help="Last day of the 7-day window to summarize (default: latest date with data).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Regenerate even when the stored hash matches the current snapshot.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the prompt without calling Claude. Useful for inspection.",
    )

    args = parser.parse_args(argv)

    # claude CLI check (skip in dry-run — prompt inspection needs no CLI call).
    if not args.dry_run and not shutil.which("claude"):
        print(
            "ERROR: 'claude' is not on PATH. "
            "Install Claude Code (https://claude.ai/code) and re-run.\n"
            "Tip: use --dry-run to inspect the prompt without calling Claude.",
            file=sys.stderr,
        )
        return 1

    engine = get_engine()
    # Ensure the narrative table exists (idempotent).
    Base.metadata.create_all(bind=engine, tables=[SystemWeeklyNarrative.__table__])

    from sqlalchemy.orm import sessionmaker  # noqa: PLC0415

    SessionLocal = sessionmaker(bind=engine)
    db: Session = SessionLocal()

    try:
        if args.as_of:
            try:
                as_of_date = date_type.fromisoformat(args.as_of)
            except ValueError:
                print(f"ERROR: --as-of must be YYYY-MM-DD, got {args.as_of!r}", file=sys.stderr)
                return 1
        else:
            latest = db.query(func.max(SystemMetricsDaily.service_date)).scalar()
            if latest is None:
                print(
                    "ERROR: system_metrics_daily is empty. Run pipelines/run_daily_batch.py first.",
                    file=sys.stderr,
                )
                return 1
            as_of_date = date_type.fromisoformat(latest)

        _process_week(db, as_of_date, force=args.force, dry_run=args.dry_run)

    finally:
        db.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
