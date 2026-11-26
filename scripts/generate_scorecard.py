"""
Generate a period scorecard: Markdown report + JSON of every number (issue #267).

Read-only. Opens each agency's database with Postgres
``default_transaction_read_only=on`` so the server itself rejects any
write, and reads only the kept-forever daily aggregate tables
(``system_metrics_daily``, ``route_metrics_daily_overlay``) plus the
versioned GTFS tables. See ``src/scorecard.py`` for the method.

WMATA is the deep subject by default (system KPIs, day-type split, best and
worst routes); each ``--compare`` agency gets the same headline KPIs from
the same code path, with comparability caveats and no route detail.

Usage::

    uv run python scripts/generate_scorecard.py --start 2026-10-01 --end 2026-10-31
    uv run python scripts/generate_scorecard.py --start 2026-10-01 --end 2026-10-31 \\
        --agency wmata --compare sfmta --out reports/
    uv run python scripts/generate_scorecard.py --start 2026-10-01 --end 2026-10-31 --no-compare

Outputs ``<out>/scorecard_<agency>_<start>_<end>.md`` and ``.json``.
Database URLs come from each agency's configured env var
(``DATABASE_URL`` for WMATA, ``SFMTA_DATABASE_URL`` for SFMTA).
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import date as date_type
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from sqlalchemy import create_engine  # noqa: E402
from sqlalchemy.orm import Session, sessionmaker  # noqa: E402

from src.agency_config import load_agency_config, resolve_agency_db_url  # noqa: E402
from src.scorecard import build_scorecard, render_markdown  # noqa: E402
from src.timezones import local_today, utcnow_naive  # noqa: E402


def _parse_date(value: str) -> date_type:
    """Parse YYYY-MM-DD for argparse."""
    return datetime.strptime(value, "%Y-%m-%d").date()


def open_read_only_session(db_url: str) -> Session:
    """Open a session whose transactions the server forces read-only (Postgres).

    For a Postgres URL the connection sets ``default_transaction_read_only``
    so any INSERT / UPDATE / DELETE / DDL fails at the server. Other URLs
    (tests with SQLite) get a plain session.
    """
    connect_args = {}
    if db_url.startswith("postgres"):
        connect_args["options"] = "-c default_transaction_read_only=on"
    engine = create_engine(db_url, connect_args=connect_args)
    return sessionmaker(bind=engine)()


def _git_commit() -> str | None:
    """Return the current git commit hash, or None outside a git checkout."""
    try:
        return (
            subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                check=True,
            ).stdout.strip()
            or None
        )
    except (OSError, subprocess.CalledProcessError):
        return None


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--start", type=_parse_date, required=True, help="First service date")
    parser.add_argument("--end", type=_parse_date, required=True, help="Last service date")
    parser.add_argument("--agency", default="wmata", help="Deep subject agency (default: wmata)")
    parser.add_argument(
        "--compare",
        action="append",
        default=None,
        help="Peer agency for headline comparison; repeatable (default: sfmta)",
    )
    parser.add_argument(
        "--no-compare", action="store_true", help="Subject agency only, no peer columns"
    )
    parser.add_argument("--out", type=Path, default=REPO_ROOT / "reports", help="Output directory")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Generate the scorecard files; return a process exit code."""
    args = parse_args(argv)
    if args.end < args.start:
        print(f"--end {args.end} is before --start {args.start}", file=sys.stderr)
        return 2

    peers = [] if args.no_compare else (args.compare or ["sfmta"])
    agencies = [args.agency] + [p for p in peers if p != args.agency]

    sessions: dict[str, Session] = {}
    try:
        for agency in agencies:
            cfg = load_agency_config(agency)
            today = local_today(cfg.timezone)
            if args.end >= today:
                print(
                    f"warning: period end {args.end} is not before {cfg.display_name}'s "
                    f"local today ({today}); later dates will show as missing",
                    file=sys.stderr,
                )
            db_url = resolve_agency_db_url(cfg)
            if not db_url:
                print(f"{cfg.database_url_env} is not set", file=sys.stderr)
                return 2
            sessions[agency] = open_read_only_session(db_url)

        scorecard = build_scorecard(
            sessions,
            args.start,
            args.end,
            subject=args.agency,
            generated_at_utc=utcnow_naive(),
            git_commit=_git_commit(),
        )
    finally:
        for session in sessions.values():
            session.rollback()
            session.close()

    args.out.mkdir(parents=True, exist_ok=True)
    stem = f"scorecard_{args.agency}_{args.start}_{args.end}"
    json_path = args.out / f"{stem}.json"
    md_path = args.out / f"{stem}.md"
    json_path.write_text(json.dumps(scorecard, indent=2, sort_keys=False) + "\n")
    md_path.write_text(render_markdown(scorecard))
    print(f"wrote {md_path}")
    print(f"wrote {json_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
