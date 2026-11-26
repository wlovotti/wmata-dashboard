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
import hashlib
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


# Config files read while generating (modules are discovered from sys.modules).
CONFIG_FILES = (
    "config/scorecard_annotations.yaml",
    "config/frequent_routes.yaml",
    "config/agencies/wmata.yaml",
    "config/agencies/sfmta.yaml",
)


def _source_files() -> list[str]:
    """Repo-relative paths of every repository module currently loaded, plus `CONFIG_FILES`.

    Called after the scorecard is built, so every module that computed a
    number (``src/``, ``api/``, this script) has been imported.
    """
    paths = set(CONFIG_FILES)
    for module in list(sys.modules.values()):
        f = getattr(module, "__file__", None)
        if not f:
            continue
        try:
            rel = Path(f).resolve().relative_to(REPO_ROOT)
        except ValueError:
            continue
        if rel.parts and rel.parts[0] in ("src", "api", "scripts", "pipelines"):
            paths.add(rel.as_posix())
    return sorted(p for p in paths if (REPO_ROOT / p).exists())


def _git(*args: str) -> str | None:
    """Run a git command in the repo and return stdout, or None if git is unavailable."""
    try:
        return subprocess.run(
            ["git", *args], cwd=REPO_ROOT, capture_output=True, text=True, check=True
        ).stdout
    except (OSError, subprocess.CalledProcessError):
        return None


def build_provenance(out_dir: Path) -> dict:
    """Record what generated the report: commit, uncommitted paths, source hashes.

    ``git_dirty_paths`` lists paths with uncommitted changes, ignoring the
    output directory (whose files are being regenerated); None when git is
    unavailable. ``source_sha256`` hashes every loaded repository module and
    the config files read (`_source_files`); ``source_digest`` is the sha256
    of the sorted ``path:hash`` lines, so the generating code stays
    identifiable if the commit becomes unreachable (e.g. after a squash
    merge). Library versions and database contents are not covered.
    """
    commit = (_git("rev-parse", "HEAD") or "").strip() or None
    porcelain = _git("status", "--porcelain")
    dirty = None
    if porcelain is not None:
        try:
            out_rel = out_dir.resolve().relative_to(REPO_ROOT).as_posix().rstrip("/") + "/"
        except ValueError:
            out_rel = None
        dirty = []
        for line in porcelain.splitlines():
            path = line[3:].strip()
            if out_rel and (path + "/").startswith(out_rel):
                continue
            dirty.append(path)
    hashes = {
        rel: hashlib.sha256((REPO_ROOT / rel).read_bytes()).hexdigest() for rel in _source_files()
    }
    digest = hashlib.sha256(
        "".join(f"{rel}:{h}\n" for rel, h in sorted(hashes.items())).encode()
    ).hexdigest()
    return {
        "git_commit": commit,
        "git_dirty_paths": dirty,
        "source_sha256": hashes,
        "source_digest": digest,
    }


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
        )
    finally:
        for session in sessions.values():
            session.rollback()
            session.close()

    scorecard["provenance"] = build_provenance(args.out)
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
