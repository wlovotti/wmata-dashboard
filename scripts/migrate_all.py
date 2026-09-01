"""
Run every `scripts/migrate_*.py` migration in alphabetical order.

Auto-discovers sibling files matching `migrate_*.py` (excluding this one),
imports each as a module, and invokes its `main()` entry point. Existing
migrations are idempotent — they use `ADD COLUMN IF NOT EXISTS` and
re-runnable backfills — so re-execution is a no-op once a column has
landed.

Used by CI's Postgres lane to bring a freshly initialized database up to
the current model schema before `scripts/check_schema_drift.py` runs. Also
safe to run by hand against a live database after pulling a branch that
adds a migration.

Usage:
  uv run python scripts/migrate_all.py

migrate_all.py accepts no passthrough arguments of its own. Any argv
given to it (e.g. flags meant for one specific sibling) is not forwarded
to the sibling scripts it runs — each sibling's main() sees only its own
path as argv, isolated from migrate_all's.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _run_sibling(path: Path) -> None:
    """Load one migrate_*.py sibling and invoke its main(), isolated from our own argv.

    Sibling scripts call ``argparse.ArgumentParser().parse_args()`` with no
    explicit argv, which defaults to reading ``sys.argv[1:]``. Without
    isolation, any flag passed to ``migrate_all.py`` itself (e.g. ``--yes``)
    would pass straight through to *every* sibling's own parser — including
    destructive migrations that happen to define the same flag name (the
    migrate_all argv-isolation fix, PR #230). Reset ``sys.argv`` to just
    the sibling's own path for the duration of its ``main()``, and always
    restore migrate_all's original argv afterward (even if the sibling
    raises) so later siblings, and the caller, are unaffected.

    Args:
        path: Filesystem path to the migrate_*.py sibling to load and run.

    Raises:
        RuntimeError: If the module spec can't be loaded, or the module has
            no ``main()`` entry point.
    """
    module_name = f"_migrate_all__{path.stem}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module spec for {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    if not hasattr(module, "main"):
        raise RuntimeError(f"{path.name} has no main() entry point")

    original_argv = sys.argv
    try:
        sys.argv = [str(path)]
        module.main()
    finally:
        sys.argv = original_argv


def main() -> None:
    """Discover migrate_*.py siblings and invoke each module's main()."""
    scripts_dir = Path(__file__).resolve().parent
    self_name = Path(__file__).name
    migrations = sorted(p for p in scripts_dir.glob("migrate_*.py") if p.name != self_name)

    if not migrations:
        print("No migrations found.")
        return

    print(f"Discovered {len(migrations)} migration(s):")
    for path in migrations:
        print(f"  - {path.name}")
    print()

    for path in migrations:
        print(f"==> Running {path.name}")
        _run_sibling(path)
        print(f"<== Finished {path.name}\n")

    print(f"All {len(migrations)} migration(s) completed.")


if __name__ == "__main__":
    main()
