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
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


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
        module_name = f"_migrate_all__{path.stem}"
        spec = importlib.util.spec_from_file_location(module_name, path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Could not load module spec for {path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        if not hasattr(module, "main"):
            raise RuntimeError(f"{path.name} has no main() entry point")
        module.main()
        print(f"<== Finished {path.name}\n")

    print(f"All {len(migrations)} migration(s) completed.")


if __name__ == "__main__":
    main()
