"""
Migration: Add Headway Regularity Metrics

This script adds headway standard deviation and coefficient of variation columns
to support bus bunching detection and service regularity analysis.

New columns added to:
  - route_metrics_daily: headway_std_dev_minutes, headway_cv
  - route_metrics_summary: headway_std_dev_minutes, headway_cv

Run this ONCE before computing metrics with new headway fields.

Usage:
  python scripts/migrate_add_headway_metrics.py
"""

from sqlalchemy import text

from src.database import get_session

# SQL migrations for each table
MIGRATIONS = {
    "route_metrics_daily": """
        ALTER TABLE route_metrics_daily ADD COLUMN headway_std_dev_minutes FLOAT;
        ALTER TABLE route_metrics_daily ADD COLUMN headway_cv FLOAT;
    """,
    "route_metrics_summary": """
        ALTER TABLE route_metrics_summary ADD COLUMN headway_std_dev_minutes FLOAT;
        ALTER TABLE route_metrics_summary ADD COLUMN headway_cv FLOAT;
    """,
}


def run_migrations():
    """Apply all headway metrics migrations"""
    db = get_session()

    try:
        print("=" * 70)
        print("Headway Regularity Metrics Migration")
        print("=" * 70)

        for table_name, migration_sql in MIGRATIONS.items():
            print(f"\nMigrating {table_name}...")
            try:
                for statement in migration_sql.split(";"):
                    statement = statement.strip()
                    if statement:
                        db.execute(text(statement))
                        db.commit()
                print(f"  ✓ {table_name} migrated successfully")
            except Exception as e:
                print(f"  ⚠ {table_name}: {e}")
                # Don't fail on individual table errors (columns might already exist)
                db.rollback()

        print("\n" + "=" * 70)
        print("✓ Migration complete!")
        print("=" * 70)
        print("\nNew columns added:")
        print("  - headway_std_dev_minutes (standard deviation of headways)")
        print("  - headway_cv (coefficient of variation: std_dev / mean)")
        print("\nThese metrics help detect bus bunching:")
        print("  - Lower std dev = more regular service")
        print("  - Higher std dev = more bunching/gaps")
        print("\nTo recompute metrics with new fields:")
        print("  python pipelines/compute_daily_metrics.py --days 7 --recalculate")

    except Exception as e:
        print(f"\n✗ Migration failed: {e}")
        import traceback

        traceback.print_exc()
        raise

    finally:
        db.close()


if __name__ == "__main__":
    run_migrations()
