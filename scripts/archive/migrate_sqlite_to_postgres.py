"""
Migrate collected vehicle position data from SQLite to PostgreSQL

This script migrates:
- vehicle_positions (all collected real-time data)
- route_metrics_daily (computed daily metrics)
- route_metrics_summary (aggregated summaries)

Note: Static GTFS data (routes, stops, trips, etc.) will be reloaded
via init_database.py to ensure consistency.
"""

import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models import RouteMetricsDaily, RouteMetricsSummary, VehiclePosition

# Load environment variables
load_dotenv()


def migrate_data():
    """Migrate collected data from SQLite to PostgreSQL"""

    # Source database (SQLite)
    sqlite_url = "sqlite:///./wmata_dashboard.db"
    sqlite_engine = create_engine(sqlite_url, connect_args={"check_same_thread": False})
    SqliteSession = sessionmaker(bind=sqlite_engine)
    sqlite_session = SqliteSession()

    # Destination database (PostgreSQL from environment)
    postgres_url = os.getenv("DATABASE_URL")
    if not postgres_url or postgres_url.startswith("sqlite"):
        print("ERROR: DATABASE_URL must be set to a PostgreSQL connection string")
        print("Example: DATABASE_URL=postgresql://localhost/wmata_dashboard")
        return False

    postgres_engine = create_engine(postgres_url, pool_pre_ping=True)
    PostgresSession = sessionmaker(bind=postgres_engine)
    postgres_session = PostgresSession()

    print("=" * 80)
    print("SQLite to PostgreSQL Migration")
    print("=" * 80)
    print(f"\nSource (SQLite): {sqlite_url}")
    print(f"Destination (PostgreSQL): {postgres_url}")
    print()

    try:
        # 1. Migrate vehicle_positions
        print("=" * 80)
        print("Migrating vehicle_positions...")
        print("=" * 80)

        total_positions = sqlite_session.query(VehiclePosition).count()
        print(f"Total vehicle positions to migrate: {total_positions:,}")

        if total_positions > 0:
            batch_size = 1000
            migrated = 0

            # Query in batches
            for offset in range(0, total_positions, batch_size):
                positions = (
                    sqlite_session.query(VehiclePosition)
                    .order_by(VehiclePosition.id)
                    .limit(batch_size)
                    .offset(offset)
                    .all()
                )

                # Convert to dicts for bulk insert
                position_dicts = []
                for pos in positions:
                    position_dict = {
                        "timestamp": pos.timestamp,
                        "vehicle_id": pos.vehicle_id,
                        "route_id": pos.route_id,
                        "trip_id": pos.trip_id,
                        "latitude": pos.latitude,
                        "longitude": pos.longitude,
                        "speed": pos.speed,
                        "bearing": pos.bearing,
                        "stop_id": pos.stop_id,
                        "current_status": pos.current_status,
                        "current_stop_sequence": pos.current_stop_sequence,
                        "direction_id": pos.direction_id,
                        "occupancy_status": pos.occupancy_status,
                        # Only include fields that exist in the SQLite schema
                        "stop_id_assigned": getattr(pos, "stop_id_assigned", None),
                        "distance_from_stop": getattr(pos, "distance_from_stop", None),
                        "diff_seconds": getattr(pos, "diff_seconds", None),
                    }
                    position_dicts.append(position_dict)

                # Bulk insert
                postgres_session.bulk_insert_mappings(VehiclePosition, position_dicts)
                postgres_session.commit()

                migrated += len(positions)
                print(
                    f"  Progress: {migrated:,}/{total_positions:,} "
                    f"({migrated / total_positions * 100:.1f}%)"
                )

            print(f"✓ Migrated {migrated:,} vehicle positions")
        else:
            print("  No vehicle positions to migrate")

        # 2. Migrate route_metrics_daily
        print("\n" + "=" * 80)
        print("Migrating route_metrics_daily...")
        print("=" * 80)

        total_daily_metrics = sqlite_session.query(RouteMetricsDaily).count()
        print(f"Total daily metrics to migrate: {total_daily_metrics:,}")

        if total_daily_metrics > 0:
            daily_metrics = sqlite_session.query(RouteMetricsDaily).all()

            daily_dicts = []
            for metric in daily_metrics:
                metric_dict = {
                    "route_id": metric.route_id,
                    "date": metric.date,
                    "otp_percentage": getattr(metric, "otp_percentage", None),
                    "early_percentage": getattr(metric, "early_percentage", None),
                    "late_percentage": getattr(metric, "late_percentage", None),
                    "avg_headway_minutes": getattr(metric, "avg_headway_minutes", None),
                    "headway_std_dev_minutes": getattr(metric, "headway_std_dev_minutes", None),
                    "headway_cv": getattr(metric, "headway_cv", None),
                    "avg_speed_mph": getattr(metric, "avg_speed_mph", None),
                    "total_arrivals": getattr(metric, "total_arrivals", None),
                    "total_positions": getattr(metric, "total_positions", None),
                    "data_updated_at": getattr(metric, "data_updated_at", None),
                }
                daily_dicts.append(metric_dict)

            postgres_session.bulk_insert_mappings(RouteMetricsDaily, daily_dicts)
            postgres_session.commit()

            print(f"✓ Migrated {total_daily_metrics:,} daily metrics")
        else:
            print("  No daily metrics to migrate")

        # 3. Migrate route_metrics_summary
        print("\n" + "=" * 80)
        print("Migrating route_metrics_summary...")
        print("=" * 80)

        total_summaries = sqlite_session.query(RouteMetricsSummary).count()
        print(f"Total summary metrics to migrate: {total_summaries:,}")

        if total_summaries > 0:
            summaries = sqlite_session.query(RouteMetricsSummary).all()

            summary_dicts = []
            for summary in summaries:
                summary_dict = {
                    "route_id": summary.route_id,
                    "otp_percentage": getattr(summary, "otp_percentage", None),
                    "early_percentage": getattr(summary, "early_percentage", None),
                    "late_percentage": getattr(summary, "late_percentage", None),
                    "avg_headway_minutes": getattr(summary, "avg_headway_minutes", None),
                    "headway_std_dev_minutes": getattr(summary, "headway_std_dev_minutes", None),
                    "headway_cv": getattr(summary, "headway_cv", None),
                    "avg_speed_mph": getattr(summary, "avg_speed_mph", None),
                    "total_arrivals": getattr(summary, "total_arrivals", None),
                    "total_positions": getattr(summary, "total_positions", None),
                    "data_updated_at": getattr(summary, "data_updated_at", None),
                }
                summary_dicts.append(summary_dict)

            postgres_session.bulk_insert_mappings(RouteMetricsSummary, summary_dicts)
            postgres_session.commit()

            print(f"✓ Migrated {total_summaries:,} summary metrics")
        else:
            print("  No summary metrics to migrate")

        # Summary
        print("\n" + "=" * 80)
        print("Migration Summary")
        print("=" * 80)
        print(f"✓ Vehicle positions: {total_positions:,}")
        print(f"✓ Daily metrics: {total_daily_metrics:,}")
        print(f"✓ Summary metrics: {total_summaries:,}")
        print("\nMigration completed successfully!")
        print("=" * 80)

        return True

    except Exception as e:
        print(f"\n❌ ERROR during migration: {e}")
        postgres_session.rollback()
        return False

    finally:
        sqlite_session.close()
        postgres_session.close()


if __name__ == "__main__":
    success = migrate_data()
    exit(0 if success else 1)
