"""
Migration script to add shapes table to existing database.

This script adds the shapes table and shape_id column to trips table
for existing databases that don't have these yet.

Run this script after updating to the new code with shapes support.
"""

from sqlalchemy import inspect, text

from src.database import get_engine


def check_table_exists(engine, table_name):
    """Check if a table exists in the database"""
    inspector = inspect(engine)
    return table_name in inspector.get_table_names()


def check_column_exists(engine, table_name, column_name):
    """Check if a column exists in a table"""
    inspector = inspect(engine)
    columns = [col["name"] for col in inspector.get_columns(table_name)]
    return column_name in columns


def migrate_shapes(engine):
    """Add shapes table and update trips table"""
    print("=" * 70)
    print("Database Migration: Adding Shapes Support")
    print("=" * 70)

    # Check database type
    db_url = str(engine.url)
    is_sqlite = db_url.startswith("sqlite")

    print(f"\nDatabase: {db_url}")
    print(f"Type: {'SQLite' if is_sqlite else 'PostgreSQL'}")

    with engine.connect() as conn:
        # Check if shapes table exists
        shapes_exists = check_table_exists(engine, "shapes")

        if shapes_exists:
            print("\n✓ shapes table already exists")
        else:
            print("\n→ Creating shapes table...")

            # Create shapes table
            if is_sqlite:
                conn.execute(
                    text("""
                    CREATE TABLE shapes (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        shape_id VARCHAR NOT NULL,
                        shape_pt_lat FLOAT NOT NULL,
                        shape_pt_lon FLOAT NOT NULL,
                        shape_pt_sequence INTEGER NOT NULL,
                        shape_dist_traveled FLOAT,
                        created_at DATETIME
                    )
                """)
                )

                # Create indexes
                conn.execute(
                    text("""
                    CREATE INDEX idx_shape_sequence ON shapes (shape_id, shape_pt_sequence)
                """)
                )
                conn.execute(
                    text("""
                    CREATE INDEX ix_shapes_shape_id ON shapes (shape_id)
                """)
                )

            else:  # PostgreSQL
                conn.execute(
                    text("""
                    CREATE TABLE shapes (
                        id SERIAL PRIMARY KEY,
                        shape_id VARCHAR NOT NULL,
                        shape_pt_lat DOUBLE PRECISION NOT NULL,
                        shape_pt_lon DOUBLE PRECISION NOT NULL,
                        shape_pt_sequence INTEGER NOT NULL,
                        shape_dist_traveled DOUBLE PRECISION,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                )

                # Create indexes
                conn.execute(
                    text("""
                    CREATE INDEX idx_shape_sequence ON shapes (shape_id, shape_pt_sequence)
                """)
                )
                conn.execute(
                    text("""
                    CREATE INDEX ix_shapes_shape_id ON shapes (shape_id)
                """)
                )

            conn.commit()
            print("  ✓ Created shapes table")
            print("  ✓ Created indexes")

        # Check if trips.shape_id column exists
        if check_table_exists(engine, "trips"):
            shape_id_exists = check_column_exists(engine, "trips", "shape_id")
            block_id_exists = check_column_exists(engine, "trips", "block_id")

            if shape_id_exists:
                print("\n✓ trips.shape_id column already exists")
            else:
                print("\n→ Adding shape_id column to trips table...")

                if is_sqlite:
                    conn.execute(
                        text("""
                        ALTER TABLE trips ADD COLUMN shape_id VARCHAR
                    """)
                    )
                    conn.execute(
                        text("""
                        CREATE INDEX ix_trips_shape_id ON trips (shape_id)
                    """)
                    )
                else:  # PostgreSQL
                    conn.execute(
                        text("""
                        ALTER TABLE trips ADD COLUMN shape_id VARCHAR
                    """)
                    )
                    conn.execute(
                        text("""
                        CREATE INDEX ix_trips_shape_id ON trips (shape_id)
                    """)
                    )

                conn.commit()
                print("  ✓ Added shape_id column to trips")
                print("  ✓ Created index on shape_id")

            if block_id_exists:
                print("✓ trips.block_id column already exists")
            else:
                print("→ Adding block_id column to trips table...")

                if is_sqlite:
                    conn.execute(
                        text("""
                        ALTER TABLE trips ADD COLUMN block_id VARCHAR
                    """)
                    )
                    conn.execute(
                        text("""
                        CREATE INDEX ix_trips_block_id ON trips (block_id)
                    """)
                    )
                else:  # PostgreSQL
                    conn.execute(
                        text("""
                        ALTER TABLE trips ADD COLUMN block_id VARCHAR
                    """)
                    )
                    conn.execute(
                        text("""
                        CREATE INDEX ix_trips_block_id ON trips (block_id)
                    """)
                    )

                conn.commit()
                print("  ✓ Added block_id column to trips")
                print("  ✓ Created index on block_id")
        else:
            print("\n⚠ trips table does not exist yet")

    print("\n" + "=" * 70)
    print("Migration Complete!")
    print("=" * 70)
    print("\nNext steps:")
    print("1. Run the GTFS loader to import shapes data:")
    print("   python scripts/init_database.py")
    print("\n2. The loader will automatically import shapes.txt")


def main():
    """Run the migration"""
    engine = get_engine()

    try:
        migrate_shapes(engine)
        print("\n✓ Migration successful!")
    except Exception as e:
        print(f"\n✗ Migration failed: {e}")
        raise
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
