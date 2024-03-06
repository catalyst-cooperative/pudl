#! /usr/bin/env python
"""A naive script for converting SQLite to DuckDB."""

import logging
from pathlib import Path

import click
import duckdb

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@click.command()
@click.argument("sqlite_path", type=click.Path(exists=True, resolve_path=True))
@click.argument(
    "duckdb_path", type=click.Path(resolve_path=True, writable=True, allow_dash=False)
)
def convert_sqlite_to_duckdb(sqlite_path, duckdb_path):
    """Convert an SQLite database to DuckDB format.

    Args:
        sqlite_path (str): Path to the existing SQLite database file.
        duckdb_path (str): Path to the new DuckDB database file (should not exist).

    Example:
        python sqlite_to_duckdb.py sqlite.db duckdb.db
    """
    sqlite_path = Path(sqlite_path)
    duckdb_path = Path(duckdb_path)

    # Check if DuckDB file already exists
    if duckdb_path.exists():
        click.echo(
            f"Error: DuckDB file '{duckdb_path}' already exists. Please provide a new filename."
        )
        return

    # Connect to DuckDB database
    duckdb_conn = duckdb.connect(database=str(duckdb_path))
    duckdb_cursor = duckdb_conn.cursor()

    # Fetch table names from SQLite database using DuckDB
    duckdb_cursor.execute(f"ATTACH DATABASE '{sqlite_path}' AS sqlite_db;")
    duckdb_cursor.execute("SELECT name FROM main.sqlite_master WHERE type='table';")
    table_names = [row[0] for row in duckdb_cursor.fetchall()]

    # Copy tables from SQLite to DuckDB
    for table_name in table_names:
        logger.info(f"Working on table: {table_name}")
        # Fetch column names and types from SQLite table using DuckDB
        duckdb_cursor.execute(f"PRAGMA table_info(sqlite_db.{table_name});")
        columns_info = duckdb_cursor.fetchall()
        column_definitions = ", ".join([f"{col[1]} {col[2]}" for col in columns_info])

        # Create equivalent table in DuckDB
        duckdb_cursor.execute(f"CREATE TABLE {table_name} ({column_definitions});")

        # Copy data from SQLite to DuckDB using DuckDB
        duckdb_cursor.execute(
            f"INSERT INTO {table_name} SELECT * FROM sqlite_db.{table_name};"  # noqa: S608
        )

    # Commit and close connections
    duckdb_conn.commit()
    duckdb_conn.close()


if __name__ == "__main__":
    convert_sqlite_to_duckdb()
