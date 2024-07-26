#! /usr/bin/env python
"""Script that creates a DuckDB database from a collection of PUDL Parquet files."""

import logging
from pathlib import Path

import click
import sqlalchemy as sa

from pudl.metadata import PUDL_PACKAGE
from pudl.metadata.classes import Package

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@click.command()
@click.argument("parquet_dir", type=click.Path(exists=True, resolve_path=True))
@click.argument(
    "duckdb_path", type=click.Path(resolve_path=True, writable=True, allow_dash=False)
)
def convert_parquet_to_duckdb(parquet_dir: str, duckdb_path: str):
    """Convert a directory of Parquet files to a DuckDB database.

    Args:
        parquet_dir: Path to a directory of parquet files.
        duckdb_path: Path to the new DuckDB database file (should not exist).

    Example:
        python parquet_to_duckdb.py /path/to/parquet/directory duckdb.db
    """
    parquet_dir = Path(parquet_dir)
    duckdb_path = Path(duckdb_path)

    # Check if DuckDB file already exists
    if duckdb_path.exists():
        click.echo(
            f"Error: DuckDB file '{duckdb_path}' already exists. Please provide a new filename."
        )
        return

    # create duck db schema from pudl package
    resource_ids = (r.name for r in PUDL_PACKAGE.resources if len(r.name) <= 63)
    package = Package.from_resource_ids(resource_ids)

    metadata = package.to_sql(dialect="duckdb")
    engine = sa.create_engine(f"duckdb:///{duckdb_path}")
    metadata.create_all(engine)

    # iterate through the parquet files and add them to the db
    # throw an error if there is a missing paruqet file or an extra one

    # # Fetch table names from SQLite database using DuckDB
    # duckdb_cursor.execute(f"ATTACH DATABASE '{sqlite_path}' AS sqlite_db;")
    # duckdb_cursor.execute("SELECT name FROM main.sqlite_master WHERE type='table';")
    # table_names = [row[0] for row in duckdb_cursor.fetchall()]

    # # Copy tables from SQLite to DuckDB
    # for table_name in table_names:
    #     logger.info(f"Working on table: {table_name}")
    #     # Fetch column names and types from SQLite table using DuckDB
    #     duckdb_cursor.execute(f"PRAGMA table_info(sqlite_db.{table_name});")
    #     columns_info = duckdb_cursor.fetchall()
    #     column_definitions = ", ".join([f"{col[1]} {col[2]}" for col in columns_info])

    #     # Create equivalent table in DuckDB
    #     duckdb_cursor.execute(f"CREATE TABLE {table_name} ({column_definitions});")

    #     # Copy data from SQLite to DuckDB using DuckDB
    #     duckdb_cursor.execute(
    #         f"INSERT INTO {table_name} SELECT * FROM sqlite_db.{table_name};"  # noqa: S608
    #     )

    # # Commit and close connections
    # duckdb_conn.commit()
    # duckdb_conn.close()


if __name__ == "__main__":
    convert_parquet_to_duckdb()
