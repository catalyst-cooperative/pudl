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

    # Iterate through the tables in order of foreign key dependency
    for table in metadata.sorted_tables:
        parquet_file_path = parquet_dir / f"{table.name}.parquet"
        logger.info(f"Loading table: {table.name} into DuckDB")
        if parquet_file_path.exists():
            sql_command = f"""
                COPY {table.name} FROM '{parquet_file_path}' (FORMAT PARQUET);
            """
            with engine.connect() as conn:
                conn.execute(sa.text(sql_command))
        else:
            print("File not found: ", parquet_file_path)
            # raise FileNotFoundError("Parquet file not found for: ", table.name)


if __name__ == "__main__":
    convert_parquet_to_duckdb()
