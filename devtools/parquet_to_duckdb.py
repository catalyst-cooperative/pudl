#! /usr/bin/env python
"""Script that creates a DuckDB database from a collection of PUDL Parquet files."""

import logging
from pathlib import Path

import click
import duckdb
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
@click.option(
    "--no-load",
    is_flag=True,
    show_default=True,
    default=False,
    help="Only create metadata, don't load data.",
)
@click.option(
    "--disable-fks",
    is_flag=True,
    show_default=True,
    default=False,
    help="Only create metadata, don't load data.",
)
def convert_parquet_to_duckdb(
    parquet_dir: str, duckdb_path: str, no_load: bool, disable_fks: bool
):
    """Convert a directory of Parquet files to a DuckDB database.

    Args:
        parquet_dir: Path to a directory of parquet files.
        duckdb_path: Path to the new DuckDB database file (should not exist).
        no_load: Only create metadata, don't load data.
        disable_fks: Don't add foreign keys to the database.

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

    metadata = package.to_sql(dialect="duckdb", check_foreign_keys=not disable_fks)
    engine = sa.create_engine(f"duckdb:///{duckdb_path}")
    metadata.create_all(engine)

    if not no_load:
        # Connect to DuckDB database
        duckdb_conn = duckdb.connect(database=str(duckdb_path))
        duckdb_cursor = duckdb_conn.cursor()

        # Iterate through the tables in order of foreign key dependency
        for table in metadata.sorted_tables:
            parquet_file_path = parquet_dir / f"{table.name}.parquet"
            if parquet_file_path.exists():
                logger.info(f"Loading table: {table.name} into DuckDB")
                sql_command = f"""
                    COPY {table.name} FROM '{parquet_file_path}' (FORMAT PARQUET);
                """
                logger.info(sql_command)
                duckdb_cursor.execute(sql_command)
            else:
                logger.info("File not found: ", parquet_file_path)
                # TODO: throw an error if there is a file that doesn't exist in the database
                # raise FileNotFoundError("Parquet file not found for: ", table.name)

        # Commit and close connections
        duckdb_conn.commit()
        duckdb_conn.close()


if __name__ == "__main__":
    convert_parquet_to_duckdb()
