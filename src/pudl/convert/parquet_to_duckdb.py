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


@click.command(
    name="parquet_to_duckdb",
    context_settings={"help_option_names": ["-h", "--help"]},
)
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
    "--check-fks",
    is_flag=True,
    show_default=True,
    default=False,
    help="If true, enable foreign keys in the database. Currently,"
    "the parquet load process freezes up when foreign keys are enabled.",
)
def parquet_to_duckdb(
    parquet_dir: str, duckdb_path: str, no_load: bool, check_fks: bool
):
    """Convert a directory of Parquet files to a DuckDB database.

    Args:
        parquet_dir: Path to a directory of parquet files.
        duckdb_path: Path to the new DuckDB database file (should not exist).
        no_load: Only create metadata, don't load data.
        check_fks: If true, enable foreign keys in the database. Currently,
            the parquet load process freezes up when foreign keys are enabled.

    Example:
        parquet_to_duckdb /path/to/parquet/directory duckdb.db
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

    metadata = package.to_sql(dialect="duckdb", check_foreign_keys=check_fks)
    engine = sa.create_engine(f"duckdb:///{duckdb_path}")
    metadata.create_all(engine)

    if not no_load:
        with duckdb.connect(database=str(duckdb_path)) as duckdb_conn:
            duckdb_cursor = duckdb_conn.cursor()
            # Load data into the DuckDB database from parquet files, if requested:
            # Iterate through the tables in order of foreign key dependency
            for table in metadata.sorted_tables:
                parquet_file_path = parquet_dir / f"{table.name}.parquet"
                if parquet_file_path.exists():
                    logger.info(
                        f"Loading parquet file: {parquet_file_path} into {duckdb_path}"
                    )
                    sql_command = f"""
                        COPY {table.name} FROM '{parquet_file_path}' (FORMAT PARQUET);
                    """
                    duckdb_cursor.execute(sql_command)
                else:
                    raise FileNotFoundError("Parquet file not found for: ", table.name)


if __name__ == "__main__":
    parquet_to_duckdb()
