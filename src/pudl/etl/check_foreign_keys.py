"""Check that foreign key constraints in the PUDL database are respected."""
import pathlib
import sys

import click
from dagster import build_init_resource_context
from dotenv import load_dotenv

import pudl
from pudl.io_managers import PudlSQLiteIOManager, pudl_io_manager

logger = pudl.logging_helpers.get_logger(__name__)


@click.command(
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.option(
    "--logfile",
    help="If specified, write logs to this file.",
    type=click.Path(
        exists=False,
        resolve_path=True,
        path_type=pathlib.Path,
    ),
)
@click.option(
    "--loglevel",
    default="INFO",
    type=click.Choice(
        ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False
    ),
)
def pudl_check_fks(logfile: pathlib.Path, loglevel: str):
    """Check that foreign key constraints in the PUDL database are respected.

    Dagster manages the dependencies between various assets in our ETL pipeline,
    attempting to materialize tables only after their upstream dependencies have been
    satisfied. However, this order is non deterministic because they are executed in
    parallel, and doesn't necessarily correspond to the foreign-key constraints within
    the database, so durint the ETL we disable foreign key constraints within
    ``pudl.sqlite``.

    However, we still expect foreign key constraints to be satisfied once all of the
    tables have been loaded, so we check that they are valid after the ETL has
    completed. This script runs the same check.
    """
    load_dotenv()

    # Display logged output from the PUDL package:
    pudl.logging_helpers.configure_root_logger(logfile=logfile, loglevel=loglevel)

    context = build_init_resource_context()
    io_manager: PudlSQLiteIOManager = pudl_io_manager(context)._sqlite_io_manager
    # TODO(rousik): This reaches into the io_manager and assumes
    # PudlSQLiteIOManager and the corresponding foreign key
    # functionality. It's strange to be constructing dagster
    # resources to achieve this, but alas, that's where the logic
    # lies.

    database_path = io_manager.base_dir / f"{io_manager.db_name}.sqlite"
    logger.info(f"Checking foreign key constraints in {database_path}")

    io_manager.check_foreign_keys()
    return 0


if __name__ == "__main__":
    sys.exit(pudl_check_fks())
