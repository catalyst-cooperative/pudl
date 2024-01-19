"""Check that foreign key constraints in the PUDL database are respected."""
import pathlib
import sys

import click
from dotenv import load_dotenv

import pudl
from pudl.helpers import check_foreign_keys
from pudl.workspace.setup import PudlPaths

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
    check_foreign_keys(pathlib.Path(PudlPaths().pudl_db))
    return 0


if __name__ == "__main__":
    sys.exit(pudl_check_fks())
