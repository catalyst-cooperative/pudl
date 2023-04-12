"""A command line interface (CLI) to check foreign key constraints in the PUDL database.

Assets are executed once their upstream dependencies have been executed.  However, this
order is non deterministic because they are executed in parallel.  This means the order
that tables are loaded into ``pudl.sqlite`` will not satisfy foreign key constraints.

Foreign key constraints on ``pudl.sqlite`` are disabled so dagster can load tables into
the database without a foreign key error being raised. However, foreign key constraints
can be evaluated after all of the data has been loaded into the database.  To check the
constraints, run the ``pudl_check_fks`` cli command once the data has been loaded into
``pudl.sqlite``.
"""
import argparse
import sys

from dagster import build_init_resource_context
from dotenv import load_dotenv

import pudl
from pudl.io_managers import pudl_sqlite_io_manager

logger = pudl.logging_helpers.get_logger(__name__)


def parse_command_line(argv):
    """Parse script command line arguments. See the -h option.

    Args:
        argv (list): command line arguments including caller file name.

    Returns:
        dict: A dictionary mapping command line arguments to their values.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--logfile",
        default=None,
        help="If specified, write logs to this file.",
    )
    parser.add_argument(
        "--loglevel",
        help="Set logging level (DEBUG, INFO, WARNING, ERROR, or CRITICAL).",
        default="INFO",
    )
    arguments = parser.parse_args(argv[1:])
    return arguments


def main():
    """Parse command line and check PUDL foreign key constraints."""
    load_dotenv()
    args = parse_command_line(sys.argv)

    # Display logged output from the PUDL package:
    pudl.logging_helpers.configure_root_logger(
        logfile=args.logfile, loglevel=args.loglevel
    )

    context = build_init_resource_context()
    io_manager = pudl_sqlite_io_manager(context)

    database_path = io_manager.base_dir / f"{io_manager.db_name}.sqlite"
    logger.info(f"Checking foreign key constraints in {database_path}")

    io_manager.check_foreign_keys()


if __name__ == "__main__":
    sys.exit(main())
