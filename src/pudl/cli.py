"""A command line interface (CLI) to the main PUDL ETL functionality.

This script cordinates the PUDL ETL process, based on parameters provided via a YAML
settings file.

If the settings for a dataset has empty parameters (meaning there are no years or tables
included), no outputs will be generated. See :doc:`/dev/settings_files` for details.

The output SQLite and Parquet files will be stored in ``PUDL_OUT`` in directories named
``sqlite`` and ``parquet``.  To setup your default ``PUDL_IN`` and ``PUDL_OUT``
directories see ``pudl_setup --help``.

"""
import argparse
import logging
import sys
from sqlite3 import sqlite_version

import coloredlogs
from packaging import version

import pudl
from pudl.load import MINIMUM_SQLITE_VERSION
from pudl.settings import EtlSettings

logger = logging.getLogger(__name__)


def parse_command_line(argv):
    """
    Parse script command line arguments. See the -h option.

    Args:
        argv (list): command line arguments including caller file name.

    Returns:
        dict: A dictionary mapping command line arguments to their values.

    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        dest='settings_file',
        type=str,
        default='',
        help="path to ETL settings file."
    )
    parser.add_argument(
        '--ignore-foreign-key-constraints',
        action='store_true',
        default=False,
        help="Ignore foreign key constraints when loading into SQLite.",
    )
    parser.add_argument(
        '--ignore-type-constraints',
        action='store_true',
        default=False,
        help="Ignore column data type constraints when loading into SQLite.",
    )
    parser.add_argument(
        '--ignore-value-constraints',
        action='store_true',
        default=False,
        help="Ignore column value constraints when loading into SQLite.",
    )
    parser.add_argument(
        '-c',
        '--clobber',
        action='store_true',
        default=False,
        help="Clobber existing PUDL SQLite and Parquet outputs if they exist.",
    )
    parser.add_argument(
        "--sandbox",
        action="store_true",
        default=False,
        help="Use the Zenodo sandbox rather than production",
    )
    parser.add_argument(
        "--logfile",
        default=None,
        help="If specified, write logs to this file.",
    )
    parser.add_argument(
        "--gcs-cache-path",
        type=str,
        help="Load datastore resources from Google Cloud Storage. Should be gs://bucket[/path_prefix]",
    )
    parser.add_argument(
        "--bypass-local-cache",
        action="store_true",
        default=False,
        help="If enabled, the local file cache for datastore will not be used.",
    )

    arguments = parser.parse_args(argv[1:])
    return arguments


def main():
    """Parse command line and initialize PUDL DB."""
    # Display logged output from the PUDL package:
    pudl_logger = logging.getLogger("pudl")
    log_format = '%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s'
    coloredlogs.install(fmt=log_format, level='INFO', logger=pudl_logger)

    args = parse_command_line(sys.argv)
    if args.logfile:
        file_logger = logging.FileHandler(args.logfile)
        file_logger.setFormatter(logging.Formatter(log_format))
        pudl_logger.addHandler(file_logger)

    etl_settings = EtlSettings.from_yaml(args.settings_file)

    pudl_settings = pudl.workspace.setup.derive_paths(
        pudl_in=etl_settings.pudl_in,
        pudl_out=etl_settings.pudl_out
    )
    pudl_settings["sandbox"] = args.sandbox

    bad_sqlite_version = (
        version.parse(sqlite_version) < version.parse(MINIMUM_SQLITE_VERSION)
    )
    if bad_sqlite_version and not args.ignore_type_constraints:
        args.ignore_type_constraints = False
        pudl_logger.warning(
            f"Found SQLite {sqlite_version} which is less than "
            f"the minimum required version {MINIMUM_SQLITE_VERSION} "
            "As a result, data type constraint checking will be disabled."
        )

    pudl.etl.etl(
        etl_settings=etl_settings,
        pudl_settings=pudl_settings,
        clobber=args.clobber,
        use_local_cache=not args.bypass_local_cache,
        gcs_cache_path=args.gcs_cache_path,
        check_foreign_keys=not args.ignore_foreign_key_constraints,
        check_types=not args.ignore_type_constraints,
        check_values=not args.ignore_value_constraints,
    )


if __name__ == "__main__":
    sys.exit(main())
