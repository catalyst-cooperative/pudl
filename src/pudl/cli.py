"""A command line interface (CLI) to the main PUDL ETL functionality.

This script generates datapacakges based on the datapackage settings enumerated
in the settings_file which is given as an argument to this script. If the
settings has empty datapackage parameters (meaning there are no years or
tables included), no datapacakges will be generated. If the settings include a
datapackage that has empty parameters, the other valid datatpackages will be
generated, but not the empty one. If there are invalid parameters (meaning a
partition that is not included in the pudl.constant.WORKING_PARTITIONS), the
build will fail early on in the process.

The datapackages will be stored in "PUDL_OUT" in the "datapackge" subdirectory.
Currently, this function only uses default directories for "PUDL_IN" and
"PUDL_OUT" (meaning those stored in $HOME/.pudl.yml). To setup your default
pudl directories see the pudl_setup script (pudl_setup --help for more details).

"""
import argparse
import logging
import os
import sys
import uuid
from datetime import datetime
from sqlite3 import sqlite_version
from typing import Dict

import coloredlogs
import fsspec
import prefect
import yaml
from fsspec.implementations.local import LocalFileSystem
from packaging import version

import pudl
from pudl.load.sqlite import MINIMUM_SQLITE_VERSION

logger = logging.getLogger(__name__)


def parse_command_line(argv):
    """
    Parse script command line arguments. See the -h option.

    Args:
        argv (list): command line arguments including caller file name.

    Returns:
        dict: A dictionary mapping command line arguments to their values.

    """
    parser = argparse.ArgumentParser(
        description=__doc__,
        parents=[pudl.etl.command_line_flags()])

    parser.add_argument(
        dest='settings_file',
        type=str,
        nargs='?',
        default=os.environ.get('PUDL_SETTINGS_FILE'),
        help="""Path to YAML datapackage settings file.

        If not specified, the default will be set from PUDL_SETTINGS_FILE environment
        variable. If this is also not set the script will fail.""")
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
        "--timestamped-logfile",
        default="/tmp/pudl_etl.%F-%H%M%S.log",  # nosec
        help="""If specified, also log to the timestamped logfile. The value of
        this flag is passed to strftime method of datetime.now().""")
    parser.add_argument(
        "--loglevel",
        help="Set logging level (DEBUG, INFO, WARNING, ERROR, or CRITICAL).",
        default="INFO",
    )
    parser.add_argument(
        "--rerun",
        type=str,
        default=os.environ.get('PUDL_RUN_ID'),
        help="If specified, try to resume ETL execution for a given run_id.""")
    parser.add_argument(
        "--run-id",
        type=str,
        help="""If specified, use this run_id instead of generating random one.""")
    # TODO(rousik): we could also consider --rerun-latest that will pick up the most recent run_id
    # from the provided cache directory.
    parser.add_argument(
        "--gcs-cache-path",
        type=str,
        help="Load datastore resources from Google Cloud Storage. Should be gs://bucket[/path_prefix]",
    )
    arguments = parser.parse_args(argv[1:])
    return arguments


def setup_logging(args):
    """Configures the logging based on the command-line flags.

    Args:
        args: parsed command line flags.
    """
    logger = logging.getLogger(pudl.__name__)
    log_format = '%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s'
    coloredlogs.install(fmt=log_format, level='INFO', logger=logger)
    if args.logfile:
        file_logger = logging.FileHandler(args.logfile)
        file_logger.setFormatter(logging.Formatter(log_format))
        logger.addHandler(file_logger)
    if args.timestamped_logfile:
        file_logger = logging.FileHandler(
            datetime.now().strftime(args.timestamped_logfile))
        file_logger.setFormatter(logging.Formatter(log_format))
        logger.addHandler(file_logger)
        logger.info(f"Command line: {' '.join(sys.argv)}")
    logger.setLevel(args.loglevel)


def generate_run_id(args):
    """Generates run_id for this ETL execution.

    If --run-id is specified, use that. Otherwise generate random run_id based on timestamp
    and uuid.
    """
    if args.rerun:
        return args.rerun
    elif args.run_id:
        return args.run_id
    else:
        ts = datetime.now().strftime('%F-%H%M')
        return f"{ts}-{uuid.uuid4()}"


def load_script_settings(args, run_id) -> Dict:
    """Loads the script settings from the right location.

    If --rerun is specified, it loads the settings file from the cache. Otherwise
    it will assume that this is the first positional argument of the pudl_etl
    script and loads that.
    """
    if args.rerun:
        if not args.pipeline_cache_path:
            raise AssertionError(
                'When using --rerun, --pipeline-cache-path must be also set.')
        settings_file_path = os.path.join(
            args.pipeline_cache_path, run_id, "settings.yml")
    else:
        if not args.settings_file:
            raise ValueError(
                "settings_file must be set on command-line or"
                " via PUDL_SETTINGS_FILE when not using --rerun flag.")
        settings_file_path = args.settings_file
    logger.info(f'Loading settings from {settings_file_path}')
    with fsspec.open(settings_file_path, "r") as fs:
        script_settings = yaml.safe_load(fs)
        script_settings["run_id"] = run_id
        return script_settings


def build_pudl_settings(script_settings, args):
    """Builds pudl_settings object with correct path and other configurations."""
    pudl_in = script_settings.get(
        "pudl_in", pudl.workspace.setup.get_defaults()["pudl_in"])
    pudl_out = script_settings.get(
        "pudl_out", pudl.workspace.setup.get_defaults()["pudl_out"])
    pudl_settings = pudl.workspace.setup.derive_paths(
        pudl_in=pudl_in,
        pudl_out=pudl_out
    )
    pudl_settings["sandbox"] = args.sandbox
    return pudl_settings


def main():
    """Parse command line and initialize PUDL DB."""
    # Display logged output from the PUDL package:
    args = parse_command_line(sys.argv)
    setup_logging(args)

    # Ensure that directories are automatically created when dealing with local files.
    LocalFileSystem(auto_mkdir=True)

    run_id = generate_run_id(args)
    prefect.context.pudl_run_id = run_id

    logger.warning(
        f'Running pipeline with run_id {run_id} (use this with --rerun to resume).')

    script_settings = load_script_settings(args, run_id)
    pudl_settings = build_pudl_settings(script_settings, args)

    if args.pipeline_cache_path:
        args.pipeline_cache_path = os.path.join(args.pipeline_cache_path, run_id)
    else:
        args.pipeline_cache_path = os.path.join(
            pudl_settings["pudl_out"], "cache", run_id)
    prefect.context.pudl_pipeline_cache_path = args.pipeline_cache_path

    datapkg_bundle_doi = script_settings.get("datapkg_bundle_doi")
    if datapkg_bundle_doi and not pudl.helpers.is_doi(datapkg_bundle_doi):
        raise ValueError(
            f"Found invalid bundle DOI: {datapkg_bundle_doi} "
            f"in bundle {script_settings['datpkg_bundle_name']}."
        )
    with fsspec.open(os.path.join(args.pipeline_cache_path, "settings.yml"), "w") as outfile:
        yaml.dump(script_settings, outfile, default_flow_style=False)

    bad_sqlite_version = (
        version.parse(sqlite_version) < version.parse(MINIMUM_SQLITE_VERSION)
    )
    if bad_sqlite_version and not args.ignore_type_constraints:
        args.ignore_type_constraints = False
        logger.warning(
            f"Found SQLite {sqlite_version} which is less than "
            f"the minimum required version {MINIMUM_SQLITE_VERSION} "
            "As a result, data type constraint checking will be disabled."
        )

    pudl.etl.etl(
        etl_settings=script_settings,
        pudl_settings=pudl_settings,
        clobber=args.clobber,
        use_local_cache=not args.bypass_local_cache,
        gcs_cache_path=args.gcs_cache_path,
        check_foreign_keys=not args.ignore_foreign_key_constraints,
        check_types=not args.ignore_type_constraints,
        check_values=not args.ignore_value_constraints,
        commandline_args=args
    )


if __name__ == "__main__":
    sys.exit(main())
