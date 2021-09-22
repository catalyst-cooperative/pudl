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
import pathlib
import sys

import coloredlogs
import yaml

import pudl

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
    with pathlib.Path(args.settings_file).open() as f:
        script_settings = yaml.safe_load(f)

    default_settings = pudl.workspace.setup.get_defaults()
    pudl_in = script_settings.get("pudl_in", default_settings["pudl_in"])
    pudl_out = script_settings.get("pudl_out", default_settings["pudl_out"])

    pudl_settings = pudl.workspace.setup.derive_paths(
        pudl_in=pudl_in,
        pudl_out=pudl_out
    )
    pudl_settings["sandbox"] = args.sandbox

    pudl.etl.etl(
        etl_settings_bundle=script_settings['datapkg_bundle_settings'],
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
