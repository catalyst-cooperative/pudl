"""A command line interface (CLI) to the main PUDL ETL functionality.

This script generates datapacakges based on the datapackage settings enumerated
in the settings_file which is given as an argument to this script. If the
settings has empty datapackage parameters (meaning there are no years or
tables included), no datapacakges will be generated. If the settings include a
datapackage that has empty parameters, the other valid datatpackages will be
generated, but not the empty one. If there are invalid parameters (meaning a
partition that is not included in the pudl.constant.working_partitions), the
build will fail early on in the process.

The datapackages will be stored in "PUDL_OUT" in the "datapackge" subdirectory.
Currently, this function only uses default directories for "PUDL_IN" and
"PUDL_OUT" (meaning those stored in $HOME/.pudl.yml). To setup your default
pudl directories see the pudl_setup script (pudl_setup --help for more details).

"""
import argparse
import logging
import os
import pathlib
import sys
from datetime import datetime

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
    parser = argparse.ArgumentParser(
        description=__doc__,
        parents=[pudl.etl.command_line_flags()])

    parser.add_argument(
        dest='settings_file',
        type=str,
        default=os.environ.get('PUDL_SETTINGS_FILE'),
        help="""Path to YAML datapackage settings file.

        If not specified, the default will be set from PUDL_SETTINGS_FILE environment
        variable. If this is also not set the script will fail.""")
    parser.add_argument(
        "--sandbox", action="store_true", default=False,
        help="Use the Zenodo sandbox rather than production")
    parser.add_argument(
        "--logfile", default=None,
        help="If specified, write logs to this file.")
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
        "--datapkg-bundle-name",
        type=str,
        help="If specified, use this datpkg_bundle_name instead of the default from the config.""")

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


def main():
    """Parse command line and initialize PUDL DB."""
    # Display logged output from the PUDL package:
    args = parse_command_line(sys.argv)
    setup_logging(args)

    with pathlib.Path(args.settings_file).open() as f:
        script_settings = yaml.safe_load(f)

    if args.datapkg_bundle_name:
        script_settings["datapkg_bundle_name"] = args.datapkg_bundle_name

    pudl_in = script_settings.get(
        "pudl_in", pudl.workspace.setup.get_defaults()["pudl_in"])
    pudl_out = script_settings.get(
        "pudl_out", pudl.workspace.setup.get_defaults()["pudl_out"])
    pudl_settings = pudl.workspace.setup.derive_paths(
        pudl_in=pudl_in, pudl_out=pudl_out)
    pudl_settings["sandbox"] = args.sandbox

    datapkg_bundle_doi = script_settings.get("datapkg_bundle_doi")
    if datapkg_bundle_doi and not pudl.helpers.is_doi(datapkg_bundle_doi):
        raise ValueError(
            f"Found invalid bundle DOI: {datapkg_bundle_doi} "
            f"in bundle {script_settings['datpkg_bundle_name']}."
        )

    pudl.etl.generate_datapkg_bundle(
        script_settings,
        pudl_settings,
        datapkg_bundle_name=script_settings['datapkg_bundle_name'],
        datapkg_bundle_doi=datapkg_bundle_doi,
        commandline_args=args)


if __name__ == "__main__":
    sys.exit(main())
