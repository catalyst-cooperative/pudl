"""A command line interface (CLI) to the main PUDL ETL functionality."""

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
    parser.add_argument(dest='settings_file', type=str, default='',
                        help="path to YAML datapackage settings file.")
    parser.add_argument(
        '--dp_bundle_name',
        default="",
        help="Debug Mode. Set debug to True to get additional logs")
    parser.add_argument(
        '-d',
        '--debug',
        default=False,
        help="Debug Mode. Set debug to True to get additional logs")
    parser.add_argument(
        '-c',
        '--clobber',
        action='store_true',
        help="Clobber existing datapackages if they exist.",
        default=False)
    arguments = parser.parse_args(argv[1:])
    return arguments


def main():
    """Parse command line and initialize PUDL DB."""
    # Display logged output from the PUDL package:
    logger = logging.getLogger(pudl.__name__)
    log_format = '%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s'
    coloredlogs.install(fmt=log_format, level='INFO', logger=logger)

    args = parse_command_line(sys.argv)
    with pathlib.Path(args.settings_file).open() as f:
        script_settings = yaml.safe_load(f)

    try:
        pudl_in = script_settings["pudl_in"]
    except TypeError:
        pudl_in = pudl.workspace.setup.get_defaults()["pudl_in"]
    try:
        pudl_out = script_settings["pudl_out"]
    except TypeError:
        pudl_out = pudl.workspace.setup.get_defaults()["pudl_out"]

    pudl_settings = pudl.workspace.setup.derive_paths(
        pudl_in=pudl_in, pudl_out=pudl_out)

    pudl.output.export.generate_data_packages(
        script_settings,
        pudl_settings,
        debug=args.debug,
        pkg_bundle_dir_name=args.dp_bundle_name,
        clobber=args.clobber)


if __name__ == '__main__':
    sys.exit(main())
