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
                        help="path to YAML settings file.")
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
    except KeyError:
        pudl_in = pudl.workspace.setup.get_defaults()["pudl_in"]
    try:
        pudl_out = script_settings["pudl_out"]
    except KeyError:
        pudl_out = pudl.workspace.setup.get_defaults()["pudl_out"]

    pudl_settings = pudl.workspace.setup.derive_paths(
        pudl_in=pudl_in, pudl_out=pudl_out)

    logger.info(f"Checking for input files in {pudl_settings['data_dir']}")
    pudl.helpers.verify_input_files(
        ferc1_years=script_settings["ferc1_years"],
        eia923_years=script_settings["eia923_years"],
        eia860_years=script_settings["eia860_years"],
        epacems_years=script_settings["epacems_years"],
        epacems_states=script_settings["epacems_states"],
        data_dir=pudl_settings["data_dir"],
    )

    try:
        debug = script_settings["debug"]
    except KeyError:
        debug = False
    try:
        pudl_testing = script_settings["pudl_testing"]
    except KeyError:
        pudl_testing = False

    pudl.init.init_db(ferc1_tables=script_settings["ferc1_tables"],
                      ferc1_years=script_settings["ferc1_years"],
                      eia923_tables=script_settings["eia923_tables"],
                      eia923_years=script_settings["eia923_years"],
                      eia860_tables=script_settings["eia860_tables"],
                      eia860_years=script_settings["eia860_years"],
                      epacems_years=script_settings["epacems_years"],
                      epacems_states=script_settings["epacems_states"],
                      epaipm_tables=script_settings["epaipm_tables"],
                      pudl_testing=pudl_testing,
                      pudl_settings=pudl_settings,
                      debug=debug)


if __name__ == "__main__":
    sys.exit(main())
