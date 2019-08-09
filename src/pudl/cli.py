"""A command line interface (CLI) to the main PUDL ETL functionality."""

import argparse
import logging
import sys

import coloredlogs

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
    parser = argparse.ArgumentParser()
    parser.add_argument(dest='settings_file', type=str, default='',
                        help="path to YAML settings file.")
    arguments = parser.parse_args(argv[1:])
    return arguments


def main():
    """The main function."""
    # Display logged output from the PUDL package:
    logger = logging.getLogger(pudl.__name__)
    log_format = '%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s'
    coloredlogs.install(fmt=log_format, level='INFO', logger=logger)

    args = parse_command_line(sys.argv)
    script_settings = pudl.settings.read_script_settings(args.settings_file)
    pudl_settings = pudl.settings.init(
        pudl_in=script_settings['pudl_in'],
        pudl_out=script_settings['pudl_out']
    )
    logger.info(f"Checking for input files in {pudl_settings['data_dir']}")
    pudl.helpers.verify_input_files(
        ferc1_years=script_settings['ferc1_years'],
        eia923_years=script_settings['eia923_years'],
        eia860_years=script_settings['eia860_years'],
        epacems_years=script_settings['epacems_years'],
        epacems_states=script_settings['epacems_states'],
        data_dir=pudl_settings['data_dir'],
    )

    pudl.init.init_db(ferc1_tables=script_settings['ferc1_tables'],
                      ferc1_years=script_settings['ferc1_years'],
                      eia923_tables=script_settings['eia923_tables'],
                      eia923_years=script_settings['eia923_years'],
                      eia860_tables=script_settings['eia860_tables'],
                      eia860_years=script_settings['eia860_years'],
                      epacems_years=script_settings['epacems_years'],
                      epacems_states=script_settings['epacems_states'],
                      epaipm_tables=script_settings['epaipm_tables'],
                      pudl_testing=script_settings['pudl_testing'],
                      pudl_settings=pudl_settings,
                      debug=script_settings['debug'],
                      csvdir=script_settings['csvdir'],
                      keep_csv=script_settings['keep_csv'])


if __name__ == '__main__':
    sys.exit(main())
