"""A command line interface (CLI) to the main PUDL ETL functionality."""

import argparse
import logging
import sys

import coloredlogs

import pudl


def parse_command_line(argv):
    """
    Parse command line arguments. See the -h option.

    :param argv: arguments on the command line must include caller file name.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(dest='settings_file', type=str, default='',
                        help="Specify a YAML settings file.")
    arguments = parser.parse_args(argv[1:])
    return arguments


def main():
    """The main function."""
    # Display logged output from the PUDL package:
    logger = logging.getLogger(pudl.__name__)
    log_format = '%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s'
    coloredlogs.install(fmt=log_format, level='INFO', logger=logger)

    args = parse_command_line(sys.argv)
    settings_init = pudl.settings.settings_init(
        settings_file=args.settings_file)

    # Give pudl_in and pudl_out from settings file priority, if found:
    # Otherwise fall back to user default pudl config file
    try:
        pudl_in = settings_init['pudl_in']
    except KeyError:
        pudl_in = pudl.settings.read_user_settings()['pudl_in']

    try:
        pudl_out = settings_init['pudl_out']
    except KeyError:
        pudl_out = pudl.settings.read_user_settings()['pudl_out']

    pudl_settings = pudl.settings.init(pudl_in=pudl_in, pudl_out=pudl_out)

    logger.info(f"Checking for input files in {pudl_settings['data_dir']}")
    pudl.helpers.verify_input_files(
        ferc1_years=settings_init['ferc1_years'],
        eia923_years=settings_init['eia923_years'],
        eia860_years=settings_init['eia860_years'],
        epacems_years=settings_init['epacems_years'],
        epacems_states=settings_init['epacems_states'],
        data_dir=pudl_settings['data_dir'],
    )

    pudl.init.init_db(ferc1_tables=settings_init['ferc1_tables'],
                      ferc1_years=settings_init['ferc1_years'],
                      eia923_tables=settings_init['eia923_tables'],
                      eia923_years=settings_init['eia923_years'],
                      eia860_tables=settings_init['eia860_tables'],
                      eia860_years=settings_init['eia860_years'],
                      epacems_years=settings_init['epacems_years'],
                      epacems_states=settings_init['epacems_states'],
                      epaipm_tables=settings_init['epaipm_tables'],
                      pudl_testing=settings_init['pudl_testing'],
                      ferc1_testing=settings_init['ferc1_testing'],
                      pudl_settings=pudl_settings,
                      debug=settings_init['debug'],
                      csvdir=settings_init['csvdir'],
                      keep_csv=settings_init['keep_csv'])


if __name__ == '__main__':
    sys.exit(main())
