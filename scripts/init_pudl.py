#!/usr/bin/env python
"""This is a script for initializing the PUDL database locally."""

import logging
import sys
import argparse
import pudl

# Create a logger to output any messages we might have...
logger = logging.getLogger(pudl.__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter(
    # More extensive test-like formatter...
    '%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s',
    # This is the datetime format string.
    "%Y-%m-%d %H:%M:%S"
)
handler.setFormatter(formatter)
logger.addHandler(handler)

# require modern python
if not sys.version_info >= (3, 6):
    raise AssertionError(
        f"PUDL requires Python 3.6 or later. {sys.version_info} found."
    )


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

    args = parse_command_line(sys.argv)
    settings_init = pudl.settings.settings_init(
        settings_file=args.settings_file)

    pudl.init.verify_input_files(
        ferc1_years=settings_init['ferc1_years'],
        eia923_years=settings_init['eia923_years'],
        eia860_years=settings_init['eia860_years'],
        epacems_years=settings_init['epacems_years'],
        epacems_states=settings_init['epacems_states']
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
                      csvdir=settings_init['csvdir'],
                      keep_csv=settings_init['keep_csv'])


if __name__ == '__main__':
    sys.exit(main())
