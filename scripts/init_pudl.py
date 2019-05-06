#!/usr/bin/env python
"""This is a script for initializing the PUDL database locally."""

import sys
import argparse
import pudl
import pudl.constants as pc
from pudl.settings import SETTINGS
import pudl.models.glue
import pudl.models.eia860
import pudl.models.eia923
import pudl.models.entities
import pudl.models.ferc1

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
    parser.add_argument('-f', '--settings_file', dest='settings_file',
                        type=str, help="Specify a YAML settings file.",
                        default='settings.yml')
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

    pudl.extract.ferc1.init_db(ferc1_tables=pc.ferc1_default_tables,
                               refyear=settings_init['ferc1_ref_year'],
                               years=settings_init['ferc1_years'],
                               def_db=True,
                               verbose=settings_init['verbose'],
                               testing=settings_init['ferc1_testing'])

    pudl.init.init_db(ferc1_tables=settings_init['ferc1_tables'],
                      ferc1_years=settings_init['ferc1_years'],
                      eia923_tables=settings_init['eia923_tables'],
                      eia923_years=settings_init['eia923_years'],
                      eia860_tables=settings_init['eia860_tables'],
                      eia860_years=settings_init['eia860_years'],
                      epacems_years=settings_init['epacems_years'],
                      epacems_states=settings_init['epacems_states'],
                      verbose=settings_init['verbose'],
                      debug=settings_init['debug'],
                      pudl_testing=settings_init['pudl_testing'],
                      ferc1_testing=settings_init['ferc1_testing'],
                      csvdir=SETTINGS['csvdir'],
                      keep_csv=settings_init['keep_csv'])


if __name__ == '__main__':
    sys.exit(main())
