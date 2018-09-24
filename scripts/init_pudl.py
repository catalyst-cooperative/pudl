#!/usr/bin/env python
"""This is a script for initializing the PUDL database locally."""

import os
import sys
import argparse

assert sys.version_info >= (3, 5)  # require modern python

# This is a hack to make the pudl package importable from within this script,
# even though it isn't in one of the normal site-packages directories where
# Python typically searches.  When we have some real installation/packaging
# happening, this will no longer be necessary.
sys.path.append(os.path.abspath('..'))


def parse_command_line(argv):
    """
    Parse command line arguments. See the -h option.

    :param argv: arguments on the command line must include caller file name.
    """
    from pudl import constants
    parser = argparse.ArgumentParser()
    parser.add_argument('--settings_file', dest='settings_file', type=str,
                        help="Specify a YAML settings file.",
                        default='settings.yml')
    arguments = parser.parse_args(argv[1:])
    return arguments


def main():
    """The main function."""
    from pudl import init, constants
    from pudl import extract
    import pudl.models.glue
    import pudl.models.eia860
    import pudl.models.eia923
    import pudl.models.eia
    import pudl.models.ferc1

    args = parse_command_line(sys.argv)
    from pudl.settings import SETTINGS
    settings_init = pudl.settings.settings_init(
        settings_file=args.settings_file)

    extract.ferc1.init_db(ferc1_tables=constants.ferc1_default_tables,
                          refyear=settings_init['ferc1_ref_year'],
                          years=settings_init['ferc1_years'],
                          def_db=True,
                          verbose=settings_init['verbose'],
                          testing=settings_init['ferc1_testing'])

    init.init_db(ferc1_tables=settings_init['ferc1_tables'],
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
