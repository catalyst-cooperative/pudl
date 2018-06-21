"""This is a script for initializing the PUDL database locally."""

import os
import sys
import argparse

assert sys.version_info >= (3, 3)  # require modern python

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

    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument(
        '-v', '--verbose', dest='verbose', action='store_true',
        help="Display messages indicating progress or errors.",
        default=True)
    verbosity.add_argument(
        '-q', '--quiet', dest='verbose', action='store_false',
        help="Suppress messages indicating progress or errors.")

    parser.add_argument('-t', '--test', dest='test', action='store_true',
                        help="Use ferc1_test & pudl_test databases.",
                        default=False)
    parser.add_argument('--keep_csv', dest='keep_csv', action='store_true',
                        help="Do not delete CSV files after loading them.",
                        default=False)
    parser.add_argument('--csvdir', dest='csvdir', type=str,
                        help="Path to directory for CSV file storage.",
                        default='')

    parser.add_argument('--ferc1_refyear', dest='ferc1_refyear', type=int,
                        default=max(constants.working_years['ferc1']),
                        help="Reference year for FERC Form 1 database.")
    parser.add_argument('--ferc1_start', dest='ferc1_start', type=int,
                        default=min(constants.working_years['ferc1']),
                        help="First year of FERC Form 1 data to load.")
    parser.add_argument('--ferc1_end', dest='ferc1_end', type=int,
                        default=max(constants.working_years['ferc1']),
                        help="Last year of FERC Form 1 data to load.")

    parser.add_argument('--eia923_start', dest='eia923_start', type=int,
                        default=min(constants.working_years['eia923']),
                        help="First year of EIA Form 923 data to load.")
    parser.add_argument('--eia923_end', dest='eia923_end', type=int,
                        default=max(constants.working_years['eia923']),
                        help="Last year of EIA Form 923 data to load.")

    parser.add_argument('--eia860_start', dest='eia860_start', type=int,
                        default=min(constants.working_years['eia860']),
                        help="First year of EIA Form 860 data to load.")
    parser.add_argument('--eia860_end', dest='eia860_end', type=int,
                        default=max(constants.working_years['eia860']),
                        help="Last year of EIA Form 860 data to load.")

    parser.add_argument('--epacems_start', dest='epacems_start', type=int,
                        default=min(constants.working_years['epacems']),
                        help="First year of EPA hourly CEMS data to load.")
    parser.add_argument('--epacems_end', dest='epacems_end', type=int,
                        default=max(constants.working_years['epacems']),
                        help="Last year of EPA hourly CEMS data to load.")
    parser.add_argument('--epacems_states', dest='epacems_states',
                        nargs='+', default=['CO',],
                        help="Abbreviations of US states for which to load CEMS\
                              data. Default: 'CO'. (Use 'all' to load all)")
    arguments = parser.parse_args(argv[1:])

    return arguments


def main():
    """The main function."""
    from pudl import init, settings, constants
    from pudl import extract
    import pudl.models.entities
    import pudl.models.glue
    import pudl.models.eia860
    import pudl.models.eia923
    import pudl.models.eia
    import pudl.models.ferc1

    args = parse_command_line(sys.argv)
    if args.epacems_states[0]=='all':
        epacems_states = list(constants.cems_states.keys())
    else:
        epacems_states = args.epacems_states

    extract.ferc1.init_db(ferc1_tables=constants.ferc1_default_tables,
                          refyear=args.ferc1_refyear,
                          years=range(args.ferc1_start, args.ferc1_end + 1),
                          def_db=True,
                          verbose=args.verbose,
                          testing=args.test)

    init.init_db(ferc1_tables=constants.ferc1_pudl_tables,
                 ferc1_years=range(args.ferc1_start, args.ferc1_end + 1),
                 eia923_tables=constants.eia923_pudl_tables,
                 eia923_years=range(args.eia923_start,
                                    args.eia923_end + 1),
                 eia860_tables=constants.eia860_pudl_tables,
                 eia860_years=range(args.eia860_start,
                                    args.eia860_end + 1),
                 epacems_years = range(args.epacems_start,
                                       args.epacems_end + 1),
                 epacems_states=epacems_states,
                 verbose=args.verbose,
                 debug=False,
                 pudl_testing=args.test,
                 ferc1_testing=args.test,
                 csvdir=args.csvdir,
                 keep_csv=args.keep_csv)


if __name__ == '__main__':
    sys.exit(main())
