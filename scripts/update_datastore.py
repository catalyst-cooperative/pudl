#!/usr/bin/env python
"""A script for fetching public utility data from reporting agency servers."""

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
    from pudl.settings import SETTINGS
    import pudl.constants as pc
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-q',
        '--quiet',
        dest='verbose',
        action='store_false',
        help="Quiet mode. Suppress download progress indicators and warnings.",
        default=True
    )
    parser.add_argument(
        '-z',
        '--zip',
        dest='unzip',
        action='store_false',
        help="Do not unzip downloaded data files.",
        default=True
    )
    parser.add_argument(
        '-c',
        '--clobber',
        action='store_true',
        help="Clobber existing zipfiles in the datastore if they exist.",
        default=False
    )
    parser.add_argument(
        '-d',
        '--datadir',
        type=str,
        help="""Path to the top level datastore directory. (default:
        %(default)s).""",
        default=SETTINGS['data_dir']
    )
    parser.add_argument(
        '-s',
        '--sources',
        nargs='+',
        choices=pc.data_sources,
        help="""List of data sources which should be downloaded.
        (default: %(default)s).""",
        default=pc.data_sources
    )
    parser.add_argument(
        '-y',
        '--years',
        dest='year',
        nargs='+',
        help="""List of years for which data should be downloaded. Different
        data sources have differet valid years. If data is not available for a
        specified year and data source, it will be ignored. If no years are
        specified, all available data will be downloaded for all requested data
        sources.""",
        default=[]
    )
    parser.add_argument(
        '-n',
        '--no-download',
        dest='no_download',
        action='store_true',
        help="Do not download data files, only unzip ones that are already present.",
        default=False
    )
    parser.add_argument(
        '-t',
        '--states',
        nargs='+',
        choices=pc.cems_states.keys(),
        help="""List of two letter US state abbreviations indicating which
        states data should be downloaded. Currently only applicable to the EPA's
        CEMS dataset.""",
        default=pc.cems_states.keys()
    )

    arguments = parser.parse_args(argv[1:])
    return arguments


def main():
    """Main function controlling flow of the script."""
    import concurrent.futures
    import pudl
    import pudl.constants as pc

    args = parse_command_line(sys.argv)

    # Generate a list of valid years of data to download for each data source.
    # If no years were specified, use the full set of valid years.
    # If years were specified, keep only the years which are valid for that
    # data source, and optionally output a message saying which years are
    # being ignored because they aren't valid.
    yrs_by_src = {}
    for src in args.sources:
        if not args.year:
            yrs_by_src[src] = pc.data_years[src]
        else:
            yrs_by_src[src] = [int(yr) for yr in args.year
                               if int(yr) in pc.data_years[src]]
            bad_yrs = [int(yr) for yr in args.year
                       if int(yr) not in pc.data_years[src]]
            if args.verbose and bad_yrs:
                print("Invalid {} years ignored: {}.".format(src, bad_yrs))

    with concurrent.futures.ThreadPoolExecutor() as executor:
        for src in args.sources:
            for yr in yrs_by_src[src]:
                executor.submit(pudl.datastore.update, src, yr, args.states,
                                clobber=args.clobber,
                                unzip=args.unzip,
                                verbose=args.verbose,
                                datadir=args.datadir,
                                no_download=args.no_download)


if __name__ == '__main__':
    sys.exit(main())
