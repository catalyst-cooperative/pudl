"""
A CLI for fetching public utility data from reporting agency servers.

This script will generate a datastore on a datastore directory. By default, the
directory will end up in wherever you have designated "PUDL_IN" in the settings
file $HOME/.pudl.yml. You can use this script to specific only specific datasets
to download, only specific years or states but by default, it will grab
everything. A populated datastore is required to use other PUDL tools, like the
ETL script (`pudl_etl`) and all of the post-ETL processes.
"""

import argparse
import logging
import pathlib
import sys
import warnings

import coloredlogs

import pudl
import pudl.constants as pc
from pudl.workspace import datastore


def parse_command_line(argv):
    """
    Parse command line arguments. See the -h option for more details.

    Args:
        argv (str): Command line arguments, which must include caller filename.

    Returns:
        dict: Dictionary of command line arguments and their parsed values.

    """
    parser = argparse.ArgumentParser(description=__doc__)

    # This is necessary because on the Windows console "None" doesn't end up
    # getting encoded correctly to print at the console. Somehow.
    default_pudl_in = pudl.workspace.setup.get_defaults()["pudl_in"]
    if default_pudl_in is None:
        default_pudl_in = "None"

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
        '--datastore_dir',
        type=str,
        help="""Directory where the datastore should be located. (default:
        %(default)s).""",
        default=default_pudl_in,
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
        dest='years',
        nargs='+',
        help="""List of years for which data should be downloaded. Different
        data sources have differet valid years. If data is not available for a
        specified year and data source, it will be ignored. If no years are
        specified, all available data will be downloaded for all requested data
        sources.""",
        default=[]
    )
    parser.add_argument(
        '--no_download',
        '-n',
        action='store_false',
        dest='download',
        help="""Do not attempt to download fresh data from the original
        sources. Instead assume that the zipfiles or other original data is
        already present, and organize it locally.""",
        default=True
    )
    parser.add_argument(
        '-t',
        '--states',
        nargs='+',
        choices=pc.cems_states.keys(),
        type=str.upper,
        help="""List of two letter US state abbreviations indicating which
        states data should be downloaded. Currently only applicable to the
        EPA's CEMS dataset.""",
        default=pc.cems_states.keys()
    )
    arguments = parser.parse_args(argv[1:])
    return arguments


def main():
    """Manage and update the PUDL datastore."""
    # Display logged output from the PUDL package:
    logger = logging.getLogger(pudl.__name__)
    log_format = '%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s'
    coloredlogs.install(fmt=log_format, level='INFO', logger=logger)

    args = parse_command_line(sys.argv)

    # Generate a list of valid years of data to download for each data source.
    # If no years were specified, use the full set of valid years.
    # If years were specified, keep only th years which are valid for that
    # data source, and optionally output a message saying which years are
    # being ignored because they aren't valid.
    years_by_source = {}
    for source in args.sources:
        if not args.years:
            years_by_source[source] = pc.data_years[source]
        else:
            years_by_source[source] = [int(year) for year in args.years
                                       if int(year) in pc.data_years[source]]
            if source == "epaipm":
                years_by_source[source] = pc.data_years[source]
                continue
            bad_years = [int(year) for year in args.years
                         if int(year) not in pc.data_years[source]]
            if args.verbose and bad_years:
                warnings.warn(f"Invalid {source} years ignored: {bad_years}.")

    datastore.parallel_update(
        sources=args.sources,
        years_by_source=years_by_source,
        states=args.states,
        data_dir=str(pathlib.Path(args.datastore_dir, "data")),
        clobber=args.clobber,
        unzip=args.unzip,
        dl=args.download,
    )


if __name__ == '__main__':
    sys.exit(main())
