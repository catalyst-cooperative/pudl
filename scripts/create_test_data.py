#!/usr/bin/env python
"""A script to compile a minimal dataset for continuous integration tests.

Continuous integration is really meant to test our code, not data acquisition
or quality. In addition, there have sometimes been issues with downloading data
to the build server, which make testing frustrating. In order to
compartmentalize the code testing and continuous integration, it seems like a
good idea to set up a standalone dataset for testing. We'll still need a
separate locally run test for the datastore management.

In addition, we

"""

import logging
import os
import sys
import argparse
import shutil
import zipfile
import pudl
from pudl.settings import SETTINGS
import pudl.constants as pc

# Create a logger to output any messages we might have...
logger = logging.getLogger(pudl.__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(message)s')
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

    parser.add_argument(
        '-d',
        '--datadir',
        type=str,
        help="""Path to the test data directory. (default:
        %(default)s).""",
        default=os.path.join(SETTINGS['pudl_dir'], 'test', 'data')
    )
    parser.add_argument(
        '-s',
        '--sources',
        nargs='+',
        choices=['ferc1', 'epacems'],
        help="""List of data sources from which to grab data.
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
        '-t',
        '--states',
        nargs='+',
        choices=pc.cems_states.keys(),
        help="""List of two letter US state abbreviations indicating which
        states data should be downloaded. Currently only applicable to the EPA
        CEMS dataset.""",
        default=pc.cems_states.keys()
    )

    arguments = parser.parse_args(argv[1:])
    return arguments


def main():
    """Main function controlling flow of the script.

    Assumes you have a local datastore, and need to copy a small subset of it
    over into the Travis CI test data directory.
    """

    args = parse_command_line(sys.argv)

    # If no years were specified, use the most recent year of data.
    # If years were specified, keep only the years which are valid for that
    # data source, and optionally output a message saying which years are
    # being ignored because they aren't valid.
    yrs_by_src = {}
    for src in args.sources:
        if not args.year:
            yrs_by_src[src] = [max(pc.data_years[src])]
        else:
            yrs_by_src[src] = [int(yr) for yr in args.year
                               if int(yr) in pc.data_years[src]]
            bad_yrs = [int(yr) for yr in args.year
                       if int(yr) not in pc.data_years[src]]
            if bad_yrs:
                logger.warning(f"Invalid {src} years ignored: {bad_yrs}.")

    test_datadir = os.path.join(pudl.settings.SETTINGS['test_dir'], 'data')
    for src in args.sources:
        for yr in yrs_by_src[src]:
            src_dir = pudl.datastore.path(src, year=yr, file=False)
            tmp_dir = os.path.join(test_datadir, 'tmp')

            if src == 'ferc1':
                files_to_move = [f"{pc.ferc1_tbl2dbf[f]}.DBF" for f in
                                 pc.ferc1_default_tables]
                files_to_move = files_to_move + ['F1_PUB.DBC', 'F1_32.FPT']
            elif src == 'epacems':
                files_to_move = [
                    pudl.datastore.path('epacems', year=yr, state=st, month=mo)
                    for mo in range(1, 13) for st in args.states
                ]
                files_to_move = [os.path.basename(f) for f in files_to_move]
            else:
                raise AssertionError(f"Unrecognized data source {src}")

            if not os.path.exists(tmp_dir):
                os.makedirs(tmp_dir)
            logger.info(f"src: {src_dir}")
            logger.info(f"tmp: {tmp_dir}")
            src_files = [os.path.join(src_dir, f) for f in files_to_move]
            dst_files = [os.path.join(tmp_dir, f) for f in files_to_move]

            for src_file, dst_file in zip(src_files, dst_files):
                if os.path.exists(dst_file):
                    os.remove(dst_file)
                shutil.copy(src_file, dst_file)

            if src == 'ferc1':
                ferc1_test_zipfile = os.path.join(test_datadir, f"f1_{yr}.zip")
                z = zipfile.ZipFile(ferc1_test_zipfile, mode='w',
                                    compression=zipfile.ZIP_DEFLATED)
                for root, dirs, files in os.walk(tmp_dir):
                    for filename in files:
                        z.write(os.path.join(root, filename), arcname=filename)
                logger.info(f"closing {ferc1_test_zipfile}")
                z.close()
                shutil.move(ferc1_test_zipfile, tmp_dir)
                for f in dst_files:
                    os.remove(f)

            logger.info(f"organizing datastore for {src} {yr}")
            pudl.datastore.organize(src, yr, states=args.states,
                                    datadir=test_datadir, unzip=False)


if __name__ == '__main__':
    sys.exit(main())
