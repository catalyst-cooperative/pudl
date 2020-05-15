"""
Merge a compatible PUDL datapackages and load the result into an SQLite DB.

This script merges a set of compatible PUDL datapackages into a single
tabular datapackage that can be loaded into an SQLite database (or potentially
other storage media like Google BigQuery, PostgreSQL, etc.). The input
datapackages must all have been produced in the same ETL run, and share the
same ``datapkg-bundle-uuid`` value. Any data sources (e.g. ferc1, eia923) that
appear in more than one of the datapackages to be merged must also share
identical ETL parameters (years, tables, states, etc.), allowing easy
deduplication of resources.

Having the ability to load only a subset of the datapackages resulting from an
ETL run into the SQLite database is helpful because larger datasets like the
EPA CEMS hourly emissions table which have ~1 billion records and take up
~100 GB of space when uncompressed are much easier to work with via columnar
datastores like Apache Parquet -- loading all of EPA CEMS into SQLite can take
more than 24 hours. PUDL also provides a separate epacems_to_parquet script
that can be used to generate a Parquet dataset that is partitioned by state
and year, which can be read directly into pandas or dask dataframes, for use
in conjunction with the other PUDL data that is stored in the SQLite DB.

"""

import argparse
import logging
import pathlib
import shutil
import sys

import coloredlogs
import datapackage
import sqlalchemy as sa
from tableschema import exceptions

import pudl
from pudl.convert.merge_datapkgs import merge_datapkgs

logger = logging.getLogger(__name__)


def datapkg_to_sqlite(sqlite_url, out_path, clobber=False):
    """
    Load a PUDL datapackage into a sqlite database.

    Args:
        sqlite_url (str): An SQLite database connection URL.
        out_path (path-like): Path to the base directory of the datapackage
            to be loaded into SQLite. Must contain the datapackage.json file.
        clobber (bool): If True, replace an existing PUDL DB if it exists. If
            False (the default), fail if an existing PUDL DB is found.

    Returns:
        None

    """
    # prepping the sqlite engine
    pudl_engine = sa.create_engine(sqlite_url)
    logger.info("Dropping the current PUDL DB, if it exists.")
    try:
        # So that we can wipe it out
        pudl.helpers.drop_tables(pudl_engine, clobber=clobber)
    except sa.exc.OperationalError:
        pass
    # And start anew
    pudl_engine = sa.create_engine(sqlite_url)

    # grab the merged datapackage metadata file:
    pkg = datapackage.DataPackage(
        descriptor=str(pathlib.Path(out_path, 'datapackage.json')))
    # we want to grab the dictionary of columns that need autoincrement id cols
    try:
        autoincrement = pkg.descriptor['autoincrement']
    # in case there is no autoincrement columns in the metadata..
    except KeyError:
        autoincrement = {}

    logger.info("Loading merged datapackage into SQLite.")
    logger.info("This could take a while. It might be a good time")
    logger.info("to get a drink of water. Hydrate or die!")
    try:
        # Save the data package in SQL
        pkg.save(storage='sql', engine=pudl_engine, merge_groups=True,
                 autoincrement=autoincrement)
    except exceptions.TableSchemaException as exception:
        logger.error('SQLite conversion failed. See following errors:')
        logger.error(exception.errors)


def parse_command_line(argv):
    """
    Parse command line arguments. See the -h option.

    Args:
        argv (str): Command line arguments, including caller filename.

    Returns:
        dict: Dictionary of command line arguments and their parsed values.

    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '-m',
        '--merge_only',
        dest="load_sqlite",
        action="store_false",
        default=True,
        help="""Only merge the datapackages, don't attempt to load the
        resulting datapackage into the SQLite database.""")
    parser.add_argument(
        '-o',
        '--out_path',
        required=True,
        help="""Path to the directory in which the datapackage.json for the
        merged datapackage will be output. The last element of the path will
        also be used as the name of the output datapackage.""")
    parser.add_argument(
        '-c',
        '--clobber',
        action='store_true',
        help="""If the output directory already exists, remove it and replace
        it with the newly merged datapackage. Similarly, overwrite the existing
        sqlite database if it exists. Otherwise, the pre-existing directory or
        database will cause the process to fail.""",
        default=False)
    parser.add_argument(
        'in_paths',
        nargs="+",
        help="""A list of paths to the datapackage.json files containing the
        metadata for the PUDL tabular data packages to be merged and
        potentially loaded into an SQLite database.""")
    arguments = parser.parse_args(argv[1:])
    return arguments


def main():
    """Merge PUDL datapackages and save them into an SQLite database."""
    # Display logged output from the PUDL package:
    logger = logging.getLogger(pudl.__name__)
    log_format = '%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s'
    coloredlogs.install(fmt=log_format, level='INFO', logger=logger)

    args = parse_command_line(sys.argv)
    pudl_settings = pudl.workspace.setup.get_defaults()
    logger.info(f"pudl_in={pudl_settings['pudl_in']}")
    logger.info(f"pudl_out={pudl_settings['pudl_out']}")

    logger.info("Merging datapackages.")
    # Verify that the input data package descriptors exist, and if so create
    # datapackage objects using them:
    dps = []
    for path in args.in_paths:
        if not pathlib.Path(path).exists():
            raise FileNotFoundError(
                f"Input datapackage path {path} does not exist.")
        dps.append(datapackage.DataPackage(descriptor=path))

    # Check whether the output destination exists if we're not clobbering
    out_path = pathlib.Path(args.out_path)
    # If it exists we either fail (because we're not clobbering) or we remove
    # the whole directory (if we are clobbering)
    if out_path.exists():
        if args.clobber is False:
            raise FileExistsError(
                f"Output directory {out_path} exists and clobber is False.")
        shutil.rmtree(out_path)

    merge_datapkgs(dps, out_path, clobber=args.clobber)

    if args.load_sqlite is True:
        logger.info("Loading merged datapackage into an SQLite database.")
        datapkg_to_sqlite(
            pudl_settings['pudl_db'], out_path, clobber=args.clobber)
        logger.info("Success! You can connect to the PUDL DB at this URL:")
        logger.info(f"{pudl_settings['pudl_db']}")
    else:
        logger.info("Not loading merged datapackage into SQLite database.")
