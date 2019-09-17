"""
Convert a set of datapackages to SQLite database.

This script will convert a bundle of datapackages into one SQLite database.
First, it flattens that datapackages into one datapackage named 'pudl-all'.
Then, it converts that flattened pudl datapackage into a SQLite database.

You will need to give the name of the directory which contains datapackages
that you want to flatten and convert. This directory needs to be in your
"PUDL_OUT" and "datapackage" directory. To see more info on directory setup see
the pudl_setup script (pudl_setup --help for more details).

"""

import argparse
import logging
import pathlib
import sys

import coloredlogs
import sqlalchemy as sa
from datapackage import Package
from tableschema import exceptions

import pudl

logger = logging.getLogger(__name__)


def pkg_to_sqlite_db(pudl_settings,
                     pkg_bundle_name,
                     pkg_name=None):
    """
    Turn a data package into a sqlite database.

    Args:
        pudl_settings (dict) : a dictionary filled with settings that mostly
            describe paths to various resources and outputs.
        pkg_name (str): name of data package. this can be the flattened
            datapackge (by default named 'pudl-all') or any of the sub-
            datapackages.
        pkg_bundle_name (str): the name of the directory where the bundle
            of datapackages live that you want to convert.

    """
    # prepping the sqlite engine
    pudl_engine = sa.create_engine(pudl_settings['pudl_db'])
    logger.info("Dropping the current PUDL DB, if it exists.")
    try:
        # So that we can wipe it out
        pudl.helpers.drop_tables(pudl_engine)
    except sa.exc.OperationalError:
        pass
    # And start anew
    pudl_engine = sa.create_engine(pudl_settings['pudl_db'])
    # we can assume the flattened package's name
    if not pkg_name:
        pkg_name = 'pudl-all'
    # grabbing the datapackage
    pkg = Package(str(pathlib.Path(pudl_settings['datapackage_dir'],
                                   pkg_bundle_name,
                                   pkg_name, 'datapackage.json')))
    # we want to grab the dictionary of columns that need autoincrement id cols
    try:
        autoincrement = pkg.descriptor['autoincrement']
    # in case there is no autoincrement columns in the metadata..
    except KeyError:
        autoincrement = {}

    logger.info("Loading the data package into SQLite.")
    logger.info("If you're loading EPA CEMS, this could take a while.")
    logger.info("It might be a good time to get lunch, or go for a bike ride.")
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
        '--pkg_bundle_name',
        default="",
        help="""Name for data package bundle directory. If no name is given the
        default is the pudl python package version.""")
    arguments = parser.parse_args(argv[1:])
    return arguments


def main():
    """Convert a set of datapackages to a sqlite database."""
    # Display logged output from the PUDL package:
    logger = logging.getLogger(pudl.__name__)
    log_format = '%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s'
    coloredlogs.install(fmt=log_format, level='INFO', logger=logger)
    args = parse_command_line(sys.argv)
    logger.info("Determining PUDL data management environment.")
    pudl_in = pudl.workspace.setup.get_defaults()["pudl_in"]
    pudl_out = pudl.workspace.setup.get_defaults()["pudl_out"]
    logger.info(f"pudl_in={pudl_in}")
    logger.info(f"pudl_out={pudl_out}")
    pudl_settings = pudl.workspace.setup.derive_paths(
        pudl_in=pudl_in, pudl_out=pudl_out)

    logger.info(f"Flattening datapackages within {args.pkg_bundle_name}.")
    pudl.convert.flatten_datapkgs.flatten_pudl_datapackages(
        pudl_settings,
        pkg_bundle_name=args.pkg_bundle_name,
        pkg_name='pudl-all'
    )

    logger.info(f"Converting flattened datapackage into an SQLite database.")
    pudl.convert.datapkg_to_sqlite.pkg_to_sqlite_db(
        pudl_settings,
        pkg_bundle_name=args.pkg_bundle_name,
        pkg_name='pudl-all')
    logger.info(f"Success! You can connect to the PUDL DB using this URL:")
    logger.info(f"{pudl_settings['pudl_db']}")
