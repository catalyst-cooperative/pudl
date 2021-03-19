"""
Convert the US Census DP1 ESRI GeoDatabase into an SQLite Database.

This is a thin wrapper around GDAL's ogr2ogr utility, which does the actual
conversion. The code in this module tells that utility where to find the input
data in the PUDL Datastore, and where to output the SQLite DB, alongside our
other SQLite Databases (ferc1.sqlite and pudl.sqlite)

"""

import argparse
import logging
import os
import subprocess  # nosec: B404
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import coloredlogs

import pudl
from pudl.workspace.datastore import Datastore

logger = logging.getLogger(__name__)


def censusdp1tract_to_sqlite(pudl_settings=None, year=2010):
    """
    Use GDAL's ogr2ogr utility to convert the Census DP1 GeoDB to an SQLite DB.

    The Census DP1 GeoDB is read from the datastore, unzipped into a temporary
    directory, and then converted to SQLite using an external process. The
    resulting SQLite DB is put in the PUDL output directory alongside the ferc1
    and pudl SQLite databases.

    Args:
        pudl_settings (dict): A PUDL settings dictionary.
        year (int): Year of Census data to extract (currently must be 2010)

    Returns:
        None

    """
    if pudl_settings is None:
        pudl_settings = pudl.workspace.setup.get_defaults()
    ds = Datastore(local_cache_path=pudl_settings["data_dir"])

    # Avoid executing any random program that happens to be named ogr2ogr
    # If we're in a conda environment, use the ogr2ogr there
    # Otherwise assume it's where Ubuntu installs it /usr/bin/ogr2ogr
    prefix_dir = os.environ.get("CONDA_PREFIX", "/usr")
    ogr2ogr = prefix_dir + "/bin/ogr2ogr"

    # Do all this work in a temporary directory since it's transient and big:
    with TemporaryDirectory() as tmpdir:
        # Use datastore to grab the Census DP1 zipfile
        tmpdir_path = Path(tmpdir)
        zip_ref = ds.get_zipfile_resource("censusdp1tract", year=year)
        extract_root = tmpdir_path / Path(zip_ref.filelist[0].filename)
        out_path = Path(pudl_settings["sqlite_dir"]) / "censusdp1tract.sqlite"
        logger.info("Extracting the Census DP1 GeoDB to %s", out_path)
        zip_ref.extractall(tmpdir_path)
        logger.info("extract_root = %s", extract_root)
        logger.info("out_path = %s", out_path)
        subprocess.run(  # nosec: B603 Trying to use absolute paths.
            [ogr2ogr, str(out_path), str(extract_root)],
            check=True
        )


def parse_command_line(argv):
    """
    Parse command line arguments. See the -h option.

    Args:
        argv (str): Command line arguments, including caller filename.

    Returns:
        dict: Dictionary of command line arguments and their parsed values.

    """
    parser = argparse.ArgumentParser(description=__doc__)
    arguments = parser.parse_args(argv[1:])
    return arguments


def main():
    """Convert the Census DP1 GeoDatabase into an SQLite Database."""
    log_format = '%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s'
    coloredlogs.install(fmt=log_format, level='INFO', logger=logger)

    # Currently have no arguments, but want to generate a usage message.
    _ = parse_command_line(sys.argv)

    censusdp1tract_to_sqlite()


if __name__ == '__main__':
    sys.exit(main())
