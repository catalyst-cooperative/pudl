"""Convert the US Census DP1 ESRI GeoDatabase into an SQLite Database.

This is a thin wrapper around the GDAL ogr2ogr command line tool. We use it
to convert the Census DP1 data which is distributed as an ESRI GeoDB into an
SQLite DB. The module provides ogr2ogr with the Census DP 1 data from the
PUDL datastore, and directs it to be output into the user's SQLite directory
alongside our other SQLite Databases (ferc1.sqlite and pudl.sqlite)

Note that the ogr2ogr command line utility must be available on the user's
system for this to work. This tool is part of the ``pudl-dev`` conda
environment, but if you are using PUDL outside of the conda environment, you
will need to install ogr2ogr separately. On Debian Linux based systems such
as Ubuntu it can be installed with ``sudo apt-get install gdal-bin`` (which
is what we do in our CI setup and Docker images.)
"""

import argparse
import os
import subprocess  # nosec: B404
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import pudl
from pudl.workspace.datastore import Datastore

logger = pudl.logging_helpers.get_logger(__name__)


def censusdp1tract_to_sqlite(pudl_settings=None, year=2010, ds=None, clobber=False):
    """Use GDAL's ogr2ogr utility to convert the Census DP1 GeoDB to an SQLite DB.

    The Census DP1 GeoDB is read from the datastore, where it is stored as a
    zipped archive. This archive is unzipped into a temporary directory so
    that ogr2ogr can operate on the ESRI GeoDB, and convert it to SQLite. The
    resulting SQLite DB file is put in the PUDL output directory alongside the
    ferc1 and pudl SQLite databases.

    Args:
        pudl_settings (dict): A PUDL settings dictionary.
        year (int): Year of Census data to extract (currently must be 2010)

    Returns:
        None
    """
    if ds is None:
        ds = Datastore()
    # If we're in a conda environment, use the version of ogr2ogr that has been
    # installed by conda. Otherwise, try and use a system installed version
    # at /usr/bin/ogr2ogr  This allows us to avoid simply running whatever
    # program happens to be in the user's path and named ogr2ogr. This is a
    # fragile solution that will not work on all platforms, but should cover
    # conda environments, Docker, and continuous integration on GitHub.
    ogr2ogr = os.environ.get("CONDA_PREFIX", "/usr") + "/bin/ogr2ogr"
    # Extract the sippzed GeoDB archive from the Datastore into a temporary
    # directory so that ogr2ogr can operate on it. Output the resulting SQLite
    # database into the user's PUDL workspace. We do not need to keep the
    # unzipped GeoDB around after this conversion. Using a temporary directory
    # makes the cleanup automatic.
    with TemporaryDirectory() as tmpdir:
        # Use datastore to grab the Census DP1 zipfile
        tmpdir_path = Path(tmpdir)
        zip_ref = ds.get_zipfile_resource("censusdp1tract", year=year)
        extract_root = tmpdir_path / Path(zip_ref.filelist[0].filename)
        out_path = Path(pudl_settings["pudl_out"]) / "censusdp1tract.sqlite"

        if out_path.exists():
            if clobber:
                out_path.unlink()
            else:
                raise SystemExit(
                    "The Census DB already exists, and we don't want to clobber it.\n"
                    f"Move {out_path} aside or set clobber=True and try again."
                )

        logger.info("Extracting the Census DP1 GeoDB to %s", out_path)
        zip_ref.extractall(tmpdir_path)
        logger.info("extract_root = %s", extract_root)
        logger.info("out_path = %s", out_path)
        subprocess.run(  # nosec: B603 Trying to use absolute paths.
            [ogr2ogr, str(out_path), str(extract_root)], check=True
        )


def parse_command_line(argv):
    """Parse command line arguments. See the -h option.

    Args:
        argv (str): Command line arguments, including caller filename.

    Returns:
        dict: Dictionary of command line arguments and their parsed values.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--logfile",
        default=None,
        type=str,
        help="If specified, write logs to this file.",
    )
    parser.add_argument(
        "-c",
        "--clobber",
        action="store_true",
        help="""Clobber existing sqlite database if it exists. If clobber is
        not included but the sqlite databse already exists the _build will
        fail.""",
        default=False,
    )
    parser.add_argument(
        "--sandbox",
        action="store_true",
        default=False,
        help="Use the Zenodo sandbox rather than production",
    )
    parser.add_argument(
        "--gcs-cache-path",
        type=str,
        help="Load datastore resources from Google Cloud Storage. Should be gs://bucket[/path_prefix]",
    )
    parser.add_argument(
        "--bypass-local-cache",
        action="store_true",
        default=False,
        help="If enabled, the local file cache for datastore will not be used.",
    )
    parser.add_argument(
        "--loglevel",
        help="Set logging level (DEBUG, INFO, WARNING, ERROR, or CRITICAL).",
        default="INFO",
    )
    arguments = parser.parse_args(argv[1:])
    return arguments


def main():
    """Convert the Census DP1 GeoDatabase into an SQLite Database."""
    args = parse_command_line(sys.argv)
    pudl.logging_helpers.configure_root_logger(
        logfile=args.logfile, loglevel=args.loglevel
    )
    pudl_settings = pudl.workspace.setup.get_defaults()

    # Configure how we want to obtain raw input data:
    ds_kwargs = dict(
        gcs_cache_path=args.gcs_cache_path, sandbox=pudl_settings.get("sandbox", False)
    )
    if not args.bypass_local_cache:
        ds_kwargs["local_cache_path"] = Path(pudl_settings["pudl_in"]) / "data"

    ds = Datastore(**ds_kwargs)

    pudl_settings["sandbox"] = args.sandbox

    censusdp1tract_to_sqlite(pudl_settings=pudl_settings, ds=ds, clobber=args.clobber)


if __name__ == "__main__":
    sys.exit(main())
