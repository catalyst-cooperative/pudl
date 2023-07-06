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

import os
import subprocess  # nosec: B404
from pathlib import Path
from tempfile import TemporaryDirectory

from dagster import Field, asset

import pudl
from pudl.workspace.datastore import Datastore
from pudl.workspace.setup import PudlPaths

logger = pudl.logging_helpers.get_logger(__name__)


@asset(
    config_schema={
        "clobber": Field(
            bool, description="Clobber existing Census database.", default_value=True
        ),
        "year": Field(
            int,
            description="Year of Census data to extract (currently must be 2010).",
            default_value=2010,
        ),
    },
    required_resource_keys={"datastore", "pudl_paths"},
)
def censusdp1tract_to_sqlite(context):
    """Use GDAL's ogr2ogr utility to convert the Census DP1 GeoDB to an SQLite DB.

    The Census DP1 GeoDB is read from the datastore, where it is stored as a
    zipped archive. This archive is unzipped into a temporary directory so
    that ogr2ogr can operate on the ESRI GeoDB, and convert it to SQLite. The
    resulting SQLite DB file is put in the PUDL output directory alongside the
    ferc1 and pudl SQLite databases.

    Returns:
        None
    """
    ds = context.resources.datastore
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
        zip_ref = ds.get_zipfile_resource(
            "censusdp1tract", year=context.op_config["year"]
        )
        extract_root = tmpdir_path / Path(zip_ref.filelist[0].filename)
        out_path = PudlPaths().output_dir / "censusdp1tract.sqlite"

        if out_path.exists():
            if context.op_config["clobber"]:
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
    return out_path
