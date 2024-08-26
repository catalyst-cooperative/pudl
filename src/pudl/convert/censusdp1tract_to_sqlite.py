"""Convert the US Census DP1 ESRI GeoDatabase into an SQLite Database.

This is a thin wrapper around the GDAL ogr2ogr command line tool. We use it to convert
the Census DP1 data which is distributed as an ESRI GeoDB into an SQLite DB. The module
provides ogr2ogr with the Census DP 1 data from the PUDL datastore, and writes the
output into the output directory indicated by the ``$PUDL_OUTPUT`` environment variable
alongside the FERC and PUDL SQLite databases.
"""

import os
import subprocess  # noqa: S404
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
    required_resource_keys={"datastore"},
)
def raw_censusdp1tract__all_tables(context):
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
    ogr2ogr = Path(os.environ.get("CONDA_PREFIX", "/usr")) / "bin/ogr2ogr"
    assert ogr2ogr.is_file()
    # Extract the sippzed GeoDB archive from the Datastore into a temporary
    # directory so that ogr2ogr can operate on it. Output the resulting SQLite
    # database into the user's PUDL workspace. We do not need to keep the
    # unzipped GeoDB around after this conversion. Using a temporary directory
    # makes the cleanup automatic.
    with TemporaryDirectory() as tmpdir:
        # Use datastore to grab the Census DP1 zipfile
        tmpdir_path = Path(tmpdir)
        assert tmpdir_path.is_dir()
        with ds.get_zipfile_resource(
            "censusdp1tract", year=context.op_config["year"]
        ) as zip_ref:
            extract_root = tmpdir_path / Path(zip_ref.filelist[0].filename)
            out_dir = PudlPaths().output_dir
            assert out_dir.is_dir()
            out_path = PudlPaths().output_dir / "censusdp1tract.sqlite"

            if out_path.exists():
                if context.op_config["clobber"]:
                    out_path.unlink()
                else:
                    raise SystemExit(
                        "The Census DB already exists, and we don't want to clobber it.\n"
                        f"Move {out_path} aside or set clobber=True and try again."
                    )

            logger.info(f"Extracting the Census DP1 GeoDB to {out_path}")
            zip_ref.extractall(tmpdir_path)
            logger.info(f"extract_root = {extract_root}")
            assert extract_root.is_dir()
            logger.info(f"out_path = {out_path}")
            subprocess.run(  # noqa: S603
                [ogr2ogr, str(out_path), str(extract_root)],
                check=True,
                capture_output=True,
            )
    return out_path
