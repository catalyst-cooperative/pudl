"""Pipeline test verifying the Parquet files PUDL distributes use its codec.

PUDL writes Parquet from many places, and each library has its own default codec. Each
writer's own unit tests check that it takes the codec from
:data:`pudl.PARQUET_COMPRESSION`; this checks the files the ETL actually produced for
distribution, so a change that bypasses it is caught.

Run with --live-pudl-output to skip the ETL pre-build and check existing outputs:

    pixi run pytest --no-cov --live-pudl-output \
        tests/pipeline/dagster/parquet_compression_test.py
"""

import pyarrow.parquet as pq
import pytest

import pudl
from pudl.metadata.classes import PUDL_PACKAGE
from pudl.workspace.setup import PudlPaths

MAX_FILES_REPORTED = 25


@pytest.mark.order(2)
@pytest.mark.usefixtures("prebuilt_outputs")
def test_distributed_parquet_files_use_configured_compression(
    pudl_test_paths: PudlPaths,
) -> None:
    """Every table in the PUDL package is compressed with the configured codec.

    The distributed files are the resources of :data:`PUDL_PACKAGE`, each at
    ``parquet/{resource_name}.parquet``. Resources that weren't built in this run are
    skipped, since other tests check that tables exist. In each file found, every
    column chunk of every row group must use :data:`pudl.PARQUET_COMPRESSION`. The
    level isn't recorded in Parquet files, so that is checked by the unit tests of each
    writer.
    """
    parquet_files = {
        resource.name: pudl_test_paths.parquet_path(resource.name)
        for resource in PUDL_PACKAGE.resources
    }
    parquet_files = {
        name: path for name, path in parquet_files.items() if path.exists()
    }
    assert parquet_files, "None of the PUDL package's Parquet files were found."

    codecs = {}
    for name, path in parquet_files.items():
        metadata = pq.read_metadata(path)
        codecs[name] = {
            metadata.row_group(i).column(j).compression
            for i in range(metadata.num_row_groups)
            for j in range(metadata.num_columns)
        }
    # An empty table has no row groups, and so no compressed data to check.
    assert any(codecs.values()), "Every Parquet file is empty, so none can be checked."

    expected = pudl.PARQUET_COMPRESSION.upper()
    wrong_codec = {
        name: sorted(used)
        for name, used in codecs.items()
        if used and used != {expected}
    }
    reported = dict(list(wrong_codec.items())[:MAX_FILES_REPORTED])
    assert not wrong_codec, (
        f"{len(wrong_codec)} of {len(codecs)} PUDL tables aren't all {expected} "
        f"(showing up to {MAX_FILES_REPORTED}, with the codecs they use): {reported}"
    )
