"""Test the PUDL console scripts from within PyTest."""
from pathlib import Path

import pytest
import sqlalchemy as sa


@pytest.mark.parametrize(
    "command",
    [
        # Force the download of one small partition from Zenodo
        "pudl_datastore --dataset ferc60 --bypass-local-cache --partition year=2020",
        # Force the download of one small partition from GCS
        "pudl_datastore -d ferc60 --bypass-local-cache --gcs-cache-path gs://zenodo-cache.catalyst.coop --partition year=2020",
        # Exercise the datastore validation code
        "pudl_datastore -d ferc60 --validate --partition year=2020",
        # Ensure that all data source partitions are legible
        "pudl_datastore --list-partitions --gcs-cache-path gs://zenodo-cache.catalyst.coop",
    ],
)
@pytest.mark.script_launch_mode("inprocess")
def test_pudl_datastore(script_runner, command: str):
    """CLI tests specific to the pudl_datastore script."""
    runner_args = command.split(" ")
    ret = script_runner.run(runner_args, print_result=True)
    assert ret.success


# @pytest.mark.skip("Test exceeds memory or disk limits of GitHub runner.")
@pytest.mark.script_launch_mode("inprocess")
def test_pudl_service_territories(
    script_runner, tmp_path: Path, pudl_engine: sa.Engine
):
    """CLI tests specific to the pudl_service_territories script.

    Depends on the ``pudl_engine`` fixture to ensure that the censusdp1tract.sqlite
    database has been generated, since that data is required for the script to run.
    """
    out_path = tmp_path / "balancing_authority_geometry.parquet"
    assert not out_path.exists()
    command = (
        f"pudl_service_territories --entity-type ba -y 2022 --no-dissolve -o {tmp_path}"
    )
    ret = script_runner.run(command.split(" "), print_result=True)
    assert ret.success
    assert out_path.exists()
    assert out_path.is_file()
