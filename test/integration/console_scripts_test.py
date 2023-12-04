"""Test the PUDL console scripts from within PyTest."""
import pytest
import sqlalchemy as sa

from pudl.workspace.setup import PudlPaths


@pytest.mark.parametrize(
    "command",
    [
        # Force the download of one small partition from Zenodo
        "pudl_datastore --dataset ferc60 --bypass-local-cache --partition year=2020",
        # Force the download of one small partition from GCS
        "pudl_datastore -d ferc60 --bypass-local-cache --gcs-cache-path gs://internal-zenodo-cache.catalyst.coop --partition year=2020",
        # Exercise the datastore validation code
        "pudl_datastore -d ferc60 --validate --partition year=2020",
        # Ensure that all data source partitions are legible
        "pudl_datastore --list-partitions --gcs-cache-path gs://internal-zenodo-cache.catalyst.coop",
    ],
)
@pytest.mark.script_launch_mode("inprocess")
def test_pudl_datastore(script_runner, command: str):
    """CLI tests specific to the pudl_datastore script."""
    runner_args = command.split(" ")
    ret = script_runner.run(runner_args, print_result=True)
    assert ret.success


@pytest.mark.skip("Check if this exceeds memory or disk of the GitHub runner.")
@pytest.mark.script_launch_mode("inprocess")
def test_pudl_service_territories(script_runner, pudl_engine: sa.Engine):
    """CLI tests specific to the pudl_service_territories script.

    Depends on the ``pudl_engine`` fixture to ensure that the censusdp1tract.sqlite
    database has been generated, since that data is required for the script to run.
    """
    command = f"pudl_service_territories --entity-type ba --no-dissolve -o {PudlPaths().output_dir}"
    runner_args = command.split(" ")
    ret = script_runner.run(runner_args, print_result=True)
    assert ret.success
    out_path = PudlPaths().output_dir / "balancing_authority_geometry.parquet"
    assert out_path.exists()
    assert out_path.is_file()
