"""Test the PUDL console scripts from within PyTest."""
import pytest
import sqlalchemy as sa

from pudl.workspace.setup import PudlPaths


@pytest.mark.parametrize(
    "command",
    [
        "pudl_datastore -d ferc60 --gcs-cache-path gs://internal-zenodo-cache.catalyst.coop --partition year=2006",
        "pudl_datastore --dataset ferc60 --bypass-local-cache --partition year=2006",
        "pudl_datastore -d ferc60 --validate --partition year=2006",
        "pudl_datastore -d ferc1 --validate --partition year=2021",
        "pudl_datastore -d ferc2 --validate --partition year=2020",
        "pudl_datastore -d ferc6 --validate --partition year=2021",
        "pudl_datastore -d ferc60 --validate --partition year=2020",
        "pudl_datastore -d ferc714 --validate --partition year=2021",
    ],
)
@pytest.mark.script_launch_mode("inprocess")
def test_pudl_datastore(script_runner, command: str):
    """CLI tests specific to the pudl_datastore script."""
    runner_args = command.split(" ")
    ret = script_runner.run(runner_args, print_result=True)
    assert ret.success


@pytest.mark.script_launch_mode("inprocess")
def test_pudl_service_territories(script_runner, pudl_engine: sa.Engine):
    """CLI tests specific to the pudl_service_territories script.

    Depends on the ``pudl_engine`` fixture to ensure that the censusdp1tract.sqlite
    database has been generated, since that data is required for the script to run.
    """
    command = f"pudl_service_territories --entity-type ba --no-dissolve --limit-by-state -o {PudlPaths().output_dir}"
    runner_args = command.split(" ")
    ret = script_runner.run(runner_args, print_result=True)
    assert ret.success
