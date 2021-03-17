"""Test the PUDL console scripts from within PyTest."""

import pytest


@pytest.mark.parametrize(
    "script_name", [
        "pudl_setup",
        "pudl_datastore",
        "ferc1_to_sqlite",
        "pudl_etl",
        "datapkg_to_sqlite",
        "epacems_to_parquet",
        "pudl_territories",
    ])
@pytest.mark.script_launch_mode('inprocess')
def test_pudl_setup(script_runner, script_name):
    """Run each console script in --help mode for testing."""
    ret = script_runner.run(script_name, '--help', print_result=False)
    assert ret.success
