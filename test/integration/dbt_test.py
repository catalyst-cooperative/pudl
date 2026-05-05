import json
import re

import pytest
from click.testing import CliRunner

from pudl.dbt_wrapper import install_dbt_deps
from pudl.scripts.dbt_helper import dbt_helper


@pytest.fixture(scope="module")
def dbt_dependencies() -> None:
    """Install dbt package dependencies for tests that exercise dbt commands."""
    install_dbt_deps()


@pytest.mark.script_launch_mode("inprocess")
@pytest.mark.usefixtures("prebuilt_outputs")
def test_update_tables(dbt_target: str, script_runner):
    """Run update-tables. Should detect everything already exists, and do nothing.

    This test relies on the prebuilt outputs so the Parquet files are available.
    """
    args = [
        "dbt_helper",
        "update-tables",
        # "--schema",  # Uncomment when we have schema-preserving updates
        "all",
    ]
    if dbt_target == "etl-full":
        args.append("--row-counts")
    ret = script_runner.run(
        args,
        print_result=True,
    )
    assert ret.success


def test_validate_asset_selection(dbt_dependencies):
    runner = CliRunner()
    result = runner.invoke(
        dbt_helper,
        [
            "validate",
            "--dry-run",
            "--asset-select",
            '+key:"core_eia860_*"',
        ],
    )
    output = result.output
    if "node_selection" not in output:
        raise AssertionError(f"Unexpected output: {output}")
    params_match = re.search(r"({.+})", output)
    if params_match is None:
        raise AssertionError(f"Could not parse JSON parameters from output: {output}")
    out_params = json.loads(params_match.group(0))
    obs_node_selection = out_params["node_selection"].split(" ")
    # just need to know that the key got expanded at all - specifics of expansion tested in dbt_wrapper_test
    assert len(obs_node_selection) > 1
