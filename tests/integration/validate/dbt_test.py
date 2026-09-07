import json
import re

import pytest
from click.testing import CliRunner

from pudl.scripts.dbt_helper import dbt_helper
from pudl.validate.dbt import install_dbt_deps


@pytest.fixture(scope="module")
def dbt_dependencies() -> None:
    """Install dbt package dependencies for tests that exercise dbt commands."""
    install_dbt_deps()


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
