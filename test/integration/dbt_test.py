"""Integration tests for dbt validations and helper commands on prebuilt outputs."""

import contextlib
import json
import logging
import re

import pytest
from click.testing import CliRunner

from pudl.dbt_wrapper import install_dbt_deps
from pudl.scripts.dbt_helper import dbt_helper

logger = logging.getLogger(__name__)


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


def test_validate_asset_selection(caplog, dbt_dependencies):
    """Verify that dbt_helper expands asset selections in dry-run mode."""
    caplog.set_level(logging.INFO)
    runner = CliRunner()
    # Click 8.3.1 still raises "I/O operation on closed file" in invoke() here,
    # so keep using isolation() until the bundled version actually behaves.
    # See https://github.com/pallets/click/issues/3110
    with runner.isolation(), contextlib.suppress(SystemExit):
        dbt_helper.main(
            args=[
                "validate",
                "--dry-run",
                "--asset-select",
                '+key:"core_eia860_*"',
            ],
            prog_name="dbt_helper",
            standalone_mode=False,
        )

    output = caplog.text
    if "node_selection" not in output:
        raise AssertionError(f"Unexpected output: {output}")
    params_match = re.search(r"({.+})", output)
    if params_match is None:
        raise AssertionError(f"Could not parse JSON parameters from output: {output}")
    out_params = json.loads(params_match.group(0))
    obs_node_selection = out_params["node_selection"].split(" ")
    # just need to know that the key got expanded at all - specifics of expansion tested in dbt_wrapper_test
    assert len(obs_node_selection) > 1
