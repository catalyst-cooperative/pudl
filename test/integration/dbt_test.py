"""Integration tests for dbt validations and helper commands on prebuilt outputs."""

import contextlib
import json
import logging
import re
from pathlib import Path

import pytest
from click.testing import CliRunner

from pudl.dbt_wrapper import build_with_context
from pudl.scripts.dbt_helper import dbt_helper

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def dbt_target(etl_settings_path: Path) -> str:
    """Fixture defining the dbt target based on the referenced ETL settings."""
    if etl_settings_path.name == "etl_full.yml":
        dbt_target = "etl-full"
    elif etl_settings_path.name == "etl_fast.yml":
        dbt_target = "etl-fast"
    else:
        raise ValueError(f"Unexpected ETL settings file: {etl_settings_path}")
    return dbt_target


@pytest.mark.order(4)
@pytest.mark.usefixtures("prebuilt_outputs", "test_dir")
def test_dbt(dbt_target: str):
    """Run the dbt data validations programmatically.

    Because dbt reads data from our Parquet outputs, and the location of the Parquet
    outputs is determined by the PUDL_OUTPUT environment variable, and that environment
    variable is set during the test setup, we shouldn't need to do any special setup
    here to point dbt at the correct outputs.

    This test relies on the prebuilt outputs so the Parquet files are available.

    Note that the row count checks will automatically be disabled unless dbt_target is
    'etl-full'. See the ``check_row_counts_per_partition.sql` generic test.
    """
    test_result = build_with_context(
        node_selection="*",
        dbt_target=dbt_target,
    )

    if not test_result.success:
        raise AssertionError(
            f"failure contexts:\n{test_result.format_failure_contexts()}"
        )


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


# Has to run after test_dbt above otherwise dbt dependencies aren't installed
@pytest.mark.order(5)
@pytest.mark.xfail(reason="Logs swallowed by pytest. Revisit when click >=8.3.2")
def test_validate_asset_selection(caplog):
    caplog.set_level(logging.INFO)
    runner = CliRunner()
    # Workaround for https://github.com/pallets/click/issues/3110
    # Use isolation() directly instead of invoke() to avoid "ValueError: I/O operation on closed file"
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
