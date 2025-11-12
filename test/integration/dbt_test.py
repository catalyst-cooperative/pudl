import json
import logging
import re
from pathlib import Path

import pytest
from click.testing import CliRunner

from pudl.dbt_wrapper import build_with_context
from pudl.io_managers import PudlMixedFormatIOManager
from pudl.scripts.dbt_helper import dbt_helper

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def dbt_target(test_dir: Path, request) -> str:
    """Fixture defining the dbt target based on the full/fast ETL spec."""
    # Identify whether we're running the full or fast ETL, and set the dbt target
    # appropriately (since we have different test expectations in the two cases)
    if request.config.getoption("--etl-settings"):
        etl_settings_yml = Path(request.config.getoption("--etl-settings"))
    else:
        etl_settings_yml = Path(
            test_dir.parent / "src/pudl/package_data/settings/etl_fast.yml"
        )
    if etl_settings_yml.name == "etl_full.yml":
        dbt_target = "etl-full"
    elif etl_settings_yml.name == "etl_fast.yml":
        dbt_target = "etl-fast"
    else:
        raise ValueError(f"Unexpected ETL settings file: {etl_settings_yml}")
    return dbt_target


@pytest.mark.order(4)
def test_dbt(
    pudl_io_manager: PudlMixedFormatIOManager,
    test_dir: Path,
    dbt_target: str,
):
    """Run the dbt data validations programmatically.

    Because dbt reads data from our Parquet outputs, and the location of the Parquet
    outputs is determined by the PUDL_OUTPUT environment variable, and that environment
    variable is set during the test setup, we shouldn't need to do any special setup
    here to point dbt at the correct outputs.

    The dependency on pudl_io_manager is necessary because it ensures that the dbt
    tests don't run until after the ETL has completed and the Parquet files are
    available.

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
def test_update_tables(
    dbt_target: str,
    pudl_io_manager: PudlMixedFormatIOManager,
    script_runner,
):
    """Run update-tables. Should detect everything already exists, and do nothing.

    The dependency on pudl_io_manager is necessary because it ensures that the dbt
    tests don't run until after the ETL has completed and the Parquet files are
    available.
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
def test_validate_asset_selection():
    runner = CliRunner()
    result = runner.invoke(
        dbt_helper,
        ["validate", "--dry-run", "--asset-select", '+key:"core_eia860_*"'],
    )
    output = result.output
    if "node_selection" not in result.output:
        raise AssertionError(f"Unexpected output: {output}")
    out_params = json.loads(re.search(r"({.+})", output).group(0))
    obs_node_selection = out_params["node_selection"].split(" ")
    # just need to know that the key got expanded at all - specifics of expansion tested in dbt_wrapper_test
    assert len(obs_node_selection) > 1
