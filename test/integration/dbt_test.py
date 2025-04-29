import logging
import os
import shutil
from contextlib import chdir
from pathlib import Path

import pytest

from dbt.cli.main import dbtRunner, dbtRunnerResult
from pudl.io_managers import PudlMixedFormatIOManager

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


def test_dbt(
    pudl_io_manager: PudlMixedFormatIOManager,
    test_dir: Path,
    dbt_target,
):
    """Run the dbt data validations programmatically.

    Because the dbt read data from our Parquet outputs, and the location of the Parquet
    outputs is determined by the PUDL_OUTPUT environment variable, and that environment
    variable is set during the test setup, we shouldn't need to do any special setup
    here to point dbt at the outputs.

    The dependency on pudl_io_manager is necessary because it ensures that the dbt
    tests don't run until after the ETL has completed and the Parquet files are
    available.

    See https://docs.getdbt.com/reference/programmatic-invocations/ for more details on
    how to invoke dbt programmatically.
    """

    # NOTE 2025-03-14: running this with more threads was causing segfaults
    logger.info("Initializing dbt test runner")
    dbt = dbtRunner()
    cli_args = [
        "--store-failures",
        "--threads",
        "1",
        "--target",
        dbt_target,
    ]

    # Change to the dbt directory so we can run dbt commands
    with chdir(test_dir.parent / "dbt"):
        _ = dbt.invoke(["deps"])
        _ = dbt.invoke(["seed"])
        _ = dbt.invoke(["build"] + cli_args)
        test_result: dbtRunnerResult = dbt.invoke(["test"] + cli_args)

    # copy the output database to a known location if we are in CI
    # so it can be uploaded as an artifact
    if os.getenv("GITHUB_ACTIONS", False):
        db_path = Path(os.environ["PUDL_OUTPUT"]) / "pudl_dbt_tests.duckdb"
        if db_path.exists():
            logger.info("PUDL dbt tests DB exists.")
            shutil.move(db_path, test_dir.parent / "pudl_dbt_tests.duckdb")

    assert test_result.success


@pytest.mark.script_launch_mode("inprocess")
def test_dbt_helper(
    pudl_io_manager: PudlMixedFormatIOManager,
    dbt_target: str,
    script_runner,
):
    """Run add-tables. Should detect everything already exists, and do nothing.

    The dependency on pudl_io_manager is necessary because it ensures that the dbt
    tests don't run until after the ETL has completed and the Parquet files are
    available.
    """
    ret = script_runner.run(
        [
            "dbt_helper",
            "add-tables",
            "--target",
            dbt_target,
            "--use-local-tables",
            "all",
        ],
        print_result=True,
    )
    assert ret.success
