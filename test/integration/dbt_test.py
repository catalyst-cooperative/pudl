import logging
import os
import shutil
from contextlib import chdir
from pathlib import Path

import duckdb
import pytest

from dbt.cli.main import dbtRunner, dbtRunnerResult
from pudl.io_managers import PudlMixedFormatIOManager

logger = logging.getLogger(__name__)

# Ensure that httpfs is installed before doing any multi-threaded dbt operations
# This prevents errors where multiple threads attempt to install the extension, and
# all but the first one finds that it's mysteriously already been installed. Only
# important if we are running dbt using multiple threads.
duckdb.execute("FORCE INSTALL httpfs")


@pytest.mark.xfail(reason="Gas capacity factor & SEC 10-K tests are failing")
def test_dbt(
    pudl_io_manager: PudlMixedFormatIOManager,
    test_dir: Path,
    request,
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

    print("Initializing dbt test runner")
    dbt = dbtRunner()

    # Change to the dbt directory so we can run dbt commands
    with chdir(test_dir.parent / "dbt"):
        _deps_result: dbtRunnerResult = dbt.invoke(["deps"])
        _seed_result: dbtRunnerResult = dbt.invoke(
            [
                "seed",
                "--threads",
                "1",
            ]
        )
        _build_result: dbtRunnerResult = dbt.invoke(
            [
                "build",
                "--threads",
                "1",
                "--target",
                dbt_target,
            ]
        )
        test_result: dbtRunnerResult = dbt.invoke(
            [
                "test",
                "--threads",
                "1",
                "--store-failures",
                "--target",
                dbt_target,
            ]
        )

    total_tests = len(test_result.result)
    passed_tests = len([r for r in test_result.result if r.status == "pass"])
    logger.info(f"{passed_tests}/{total_tests} dbt tests passed")
    if passed_tests < total_tests:
        logger.error("Failed dbt tests:")
        for r in test_result.result:
            if r.status != "pass":
                logger.error(f"{r.node.name}: {r.status}")

    db_path = Path(os.environ["PUDL_OUTPUT"]) / "pudl_dbt_tests.duckdb"
    # copy the output database to a known location if we are in CI
    # so it can be uploaded as an artifact
    if os.getenv("GITHUB_ACTIONS", False):
        shutil.move(db_path, test_dir.parent / "pudl_dbt_tests.duckdb")
    assert test_result.success
