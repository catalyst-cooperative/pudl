"""Pipeline-level end-to-end tests for the FERC EQR build process.

These tests exercise the full batch pipeline mechanics: starting a Dagster daemon,
submitting a backfill, waiting for the deployment sentinel, and asserting that Parquet
outputs land in the expected location.  They require real FERC EQR raw data in
``$PUDL_INPUT`` and are not gated behind ``--live-pudl-output`` — the point is to
produce and validate the outputs from scratch.

Run explicitly::

    pixi run pytest --no-cov -s tests/pipeline/ferceqr_test.py -v

"""

import json
import logging
import os
import shutil
import subprocess
import time
from collections.abc import Iterator
from pathlib import Path

import polars as pl
import pytest
import yaml
from upath import UPath
from upath.implementations.local import LocalPath

from pudl import PUDL_ROOT_PATH
from pudl.dagster.assets.deploy.ferceqr import FERCEQR_TRANSFORM_ASSETS
from pudl.metadata.classes import PUDL_PACKAGE

logger = logging.getLogger(__name__)

# Two smallest partitions — enough to validate end-to-end flow without running all 51.
FERCEQR_TEST_PARTITIONS = ["2013q3", "2013q4"]
FERCEQR_TIMEOUT_SECONDS = 300  # 5 minutes; local data + two partitions


def _get_local_ferceqr_archive_path(year_quarter: str) -> str:
    """Return a readable local FERCEQR archive path or skip with guidance."""
    archive_path = os.environ.get("PUDL_FERCEQR_ARCHIVE_PATH")
    if not archive_path:
        pytest.skip(
            "FERCEQR pipeline test requires PUDL_FERCEQR_ARCHIVE_PATH to point "
            "at a local archive directory. No archive path was provided."
        )

    if not isinstance(UPath(archive_path), LocalPath):
        pytest.skip(
            "FERCEQR pipeline test requires PUDL_FERCEQR_ARCHIVE_PATH to point "
            f"at a local archive directory. Received non-local path: {archive_path}"
        )

    archive_file = UPath(archive_path) / f"ferceqr-{year_quarter}.zip"
    try:
        with archive_file.open("rb") as archive_stream:
            archive_stream.read(1)
    except Exception as exc:
        pytest.skip(
            "FERCEQR pipeline test requires a readable local archive directory at "
            f"PUDL_FERCEQR_ARCHIVE_PATH. Could not read {archive_file}: {exc}"
        )

    return archive_path


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def ferceqr_deploy_dir(tmp_path_factory) -> Path:
    """Temporary local directory that acts as the deployment target."""
    return tmp_path_factory.mktemp("ferceqr_deploy")


@pytest.fixture(scope="session")
def ferceqr_pudl_output(tmp_path_factory) -> Path:
    """Isolated PUDL_OUTPUT directory for the ferceqr pipeline run."""
    return tmp_path_factory.mktemp("ferceqr_pudl_output")


@pytest.fixture(scope="session")
def ferceqr_dagster_home(tmp_path_factory) -> Path:
    """Temporary DAGSTER_HOME with the ferceqr-specific ``dagster.yaml``."""
    dagster_home = tmp_path_factory.mktemp("ferceqr_dagster_home")
    dagster_yaml = {
        "run_coordinator": {
            "module": "dagster",
            "class": "QueuedRunCoordinator",
            "config": {"max_concurrent_runs": 2},
        }
    }
    (dagster_home / "dagster.yaml").write_text(yaml.dump(dagster_yaml))
    return dagster_home


@pytest.fixture(scope="session")
def ferceqr_deployment_config_path(tmp_path_factory, ferceqr_deploy_dir: Path) -> Path:
    """Temporary deployment-target YAML pointing at the local deploy directory."""
    deployment_config_path = tmp_path_factory.mktemp("ferceqr_config") / "targets.yml"
    deployment_config_path.write_text(
        yaml.safe_dump(
            {
                "deployment_targets": [
                    {
                        "path": str(ferceqr_deploy_dir),
                        "storage_options": {},
                    }
                ]
            }
        )
    )
    return deployment_config_path


@pytest.fixture(scope="session")
def ferceqr_outputs(
    ferceqr_dagster_home: Path,
    ferceqr_deployment_config_path: Path,
    ferceqr_deploy_dir: Path,
    ferceqr_pudl_output: Path,
) -> Iterator[Path]:
    """Run the ferceqr pipeline end-to-end and yield the deploy directory.

    Steps:
    1. Start ``dagster-daemon run`` in the background.
    2. Submit a backfill for ``FERCEQR_TEST_PARTITIONS`` via ``dagster job backfill``.
    3. Poll for ``FERCEQR_SUCCESS`` or ``FERCEQR_FAILURE`` in ``ferceqr_pudl_output``.
    4. Stop the daemon.

    The deployment target is configured via a temporary deployment-target YAML file that
    points at the local ``ferceqr_deploy_dir`` so no cloud credentials are needed.
    """
    dagster_daemon = shutil.which("dagster-daemon")
    dagster_cli = shutil.which("dagster")
    if dagster_daemon is None or dagster_cli is None:
        pytest.skip("dagster-daemon or dagster CLI not found in PATH")

    env = os.environ.copy()
    archive_path = _get_local_ferceqr_archive_path(FERCEQR_TEST_PARTITIONS[0])
    env["DAGSTER_HOME"] = str(ferceqr_dagster_home)
    env["PUDL_OUTPUT"] = str(ferceqr_pudl_output)
    env["PUDL_FERCEQR_ARCHIVE_PATH"] = archive_path
    env["PUDL_FERCEQR_DEPLOYMENT_CONFIG_PATH"] = str(ferceqr_deployment_config_path)
    env.pop("GCP_BILLING_PROJECT", None)
    env["BUILD_ID"] = "pytest-ferceqr"

    # Pre-initialize the Dagster SQLite storage before launching any subprocesses.
    # The daemon and the backfill CLI both call DagsterInstance.get() on startup; if
    # the SQLite database doesn't exist yet they race to stamp the alembic version,
    # causing a UNIQUE constraint error.  Running a tiny Python process here with the
    # correct DAGSTER_HOME ensures the schema is fully migrated before either
    # long-lived subprocess starts.
    subprocess.run(  # noqa: S603
        [
            shutil.which("python") or "python",
            "-c",
            "from dagster import DagsterInstance\nwith DagsterInstance.get(): pass",
        ],
        env=env,
        check=True,
        text=True,
    )

    # Daemon output goes to a log file so it's always available, both live (tail -f)
    # and in failure messages.  The log file path is logged so the user can follow
    # it while the test runs (visible with pytest -s or live log enabled).
    daemon_log_path = ferceqr_dagster_home / "dagster-daemon.log"
    logger.info("[ferceqr] daemon log: %s", daemon_log_path)
    with daemon_log_path.open("w") as daemon_log:
        daemon_proc = subprocess.Popen(  # noqa: S603
            [dagster_daemon, "run"],
            env=env,
            cwd=PUDL_ROOT_PATH,
            stdout=daemon_log,
            stderr=subprocess.STDOUT,
        )
    try:
        # Let the backfill CLI write directly to the terminal so the user can see
        # progress.  Uses cwd=PUDL_ROOT_PATH so dagster finds the [tool.dagster]
        # workspace definition in pyproject.toml.
        subprocess.run(  # noqa: S603
            [
                dagster_cli,
                "job",
                "backfill",
                "--noprompt",
                "--from",
                FERCEQR_TEST_PARTITIONS[0],
                "--to",
                FERCEQR_TEST_PARTITIONS[-1],
                "--job",
                "ferceqr",
            ],
            env=env,
            cwd=PUDL_ROOT_PATH,
            check=True,
        )

        deadline = time.monotonic() + FERCEQR_TIMEOUT_SECONDS
        last_status_print = time.monotonic()
        while time.monotonic() < deadline:
            if (ferceqr_pudl_output / "FERCEQR_SUCCESS").exists():
                yield ferceqr_deploy_dir
                return
            if (ferceqr_pudl_output / "FERCEQR_FAILURE").exists():
                daemon_output = daemon_log_path.read_text()
                pytest.fail(
                    "FERC EQR pipeline test: FERCEQR_FAILURE sentinel written.\n"
                    f"Daemon log ({daemon_log_path}):\n{daemon_output[-4000:]}"
                )
            # Log a heartbeat every 60 s so the user knows the test is alive.
            if time.monotonic() - last_status_print >= 60:
                elapsed = int(time.monotonic() - (deadline - FERCEQR_TIMEOUT_SECONDS))
                remaining = int(deadline - time.monotonic())
                logger.info(
                    "[ferceqr] waiting for sentinel … %ds elapsed, %ds remaining",
                    elapsed,
                    remaining,
                )
                last_status_print = time.monotonic()
            time.sleep(5)

        daemon_output = daemon_log_path.read_text()
        pytest.fail(
            f"FERC EQR pipeline test: timed out after {FERCEQR_TIMEOUT_SECONDS}s.\n"
            f"Daemon log ({daemon_log_path}):\n{daemon_output[-4000:]}"
        )
    finally:
        if daemon_proc.poll() is None:
            daemon_proc.terminate()
            try:
                daemon_proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                daemon_proc.kill()
        for status_name in ("FERCEQR_SUCCESS", "FERCEQR_FAILURE"):
            (ferceqr_pudl_output / status_name).unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("table", FERCEQR_TRANSFORM_ASSETS)
def test_ferceqr_parquet_deployed(ferceqr_outputs: Path, table: str):
    """Test that the deployed FERC EQR data is as expected.

    - Only the specified test partitions are deployed.
    - Each table has the expected Parquet tables.
    - Each table has the expected columns based on the PUDL package schema.
    - Each table has at least some data (not empty)
    """
    table_dir = ferceqr_outputs / table
    assert table_dir.is_dir(), f"Missing deploy directory for table: {table}"
    parquet_files = {path.stem for path in table_dir.glob("*.parquet")}
    assert parquet_files == set(FERCEQR_TEST_PARTITIONS), (
        f"Unexpected Parquet partitions for table {table}: {parquet_files}"
    )
    expected_fields = {f.name for f in PUDL_PACKAGE.get_resource(table).schema.fields}

    # The core_ferceqr__quarterly_identity table outputs the filing_quarter
    # field, even though it does not appear in the PUDL schema.
    if table == "core_ferceqr__quarterly_identity":
        expected_fields = expected_fields | {"filing_quarter"}

    # Use lazy scanning to check metadata without loading all data
    lf = pl.scan_parquet(table_dir / "*.parquet")

    # Check for expected column names
    actual_fields = set(lf.collect_schema().keys())
    assert actual_fields == expected_fields, (
        f"Table {table} column mismatch.\n"
        f"Expected: {expected_fields}\n"
        f"Actual:   {actual_fields}"
    )

    # Check for empty tables
    row_count = lf.select(pl.len()).collect().item()
    assert row_count > 0, f"Table {table} is empty."


def test_ferceqr_datapackage_deployed(ferceqr_outputs: Path):
    """A ``ferceqr_parquet_datapackage.json`` must be written to the deploy directory.

    Also checks that it contains the expected resources corresponding to the transformed
    tables.
    """
    datapackage = ferceqr_outputs / "ferceqr_parquet_datapackage.json"
    assert datapackage.exists(), (
        "ferceqr_parquet_datapackage.json not found in deploy dir"
    )
    assert datapackage.stat().st_size > 0, "ferceqr_parquet_datapackage.json is empty"

    with datapackage.open("r") as f:
        data = json.load(f)

    assert "resources" in data, "datapackage missing 'resources' key"
    resources = data["resources"]
    assert isinstance(resources, list), "'resources' is not an array"

    expected_resources = set(FERCEQR_TRANSFORM_ASSETS)
    # Extract names from the resource objects (assuming they have a 'name' field)
    actual_resources = {r["name"] for r in resources}
    assert actual_resources == expected_resources, (
        f"expected resources {expected_resources}, but found {actual_resources}"
    )


def test_ferceqr_success_sentinel(ferceqr_outputs: Path, ferceqr_pudl_output: Path):
    """The pipeline should leave a success sentinel in place until fixture teardown."""
    assert (ferceqr_pudl_output / "FERCEQR_SUCCESS").exists()
    assert not (ferceqr_pudl_output / "FERCEQR_FAILURE").exists()
