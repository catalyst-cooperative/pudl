"""Pipeline-level end-to-end tests for the FERC EQR build process.

These tests exercise the full batch pipeline mechanics: starting a Dagster daemon,
submitting a backfill, waiting for the deployment sentinel, and asserting that Parquet
outputs land in the expected location.  They require real FERC EQR raw data in
``$PUDL_INPUT`` and are not gated behind ``--live-pudl-output`` — the point is to
produce and validate the outputs from scratch.

Run explicitly::

    pixi run pytest --no-cov -s tests/pipeline/ferceqr_test.py -v

"""

import logging
import os
import shutil
import subprocess
import time
from pathlib import Path

import pytest
import yaml

from pudl import PUDL_ROOT_PATH
from pudl.dagster.assets.deploy.ferceqr import FERCEQR_TRANSFORM_ASSETS

logger = logging.getLogger(__name__)

# Two smallest partitions — enough to validate end-to-end flow without running all 51.
FERCEQR_TEST_PARTITIONS = ["2013q3", "2013q4"]
FERCEQR_TIMEOUT_SECONDS = 300  # 5 minutes; local data + two partitions

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
) -> Path:
    """Run the ferceqr pipeline end-to-end and return the deploy directory.

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
    env["DAGSTER_HOME"] = str(ferceqr_dagster_home)
    env["PUDL_OUTPUT"] = str(ferceqr_pudl_output)
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
                return ferceqr_deploy_dir
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


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_ferceqr_parquet_outputs_exist(ferceqr_outputs: Path):
    """Every FERCEQR_TRANSFORM_ASSETS table must have at least one Parquet file."""
    for table in FERCEQR_TRANSFORM_ASSETS:
        table_dir = ferceqr_outputs / table
        assert table_dir.is_dir(), f"Missing deploy directory for table: {table}"
        parquet_files = list(table_dir.glob("*.parquet"))
        assert parquet_files, f"No Parquet files found for table: {table}"


def test_ferceqr_datapackage_written(ferceqr_outputs: Path):
    """A ``ferceqr_parquet_datapackage.json`` must be written to the deploy directory."""
    datapackage = ferceqr_outputs / "ferceqr_parquet_datapackage.json"
    assert datapackage.exists(), (
        "ferceqr_parquet_datapackage.json not found in deploy dir"
    )
    assert datapackage.stat().st_size > 0, "ferceqr_parquet_datapackage.json is empty"


def test_ferceqr_success_sentinel_written(ferceqr_outputs, ferceqr_pudl_output: Path):
    """FERCEQR_SUCCESS sentinel must exist and FERCEQR_FAILURE must not."""
    assert (ferceqr_pudl_output / "FERCEQR_SUCCESS").exists()
    assert not (ferceqr_pudl_output / "FERCEQR_FAILURE").exists()
