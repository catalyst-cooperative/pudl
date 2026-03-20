"""Shared pytest fixtures and CLI options for integration test setup."""

import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path

import duckdb
import pydantic
import pytest
import sqlalchemy as sa
import yaml
from dagster import AssetValueLoader, build_init_resource_context, materialize_to_memory

import pudl
from pudl import resources
from pudl.etl import defs
from pudl.extract.ferc1 import raw_ferc1_xbrl__metadata_json
from pudl.extract.ferc714 import raw_ferc714_xbrl__metadata_json
from pudl.io_managers import (
    PudlMixedFormatIOManager,
    ferc1_dbf_sqlite_io_manager,
    ferc1_xbrl_sqlite_io_manager,
    ferc714_xbrl_sqlite_io_manager,
    pudl_mixed_format_io_manager,
)
from pudl.metadata import PUDL_PACKAGE
from pudl.settings import (
    DatasetsSettings,
    EtlSettings,
    FercToSqliteSettings,
)
from pudl.workspace.datastore import Datastore
from pudl.workspace.setup import PudlPaths

logger = logging.getLogger(__name__)

# In general we run tests and subprocesses with multiple workers, and some tests touch
# remote HTTPS / S3 resources. We try to LOAD first so collection works in
# network-restricted environments (for example, sandboxed CI/test runners). If the
# extension is missing, we install it once and then load it.
try:
    duckdb.execute("LOAD httpfs")
except duckdb.Error:
    duckdb.execute("INSTALL httpfs")
    duckdb.execute("LOAD httpfs")


def pytest_addoption(parser):
    parser.addoption(
        "--live-dbs",
        action="store_true",
        default=False,
        help="Use existing PUDL/FERC1 DBs instead of creating temporary ones.",
    )
    parser.addoption(
        "--tmp-data",
        action="store_true",
        default=False,
        help="Download fresh input data for use with this test run only.",
    )
    parser.addoption(
        "--dg-config",
        action="store",
        default=False,
        help=(
            "Path to a Dagster dg launch config YAML file for integration tests. "
            "Defaults to src/pudl/package_data/settings/dg_pytest.yml."
        ),
    )
    parser.addoption(
        "--bypass-local-cache",
        action="store_true",
        default=False,
        help="If enabled, the local file cache for datastore will not be used.",
    )
    parser.addoption(
        "--save-unmapped-ids",
        action="store_true",
        default=False,
        help="Write the unmapped IDs to disk.",
    )
    parser.addoption(
        "--ignore-fks",
        action="store_true",
        default=False,
        help="If enabled, do not check the foreign keys.",
    )


def _pudl_etl(config_file: Path) -> None:
    """Run a dg launch job for pudl including coverage collection.

    Uses the dg executable path directly since ``dg`` is a console script and not a
    Python module importable via ``python -m dg``.
    """
    dg_path = shutil.which("dg")
    if dg_path is None:
        pytest.exit("Could not find `dg` executable in PATH.")

    cmd = [
        sys.executable,
        "-m",
        "coverage",
        "run",
        "--append",
        dg_path,
        "launch",
        "--job",
        "pudl",
        "--config",
        str(config_file),
        "--verbose",
    ]
    # Command args are fully constructed in-process and do not include user input.
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    logger.info("Starting prebuild job via dg launch: pudl")
    logger.info("Command: %s", " ".join(cmd))

    # Stream subprocess output into pytest's live logging so progress is visible.
    # Popen is used instead of run to allow streaming output. We also set text=True and
    # line-buffered output to ensure logs are emitted in real time.
    with subprocess.Popen(  # noqa: S603
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=env,
    ) as proc:
        assert proc.stdout is not None
        for line in proc.stdout:
            logger.info("[dg pudl] %s", line.rstrip())

        returncode = proc.wait()
        if returncode != 0:
            raise subprocess.CalledProcessError(returncode, cmd)

    logger.info("Completed prebuild job via dg launch: pudl")


def _build_resource_context(dataset_settings_config: dict[str, object] | None = None):
    """Create a Dagster resource context for test IO managers."""
    resources = {}
    if dataset_settings_config is not None:
        resources["dataset_settings"] = dataset_settings_config
    return build_init_resource_context(resources=resources)


def _engine_from_io_manager(
    io_manager_factory,
    dataset_settings_config: dict[str, object] | None = None,
) -> sa.Engine:
    """Return the SQLAlchemy engine exposed by a Dagster IO manager resource."""
    io_manager = io_manager_factory(_build_resource_context(dataset_settings_config))
    if isinstance(io_manager, PudlMixedFormatIOManager):
        return io_manager._sqlite_io_manager.engine
    return io_manager.engine


@pytest.fixture(scope="session")
def test_dir():
    """Return the path to the top-level directory containing the tests."""
    return Path(__file__).parent


@pytest.fixture(scope="session")
def dg_config_path(request, test_dir: Path) -> Path:
    """Resolve Dagster launch config path used by integration-test prebuild."""
    dg_config_option = request.config.getoption("--dg-config")

    if dg_config_option:
        config_path = Path(dg_config_option)
    else:
        config_path = test_dir.parent / "src/pudl/package_data/settings/dg_pytest.yml"

    if not config_path.is_absolute():
        config_path = (test_dir.parent / config_path).resolve()

    if not config_path.exists():
        raise FileNotFoundError(f"Missing dg integration config file: {config_path}")

    return config_path


@pytest.fixture(scope="session")
def asset_value_loader() -> AssetValueLoader:
    """Fixture that initializes an asset value loader.

    Use this as ``asset_value_loader.load_asset_value`` instead of
    ``defs.load_asset_value`` to not reinitialize the asset value loader over and over
    again.
    """
    return defs.get_asset_value_loader()


@pytest.fixture(scope="session")
def save_unmapped_ids(request) -> bool:
    """Fixture that indicates whether to save unmapped IDs to disk."""
    return request.config.getoption("--save-unmapped-ids")


@pytest.fixture
def check_fks(request) -> bool:
    """Fixture that indicates whether to check foreign key constraints)."""
    return not request.config.getoption("--ignore-fks")


@pytest.fixture(scope="session")
def etl_settings(etl_settings_path: Path) -> EtlSettings:
    """Read ETL settings referenced by Dagster integration config."""
    return EtlSettings.from_yaml(str(etl_settings_path))


@pytest.fixture(scope="session")
def etl_settings_path(dg_config_path: Path, test_dir: Path) -> Path:
    """Resolve the ETL settings file referenced by Dagster integration config."""
    with dg_config_path.open() as f:
        dg_config = yaml.safe_load(f)

    try:
        etl_settings_ref = dg_config["resources"]["dataset_settings"]["config"][
            "etl_settings_path"
        ]
    except KeyError as err:
        raise ValueError(
            "Dagster integration config must define "
            "resources.dataset_settings.config.etl_settings_path"
        ) from err

    etl_settings_yml = Path(etl_settings_ref)
    if not etl_settings_yml.is_absolute():
        etl_settings_yml = (test_dir.parent / etl_settings_yml).resolve()

    if not etl_settings_yml.exists():
        raise FileNotFoundError(f"Missing ETL settings file: {etl_settings_yml}")

    return etl_settings_yml


@pytest.fixture(scope="session")
def ferc_to_sqlite_settings(etl_settings: EtlSettings) -> FercToSqliteSettings:
    """Read ferc_to_sqlite parameters out of test settings dictionary."""
    if etl_settings.ferc_to_sqlite_settings is None:
        raise ValueError("Missing ferc_to_sqlite_settings in ETL settings.")
    return etl_settings.ferc_to_sqlite_settings


@pytest.fixture(scope="session")
def pudl_etl_settings(etl_settings: EtlSettings) -> DatasetsSettings:
    """Read PUDL ETL parameters out of test settings dictionary."""
    if etl_settings.datasets is None:
        raise ValueError("Missing datasets settings in ETL settings.")
    return etl_settings.datasets


@pytest.fixture(scope="session")
def ferc1_engine_dbf(prebuilt_outputs, dataset_settings_config) -> sa.Engine:
    """Return the SQLAlchemy engine for the prebuilt FERC Form 1 DBF database."""
    return _engine_from_io_manager(
        ferc1_dbf_sqlite_io_manager,
        dataset_settings_config,
    )


@pytest.fixture(scope="session")
def prebuilt_outputs(request, dg_config_path: Path):
    """Prebuild fast integration databases in pytest-managed output directories.

    When ``--live-dbs`` is not set, ``configure_test_paths`` should have already
    set ``PUDL_OUTPUT`` to point at a temporary pytest session directory.
    """
    if request.config.getoption("--live-dbs"):
        logger.info("Using live DBs; skipping fixture-managed prebuild.")
        return

    active_paths = PudlPaths(
        pudl_input=Path(os.environ["PUDL_INPUT"]),
        pudl_output=Path(os.environ["PUDL_OUTPUT"]),
    )
    logger.info(
        f"Prebuilding integration DBs in temporary output: {active_paths.output_dir}"
    )
    logger.info("Initializing empty pudl.sqlite with current metadata schema.")
    md = PUDL_PACKAGE.to_sql()
    pudl_engine = sa.create_engine(active_paths.pudl_db)
    md.create_all(pudl_engine)

    _pudl_etl(config_file=dg_config_path)


@pytest.fixture(scope="session")
def ferc1_engine_xbrl(prebuilt_outputs, dataset_settings_config) -> sa.Engine:
    """Return the SQLAlchemy engine for the prebuilt FERC Form 1 XBRL database."""
    return _engine_from_io_manager(
        ferc1_xbrl_sqlite_io_manager,
        dataset_settings_config,
    )


@pytest.fixture(scope="session")
def ferc1_xbrl_taxonomy_metadata(ferc1_engine_xbrl: sa.Engine):
    """Read the FERC 1 XBRL taxonomy metadata from JSON.

    ``ferc1_engine_xbrl`` is an ordering-only dependency that ensures the FERC 1 XBRL
    database is prebuilt before this fixture runs. Its return value is not used here.
    """
    result = materialize_to_memory([raw_ferc1_xbrl__metadata_json])
    assert result.success

    return result.output_for_node("raw_ferc1_xbrl__metadata_json")


@pytest.fixture(scope="session")
def ferc714_engine_xbrl(prebuilt_outputs, dataset_settings_config) -> sa.Engine:
    """Return the SQLAlchemy engine for the prebuilt FERC Form 714 XBRL database."""
    return _engine_from_io_manager(
        ferc714_xbrl_sqlite_io_manager,
        dataset_settings_config,
    )


@pytest.fixture(scope="session")
def ferc714_xbrl_taxonomy_metadata(ferc714_engine_xbrl: sa.Engine):
    """Read the FERC 714 XBRL taxonomy metadata from JSON.

    ``ferc714_engine_xbrl`` is an ordering-only dependency that ensures the FERC 714
    XBRL database is prebuilt before this fixture runs. Its return value is not used
    here.
    """
    result = materialize_to_memory([raw_ferc714_xbrl__metadata_json])
    assert result.success

    return result.output_for_node("raw_ferc714_xbrl__metadata_json")


@pytest.fixture(scope="session")
def pudl_engine(prebuilt_outputs) -> sa.Engine:
    """Return the SQLAlchemy engine for the prepared PUDL integration database."""
    return _engine_from_io_manager(pudl_mixed_format_io_manager)


@pytest.fixture(scope="session", autouse=True)
def configure_test_paths(tmp_path_factory, request):
    """Configures PudlPaths for tests.

    Default behavior:

    PUDL_INPUT is read from the environment.
    PUDL_OUTPUT is set to a tmp path, to avoid clobbering existing databases.

    Set ``--tmp-data`` to force PUDL_INPUT to a temporary directory, causing
    re-downloads of all raw inputs.

    Set ``--live-dbs`` to force PUDL_OUTPUT to *NOT* be a temporary directory
    and instead inherit from environment.

    ``--live-dbs`` flag is ignored in unit tests, see pudl/test/unit/conftest.py.
    """
    # Just in case we need this later...
    pudl_tmpdir = tmp_path_factory.mktemp("pudl")
    # We only use a temporary input directory when explicitly requested.
    # This will force a re-download of raw inputs from Zenodo or the S3 cache.
    if request.config.getoption("--tmp-data"):
        in_tmp = pudl_tmpdir / "input"
        in_tmp.mkdir()
        PudlPaths.set_path_overrides(
            input_dir=str(Path(in_tmp).resolve()),
        )
        logger.info(f"Using temporary PUDL_INPUT: {in_tmp}")

    # Temporary output path is used when not using live DBs.
    if not request.config.getoption("--live-dbs"):
        out_tmp = pudl_tmpdir / "output"
        out_tmp.mkdir()
        PudlPaths.set_path_overrides(
            output_dir=str(Path(out_tmp).resolve()),
        )
        logger.info(f"Using temporary PUDL_OUTPUT: {out_tmp}")

    try:
        return PudlPaths(
            pudl_input=Path(os.environ["PUDL_INPUT"]),
            pudl_output=Path(os.environ["PUDL_OUTPUT"]),
        )
    except pydantic.ValidationError as err:
        pytest.exit(
            f"Set PUDL_INPUT, PUDL_OUTPUT env variables, or use --tmp-path, --live-dbs flags. Error: {err}."
        )


@pytest.fixture(scope="session")
def dataset_settings_config(request, etl_settings: EtlSettings):
    """Create dagster dataset_settings resource."""
    if etl_settings.datasets is None:
        raise ValueError("Missing datasets settings in ETL settings.")
    return etl_settings.datasets.model_dump()


@pytest.fixture(scope="session", autouse=True)
def logger_config():
    """Configure root logger to filter out excessive logs from certain dependencies."""
    pudl.logging_helpers.configure_root_logger(
        dependency_loglevels={
            "aiobotocore": logging.WARNING,
            "alembic": logging.WARNING,
            "arelle": logging.INFO,
            "asyncio": logging.INFO,
            "boto3": logging.WARNING,
            "botocore": logging.WARNING,
            "fsspec": logging.INFO,
            "google": logging.INFO,
            "matplotlib": logging.WARNING,
            "numba": logging.WARNING,
            "urllib3": logging.INFO,
        },
        propagate=True,
    )


@pytest.fixture(scope="session")
def pudl_datastore_fixture(request) -> Datastore:
    """Create pudl Datastore resource."""
    init_context = build_init_resource_context(
        config={
            "use_local_cache": not request.config.getoption("--bypass-local-cache"),
        }
    )
    return resources.datastore(init_context)
