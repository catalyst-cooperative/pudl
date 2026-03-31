"""Shared pytest fixtures and CLI options for integration test setup."""

import logging
import os
import shutil
import subprocess
import sys
from collections.abc import Generator
from pathlib import Path

import duckdb
import pydantic
import pytest
import sqlalchemy as sa
import yaml
from dagster import (
    AssetValueLoader,
    DagsterInstance,
    build_init_resource_context,
    materialize_to_memory,
)

import pudl
from pudl.dagster import build_defs, resources
from pudl.dagster.io_managers import (
    PudlMixedFormatIOManager,
    ferc1_dbf_sqlite_io_manager,
    ferc1_xbrl_sqlite_io_manager,
    ferc714_xbrl_sqlite_io_manager,
    pudl_mixed_format_io_manager,
)
from pudl.extract.ferc1 import raw_ferc1_xbrl__metadata_json
from pudl.extract.ferc714 import raw_ferc714_xbrl__metadata_json
from pudl.metadata import PUDL_PACKAGE
from pudl.settings import (
    DatasetsSettings,
    EtlSettings,
    FercToSqliteSettings,
)
from pudl.workspace.datastore import Datastore
from pudl.workspace.setup import PudlPaths

logger = logging.getLogger(__name__)

DG_CONFIG_PATH_DEFAULT = "src/pudl/package_data/settings/dg_pytest.yml"

# Preamble: before pytest starts handling this module's CLI options and fixtures, we
# do a small amount of one-time environment setup and controller-side validation. The
# DuckDB httpfs extension must be available early so collection-time imports and any
# tests that touch remote resources can work in restricted environments. We also
# inspect the requested test targets and reject incompatible combinations up front,
# before xdist workers start or fixture setup can poison shared environment variables.

# In general we run tests and subprocesses with multiple workers, and some tests touch
# remote HTTPS / S3 resources. We try to LOAD first so collection works in
# network-restricted environments (for example, sandboxed CI/test runners). If the
# extension is missing, we install it once and then load it.
try:
    duckdb.execute("LOAD httpfs")
except duckdb.Error:
    duckdb.execute("INSTALL httpfs")
    duckdb.execute("LOAD httpfs")


def _requested_test_targets(config: pytest.Config) -> list[Path]:
    """Return normalized path targets requested on the pytest command line."""
    raw_targets = config.args or ["test"]
    targets: list[Path] = []

    for raw_target in raw_targets:
        path_text = raw_target.split("::", maxsplit=1)[0]
        if not path_text:
            continue

        target = Path(path_text)
        if not target.is_absolute():
            target = (Path(config.rootpath) / target).resolve()
        else:
            target = target.resolve()
        targets.append(target)

    return targets


def _target_includes_suite(config: pytest.Config, suite_root: str) -> bool:
    """Return whether any requested target could include the named test suite."""
    suite_path = (Path(config.rootpath) / suite_root).resolve()

    return any(
        suite_path.is_relative_to(target) or target.is_relative_to(suite_path)
        for target in _requested_test_targets(config)
    )


def _raise_if_live_output_mixes_unit_and_integration(config: pytest.Config) -> None:
    """Reject invocations that mix live-output unit and integration suites."""
    if not config.getoption("--live-pudl-output", default=False):
        return

    has_unit = _target_includes_suite(config, "test/unit")
    has_integration = _target_includes_suite(config, "test/integration")
    if has_unit and has_integration:
        raise pytest.UsageError(
            "Cannot combine unit and integration tests in one session with "
            "--live-pudl-output: the unit fixture overrides PUDL_OUTPUT to a "
            "temp directory, which would corrupt the integration test environment. "
            "Run them in separate pytest invocations."
        )


def pytest_configure(config: pytest.Config) -> None:
    """Run controller-only validation of incompatible pytest CLI combinations."""
    if hasattr(config, "workerinput"):
        return

    _raise_if_live_output_mixes_unit_and_integration(config)


def pytest_collection_finish(session) -> None:
    """Abort if unit and integration tests are collected together with --live-pudl-output.

    When both suites run in a single pytest process with ``--live-pudl-output``, the
    unit-scoped ``pudl_test_paths`` override in ``test/unit/conftest.py`` would
    overwrite ``os.environ["PUDL_OUTPUT"]`` to a temporary directory *after* the
    top-level fixture has set it to the live path.  Integration tests that construct
    ``PudlPaths()`` directly (rather than via the fixture) would then silently resolve
    to the wrong directory.  Run unit and integration tests in separate invocations.
    """
    if hasattr(session.config, "workerinput"):
        return

    if not session.config.getoption("--live-pudl-output", default=False):
        return

    has_unit = any(item.nodeid.startswith("test/unit/") for item in session.items)
    has_integration = any(
        item.nodeid.startswith("test/integration/") for item in session.items
    )
    if has_unit and has_integration:
        pytest.exit(
            "Cannot combine unit and integration tests in one session with "
            "--live-pudl-output: the unit fixture overrides PUDL_OUTPUT to a "
            "temp directory, which would corrupt the integration test environment. "
            "Run them in separate pytest invocations.",
            returncode=4,
        )


################################################################################
# Main test configuration, helper functions, and fixture definitions start here.
################################################################################


def pytest_addoption(parser):
    parser.addoption(
        "--live-pudl-output",
        action="store_true",
        default=False,
        help="Use existing PUDL/FERC1 DBs instead of creating temporary ones.",
    )
    parser.addoption(
        "--temp-pudl-input",
        action="store_true",
        default=False,
        help="Download fresh input data for use with this test run only.",
    )
    parser.addoption(
        "--dg-config",
        action="store",
        default=DG_CONFIG_PATH_DEFAULT,
        help=(
            "Path to a Dagster dg launch config YAML file for integration tests. "
            f"Defaults to {DG_CONFIG_PATH_DEFAULT}."
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


def _pudl_etl(
    dg_config_path: Path,
    pudl_test_paths: PudlPaths,
    dagster_home: Path,
) -> None:
    """Run a dg launch job for pudl_with_ferc_to_sqlite including coverage collection.

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
        "pudl_with_ferc_to_sqlite",
        "--config",
        str(dg_config_path),
        "--verbose",
    ]
    # Command args are fully constructed in-process and do not include user input.
    env = os.environ.copy()
    # Force dg launch to read/write within pytest-managed paths.
    env["PUDL_INPUT"] = str(pudl_test_paths.input_dir)
    env["PUDL_OUTPUT"] = str(pudl_test_paths.output_dir)
    env["DAGSTER_HOME"] = str(dagster_home)
    env["PYTHONUNBUFFERED"] = "1"
    logger.info("Starting PUDL pytest ETL using dg launch.")
    logger.info(f"Command: {' '.join(cmd)}")
    logger.info(
        "Running dg launch with "
        f"{env['PUDL_INPUT']=} {env['PUDL_OUTPUT']=} {env['DAGSTER_HOME']=}"
    )

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
            logger.info(line.rstrip())

        returncode = proc.wait()
        if returncode != 0:
            raise subprocess.CalledProcessError(returncode, cmd)

    logger.info("Completed PUDL pytest ETL using dg launch.")


def _assert_prebuilt_ferc_sqlite_dbs(pudl_test_paths: PudlPaths) -> None:
    """Validate that required FERC SQLite databases exist after prebuild."""
    required = [
        pudl_test_paths.output_dir / "ferc1_dbf.sqlite",
        pudl_test_paths.output_dir / "ferc1_xbrl.sqlite",
        pudl_test_paths.output_dir / "ferc714_xbrl.sqlite",
    ]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise FileNotFoundError(
            "Missing expected FERC SQLite outputs after prebuild: " + ", ".join(missing)
        )


def _engine_from_io_manager(
    io_manager_factory,
    dataset_settings_config: DatasetsSettings | None = None,
) -> sa.Engine:
    """Return the SQLAlchemy engine exposed by a Dagster IO manager resource."""
    io_manager = io_manager_factory
    if dataset_settings_config is not None:
        io_manager = io_manager_factory.model_copy(
            update={"etl_settings": EtlSettings(datasets=dataset_settings_config)}
        )
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
    config_path = Path(request.config.getoption("--dg-config"))

    if not config_path.is_absolute():
        config_path = (test_dir.parent / config_path).resolve()

    if not config_path.exists():
        raise FileNotFoundError(f"Missing dg config file: {config_path}")

    return config_path


@pytest.fixture(scope="session")
def dagster_home(tmp_path_factory, request) -> Path:
    """Resolve the Dagster home shared by this test session.

    Live-output integration runs need to reuse the existing Dagster instance from the
    ETL build so FERC SQLite provenance metadata written during `dg launch` remains
    visible to later in-process reads. Fixture-managed prebuilds still use an isolated
    temporary Dagster home.
    """
    if request.config.getoption("--live-pudl-output") and os.environ.get(
        "DAGSTER_HOME"
    ):
        return Path(os.environ["DAGSTER_HOME"]).resolve()

    dagster_home = tmp_path_factory.mktemp("dagster_home")
    (dagster_home / "dagster.yaml").touch()
    os.environ["DAGSTER_HOME"] = str(dagster_home)
    return dagster_home.resolve()


@pytest.fixture(scope="session")
def dagster_instance(dagster_home: Path) -> DagsterInstance:
    """Return the Dagster instance shared by the pytest session and ETL subprocess."""
    return DagsterInstance.get()


@pytest.fixture(scope="session")
def asset_value_loader(
    prebuilt_outputs,
    etl_settings_path: Path,
    dagster_instance: DagsterInstance,
) -> Generator[AssetValueLoader]:
    """Fixture that initializes an asset value loader.

    Use this as ``asset_value_loader.load_asset_value`` instead of
    ``defs.load_asset_value`` to not reinitialize the asset value loader over and over
    again.
    """
    configured_defs = build_defs(
        resource_overrides={
            "etl_settings": resources.PudlEtlSettingsResource(
                etl_settings_path=str(etl_settings_path)
            )
        }
    )
    with configured_defs.get_asset_value_loader(instance=dagster_instance) as loader:
        yield loader


@pytest.fixture(scope="session")
def save_unmapped_ids(request) -> bool:
    """Fixture that indicates whether to save unmapped IDs to disk."""
    return request.config.getoption("--save-unmapped-ids")


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
        etl_settings_ref = dg_config["resources"]["etl_settings"]["config"][
            "etl_settings_path"
        ]
    except KeyError as err:
        raise ValueError(
            "Dagster config must define resources.etl_settings.config.etl_settings_path"
        ) from err

    etl_settings_yml = Path(etl_settings_ref)
    if not etl_settings_yml.is_absolute():
        etl_settings_yml = (test_dir.parent / etl_settings_yml).resolve()

    if not etl_settings_yml.exists():
        raise FileNotFoundError(f"Missing ETL settings file: {etl_settings_yml}")

    return etl_settings_yml


@pytest.fixture(scope="session")
def dbt_target(etl_settings_path: Path) -> str:
    """Infer the dbt target name from the ETL settings used for the test run."""
    if etl_settings_path.name == "etl_full.yml":
        return "etl-full"
    if etl_settings_path.name == "etl_fast.yml":
        return "etl-fast"

    raise ValueError(f"Unexpected ETL settings file: {etl_settings_path}")


@pytest.fixture(scope="session")
def ferc_to_sqlite_settings(etl_settings: EtlSettings) -> FercToSqliteSettings:
    """Read ferc_to_sqlite parameters out of test settings dictionary."""
    return etl_settings.ferc_to_sqlite


@pytest.fixture(scope="session")
def pudl_etl_settings(etl_settings: EtlSettings) -> DatasetsSettings:
    """Read PUDL ETL parameters out of test settings dictionary."""
    return etl_settings.dataset_settings


@pytest.fixture(scope="session")
def ferc1_engine_dbf(prebuilt_outputs, dataset_settings_config) -> sa.Engine:
    """Return the SQLAlchemy engine for the prebuilt FERC Form 1 DBF database."""
    return _engine_from_io_manager(
        ferc1_dbf_sqlite_io_manager,
        dataset_settings_config,
    )


@pytest.fixture(scope="session")
def prebuilt_outputs(
    request,
    dg_config_path: Path,
    pudl_test_paths: PudlPaths,
    dagster_home: Path,
):
    """Prebuild fast integration databases in pytest-managed output directories.

    When ``--live-pudl-output`` is not set, ``pudl_test_paths`` should have already
    set ``PUDL_OUTPUT`` to point at a temporary pytest session directory.
    """
    if request.config.getoption("--live-pudl-output"):
        logger.info("Using live PUDL_OUTPUT; skipping fixture-managed prebuild.")
        return

    logger.info(
        f"Prebuilding PUDL outputs in temporary directory: {pudl_test_paths.output_dir}"
    )
    logger.info(
        f"Initializing empty pudl.sqlite with current schema at {pudl_test_paths.pudl_db}."
    )
    md = PUDL_PACKAGE.to_sql()
    pudl_engine = sa.create_engine(pudl_test_paths.pudl_db)
    md.create_all(pudl_engine)

    _pudl_etl(dg_config_path, pudl_test_paths, dagster_home)
    _assert_prebuilt_ferc_sqlite_dbs(pudl_test_paths)


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
def pudl_test_paths(tmp_path_factory, request, dagster_home: Path):
    """Configures PudlPaths for tests.

    Default behavior:

    PUDL_INPUT is read from the environment.
    PUDL_OUTPUT is set to a temporary path, to avoid clobbering existing outputs.

    Set ``--temp-pudl-input`` to force PUDL_INPUT to a temporary directory, causing
    re-downloads of all raw inputs.

    Set ``--live-pudl-output`` to force PUDL_OUTPUT to *NOT* be a temporary directory
    and instead inherit from environment.

    Note: ``test/unit/conftest.py`` defines ``unit_pudl_test_paths`` which overrides
    this fixture for the unit test subtree. It ignores ``--live-pudl-output`` and always
    forces a temporary ``PUDL_OUTPUT`` so unit tests can never write to the live output
    directory.

    Warning: running unit and integration tests *together* with ``--live-pudl-output``
    in the same pytest session is not supported. The unit fixture would overwrite
    ``os.environ["PUDL_OUTPUT"]`` after this fixture has set it to the live path,
    silently misdirecting any integration-test code that constructs ``PudlPaths()``
    directly. A ``pytest_collection_finish`` hook in this file prevents that combination.
    """
    # Just in case we need this later...
    pudl_tmpdir = tmp_path_factory.mktemp("pudl")

    input_dir = Path(os.environ["PUDL_INPUT"]).resolve()
    output_dir = Path(os.environ["PUDL_OUTPUT"]).resolve()

    # We only use a temporary input directory when explicitly requested.
    # This will force a re-download of raw inputs from Zenodo or the S3 cache.
    if request.config.getoption("--temp-pudl-input"):
        in_tmp = pudl_tmpdir / "input"
        in_tmp.mkdir()
        input_dir = in_tmp.resolve()
        logger.info(f"Using temporary PUDL_INPUT: {in_tmp}")

    # Temporary output path is used when not using live DBs.
    if not request.config.getoption("--live-pudl-output"):
        out_tmp = pudl_tmpdir / "output"
        out_tmp.mkdir()
        output_dir = out_tmp.resolve()
        logger.info(f"Using temporary PUDL_OUTPUT: {out_tmp}")

    os.environ["DAGSTER_HOME"] = str(dagster_home)
    logger.info(f"Using temporary DAGSTER_HOME: {dagster_home}")

    PudlPaths.set_path_overrides(
        input_dir=str(input_dir),
        output_dir=str(output_dir),
    )
    # Keep process env in sync so subprocesses inherit the same locations.
    os.environ["PUDL_INPUT"] = str(input_dir)
    os.environ["PUDL_OUTPUT"] = str(output_dir)

    try:
        return PudlPaths(
            pudl_input=input_dir,
            pudl_output=output_dir,
        )
    except pydantic.ValidationError as err:
        pytest.exit(
            f"Set PUDL_INPUT, PUDL_OUTPUT env variables, or use --temp-pudl-input, --live-pudl-output flags. Error: {err}."
        )


@pytest.fixture(scope="session")
def dataset_settings_config(request, etl_settings: EtlSettings):
    """Create dataset settings for test helpers and IO managers."""
    if etl_settings.datasets is None:
        raise ValueError("Missing datasets settings in ETL settings.")
    return etl_settings.datasets


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
def pudl_datastore_fixture(request) -> Generator[Datastore]:
    """Create pudl Datastore resource."""
    with resources.ZenodoDoiSettingsResource.from_resource_context_cm(
        build_init_resource_context()
    ) as zenodo_dois:
        init_context = build_init_resource_context(
            config={
                "use_local_cache": not request.config.getoption("--bypass-local-cache"),
            },
            resources={"zenodo_dois": zenodo_dois},
        )
        with resources.DatastoreResource.from_resource_context_cm(
            init_context
        ) as datastore:
            yield datastore
