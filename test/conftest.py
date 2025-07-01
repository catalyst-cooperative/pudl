"""PyTest configuration module.

Defines useful fixtures, command line args.
"""

import logging
from pathlib import Path
from typing import Any

import duckdb
import pydantic
import pytest
import sqlalchemy as sa
from dagster import (
    AssetValueLoader,
    build_init_resource_context,
    graph,
    materialize_to_memory,
)

import pudl
from pudl import resources
from pudl.etl import defs
from pudl.etl.cli import pudl_etl_job_factory
from pudl.extract.ferc1 import Ferc1DbfExtractor, raw_ferc1_xbrl__metadata_json
from pudl.extract.ferc714 import raw_ferc714_xbrl__metadata_json
from pudl.extract.xbrl import xbrl2sqlite_op_factory
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
    XbrlFormNumber,
)
from pudl.workspace.datastore import Datastore
from pudl.workspace.setup import PudlPaths

logger = logging.getLogger(__name__)

AS_MS_ONLY_FREQ_TABLES = [
    "gen_eia923",
    "gen_fuel_by_generator_eia923",
]

# In general we run our tests and some subprocesses using more than one thread, and
# sometimes we access remote HTTPS / S3 resources. When this happens it's possible
# for DuckDB to get confused about whether the httpfs extension is installed and error
# out if one processes is trying to install it after another one already has. Doing
# this forced installation during setup avoids that issue.
duckdb.execute("FORCE INSTALL httpfs")


def pytest_addoption(parser):
    """Add a command line option Requiring fresh data download."""
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
        "--etl-settings",
        action="store",
        default=False,
        help="Path to a non-standard ETL settings file to use.",
    )
    parser.addoption(
        "--gcs-cache-path",
        default=None,
        help="If set, use this GCS path as a datastore cache layer.",
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
        "--ignore-foreign-key-constraints",
        action="store_true",
        default=False,
        help="If enabled, do not check the foreign keys.",
    )


@pytest.fixture(scope="session", name="test_dir")
def test_directory():
    """Return the path to the top-level directory containing the tests."""
    return Path(__file__).parent


@pytest.fixture(scope="session", name="live_dbs")
def live_databases(request) -> bool:
    """Fixture that tells whether to use existing live FERC1/PUDL DBs)."""
    return request.config.getoption("--live-dbs")


@pytest.fixture(scope="session")
def asset_value_loader() -> AssetValueLoader:
    """Fixture that initializes an asset value loader.

    Use this as ``asset_value_loader.load_asset_value`` instead
    of ``defs.load_asset_value`` to not reinitialize the asset
    value loader over and over again.
    """
    return defs.get_asset_value_loader()


@pytest.fixture(scope="session", name="save_unmapped_ids")
def save_unmapped_ids(request) -> bool:
    """Fixture that tells whether to use existing live FERC1/PUDL DBs)."""
    return request.config.getoption("--save-unmapped-ids")


@pytest.fixture
def check_foreign_keys_flag(request) -> bool:
    """Fixture that tells whether to use existing live FERC1/PUDL DBs)."""
    return not request.config.getoption("--ignore-foreign-key-constraints")


@pytest.fixture(scope="session", name="etl_settings")
def etl_parameters(request, test_dir) -> EtlSettings:
    """Read the ETL parameters from the test settings or proffered file."""
    if request.config.getoption("--etl-settings"):
        etl_settings_yml = Path(request.config.getoption("--etl-settings"))
    else:
        etl_settings_yml = Path(
            test_dir.parent / "src/pudl/package_data/settings/etl_fast.yml"
        )
    etl_settings = EtlSettings.from_yaml(etl_settings_yml)
    return etl_settings


@pytest.fixture(scope="session", name="ferc_to_sqlite_settings")
def ferc_to_sqlite_parameters(etl_settings: EtlSettings) -> FercToSqliteSettings:
    """Read ferc_to_sqlite parameters out of test settings dictionary."""
    return etl_settings.ferc_to_sqlite_settings


@pytest.fixture(scope="session", name="pudl_etl_settings")
def pudl_etl_parameters(etl_settings: EtlSettings) -> DatasetsSettings:
    """Read PUDL ETL parameters out of test settings dictionary."""
    return etl_settings.datasets


@pytest.fixture(scope="session")
def ferc1_dbf_extract(
    live_dbs: bool,
    pudl_datastore_config,
    etl_settings: EtlSettings,
):
    """Creates raw FERC 1 SQlite DBs, based only on DBF sources."""

    @graph
    def local_dbf_ferc1_graph():
        Ferc1DbfExtractor.get_dagster_op()()

    if not live_dbs:
        execute_result = local_dbf_ferc1_graph.to_job(
            name="ferc_to_sqlite_dbf_ferc1",
            resource_defs=pudl.ferc_to_sqlite.default_resources_defs,
        ).execute_in_process(
            run_config={
                "resources": {
                    "ferc_to_sqlite_settings": {
                        "config": etl_settings.ferc_to_sqlite_settings.model_dump()
                    },
                    "datastore": {
                        "config": pudl_datastore_config,
                    },
                    "runtime_settings": {"config": {"xbrl_num_workers": 2}},
                },
            },
        )
        assert execute_result.success, "ferc_to_sqlite_dbf_ferc1 failed!"


@pytest.fixture(scope="session")
def ferc1_xbrl_extract(
    live_dbs: bool, pudl_datastore_config, etl_settings: EtlSettings
):
    """Runs ferc_to_sqlite dagster job for FERC Form 1 XBRL data."""

    @graph
    def local_xbrl_ferc1_graph():
        xbrl2sqlite_op_factory(XbrlFormNumber.FORM1)()

    if not live_dbs:
        execute_result = local_xbrl_ferc1_graph.to_job(
            name="ferc_to_sqlite_xbrl_ferc1",
            resource_defs=pudl.ferc_to_sqlite.default_resources_defs,
        ).execute_in_process(
            run_config={
                "resources": {
                    "ferc_to_sqlite_settings": {
                        "config": etl_settings.ferc_to_sqlite_settings.model_dump(),
                    },
                    "datastore": {
                        "config": pudl_datastore_config,
                    },
                    "runtime_settings": {"config": {"xbrl_num_workers": 2}},
                },
            }
        )
        assert execute_result.success, "ferc_to_sqlite_xbrl_ferc1 failed!"


@pytest.fixture(scope="session", name="ferc1_engine_dbf")
def ferc1_dbf_sql_engine(ferc1_dbf_extract, dataset_settings_config) -> sa.Engine:
    """Grab a connection to the FERC Form 1 DB clone."""
    context = build_init_resource_context(
        resources={"dataset_settings": dataset_settings_config}
    )
    return ferc1_dbf_sqlite_io_manager(context).engine


@pytest.fixture(scope="session")
def ferc714_xbrl_extract(
    live_dbs: bool, pudl_datastore_config, etl_settings: EtlSettings
):
    """Runs ferc_to_sqlite dagster job for FERC Form 714 XBRL data."""

    @graph
    def local_xbrl_ferc714_graph():
        xbrl2sqlite_op_factory(XbrlFormNumber.FORM714)()

    if not live_dbs:
        execute_result = local_xbrl_ferc714_graph.to_job(
            name="ferc_to_sqlite_xbrl_ferc1",
            resource_defs=pudl.ferc_to_sqlite.default_resources_defs,
        ).execute_in_process(
            run_config={
                "resources": {
                    "ferc_to_sqlite_settings": {
                        "config": etl_settings.ferc_to_sqlite_settings.model_dump(),
                    },
                    "datastore": {
                        "config": pudl_datastore_config,
                    },
                    "runtime_settings": {"config": {"xbrl_num_workers": 2}},
                },
            }
        )
        assert execute_result.success, "ferc_to_sqlite_xbrl_ferc714 failed!"


@pytest.fixture(scope="session", name="ferc1_engine_xbrl")
def ferc1_xbrl_sql_engine(ferc1_xbrl_extract, dataset_settings_config) -> sa.Engine:
    """Grab a connection to the FERC Form 1 DB clone."""
    context = build_init_resource_context(
        resources={"dataset_settings": dataset_settings_config}
    )
    return ferc1_xbrl_sqlite_io_manager(context).engine


@pytest.fixture(scope="session", name="ferc1_xbrl_taxonomy_metadata")
def ferc1_xbrl_taxonomy_metadata(ferc1_engine_xbrl: sa.Engine):
    """Read the FERC 1 XBRL taxonomy metadata from JSON."""
    result = materialize_to_memory([raw_ferc1_xbrl__metadata_json])
    assert result.success

    return result.output_for_node("raw_ferc1_xbrl__metadata_json")


@pytest.fixture(scope="session", name="ferc714_engine_xbrl")
def ferc714_xbrl_sql_engine(ferc714_xbrl_extract, dataset_settings_config) -> sa.Engine:
    """Grab a connection to the FERC Form 714 DB clone."""
    context = build_init_resource_context(
        resources={"dataset_settings": dataset_settings_config}
    )
    return ferc714_xbrl_sqlite_io_manager(context).engine


@pytest.fixture(scope="session", name="ferc714_xbrl_taxonomy_metadata")
def ferc714_xbrl_taxonomy_metadata(ferc714_engine_xbrl: sa.Engine):
    """Read the FERC 714 XBRL taxonomy metadata from JSON."""
    result = materialize_to_memory([raw_ferc714_xbrl__metadata_json])
    assert result.success

    return result.output_for_node("raw_ferc714_xbrl__metadata_json")


@pytest.fixture(scope="session")
def pudl_io_manager(
    ferc1_engine_dbf: sa.Engine,  # Implicit dependency
    ferc1_engine_xbrl: sa.Engine,  # Implicit dependency
    ferc714_engine_xbrl: sa.Engine,
    live_dbs: bool,
    pudl_datastore_config,
    dataset_settings_config,
    request,
) -> PudlMixedFormatIOManager:
    """Grab a connection to the PUDL IO manager.

    If we are using the test database, we initialize the PUDL DB from scratch. If we're
    using the live database, then we just make a connection to it.
    """
    logger.info("setting up the pudl_engine fixture")
    if not live_dbs:
        # Create the database and schemas
        engine = sa.create_engine(PudlPaths().pudl_db)
        md = PUDL_PACKAGE.to_sql()
        md.create_all(engine)
        # Run the ETL and generate a new PUDL SQLite DB for testing:
        execute_result = pudl_etl_job_factory(base_job="etl_fast")().execute_in_process(
            run_config={
                "resources": {
                    "dataset_settings": {
                        "config": dataset_settings_config,
                    },
                    "datastore": {
                        "config": pudl_datastore_config,
                    },
                },
            },
        )
        assert execute_result.success, "pudl_etl failed!"
    # Grab a connection to the freshly populated PUDL DB, and hand it off.
    # All the hard work here is being done by the datapkg and
    # datapkg_to_sqlite fixtures, above.
    context = build_init_resource_context()
    return pudl_mixed_format_io_manager(context)


@pytest.fixture(scope="session")
def pudl_engine(pudl_io_manager: PudlMixedFormatIOManager) -> sa.Engine:
    """Get PUDL SQL engine from io manager."""
    return pudl_io_manager._sqlite_io_manager.engine


@pytest.fixture(scope="session", autouse=True)
def configure_paths_for_tests(tmp_path_factory, request):
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
    # This will force a re-download of raw inputs from Zenodo or the GCS cache.
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
        return PudlPaths()
    except pydantic.ValidationError as err:
        pytest.exit(
            f"Set PUDL_INPUT, PUDL_OUTPUT env variables, or use --tmp-path, --live-dbs flags. Error: {err}."
        )


@pytest.fixture(scope="session")
def dataset_settings_config(request, etl_settings: EtlSettings):
    """Create dagster dataset_settings resource."""
    return etl_settings.datasets.model_dump()


@pytest.fixture(scope="session")
def pudl_datastore_config(request) -> dict[str, Any]:
    """Produce a :class:pudl.workspace.datastore.Datastore."""
    gcs_cache_path = request.config.getoption("--gcs-cache-path")
    return {
        "gcs_cache_path": gcs_cache_path if gcs_cache_path else "",
        "use_local_cache": not request.config.getoption("--bypass-local-cache"),
    }


@pytest.fixture(scope="session")
def pudl_datastore_fixture(pudl_datastore_config: dict[str, Any]) -> Datastore:
    """Create pudl Datastore resource."""
    init_context = build_init_resource_context(config=pudl_datastore_config)
    return resources.datastore(init_context)


def skip_table_if_null_freq_table(table_name: str, freq: str | None):
    """Check."""
    if table_name in AS_MS_ONLY_FREQ_TABLES and freq is None:
        pytest.skip(
            f"Data validation for {table_name} does not work with a null frequency."
        )
