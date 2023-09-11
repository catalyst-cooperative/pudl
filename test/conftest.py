"""PyTest configuration module.

Defines useful fixtures, command line args.
"""
import logging
import os
from pathlib import Path

import pytest
import sqlalchemy as sa
import yaml
from dagster import build_init_resource_context, materialize_to_memory
from ferc_xbrl_extractor import xbrl

import pudl
from pudl import resources
from pudl.cli.etl import pudl_etl_job_factory
from pudl.extract.ferc1 import raw_xbrl_metadata_json
from pudl.extract.xbrl import FercXbrlDatastore, _get_sqlite_engine
from pudl.ferc_to_sqlite.cli import ferc_to_sqlite_job_factory
from pudl.io_managers import (
    ferc1_dbf_sqlite_io_manager,
    ferc1_xbrl_sqlite_io_manager,
    pudl_sqlite_io_manager,
)
from pudl.metadata.classes import Package
from pudl.output.pudltabl import PudlTabl
from pudl.settings import DatasetsSettings, EtlSettings, XbrlFormNumber
from pudl.workspace.setup import PudlPaths

logger = logging.getLogger(__name__)

AS_MS_ONLY_FREQ_TABLES = [
    "gen_eia923",
    "gen_fuel_by_generator_eia923",
]


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
def live_databases(request):
    """Fixture that tells whether to use existing live FERC1/PUDL DBs)."""
    return request.config.getoption("--live-dbs")


@pytest.fixture(scope="session", name="save_unmapped_ids")
def save_unmapped_ids(request):
    """Fixture that tells whether to use existing live FERC1/PUDL DBs)."""
    return request.config.getoption("--save-unmapped-ids")


@pytest.fixture(scope="session", name="check_foreign_keys")
def check_foreign_keys(request):
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
    with Path.open(etl_settings_yml, encoding="utf8") as settings_file:
        etl_settings_out = yaml.safe_load(settings_file)
    etl_settings = EtlSettings().parse_obj(etl_settings_out)
    return etl_settings


@pytest.fixture(scope="session", name="ferc_to_sqlite_settings")
def ferc_to_sqlite_parameters(etl_settings):
    """Read ferc_to_sqlite parameters out of test settings dictionary."""
    return etl_settings.ferc_to_sqlite_settings


@pytest.fixture(scope="session", name="pudl_etl_settings")
def pudl_etl_parameters(etl_settings: EtlSettings) -> DatasetsSettings:
    """Read PUDL ETL parameters out of test settings dictionary."""
    return etl_settings.datasets


@pytest.fixture(scope="session", params=["AS"], ids=["ferc1_annual"])
def pudl_out_ferc1(live_dbs, pudl_engine, request):
    """Define parameterized PudlTabl output object fixture for FERC 1 tests."""
    if not live_dbs:
        pytest.skip("Validation tests only work with a live PUDL DB.")
    return PudlTabl(pudl_engine=pudl_engine, freq=request.param)


@pytest.fixture(
    scope="session",
    params=[None, "AS", "MS"],
    ids=["eia_raw", "eia_annual", "eia_monthly"],
)
def pudl_out_eia(live_dbs, pudl_engine, request):
    """Define parameterized PudlTabl output object fixture for EIA tests."""
    if not live_dbs:
        pytest.skip("Validation tests only work with a live PUDL DB.")
    return PudlTabl(
        pudl_engine=pudl_engine,
        freq=request.param,
        fill_fuel_cost=True,
        roll_fuel_cost=True,
        fill_net_gen=True,
    )


@pytest.fixture(scope="session", name="fast_out_annual")
def fast_out_annual(pudl_engine, pudl_datastore_fixture):
    """A PUDL output object for use in CI."""
    return pudl.output.pudltabl.PudlTabl(
        pudl_engine,
        freq="AS",
        fill_fuel_cost=True,
        roll_fuel_cost=True,
        fill_net_gen=True,
    )


@pytest.fixture(scope="session")
def pudl_out_orig(live_dbs, pudl_engine):
    """Create an unaggregated PUDL output object for checking raw data."""
    if not live_dbs:
        pytest.skip("Validation tests only work with a live PUDL DB.")
    return PudlTabl(pudl_engine=pudl_engine)


@pytest.fixture(scope="session")
def ferc_to_sqlite_dbf_only(live_dbs, pudl_datastore_config, etl_settings):
    """Create raw FERC 1 SQLite DBs, but only based on DBF sources."""
    if not live_dbs:
        ferc_to_sqlite_job_factory(
            enable_xbrl=False,
        )().execute_in_process(
            run_config={
                "resources": {
                    "ferc_to_sqlite_settings": {
                        "config": etl_settings.ferc_to_sqlite_settings.dict()
                    },
                    "datastore": {
                        "config": pudl_datastore_config,
                    },
                },
            },
        )


@pytest.fixture(scope="session")
def ferc_to_sqlite_xbrl_only(live_dbs, pudl_datastore_config, etl_settings):
    """Create raw FERC 1 SQLite DBs, but only based on XBRL sources."""
    if not live_dbs:
        ferc_to_sqlite_job_factory(
            enable_dbf=False,
        )().execute_in_process(
            run_config={
                "resources": {
                    "ferc_to_sqlite_settings": {
                        "config": etl_settings.ferc_to_sqlite_settings.dict()
                    },
                    "datastore": {
                        "config": pudl_datastore_config,
                    },
                },
            },
        )


@pytest.fixture(scope="session")
def ferc_to_sqlite(live_dbs, pudl_datastore_config, etl_settings):
    """Create raw FERC 1 SQLite DBs.

    If we are using the test database, we initialize it from scratch first. If we're
    using the live database, then the sql engine fixtures will return connections to the
    existing databases
    """
    if not live_dbs:
        logger.info(
            f"ferc_to_sqlite_settings: {etl_settings.ferc_to_sqlite_settings.dict()}"
        )
        logger.info(f"ferc_to_sqlite PUDL_OUTPUT: {os.getenv('PUDL_OUTPUT')}")
        ferc_to_sqlite_job_factory()().execute_in_process(
            run_config={
                "resources": {
                    "ferc_to_sqlite_settings": {
                        "config": etl_settings.ferc_to_sqlite_settings.dict()
                    },
                    "datastore": {
                        "config": pudl_datastore_config,
                    },
                },
            },
        )


@pytest.fixture(scope="session", name="ferc1_engine_dbf")
def ferc1_dbf_sql_engine(ferc_to_sqlite_dbf_only):
    """Grab a connection to the FERC Form 1 DB clone."""
    context = build_init_resource_context(
        resources={"dataset_settings": dataset_settings_config}
    )
    return ferc1_dbf_sqlite_io_manager(context).engine


@pytest.fixture(scope="session", name="ferc1_engine_xbrl")
def ferc1_xbrl_sql_engine(ferc_to_sqlite_xbrl_only, dataset_settings_config):
    """Grab a connection to the FERC Form 1 DB clone."""
    context = build_init_resource_context(
        resources={"dataset_settings": dataset_settings_config}
    )
    return ferc1_xbrl_sqlite_io_manager(context).engine


@pytest.fixture(scope="session")
def ferc_xbrl(
    live_dbs,
    ferc_to_sqlite_settings,
    pudl_datastore_fixture,
):
    """Extract XBRL filings and produce raw DB+metadata files.

    Extracts a subset of filings for each form for the year 2021.
    """
    if not live_dbs:
        year = 2021

        # Prep datastore
        datastore = FercXbrlDatastore(pudl_datastore_fixture)

        # Set step size for subsetting
        step_size = 5

        for form in XbrlFormNumber:
            raw_archive, taxonomy_entry_point = datastore.get_taxonomy(year, form)

            sqlite_engine = _get_sqlite_engine(form.value, True)

            form_settings = ferc_to_sqlite_settings.get_xbrl_dataset_settings(form)

            # Extract every fifth filing
            filings_subset = datastore.get_filings(year, form)[::step_size]
            xbrl.extract(
                filings_subset,
                sqlite_engine,
                raw_archive,
                form.value,
                requested_tables=form_settings.tables,
                batch_size=len(filings_subset) // step_size + 1,
                workers=step_size,
                # TODO(janrous): the following should ideally be provided by some
                # ferc dataset metadata object rather than encoding this in settings.
                datapackage_path=PudlPaths().output_file(
                    f"ferc{form.value}_xbrl_datapackage.json"
                ),
                metadata_path=PudlPaths().output_file(
                    f"ferc{form.value}_xbrl_taxonomy_metadata.json"
                ),
                archive_file_path=taxonomy_entry_point,
            )


@pytest.fixture(scope="session", name="ferc1_xbrl_taxonomy_metadata")
def ferc1_xbrl_taxonomy_metadata(ferc1_engine_xbrl):
    """Read the FERC 1 XBRL taxonomy metadata from JSON."""
    result = materialize_to_memory([raw_xbrl_metadata_json])
    assert result.success

    return result.output_for_node("raw_xbrl_metadata_json")


@pytest.fixture(scope="session")
def pudl_sql_io_manager(
    ferc1_engine_dbf,  # Implicit dependency
    ferc1_engine_xbrl,  # Implicit dependency
    live_dbs,
    pudl_datastore_config,
    dataset_settings_config,
    check_foreign_keys,
    request,
):
    """Grab a connection to the PUDL IO manager.

    If we are using the test database, we initialize the PUDL DB from scratch. If we're
    using the live database, then we just make a conneciton to it.
    """
    logger.info("setting up the pudl_engine fixture")
    if not live_dbs:
        # Create the database and schemas
        engine = sa.create_engine(PudlPaths().pudl_db)
        md = Package.from_resource_ids().to_sql()
        md.create_all(engine)
        # Run the ETL and generate a new PUDL SQLite DB for testing:
        pudl_etl_job_factory()().execute_in_process(
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
    # Grab a connection to the freshly populated PUDL DB, and hand it off.
    # All the hard work here is being done by the datapkg and
    # datapkg_to_sqlite fixtures, above.
    context = build_init_resource_context()
    return pudl_sqlite_io_manager(context)


@pytest.fixture(scope="session")
def pudl_engine(pudl_sql_io_manager):
    """Get PUDL SQL engine from io manager."""
    return pudl_sql_io_manager.engine


@pytest.fixture(scope="session", autouse=True)
def configure_paths_for_tests(tmp_path_factory, request):
    """Configures PudlPaths for tests."""
    gha_override_input = False
    gha_override_output = False
    if os.environ.get("GITHUB_ACTIONS", False):
        gha_override_input = "PUDL_INPUTS" not in os.environ
        gha_override_output = "PUDL_OUTPUTS" not in os.environ
        logger.info(
            "Running in GitHub Actions environment, using"
            f" temporary input dir: {gha_override_input}, and"
            f" temporary output dir: {gha_override_output}"
        )
    pudl_tmpdir = tmp_path_factory.mktemp("pudl")
    if gha_override_output or request.config.getoption("--tmp-data"):
        in_tmp = pudl_tmpdir / "data"
        in_tmp.mkdir()
        PudlPaths.set_path_overrides(
            input_dir=str(Path(in_tmp).resolve()),
        )
    if gha_override_output or not request.config.getoption("--live-dbs"):
        out_tmp = pudl_tmpdir / "output"
        out_tmp.mkdir()
        PudlPaths.set_path_overrides(
            output_dir=str(Path(out_tmp).resolve()),
        )
    logger.info(f"Starting unit tests with output path {PudlPaths().output_dir}")
    pudl.workspace.setup.init()


@pytest.fixture(scope="session")
def dataset_settings_config(request, etl_settings):
    """Create dagster dataset_settings resource."""
    return etl_settings.datasets.dict()


@pytest.fixture(scope="session")  # noqa: C901
def pudl_datastore_config(request):
    """Produce a :class:pudl.workspace.datastore.Datastore."""
    gcs_cache_path = request.config.getoption("--gcs-cache-path")
    return {
        "gcs_cache_path": gcs_cache_path if gcs_cache_path else "",
        "use_local_cache": not request.config.getoption("--bypass-local-cache"),
    }


@pytest.fixture(scope="session")
def pudl_datastore_fixture(pudl_datastore_config):
    """Create pudl Datastore resource."""
    init_context = build_init_resource_context(config=pudl_datastore_config)
    return resources.datastore(init_context)


def skip_table_if_null_freq_table(table_name, freq):
    """Check."""
    if table_name in AS_MS_ONLY_FREQ_TABLES and freq is None:
        pytest.skip(
            f"Data validation for {table_name} does not work with a null frequency."
        )
