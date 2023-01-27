"""PyTest configuration module.

Defines useful fixtures, command line args.
"""
import json
import logging
import os
from pathlib import Path

import pytest
import sqlalchemy as sa
import yaml
from ferc_xbrl_extractor import xbrl

import pudl
from pudl.extract.ferc1 import extract_xbrl_metadata
from pudl.extract.xbrl import FercXbrlDatastore, _get_sqlite_engine
from pudl.output.pudltabl import PudlTabl
from pudl.settings import DatasetsSettings, EtlSettings, XbrlFormNumber

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
        "--sandbox",
        action="store_true",
        default=False,
        help="Use raw inputs from the Zenodo sandbox server.",
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
    with open(etl_settings_yml, encoding="utf8") as settings_file:
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


@pytest.fixture(scope="session")
def pudl_out_orig(live_dbs, pudl_engine):
    """Create an unaggregated PUDL output object for checking raw data."""
    if not live_dbs:
        pytest.skip("Validation tests only work with a live PUDL DB.")
    return PudlTabl(pudl_engine=pudl_engine)


@pytest.fixture(scope="session", name="ferc1_engine_dbf")
def ferc1_dbf_sql_engine(
    pudl_settings_fixture,
    live_dbs,
    ferc_to_sqlite_settings,
    pudl_datastore_fixture,
):
    """Grab a connection to the FERC Form 1 DB clone.

    If we are using the test database, we initialize it from scratch first. If we're
    using the live database, then we just yield a conneciton to it.
    """
    if not live_dbs:
        pudl.extract.ferc1.dbf2sqlite(
            ferc1_to_sqlite_settings=ferc_to_sqlite_settings.ferc1_dbf_to_sqlite_settings,
            pudl_settings=pudl_settings_fixture,
            clobber=False,
            datastore=pudl_datastore_fixture,
        )
    engine = sa.create_engine(pudl_settings_fixture["ferc1_db"])
    logger.info("FERC1 Engine: %s", engine)
    return engine


@pytest.fixture(scope="session")
def ferc_xbrl(
    pudl_settings_fixture,
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

            sqlite_engine = _get_sqlite_engine(form.value, pudl_settings_fixture, True)

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
                datapackage_path=pudl_settings_fixture[
                    f"ferc{form.value}_xbrl_datapackage"
                ],
                metadata_path=pudl_settings_fixture[
                    f"ferc{form.value}_xbrl_taxonomy_metadata"
                ],
                archive_file_path=taxonomy_entry_point,
            )


@pytest.fixture(scope="session")
def ferc1_engine_xbrl(pudl_settings_fixture, ferc_xbrl):
    """Grab connection to raw FERC1 XBRL db."""
    engine = sa.create_engine(pudl_settings_fixture["ferc1_xbrl_db"])
    logger.info("FERC1 Engine: %s", engine)
    return engine


@pytest.fixture(scope="session", name="ferc1_xbrl_taxonomy_metadata")
def ferc1_xbrl_taxonomy_metadata(
    pudl_settings_fixture, pudl_etl_settings: DatasetsSettings, ferc1_engine_xbrl
):
    """Read the FERC 1 XBRL taxonomy metadata from JSON."""
    return extract_xbrl_metadata(
        ferc1_settings=pudl_etl_settings.ferc1,
        pudl_settings=pudl_settings_fixture,
    )


@pytest.fixture(scope="session", name="pudl_engine")
def pudl_sql_engine(
    ferc1_engine_dbf,  # Implicit dependency
    ferc1_engine_xbrl,  # Implicit dependency
    live_dbs,
    pudl_settings_fixture,
    etl_settings,
    check_foreign_keys,
    request,
):
    """Grab a connection to the PUDL Database.

    If we are using the test database, we initialize the PUDL DB from scratch. If we're
    using the live database, then we just make a conneciton to it.
    """
    logger.info("setting up the pudl_engine fixture")
    if not live_dbs:
        # Run the ETL and generate a new PUDL SQLite DB for testing:
        pudl.etl.etl(
            etl_settings=etl_settings,
            pudl_settings=pudl_settings_fixture,
            clobber=False,
            check_foreign_keys=check_foreign_keys,
            check_types=True,
            check_values=True,
            use_local_cache=not request.config.getoption("--bypass-local-cache"),
            gcs_cache_path=request.config.getoption("--gcs-cache-path"),
        )
    # Grab a connection to the freshly populated PUDL DB, and hand it off.
    # All the hard work here is being done by the datapkg and
    # datapkg_to_sqlite fixtures, above.
    engine = sa.create_engine(pudl_settings_fixture["pudl_db"])
    logger.info("PUDL Engine: %s", engine)
    return engine


@pytest.fixture(scope="session", name="pudl_settings_fixture")
def pudl_settings_dict(request, live_dbs, tmpdir_factory):  # noqa: C901
    """Determine some settings (mostly paths) for the test session."""
    logger.info("setting up the pudl_settings_fixture")
    # Create a session scoped temporary directory.
    tmpdir = tmpdir_factory.mktemp("pudl")
    # Outputs are always written to a temporary directory:
    pudl_out = tmpdir

    # In CI we want a hard-coded path for input caching purposes:
    if os.environ.get("GITHUB_ACTIONS", False):
        pudl_in = Path(os.environ["HOME"]) / "pudl-work"
    # If --tmp-data is set, create a disposable temporary datastore:
    elif request.config.getoption("--tmp-data"):
        pudl_in = tmpdir
    # Otherwise, default to the user's existing datastore:
    else:
        try:
            defaults = pudl.workspace.setup.get_defaults()
        except FileNotFoundError as err:
            logger.critical("Could not identify PUDL_IN / PUDL_OUT.")
            raise err
        pudl_in = defaults["pudl_in"]

    # Set these environment variables for future reference...
    logger.info("Using PUDL_IN=%s", pudl_in)
    os.environ["PUDL_IN"] = str(pudl_in)
    logger.info("Using PUDL_OUT=%s", pudl_out)
    os.environ["PUDL_OUT"] = str(pudl_out)

    # Build all the pudl_settings paths:
    pudl_settings = pudl.workspace.setup.derive_paths(
        pudl_in=pudl_in, pudl_out=pudl_out
    )
    pudl_settings["sandbox"] = request.config.getoption("--sandbox")
    # Set up the pudl workspace:
    pudl.workspace.setup.init(pudl_in=pudl_in, pudl_out=pudl_out)

    if live_dbs:
        pudl_defaults = pudl.workspace.setup.get_defaults()
        # everything with the following suffixes should use the defaults as opposed to
        # the generated settings. We should overhaul using temp_dir's for pudl_out at
        # all while using `live_dbs` and perhaps change the name of `live_dbs` bc it
        # now encompasses more than just dbs
        overwrite_suffixes = ("_db", "_datapackage", "_xbrl_taxonomy_metadata")
        pudl_settings = {
            k: pudl_defaults[k] if k.endswith(overwrite_suffixes) else v
            for (k, v) in pudl_settings.items()
        }

        pudl_settings["parquet_dir"] = pudl_defaults["parquet_dir"]
        pudl_settings["sqlite_dir"] = pudl_defaults["sqlite_dir"]

    pretty_settings = json.dumps(
        {str(k): str(v) for k, v in pudl_settings.items()}, indent=2
    )
    logger.info(f"pudl_settings being used: {pretty_settings}")
    return pudl_settings


@pytest.fixture(scope="session")  # noqa: C901
def ferc1_dbf_datastore_fixture(pudl_datastore_fixture):
    """Produce a :class:pudl.extract.ferc1.Ferc1DbfDatastore."""
    return pudl.extract.ferc1.Ferc1DbfDatastore(pudl_datastore_fixture)


@pytest.fixture(scope="session", name="pudl_ds_kwargs")
def ds_kwargs(pudl_settings_fixture, request):
    """Return a dictionary of keyword args for creating a PUDL datastore."""
    kwargs = dict(
        gcs_cache_path=request.config.getoption("--gcs-cache-path"),
        sandbox=pudl_settings_fixture["sandbox"],
    )

    use_local_cache = not request.config.getoption("--bypass-local-cache")
    if use_local_cache:
        kwargs["local_cache_path"] = Path(pudl_settings_fixture["pudl_in"]) / "data"

    return kwargs


@pytest.fixture(scope="session", name="pudl_datastore_fixture")  # noqa: C901
def pudl_datastore(pudl_ds_kwargs):
    """Produce a :class:pudl.workspace.datastore.Datastore."""
    return pudl.workspace.datastore.Datastore(**pudl_ds_kwargs)


def skip_table_if_null_freq_table(table_name, freq):
    """Check."""
    if table_name in AS_MS_ONLY_FREQ_TABLES and freq is None:
        pytest.skip(
            f"Data validation for {table_name} does not work with a null frequency."
        )
