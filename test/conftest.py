"""PyTest configuration module. Defines useful fixtures, command line args."""
import glob
import logging
import os
from pathlib import Path

import datapackage
import pytest
import sqlalchemy as sa
import yaml

import pudl
from pudl.output.pudltabl import PudlTabl

logger = logging.getLogger(__name__)


def pytest_addoption(parser):
    """Add a command line option Requiring fresh data download."""
    parser.addoption(
        "--live-dbs",
        action="store_true",
        default=False,
        help="Use existing PUDL/FERC1 DBs instead of creating temporary ones."
    )
    parser.addoption(
        "--tmp-data",
        action="store_true",
        default=False,
        help="Download fresh input data for use with this test run only."
    )
    parser.addoption(
        "--etl-settings",
        action="store",
        default=False,
        help="Path to a non-standard ETL settings file to use."
    )
    parser.addoption(
        "--gcs-cache-path",
        default=None,
        help="If set, use this GCS path as a datastore cache layer."
    )
    parser.addoption(
        "--sandbox",
        action="store_true",
        default=False,
        help="Use raw inputs from the Zenodo sandbox server."
    )


@pytest.fixture(scope="session", name="test_dir")
def test_directory():
    """Return the path to the top-level directory containing the tests."""
    return Path(__file__).parent


@pytest.fixture(scope='session', name="live_dbs")
def live_databases(request):
    """Fixture that tells whether to use existing live FERC1/PUDL DBs)."""
    return request.config.getoption("--live-dbs")


@pytest.fixture(scope='session', name="etl_params")
def etl_parameters(request, test_dir):
    """Read the ETL parameters from the test settings or proffered file."""
    if request.config.getoption("--etl-settings"):
        etl_params_yml = Path(request.config.getoption("--etl-settings"))
    else:
        etl_params_yml = Path(
            test_dir.parent / "src/pudl/package_data/settings/etl_fast.yml")
    with open(etl_params_yml, "r") as f:
        etl_params_out = yaml.safe_load(f)
    return etl_params_out


@pytest.fixture(scope="session", name="ferc1_etl_params")
def ferc1_etl_parameters(etl_params):
    """Read ferc1_to_sqlite parameters out of test settings dictionary."""
    return {k: etl_params[k] for k in etl_params if "ferc1_to_sqlite" in k}


@pytest.fixture(scope="session", name="pudl_etl_params")
def pudl_etl_parameters(etl_params):
    """Read PUDL ETL parameters out of test settings dictionary."""
    return {k: etl_params[k] for k in etl_params if "datapkg_bundle" in k}


@pytest.fixture(scope='session', params=['AS'], ids=['ferc1_annual'])
def pudl_out_ferc1(live_dbs, pudl_engine, request):
    """Define parameterized PudlTabl output object fixture for FERC 1 tests."""
    if not live_dbs:
        pytest.skip("Output tests only work with a live PUDL DB.")
    return PudlTabl(pudl_engine=pudl_engine, freq=request.param)


@pytest.fixture(
    scope="session",
    params=[None, "AS", "MS"],
    ids=["eia_raw", "eia_annual", "eia_monthly"]
)
def pudl_out_eia(live_dbs, pudl_engine, request):
    """Define parameterized PudlTabl output object fixture for EIA tests."""
    if not live_dbs:
        pytest.skip("Output tests only work with a live PUDL DB.")
    return PudlTabl(
        pudl_engine=pudl_engine,
        freq=request.param,
        fill_fuel_cost=True,
        roll_fuel_cost=True,
        fill_net_gen=False,
    )


@pytest.fixture(scope='session')
def pudl_out_orig(live_dbs, pudl_engine):
    """Create an unaggregated PUDL output object for checking raw data."""
    if not live_dbs:
        pytest.skip("Output tests only work with a live PUDL DB.")
    return PudlTabl(pudl_engine=pudl_engine)


@pytest.fixture(scope='session', name="ferc1_engine")
def ferc1_sql_engine(
    pudl_settings_fixture,
    live_dbs,
    ferc1_etl_params,
    pudl_datastore_fixture,
):
    """
    Grab a connection to the FERC Form 1 DB clone.

    If we are using the test database, we initialize it from scratch first.
    If we're using the live database, then we just yield a conneciton to it.
    """
    if not live_dbs:
        pudl.extract.ferc1.dbf2sqlite(
            tables=ferc1_etl_params['ferc1_to_sqlite_tables'],
            years=ferc1_etl_params['ferc1_to_sqlite_years'],
            refyear=ferc1_etl_params['ferc1_to_sqlite_refyear'],
            pudl_settings=pudl_settings_fixture,
            clobber=False,
            datastore=pudl_datastore_fixture
        )
    engine = sa.create_engine(pudl_settings_fixture["ferc1_db"])
    logger.info("FERC1 Engine: %s", engine)
    yield engine

    # Clean up after ourselves by dropping the test DB tables.
    if not live_dbs:
        pudl.helpers.drop_tables(engine, clobber=True)


@pytest.fixture(scope='session', name="datapkg_bundle")
def datapackage_bundle(
    ferc1_engine,  # Implicit dependency
    pudl_settings_fixture,
    live_dbs,
    pudl_etl_params
):
    """Generate limited packages for testing."""
    if not live_dbs:
        logger.info('setting up the datapkg_bundle fixture')
        pudl.etl.generate_datapkg_bundle(
            pudl_etl_params["datapkg_bundle_settings"],
            pudl_settings_fixture,
            datapkg_bundle_name=pudl_etl_params['datapkg_bundle_name'],
            datapkg_bundle_doi=pudl_etl_params['datapkg_bundle_doi'],
            clobber=False,
        )


@pytest.fixture(scope='session', name="pudl_engine")
def pudl_sql_engine(
    ferc1_engine,  # Implicit dependency
    live_dbs,
    pudl_settings_fixture,
    pudl_etl_params,
    datapkg_bundle,  # Implicity dependency
):
    """
    Grab a connection to the PUDL Database.

    If we are using the test database, we initialize the PUDL DB from scratch.
    If we're using the live database, then we just make a conneciton to it.
    """
    logger.info('setting up the pudl_engine fixture')
    if not live_dbs:
        # Generate the list of datapackages to merge...
        datapkg_bundle_dir = Path(
            pudl_settings_fixture["datapkg_dir"],
            pudl_etl_params["datapkg_bundle_name"],
        )
        # Here we're gonna merge *any* datapackages found within the bundle:
        in_paths = glob.glob(f"{datapkg_bundle_dir}/*/datapackage.json")
        dps = [datapackage.DataPackage(descriptor=path) for path in in_paths]
        out_path = Path(
            pudl_settings_fixture["datapkg_dir"],
            pudl_etl_params["datapkg_bundle_name"],
            "pudl-merged"
        )
        # clobber has to be False here, because if the pudl-merged datapackage
        # already existed somehow in the datapkg_bundle_dir, then we're
        # merging things back in more than once and that's broken... so we want
        # it to fail if the merged package exists already.
        pudl.convert.merge_datapkgs.merge_datapkgs(
            dps, out_path, clobber=False)
        pudl.convert.datapkg_to_sqlite.datapkg_to_sqlite(
            sqlite_url=pudl_settings_fixture["pudl_db"],
            out_path=out_path,
            clobber=False,
            fkeys=True,
        )
    # Grab a connection to the freshly populated PUDL DB, and hand it off.
    # All the hard work here is being done by the datapkg and
    # datapkg_to_sqlite fixtures, above.
    engine = sa.create_engine(pudl_settings_fixture["pudl_db"])
    logger.info('PUDL Engine: %s', engine)
    yield engine

    # Clean up after ourselves by dropping the test DB tables.
    if not live_dbs:
        pudl.helpers.drop_tables(engine, clobber=True)


@pytest.fixture(scope='session', name="pudl_settings_fixture")
def pudl_settings_dict(request, live_dbs, tmpdir_factory):  # noqa: C901
    """Determine some settings (mostly paths) for the test session."""
    logger.info('setting up the pudl_settings_fixture')
    # Create a session scoped temporary directory.
    tmpdir = tmpdir_factory.mktemp('pudl')
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
        pudl_in=pudl_in,
        pudl_out=pudl_out
    )
    pudl_settings["sandbox"] = request.config.getoption("--sandbox")
    # Set up the pudl workspace:
    pudl.workspace.setup.init(pudl_in=pudl_in, pudl_out=pudl_out)

    if live_dbs:
        pudl_settings["pudl_db"] = pudl.workspace.setup.get_defaults()[
            "pudl_db"]
        pudl_settings["ferc1_db"] = pudl.workspace.setup.get_defaults()[
            "ferc1_db"]

    logger.info("pudl_settings being used: %s", pudl_settings)
    return pudl_settings


@pytest.fixture(scope='session')  # noqa: C901
def pudl_ferc1datastore_fixture(pudl_datastore_fixture):
    """Produce a :class:pudl.extract.ferc1.Ferc1Datastore."""
    return pudl.extract.ferc1.Ferc1Datastore(pudl_datastore_fixture)


@pytest.fixture(scope='session', name="pudl_datastore_fixture")  # noqa: C901
def pudl_datastore(pudl_settings_fixture, request):
    """Produce a :class:pudl.workspace.datastore.Datastore."""
    gcs_cache = request.config.getoption("--gcs-cache-path")
    return pudl.workspace.datastore.Datastore(
        local_cache_path=Path(
            pudl_settings_fixture["pudl_in"]) / "data",
        gcs_cache_path=gcs_cache,
        sandbox=pudl_settings_fixture["sandbox"])
