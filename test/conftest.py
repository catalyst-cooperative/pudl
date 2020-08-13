"""PyTest configuration module. Defines useful fixtures, command line args."""
import glob
import logging
import os
import pathlib

import datapackage
import pandas as pd
import pytest
import sqlalchemy as sa
import yaml

import pudl
from pudl import constants as pc
from pudl.output.pudltabl import PudlTabl

logger = logging.getLogger(__name__)

START_DATE_EIA = pd.to_datetime(f"{min(pc.working_years['eia923'])}-01-01")
END_DATE_EIA = pd.to_datetime(f"{max(pc.working_years['eia923'])}-12-31")
START_DATE_FERC1 = pd.to_datetime(f"{min(pc.working_years['ferc1'])}-01-01")
END_DATE_FERC1 = pd.to_datetime(f"{max(pc.working_years['ferc1'])}-12-31")


def pytest_addoption(parser):
    """Add a command line option for using the live FERC/PUDL DB."""
    parser.addoption("--live_ferc1_db", action="store", default=False,
                     help="""Path to a live rather than temporary FERC DB.
                          Use --live_ferc1_db=AUTO if FERC DB location concurs
                          with derived pudl_out.""")
    parser.addoption("--live_pudl_db", action="store", default=False,
                     help="""Path to a live rather than temporary PUDL DB.
                          Use --live_pudl_db=AUTO if PUDL DB location concurs
                          with derived pudl_out.""")
    parser.addoption("--pudl_in", action="store", default=False,
                     help="Path to the top level PUDL inputs directory.")
    parser.addoption("--pudl_out", action="store", default=False,
                     help="Path to the top level PUDL outputs directory.")
    parser.addoption("--fast", action="store_true", default=False,
                     help="Use minimal test data to speed up the tests.")
    parser.addoption("--clobber", action="store_true", default=False,
                     help="Clobber the existing datapackages if they exist")


@pytest.fixture(scope='session')
def live_ferc1_db(request):
    """Use the live FERC DB or make a temporary one."""
    return request.config.getoption("--live_ferc1_db")


@pytest.fixture(scope='session')
def live_pudl_db(request):
    """Fixture that tells use which PUDL DB to use (live vs. testing)."""
    return request.config.getoption("--live_pudl_db")


@pytest.fixture(scope='session')
def fast_tests(request):
    """
    Set a boolean flag indicating whether we are doing full or fast tests.

    We sometimes want to do a quick sanity check while testing locally, and
    that can be accomplished by setting the --fast flag on the command line.
    if fast_tests is true, then we only use 1 year of data, otherwise we use
    all available data (all the working_years for each dataset).

    Additionally, if we are on a CI platform, we *always* want to use the fast
    tests, regardless of what has been passed in on the command line with the
    --fast arguement.

    Returns:
        boolean

    """
    fast_tests = request.config.getoption("--fast")
    if os.getenv('CI'):
        logger.info("We're testing on CI platform, using minimal data.")
        fast_tests = True

    return fast_tests


@pytest.fixture(scope='session')
def data_scope(fast_tests, pudl_settings_fixture):
    """Define data scope for tests for CI vs. local use."""
    scope = {}
    test_dir = pathlib.Path(__file__).parent
    if fast_tests:
        settings_file = 'fast-test.yml'
        # the ferc1_dbf_tables are for the ferc1_engine. they refer to ferc1
        # dbf table names, not pudl table names. for the fast test, we only pull
        # in tables we need for pudl.
        scope['ferc1_dbf_tables'] = [
            pc.table_map_ferc1_pudl[k] for k in pc.pudl_tables["ferc1"]
        ] + ["f1_respondent_id"]
    else:
        settings_file = 'full-test.yml'
        scope['ferc1_dbf_tables'] = [
            tbl for tbl in pc.ferc1_tbl2dbf if tbl not in pc.ferc1_huge_tables
        ]
    with open(pathlib.Path(test_dir, 'settings', settings_file),
              "r") as f:
        datapkg_settings = yaml.safe_load(f)
    # put the whole settings dictionary
    scope.update(datapkg_settings)
    try:
        datapkg_bundle_doi = datapkg_settings["datapkg_bundle_doi"]
        if not pudl.helpers.is_doi(datapkg_bundle_doi):
            raise ValueError(
                f"Found invalid bundle DOI: {datapkg_bundle_doi} "
                f"in bundle {datapkg_settings['datpkg_bundle_name']}."
            )
    except KeyError:
        datapkg_bundle_doi = None

    scope["datapkg_bundle_doi"] = datapkg_bundle_doi
    # copy the etl parameters (years, tables, states) from the datapkg dataset
    # settings into the scope so they are more easily available
    scope.update(pudl.etl.get_flattened_etl_parameters(
        datapkg_settings['datapkg_bundle_settings']))
    return scope


@pytest.fixture(scope='session', params=['AS'], ids=['ferc1_annual'])
def pudl_out_ferc1(live_pudl_db, pudl_engine, request):
    """Define parameterized PudlTabl output object fixture for FERC 1 tests."""
    if not live_pudl_db:
        raise AssertionError("Output tests only work with a live PUDL DB.")
    return PudlTabl(pudl_engine=pudl_engine,
                    start_date=START_DATE_FERC1,
                    end_date=END_DATE_FERC1,
                    freq=request.param)


@pytest.fixture(
    scope="session",
    params=[None, "AS", "MS"],
    ids=["eia_raw", "eia_annual", "eia_monthly"]
)
def pudl_out_eia(live_pudl_db, pudl_engine, request):
    """Define parameterized PudlTabl output object fixture for EIA tests."""
    if not live_pudl_db:
        raise AssertionError("Output tests only work with a live PUDL DB.")
    return PudlTabl(pudl_engine=pudl_engine,
                    start_date=START_DATE_EIA,
                    end_date=END_DATE_EIA,
                    freq=request.param,
                    fill=True,
                    roll=True,
                    )


@pytest.fixture(scope='session')
def pudl_out_orig(live_pudl_db, pudl_engine):
    """Create an unaggregated PUDL output object for checking raw data."""
    if not live_pudl_db:
        raise AssertionError("Output tests only work with a live PUDL DB.")
    return PudlTabl(pudl_engine=pudl_engine,
                    start_date=START_DATE_EIA,
                    end_date=END_DATE_EIA)


@pytest.fixture(scope='session')
def ferc1_engine(live_ferc1_db, pudl_settings_fixture,
                 data_scope, request):
    """
    Grab a connection to the FERC Form 1 DB clone.

    If we are using the test database, we initialize it from scratch first.
    If we're using the live database, then we just yield a conneciton to it.
    """
    clobber = request.config.getoption("--clobber")
    if not live_ferc1_db:
        pudl.extract.ferc1.dbf2sqlite(
            tables=data_scope['ferc1_dbf_tables'],
            years=data_scope['ferc1_years'],
            refyear=max(data_scope['ferc1_years']),
            pudl_settings=pudl_settings_fixture,
            clobber=clobber)
    engine = sa.create_engine(pudl_settings_fixture["ferc1_db"])
    yield engine

    logger.info(f'Engine: {engine}')

    if not live_ferc1_db:
        # Clean up after ourselves by dropping the test DB tables.
        pudl.helpers.drop_tables(engine, clobber=True)


@pytest.fixture(scope='session')
def datapkg_bundle(request, ferc1_engine,
                   pudl_settings_fixture, live_pudl_db, data_scope):
    """Generate limited packages for testing."""
    if not live_pudl_db:
        logger.info('setting up the datapkg_bundle fixture')
        clobber = request.config.getoption("--clobber")
        pudl.etl.generate_datapkg_bundle(
            data_scope['datapkg_bundle_settings'],
            pudl_settings_fixture,
            datapkg_bundle_name=data_scope['datapkg_bundle_name'],
            datapkg_bundle_doi=data_scope['datapkg_bundle_doi'],
            clobber=clobber)


@pytest.fixture(scope='session')
def pudl_engine(ferc1_engine, live_pudl_db, pudl_settings_fixture,
                data_scope, datapkg_bundle, request):
    """
    Grab a connection to the PUDL Database.

    If we are using the test database, we initialize the PUDL DB from scratch.
    If we're using the live database, then we just make a conneciton to it.
    """
    logger.info('setting up the pudl_engine fixture')
    if not live_pudl_db:
        # Generate the list of datapackages to merge...
        datapkg_bundle_dir = pathlib.Path(
            pudl_settings_fixture["datapkg_dir"],
            data_scope["datapkg_bundle_name"],
        )
        # Here we're gonna merge *any* datapackages found within the bundle:
        in_paths = glob.glob(f"{datapkg_bundle_dir}/*/datapackage.json")
        dps = [datapackage.DataPackage(descriptor=path) for path in in_paths]
        out_path = pathlib.Path(
            pudl_settings_fixture["datapkg_dir"],
            data_scope["datapkg_bundle_name"],
            "pudl-merged")
        # clobber has to be False here, because if the pudl-merged datapackage
        # already existed somehow in the datapkg_bundle_dir, then we're
        # merging things back in more than once and that's broken... so we want
        # it to fail if the merged package exists already.
        pudl.convert.merge_datapkgs.merge_datapkgs(
            dps, out_path, clobber=False)

        pudl.convert.datapkg_to_sqlite.datapkg_to_sqlite(
            sqlite_url=pudl_settings_fixture["pudl_db"],
            out_path=out_path, clobber=False)
    # Grab a connection to the freshly populated PUDL DB, and hand it off.
    # All the hard work here is being done by the datapkg and
    # datapkg_to_sqlite fixtures, above.
    pudl_engine = sa.create_engine(pudl_settings_fixture["pudl_db"])
    logger.info(pudl_engine)
    yield pudl_engine

    if not live_pudl_db:
        # Clean up after ourselves by dropping the test DB tables.
        pudl.helpers.drop_tables(pudl_engine, clobber=True)


@pytest.fixture(scope='session')  # noqa: C901
def pudl_settings_fixture(request, tmpdir_factory,  # noqa: C901
                          live_ferc1_db, live_pudl_db):  # noqa: C901
    """Determine some settings (mostly paths) for the test session."""
    logger.info('setting up the pudl_settings_fixture')
    # Create a session scoped temporary directory.
    pudl_dir = tmpdir_factory.mktemp('pudl')

    # Grab the user configuration, if it exists:
    try:
        pudl_auto = pudl.workspace.setup.get_defaults()
    except FileNotFoundError:
        pass

    # Grab the input / output dirs specified on the command line, if any:
    pudl_in = request.config.getoption("--pudl_in")
    pudl_out = request.config.getoption("--pudl_out")

    # Having this in a constant place will allow us to cache inputs which should
    # speed up the tests considerably
    if os.environ["GITHUB_ACTIONS"]:
        pudl_in = pathlib.Path(os.environ["HOME"]) / "pudl-work"
        pudl_out = pathlib.Path(os.environ["HOME"]) / "pudl-work"

    # By default, we use the command line option. If that is left False, then
    # we use a temporary directory. If the command_line option is AUTO, then
    # we use whatever the user has configured in their $HOME/.pudl.yml file.
    if pudl_in is False:
        pudl_in = pudl_dir
        os.environ["PUDL_IN"] = str(pudl_in)
    elif pudl_in == 'AUTO':
        pudl_in = pudl_auto['pudl_in']

    if pudl_out is False:
        pudl_out = pudl_dir
        os.environ["PUDL_OUT"] = str(pudl_out)
    elif pudl_out == 'AUTO':
        pudl_out = pudl_auto['pudl_out']

    logger.info(f"Using PUDL_IN={pudl_in}")
    logger.info(f"Using PUDL_OUT={pudl_out}")

    pudl_settings = pudl.workspace.setup.derive_paths(
        pudl_in=pudl_in,
        pudl_out=pudl_out)

    pudl.workspace.setup.init(pudl_in=pudl_in, pudl_out=pudl_out)

    if live_ferc1_db == 'AUTO':
        pudl_settings['ferc1_db'] = pudl_auto['ferc1_db']
    elif live_ferc1_db:
        live_ferc1_db_path = pathlib.Path(live_ferc1_db).expanduser().resolve()
        pudl_settings['ferc1_db'] = 'sqlite:///' + str(live_ferc1_db_path)

    if live_pudl_db == 'AUTO':
        pudl_settings['pudl_db'] = pudl_auto['pudl_db']
    elif live_pudl_db:
        live_pudl_db_path = pathlib.Path(live_pudl_db).expanduser().resolve()
        pudl_settings['pudl_db'] = 'sqlite:///' + \
            str(live_pudl_db_path)

    logger.info(f'pudl_settings being used : {pudl_settings}')
    pudl_settings["sandbox"] = True
    return pudl_settings


@pytest.fixture(scope='session')  # noqa: C901
def pudl_ferc1datastore_fixture(pudl_settings_fixture):
    """Produce a :class:pudl.extract.ferc1.Ferc1Datastore."""
    return pudl.extract.ferc1.Ferc1Datastore(
        pathlib.Path(pudl_settings_fixture["pudl_in"]),
        sandbox=pudl_settings_fixture["sandbox"])


@pytest.fixture(scope='session')  # noqa: C901
def pudl_datastore_fixture(pudl_settings_fixture):
    """Produce a :class:pudl.workspace.datastore.Datastore."""
    return pudl.workspace.datastore.Datastore(
        pathlib.Path(
            pudl_settings_fixture["pudl_in"]),
        sandbox=pudl_settings_fixture["sandbox"])


@pytest.fixture(scope='session')  # noqa: C901
def pudl_epacemsdatastore_fixture(pudl_settings_fixture):
    """Produce a :class:pudl.extract.epacems.EpaCemsDatastore."""
    return pudl.extract.epacems.EpaCemsDatastore(
        pathlib.Path(pudl_settings_fixture["pudl_in"]),
        sandbox=pudl_settings_fixture["sandbox"])
