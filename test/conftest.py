"""PyTest configuration module. Defines useful fixtures, command line args."""

import logging
import os
import pathlib
import shutil

import pandas as pd
import pytest
import sqlalchemy as sa
import yaml

import pudl
from pudl import constants as pc
from pudl.output.pudltabl import PudlTabl
from pudl.workspace import datastore

logger = logging.getLogger(__name__)

START_DATE_EIA = pd.to_datetime(f"{min(pc.working_years['eia923'])}-01-01")
END_DATE_EIA = pd.to_datetime(f"{max(pc.working_years['eia923'])}-12-31")
START_DATE_FERC1 = pd.to_datetime(f"{min(pc.working_years['ferc1'])}-01-01")
END_DATE_FERC1 = pd.to_datetime(f"{max(pc.working_years['ferc1'])}-12-31")


@pytest.fixture(scope='session')
def data_scope(fast_tests, pudl_settings_fixture):
    """Define data scope for tests for CI vs. local use."""
    data_scope = {}
    test_dir = pathlib.Path(__file__).parent
    if fast_tests:
        settings_file = 'settings_datapackage_fast.yml'
        # the ferc1_dbf_tables are for the ferc1_engine. they refer to ferc1
        # dbf table names, not pudl table names. for the fast test, we only pull
        # in tables we need for pudl.
        data_scope['ferc1_dbf_tables'] = pc.ferc1_default_tables
    else:
        settings_file = 'settings_datapackage_test.yml'
        data_scope['ferc1_dbf_tables'] = [
            tbl for tbl in pc.ferc1_tbl2dbf if tbl not in pc.ferc1_huge_tables
        ]
    with open(pathlib.Path(test_dir, 'settings', settings_file),
              "r") as f:
        pkg_settings = yaml.safe_load(f)
    # put the whole settings dictionary
    data_scope.update(pkg_settings)
    # copy the etl parameters (years, tables, states) from the pkg dataset
    # settings into the data_scope so they are more easily available
    data_scope.update(pudl.etl.get_flattened_etl_parameters(
        pkg_settings['pkg_bundle_settings']))
    return data_scope


def pytest_addoption(parser):
    """Add a command line option for using the live FERC/PUDL DB."""
    parser.addoption("--live_ferc_db", action="store", default=False,
                     help="""Path to a live rather than temporary FERC DB.
                          Use --live_ferc_db=AUTO if FERC DB location concurs
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
def live_ferc_db(request):
    """Use the live FERC DB or make a temporary one."""
    return request.config.getoption("--live_ferc_db")


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
    params=["AS", "MS"],
    ids=["eia_annual", "eia_monthly"]
)
def pudl_out_eia(live_pudl_db, pudl_engine, request):
    """Define parameterized PudlTabl output object fixture for EIA tests."""
    if not live_pudl_db:
        raise AssertionError("Output tests only work with a live PUDL DB.")
    return PudlTabl(pudl_engine=pudl_engine,
                    start_date=START_DATE_EIA,
                    end_date=END_DATE_EIA,
                    freq=request.param)


@pytest.fixture(scope='session')
def pudl_out_orig(live_pudl_db, pudl_engine):
    """Create an unaggregated PUDL output object for checking raw data."""
    if not live_pudl_db:
        raise AssertionError("Output tests only work with a live PUDL DB.")
    return PudlTabl(pudl_engine=pudl_engine,
                    start_date=START_DATE_EIA,
                    end_date=END_DATE_EIA)


@pytest.fixture(scope='session')
def ferc1_engine(live_ferc_db, pudl_settings_fixture,
                 datastore_fixture, data_scope):
    """
    Grab a connection to the FERC Form 1 DB clone.

    If we are using the test database, we initialize it from scratch first.
    If we're using the live database, then we just yield a conneciton to it.
    """
    if not live_ferc_db:
        pudl.extract.ferc1.dbf2sqlite(
            tables=data_scope['ferc1_dbf_tables'],
            years=data_scope['ferc1_years'],
            refyear=max(data_scope['ferc1_years']),
            pudl_settings=pudl_settings_fixture)
    # for now let's check if the ferc1 database has any tables...
    pudl.extract.ferc1.get_ferc1_meta(pudl_settings_fixture)

    # Grab a connection to the freshly populated database, and hand it off.
    engine = sa.create_engine(pudl_settings_fixture["ferc1_db"])
    yield engine

    logger.info(f'Engine: {engine}')

    if not live_ferc_db:
        # Clean up after ourselves by dropping the test DB tables.
        pudl.helpers.drop_tables(engine)


@pytest.fixture(scope='session')
def data_packaging(request, datastore_fixture, ferc1_engine,
                   pudl_settings_fixture, data_scope):
    """Generate limited packages for testing."""
    logger.info('setting up the data_packaging fixture')
    clobber = request.config.getoption("--clobber")
    pudl.etl.generate_data_packages(
        data_scope['pkg_bundle_settings'],
        pudl_settings_fixture,
        pkg_bundle_name=data_scope['pkg_bundle_name'],
        clobber=clobber)


@pytest.fixture(scope='session')
def data_packaging_to_sqlite(pudl_settings_fixture, data_scope, data_packaging):
    """Try flattening the data packages."""
    logger.info('setting up the data_packaging_to_sqlite fixture')
    pudl.convert.flatten_datapkgs.flatten_pudl_datapackages(
        pudl_settings_fixture,
        pkg_bundle_name=data_scope['pkg_bundle_name'],
        pkg_name='pudl-all')

    pudl.convert.datapkg_to_sqlite.pkg_to_sqlite_db(
        pudl_settings_fixture,
        pkg_bundle_name=data_scope['pkg_bundle_name'],
        pkg_name='pudl-all')


@pytest.fixture(scope='session')
def pudl_engine(ferc1_engine, live_pudl_db,
                pudl_settings_fixture,
                datastore_fixture, data_scope, request):
    """
    Grab a connection to the PUDL Database.

    If we are using the test database, we initialize the PUDL DB from scratch.
    If we're using the live database, then we just make a conneciton to it.
    """
    logger.info('setting up the pudl_engine fixture')
    if not live_pudl_db:
        clobber = request.config.getoption("--clobber")
        pudl.etl.generate_data_packages(
            data_scope['pkg_bundle_settings'],
            pudl_settings_fixture,
            pkg_bundle_name=data_scope['pkg_bundle_name'],
            clobber=clobber)

        pudl.convert.flatten_datapkgs.flatten_pudl_datapackages(
            pudl_settings_fixture,
            pkg_bundle_name=data_scope['pkg_bundle_name'],
            pkg_name='pudl-all')

        pudl.convert.datapkg_to_sqlite.pkg_to_sqlite_db(
            pudl_settings_fixture,
            pkg_bundle_name=data_scope['pkg_bundle_name'],
            pkg_name='pudl-all')
    # Grab a connection to the freshly populated PUDL DB, and hand it off.
    pudl_engine = sa.create_engine(pudl_settings_fixture["pudl_db"])
    logger.info(pudl_engine)
    yield pudl_engine

    if not live_pudl_db:
        # Clean up after ourselves by dropping the test DB tables.
        pudl.helpers.drop_tables(pudl_engine)


@pytest.fixture(scope='session')  # noqa: C901
def pudl_settings_fixture(request, tmpdir_factory, live_ferc_db, live_pudl_db):
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

    # By default, we use the command line option. If that is left False, then
    # we use a temporary directory. If the command_line option is AUTO, then
    # we use whatever the user has configured in their $HOME/.pudl.yml file.
    if pudl_in is False:
        pudl_in = pudl_dir
    elif pudl_in == 'AUTO':
        pudl_in = pudl_auto['pudl_in']

    if pudl_out is False:
        pudl_out = pudl_dir
    elif pudl_out == 'AUTO':
        pudl_out = pudl_auto['pudl_out']

    logger.info(f"Using PUDL_IN={pudl_in}")
    logger.info(f"Using PUDL_OUT={pudl_out}")

    pudl_settings = pudl.workspace.setup.derive_paths(
        pudl_in=pudl_in,
        pudl_out=pudl_out)

    pudl.workspace.setup.init(pudl_in=pudl_in, pudl_out=pudl_out)

    if live_ferc_db == 'AUTO':
        pudl_settings['ferc1_db'] = pudl_auto['ferc1_db']
    elif live_ferc_db:
        live_ferc_db_path = pathlib.Path(live_ferc_db).expanduser().resolve()
        pudl_settings['ferc1_db'] = 'sqlite:///' + str(live_ferc_db_path)

    if live_pudl_db == 'AUTO':
        pudl_settings['pudl_db'] = pudl_auto['pudl_db']
    elif live_pudl_db:
        live_pudl_db_path = pathlib.Path(live_pudl_db).expanduser().resolve()
        pudl_settings['pudl_db'] = 'sqlite:///' + \
            str(live_pudl_db_path)

    logger.info(f'pudl_settings being used : {pudl_settings}')
    return pudl_settings


@pytest.fixture(scope='session')
def datastore_fixture(pudl_settings_fixture, data_scope):
    """
    Populate a minimal PUDL datastore for the Travis CI tests to access.

    Downloads from FERC & EPA have been... throttled or something, so we
    apparently can't pull the data from them directly any more. To deal with
    that we have checked a small amount of FERC Form 1 and EPA CEMS data into
    the PUDL repository.

    For the EIA 860 and EIA 923 data, we can use the datastore management
    library directly. It's important that we do this from within the tests,
    and not by using the update_datstore script during setup for the Travis
    tests, for two reasons:
     * the pudl module is not installed and importable until after the tox
       run has begun, so unless we import pudl from the filesystem, we can't
       use the script beforehand... and doing so would contaminate the
       environment.
     * Calling the datastore management functions from within the tests will
       add that code to our measurement of test coverage, which is good!
    """
    sources_to_update = [
        'epaipm',
        'eia860',
        'eia923'
    ]

    years_by_source = {
        'eia860': data_scope['eia860_years'],
        'eia923': data_scope['eia923_years'],
        'epacems': [],
        'epaipm': [None, ],
        'ferc1': [],
    }
    # Sadly, FERC & EPA only provide access to their data via FTP, and it's
    # not possible to use FTP from within the Travis CI environment:
    if os.getenv('TRAVIS'):
        logger.info(f"Building a special Travis CI datastore for PUDL.")
        # Simulate having downloaded the data...
        dl_dir = pathlib.Path(pudl_settings_fixture['data_dir'], 'tmp')

        epacems_files = (
            pathlib.Path(os.getenv('TRAVIS_BUILD_DIR'),
                         'test/data/epa/cems/epacems2017/').
            glob('*.zip')
        )
        # Copy the files over to the test-run proto-datastore:
        for file in epacems_files:
            logger.info(
                f"Faking download of {os.path.basename(file)} to {dl_dir}")
            shutil.copy(file, dl_dir)

        # The datastore knows what to do with a file it finds in this dir:
        datastore.organize(
            source='epacems',
            year=2017,
            states=['ID'],
            data_dir=pudl_settings_fixture['data_dir'],
            unzip=True
        )

        ferc1_files = (
            pathlib.Path(os.getenv('TRAVIS_BUILD_DIR'),
                         'test/data/ferc/form1/f1_2017/').
            glob('*.zip')
        )
        # Copy the files over to the test-run proto-datastore:
        for file in ferc1_files:
            logger.info(f"Faking the download of {file} to {dl_dir}")
            shutil.copy(file, dl_dir)

        # The datastore knows what to do with a file it finds in this dir:
        datastore.organize(
            source='ferc1',
            year=2017,
            states=None,
            data_dir=pudl_settings_fixture['data_dir'],
            unzip=True
        )
        states = []
    else:
        sources_to_update.extend(["ferc1", "epacems"])
        years_by_source["ferc1"] = data_scope["ferc1_years"]
        years_by_source["epacems"] = data_scope["epacems_years"]
        states = ["id"]

    # Download the test year for each dataset that we're downloading...
    datastore.parallel_update(
        sources=sources_to_update,
        years_by_source=years_by_source,
        states=states,
        data_dir=pudl_settings_fixture["data_dir"],
    )

    pudl.helpers.verify_input_files(
        ferc1_years=years_by_source["ferc1"],
        eia923_years=years_by_source["eia923"],
        eia860_years=years_by_source["eia860"],
        epacems_years=years_by_source["epacems"],
        epacems_states=states,
        pudl_settings=pudl_settings_fixture,
    )
