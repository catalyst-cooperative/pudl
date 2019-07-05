"""PyTest configuration module. Defines useful fixtures, command line args."""

import logging
import os
import pathlib
import shutil

import pandas as pd
import pytest

import pudl
from pudl import constants as pc
from pudl.datastore import datastore
from pudl.output.pudltabl import PudlTabl

logger = logging.getLogger(__name__)

START_DATE_EIA = pd.to_datetime(f"{min(pc.working_years['eia923'])}-01-01")
END_DATE_EIA = pd.to_datetime(f"{max(pc.working_years['eia923'])}-12-31")
START_DATE_FERC1 = pd.to_datetime(f"{min(pc.working_years['ferc1'])}-01-01")
END_DATE_FERC1 = pd.to_datetime(f"{max(pc.working_years['ferc1'])}-12-31")


def pytest_addoption(parser):
    """Add a command line option for using the live FERC/PUDL DB."""
    parser.addoption("--live_ferc_db", action="store_true", default=False,
                     help="Use live FERC DB rather than test DB.")
    parser.addoption("--live_pudl_db", action="store_true", default=False,
                     help="Use live PUDL DB rather than test DB.")
    parser.addoption("--pudl_in", action="store", default=False,
                     help="Path to the top level PUDL inputs directory.")
    parser.addoption("--pudl_out", action="store", default=False,
                     help="Path to the top level PUDL outputs directory.")


@pytest.fixture(scope='session')
def live_ferc_db(request):
    """Fixture that tells use which FERC DB to use (live vs. testing)."""
    return request.config.getoption("--live_ferc_db")


@pytest.fixture(scope='session')
def live_pudl_db(request):
    """Fixture that tells use which PUDL DB to use (live vs. testing)."""
    return request.config.getoption("--live_pudl_db")


@pytest.fixture(
    scope='session',
    params=[
        PudlTabl(start_date=START_DATE_FERC1,
                 end_date=END_DATE_FERC1,
                 freq='AS', testing=False),
    ],
    ids=['ferc1_annual']
)
def pudl_out_ferc1(live_pudl_db, request):
    if not live_pudl_db:
        raise AssertionError("Output tests only work with a live PUDL DB.")
    return request.param


@pytest.fixture(
    scope='session',
    params=[
        PudlTabl(start_date=START_DATE_EIA,
                 end_date=END_DATE_EIA,
                 freq='AS', testing=False),
        PudlTabl(start_date=START_DATE_EIA,
                 end_date=END_DATE_EIA,
                 freq='MS', testing=False)
    ],
    ids=['eia_annual', 'eia_monthly']
)
def pudl_out_eia(live_pudl_db, request):
    if not live_pudl_db:
        raise AssertionError("Output tests only work with a live PUDL DB.")
    return request.param


@pytest.fixture(scope='session')
def ferc1_engine(live_ferc_db, pudl_settings_fixture):
    """
    Grab a connection to the FERC Form 1 DB clone.

    If we are using the test database, we initialize it from scratch first.
    If we're using the live database, then we just yield a conneciton to it.
    """
    # Avoid the 6.5 GB of binary garbage in these tables for testing....
    tables = [
        tbl for tbl in pc.ferc1_tbl2dbf if tbl not in pc.ferc1_huge_tables
    ]

    if not live_ferc_db:
        pudl.extract.ferc1.dbf2sqlite(
            tables=tables,
            years=pc.data_years['ferc1'],
            refyear=max(pc.working_years['ferc1']),
            pudl_settings=pudl_settings_fixture,
            testing=(not live_ferc_db))

    # Grab a connection to the freshly populated database, and hand it off.
    engine = pudl.extract.ferc1.connect_db(
        pudl_settings=pudl_settings_fixture,
        testing=(not live_ferc_db)
    )
    yield engine

    if not live_ferc_db:
        # Clean up after ourselves by dropping the test DB tables.
        pudl.extract.ferc1.drop_tables(engine)


@pytest.fixture(scope='session')
def pudl_engine(ferc1_engine,
                live_pudl_db, live_ferc_db,
                pudl_settings_fixture):
    """
    Grab a connection to the PUDL Database.

    If we are using the test database, we initialize the PUDL DB from scratch.
    If we're using the live database, then we just make a conneciton to it.
    """
    if not live_pudl_db:
        pudl.init.init_db(ferc1_tables=pc.pudl_tables['ferc1'],
                          ferc1_years=pc.working_years['ferc1'],
                          eia923_tables=pc.pudl_tables['eia923'],
                          eia923_years=pc.working_years['eia923'],
                          eia860_tables=pc.pudl_tables['eia860'],
                          eia860_years=pc.working_years['eia860'],
                          # Full EPA CEMS ingest takes 8 hours, so using an
                          # abbreviated list here
                          epacems_years=pc.travis_ci_epacems_years,
                          epacems_states=pc.travis_ci_epacems_states,
                          epaipm_tables=pc.pudl_tables['epaipm'],
                          pudl_settings=pudl_settings_fixture,
                          pudl_testing=(not live_pudl_db),
                          ferc1_testing=(not live_ferc_db))

    # Grab a connection to the freshly populated PUDL DB, and hand it off.
    pudl_engine = pudl.init.connect_db(
        pudl_settings=pudl_settings_fixture,
        testing=(not live_pudl_db)
    )
    yield pudl_engine

    if not live_pudl_db:
        # Clean up after ourselves by dropping the test DB tables.
        pudl.init.drop_tables(pudl_engine)


@pytest.fixture(scope='session')
def pudl_settings_fixture(request, tmpdir_factory):
    """Determine some settings (mostly paths) for the test session."""
    # Create a session scoped temporary directory.
    pudl_dir = tmpdir_factory.mktemp('pudl')

    # Grab the user configuration, if it exists:
    try:
        pudl_auto = pudl.settings.init()
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

    pudl_settings = pudl.settings.init(pudl_in=pudl_in, pudl_out=pudl_out)
    pudl.settings.setup(pudl_settings)

    return pudl_settings


@pytest.fixture(scope='session')
def datastore_fixture(pudl_settings_fixture):
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
    # In an ideal world it would work like this...
    sources = ['epaipm', 'eia860', 'eia923']
    years_by_source = {
        'epaipm': [None],
        'eia860': [2017],
        'eia923': [2017],
    }
    # Sadly, FERC & EPA have blocked downloads from Travis, so their data has
    # to be hacked in locally if we're running there:
    if os.getenv('TRAVIS'):
        logger.info(f"Building a PUDL datastore for Travis CI.")
        # Simulate having downloaded the data...
        dl_dir = pathlib.Path(pudl_settings_fixture['data_dir'], 'tmp')

        epacems_files = (
            pathlib.Path(os.getenv('TRAVIS_BUILD_DIR'),
                         'test/data/epa/cems/epacems2017/').
            glob('*.zip')
        )
        # Copy the files over to the test-run proto-datastore:
        for file in epacems_files:
            logger.info(f"Faking the download of {file} to {dl_dir}")
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
        sources.extend(['ferc1', 'epacems'])
        years_by_source['ferc1'] = [2017]
        years_by_source['epacems'] = [2017]
        states = ['ID']

    # Download the test year for each dataset that we're downloading...
    datastore.parallel_update(
        sources=sources,
        years_by_source=years_by_source,
        states=states,
        pudl_settings=pudl_settings_fixture,
    )


@pytest.fixture(scope='session')
def ferc1_engine_travis_ci(live_ferc_db,
                           datastore_fixture,
                           pudl_settings_fixture):
    """
    Grab a connection to the FERC Form 1 DB clone.

    If we are using the test database, we initialize it from scratch first.
    If we're using the live database, then we just yield a conneciton to it.
    """
    tables = pc.ferc1_default_tables
    refyear = max(pc.travis_ci_ferc1_years)
    testing = (not live_ferc_db)

    if testing:
        pudl.extract.ferc1.dbf2sqlite(
            tables=tables,
            years=pc.travis_ci_ferc1_years,
            refyear=refyear,
            pudl_settings=pudl_settings_fixture,
            testing=testing)

    # Grab a connection to the approbriate SQLite database, and hand it off.
    engine = pudl.extract.ferc1.connect_db(
        pudl_settings=pudl_settings_fixture, testing=testing)
    yield engine

    if testing:
        # Clean up after ourselves by dropping the test DB tables.
        pudl.extract.ferc1.drop_tables(engine)


@pytest.fixture(scope='session')
def pudl_engine_travis_ci(ferc1_engine_travis_ci,
                          datastore_fixture,
                          pudl_settings_fixture,
                          live_pudl_db, live_ferc_db):
    """
    Grab a connection to the PUDL Database, with a limited amount of data.

    This fixture always initializes the DB from scratch, and only does a small
    subset of the data ETL, for structural testing within Travis CI.
    """
    if not live_pudl_db:
        pudl.init.init_db(ferc1_tables=pc.ferc1_pudl_tables,
                          ferc1_years=pc.travis_ci_ferc1_years,
                          eia923_tables=pc.eia923_pudl_tables,
                          eia923_years=pc.travis_ci_eia923_years,
                          eia860_tables=pc.eia860_pudl_tables,
                          eia860_years=pc.travis_ci_eia860_years,
                          epacems_years=pc.travis_ci_epacems_years,
                          epacems_states=pc.travis_ci_epacems_states,
                          epaipm_tables=pc.epaipm_pudl_tables,
                          pudl_settings=pudl_settings_fixture,
                          pudl_testing=(not live_pudl_db),
                          ferc1_testing=(not live_ferc_db))

    # Grab a connection to the freshly populated PUDL DB, and hand it off.
    pudl_engine = pudl.init.connect_db(
        pudl_settings=pudl_settings_fixture,
        testing=(not live_pudl_db)
    )
    yield pudl_engine

    if not live_pudl_db:
        # Clean up after ourselves by dropping the test DB tables.
        pudl.init.drop_tables(pudl_engine)
