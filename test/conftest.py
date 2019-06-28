"""PyTest configuration module. Defines useful fixtures, command line args."""

import logging
import pytest
import pandas as pd
import pudl
from pudl.output.pudltabl import PudlTabl
from pudl import constants as pc

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
def ferc1_engine(live_ferc_db):
    """
    Grab a connection to the FERC Form 1 DB clone.

    If we are using the test database, we initialize it from scratch first.
    If we're using the live database, then we just yield a conneciton to it.
    """
    # Avoid the 6.5 GB of binary garbage in these tables for testing....
    tables = [
        tbl for tbl in pc.ferc1_tbl2dbf if tbl not in pc.ferc1_huge_tables
    ]
    refyear = max(pc.working_years['ferc1'])
    testing = not live_ferc_db

    if testing:
        pudl.extract.ferc1.dbf2sqlite(
            tables=tables,
            years=pc.data_years['ferc1'],
            refyear=refyear,
            testing=testing)

    # Grab a connection to the freshly populated database, and hand it off.
    engine = pudl.extract.ferc1.connect_db(testing=testing)
    yield engine

    if testing:
        # Clean up after ourselves by dropping the test DB tables.
        pudl.extract.ferc1.drop_tables(engine)


@pytest.fixture(scope='session')
def pudl_engine(ferc1_engine, live_pudl_db, live_ferc_db):
    """
    Grab a connection to the PUDL Database.

    If we are using the test database, we initialize the PUDL DB from scratch.
    If we're using the live database, then we just make a conneciton to it.
    """
    if not live_pudl_db:
        pudl.init.init_db(ferc1_tables=pc.ferc1_pudl_tables,
                          ferc1_years=pc.working_years['ferc1'],
                          eia923_tables=pc.eia923_pudl_tables,
                          eia923_years=pc.working_years['eia923'],
                          eia860_tables=pc.eia860_pudl_tables,
                          eia860_years=pc.working_years['eia860'],
                          # Full EPA CEMS ingest takes 8 hours, so using an
                          # abbreviated list here
                          epacems_years=pc.travis_ci_epacems_years,
                          epacems_states=pc.travis_ci_epacems_states,
                          epaipm_tables=pc.epaipm_pudl_tables,
                          pudl_testing=True,
                          ferc1_testing=(not live_ferc_db))

        # Grab a connection to the freshly populated PUDL DB, and hand it off.
        pudl_engine = pudl.init.connect_db(testing=True)
        yield pudl_engine

        # Clean up after ourselves by dropping the test DB tables.
        pudl.init.drop_tables(pudl_engine)
    else:
        logger.info("Connecting to the live PUDL database.")
        yield pudl.init.connect_db(testing=False)


@pytest.fixture(scope='session')
def ferc1_engine_travis_ci(live_ferc_db):
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
            testing=testing)

    # Grab a connection to the approbriate SQLite database, and hand it off.
    engine = pudl.extract.ferc1.connect_db(testing=testing)
    yield engine

    if testing:
        # Clean up after ourselves by dropping the test DB tables.
        pudl.extract.ferc1.drop_tables(engine)


@pytest.fixture(scope='session')
def pudl_engine_travis_ci(ferc1_engine_travis_ci, live_pudl_db, live_ferc_db):
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
                          pudl_testing=True,
                          ferc1_testing=(not live_ferc_db))

        # Grab a connection to the freshly populated PUDL DB, and hand it off.
        pudl_engine = pudl.init.connect_db(testing=True)
        yield pudl_engine

        # Clean up after ourselves by dropping the test DB tables.
        pudl.init.drop_tables(pudl_engine)

    else:
        logger.info("Connecting to the live PUDL database.")
        yield pudl.init.connect_db(testing=False)
