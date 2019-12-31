"""
PyTest based testing of the FERC Database & PUDL data package initializations.

This module also contains fixtures for returning connections to the databases.
These connections can be either to the live databases for post-ETL testing or
to new temporary databases, which are created from scratch and dropped after
the tests have completed. See the --live_ferc1_db and --live_pudl_db command
line options by running pytest --help. If you are using live databases, you
will need to tell PUDL where to find them with --pudl_in=<PUDL_IN>.

"""
import logging
import pathlib

import pytest
import yaml

import pudl
from pudl.convert.epacems_to_parquet import epacems_to_parquet

logger = logging.getLogger(__name__)


@pytest.mark.datapkg
def test_datapkg_bundle(datapkg_bundle):
    """Generate limited packages for testing."""
    pass


@pytest.mark.datapkg
def test_pudl_engine(pudl_engine):
    """Try creating a pudl_engine...."""
    pass


def test_ferc1_etl(ferc1_engine):
    """
    Create a fresh FERC Form 1 SQLite DB and attempt to access it.

    Nothing needs to be in the body of this "test" because the database
    connections are created by the ferc1_engine fixture defined in conftest.py
    """
    pass


def test_epacems_to_parquet(datapkg_bundle,
                            pudl_settings_fixture,
                            data_scope,
                            request):
    """Attempt to convert a small amount of EPA CEMS data to parquet format."""
    clobber = request.config.getoption("--clobber")
    epacems_datapkg_json = pathlib.Path(
        pudl_settings_fixture['datapkg_dir'],
        data_scope['datapkg_bundle_name'],
        'epacems-eia-test',
        "datapackage.json"
    )
    logger.info(f"Loading epacems from {epacems_datapkg_json}")
    epacems_to_parquet(
        datapkg_path=epacems_datapkg_json,
        epacems_years=data_scope['epacems_years'],
        epacems_states=data_scope['epacems_states'],
        out_dir=pathlib.Path(pudl_settings_fixture['parquet_dir'], 'epacems'),
        compression='snappy',
        clobber=clobber
    )


def test_ferc1_lost_data(pudl_settings_fixture, data_scope):
    """
    Check to make sure we aren't missing any old FERC Form 1 tables or fields.

    Exhaustively enumerate all historical sets of FERC Form 1 database tables
    and their constituent fields. Check to make sure that the current database
    definition, based on the given reference year and our compilation of the
    DBF filename to table name mapping from 2015, includes every single table
    and field that appears in the historical FERC Form 1 data.
    """
    refyear = max(data_scope['ferc1_years'])
    current_dbc_map = pudl.extract.ferc1.get_dbc_map(
        year=refyear,
        data_dir=pudl_settings_fixture['data_dir']
    )
    current_tables = list(current_dbc_map.keys())
    logger.info(f"Checking for new, unrecognized FERC1 "
                f"tables in {refyear}.")
    for table in current_tables:
        # First make sure there are new tables in refyear:
        if table not in pudl.constants.ferc1_tbl2dbf:
            raise AssertionError(
                f"New FERC Form 1 table '{table}' in {refyear} "
                f"does not exist in 2015 list of tables"
            )
    # Get all historical table collections...
    dbc_maps = {}
    for yr in data_scope['ferc1_years']:
        logger.info(f"Searching for lost FERC1 tables and fields in {yr}.")
        dbc_maps[yr] = pudl.extract.ferc1.get_dbc_map(
            year=yr,
            data_dir=pudl_settings_fixture['data_dir']
        )
        old_tables = list(dbc_maps[yr].keys())
        for table in old_tables:
            # Check to make sure there aren't any lost archaic tables:
            if table not in current_tables:
                raise AssertionError(
                    f"Long lost FERC1 table: '{table}' found in year {yr}. "
                    f"Refyear: {refyear}"
                )
            for field in dbc_maps[yr][table].values():
                if field not in current_dbc_map[table].values():
                    raise AssertionError(
                        f"Long lost FERC1 field '{field}' found in table "
                        f"'{table}' from year {yr}. "
                        f"Refyear: {refyear}"
                    )


def test_ferc1_solo_etl(datastore_fixture,
                        pudl_settings_fixture,
                        ferc1_engine,
                        live_ferc1_db):
    """Verify that a minimal FERC Form 1 can be loaded without other data."""
    with open(pathlib.Path(
            pathlib.Path(__file__).parent,
            'settings', 'ferc1-solo.yml'), "r") as f:
        datapkg_settings = yaml.safe_load(f)['datapkg_bundle_settings']

    pudl.etl.generate_datapkg_bundle(
        datapkg_settings,
        pudl_settings_fixture,
        datapkg_bundle_name='ferc1-solo',
        clobber=True)
