"""
PyTest based testing of the FERC Database & PUDL data package initializations.

This module also contains fixtures for returning connections to the databases.
These connections can be either to the live databases for post-ETL testing or
to new temporary databases, which are created from scratch and dropped after
the tests have completed. See the --live_ferc_db and --live_pudl_db command
line options by running pytest --help. If you are using live databases, you
will need to tell PUDL where to find them with --pudl_in=<PUDL_IN>.

"""
import logging
import pathlib

import pytest
import yaml

import pudl
import pudl.constants as pc
from pudl.convert.epacems_to_parquet import epacems_to_parquet

logger = logging.getLogger(__name__)


@pytest.mark.data_package
def test_datapkg(datapkg):
    """Generate limited packages for testing."""
    pass


@pytest.mark.data_package
def test_datapkg_to_sqlite(datapkg_to_sqlite):
    """Try flattening the data packages."""
    pass


def test_ferc1_init_db(ferc1_engine):
    """
    Create a fresh FERC Form 1 DB and attempt to access it.

    If we are doing ETL (ingest) testing, then these databases are populated
    anew, in their *_test form.  If we're doing post-ETL (post-ingest) testing
    then we just grab a connection to the existing DB.

    Nothing needs to be in the body of this "test" because the database
    connections are created by the fixtures defined in conftest.py
    """
    pass


def test_epacems_to_parquet(datapkg,
                            pudl_settings_fixture,
                            data_scope,
                            fast_tests):
    """Attempt to convert a small amount of EPA CEMS data to parquet format."""
    logger.info(pathlib.Path(
        pudl_settings_fixture['datapackage_dir'],
        data_scope['pkg_bundle_name'], 'epacems_eia860_923'))
    epacems_to_parquet(
        pkg_dir=pathlib.Path(
            pudl_settings_fixture['datapackage_dir'],
            data_scope['pkg_bundle_name'], 'epacems_eia860_923'),
        epacems_years=data_scope['epacems_years'],
        epacems_states=data_scope['epacems_states'],
        out_dir=pathlib.Path(
            pudl_settings_fixture['parquet_dir'], 'epacems'),
        compression='snappy')


def test_ferc1_lost_data(pudl_settings_fixture, data_scope):
    """
    Check to make sure we aren't missing any old FERC Form 1 tables or fields.

    Exhaustively enumerate all historical sets of FERC Form 1 database tables
    and their constituent fields. Check to make sure that the current database
    definition, based on the given reference year and our compilation of the
    DBF filename to table name mapping from 2015, includes every single table
    and field that appears in the historical FERC Form 1 data.

    Needs live_ferc1_db..?

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


def test_ferc1_unmapped_plants(pudl_settings_fixture, ferc1_engine):
    """
    Test that we can correctly identify unmapped FERC Form 1 DB plants.

    This test replicates :func:`pudl.glue.ferc1_eia.get_unmapped_ferc1_plants`
    but deletes a plant from the raw FERC 1 DB contents, which should then be
    identified as "unmapped."

    """
    years = pudl.constants.working_years['ferc1']
    # First try running et_unmapped_ferc1_plants... as is:
    actually_unmapped_plants = (
        pudl.glue.ferc1_eia.
        get_unmapped_ferc1_plants(pudl_settings_fixture, years)
    )
    if len(actually_unmapped_plants) != 0:
        raise AssertionError(
            f"Expected zero unmapped FERC 1 plants but found "
            f"{len(actually_unmapped_plants)}"
        )
    logger.info("Found 0 unmapped FERC 1 plants, as expected.")
    # Get all the plants in the FERC 1 DB:
    db_plants = (
        pudl.glue.ferc1_eia.
        get_raw_ferc1_plants(pudl_settings_fixture,
                             pc.working_years['ferc1']).
        set_index(["utility_id_ferc1", "plant_name"])
    )
    # Read in the mapped plants... but ditch Xcel's Comanche:
    mapped_plants = (
        pudl.glue.ferc1_eia.get_mapped_ferc1_plants().
        set_index(["utility_id_ferc1", "plant_name"]).
        drop((145, "comanche"))
    )
    new_plants_index = db_plants.index.difference(mapped_plants.index)
    unmapped_plants = db_plants.loc[new_plants_index].reset_index()
    if len(unmapped_plants) != 1:
        raise AssertionError(
            f"Found {len(unmapped_plants)} "
            f"unmapped FERC 1 plants instead of 1."
        )
    logger.info("Found 1 unmapped FERC 1 plant, as expected.")


def test_ferc1_unmapped_utils(pudl_settings_fixture, ferc1_engine):
    """Test that we can identify unmapped FERC 1 utilities."""
    years = pudl.constants.working_years['ferc1']
    # First run the unmapped utility function as is:
    actually_unmapped_utils = (
        pudl.glue.ferc1_eia.
        get_unmapped_ferc1_utils(pudl_settings_fixture, years)
    )
    if len(actually_unmapped_utils) != 0:
        raise AssertionError(
            f"Expected zero unmapped FERC 1 utilities but found "
            f"{len(actually_unmapped_utils)}"
        )
    logger.info("Found 0 unmapped FERC 1 utilities, as expected.")

    # Now do the smae thing... but yanking one of the mapped utils:
    mapped_utilities = (
        pudl.glue.ferc1_eia.
        get_mapped_ferc1_utils().
        set_index("utility_id_ferc1").
        drop(145)  # Drop Xcel Energy Colorado / PSCo.
    )
    # Get all the plants in the FERC 1 DB:
    db_plants = (
        pudl.glue.ferc1_eia.
        get_raw_ferc1_plants(pudl_settings_fixture,
                             pc.working_years['ferc1']).
        set_index(["utility_id_ferc1", "plant_name"])
    )
    # Read in the mapped plants... but ditch Xcel's Comanche:
    mapped_plants = (
        pudl.glue.ferc1_eia.get_mapped_ferc1_plants().
        set_index(["utility_id_ferc1", "plant_name"]).
        drop((145, "comanche"))
    )
    new_plants_index = db_plants.index.difference(mapped_plants.index)
    unmapped_plants = db_plants.loc[new_plants_index].reset_index()
    # Generate a list of all utilities which have unmapped plants:
    # (Since any unmapped utility *must* have unmapped plants)
    utils_with_unmapped_plants = (
        unmapped_plants.loc[:, ["utility_id_ferc1", "utility_name_ferc1"]].
        drop_duplicates("utility_id_ferc1").
        set_index("utility_id_ferc1")
    )
    # Find the indices of all utilities with unmapped plants that do not appear
    # in the list of mapped utilities at all:
    new_utilities_index = (
        utils_with_unmapped_plants.index.
        difference(mapped_utilities.index)
    )
    # Use that index to select only the previously unmapped utilities:
    unmapped_utilities = (
        utils_with_unmapped_plants.
        loc[new_utilities_index].
        reset_index()
    )
    if len(unmapped_utilities) != 1:
        raise AssertionError(
            f"Found {len(unmapped_utilities)} "
            f"unmapped FERC 1 utilities instead of 1."
        )
    logger.info("Found 1 unmapped FERC 1 utility, as expected.")


def test_only_ferc1_pudl_init_db(datastore_fixture,
                                 pudl_settings_fixture,
                                 live_ferc_db):
    """Verify that a minimal FERC Form 1 can be loaded without other data."""
    test_dir = pathlib.Path(__file__).parent
    with open(pathlib.Path(test_dir, 'settings',
                           'settings_datapackage_ferc1_only.yml'),
              "r") as f:
        pkg_settings = yaml.safe_load(f)['pkg_bundle_settings']

    pudl.etl.generate_data_packages(
        pkg_settings,
        pudl_settings_fixture,
        pkg_bundle_name='ferc_only_test',
        clobber=True)
