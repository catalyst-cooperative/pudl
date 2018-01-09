"""
Test output module functionality.

This set of tests exercises the functions within the output module, to ensure
that changes to the code haven't broken our standard outputs. These tests
depend on there being an already initialized NON TEST database, but they only
read data from the DB, so that's safe.  To use these tests you need to have
initialized the DB successfully.
"""
import pytest
import itertools
import pandas as pd
from pudl import outputs
from pudl import constants as pc

start_date_eia923 = pd.to_datetime(
    str(min(pc.working_years['eia923'])))
end_date_eia923 = pd.to_datetime(
    '{}-12-31'.format(max(pc.working_years['eia923'])))


@pytest.mark.ferc1
@pytest.mark.tabular_output
@pytest.mark.post_etl
def test_ferc1_output(live_pudl_db):
    """Test output routines for tables from FERC Form 1."""
    testing = (not live_pudl_db)
    print("\nCompiling FERC Form 1 Plants & Utilities table...")
    pu_ferc = outputs.plants_utils_ferc1(testing=testing)
    print("    {} records found.".format(len(pu_ferc)))

    print("Compiling FERC Form 1 Fuel table...")
    fuel_out = outputs.fuel_ferc1(testing=testing)
    print("    {} records found.".format(len(fuel_out)))

    print("Compiling FERC Form 1 Steam Plants table...")
    steam_out = outputs.plants_steam_ferc1(testing=testing)
    print("    {} records found.".format(len(steam_out)))


@pytest.mark.tabular_output
@pytest.mark.eia923
@pytest.mark.eia860
@pytest.mark.post_etl
@pytest.mark.parametrize('start_date, end_date', [
    (None, None),
    (start_date_eia923, end_date_eia923)
])
def test_eia_output(live_pudl_db, start_date, end_date):
    """Test output routines for tables from across EIA data sources."""
    testing = (not live_pudl_db)
    print('\nCompiling EIA plant & utility entities...')
    print("start_date={}, end_date={}".format(start_date, end_date))
    pu_eia = outputs.plants_utils_eia(testing=testing,
                                      start_date=start_date,
                                      end_date=end_date)
    print("    pu_eia: {} records found.".format(len(pu_eia)))


@pytest.mark.tabular_output
@pytest.mark.eia923
@pytest.mark.post_etl
@pytest.mark.parametrize('freq, start_date, end_date', [
    (None, start_date_eia923, end_date_eia923),
    ('MS', start_date_eia923, end_date_eia923),
    ('AS', start_date_eia923, end_date_eia923),
    (None, None, None),
    ('MS', None, None),
    ('AS', None, None),
])
def test_eia923_output(live_pudl_db, freq, start_date, end_date):
    """Test output routines for tables from EIA Form 923."""
    testing = (not live_pudl_db)
    print('\nCompiling EIA 923 tabular data...')
    print("freq={}, start_date={}, end_date={}".
          format(freq, start_date, end_date))

    frc_out = outputs.fuel_receipts_costs_eia923(
        testing=testing, freq=freq, start_date=start_date, end_date=end_date)
    print("    frc_eia923: {} records found.".format(len(frc_out)))

    gf_out = outputs.generation_fuel_eia923(
        testing=testing, freq=freq, start_date=start_date, end_date=end_date)
    print("    gf_eia923: {} records found.".format(len(gf_out)))

    bf_out = outputs.boiler_fuel_eia923(
        testing=testing, freq=freq, start_date=start_date, end_date=end_date)
    print("    bf_eia923: {} records found.".format(len(bf_out)))

    g_out = outputs.generation_eia923(
        testing=testing, freq=freq, start_date=start_date, end_date=end_date)
    print("    gen_eia923: {} records found.".format(len(g_out)))


@pytest.mark.tabular_output
@pytest.mark.eia860
@pytest.mark.post_etl
@pytest.mark.parametrize('start_date, end_date', [
    (None, None),
    (start_date_eia923, end_date_eia923)
])
def test_eia860_output(live_pudl_db, start_date, end_date):
    """Test output routines for tables from EIA Form 860."""
    testing = (not live_pudl_db)
    print('\nCompiling EIA 860 tabular data...')
    print("start_date={}, end_date={}".format(start_date, end_date))

    utils_out = outputs.utilities_eia860(
        testing=testing, start_date=start_date, end_date=end_date)
    print("    utils_eia860: {} records found.".format(len(utils_out)))

    plants_out = outputs.plants_eia860(
        testing=testing, start_date=start_date, end_date=end_date)
    print("    plants_eia860: {} records found.".format(len(plants_out)))

    gens_out = outputs.generators_eia860(
        testing=testing, start_date=start_date, end_date=end_date)
    print("    gens_eia860: {} records found.".format(len(gens_out)))

    own_out = outputs.ownership_eia860(
        testing=testing, start_date=start_date, end_date=end_date)
    print("    own_eia860: {} records found.".format(len(own_out)))

    bga_out = outputs.boiler_generator_assn_eia860(
        testing=testing, start_date=start_date, end_date=end_date)
    print("    bga_eia860: {} records found.".format(len(bga_out)))
