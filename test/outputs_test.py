"""
Test output module functionality.

This set of tests exercises the functions within the output module, to ensure
that changes to the code haven't broken our standard outputs. These tests
depend on there being an already initialized NON TEST database, but they only
read data from the DB, so that's safe.  To use these tests you need to have
initialized the DB successfully.
"""
import pytest
import pandas as pd
from scipy import stats
import pudl.output.ferc1
import pudl.output.eia860
import pudl.output.eia923
from pudl import constants as pc
from pudl import helpers

START_DATE_EIA923 = pd.to_datetime(
    str(min(pc.working_years['eia923'])))
END_DATE_EIA923 = pd.to_datetime(
    '{}-12-31'.format(max(pc.working_years['eia923'])))


@pytest.mark.ferc1
@pytest.mark.tabular_output
@pytest.mark.post_etl
def test_ferc1_output(live_pudl_db):
    """Test output routines for tables from FERC Form 1."""
    testing = (not live_pudl_db)
    print("\nCompiling FERC Form 1 Plants & Utilities table...")
    pu_ferc = pudl.output.ferc1.plants_utils_ferc1(testing=testing)
    print("    {} records found.".format(len(pu_ferc)))

    print("Compiling FERC Form 1 Fuel table...")
    fuel_out = pudl.output.ferc1.fuel_ferc1(testing=testing)
    print("    {} records found.".format(len(fuel_out)))

    print("Compiling FERC Form 1 Steam Plants table...")
    steam_out = pudl.output.ferc1.plants_steam_ferc1(testing=testing)
    print("    {} records found.".format(len(steam_out)))


@pytest.mark.tabular_output
@pytest.mark.eia923
@pytest.mark.eia860
@pytest.mark.post_etl
@pytest.mark.parametrize('start_date, end_date', [
    (None, None),
    (START_DATE_EIA923, END_DATE_EIA923)
])
def test_eia_output(live_pudl_db, start_date, end_date):
    """Test output routines for tables from across EIA data sources."""
    testing = (not live_pudl_db)
    print('\nCompiling EIA plant & utility entities...')
    print("start_date={}, end_date={}".format(start_date, end_date))
    pu_eia = pudl.output.eia860.plants_utils_eia860(testing=testing,
                                                    start_date=start_date,
                                                    end_date=end_date)
    print("    pu_eia: {} records found.".format(len(pu_eia)))


@pytest.mark.tabular_output
@pytest.mark.eia923
@pytest.mark.post_etl
@pytest.mark.parametrize('freq, start_date, end_date', [
    (None, START_DATE_EIA923, END_DATE_EIA923),
    ('MS', START_DATE_EIA923, END_DATE_EIA923),
    ('AS', START_DATE_EIA923, END_DATE_EIA923),
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

    frc_out = pudl.output.eia923.fuel_receipts_costs_eia923(
        testing=testing, freq=freq, start_date=start_date, end_date=end_date)
    print("    frc_eia923: {} records found.".format(len(frc_out)))

    gf_out = pudl.output.eia923.generation_fuel_eia923(
        testing=testing, freq=freq, start_date=start_date, end_date=end_date)
    print("    gf_eia923: {} records found.".format(len(gf_out)))

    bf_out = pudl.output.eia923.boiler_fuel_eia923(
        testing=testing, freq=freq, start_date=start_date, end_date=end_date)
    print("    bf_eia923: {} records found.".format(len(bf_out)))

    g_out = pudl.output.eia923.generation_eia923(
        testing=testing, freq=freq, start_date=start_date, end_date=end_date)
    print("    gen_eia923: {} records found.".format(len(g_out)))


@pytest.mark.tabular_output
@pytest.mark.eia860
@pytest.mark.post_etl
@pytest.mark.parametrize('start_date, end_date', [
    (None, None),
    (START_DATE_EIA923, END_DATE_EIA923)
])
def test_eia860_output(live_pudl_db, start_date, end_date):
    """Test output routines for tables from EIA Form 860."""
    testing = (not live_pudl_db)
    print('\nCompiling EIA 860 tabular data...')
    print("start_date={}, end_date={}".format(start_date, end_date))

    utils_out = pudl.output.eia860.utilities_eia860(
        testing=testing, start_date=start_date, end_date=end_date)
    print("    utils_eia860: {} records found.".format(len(utils_out)))

    plants_out = pudl.output.eia860.plants_eia860(
        testing=testing, start_date=start_date, end_date=end_date)
    print("    plants_eia860: {} records found.".format(len(plants_out)))

    gens_out = pudl.output.eia860.generators_eia860(
        testing=testing, start_date=start_date, end_date=end_date)
    print("    gens_eia860: {} records found.".format(len(gens_out)))

    own_out = pudl.output.eia860.ownership_eia860(
        testing=testing, start_date=start_date, end_date=end_date)
    print("    own_eia860: {} records found.".format(len(own_out)))

    # Verify that the reported ownership fractions add up to something very
    # close to 1.0 (i.e. that the full ownership of each generator is
    # specified by the EIA860 data)
    own_gb = own_out.groupby(['report_date', 'plant_id_eia', 'generator_id'])
    own_sum = own_gb['fraction_owned'].agg(helpers.sum_na).reset_index()
    print("    own_eia860: {} generator-years have no ownership data.".
          format(len(own_sum[own_sum.fraction_owned.isnull()])))
    own_sum = own_sum.dropna()
    print("    own_eia860: {} generator-years have incomplete ownership data.".
          format(len(own_sum[own_sum.fraction_owned < 0.98])))
    assert max(own_sum['fraction_owned'] < 1.02)
    # There might be a few generators with incomplete ownership but virtually
    # everything should be pretty fully described. If not, let us know. The
    # 0.1 threshold means 0.1% -- i.e. less than 1 in 1000 is partial.
    assert stats.percentileofscore(own_sum.fraction_owned, 0.98) < 0.1, \
        "Found too many generators with partial ownership data."

    bga_out = pudl.output.eia860.boiler_generator_assn_eia860(
        testing=testing, start_date=start_date, end_date=end_date)
    print("    bga_eia860: {} records found.".format(len(bga_out)))
