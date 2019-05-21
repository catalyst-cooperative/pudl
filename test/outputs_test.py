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
from pudl import helpers


###########################################################################
# FERC FORM 1 OUTPUTS
###########################################################################
@pytest.mark.ferc1
@pytest.mark.post_etl
def test_pu_ferc1(pudl_out_ferc1):
    """Test output routines for tables from FERC Form 1."""
    print("\nCompiling FERC Form 1 plants & utilities table...")
    print(f"    pu_ferc1: {len(pudl_out_ferc1.pu_ferc1())} records.")


@pytest.mark.ferc1
@pytest.mark.post_etl
def test_steam_ferc1(pudl_out_ferc1):
    """Test output routines for tables from FERC Form 1."""
    print("\nCompiling FERC Form 1 steam plants table...")
    print(
        f"    plants_steam_ferc1: {len(pudl_out_ferc1.plants_steam_ferc1())} records.")


@pytest.mark.ferc1
@pytest.mark.post_etl
def test_fuel_ferc1(pudl_out_ferc1):
    """Test output routines for tables from FERC Form 1."""
    print("\nCompiling FERC Form 1 fuel table...")
    print(f"    fuel_ferc1: {len(pudl_out_ferc1.fuel_ferc1())} records.")


@pytest.mark.ferc1
@pytest.mark.post_etl
def test_fbp_ferc1(pudl_out_ferc1):
    """Test output routines for tables from FERC Form 1."""
    print("\nCompiling FERC Form 1 Fuel by Plant table...")
    fbp_ferc1 = pudl_out_ferc1.fbp_ferc1()
    print(f"    fbp_ferc1: {len(fbp_ferc1)} records.")

    missing_mmbtu_pct = (fbp_ferc1.filter(like="fraction_mmbtu").
                         sum(axis=1, skipna=True).
                         pipe(stats.percentileofscore, 0.999999))

    print(f"    fbp_ferc1: {missing_mmbtu_pct:.2}% of records missing mmBTU.")
    # No more than 2% of all the records can have their fuel heat
    # content proportions add up to less than 0.999999
    if missing_mmbtu_pct > 2.0:
        raise AssertionError(
            f"Too many records ({missing_mmbtu_pct:.2}%) missing mmBTU."
        )

    missing_cost_pct = (fbp_ferc1.filter(like="fraction_cost").
                        sum(axis=1, skipna=True).
                        pipe(stats.percentileofscore, 0.999999))

    print(
        f"    fbp_ferc1: {missing_cost_pct:.2}% of records missing fuel costs.")
    # No more than 1% of all the records can have their fuel
    # cost proportions add up to less than 0.999999
    if missing_cost_pct > 1.0:
        raise AssertionError(
            f"Too many records ({missing_cost_pct:.2}%) missing fuel costs."
        )

    # High proportion of primary fuel by cost and by mmbtu should be the same
    mismatched_fuels = len(fbp_ferc1[
        fbp_ferc1.primary_fuel_by_cost != fbp_ferc1.primary_fuel_by_mmbtu
    ]) / len(fbp_ferc1)
    print(f"    fbp_ferc1: {mismatched_fuels:.2%} of records "
          f"have mismatched primary fuel types.")
    if mismatched_fuels > 0.05:
        raise AssertionError(
            f"Too many records ({mismatched_fuels:.2%}) have mismatched "
            f"primary fuel types."
        )

    # No duplicate [report_year, utility_id_ferc1, plant_name] combinations
    # Should use the mcoe_test.single_records() funciton for this... but I
    # guess we need to create a module just of data validity functions.
    key_cols = ['report_year', 'utility_id_ferc1', 'plant_name']
    len1 = len(fbp_ferc1)
    len2 = len(fbp_ferc1.drop_duplicates(subset=key_cols))
    if len1 != len2:
        raise AssertionError(
            f"{len1-len2} duplicate records found in fbp_ferc1."
        )

    # Pure gas plants should have a certain fuel cost ($/mmBTU) distribution.
    pure_gas = fbp_ferc1[fbp_ferc1.gas_fraction_mmbtu >= 0.95]
    gas_cost_per_mmbtu = pure_gas.fuel_cost / pure_gas.fuel_mmbtu
    gas95 = gas_cost_per_mmbtu.quantile(0.95)
    gas50 = gas_cost_per_mmbtu.quantile(0.50)
    gas05 = gas_cost_per_mmbtu.quantile(0.05)
    print(f"    fbp_ferc1: natural gas 95% ${gas95:0.2f}/mmBTU")
    print(f"    fbp_ferc1: natural gas 50% ${gas50:0.2f}/mmBTU")
    print(f"    fbp_ferc1: natural gas 5% ${gas05:0.2f}/mmBTU")
    if (gas95 > 20.0) or (gas05 < 2.0):
        raise AssertionError(
            f"Too many outliers in FERC Form 1 natural gas prices."
        )
    # These are the 60% and 40% prices from 2004-2017
    if (gas50 > 6.4) or (gas50 < 4.9):
        raise AssertionError(
            f"Median FERC Form 1 natural gas price is outside "
            f"expected range."
        )

    # Pure coal plants should have a certain fuel cost ($/mmBTU) distribution.
    pure_coal = fbp_ferc1[fbp_ferc1.coal_fraction_mmbtu >= 0.85]
    coal_cost_per_mmbtu = pure_coal.fuel_cost / pure_coal.fuel_mmbtu

    coal99 = coal_cost_per_mmbtu.quantile(0.99)
    coal50 = coal_cost_per_mmbtu.quantile(0.50)
    coal01 = coal_cost_per_mmbtu.quantile(0.01)
    print(f"    fbp_ferc1: coal 99% ${coal99:0.2f}/mmBTU")
    print(f"    fbp_ferc1: coal 50% ${coal50:0.2f}/mmBTU")
    print(f"    fbp_ferc1: coal 1% ${coal01:0.2f}/mmBTU")
    if (coal99 > 6.0) or (coal01 < 0.5):
        raise AssertionError(
            f"Too many outliers in FERC Form 1 coal prices."
        )
    # These are the 60% and 40% prices from 2004-2017
    if (coal50 > 2.3) or (coal50 < 1.9):
        raise AssertionError(
            f"Median FERC Form 1 coal price is outside expected range."
        )


###########################################################################
# EIA 860 OUTPUTS
###########################################################################
@pytest.mark.eia860
@pytest.mark.post_etl
def test_plants_eia860(pudl_out_eia):
    """Sanity checks for EIA 860 plants data."""
    print('\nReading EIA 860 plant data...')
    print(f"    plants_eia860: {len(pudl_out_eia.plants_eia860())} records.")


@pytest.mark.eia860
@pytest.mark.post_etl
def test_utils_eia860(pudl_out_eia):
    """Sanity checks for EIA 860 utility data."""
    print('\nReading EIA 860 utility data...')
    print(f"    utils_eia860: {len(pudl_out_eia.utils_eia860())} records.")


@pytest.mark.eia860
@pytest.mark.post_etl
def test_pu_eia860(pudl_out_eia):
    """Sanity checks for EIA 860 plant & utility data."""
    print('\nReading EIA 860 plant & utility data...')
    print(f"    pu_eia860: {len(pudl_out_eia.pu_eia860())} records.")


@pytest.mark.eia860
@pytest.mark.post_etl
def test_gens_eia860(pudl_out_eia):
    """Sanity checks for EIA 860 generator data."""
    print('\nReading EIA 860 generator data...')
    print(f"    gens_eia860: {len(pudl_out_eia.gens_eia860())} records.")


@pytest.mark.eia860
@pytest.mark.post_etl
def test_bga_eia860(pudl_out_eia):
    """Sanity checks for EIA 860 boiler-generator associations."""
    print('\nReading original EIA 860 boiler-generator associations...')
    print(f"    bga_eia860: {len(pudl_out_eia.bga_eia860())} records.")


@pytest.mark.eia860
@pytest.mark.post_etl
def test_own_eia860(pudl_out_eia):
    """Sanity checks for EIA 860 generator ownership data."""
    print('\nReading EIA 860 generator ownership data...')
    own_out = pudl_out_eia.own_eia860()
    print(f"    own_eia860: {len(own_out)} records found.")

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
    if not max(own_sum['fraction_owned'] < 1.02):
        raise AssertionError(
            "Plants with more than 100% ownership found..."
        )
    # There might be a few generators with incomplete ownership but virtually
    # everything should be pretty fully described. If not, let us know. The
    # 0.1 threshold means 0.1% -- i.e. less than 1 in 1000 is partial.
    if stats.percentileofscore(own_sum.fraction_owned, 0.98) >= 0.1:
        raise AssertionError(
            "Found too many generators with partial ownership data."
        )


###########################################################################
# EIA 923 OUTPUTS
###########################################################################
@pytest.mark.eia923
@pytest.mark.post_etl
def test_frc_eia923(pudl_out_eia):
    """Sanity checks for EIA 923 Fuel Recepts and Costs output."""
    print('\nReading EIA 923 Fuel Receipts and Costs data...')
    print(f"    frc_eia923: {len(pudl_out_eia.frc_eia923())} records.")


@pytest.mark.eia923
@pytest.mark.post_etl
def test_gf_eia923(pudl_out_eia):
    """Sanity checks for EIA 923 Generator Fuel output."""
    print('\nReading EIA 923 Generator Fuel data...')
    print(f"    gf_eia923: {len(pudl_out_eia.gf_eia923())} records.")


@pytest.mark.eia923
@pytest.mark.post_etl
def test_bf_eia923(pudl_out_eia):
    """Sanity checks for EIA 923 Boiler Fuel output."""
    print('\nReading EIA 923 Boiler Fuel data...')
    print(f"    bf_eia923: {len(pudl_out_eia.bf_eia923())} records.")


@pytest.mark.eia923
@pytest.mark.post_etl
def test_gen_eia923(pudl_out_eia):
    """Sanity checks for EIA 923 Generation output."""
    print('\nReading EIA 923 Generation data...')
    print(f"    gen_eia923: {len(pudl_out_eia.gen_eia923())} records.")
