"""
Validate post-ETL FERC Form 1 data and the associated derived outputs.

These tests depend on a FERC Form 1 specific PudlTabl output object, which is
a parameterized fixture that has session scope.
"""

import logging

import pandas as pd
import pytest
from scipy import stats

import pudl.constants as pc

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("table_name", pc.pudl_tables["ferc1"])
def test_record_id_dupes(pudl_engine, table_name):
    """Verify that the generated ferc1 record_ids are unique."""
    table = pd.read_sql(table_name, pudl_engine)
    n_dupes = table.record_id.duplicated().values.sum()
    logger.info(f"{n_dupes} duplicate record_ids found in {table_name}")

    if n_dupes:
        dupe_ids = (table.record_id[table.record_id.duplicated()].values)
        raise AssertionError(
            f"{n_dupes} duplicate record_ids found in "
            f"{table_name}: {dupe_ids}."
        )


def test_pu_ferc1(pudl_out_ferc1):
    """Test output routines for tables from FERC Form 1."""
    logger.info("Compiling FERC Form 1 plants & utilities table...")
    logger.info(f"{len(pudl_out_ferc1.pu_ferc1())} plant & utility "
                f"records found.")


def test_steam_ferc1_trivial(pudl_out_ferc1):
    """Test output routines for tables from FERC Form 1."""
    logger.info("Compiling FERC Form 1 steam plants table...")
    logger.info(
        f"{len(pudl_out_ferc1.plants_steam_ferc1())} steam plant "
        f"records found."
    )


@pytest.mark.xfail
def test_steam_ferc1_duplicate_years_in_plant_id_ferc1(pudl_out_ferc1):
    """
    Test that we have no duplicate years within any plant_id_ferc1.

    Test to make sure that we don't have any plant_id_ferc1 time series
    which include more than one record from a given year. Fail the test
    if we find such cases (which... we do, as of writing).
    """
    steam_df = pudl_out_ferc1.plants_steam_ferc1()
    year_dupes = (
        steam_df.
        groupby(['plant_id_ferc1', 'report_year'])['utility_id_ferc1'].
        count().
        reset_index().
        rename(columns={'utility_id_ferc1': 'year_dupes'}).
        query('year_dupes>1')
    )
    for dupe in year_dupes.itertuples():
        logger.error(
            f"Found report_year={dupe.report_year} "
            f"{dupe.year_dupes} times in "
            f"plant_id_ferc1={dupe.plant_id_ferc1}"
        )
    if len(year_dupes) != 0:
        raise AssertionError(
            f"Found {len(year_dupes)} duplicate years in FERC1 "
            f"plant ID time series"
        )


@pytest.mark.xfail
def test_steam_ferc1_plant_id_clash(pudl_out_ferc1):
    """
    Test for FERC & PUDL Plant ID consistency.

    Each PUDL Plant ID may contain several FERC Plant IDs, but one FERC Plant
    ID should only ever appear within a single PUDL Plant ID. Test this
    assertion and fail if it is untrue (as... we know it is right now).
    """
    steam_df = pudl_out_ferc1.plants_steam_ferc1()
    bad_plant_ids_ferc1 = (
        steam_df[['plant_id_pudl', 'plant_id_ferc1']].
        drop_duplicates().
        groupby('plant_id_ferc1').
        count().
        rename(columns={'plant_id_pudl': 'pudl_id_count'}).
        query('pudl_id_count>1').
        reset_index().
        plant_id_ferc1.values.tolist()
    )
    if bad_plant_ids_ferc1:
        bad_records = steam_df[steam_df.plant_id_ferc1.
                               isin(bad_plant_ids_ferc1)]
        bad_plant_ids_pudl = bad_records.plant_id_pudl.unique().tolist()
        raise AssertionError(
            f"Found {len(bad_plant_ids_ferc1)} plant_id_ferc1 values "
            f"associated with {len(bad_plant_ids_pudl)} non-unique "
            f"plant_id_pudl values.\nplant_id_ferc1: {bad_plant_ids_ferc1}\n"
            f"plant_id_pudl: {bad_plant_ids_pudl}."
        )


def test_fuel_ferc1(pudl_out_ferc1):
    """Test output routines for tables from FERC Form 1."""
    logger.info("Compiling FERC Form 1 fuel table...")
    logger.info(f"{len(pudl_out_ferc1.fuel_ferc1())} fuel records found")


def test_fbp_ferc1_trivial(pudl_out_ferc1):
    """Test output routines for tables from FERC Form 1."""
    logger.info("Compiling FERC Form 1 Fuel by Plant table...")
    fbp_ferc1 = pudl_out_ferc1.fbp_ferc1()
    logger.info(f"{len(fbp_ferc1)} fuel by plant records found")


def test_fbp_ferc1_missing_mmbtu(pudl_out_ferc1):
    """Test output routines for tables from FERC Form 1."""
    fbp_ferc1 = pudl_out_ferc1.fbp_ferc1()
    missing_mmbtu_pct = (fbp_ferc1.filter(like="fraction_mmbtu").
                         sum(axis=1, skipna=True).
                         pipe(stats.percentileofscore, 0.999999))

    logger.info(
        f"{missing_mmbtu_pct:0.3}% of records missing mmBTU.")
    # No more than 2% of all the records can have their fuel heat
    # content proportions add up to less than 0.999999
    if missing_mmbtu_pct > 2.0:
        raise AssertionError(
            f"Too many records ({missing_mmbtu_pct:.2}%) missing mmBTU.")


def test_fbp_ferc1_missing_cost(pudl_out_ferc1):
    """Check whether FERC 1 fuel costs by plant appear to be complete."""
    fbp_ferc1 = pudl_out_ferc1.fbp_ferc1()
    missing_cost_pct = (fbp_ferc1.filter(like="fraction_cost").
                        sum(axis=1, skipna=True).
                        pipe(stats.percentileofscore, 0.999999))

    logger.info(f"{missing_cost_pct:.2}% of records missing fuel costs.")
    # No more than 1% of all the records can have their fuel
    # cost proportions add up to less than 0.999999
    if missing_cost_pct > 1.0:
        raise AssertionError(
            f"Too many records ({missing_cost_pct:.2}%) missing fuel costs.")


def test_fbp_ferc1_mismatched_fuels(pudl_out_ferc1):
    """Check whether FERC 1 primary fuel by cost and by heat content match."""
    fbp_ferc1 = pudl_out_ferc1.fbp_ferc1()
    # High proportion of primary fuel by cost and by mmbtu should be the same
    mismatched_fuels = len(fbp_ferc1[
        fbp_ferc1.primary_fuel_by_cost != fbp_ferc1.primary_fuel_by_mmbtu
    ]) / len(fbp_ferc1)
    logger.info(f"{mismatched_fuels:.2%} of records "
                f"have mismatched primary fuel types.")
    if mismatched_fuels > 0.05:
        raise AssertionError(
            f"Too many records ({mismatched_fuels:.2%}) have mismatched "
            f"primary fuel types.")


def test_fbp_ferc1_no_dupes(pudl_out_ferc1):
    """Check for duplicate primary keys in FERC 1 fuel by plant."""
    fbp_ferc1 = pudl_out_ferc1.fbp_ferc1()
    # No duplicate [report_year, utility_id_ferc1, plant_name] combinations
    # Should use the mcoe_test.single_records() funciton for this... but I
    # guess we need to create a module just of data validity functions.
    key_cols = ['report_year', 'utility_id_ferc1', 'plant_name']
    len1 = len(fbp_ferc1)
    len2 = len(fbp_ferc1.drop_duplicates(subset=key_cols))
    logger.info(f"{len1-len2} duplicate records found.")
    if len1 != len2:
        raise AssertionError(
            f"{len1-len2} duplicate records found in fbp_ferc1."
        )


def test_fbp_ferc1_gas_price_distribution(pudl_out_ferc1):
    """Check whether FERC 1 gas price distribution appears reasonable."""
    fbp_ferc1 = pudl_out_ferc1.fbp_ferc1()
    # Pure gas plants should have a certain fuel cost ($/mmBTU) distribution.
    pure_gas = fbp_ferc1[fbp_ferc1.gas_fraction_mmbtu >= 0.95]
    gas_cost_per_mmbtu = pure_gas.fuel_cost / pure_gas.fuel_mmbtu
    gas95 = gas_cost_per_mmbtu.quantile(0.95)
    gas50 = gas_cost_per_mmbtu.quantile(0.50)
    gas05 = gas_cost_per_mmbtu.quantile(0.05)
    logger.info(f"natural gas 95% ${gas95:0.2f}/mmBTU")
    logger.info(f"natural gas 50% ${gas50:0.2f}/mmBTU")
    logger.info(f"natural gas 5% ${gas05:0.2f}/mmBTU")
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


def test_fbp_ferc1_coal_price_distribution(pudl_out_ferc1):
    """Check whether FERC 1 coal price distribution appears reasonable."""
    fbp_ferc1 = pudl_out_ferc1.fbp_ferc1()
    # Pure coal plants should have a certain fuel cost ($/mmBTU) distribution.
    pure_coal = fbp_ferc1[fbp_ferc1.coal_fraction_mmbtu >= 0.85]
    coal_cost_per_mmbtu = pure_coal.fuel_cost / pure_coal.fuel_mmbtu

    coal99 = coal_cost_per_mmbtu.quantile(0.99)
    coal50 = coal_cost_per_mmbtu.quantile(0.50)
    coal01 = coal_cost_per_mmbtu.quantile(0.01)
    logger.info(f"coal 99% ${coal99:0.2f}/mmBTU")
    logger.info(f"coal 50% ${coal50:0.2f}/mmBTU")
    logger.info(f"coal 1% ${coal01:0.2f}/mmBTU")
    if (coal99 > 6.0) or (coal01 < 0.5):
        raise AssertionError(
            f"Too many outliers in FERC Form 1 coal prices."
        )
    # These are the 60% and 40% prices from 2004-2017
    if (coal50 > 2.3) or (coal50 < 1.9):
        raise AssertionError(
            f"Median FERC Form 1 coal price is outside expected range."
        )
