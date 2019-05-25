"""
Validate post-ETL FERC Form 1 data and the associated derived outputs.

These tests depend on a FERC Form 1 specific PudlTabl output object, which is
a parameterized fixture that has session scope.
"""

import logging
import pytest
from scipy import stats

logger = logging.getLogger(__name__)


@pytest.mark.ferc1
@pytest.mark.post_etl
def test_pu_ferc1(pudl_out_ferc1):
    """Test output routines for tables from FERC Form 1."""
    logger.info("Compiling FERC Form 1 plants & utilities table...")
    logger.info(f"{len(pudl_out_ferc1.pu_ferc1())} plant & utility "
                f"records found.")


@pytest.mark.ferc1
@pytest.mark.post_etl
def test_steam_ferc1(pudl_out_ferc1):
    """Test output routines for tables from FERC Form 1."""
    logger.info("Compiling FERC Form 1 steam plants table...")
    logger.info(f"{len(pudl_out_ferc1.plants_steam_ferc1())} "
                f"steam plant records found.")


@pytest.mark.ferc1
@pytest.mark.post_etl
def test_fuel_ferc1(pudl_out_ferc1):
    """Test output routines for tables from FERC Form 1."""
    logger.info("Compiling FERC Form 1 fuel table...")
    logger.info(f"{len(pudl_out_ferc1.fuel_ferc1())} fuel records found")


@pytest.mark.ferc1
@pytest.mark.post_etl
def test_fbp_ferc1_missing_mmbtu(pudl_out_ferc1):
    """Test output routines for tables from FERC Form 1."""
    logger.info("Compiling FERC Form 1 Fuel by Plant table...")
    fbp_ferc1 = pudl_out_ferc1.fbp_ferc1()
    logger.info(f"{len(fbp_ferc1)} fuel by plant records found")

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


@pytest.mark.ferc1
@pytest.mark.post_etl
def test_fbp_ferc1_missing_cost(pudl_out_ferc1):
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


@pytest.mark.ferc1
@pytest.mark.post_etl
def test_fbp_ferc1_mismatched_fuels(pudl_out_ferc1):
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


@pytest.mark.ferc1
@pytest.mark.post_etl
def test_fbp_ferc1_no_dupes(pudl_out_ferc1):
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


@pytest.mark.ferc1
@pytest.mark.post_etl
def test_fbp_ferc1_gas_price_distribution(pudl_out_ferc1):
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


@pytest.mark.ferc1
@pytest.mark.post_etl
def test_fbp_ferc1_coal_price_distribution(pudl_out_ferc1):
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
