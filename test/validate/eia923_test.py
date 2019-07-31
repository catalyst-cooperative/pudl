"""Validate post-ETL EIA 923 data and associated derived outputs."""

import logging
from decimal import Decimal

import pytest

logger = logging.getLogger(__name__)


@pytest.mark.eia923
@pytest.mark.post_etl
def test_frc_eia923(pudl_out_eia,
                    max_unit_fuel_cost=35,
                    max_unit_heat_content=32):
    """Sanity checks for EIA 923 Fuel Recepts and Costs output."""
    logger.info("Reading EIA 923 Fuel Receipts and Costs data...")

    frc_eia923 = pudl_out_eia.frc_eia923()

    fuel_unit_cost_outlier = len(
        frc_eia923.loc[(frc_eia923.fuel_cost_per_mmbtu > max_unit_fuel_cost) |
                       (frc_eia923.fuel_cost_per_mmbtu < 0)]
    )

    decimal = Decimal((fuel_unit_cost_outlier / (len(frc_eia923))) * 100)
    proportion = round(decimal, 2)

    logger.info(
        f"{fuel_unit_cost_outlier} records, {proportion}% of the total, "
        f"have fuel unit costs ($/mmbtu) less than 0 or greater than "
        f"{max_unit_fuel_cost}."
    )

    heat_content_outlier = len(
        frc_eia923.loc[
            (frc_eia923.heat_content_mmbtu_per_unit > max_unit_heat_content) |
            (frc_eia923.heat_content_mmbtu_per_unit < 0)
        ]
    )
    decimal = Decimal((heat_content_outlier / (len(frc_eia923))) * 100)
    proportion = round(decimal, 2)

    logger.info(
        f"{heat_content_outlier} records, {proportion}% of the total, have "
        f"fuel heat content (mmbtu/unit) less than 0 or greater than "
        f"{max_unit_heat_content}."
    )


@pytest.mark.eia923
@pytest.mark.post_etl
def test_gf_eia923(pudl_out_eia):
    """Sanity checks for EIA 923 Generator Fuel output."""
    logger.info("Reading EIA 923 Generator Fuel data...")
    logger.info(f"Successfully pulled{len(pudl_out_eia.gf_eia923())} records.")


@pytest.mark.eia923
@pytest.mark.post_etl
def test_bf_eia923(pudl_out_eia):
    """Sanity checks for EIA 923 Boiler Fuel output."""
    logger.info("Reading EIA 923 Boiler Fuel data...")
    logger.info(
        f"Successfully pulled {len(pudl_out_eia.bf_eia923())} records.")


@pytest.mark.eia923
@pytest.mark.post_etl
def test_gen_eia923(pudl_out_eia):
    """Sanity checks for EIA 923 Generation output."""
    logger.info("Reading EIA 923 Generation data...")
    logger.info(
        f"Successfully pulled {len(pudl_out_eia.gen_eia923())} records.")
