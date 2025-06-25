"""PyTest cases related to generating dervied outputs."""

import logging

import pandas as pd
import pytest
import sqlalchemy as sa

import pudl.validate as pv
from pudl.helpers import get_parquet_table
from pudl.metadata.classes import Resource

logger = logging.getLogger(__name__)


def nuke_gen_fraction(df: pd.DataFrame) -> float:
    """Calculate the nuclear fraction of net generation."""
    total_gen = df.net_generation_mwh.sum()
    nuke_gen = df[df.fuel_type_code_pudl == "nuclear"].net_generation_mwh.sum()
    return nuke_gen / total_gen


@pytest.mark.parametrize(
    "table_name,expected_nuke_fraction,tolerance",
    [
        ("out_eia923__monthly_generation_fuel_combined", 0.2, 0.02),
        ("out_eia__monthly_generators", 0.2, 0.02),
    ],
)
def test_nuclear_fraction(
    table_name: str,
    expected_nuke_fraction: float,
    tolerance: float,
    pudl_engine: sa.Engine,  # Required to ensure that the data is available.
):
    """Ensure that overall nuclear generation fractions are as expected."""
    df = get_parquet_table(
        table_name, columns=["fuel_type_code_pudl", "net_generation_mwh"]
    )
    actual_nuke_fraction = nuke_gen_fraction(df)
    assert abs(actual_nuke_fraction - expected_nuke_fraction) <= tolerance


@pytest.mark.parametrize(
    "test_table,mult",
    [
        ("core_eia860__assn_boiler_generator", 1 / 1),
        ("out_eia860__yearly_ownership", 1 / 1),
        ("out_eia__yearly_plants", 1 / 1),
        ("out_eia__yearly_boilers", 1 / 1),
        ("out_eia__yearly_utilities", 1 / 1),
        ("out_eia923__monthly_boiler_fuel", 12 / 1),
        ("out_eia923__monthly_fuel_receipts_costs", 12 / 1),
        ("out_eia923__monthly_generation", 12 / 1),
        ("out_eia923__monthly_generation_fuel_by_generator_energy_source", 12 / 1),
        ("out_eia923__monthly_generation_fuel_by_generator", 12 / 1),
        ("out_eia923__monthly_generation_fuel_combined", 12 / 1),
    ],
)
def test_eia_outputs(
    test_table: str,
    mult: int,
    pudl_engine: sa.Engine,  # Required to ensure that the data is available.
):
    """Check EIA output functions and date frequencies of output dataframes."""
    yearly_gens_name = "out_eia__yearly_generators"

    # Get columns needed for date frequency checking
    # Include data_maturity if it exists in schema for proper incremental_ytd filtering
    def get_date_freq_columns(table_name: str) -> list[str]:
        """Get columns needed for date frequency checking."""
        columns = ["report_date"]
        resource = Resource.from_id(table_name)
        if "data_maturity" in resource.get_field_names():
            columns.append("data_maturity")
        return columns

    yearly_gens_df = get_parquet_table(
        yearly_gens_name, columns=get_date_freq_columns(yearly_gens_name)
    )
    logger.info(f"Reading {test_table} table.")
    test_df = get_parquet_table(test_table, columns=get_date_freq_columns(test_table))
    logger.info(f"Found {len(test_df)} rows in {test_table}")
    logger.info(f"Checking {test_table} date frequency relative to {yearly_gens_name}.")
    pv.check_date_freq(yearly_gens_df, test_df, mult)
