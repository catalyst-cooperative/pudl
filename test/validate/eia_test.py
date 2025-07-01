"""Validate post-ETL EIA 860 data and the associated derived outputs."""

import logging

import pytest
import sqlalchemy as sa

from pudl import validate as pv
from pudl.helpers import get_parquet_table

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "table_name",
    [
        # Yearly EIA923 tables
        "out_eia923__yearly_boiler_fuel",
        "out_eia923__yearly_fuel_receipts_costs",
        "out_eia923__yearly_generation",
        "out_eia923__yearly_generation_fuel_combined",
        # Monthly EIA923 tables
        "out_eia923__monthly_boiler_fuel",
        "out_eia923__monthly_fuel_receipts_costs",
        "out_eia923__monthly_generation",
        "out_eia923__monthly_generation_fuel_combined",
        # Core EIA923 monthly tables
        "core_eia923__monthly_boiler_fuel",
        "core_eia923__monthly_fuel_receipts_costs",
        "core_eia923__monthly_generation",
        "core_eia923__monthly_generation_fuel_nuclear",
        # Unaggregated EIA923 tables
        "out_eia923__boiler_fuel",
        "out_eia923__fuel_receipts_costs",
        "out_eia923__generation",
        "out_eia923__generation_fuel_combined",
        # EIA860 tables
        "core_eia860__assn_boiler_generator",
        "out_eia__yearly_boilers",
        "out_eia__yearly_generators",
        "out_eia860__yearly_ownership",
        "out_eia__yearly_plants",
        "out_eia__yearly_utilities",
        "core_eia860__scd_emissions_control_equipment",
        "core_eia860__assn_yearly_boiler_emissions_control_equipment",
        "out_eia860__yearly_emissions_control_equipment",
        "core_eia860__assn_boiler_stack_flue",
        "core_eia860__assn_boiler_cooling",
    ],
)
def test_no_null_cols_eia(
    live_dbs: bool,
    table_name: str,
    pudl_engine: sa.Engine,  # Required to ensure that the data is available.
) -> None:
    """Verify that output DataFrames have no entirely NULL columns."""
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    df = get_parquet_table(table_name)
    pv.no_null_cols(df, df_name=table_name)
