"""Validate post-ETL EIA 860 data and the associated derived outputs."""

import logging

import pytest

from pudl import validate as pv
from pudl.metadata.classes import Resource
from test.conftest import skip_table_if_null_freq_table

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "df_name,cols",
    [
        ("bf_eia923", "all"),
        ("bga_eia860", "all"),
        ("boil_eia860", "all"),
        ("frc_eia923", "all"),
        ("gen_eia923", "all"),
        ("gens_eia860", "all"),
        ("gf_eia923", "all"),
        ("own_eia860", "all"),
        ("plants_eia860", "all"),
        ("pu_eia860", "all"),
        ("utils_eia860", "all"),
        ("emissions_control_equipment_eia860", "all"),
        ("boiler_emissions_control_equipment_assn_eia860", "all"),
        ("denorm_emissions_control_equipment_eia860", "all"),
        ("boiler_stack_flue_assn_eia860", "all"),
        ("boiler_cooling_assn_eia860", "all"),
    ],
)
def test_no_null_cols_eia(pudl_out_eia, live_dbs, cols, df_name):
    """Verify that output DataFrames have no entirely NULL columns."""
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    skip_table_if_null_freq_table(table_name=df_name, freq=pudl_out_eia.freq)
    pv.no_null_cols(
        pudl_out_eia.__getattribute__(df_name)(), cols=cols, df_name=df_name
    )


@pytest.mark.parametrize(
    "df_name,unique_subset",
    [
        (
            "bf_eia923",
            Resource.from_id("core_eia923__monthly_boiler_fuel").schema.primary_key,
        ),
        (
            "bga_eia860",
            Resource.from_id("core_eia860__assn_boiler_generator").schema.primary_key,
        ),
        (
            "boil_eia860",
            Resource.from_id("core_eia860__scd_boilers").schema.primary_key,
        ),
        (
            "gen_eia923",
            Resource.from_id("core_eia923__monthly_generation").schema.primary_key,
        ),
        (
            "gens_eia860",
            Resource.from_id("core_eia860__scd_generators").schema.primary_key,
        ),
        (
            "gf_eia923",
            Resource.from_id("out_eia923__generation_fuel_combined").schema.primary_key,
        ),
        (
            "own_eia860",
            Resource.from_id("core_eia860__scd_ownership").schema.primary_key,
        ),
        (
            "plants_eia860",
            Resource.from_id("core_eia860__scd_plants").schema.primary_key,
        ),
        (
            "pu_eia860",
            Resource.from_id("_out_eia__plants_utilities").schema.primary_key,
        ),
        (
            "utils_eia860",
            Resource.from_id("core_eia860__scd_utilities").schema.primary_key,
        ),
        (
            "denorm_emissions_control_equipment_eia860",
            (
                Resource.from_id(
                    "out_eia860__yearly_emissions_control_equipment"
                ).schema.primary_key
            ),
        ),
        (
            "emissions_control_equipment_eia860",
            (
                Resource.from_id(
                    "core_eia860__scd_emissions_control_equipment"
                ).schema.primary_key
            ),
        ),
        (
            "boiler_emissions_control_equipment_assn_eia860",
            (
                Resource.from_id(
                    "core_eia860__assn_yearly_boiler_emissions_control_equipment"
                ).schema.primary_key
            ),
        ),
        (
            "boiler_cooling_assn_eia860",
            (Resource.from_id("core_eia860__assn_boiler_cooling").schema.primary_key),
        ),
        (
            "boiler_stack_flue_assn_eia860",
            (
                Resource.from_id(
                    "core_eia860__assn_boiler_stack_flue"
                ).schema.primary_key
            ),
        ),
    ],
)
def test_unique_rows_eia(pudl_out_eia, live_dbs, unique_subset, df_name):
    """Test whether dataframe has unique records within a subset of columns."""
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    skip_table_if_null_freq_table(table_name=df_name, freq=pudl_out_eia.freq)
    pv.check_unique_rows(
        pudl_out_eia.__getattribute__(df_name)(), subset=unique_subset, df_name=df_name
    )
