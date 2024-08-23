"""Validate post-ETL EIA 860 data and the associated derived outputs."""

import logging

import pytest

from pudl import validate as pv
from pudl.metadata.classes import Resource
from pudl.output.pudltabl import PudlTabl
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
    "df_name,raw_rows,monthly_rows,annual_rows",
    [
        ("bf_eia923", 1_661_460, 1_661_460, 135_980),
        ("bga_eia860", 153_487, 153_487, 153_487),
        ("boil_eia860", 89_050, 89_050, 89_050),
        ("frc_eia923", 676_851, 276_108, 26_709),
        ("gen_eia923", None, 5_494_932, 459_711),
        ("gens_eia860", 591_256, 591_256, 591_256),
        ("gf_eia923", 3_074_806, 3_074_806, 260_842),
        ("own_eia860", 95_104, 95_104, 95_104),
        ("plants_eia860", 216_206, 216_206, 216_206),
        ("pu_eia860", 215_288, 215_288, 215_288),
        ("utils_eia860", 147_922, 147_922, 147_922),
        ("emissions_control_equipment_eia860", 62_102, 62_102, 62_102),
        ("denorm_emissions_control_equipment_eia860", 62_102, 62_102, 62_102),
        ("boiler_emissions_control_equipment_assn_eia860", 83_977, 83_977, 83_977),
        ("boiler_cooling_assn_eia860", 44_185, 44_185, 44_185),
        ("boiler_stack_flue_assn_eia860", 44_579, 44_579, 44_579),
    ],
)
def test_minmax_rows(
    pudl_out_eia: PudlTabl,
    live_dbs: bool,
    raw_rows: int | None,
    annual_rows: int,
    monthly_rows: int,
    df_name: str,
):
    """Verify that output DataFrames don't have too many or too few rows.

    Args:
        pudl_out_eia: A PudlTabl output object.
        live_dbs: Whether we're using a live or testing DB.
        raw_rows: The expected original number of rows, without aggregation.
        annual_rows: The expected number of rows when using annual aggregation.
        monthly_rows: The expected number of rows when using monthly aggregation.
        df_name: Shorthand name identifying the dataframe, corresponding
            to the name of the function used to pull it from the PudlTabl
            output object.
    """
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    skip_table_if_null_freq_table(table_name=df_name, freq=pudl_out_eia.freq)
    if pudl_out_eia.freq == "YS":
        expected_rows = annual_rows
    elif pudl_out_eia.freq == "MS":
        expected_rows = monthly_rows
    else:
        assert pudl_out_eia.freq is None
        expected_rows = raw_rows

    _ = (
        pudl_out_eia.__getattribute__(df_name)()
        .pipe(
            pv.check_min_rows, expected_rows=expected_rows, margin=0.0, df_name=df_name
        )
        .pipe(
            pv.check_max_rows, expected_rows=expected_rows, margin=0.0, df_name=df_name
        )
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
