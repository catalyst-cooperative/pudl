"""Validate post-ETL EIA 860 data and the associated derived outputs."""
import logging
from test.conftest import skip_table_if_null_freq_table

import pytest

from pudl import validate as pv
from pudl.metadata.classes import Resource
from pudl.output.pudltabl import PudlTabl

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
        ("bf_eia923", 1_521_304, 1_521_304, 127_412),
        ("bga_eia860", 141_638, 141_638, 141_638),
        ("boil_eia860", 80_059, 80_059, 80_059),
        ("frc_eia923", 631_975, 258_115, 25_370),
        ("gen_eia923", None, 5_178_892, 433_286),
        ("gens_eia860", 556_578, 556_578, 556_578),
        ("gf_eia923", 2_859_269, 2_859_269, 244_796),
        ("own_eia860", 89_687, 89_687, 89_687),
        ("plants_eia860", 200_177, 200_177, 200_177),
        ("pu_eia860", 199_332, 199_332, 199_332),
        ("utils_eia860", 125_280, 125_280, 125_280),
        ("emissions_control_equipment_eia860", 56_612, 56_612, 56_612),
        ("denorm_emissions_control_equipment_eia860", 56_612, 56_612, 56_612),
        ("boiler_emissions_control_equipment_assn_eia860", 77_707, 77_707, 77_707),
        ("boiler_cooling_assn_eia860", 41_280, 41_280, 41_280),
        ("boiler_stack_flue_assn_eia860", 41_673, 41_673, 41_673),
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
        live_dbs (bool): Whether we're using a live or testing DB.
        raw_rows: The expected original number of rows, without aggregation.
        annual_rows: The expected number of rows when using annual aggregation.
        monthly_rows: The expected number of rows when using monthly aggregation.
        df_name (str): Shorthand name identifying the dataframe, corresponding
            to the name of the function used to pull it from the PudlTabl
            output object.
    """
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    skip_table_if_null_freq_table(table_name=df_name, freq=pudl_out_eia.freq)
    if pudl_out_eia.freq == "AS":
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
        ("bf_eia923", Resource.from_id("boiler_fuel_eia923").schema.primary_key),
        (
            "bga_eia860",
            Resource.from_id("boiler_generator_assn_eia860").schema.primary_key,
        ),
        (
            "boil_eia860",
            Resource.from_id("boilers_eia860").schema.primary_key,
        ),
        (
            "gen_eia923",
            Resource.from_id("generation_eia923").schema.primary_key,
        ),
        (
            "gens_eia860",
            Resource.from_id("generators_eia860").schema.primary_key,
        ),
        (
            "gf_eia923",
            Resource.from_id(
                "denorm_generation_fuel_combined_eia923"
            ).schema.primary_key,
        ),
        (
            "own_eia860",
            Resource.from_id("ownership_eia860").schema.primary_key,
        ),
        (
            "plants_eia860",
            Resource.from_id("plants_eia860").schema.primary_key,
        ),
        (
            "pu_eia860",
            Resource.from_id("denorm_plants_utilities_eia").schema.primary_key,
        ),
        (
            "utils_eia860",
            Resource.from_id("utilities_eia860").schema.primary_key,
        ),
        (
            "denorm_emissions_control_equipment_eia860",
            (
                Resource.from_id(
                    "denorm_emissions_control_equipment_eia860"
                ).schema.primary_key
            ),
        ),
        (
            "emissions_control_equipment_eia860",
            (Resource.from_id("emissions_control_equipment_eia860").schema.primary_key),
        ),
        (
            "boiler_emissions_control_equipment_assn_eia860",
            (
                Resource.from_id(
                    "boiler_emissions_control_equipment_assn_eia860"
                ).schema.primary_key
            ),
        ),
        (
            "boiler_cooling_assn_eia860",
            (Resource.from_id("boiler_cooling_assn_eia860").schema.primary_key),
        ),
        (
            "boiler_stack_flue_assn_eia860",
            (Resource.from_id("boiler_stack_flue_assn_eia860").schema.primary_key),
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
