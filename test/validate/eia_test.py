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
        ("bf_eia923", 1_559_257, 1_559_257, 127_412),
        ("bga_eia860", 141_652, 141_652, 141_652),
        ("boil_eia860", 83_356, 83_356, 83_356),
        ("frc_eia923", 639_647, 261_583, 25_370),
        ("gen_eia923", None, 5_179_377, 433_332),
        ("gens_eia860", 556_948, 556_948, 556_948),
        ("gf_eia923", 2_879_884, 2_879_884, 244_795),
        ("own_eia860", 89_741, 89_741, 89_741),
        ("plants_eia860", 200_514, 200_514, 200_514),
        ("pu_eia860", 199_635, 199_635, 199_635),
        ("utils_eia860", 139_883, 139_883, 139_883),
        ("emissions_control_equipment_eia860", 56_616, 56_616, 56_616),
        ("denorm_emissions_control_equipment_eia860", 56_616, 56_616, 56_616),
        ("boiler_emissions_control_equipment_assn_eia860", 77_705, 77_705, 77_705),
        ("boiler_cooling_assn_eia860", 41_282, 41_282, 41_282),
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
