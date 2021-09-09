"""Validate post-ETL EIA 860 data and the associated derived outputs."""
import logging

import pytest

from pudl import validate as pv

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "df_name,cols", [
        ("plants_eia860", "all"),
        ("utils_eia860", "all"),
        ("pu_eia860", "all"),
        ("bga_eia860", "all"),
        ("own_eia860", "all"),
        ("gens_eia860", "all"),
        ("gen_eia923", "all"),
        ("gf_eia923", "all"),
        ("bf_eia923", "all"),
        ("frc_eia923", "all"),
    ])
def test_no_null_cols_eia(pudl_out_eia, live_dbs, cols, df_name):
    """Verify that output DataFrames have no entirely NULL columns."""
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    pv.no_null_cols(
        pudl_out_eia.__getattribute__(df_name)(),
        cols=cols, df_name=df_name)


@pytest.mark.parametrize(
    "df_name,raw_rows,monthly_rows,annual_rows", [
        ("utils_eia860", 94_896, 94_896, 94_896),
        ("plants_eia860", 155_045, 155_045, 155_045),
        ("pu_eia860", 139_444, 139_444, 139_444),
        ("own_eia860", 65_264, 65_264, 65_264),
        ("bga_eia860", 105_768, 105_768, 105_768),
        ("gens_eia860", 403_834, 403_834, 403_834),
        ("frc_eia923", 517_078, 213_563, 21_338),
        ("gen_eia923", 510_835, 510_835, 42_884),
        ("bf_eia923", 1_207_976, 1_196_908, 100_866),
        ("gf_eia923", 2_109_040, 2_099_374, 176_619),
    ])
def test_minmax_rows(
    pudl_out_eia,
    live_dbs,
    raw_rows,
    annual_rows,
    monthly_rows,
    df_name
):
    """Verify that output DataFrames don't have too many or too few rows.

    Args:
        pudl_out_eia: A PudlTabl output object.
        live_dbs (bool): Whether we're using a live or testing DB.
        min_rows (int): Minimum number of rows that the dataframe should
            contain when all data is loaded and is output without aggregation.
        df_name (str): Shorthand name identifying the dataframe, corresponding
            to the name of the function used to pull it from the PudlTabl
            output object.

    """
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    if pudl_out_eia.freq == "AS":
        expected_rows = annual_rows
    elif pudl_out_eia.freq == "MS":
        expected_rows = monthly_rows
    else:
        assert pudl_out_eia.freq is None
        expected_rows = raw_rows

    _ = (
        pudl_out_eia.__getattribute__(df_name)()
        .pipe(pv.check_min_rows, expected_rows=expected_rows,
              margin=0.0, df_name=df_name)
        .pipe(pv.check_max_rows, expected_rows=expected_rows,
              margin=0.0, df_name=df_name)
    )


@pytest.mark.parametrize(
    "df_name,unique_subset", [
        ("plants_eia860", ["report_date", "plant_id_eia"]),
        ("utils_eia860", ["report_date", "utility_id_eia"]),
        ("pu_eia860", ["report_date", "plant_id_eia"]),
        ("gens_eia860", ["report_date", "plant_id_eia", "generator_id"]),
        ("bga_eia860", ["report_date", "plant_id_eia", "boiler_id", "generator_id"]),
        ("own_eia860", ["report_date", "plant_id_eia",
         "generator_id", "owner_utility_id_eia"]),
        ("gen_eia923", ["report_date", "plant_id_eia", "generator_id"]),
    ])
def test_unique_rows_eia(pudl_out_eia, live_dbs, unique_subset, df_name):
    """Test whether dataframe has unique records within a subset of columns."""
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    if (pudl_out_eia.freq is None) and (df_name == "gen_eia923"):
        pytest.xfail(reason="RE-RUN ETL DUDE.")
    pv.check_unique_rows(
        pudl_out_eia.__getattribute__(df_name)(),
        subset=unique_subset, df_name=df_name)
