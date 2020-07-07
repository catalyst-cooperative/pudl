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
def test_no_null_cols_eia(pudl_out_eia, live_pudl_db, cols, df_name):
    """Verify that output DataFrames have no entirely NULL columns."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    pv.no_null_cols(
        pudl_out_eia.__getattribute__(df_name)(),
        cols=cols, df_name=df_name)


@pytest.mark.parametrize(
    "df_name,raw_rows,monthly_rows,annual_rows", [
        ("utils_eia860", 44_801, 44_801, 44_801),
        ("plants_eia860", 84_828, 84_828, 84_828),
        ("pu_eia860", 83_993, 83_993, 83_993),
        ("own_eia860", 42_563, 42_563, 42_563),
        ("bga_eia860", 90_127, 90_127, 90_127),
        ("gens_eia860", 242_630, 242_630, 242_630),
        ("frc_eia923", 417_723, 175_553, 17_381),
        ("gen_eia923", 422_388, 422_388, 35_199),
        ("bf_eia923", 1_021_764, 855_048, 71_254),
        ("gf_eia923", 1_379_820, 1_106_712, 92_226),
    ])
def test_minmax_rows(pudl_out_eia,
                     live_pudl_db,
                     raw_rows,
                     annual_rows,
                     monthly_rows,
                     df_name):
    """Verify that output DataFrames don't have too many or too few rows.

    Args:
        pudl_out_eia: A PudlTabl output object.
        live_pudl_db (bool): Whether we're using a live or testing DB.
        min_rows (int): Minimum number of rows that the dataframe should
            contain when all data is loaded and is output without aggregation.
        df_name (str): Shorthand name identifying the dataframe, corresponding
            to the name of the function used to pull it from the PudlTabl
            output object.

    """
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
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
              margin=0.05, df_name=df_name)
        .pipe(pv.check_max_rows, expected_rows=expected_rows,
              margin=0.05, df_name=df_name)
    )


@pytest.mark.parametrize(
    "df_name,unique_subset", [
        ("plants_eia860", ["report_date", "plant_id_eia"]),
        ("utils_eia860", ["report_date", "utility_id_eia"]),
        ("pu_eia860", ["report_date", "plant_id_eia"]),
        ("gens_eia860", ["report_date", "plant_id_eia", "generator_id"]),
        ("bga_eia860", ["report_date",
                        "plant_id_eia",
                        "boiler_id",
                        "generator_id"]),
        ("own_eia860", ["report_date",
                        "plant_id_eia",
                        "generator_id",
                        "owner_utility_id_eia"]),
        ("gen_eia923", ["report_date", "plant_id_eia", "generator_id"]),
    ])
def test_unique_rows_eia(pudl_out_eia, live_pudl_db, unique_subset, df_name):
    """Test whether dataframe has unique records within a subset of columns."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    if (pudl_out_eia.freq is None) and (df_name == "gen_eia923"):
        pytest.xfail(reason="RE-RUN ETL DUDE.")
    pv.check_unique_rows(
        pudl_out_eia.__getattribute__(df_name)(),
        subset=unique_subset, df_name=df_name)
