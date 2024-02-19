"""Validate post-ETL FERC 714 outputs and associated service territory analyses."""
import logging

import pytest

import pudl
from pudl import validate as pv

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "df_name,expected_rows",
    [
        ("summarized_demand_ferc714", 3_195),
        ("fipsified_respondents_ferc714", 135_627),
        ("compiled_geometry_balancing_authority_eia861", 112_853),
        ("compiled_geometry_utility_eia861", 248_987),
    ],
)
def test_minmax_rows(
    pudl_out_orig: "pudl.output.pudltabl.PudlTabl",
    live_dbs: bool,
    expected_rows: int,
    df_name: str,
):
    """Verify that output DataFrames don't have too many or too few rows.

    Args:
        pudl_out_orig: A PudlTabl output object.
        live_dbs: Whether we're using a live or testing DB.
        expected_rows: Expected number of rows that the dataframe should
            contain when all data is loaded and is output without aggregation.
        df_name: Shorthand name identifying the dataframe, corresponding
            to the name of the function used to pull it from the PudlTabl
            output object.
    """
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    _ = (
        pudl_out_orig.__getattribute__(df_name)()
        .pipe(
            pv.check_min_rows, expected_rows=expected_rows, margin=0.0, df_name=df_name
        )
        .pipe(
            pv.check_max_rows, expected_rows=expected_rows, margin=0.0, df_name=df_name
        )
    )


@pytest.mark.parametrize(
    "df_name,expected_rows",
    [("demand_hourly_pa_ferc714", 15_608_154)],
)
def test_minmax_rows_and_year_in_ferc714_hourly_planning_area_demand(
    pudl_out_orig: "pudl.output.pudltabl.PudlTabl",
    live_dbs: bool,
    expected_rows: int,
    df_name: str,
):
    """Test if the majority of the years in the two date columns line up & min/max rows.

    We are parameterizing this test even though it only has one input because the
    test_minmax_rows is a common test across many tables and we wanted to preserve the
    format.
    """
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    hpad_ferc714 = pudl_out_orig.__getattribute__(df_name)()
    _ = hpad_ferc714.pipe(
        pv.check_min_rows, expected_rows=expected_rows, margin=0.0, df_name=df_name
    ).pipe(pv.check_max_rows, expected_rows=expected_rows, margin=0.0, df_name=df_name)

    logger.info("Checking the consistency of the year in the multiple date columns.")
    mismatched_report_years = hpad_ferc714[
        (hpad_ferc714.utc_datetime.dt.year != hpad_ferc714.report_date.dt.year)
    ]
    if (off_ratio := len(mismatched_report_years) / len(hpad_ferc714)) > 0.001:
        raise AssertionError(
            f"Found more ({off_ratio:.2%}) than expected (>.1%) FERC714 records"
            " where the report year from the utc_datetime differs from the "
            "report_date column."
        )
