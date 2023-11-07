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
        ("fipsified_respondents_ferc714", 135_537),
        ("compiled_geometry_balancing_authority_eia861", 112_507),
        ("compiled_geometry_utility_eia861", 247_705),
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


def test_report_year_discrepency_in_demand_hourly_pa_ferc714(pudl_out_orig):
    """Test if the vast majority of the years in the two date columns line up."""
    demand_hourly_pa_ferc714 = pudl_out_orig.demand_hourly_pa_ferc714()
    mismatched_report_years = demand_hourly_pa_ferc714[
        (
            demand_hourly_pa_ferc714.utc_datetime.dt.year
            != demand_hourly_pa_ferc714.report_date.dt.year
        )
    ]
    if (
        off_ratio := len(mismatched_report_years) / len(demand_hourly_pa_ferc714)
    ) > 0.001:
        raise AssertionError(
            f"Found more ({off_ratio:.2%}) than expected (>.1%) FERC714 records"
            " where the report year from the utc_datetime differs from the "
            "report_date column."
        )
