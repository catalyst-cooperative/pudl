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
        ("compiled_geometry_balancing_authority_eia861", 108_436),
        ("compiled_geometry_utility_eia861", 237_872),
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
