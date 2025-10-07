"""Tests for Glue functions."""

from io import StringIO

import pandas as pd

from pudl.transform.phmsagas import backfill_zero_operator_id_phmsa


def test_backfill_zero_operator_id_phmsa():
    """Test backfill_zero_operator_id_phmsa."""
    test_df = pd.read_csv(
        StringIO(
            """report_date,operator_id_phmsa,operator_name_phmsa
    2025-01-01,1,Gas Guy
    2024-01-01,1,Gas Guy
    2023-01-01,1,Gas Guy
    2022-01-01,1,Gas Guy
    2021-01-01,1,
    2025-01-01,2,Gas Guyz
    2024-01-01,2,Gas Guyz
    2023-01-01,0,Gas Guyz
    2025-01-01,3,lil weirdo
    2024-01-01,4,Lil Weirdoz
    2023-01-01,0,lil weirdoz
    2023-01-01,0,shrug
    """
        ),
        parse_dates=["report_date"],
    ).convert_dtypes()

    expected_out = pd.read_csv(
        StringIO(
            """report_date,operator_id_phmsa,operator_name_phmsa
    2025-01-01,1,Gas Guy
    2024-01-01,1,Gas Guy
    2023-01-01,1,Gas Guy
    2022-01-01,1,Gas Guy
    2021-01-01,1,
    2025-01-01,2,Gas Guyz
    2024-01-01,2,Gas Guyz
    2023-01-01,2,Gas Guyz
    2025-01-01,3,Lil Weirdo
    2024-01-01,4,Lil Weirdoz
    2023-01-01,4,Lil Weirdoz
    2023-01-01,0,Shrug
    """
        ),
        parse_dates=["report_date"],
    ).convert_dtypes()
    out = backfill_zero_operator_id_phmsa(test_df).drop(
        columns=["operator_id_phmsa_old"]
    )
    pd.testing.assert_frame_equal(out, expected_out)
