"""Tests for Glue functions."""

from io import StringIO
from typing import cast

import pandas as pd
from dagster import AssetCheckResult

from pudl.transform.phmsagas import (
    _check_all_raw_columns_being_transformed,
    backfill_zero_operator_id_phmsa,
)


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


def _run_check(raw_df: pd.DataFrame) -> AssetCheckResult:
    """Invoke the asset check directly, which the decorator types as ``object``."""
    return cast(AssetCheckResult, _check_all_raw_columns_being_transformed(raw_df))


def test_check_all_raw_columns_being_transformed_passes():
    """The check passes when every raw column is accounted for in the transform."""
    result = _run_check(
        pd.DataFrame({"report_year": [2020], "main_other_material_detail": ["x"]})
    )
    assert result.passed
    assert result.metadata["num_untransformed_columns"].value == 0


def test_check_all_raw_columns_being_transformed_fails_and_reports_columns():
    """The check fails, rather than raising, and lists the untransformed columns."""
    result = _run_check(
        pd.DataFrame({"report_year": [2020], "zzz_new_col": [1], "aaa_new_col": [2]})
    )
    assert not result.passed
    assert result.metadata["num_untransformed_columns"].value == 2
    assert result.metadata["untransformed_columns"].value == [
        "aaa_new_col",
        "zzz_new_col",
    ]
    assert result.description is not None
    assert "2 raw columns" in result.description
