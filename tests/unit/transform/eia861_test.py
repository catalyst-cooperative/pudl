"""Unit tests for the pudl.transform.eia861 module."""

from io import StringIO

import pandas as pd
import pytest

from pudl.metadata.dtypes import apply_pudl_dtypes
from pudl.transform import eia861

# Test that rows with the same primary key are combined correctly
actual_1 = pd.read_csv(
    StringIO(
        """report_date,utility_id_eia,state,ba_code,value
2019-01-01,88888,TX,ERCOT,100
2019-01-01,88888,TX,ERCOT,300
2019-01-01,88888,,ERCOT,800
"""
    ),
).pipe(apply_pudl_dtypes, field_namespace="eia")

expected_1 = pd.read_csv(
    StringIO(
        """report_date,utility_id_eia,state,ba_code,value
2019-01-01,88888,TX,ERCOT,400
2019-01-01,88888,,ERCOT,800
"""
    ),
).pipe(apply_pudl_dtypes, field_namespace="eia")

# Test that rows without 88888 values are not affected
actual_2 = pd.read_csv(
    StringIO(
        """report_date,utility_id_eia,state,ba_code,value
2019-01-01,10000,TX,ERCOT,100
2019-01-01,10000,TX,ERCOT,300
2019-01-01,10000,,ERCOT,800
"""
    ),
).pipe(apply_pudl_dtypes, field_namespace="eia")

expected_2 = pd.read_csv(
    StringIO(
        """report_date,utility_id_eia,state,ba_code,value
2019-01-01,10000,TX,ERCOT,100
2019-01-01,10000,TX,ERCOT,300
2019-01-01,10000,,ERCOT,800
"""
    ),
).pipe(apply_pudl_dtypes, field_namespace="eia")

# Test that duplicate rows get dropped when non numeric values don't match
actual_3 = pd.read_csv(
    StringIO(
        """report_date,utility_id_eia,state,ba_code,value
2019-01-01,88888,TX,ERCOT,100
2019-01-01,88888,TX,EXAMPLE,300
2019-01-01,88888,,ERCOT,800
"""
    ),
).pipe(apply_pudl_dtypes, field_namespace="eia")

expected_3 = pd.read_csv(
    StringIO(
        """report_date,utility_id_eia,state,ba_code,value
    2019-01-01,88888,TX,ERCOT,100
    2019-01-01,88888,,ERCOT,800
"""
    ),
).pipe(apply_pudl_dtypes, field_namespace="eia")

# Test that NA values in the index cols aren't converted into two rows
actual_4 = pd.read_csv(
    StringIO(
        """report_date,utility_id_eia,state,ba_code,value
2019-01-01,88888,TX,ERCOT,100
2019-01-01,88888,,ERCOT,300
2019-01-01,88888,,ERCOT,800
"""
    ),
).pipe(apply_pudl_dtypes, field_namespace="eia")

expected_4 = pd.read_csv(
    StringIO(
        """report_date,utility_id_eia,state,ba_code,value
2019-01-01,88888,TX,ERCOT,100
2019-01-01,88888,,ERCOT,1100
"""
    ),
).pipe(apply_pudl_dtypes, field_namespace="eia")


@pytest.mark.parametrize(
    "actual,expected",
    [
        pytest.param(actual_1, expected_1, id="same_pk_does_combine"),
        pytest.param(actual_2, expected_2, id="no_88888_does_not_combine"),
        pytest.param(actual_3, expected_3, id="non_numeric_does_not_combine"),
        pytest.param(actual_4, expected_4, id="na_index_col_does_not_duplicate"),
    ],
)
def test__combine_88888_values(actual, expected):
    """Test that combine_88888 correctly combines data from multiple sources."""
    idx_cols = ["report_date", "utility_id_eia", "state"]
    observed_outcome = eia861._combine_88888_values(actual, idx_cols)
    pd.testing.assert_frame_equal(expected, observed_outcome)


@pytest.mark.parametrize("dtype", ["object", "string"])
def test__make_yn_bool__series(dtype):
    """Y/X/N codes become nullable booleans; nulls and other codes become NA."""
    raw = pd.Series(["Y", "y", "N", "n", "X", "x", "Z", None, pd.NA], dtype=dtype)
    actual = eia861._make_yn_bool(raw)
    expected = pd.Series(
        [True, True, False, False, True, True, pd.NA, pd.NA, pd.NA], dtype="boolean"
    )
    pd.testing.assert_series_equal(actual, expected)


def test__make_yn_bool__keeps_existing_booleans():
    """Values that are already booleans are preserved."""
    raw = pd.Series([True, False, "Y", None], dtype="object")
    expected = pd.Series([True, False, True, pd.NA], dtype="boolean")
    pd.testing.assert_series_equal(eia861._make_yn_bool(raw), expected)


def test__make_yn_bool__dataframe():
    """DataFrames are converted column by column, preserving the index."""
    raw = pd.DataFrame(
        {"a": ["Y", None], "b": pd.array(["n", pd.NA], dtype="string")},
        index=pd.Index([5, 6], name="k"),
    )
    expected = pd.DataFrame(
        {
            "a": pd.array([True, pd.NA], dtype="boolean"),
            "b": pd.array([False, pd.NA], dtype="boolean"),
        },
        index=pd.Index([5, 6], name="k"),
    )
    pd.testing.assert_frame_equal(eia861._make_yn_bool(raw), expected)
