"""Unit tests for the pudl.transform.eia861 module."""

from io import StringIO

import pandas as pd
import pytest

from pudl.metadata.fields import apply_pudl_dtypes
from pudl.transform import eia861

# Test that rows with the same primary key are combined correctly
actual_1 = pd.read_csv(
    StringIO(
        """report_date,utility_id_eia,state,ba_code,value
2019-01-01,88888,TX,ERCOT,100
2019-01-01,88888,TX,ERCOT,300
2019-01-01,88888,pd.NA,ERCOT,800
"""
    ),
).pipe(apply_pudl_dtypes, group="eia")

expected_1 = pd.read_csv(
    StringIO(
        """report_date,utility_id_eia,state,ba_code,value
2019-01-01,88888,TX,ERCOT,400
2019-01-01,88888,pd.NA,ERCOT,800
"""
    ),
).pipe(apply_pudl_dtypes, group="eia")

# Test that rows without 88888 values are not affected
actual_2 = pd.read_csv(
    StringIO(
        """report_date,utility_id_eia,state,ba_code,value
2019-01-01,10000,TX,ERCOT,100
2019-01-01,10000,TX,ERCOT,300
2019-01-01,10000,pd.NA,ERCOT,800
"""
    ),
).pipe(apply_pudl_dtypes, group="eia")

expected_2 = pd.read_csv(
    StringIO(
        """report_date,utility_id_eia,state,ba_code,value
2019-01-01,10000,TX,ERCOT,100
2019-01-01,10000,TX,ERCOT,300
2019-01-01,10000,pd.NA,ERCOT,800
"""
    ),
).pipe(apply_pudl_dtypes, group="eia")

# Test that rows get dropped when not all non numberic values match
actual_3 = pd.read_csv(
    StringIO(
        """report_date,utility_id_eia,state,ba_code,value
2019-01-01,88888,TX,ERCOT,100
2019-01-01,88888,TX,EXAMPLE,300
2019-01-01,88888,pd.NA,ERCOT,800
"""
    ),
).pipe(apply_pudl_dtypes, group="eia")

expected_3 = pd.read_csv(
    StringIO(
        """report_date,utility_id_eia,state,ba_code,value
2019-01-01,88888,pd.NA,ERCOT,800
"""
    ),
).pipe(apply_pudl_dtypes, group="eia")


@pytest.mark.parametrize(
    "actual,expected",
    [
        (actual_1, expected_1),
        (actual_2, expected_2),
        # (actual_3, expected_3),
    ],
)
def test__combine_88888_values(actual, expected):
    """Test that combine_88888 correctly combines data from multiple sources."""
    idx_cols = ["report_date", "utility_id_eia", "state"]
    actual_test = eia861._combine_88888_values(actual, idx_cols)
    pd.testing.assert_frame_equal(expected, actual_test)
