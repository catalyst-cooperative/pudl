"""Unit tests for the pudl.transform.eia923 module."""

import pandas as pd

import pudl.transform.eia923 as eia923


def test__yearly_to_monthly_records__normal_values():
    """Test that monthly columns are reshaped to rows.

    input:
    idx   other_col  value_january   value_june
    100     0           1               2
    101     3           4               5

    output:
    idx      other_col  report_month    value
    100         0           1           1
    100         0           6           2
    101         3           1           4
    101         3           6           5
    """
    test_df = pd.DataFrame(
        [[0, 1, 2], [3, 4, 5]],
        columns=["other_col", "value_january", "value_june"],
        index=[100, 101],
    )
    actual = eia923._yearly_to_monthly_records(test_df)
    expected = pd.DataFrame(
        [[0, 1, 1], [0, 6, 2], [3, 1, 4], [3, 6, 5]],
        columns=["other_col", "report_month", "value"],
        index=[100, 100, 101, 101],
    )
    pd.testing.assert_frame_equal(expected, actual)


def test__yearly_to_monthly_records__empty_frame():
    """Test that empty dataframes still have correct column names.

    input:
    idx   other_col  value_january   value_june
    <empty>

    output:
    idx      other_col  report_month    value
    <empty>
    """
    # empty dfs initialize with Index by default, so need to specify RangeIndex
    test_df = pd.DataFrame(
        [],
        columns=["other_col", "value_january", "value_june"],
        index=pd.RangeIndex(start=0, stop=0, step=1),
    )
    actual = eia923._yearly_to_monthly_records(test_df)
    expected = pd.DataFrame(
        [],
        columns=["other_col", "report_month", "value"],
        index=pd.RangeIndex(start=0, stop=0, step=1),
    )
    # report_month dtype changes from object to int64
    # but only because they are empty and get sent to default types during df.stack()
    pd.testing.assert_frame_equal(expected, actual, check_dtype=False)
