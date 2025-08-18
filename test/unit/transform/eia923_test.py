"""Unit tests for the pudl.transform.eia923 module."""

from io import StringIO

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


def test___drop_duplicates__core_eia923__generation():
    """Test whether this bespoke de-duper actually preserves one of the records.

    We test two cases. One is the "normal" case where one set of records has entirely
    null or zero-values, and the other in which there's a mix of null and non-null
    values which need to be aggregated.
    """
    ##############################################################################
    # This is the standard there are two records one is null or 0.0 dupe
    dupes = pd.read_csv(
        StringIO(
            """plant_id_eia,generator_id,report_date,net_generation_mwh
55358,CT1,2025-01-01,0.0
55358,CT1,2025-01-01,100413.0
55358,CT1,2025-02-01,
55358,CT1,2025-02-01,96550.0
"""
        ),
        parse_dates=["report_date"],
    ).convert_dtypes()

    expected_deduped = pd.read_csv(
        StringIO(
            """plant_id_eia,generator_id,report_date,net_generation_mwh
55358,CT1,2025-01-01,100413.0
55358,CT1,2025-02-01,96550.0
"""
        ),
        parse_dates=["report_date"],
    ).convert_dtypes()

    got_deduped = eia923._drop_duplicates__core_eia923__generation(
        dupes, unit_test=True
    ).reset_index(drop=True)
    pd.testing.assert_frame_equal(expected_deduped, got_deduped)

    ##############################################################################
    # This is the specific plant from 2012/2013 that has two records per gen with
    # different prime_mover_code
    still_dupes = pd.read_csv(
        StringIO(
            """plant_id_eia,generator_id,report_date,net_generation_mwh,sector_id_eia,prime_mover_code
3405,1,2012-08-01,1000,1.0,CA
3405,1,2012-08-01,50,1.0,ST
3405,1,2012-09-01,2000,1.0,CA
3405,1,2012-09-01,-80.0,1.0,ST
3405,1,2013-11-01,3000,1.0,CA
3405,1,2013-11-01,-100,1.0,ST"""
        ),
        parse_dates=["report_date"],
    ).convert_dtypes()
    got_still_deduped = eia923._drop_duplicates__core_eia923__generation(
        still_dupes, unit_test=True
    ).reset_index(drop=True)

    expected_still_deduped = pd.read_csv(
        StringIO(
            """plant_id_eia,generator_id,report_date,net_generation_mwh,sector_id_eia,prime_mover_code
3405,1,2012-08-01,1050,1.0,
3405,1,2012-09-01,1920,1.0,
3405,1,2013-11-01,2900,1.0,"""
        ),
        parse_dates=["report_date"],
    ).convert_dtypes()
    pd.testing.assert_frame_equal(
        expected_still_deduped, got_still_deduped, check_dtype=False
    )
