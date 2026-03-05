"""Unit tests for the pudl.transform.eia923 module."""

from io import StringIO

import pandas as pd
import pytest

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


@pytest.mark.parametrize(
    "raw_values, expected_dates, spot_fixes",
    [
        pytest.param(
            ["012024"],
            ["2024-01-01"],
            None,
            id="six_digit_no_dash",
        ),
        pytest.param(
            ["01-2024"],
            ["2024-01-01"],
            None,
            id="six_char_with_dash",
        ),
        pytest.param(
            ["1990"],
            ["1990-01-01"],
            None,
            id="four_digit_year_treated_as_january",
        ),
        pytest.param(
            ["12024"],
            ["2024-01-01"],
            None,
            id="five_digit_single_digit_month_treated_as_january",
        ),
        pytest.param(
            ["1/2024"],
            ["2024-01-01"],
            None,
            id="one_digit_month_slash_four_digit_year",
        ),
        pytest.param(
            ["1/05"],
            ["2005-01-01"],
            None,
            id="one_digit_month_slash_two_digit_year_2005",
        ),
        pytest.param(
            ["1/95"],
            ["1995-01-01"],
            None,
            id="one_digit_month_slash_two_digit_year_1995",
        ),
        pytest.param(
            ["1/11/07"],
            ["2007-01-11"],
            None,
            id="complete_date_with_slashes_and_two_digit_year",
        ),
        pytest.param(
            ["12/00"],
            ["2000-12-01"],
            None,
            id="two_digit_month_slash_two_digit_year_2000",
        ),
        pytest.param(
            ["12-90"],
            ["1990-12-01"],
            None,
            id="two_digit_month_dash_two_digit_year_1990",
        ),
        pytest.param(
            ["12-00"],
            ["2000-12-01"],
            None,
            id="two_digit_month_dash_two_digit_year_2000",
        ),
        pytest.param(
            ["1-20"],
            ["2020-01-01"],
            None,
            id="one_digit_month_dash_two_digit_year",
        ),
        pytest.param(
            ["122024"],
            ["2024-12-01"],
            None,
            id="december",
        ),
        pytest.param(
            ["002024"],
            ["2024-01-01"],
            None,
            id="zero_month_coerced_to_january",
        ),
        pytest.param(
            ["6-2209"],
            ["2009-06-22"],
            {"6-2209": pd.Timestamp("2009-06-22")},
            id="malformed_but_coercible_date",
        ),
        pytest.param(
            ["0-2024"],
            ["2024-01-01"],
            None,
            id="zero_month_with_dash_coerced_to_january",
        ),
        pytest.param(
            ["010000"],
            [pd.NaT],
            None,
            id="year_zero_returns_nat",
        ),
        pytest.param(
            ["na"],
            [pd.NaT],
            None,
            id="string_na_returns_nat",
        ),
        pytest.param(
            ["CEM"],
            [pd.NaT],
            None,
            id="string_nonsense_returns_nat",
        ),
        pytest.param(
            [None],
            [pd.NaT],
            None,
            id="null_returns_nat",
        ),
        pytest.param(
            ["00-0000"],
            [pd.NaT],
            None,
            id="all_zeros_returns_nat",
        ),
        pytest.param(
            ["0"],
            [pd.NaT],
            None,
            id="zero_returns_nat",
        ),
        pytest.param(
            ["012024", "01-2024", None, "002024", "010000"],
            ["2024-01-01", "2024-01-01", pd.NaT, "2024-01-01", pd.NaT],
            None,
            id="mixed_valid_and_invalid",
        ),
    ],
)
def test__clean_emissions_control_dates(raw_values, expected_dates, spot_fixes):
    """Test that raw EIA-923 date strings are parsed correctly."""
    col = pd.array(raw_values, dtype="string")
    result = eia923._clean_emissions_control_dates(
        pd.Series(col), spot_fixes=spot_fixes
    )
    expected = pd.to_datetime(pd.Series(expected_dates))
    pd.testing.assert_series_equal(result, expected, check_names=False)


@pytest.mark.parametrize(
    "raw_values, match",
    [
        pytest.param(
            ["132025"],
            None,
            id="month_13_out_of_range",
        ),
        pytest.param(
            ["202510"],
            None,
            id="year_month_swapped_order",
        ),
        pytest.param(
            ["0012025"],
            None,
            id="excess_digits",
        ),
        pytest.param(
            ["JAN2024"],
            "non-digit/non-dash",
            id="invalid_chars",
        ),
    ],
)
def test__clean_emissions_control_dates__raises(raw_values, match):
    """Test that malformed date strings raise rather than silently returning NaT.

    Covers: out-of-range months, inverted year/month order, excess digits, and
    non-digit/non-dash characters.
    """
    with pytest.raises(Exception, match=match):
        eia923._clean_emissions_control_dates(pd.Series(raw_values, dtype="string"))
