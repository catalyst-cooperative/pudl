"""Unit tests for the :mod:`pudl.helpers` module."""

from io import StringIO

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal, assert_series_equal
from pandas.tseries.offsets import BYearEnd

import pudl
from pudl.helpers import (
    add_fips_ids,
    apply_pudl_dtypes,
    convert_col_to_bool,
    convert_df_to_excel_file,
    convert_to_date,
    date_merge,
    dedupe_and_drop_nas,
    diff_wide_tables,
    expand_timeseries,
    fix_eia_na,
    flatten_list,
    remove_leading_zeros_from_numeric_strings,
    retry,
    standardize_percentages_ratio,
    zero_pad_numeric_string,
)
from pudl.output.sql.helpers import sql_asset_factory

MONTHLY_GEN_FUEL = pd.DataFrame(
    {
        "report_date": [
            "2019-12-01",
            "2020-10-01",
            "2019-01-01",
            "2019-06-01",
            "2018-07-01",
        ],
        "plant_id_eia": [2, 2, 3, 3, 3],
        "prime_mover_code": ["HY", "ST", "HY", "CT", "HY"],
        "fuel_consumed_units": [0.0, 98085.0, 0.0, 4800000.0, 0.0],
    }
)

ANNUAL_PLANTS_UTIL = pd.DataFrame(
    {
        "report_date": [
            "2020-01-01",
            "2020-01-01",
            "2019-01-01",
            "2018-01-01",
            "2020-01-01",
            "2019-01-01",
            "2018-01-01",
        ],
        "plant_id_eia": [1, 2, 2, 2, 3, 3, 3],
        "plant_name_eia": [
            "Sand Point",
            "Bankhead",
            "Bankhead Dam",
            "Bankhead Dam",
            "Barry",
            "Barry",
            "Barry",
        ],
        "utility_id_eia": [63560, 195, 195, 195, 16, 16, 16],
    }
).astype({"report_date": "datetime64[ns]"})

QUARTERLY_DATA = pd.DataFrame(
    {
        "report_date": ["2020-01-01", "2020-10-01", "2019-04-01", "2019-01-01"],
        "plant_id_eia": [2, 2, 3, 3],
        "data": [1, 4, 2, 1],
    }
)

MONTHLY_OTHER = pd.DataFrame(
    {
        "report_date": ["2019-10-01", "2020-10-01", "2019-01-01", "2018-02-01"],
        "plant_id_eia": [2, 2, 3, 3],
        "energy_source_code": ["DFO", "WND", "WND", "DFO"],
    }
)

DAILY_DATA = pd.DataFrame(
    {
        "date": ["2019-10-12", "2019-10-13", "2019-12-01", "2018-02-03"],
        "plant_id_eia": [2, 2, 2, 3],
        "daily_data": [1, 2, 3, 4],
    }
).astype({"date": "datetime64[ns]"})


def test_annual_attribute_merge():
    """Test merging annual attributes onto monthly data with a sparse report date.

    The left and right merges in this case is a one to many merge and should yield an
    output table with the exact same data records as the input data table.

    The inner merge case loses records. The outer merge case creates extra records with
    NA values.
    """
    out_expected_left = pd.DataFrame(
        {
            "report_date": [
                "2019-12-01",
                "2020-10-01",
                "2019-01-01",
                "2019-06-01",
                "2018-07-01",
            ],
            "plant_id_eia": [2, 2, 3, 3, 3],
            "prime_mover_code": ["HY", "ST", "HY", "CT", "HY"],
            "fuel_consumed_units": [0.0, 98085.0, 0.0, 4800000.0, 0.0],
            "plant_name_eia": ["Bankhead Dam", "Bankhead", "Barry", "Barry", "Barry"],
            "utility_id_eia": [195, 195, 16, 16, 16],
        }
    ).astype({"report_date": "datetime64[ns]"})

    out_left = date_merge(
        left=MONTHLY_GEN_FUEL.copy(),
        right=ANNUAL_PLANTS_UTIL.copy(),
        on=["plant_id_eia"],
        how="left",
    )

    assert_frame_equal(out_left, out_expected_left)

    out_expected_right = pd.DataFrame(
        {
            "report_date": [
                "2019-12-01",
                "2020-10-01",
                "2019-01-01",
                "2019-06-01",
                "2018-07-01",
            ],
            "plant_id_eia": [2, 2, 3, 3, 3],
            "plant_name_eia": ["Bankhead Dam", "Bankhead", "Barry", "Barry", "Barry"],
            "utility_id_eia": [195, 195, 16, 16, 16],
            "prime_mover_code": ["HY", "ST", "HY", "CT", "HY"],
            "fuel_consumed_units": [0.0, 98085.0, 0.0, 4800000.0, 0.0],
        }
    ).astype({"report_date": "datetime64[ns]"})

    out_right = date_merge(
        left=ANNUAL_PLANTS_UTIL.copy(),
        right=MONTHLY_GEN_FUEL.copy(),
        on=["plant_id_eia"],
        how="right",
    )

    assert_frame_equal(out_right, out_expected_right)

    out_expected_inner = pd.DataFrame(
        {
            "report_date": [
                "2019-12-01",
                "2020-10-01",
                "2019-01-01",
                "2019-06-01",
                "2018-07-01",
            ],
            "plant_id_eia": [2, 2, 3, 3, 3],
            "prime_mover_code": ["HY", "ST", "HY", "CT", "HY"],
            "fuel_consumed_units": [0.0, 98085.0, 0.0, 4800000.0, 0.0],
            "plant_name_eia": ["Bankhead Dam", "Bankhead", "Barry", "Barry", "Barry"],
            "utility_id_eia": [195, 195, 16, 16, 16],
        }
    ).astype({"report_date": "datetime64[ns]"})

    out_inner = date_merge(
        left=MONTHLY_GEN_FUEL.copy(),
        right=ANNUAL_PLANTS_UTIL.copy(),
        on=["plant_id_eia"],
        how="inner",
    )

    assert_frame_equal(out_inner, out_expected_inner)

    out_expected_outer = pd.read_csv(
        StringIO(
            """
report_date,plant_id_eia,prime_mover_code,fuel_consumed_units,plant_name_eia,utility_id_eia
2018-01-01,2,,,Bankhead Dam,195
2018-07-01,3,HY,0.0,Barry,16
2019-12-01,2,HY,0.0,Bankhead Dam,195
2019-01-01,3,HY,0.0,Barry,16
2019-06-01,3,CT,4800000.0,Barry,16
2020-01-01,1,,,Sand Point,63560
2020-10-01,2,ST,98085.0,Bankhead,195
2020-01-01,3,,,Barry,16
"""
        )
    ).astype({"report_date": "datetime64[ns]"})

    out_outer = date_merge(
        left=MONTHLY_GEN_FUEL.copy(),
        right=ANNUAL_PLANTS_UTIL.copy(),
        on=["plant_id_eia"],
        how="outer",
    )

    assert_frame_equal(out_outer, out_expected_outer)


def test_monthly_attribute_merge():
    """Test merging monthly attributes onto daily data with a sparse report date."""
    out_expected = pd.DataFrame(
        {
            "report_date": ["2019-10-12", "2019-10-13", "2019-12-01", "2018-02-03"],
            "plant_id_eia": [2, 2, 2, 3],
            "daily_data": [1, 2, 3, 4],
            "energy_source_code": ["DFO", "DFO", None, "DFO"],
        }
    ).astype({"report_date": "datetime64[ns]"})

    out = date_merge(
        left=DAILY_DATA.copy(),
        right=MONTHLY_OTHER.copy(),
        left_date_col="date",
        on=["plant_id_eia"],
        date_on=["year", "month"],
        how="left",
    )

    assert_frame_equal(out, out_expected)


def test_quarterly_attribute_merge():
    """Test merging quarterly attributes onto monthly data.

    This should probably be updated once we need a quarterly merge for FERC data.
    """
    out_expected = pd.DataFrame(
        {
            "report_date": [
                "2019-12-01",
                "2020-10-01",
                "2019-01-01",
                "2019-06-01",
                "2018-07-01",
            ],
            "plant_id_eia": [2, 2, 3, 3, 3],
            "prime_mover_code": ["HY", "ST", "HY", "CT", "HY"],
            "fuel_consumed_units": [0.0, 98085.0, 0.0, 4800000.0, 0.0],
            "data": [None, 4.0, 1.0, 2.0, None],
        }
    ).astype({"report_date": "datetime64[ns]"})

    out = date_merge(
        left=MONTHLY_GEN_FUEL.copy(),
        right=QUARTERLY_DATA.copy(),
        on=["plant_id_eia"],
        date_on=["year", "quarter"],
        how="left",
    )

    assert_frame_equal(out, out_expected)


def test_same_temporal_gran():
    """Test merging tables with the same temporal granularity.

    In this case, this yields the same results as ``pd.merge``.
    """
    out_expected = MONTHLY_GEN_FUEL.merge(
        MONTHLY_OTHER,
        how="left",
        on=["report_date", "plant_id_eia"],
    ).astype({"report_date": "datetime64[ns]"})

    out = date_merge(
        left=MONTHLY_GEN_FUEL.copy(),
        right=MONTHLY_OTHER.copy(),
        on=["plant_id_eia"],
        date_on=["year", "month"],
        how="left",
    )
    assert_frame_equal(out, out_expected)


def test_end_of_report_period():
    """Test merging tables repeated at the end of the report period."""
    eoy_plants_util = ANNUAL_PLANTS_UTIL.copy()
    eoy_plants_util.loc[:, "report_date"] = eoy_plants_util.report_date + BYearEnd()

    out_expected = pd.DataFrame(
        {
            "report_date": [
                "2019-12-01",
                "2020-10-01",
                "2019-01-01",
                "2019-06-01",
                "2018-07-01",
            ],
            "plant_id_eia": [2, 2, 3, 3, 3],
            "prime_mover_code": ["HY", "ST", "HY", "CT", "HY"],
            "fuel_consumed_units": [0.0, 98085.0, 0.0, 4800000.0, 0.0],
            "plant_name_eia": ["Bankhead Dam", "Bankhead", "Barry", "Barry", "Barry"],
            "utility_id_eia": [195, 195, 16, 16, 16],
        }
    ).astype({"report_date": "datetime64[ns]"})

    out = date_merge(
        MONTHLY_GEN_FUEL.copy(),
        eoy_plants_util,
        on=["plant_id_eia"],
        how="left",
        report_at_start=False,
    )

    assert_frame_equal(out, out_expected)


def test_less_granular_merge():
    """Test merging a more granular table onto a less granular table."""
    out_expected = pd.DataFrame(
        {
            "report_date": [
                "2020-01-01",
                "2020-01-01",
                "2019-01-01",
                "2018-01-01",
                "2020-01-01",
            ],
            "plant_id_eia": [1, 2, 2, 2, 3],
            "plant_name_eia": [
                "Sand Point",
                "Bankhead",
                "Bankhead Dam",
                "Bankhead Dam",
                "Barry",
            ],
            "utility_id_eia": [63560, 195, 195, 195, 16],
            "prime_mover_code": [None, "ST", "HY", None, None],
            "fuel_consumed_units": [None, 98085.0, 0.0, None, None],
        }
    ).astype({"report_date": "datetime64[ns]"})

    out = date_merge(
        ANNUAL_PLANTS_UTIL[:5].copy(),
        MONTHLY_GEN_FUEL.copy(),
        on=["plant_id_eia"],
        date_on=["year"],
        how="left",
        report_at_start=False,
    )

    assert_frame_equal(out, out_expected)


def test_timeseries_fillin(test_dir):
    """Test filling in tables to a full timeseries."""
    input_df = pd.DataFrame(
        {
            "report_date": [
                "2019-02-01",
                "2020-01-01",
                "2020-02-01",
                "2019-03-01",
                "2019-10-01",
                "2020-02-01",
            ],
            "plant_id_eia": [1, 1, 1, 1, 2, 2],
            "generator_id": [1, 2, 1, 1, 3, 3],
            "data": [2.0, 1.0, 2.0, 3.0, 10.0, 2.0],
        }
    ).pipe(apply_pudl_dtypes, group="eia")

    expected_out_path = (
        test_dir / "data/date_merge_unit_test/timeseries_fillin_expected_out.csv"
    )
    expected_out = (
        pd.read_csv(expected_out_path)
        .pipe(apply_pudl_dtypes, group="eia")
        .astype({"data": "float64"})
    )

    out = expand_timeseries(
        input_df, fill_through_freq="year", key_cols=["plant_id_eia", "generator_id"]
    )
    assert_frame_equal(expected_out, out)


def test_timeseries_fillin_through_month(test_dir):
    """Test filling in full timeseries through the end of last reported month."""
    input_df = pd.DataFrame(
        {
            "report_date": [
                "2019-12-30",
                "2020-01-02",
                "2020-01-25",
                "2020-11-27",
                "2020-12-01",
            ],
            "plant_id_eia": [1, 1, 1, 2, 2],
            "generator_id": [1, 1, 2, 1, 1],
            "data": [1.0, 2.0, 1.0, 3.0, 4.0],
        }
    ).pipe(apply_pudl_dtypes, group="eia")

    expected_out_path = (
        test_dir
        / "data/date_merge_unit_test/timeseries_fillin_through_month_expected_out.csv"
    )
    expected_out = (
        pd.read_csv(expected_out_path)
        .pipe(apply_pudl_dtypes, group="eia")
        .astype({"data": "float64"})
    )
    out = expand_timeseries(
        input_df,
        freq="D",
        fill_through_freq="month",
        key_cols=["plant_id_eia", "generator_id"],
    )
    assert_frame_equal(expected_out, out)


def test_convert_to_date():
    """Test automated cleanup of EIA date columns."""
    in_df = pd.DataFrame.from_records(
        columns=["report_year", "report_month", "report_day"],
        data=[
            (2019, 3, 14),
            ("2019", "03", "14"),
        ],
    )
    expected_df = pd.DataFrame(
        {
            "report_date": pd.to_datetime(
                [
                    "2019-03-14",
                    "2019-03-14",
                ]
            ),
        }
    )
    out_df = convert_to_date(in_df)
    assert_frame_equal(out_df, expected_df)


def test_fix_eia_na():
    """Test cleanup of bad EIA spreadsheet NA values."""
    in_df = pd.DataFrame(
        {
            "vals": [
                0,  # Don't touch integers, even if they're null-ish
                0.0,  # Don't touch floats, even if they're null-ish
                "0.",  # Should only replace naked decimals
                ".0",  # Should only replace naked decimals
                "..",  # Only replace single naked decimals
                "",
                " ",
                "\t",
                ".",
                "  ",  # Multiple whitespace characters
                "\t\t",  # 2-tabs: another Multi-whitespace
            ]
        }
    )
    expected_df = pd.DataFrame(
        {
            "vals": [
                0,
                0.0,
                "0.",
                ".0",
                "..",
                pd.NA,
                pd.NA,
                pd.NA,
                pd.NA,
                pd.NA,
                pd.NA,
            ]
        }
    )
    out_df = fix_eia_na(in_df)
    assert_frame_equal(out_df, expected_df)


def test_remove_leading_zeros_from_numeric_strings():
    """Test removal of leading zeroes from EIA generator IDs."""
    in_df = pd.DataFrame(
        {
            "generator_id": [
                "0001",  # Leading zeroes, all numeric string.
                "26",  # An appropriate numeric string w/o leading zeroes.
                100,  # Integer, should get stringified.
                100.0,  # What happens if it's a float?
                "01-A",  # Leading zeroes, alphanumeric. Should not change.
                "HRSG-01",  # Alphanumeric, should be no change.
            ]
        }
    )
    expected_df = pd.DataFrame(
        {
            "generator_id": [
                "1",
                "26",
                "100",
                "100.0",
                "01-A",
                "HRSG-01",
            ]
        }
    )
    out_df = remove_leading_zeros_from_numeric_strings(
        in_df.astype(str), "generator_id"
    )
    assert_frame_equal(out_df, expected_df)


def test_convert_df_to_excel_file():
    """Test converting a dataframe into a pandas ExcelFile."""
    in_df = pd.DataFrame([[1, 2], [1, 2]])
    expected_df = pd.DataFrame([[1, 2], [1, 2]])

    out_excel_file = convert_df_to_excel_file(in_df, index=False)
    out_df = pd.read_excel(out_excel_file)

    assert_frame_equal(out_df, expected_df)


@pytest.mark.parametrize(
    "df,n_digits",
    [
        (
            pd.DataFrame(
                [
                    (512, "512"),
                    (5, "005"),
                    (5.0, "005"),
                    (5.00, "005"),
                    ("5.0", "005"),
                    ("5.", "005"),
                    ("005", "005"),
                    (0, pd.NA),
                    (-5, pd.NA),
                    ("000", pd.NA),
                    ("5000", pd.NA),
                    ("IMP", pd.NA),
                    ("I9P", pd.NA),
                    ("", pd.NA),
                    ("nan", pd.NA),
                    (np.nan, pd.NA),
                ],
                columns=["input", "expected"],
            ).convert_dtypes(),
            3,
        ),
        (
            pd.DataFrame(
                [
                    (93657, "93657"),
                    (93657.0, "93657"),
                    ("93657.0", "93657"),
                    (9365.7, "09365"),
                    (9365, "09365"),
                    ("936S7", pd.NA),
                    ("80302-7509", pd.NA),
                    ("B2A X19", pd.NA),
                    ("", pd.NA),
                    ("nan", pd.NA),
                    (np.nan, pd.NA),
                ],
                columns=["input", "expected"],
            ).convert_dtypes(),
            5,
        ),
    ],
)
def test_zero_pad_numeric_string(df, n_digits):
    """Test zero-padding of numeric codes like ZIP and FIPS."""
    output = zero_pad_numeric_string(df.input, n_digits)
    assert_series_equal(
        output,
        df.expected,
        check_names=False,
    )
    # Make sure all outputs are the right length
    assert (output.str.len() == n_digits).all()
    # Make sure all outputs are entirely numeric
    assert output.str.match(f"^[\\d]{{{n_digits}}}$").all()


def test_flatten_strings():
    """Test if :func:`flatten_list` can flatten an arbitraty list of strings."""
    lista = ["aa", "b", ["cc", ["d", "e"]], ["fff"]]
    assert list(flatten_list(lista)) == ["aa", "b", "cc", "d", "e", "fff"]


def test_flatten_ints():
    """Test if :func:`flatten_list` can flatten an arbitraty list of ints."""
    list1 = [1, 2, [3, [4, 5]], [[6]]]
    assert list(flatten_list(list1)) == [1, 2, 3, 4, 5, 6]


def test_flatten_mix_types():
    """Test if :func:`flatten_list` can flatten an arbitraty list of ints."""
    list1a = ["1", 22, ["333", [4, "5"]], [[666]]]
    assert list(flatten_list(list1a)) == ["1", 22, "333", 4, "5", 666]


def test_sql_asset_factory_missing_file():
    """Test sql_asset_factory throws a file not found error if file doesn't exist for an
    asset name."""
    with pytest.raises(FileNotFoundError):
        sql_asset_factory(name="fake_view")()


@pytest.mark.parametrize(
    "df",
    [
        pytest.param(pd.DataFrame({"col1": ["A", "B"], "col2": [1, 2]})),
        pytest.param(
            pd.DataFrame({"col1": ["A", "B", "C"], "col2": [1, 2, 3]}),
            marks=pytest.mark.xfail,
        ),
    ],
)
def test_convert_col_to_bool(df):
    true_values = ["A"]
    false_values = ["B"]
    df_bool = convert_col_to_bool(
        df, col_name="col1", true_values=true_values, false_values=false_values
    )
    assert len(df) == len(df_bool)
    assert df_bool["col1"].dtype == "boolean"
    df_compare = pd.merge(df, df_bool, right_index=True, left_index=True)
    assert (
        df_compare.loc[df_compare["col1_x"].isin(true_values)]["col1_y"]
        .isin([True])
        .all()
    )
    assert (
        df_compare.loc[~df_compare["col1_x"].isin(true_values)]["col1_y"]
        .isin([False, np.nan])
        .all()
    )


def test_diff_wide_tables():
    # has 2020-2021 data for utils 1 and 2; fact 2 for utility 1 just never reported
    old = pd.DataFrame.from_records(
        [
            {"u_id": 1, "year": 2020, "fact1": "u1f1y20"},
            {"u_id": 1, "year": 2021, "fact1": "u1f1y21"},
            {"u_id": 2, "year": 2020, "fact1": "u2f1y20", "fact2": "u2f2y20"},
            {"u_id": 2, "year": 2021, "fact1": "u2f1y21", "fact2": "u2f2y21"},
        ]
    )

    # has 2020-2022 data for utils 1 and 2, but:
    # - utility 1 is missing 2020 data for fact 1 and fact 2; otherwise, just missing fact 2 as usual
    # - utility 2 has an updated value for 2021 fact 1
    new = pd.DataFrame.from_records(
        [
            {"u_id": 1, "year": 2020},
            {"u_id": 1, "year": 2021, "fact1": "u1f1y21"},
            {"u_id": 1, "year": 2022, "fact1": "u1f1y22"},
            {"u_id": 2, "year": 2020, "fact1": "u2f1y20", "fact2": "u2f2y20"},
            {"u_id": 2, "year": 2021, "fact1": "u2f1y21_updated", "fact2": "u2f2y21"},
            {"u_id": 2, "year": 2022, "fact1": "u2f1y22", "fact2": "u2f2y22"},
        ]
    )

    empty_diff = diff_wide_tables(primary_key=["u_id", "year"], old=old, new=old)
    assert empty_diff.added.empty
    assert empty_diff.deleted.empty
    assert empty_diff.changed.empty

    def assert_diff_equal(observed, expected):
        observed_reshaped = observed.droplevel(level=0, axis="columns")
        expected_reshaped = expected.set_index(observed_reshaped.index.names)
        assert_frame_equal(observed_reshaped, expected_reshaped)

    diff_output = diff_wide_tables(primary_key=["u_id", "year"], old=old, new=new)

    expected_deleted = pd.DataFrame.from_records(
        [{"u_id": 1, "year": 2020, "field": "fact1", "old": "u1f1y20", "new": None}]
    )
    assert_diff_equal(diff_output.deleted, expected_deleted)

    expected_added = pd.DataFrame.from_records(
        [
            {"u_id": 1, "year": 2022, "field": "fact1", "old": None, "new": "u1f1y22"},
            {"u_id": 2, "year": 2022, "field": "fact1", "old": None, "new": "u2f1y22"},
            {"u_id": 2, "year": 2022, "field": "fact2", "old": None, "new": "u2f2y22"},
        ]
    )
    assert_diff_equal(diff_output.added, expected_added)

    expected_changed = pd.DataFrame.from_records(
        [
            {
                "u_id": 2,
                "year": 2021,
                "field": "fact1",
                "old": "u2f1y21",
                "new": "u2f1y21_updated",
            }
        ]
    )
    assert_diff_equal(diff_output.changed, expected_changed)


def test_dedupe_drop_na():
    df = pd.DataFrame(
        {
            "pk_1": [1, 2, 3, 3],
            "pk_2": ["a", "b", "c", "c"],
            "val_1": [11, 12, 13, pd.NA],
            "val_2": [21, 22, pd.NA, 23],
        }
    )
    deduped = dedupe_and_drop_nas(df, ["pk_1", "pk_2"])
    assert deduped.iloc[2]["val_1"] == 13
    assert deduped.iloc[2]["val_2"] == 23
    assert len(deduped.index) == 3


def test_standardize_percentages_ratio():
    date_df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "report_date": pd.to_datetime(
                ["1995", "1997-01-01", "1997-03-01", "1998"], format="mixed"
            ),
            "mixed_col": [10, 10, 20, 0.1],
        }
    )
    standardized = standardize_percentages_ratio(
        date_df, mixed_cols=["mixed_col"], years_to_standardize=[1995, 1996, 1997]
    )
    standardized_expected = date_df
    standardized_expected["mixed_col"] = [0.1, 0.1, 0.2, 0.1]
    assert_frame_equal(standardized, standardized_expected)

    year_df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "report_year": [1995, 1996, 1997, 1998],
            "mixed_col": [10, 15, 1.0, 0.1],
        }
    )
    standardized = standardize_percentages_ratio(
        year_df, mixed_cols=["mixed_col"], years_to_standardize=[1995, 1996]
    )
    standardized_expected = year_df
    standardized_expected["mixed_col"] = [0.1, 0.15, 1.0, 0.1]
    assert_frame_equal(standardized, standardized_expected)

    junk_df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "report_date": [1995, 1996, 1997, 1998],
            "mixed_col": ["hello", "data", "junk", "nope"],
        }
    )
    with pytest.raises(AssertionError):
        standardized = standardize_percentages_ratio(
            junk_df, mixed_cols=["mixed_col"], years_to_standardize=[1995, 1996]
        )

    over_100_df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "report_date": pd.to_datetime(
                ["1995", "1996-01-01", "1997-03-01", "1998"], format="mixed"
            ),
            "mixed_col": [101, 102, 20, 0.1],
        }
    )
    with pytest.raises(AssertionError):
        standardized = standardize_percentages_ratio(
            over_100_df, mixed_cols=["mixed_col"], years_to_standardize=[1995, 1996]
        )


def test_retry(mocker):
    func = mocker.MagicMock(side_effect=[RuntimeError, RuntimeError, RuntimeError, 1])
    sleep_mock = mocker.MagicMock()
    with mocker.patch("time.sleep", sleep_mock):
        assert retry(func=func, retry_on=(RuntimeError,)) == 1

    assert sleep_mock.call_count == 3
    sleep_mock.assert_has_calls([mocker.call(2**x) for x in range(3)])


def test_generate_rolling_avg():
    # I put the og values and the rolled values in the same starting
    # df bc less duplication and its easier to see imo
    test_rolled = pd.read_csv(
        StringIO(
            """
plant_id_eia,energy_source_code,report_date,fuel_cost_per_mmbtu,fuel_cost_per_mmbtu_rolling
8102,BIT,2023-01-01,2.61,
8102,BIT,2023-02-01,2.63,2.6
8102,BIT,2023-03-01,2.56,2.6
8102,BIT,2023-04-01,,2.6
8102,BIT,2023-05-01,2.59,2.62
8102,BIT,2023-06-01,2.5,2.64
8102,BIT,2023-07-01,2.69,2.68
8102,BIT,2023-08-01,2.76,2.72
8102,BIT,2023-09-01,2.78,2.76
8102,BIT,2023-10-01,2.88,2.8
8102,BIT,2023-11-01,3.0,2.83
8102,BIT,2023-12-01,2.89,2.86
8102,BIT,2024-01-01,,2.88
8102,BIT,2024-02-01,,
8102,DFO,2023-01-01,12.1,
8102,DFO,2023-02-01,13.75,13.60
8102,DFO,2023-03-01,,14.09
8102,DFO,2023-05-01,12.49,14.49
8102,DFO,2023-06-01,15.11,14.92
8102,DFO,2023-08-01,16.78,15.44
8102,DFO,2023-09-01,19.23,16.02
8102,DFO,2023-11-01,,
8102,DFO,2024-02-01,,
"""
        )
    ).astype({"report_date": "datetime64[ns]"})
    out = pudl.helpers.generate_rolling_avg(
        test_rolled.drop(columns=["fuel_cost_per_mmbtu_rolling"]),
        group_cols=["plant_id_eia", "energy_source_code"],
        data_col="fuel_cost_per_mmbtu",
        window=12,
        min_periods=6,
        win_type="triang",
    ).round(2)
    pd.testing.assert_frame_equal(test_rolled, out, check_exact=False)
    # reorder the input df using sample to make sure this works
    # no matter how the input df is sorted
    out_reordered = pudl.helpers.generate_rolling_avg(
        test_rolled.sample(frac=1).drop(columns=["fuel_cost_per_mmbtu_rolling"]),
        group_cols=["plant_id_eia", "energy_source_code"],
        data_col="fuel_cost_per_mmbtu",
        window=12,
        min_periods=6,
        win_type="triang",
    ).round(2)
    pd.testing.assert_frame_equal(test_rolled, out_reordered, check_exact=False)


def test_add_fips_ids():
    geocodes = pd.read_csv(
        StringIO(
            """
area_name,county_id_fips,fips_level,report_year,state_id_fips,state
Maryland,00000,040,2023,24,MD
Virginia,00000,040,2023,51,VA
Baltimore County,24005,050,2023,24,MD
Baltimore city,24510,050,2023,24,MD
Bedford County,51019,050,2023,51,VA
Bedford city,51515,050,2009,51,VA"""
        ),
        dtype=pd.StringDtype(),
    )
    df = pd.read_csv(
        StringIO(
            """
county,state
Baltimore County,MD
Baltimore city,MD
Baltimore,MD
Bedford County,VA
Bedford,VA
Bedford city,VA"""
        ),
        dtype=pd.StringDtype(),
    )
    expected = pd.read_csv(
        StringIO(
            """
county,state,state_id_fips,county_id_fips
Baltimore County,MD,24,24005
Baltimore city,MD,24,24510
Baltimore,MD,24,24510
Bedford County,VA,51,51019
Bedford,VA,51,51019
Bedford city,VA,51,51515"""
        ),
        dtype=pd.StringDtype(),
    )
    out = add_fips_ids(df, geocodes)
    pd.testing.assert_frame_equal(out, expected)
