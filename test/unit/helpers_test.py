"""Unit tests for the :mod:`pudl.helpers` module."""

import os

import numpy as np
import pandas as pd
import pytest
from dagster import AssetKey
from dagster._config.errors import PostProcessingError
from pandas.testing import assert_frame_equal, assert_series_equal
from pandas.tseries.offsets import BYearEnd

import pudl
from pudl.helpers import (
    EnvVar,
    convert_df_to_excel_file,
    convert_to_date,
    date_merge,
    expand_timeseries,
    fix_eia_na,
    flatten_list,
    remove_leading_zeros_from_numeric_strings,
    zero_pad_numeric_string,
)

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

    out_expected_outer = pd.DataFrame(
        {
            "report_date": [
                "2019-12-01",
                "2020-10-01",
                "2019-01-01",
                "2019-06-01",
                "2018-07-01",
                "2020-01-01",
                "2018-01-01",
                "2020-01-01",
            ],
            "plant_id_eia": [2, 2, 3, 3, 3, 1, 2, 3],
            "prime_mover_code": ["HY", "ST", "HY", "CT", "HY", None, None, None],
            "fuel_consumed_units": [
                0.0,
                98085.0,
                0.0,
                4800000.0,
                0.0,
                None,
                None,
                None,
            ],
            "plant_name_eia": [
                "Bankhead Dam",
                "Bankhead",
                "Barry",
                "Barry",
                "Barry",
                "Sand Point",
                "Bankhead Dam",
                "Barry",
            ],
            "utility_id_eia": [195, 195, 16, 16, 16, 63560, 195, 16],
        }
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
            "data": [2, 1, 2, 3, 10, 2],
        }
    ).astype({"report_date": "datetime64[ns]"})

    expected_out_path = (
        test_dir / "data/date_merge_unit_test/timeseries_fillin_expected_out.csv"
    )
    expected_out = pd.read_csv(expected_out_path).astype(
        {"report_date": "datetime64[ns]", "data": "float64"}
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
                "2020-02-27",
                "2020-03-01",
            ],
            "plant_id_eia": [1, 1, 1, 2, 2],
            "generator_id": [1, 1, 2, 1, 1],
            "data": [1, 2, 1, 3, 4],
        }
    )

    expected_out_path = (
        test_dir
        / "data/date_merge_unit_test/timeseries_fillin_through_month_expected_out.csv"
    )
    expected_out = pd.read_csv(expected_out_path).astype(
        {"report_date": "datetime64[ns]", "data": "float64"}
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


def test_cems_selection():
    """Test CEMS asset selection remove cems assets."""
    cems_selection = pudl.etl.create_non_cems_selection(pudl.etl.default_assets)
    assert AssetKey("hourly_emissions_epacems") not in cems_selection.resolve(
        pudl.etl.default_assets
    ), "hourly_emissions_epacems or downstream asset present in selection."


def test_env_var():
    os.environ["_PUDL_TEST"] = "test value"
    env_var = EnvVar(env_var="_PUDL_TEST")
    assert env_var.post_process(None) == "test value"
    del os.environ["_PUDL_TEST"]


def test_env_var_reads_defaults(mocker):
    mocker.patch(
        "pudl.helpers.get_defaults",
        lambda: {"_PUDL_TEST": "test value default"},
    )
    env_var = EnvVar(env_var="_PUDL_TEST")
    assert env_var.post_process(None) == "test value default"


def test_env_var_missing_completely():
    with pytest.raises(PostProcessingError):
        EnvVar(env_var="_PUDL_BOGUS").post_process(None)
