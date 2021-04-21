"""Unit tests for pudl.transform.eia module."""
import pandas as pd
from pandas.testing import assert_frame_equal

import pudl


def test_convert_to_date():
    """Test automated cleanup of EIA date columns."""
    data = [
        (2019, 3, 14),
        ("2019", "03", "14"),
    ]
    in_df = pd.DataFrame.from_records(
        data, columns=["report_year", "report_month", "report_day"]
    )
    expected_df = pd.DataFrame({
        "report_date": pd.to_datetime([
            "2019-03-14",
            "2019-03-14",
        ]),
    })
    out_df = pudl.helpers.convert_to_date(in_df)
    assert_frame_equal(out_df, expected_df)


def test_fix_eia_na():
    """Test cleanup of bad EIA spreadsheet NA values."""
    in_df = pd.DataFrame({
        "vals": [
            "",
            " ",
            "\t",
            ".",
            ".0",  # Should only replace naked decimals
            "..",  # Only single naked decimals?
            "  ",  # 2 spaces -- we only replace single whitespace chars?
            "\t\t",  # 2 tabs -- we only replace single whitespace chars?
        ]
    })
    expected_df = pd.DataFrame({
        "vals": [
            pd.NA,
            pd.NA,
            pd.NA,
            pd.NA,
            ".0",
            "..",
            "  ",
            "\t\t",
        ]
    })
    out_df = pudl.helpers.fix_eia_na(in_df)
    assert_frame_equal(out_df, expected_df)


def test_fix_leading_zero_gen_ids():
    """Test removal of leading zeroes from EIA generator IDs."""
    in_df = pd.DataFrame({
        "generator_id": [
            "0001",   # Leading zeroes, all numeric string.
            "26",     # An appropriate numeric string w/o leading zeroes.
            100,      # Integer, should get stringified.
            100.0,    # What happens if it's a float?
            "01-A",   # Leading zeroes, alphanumeric. Should not change.
            "HRSG-01",   # Alphanumeric, should be no change.
        ]
    })
    expected_df = pd.DataFrame({
        "generator_id": [
            "1",
            "26",
            "100",
            "100.0",
            "01-A",
            "HRSG-01",
        ]
    })
    out_df = pudl.helpers.fix_leading_zero_gen_ids(in_df)
    assert_frame_equal(out_df, expected_df)
