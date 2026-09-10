"""Unit tests for the pudl.transform.vcerare module."""

import datetime

import pandas as pd
import polars as pl
import pytest

import pudl.transform.vcerare as vcerare


def test_standardize_census_names():
    """Make sure that census names are correctly standardized."""
    # Testing the following cases:
    # 1) rename
    # 2) no rename because the match is identical
    # 3) no rename because there's no match in the census data
    census_test_df = pd.DataFrame(
        {
            "county_id_fips": ["01001", "04002", "04003", "54003", "16004", "09025"],
            "area_name": [
                "St. John's Parish",  # Check overwrite
                "Totally Different St. John's",  # Check overwrite with same VCE name, different state
                "Example County",  # Check overwrite
                "Correctly Named County",  # Check overwrite
                "Real Fake",  # Check non-overwrite of identical county
                "Irrelevant County",  # Check that other census data isn't joined
            ],
            "state": ["AL", "AZ", "AZ", "WV", "ID", "PR"],
        }
    )
    vce_fips_test_df = pd.DataFrame(
        {
            "FIPS": ["01001", "04002", "04003", "54003", "16004", "25005"],
            "county_state_names": [
                "saint_johns_alabama",
                "saint_johns_arizona",
                "example_bad_arizona",
                "incorrectly_named_west_virginia",
                "real_fake_idaho",
                "another_massachusetts",  # Check what happens when there's no corresponding Census data
            ],
            "lat county": [1.1, 1.2, 1.2, 1.3, 1.4, 1.5],
            "long county": [0.0, 0.1, 0.1, 0.2, 0.3, 0.4],
        }
    )
    # The standardize_census_names function should overwrite the place name where the
    # census data contains county or parish data.
    expected_df = pd.DataFrame(
        {
            "county_state_names": pd.Categorical(
                values=[
                    "saint_johns_alabama",
                    "saint_johns_arizona",
                    "example_bad_arizona",
                    "incorrectly_named_west_virginia",
                    "real_fake_idaho",
                    "another_massachusetts",
                ]
            ),
            "latitude": [1.1, 1.2, 1.2, 1.3, 1.4, 1.5],
            "longitude": [0.0, 0.1, 0.1, 0.2, 0.3, 0.4],
            "county_id_fips": ["01001", "04002", "04003", "54003", "16004", "25005"],
            "state": pd.Categorical(["AL", "AZ", "AZ", "WV", "ID", "MA"]),
            "place_name": pd.Categorical(
                [
                    "st. john's",
                    "totally different st. john's",
                    "example",
                    "correctly named",
                    "real fake",
                    "another",
                ]
            ),
        }
    )

    vce_fips_test_df = vcerare._prep_lat_long_fips_df(vce_fips_test_df)
    actual_df = vcerare._standardize_census_names(vce_fips_test_df, census_test_df)

    # Ignore ordering of categoricals
    pd.testing.assert_frame_equal(
        expected_df, actual_df, check_dtype=False, check_categorical=False
    )


def _integer_hour_lf(report_year: int, n_hours: int) -> pl.LazyFrame:
    """Build a minimal capacity factor table with an integer hour_of_year column."""
    return pl.LazyFrame(
        {
            "hour_of_year": list(range(1, n_hours + 1)),
            "report_year": [report_year] * n_hours,
            "capacity_factor_solar_pv": [0.0] * n_hours,
        }
    )


def _datetime_hour_lf(report_year: int, n_hours: int) -> pl.LazyFrame:
    """Build a minimal capacity factor table with a datetime hour_of_year column."""
    start = datetime.datetime(report_year, 1, 1)
    return pl.LazyFrame(
        {
            "hour_of_year": [
                start + datetime.timedelta(hours=i) for i in range(n_hours)
            ],
            "report_year": [report_year] * n_hours,
            "capacity_factor_solar_pv": [0.0] * n_hours,
        }
    )


@pytest.mark.parametrize(
    ("lf", "year", "last_datetime"),
    [
        # Non-leap year, integer hours: Dec 31 is kept, nothing clipped.
        (
            _integer_hour_lf(2023, 8760),
            2023,
            datetime.datetime(2023, 12, 31, 23),
        ),
        # Leap year, integer hours already omitting Dec 31: nothing to clip.
        (
            _integer_hour_lf(2020, 8760),
            2020,
            datetime.datetime(2020, 12, 30, 23),
        ),
        # Leap year, datetime hours including Dec 31 (8784): Dec 31 is clipped.
        (
            _datetime_hour_lf(2024, 8784),
            2024,
            datetime.datetime(2024, 12, 30, 23),
        ),
        # Non-leap year, datetime hours (8760): nothing to clip.
        (
            _datetime_hour_lf(2025, 8760),
            2025,
            datetime.datetime(2025, 12, 31, 23),
        ),
    ],
)
def test_add_time_cols(lf, year, last_datetime):
    """_add_time_cols yields exactly 8760 hours per year across input formats."""
    result = vcerare._add_time_cols(lf, "solar_pv", year).collect()

    assert result.height == 8760
    assert result["hour_of_year"].min() == 1
    assert result["hour_of_year"].max() == 8760
    assert result["hour_of_year"].dtype == pl.Int32
    assert result["datetime_utc"].max() == last_datetime
