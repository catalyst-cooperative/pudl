"""Unit tests for the pudl.transform.vcerare module."""

import pandas as pd

import pudl.transform.vcerare as vcerare


def test_standardize_census_names():
    """Make sure that census names are correctly standardized."""
    # Testing the following cases:
    # 1) rename
    # 2) no rename because the match is identical
    # 3) no rename because there's no match in the census data
    census_test_df = pd.DataFrame(
        {
            "county_id_fips": ["01001", "04002", "54003", "16004", "09025"],
            "area_name": [
                "St. John's Parish",  # Check overwrite
                "Example County",  # Check overwrite
                "Correctly Named County",  # Check overwrite
                "Real Fake",  # Check non-overwrite of identical county
                "Irrelevant County",  # Check that other census data isn't joined
            ],
            "state": ["AL", "AZ", "WV", "ID", "PR"],
        }
    )
    vce_fips_test_df = pd.DataFrame(
        {
            "FIPS": ["01001", "04002", "54003", "16004", "25005"],
            "county_state_names": [
                "saint_johns_alabama",
                "example_bad_arizona",
                "incorrectly_named_west_virginia",
                "real_fake_idaho",
                "another_massachusetts",
            ],
            "lat county": [1.1, 1.2, 1.3, 1.4, 1.5],
            "long county": [0.0, 0.1, 0.2, 0.3, 0.4],
        }
    )
    # The standardize_census_names function should overwrite the place name where the
    # census data contains county or parish data.
    expected_df = pd.DataFrame(
        {
            "county_state_names": pd.Categorical(
                values=[
                    "st. john's_AL",
                    "example_AZ",
                    "correctly named_WV",
                    "real fake_ID",
                    "another_MA",
                ]
            ),
            "latitude": [1.1, 1.2, 1.3, 1.4, 1.5],
            "longitude": [0.0, 0.1, 0.2, 0.3, 0.4],
            "county_id_fips": ["01001", "04002", "54003", "16004", "25005"],
            "place_name": pd.Categorical(
                [
                    "st. john's",
                    "example",
                    "correctly named",
                    "real fake",
                    "another",
                ]
            ),
            "state": pd.Categorical(["AL", "AZ", "WV", "ID", "MA"]),
        }
    )

    expected_column_map = {
        "saint_johns_alabama": "st. john's_AL",
        "example_bad_arizona": "example_AZ",
        "incorrectly_named_west_virginia": "correctly named_WV",
        "real_fake_idaho": "real fake_ID",
        "another_massachusetts": "another_MA",
    }

    vce_fips_test_df = vcerare._prep_lat_long_fips_df(vce_fips_test_df)
    actual_df, column_maps = vcerare._standardize_census_names(
        vce_fips_test_df, census_test_df
    )

    # Ignore ordering of categoricals
    pd.testing.assert_frame_equal(
        expected_df, actual_df, check_dtype=False, check_categorical=False
    )

    assert column_maps == expected_column_map
