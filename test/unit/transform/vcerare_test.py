"""Unit tests for the pudl.transform.vcerare module."""

import pandas as pd

import pudl.transform.vcerare as vcerare


def test_standardize_census_names():
    """Make sure that census names are correctly standardized."""
    # Testing the following cases:
    # 1) rename
    # 2) no rename because the census data row is for a city
    # 3) no rename because there's no match in the census data
    census_test_df = pd.DataFrame(
        {
            "county_id_fips": ["00001", "00002", "00003", "00004"],
            "area_name": [
                "St. John's Parish",
                "Example County",
                "Correctly Named County",
                "Fake City",
            ],
            "state": ["AL", "AZ", "WV", "ID"],
        }
    )
    fips_test_df = pd.DataFrame(
        {
            "county_id_fips": ["00001", "00002", "00003", "00004", "00005"],
            "place_name": [
                "saint_johns",
                "example_bad",
                "incorrectly_named",
                "real_fake",
                "another",
            ],
            "county_state_names": [
                "saint_johns_alabama",
                "example_bad_arizona",
                "incorrectly_named_west_virginia",
                "real_fake_idaho",
                "another_massachusetts",
            ],
            "solar_pv_capacity": [0.1, 0.2, 0.3, 0.4, 0.5],
            "state": ["AL", "AZ", "WV", "ID", "MA"],
        }
    )
    # The standardize_census_names function should overwrite the place name where the
    # census data contains county or parish data.
    expected_df = pd.DataFrame(
        {
            "county_id_fips": ["00001", "00002", "00003", "00004", "00005"],
            "place_name_vcerare": [
                "saint johns",
                "example_bad",
                "incorrectly_named",
                "real fake",  # not the same as the original, but we never use this column again
                "another",
            ],
            "county_state_names_vcerare": [
                "saint_johns_alabama",
                "example_bad_arizona",
                "incorrectly_named_west_virginia",
                "real_fake_idaho",
                "another_massachusetts",
            ],
            "solar_pv_capacity": [0.1, 0.2, 0.3, 0.4, 0.5],
            "state": ["AL", "AZ", "WV", "ID", "MA"],
            "place_name": [
                "st. john's",
                "example",
                "correctly named",
                "fake",
                "another",
            ],
            "county_state_names": [
                "st. john's_AL",
                "example_AZ",
                "correctly named_WV",
                "real fake_ID",
                "another_MA",
            ],
        }
    )

    actual_df, column_maps = vcerare._standardize_census_names(
        fips_test_df, census_test_df
    )
    pd.testing.assert_frame_equal(expected_df, actual_df, check_dtype=False)
