"""Unit tests for the pudl.transform.vcerare module."""

import pandas as pd

import pudl.transform.vcerare as vcerare


def test_standardize_census_names():
    """Make sure that census names are correctly standardized."""
    # The test df includes a value for plant_id_epa whose value for plant_id_eia
    # is different. Because the crosswalk gets trancated for the short tests based
    # on available plant/gen and plant/boiler keys, not all of the plant ids are
    # included. I had to find an example (2713-->58697) that would exist in the
    # version of the crosswalk created by the fast etl (i.e. the last few years).
    census_test_df = pd.DataFrame(
        {
            "county_id_fips": ["00001", "00002", "00003", "00004"],
            "area_name": [
                "St. John's Parish",
                "Example County",
                "Correctly Named County",
                "Fake City",
            ],
        }
    )
    combined_test_df = pd.DataFrame(
        {
            "county_id_fips": ["00001", "00002", "00003", "00004", "00005"],
            "place_name": [
                "saint johns",
                "example_bad",
                "incorrectly_named",
                "real fake",
                "another",
            ],
            "solar_pv_capacity": [0.1, 0.2, 0.3, 0.4, 0.5],
        }
    )
    # The standardize_census_names function should overwrite the place name where the
    # census data contains county or parish data.
    expected_df = pd.DataFrame(
        {
            "county_id_fips": ["00001", "00002", "00003", "00004", "00005"],
            "place_name": pd.Categorical(
                [
                    "st. john's",
                    "example",
                    "correctly named",
                    "real fake",
                    "another",
                ]
            ),
            "solar_pv_capacity": [0.1, 0.2, 0.3, 0.4, 0.5],
        }
    )
    actual_df = vcerare._standardize_census_names(combined_test_df, census_test_df)
    pd.testing.assert_frame_equal(expected_df, actual_df, check_dtype=False)
