"""Unit tests for the pudl.transform.epacems module."""

import pandas as pd

import pudl.transform.epacems as epacems


def test_harmonize_eia_epa_orispl():
    """Make sure that incorrect EPA ORISPL codes are fixed."""
    # The test df includes a value for plant_id_epa whose value for plant_id_eia
    # is different. Because the crosswalk gets trancated for the short tests based
    # on available plant/gen and plant/boiler keys, not all of the plant ids are
    # included. I had to find an example (2713-->58697) that would exist in the
    # version of the crosswalk created by the fast etl (i.e. the last few years).
    cems_test_df = pd.DataFrame(
        {
            "plant_id_epa": [2713, 3, 10, 1111],
            "emissions_unit_id_epa": ["01A", "1", "2", "no-match"],
        }
    )
    crosswalk_test_df = pd.DataFrame(
        {
            "plant_id_epa": [2713, 3, 10],
            "plant_id_eia": [58697, 3, 10],
            "emissions_unit_id_epa": ["01A", "1", "2"],
        }
    )
    # The harmonize_eia_epa_orispl function should create a new column for the
    # official plant_id_eia values and the combined id values for instances
    # where there is no match for the the plant_id_epa/emissions_unit_id_epa
    # combination.
    expected_df = pd.DataFrame(
        {
            "plant_id_epa": [2713, 3, 10, 1111],
            "emissions_unit_id_epa": ["01A", "1", "2", "no-match"],
            "plant_id_eia": [58697, 3, 10, 1111],
        }
    )
    actual_df = epacems.harmonize_eia_epa_orispl(cems_test_df, crosswalk_test_df)
    pd.testing.assert_frame_equal(expected_df, actual_df, check_dtype=False)
