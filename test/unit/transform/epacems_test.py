"""Unit tests for the pudl.transform.epacems module."""
from io import StringIO

import pandas as pd

import pudl.etl.epacems_assets as epacems_assets
import pudl.transform.epacems as epacems
from pudl.helpers import convert_cols_dtypes


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


def test_epacamd_eia_subplant_ids():
    """Ensure subplant_id gets applied appropriately to example plants."""
    clean_epacamd_eia_test = pd.read_csv(
        StringIO(
            """
plant_id_epa,emissions_unit_id_epa,generator_id_epa,plant_id_eia,boiler_id,generator_id
1392,1A,1A,1392,1A,1A
1392,2A,2A,1392,2A,2A
1392,3A,3A,1392,3A,3A
1392,4A,4A,1392,,4A
1392,5A,5A,1392,,5A
1,A,A,1,,A
"""
        )
    )

    emissions_unit_ids_epacems_test = pd.read_csv(
        StringIO(
            """
plant_id_eia,emissions_unit_id_epa
1392,1A
1392,2A
1392,3A
1392,4A
1392,5A
1,A
"""
        )
    )

    boiler_generator_assn_eia860_test = pd.read_csv(
        StringIO(
            """
plant_id_eia,generator_id,unit_id_pudl
1392,1A,1
1392,2A,1
1392,3A,1
1,A,1
"""
        )
    )

    generators_entity_eia_test = pd.read_csv(
        StringIO(
            """
plant_id_eia,generator_id
1392,1A
1392,2A
1392,3A
1392,4A
1392,5A
1,A
"""
        )
    )
    epacamd_eia_subplant_ids_expected = pd.read_csv(
        StringIO(
            """
plant_id_eia,plant_id_epa,unit_id_pudl,emissions_unit_id_epa,generator_id,subplant_id
1392,1392,1,1A,1A,0
1392,1392,1,2A,2A,0
1392,1392,1,3A,3A,0
1392,1392,,4A,4A,1
1392,1392,,5A,5A,2
1,1,1,A,A,0
"""
        )
    ).pipe(convert_cols_dtypes, "eia")

    epacamd_eia_subplant_ids_got = epacems_assets.epacamd_eia_subplant_ids(
        clean_epacamd_eia=clean_epacamd_eia_test,
        generators_eia860=generators_entity_eia_test,
        emissions_unit_ids_epacems=emissions_unit_ids_epacems_test,
        boiler_generator_assn_eia860=boiler_generator_assn_eia860_test,
    )

    pd.testing.assert_frame_equal(
        epacamd_eia_subplant_ids_expected,
        epacamd_eia_subplant_ids_got[epacamd_eia_subplant_ids_expected.columns],
    )
