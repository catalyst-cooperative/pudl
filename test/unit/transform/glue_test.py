"""Tests for Glue functions."""

from io import StringIO

import pandas as pd

import pudl.etl.glue_assets as glue_assets


def test_epacamd_eia_subplant_ids():
    """Ensure subplant_id gets applied appropriately to example plants."""
    epacamd_eia_test = pd.read_csv(
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
    )
    expected = epacamd_eia_subplant_ids_expected.convert_dtypes()
    actual = glue_assets.core_epa__assn_eia_epacamd_subplant_ids(
        _core_epa__assn_eia_epacamd_unique=epacamd_eia_test,
        core_eia860__scd_generators=generators_entity_eia_test,
        _core_epacems__emissions_unit_ids=emissions_unit_ids_epacems_test,
        core_eia860__assn_boiler_generator=boiler_generator_assn_eia860_test,
    )[expected.columns].convert_dtypes()
    crosswalk_index = [
        "plant_id_eia",
        "plant_id_epa",
        "unit_id_pudl",
        "emissions_unit_id_epa",
        "generator_id",
    ]
    pd.testing.assert_frame_equal(
        expected.set_index(crosswalk_index),
        actual.set_index(crosswalk_index),
        check_like=True,
    )
