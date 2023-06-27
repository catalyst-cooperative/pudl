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
plant_id_eia,generator_id,unit_id_pudl,boiler_id
1392,1A,1,1A
1392,2A,1,2A
1392,3A,1,3A
1,A,1,
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
plant_id_eia,plant_id_epa,unit_id_pudl,emissions_unit_id_epa,generator_id,boiler_id,subplant_id
1392,1392,1,1A,1A,1A,0
1392,1392,1,2A,2A,2A,0
1392,1392,1,3A,3A,3A,0
1392,1392,,4A,4A,,1
1392,1392,,5A,5A,,2
1,1,1,A,A,,0
"""
        )
    )

    epacamd_eia_subplant_ids_got = glue_assets.epacamd_eia_subplant_ids(
        epacamd_eia_unique=epacamd_eia_test,
        generators_eia860=generators_entity_eia_test,
        emissions_unit_ids_epacems=emissions_unit_ids_epacems_test,
        boiler_generator_assn_eia860=boiler_generator_assn_eia860_test,
    )

    pd.testing.assert_frame_equal(
        epacamd_eia_subplant_ids_expected,
        epacamd_eia_subplant_ids_got[epacamd_eia_subplant_ids_expected.columns],
        check_dtype=False,
    )
