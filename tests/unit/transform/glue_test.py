"""Tests for Glue functions."""

from io import StringIO

import pandas as pd

import pudl.dagster.assets.core.glue as glue_assets


def _make_subplant_ids(
    crosswalk_rows: list[dict],
    generator_rows: list[dict],
    emissions_unit_rows: list[dict],
    bga_rows: list[dict],
) -> pd.DataFrame:
    """Run core_epa__assn_eia_epacamd_subplant_ids on hand-built minimal inputs.

    Each argument is a list of row dicts for one of the four input tables, using
    only the columns that function actually reads:

    * crosswalk_rows: plant_id_epa, emissions_unit_id_epa, generator_id_epa,
      plant_id_eia, boiler_id, generator_id (mirrors ``_core_epa__assn_eia_epacamd_unique``)
    * generator_rows: plant_id_eia, generator_id (mirrors ``core_eia860__scd_generators``)
    * emissions_unit_rows: plant_id_eia, emissions_unit_id_epa (mirrors
      ``_core_epacems__emissions_unit_ids``)
    * bga_rows: plant_id_eia, generator_id, unit_id_pudl (mirrors
      ``core_eia860__assn_boiler_generator``)
    """
    crosswalk_columns = [
        "plant_id_epa",
        "emissions_unit_id_epa",
        "generator_id_epa",
        "plant_id_eia",
        "boiler_id",
        "generator_id",
    ]
    # dg.asset-decorated functions type-check as returning `object`, not their
    # declared return type, when called directly.
    return glue_assets.core_epa__assn_eia_epacamd_subplant_ids(  # type: ignore[bad-return]
        _core_epa__assn_eia_epacamd_unique=pd.DataFrame(
            crosswalk_rows, columns=crosswalk_columns
        ),
        core_eia860__scd_generators=pd.DataFrame(
            generator_rows, columns=["plant_id_eia", "generator_id"]
        ),
        _core_epacems__emissions_unit_ids=pd.DataFrame(
            emissions_unit_rows, columns=["plant_id_eia", "emissions_unit_id_epa"]
        ),
        core_eia860__assn_boiler_generator=pd.DataFrame(
            bga_rows, columns=["plant_id_eia", "generator_id", "unit_id_pudl"]
        ),
    )


def test_every_eia_generator_appears_with_a_subplant_id():
    """Every (plant_id_eia, generator_id) known to EIA-860 should get a subplant_id.

    This covers generators that never show up in the EPA crosswalk or CEMS at all
    (e.g. renewables that don't report to CEMS, or generators the crosswalk simply
    missed), which are only introduced into the pipeline via the outer merge in
    ``augment_crosswalk_with_generators_eia860``.
    """
    generator_rows = [
        {"plant_id_eia": 1392, "generator_id": "1A"},
        {"plant_id_eia": 1392, "generator_id": "2A"},
        # This generator never appears in the crosswalk, CEMS, or BGA inputs at all.
        {"plant_id_eia": 1392, "generator_id": "SOLAR1"},
    ]
    crosswalk_rows = [
        {
            "plant_id_epa": 1392,
            "emissions_unit_id_epa": "1A",
            "generator_id_epa": "1A",
            "plant_id_eia": 1392,
            "boiler_id": "1A",
            "generator_id": "1A",
        },
        {
            "plant_id_epa": 1392,
            "emissions_unit_id_epa": "2A",
            "generator_id_epa": "2A",
            "plant_id_eia": 1392,
            "boiler_id": "2A",
            "generator_id": "2A",
        },
    ]
    emissions_unit_rows = [
        {"plant_id_eia": 1392, "emissions_unit_id_epa": "1A"},
        {"plant_id_eia": 1392, "emissions_unit_id_epa": "2A"},
    ]
    bga_rows = [
        {"plant_id_eia": 1392, "generator_id": "1A", "unit_id_pudl": 1},
        {"plant_id_eia": 1392, "generator_id": "2A", "unit_id_pudl": 1},
    ]

    actual = _make_subplant_ids(
        crosswalk_rows, generator_rows, emissions_unit_rows, bga_rows
    )

    for generator_row in generator_rows:
        matches = actual[
            (actual.plant_id_eia == generator_row["plant_id_eia"])
            & (actual.generator_id == generator_row["generator_id"])
        ]
        assert not matches.empty, (
            f"Generator {generator_row} is missing from the subplant ID table entirely."
        )
        assert matches.subplant_id.notna().all(), (
            f"Generator {generator_row} appears with a null subplant_id: \n{matches}"
        )


def test_every_eia_generator_gets_exactly_one_subplant_id():
    """Every (plant_id_eia, generator_id) should map to exactly one subplant_id.

    A generator that reports emissions through more than one EPA CAMD unit (e.g. it
    feeds two separate smokestacks) appears on more than one row of the subplant ID
    table -- one per emissions_unit_id_epa it's matched to. Those rows must all agree
    on subplant_id, since it's the same physical generator.
    """
    generator_rows = [
        {"plant_id_eia": 5000, "generator_id": "G1"},
        {"plant_id_eia": 5000, "generator_id": "G2"},
    ]
    crosswalk_rows = [
        # G1 reports through two different EPA units.
        {
            "plant_id_epa": 5000,
            "emissions_unit_id_epa": "U1",
            "generator_id_epa": "G1",
            "plant_id_eia": 5000,
            "boiler_id": "U1",
            "generator_id": "G1",
        },
        {
            "plant_id_epa": 5000,
            "emissions_unit_id_epa": "U2",
            "generator_id_epa": "G1",
            "plant_id_eia": 5000,
            "boiler_id": "U2",
            "generator_id": "G1",
        },
        {
            "plant_id_epa": 5000,
            "emissions_unit_id_epa": "U3",
            "generator_id_epa": "G2",
            "plant_id_eia": 5000,
            "boiler_id": "U3",
            "generator_id": "G2",
        },
    ]
    emissions_unit_rows = [
        {"plant_id_eia": 5000, "emissions_unit_id_epa": "U1"},
        {"plant_id_eia": 5000, "emissions_unit_id_epa": "U2"},
        {"plant_id_eia": 5000, "emissions_unit_id_epa": "U3"},
    ]
    bga_rows = [
        {"plant_id_eia": 5000, "generator_id": "G1", "unit_id_pudl": 1},
        {"plant_id_eia": 5000, "generator_id": "G2", "unit_id_pudl": 2},
    ]

    actual = _make_subplant_ids(
        crosswalk_rows, generator_rows, emissions_unit_rows, bga_rows
    )

    for generator_row in generator_rows:
        matches = actual[
            (actual.plant_id_eia == generator_row["plant_id_eia"])
            & (actual.generator_id == generator_row["generator_id"])
        ]
        subplant_ids = matches.subplant_id.to_numpy()
        assert (subplant_ids == subplant_ids[0]).all(), (
            f"Generator {generator_row} maps to more than one subplant_id: \n{matches}"
        )


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
1392,1392,1,1A,1A,1
1392,1392,1,2A,2A,1
1392,1392,1,3A,3A,1
1392,1392,,4A,4A,2
1392,1392,,5A,5A,3
1,1,1,A,A,1
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
