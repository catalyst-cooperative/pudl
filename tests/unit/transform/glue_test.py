"""Tests for Glue functions."""

from io import StringIO

import pandas as pd
import pytest

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


def test_every_epa_emissions_unit_appears_with_a_subplant_id():
    """Every (plant_id_eia, emissions_unit_id_epa) known to CEMS should get a subplant_id.

    EPA's crosswalk is known to be incomplete: the docs for
    core_epa__assn_eia_epacamd_subplant_ids note that ~2% of EPA CAMD records never
    get matched to an EIA generator. Those unmatched units still report to CEMS and
    still need a subplant_id, and are only introduced into the pipeline via the outer
    merge in augment_crosswalk_with_epacamd_ids.
    """
    emissions_unit_rows = [
        {"plant_id_eia": 6000, "emissions_unit_id_epa": "U1"},
        # This EPA unit never matches any EIA generator in the crosswalk.
        {"plant_id_eia": 6000, "emissions_unit_id_epa": "U_ORPHAN"},
    ]
    crosswalk_rows = [
        {
            "plant_id_epa": 6000,
            "emissions_unit_id_epa": "U1",
            "generator_id_epa": "G1",
            "plant_id_eia": 6000,
            "boiler_id": "U1",
            "generator_id": "G1",
        },
    ]
    generator_rows = [
        {"plant_id_eia": 6000, "generator_id": "G1"},
    ]
    bga_rows = [
        {"plant_id_eia": 6000, "generator_id": "G1", "unit_id_pudl": 1},
    ]

    actual = _make_subplant_ids(
        crosswalk_rows, generator_rows, emissions_unit_rows, bga_rows
    )

    for emissions_unit_row in emissions_unit_rows:
        matches = actual[
            (actual.plant_id_eia == emissions_unit_row["plant_id_eia"])
            & (
                actual.emissions_unit_id_epa
                == emissions_unit_row["emissions_unit_id_epa"]
            )
        ]
        assert not matches.empty, (
            f"EPA unit {emissions_unit_row} is missing from the subplant ID table "
            "entirely."
        )
        assert matches.subplant_id.notna().all(), (
            f"EPA unit {emissions_unit_row} appears with a null subplant_id: \n{matches}"
        )


def test_every_epa_emissions_unit_gets_exactly_one_subplant_id():
    """Every (plant_id_eia, emissions_unit_id_epa) should map to exactly one subplant_id.

    An EPA unit that feeds more than one EIA generator (e.g. a single smokestack shared
    by two combustion turbines) appears on more than one row of the subplant ID table --
    one per generator_id it's matched to. Those rows must all agree on subplant_id,
    since it's the same physical EPA unit.
    """
    emissions_unit_rows = [
        {"plant_id_eia": 7000, "emissions_unit_id_epa": "U1"},
        {"plant_id_eia": 7000, "emissions_unit_id_epa": "U2"},
    ]
    crosswalk_rows = [
        # U1 feeds two different generators.
        {
            "plant_id_epa": 7000,
            "emissions_unit_id_epa": "U1",
            "generator_id_epa": "G1",
            "plant_id_eia": 7000,
            "boiler_id": "U1",
            "generator_id": "G1",
        },
        {
            "plant_id_epa": 7000,
            "emissions_unit_id_epa": "U1",
            "generator_id_epa": "G2",
            "plant_id_eia": 7000,
            "boiler_id": "U1",
            "generator_id": "G2",
        },
        {
            "plant_id_epa": 7000,
            "emissions_unit_id_epa": "U2",
            "generator_id_epa": "G3",
            "plant_id_eia": 7000,
            "boiler_id": "U2",
            "generator_id": "G3",
        },
    ]
    generator_rows = [
        {"plant_id_eia": 7000, "generator_id": "G1"},
        {"plant_id_eia": 7000, "generator_id": "G2"},
        {"plant_id_eia": 7000, "generator_id": "G3"},
    ]
    bga_rows = [
        {"plant_id_eia": 7000, "generator_id": "G1", "unit_id_pudl": 1},
        {"plant_id_eia": 7000, "generator_id": "G2", "unit_id_pudl": 1},
        {"plant_id_eia": 7000, "generator_id": "G3", "unit_id_pudl": 2},
    ]

    actual = _make_subplant_ids(
        crosswalk_rows, generator_rows, emissions_unit_rows, bga_rows
    )

    for emissions_unit_row in emissions_unit_rows:
        matches = actual[
            (actual.plant_id_eia == emissions_unit_row["plant_id_eia"])
            & (
                actual.emissions_unit_id_epa
                == emissions_unit_row["emissions_unit_id_epa"]
            )
        ]
        subplant_ids = matches.subplant_id.to_numpy()
        assert (subplant_ids == subplant_ids[0]).all(), (
            f"EPA unit {emissions_unit_row} maps to more than one subplant_id: \n{matches}"
        )


def test_subplant_id_is_never_null():
    """No row of the subplant ID table should ever have a null subplant_id.

    This is a stronger, whole-table version of the completeness checks above: rather
    than checking that specific known entities show up with a subplant_id, it checks
    that *no* row -- across a mix of the edge cases those tests exercise individually
    (an EIA-only generator, an EPA-only unit, multiple plants) -- is missing one.
    """
    generator_rows = [
        {"plant_id_eia": 1392, "generator_id": "1A"},
        # This generator never appears in the crosswalk, CEMS, or BGA inputs at all.
        {"plant_id_eia": 1392, "generator_id": "SOLAR1"},
        {"plant_id_eia": 8000, "generator_id": "G1"},
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
            "plant_id_epa": 8000,
            "emissions_unit_id_epa": "U1",
            "generator_id_epa": "G1",
            "plant_id_eia": 8000,
            "boiler_id": "U1",
            "generator_id": "G1",
        },
    ]
    emissions_unit_rows = [
        {"plant_id_eia": 1392, "emissions_unit_id_epa": "1A"},
        {"plant_id_eia": 8000, "emissions_unit_id_epa": "U1"},
        # This EPA unit never matches any EIA generator in the crosswalk.
        {"plant_id_eia": 8000, "emissions_unit_id_epa": "U_ORPHAN"},
    ]
    bga_rows = [
        {"plant_id_eia": 1392, "generator_id": "1A", "unit_id_pudl": 1},
        {"plant_id_eia": 8000, "generator_id": "G1", "unit_id_pudl": 1},
    ]

    actual = _make_subplant_ids(
        crosswalk_rows, generator_rows, emissions_unit_rows, bga_rows
    )

    assert actual.subplant_id.notna().all(), (
        f"Found rows with a null subplant_id: \n{actual[actual.subplant_id.isna()]}"
    )


def test_unmatched_epa_units_are_not_spuriously_merged():
    """Unrelated unmatched EPA units shouldn't get merged into the same subplant.

    EPA units that never match an EIA generator all end up with a null generator_id
    at the point make_subplant_ids builds its bipartite graph. groupby(...).ngroup()
    drops null-key rows from grouping and gives all of them the shared sentinel -1,
    so naively reusing that value as a surrogate node id would connect every unmatched
    unit in the plant to the same node -- and therefore to each other -- even though
    they have nothing to do with one another. Two such units with no other connection
    to each other must still land in different subplants.
    """
    generator_rows = [
        {"plant_id_eia": 9000, "generator_id": "G1"},
    ]
    crosswalk_rows = [
        {
            "plant_id_epa": 9000,
            "emissions_unit_id_epa": "U1",
            "generator_id_epa": "G1",
            "plant_id_eia": 9000,
            "boiler_id": "U1",
            "generator_id": "G1",
        },
    ]
    emissions_unit_rows = [
        {"plant_id_eia": 9000, "emissions_unit_id_epa": "U1"},
        # Neither of these ever matches a generator, and they have no connection to
        # each other or to U1.
        {"plant_id_eia": 9000, "emissions_unit_id_epa": "U_ORPHAN1"},
        {"plant_id_eia": 9000, "emissions_unit_id_epa": "U_ORPHAN2"},
    ]
    bga_rows = [
        {"plant_id_eia": 9000, "generator_id": "G1", "unit_id_pudl": 1},
    ]

    actual = _make_subplant_ids(
        crosswalk_rows, generator_rows, emissions_unit_rows, bga_rows
    )

    def _subplant_id_for(emissions_unit_id_epa: str) -> int:
        matches = actual[
            (actual.plant_id_eia == 9000)
            & (actual.emissions_unit_id_epa == emissions_unit_id_epa)
        ]
        return matches.subplant_id.iloc[0]

    assert _subplant_id_for("U_ORPHAN1") != _subplant_id_for("U_ORPHAN2"), (
        "Two unrelated unmatched EPA units were merged into the same subplant: "
        f"\n{actual}"
    )


def test_shared_unit_id_pudl_reconciliation_handles_multiple_groups():
    """Every generator sharing a unit_id_pudl should end up in the same subplant_id.

    update_subplant_ids merges subplants together when EIA's boiler-generator
    association table (BGA) says they share a unit_id_pudl, even though the EPA
    crosswalk didn't connect them directly. connect_ids does this by replacing each
    duplicated id with the minimum id in its group -- but when a plant has more than
    one such group needing reconciliation, connect_ids collapses the fix to a single
    scalar (the first group's minimum) instead of computing it per group, corrupting
    every group but the first.

    This plant has two independent pairs of subplants that each need to be merged
    because they share a unit_id_pudl (10 for one pair, 20 for the other), plus one
    generator in each original subplant with no BGA match at all (unit_id_pudl is
    null), which is what actually triggers the bug via the NaN-fill in
    update_subplant_ids.
    """
    generator_rows = [
        {"plant_id_eia": 4002, "generator_id": g}
        for g in ["Aa1", "Aa2", "Ab1", "Ac1", "Ac2", "Ad1"]
    ]
    crosswalk_rows = [
        # Combustor Ca feeds both Aa1 (unit_id_pudl=10) and Aa2 (no BGA match).
        {
            "plant_id_epa": 4002,
            "emissions_unit_id_epa": "Ca",
            "generator_id_epa": "Aa1",
            "plant_id_eia": 4002,
            "boiler_id": "Ca",
            "generator_id": "Aa1",
        },
        {
            "plant_id_epa": 4002,
            "emissions_unit_id_epa": "Ca",
            "generator_id_epa": "Aa2",
            "plant_id_eia": 4002,
            "boiler_id": "Ca",
            "generator_id": "Aa2",
        },
        # Combustor Cb feeds only Ab1 (unit_id_pudl=10, same unit as Aa1 above).
        {
            "plant_id_epa": 4002,
            "emissions_unit_id_epa": "Cb",
            "generator_id_epa": "Ab1",
            "plant_id_eia": 4002,
            "boiler_id": "Cb",
            "generator_id": "Ab1",
        },
        # Combustor Cc feeds both Ac1 (unit_id_pudl=20) and Ac2 (no BGA match).
        {
            "plant_id_epa": 4002,
            "emissions_unit_id_epa": "Cc",
            "generator_id_epa": "Ac1",
            "plant_id_eia": 4002,
            "boiler_id": "Cc",
            "generator_id": "Ac1",
        },
        {
            "plant_id_epa": 4002,
            "emissions_unit_id_epa": "Cc",
            "generator_id_epa": "Ac2",
            "plant_id_eia": 4002,
            "boiler_id": "Cc",
            "generator_id": "Ac2",
        },
        # Combustor Cd feeds only Ad1 (unit_id_pudl=20, same unit as Ac1 above).
        {
            "plant_id_epa": 4002,
            "emissions_unit_id_epa": "Cd",
            "generator_id_epa": "Ad1",
            "plant_id_eia": 4002,
            "boiler_id": "Cd",
            "generator_id": "Ad1",
        },
    ]
    emissions_unit_rows = [
        {"plant_id_eia": 4002, "emissions_unit_id_epa": u}
        for u in ["Ca", "Cb", "Cc", "Cd"]
    ]
    bga_rows = [
        {"plant_id_eia": 4002, "generator_id": "Aa1", "unit_id_pudl": 10},
        {"plant_id_eia": 4002, "generator_id": "Ab1", "unit_id_pudl": 10},
        {"plant_id_eia": 4002, "generator_id": "Ac1", "unit_id_pudl": 20},
        {"plant_id_eia": 4002, "generator_id": "Ad1", "unit_id_pudl": 20},
    ]

    actual = _make_subplant_ids(
        crosswalk_rows, generator_rows, emissions_unit_rows, bga_rows
    )

    def _subplant_id_for(generator_id: str) -> int:
        matches = actual[
            (actual.plant_id_eia == 4002) & (actual.generator_id == generator_id)
        ]
        return matches.subplant_id.iloc[0]

    assert _subplant_id_for("Ac1") == _subplant_id_for("Ad1"), (
        "Ac1 and Ad1 share unit_id_pudl=20 but landed in different subplants: "
        f"\n{actual}"
    )
    assert _subplant_id_for("Ac1") != _subplant_id_for("Aa1"), (
        "Ac1 (unit_id_pudl=20) was incorrectly merged into the unrelated "
        f"unit_id_pudl=10 group: \n{actual}"
    )


def test_subplant_id_table_preserves_row_count():
    """The subplant ID table should have one row per crosswalk association.

    core_epa__assn_eia_epacamd_subplant_ids shouldn't silently gain or lose rows
    relative to the augmented crosswalk that feeds make_subplant_ids -- every
    (EPA unit, EIA generator) association from the input should show up exactly once
    in the output, with none dropped and none duplicated.
    """
    generator_rows = [
        {"plant_id_eia": 6500, "generator_id": "G1"},
        {"plant_id_eia": 6500, "generator_id": "G2"},
    ]
    crosswalk_rows = [
        {
            "plant_id_epa": 6500,
            "emissions_unit_id_epa": "U1",
            "generator_id_epa": "G1",
            "plant_id_eia": 6500,
            "boiler_id": "U1",
            "generator_id": "G1",
        },
        {
            "plant_id_epa": 6500,
            "emissions_unit_id_epa": "U2",
            "generator_id_epa": "G2",
            "plant_id_eia": 6500,
            "boiler_id": "U2",
            "generator_id": "G2",
        },
    ]
    emissions_unit_rows = [
        {"plant_id_eia": 6500, "emissions_unit_id_epa": "U1"},
        {"plant_id_eia": 6500, "emissions_unit_id_epa": "U2"},
    ]
    bga_rows = [
        {"plant_id_eia": 6500, "generator_id": "G1", "unit_id_pudl": 1},
        {"plant_id_eia": 6500, "generator_id": "G2", "unit_id_pudl": 2},
    ]

    actual = _make_subplant_ids(
        crosswalk_rows, generator_rows, emissions_unit_rows, bga_rows
    )

    assert len(actual) == len(crosswalk_rows), (
        f"Expected {len(crosswalk_rows)} rows (one per crosswalk association), got "
        f"{len(actual)}: \n{actual}"
    )


def test_duplicate_crosswalk_edges_raise_instead_of_silently_dropping_rows():
    """A duplicated (EPA unit, EIA generator) edge should raise, not vanish.

    make_subplant_ids builds a networkx graph with one edge per crosswalk row. A
    plain nx.Graph silently collapses duplicate edges between the same pair of nodes,
    keeping only the last one's attributes -- so a duplicated (combustor_id,
    generator_id_unique) pair would otherwise make a row disappear with no warning.

    make_subplant_ids itself already deduplicates its input before this can happen in
    normal use (see test_subplant_id_table_preserves_row_count above and
    make_subplant_ids's docstring), so this exercises the lower-level function
    directly to confirm its own row-count guarantee holds independent of that
    caller-side protection.
    """
    prepped = pd.DataFrame(
        {
            "plant_id_eia": [6501, 6501],
            "emissions_unit_id_epa": ["U1", "U1"],
            "generator_id": ["G1", "G1"],
            "combustor_id": [0, 0],
            "generator_id_unique": [1, 1],
        }
    )

    with pytest.raises(AssertionError, match="row"):
        glue_assets._subplant_ids_from_prepped_crosswalk(prepped)


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
    # dg.asset-decorated functions type-check as returning `object`, not their
    # declared return type, when called directly.
    actual = glue_assets.core_epa__assn_eia_epacamd_subplant_ids(  # type: ignore[bad-return]
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
