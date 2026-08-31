"""Tests for Glue functions."""

from io import StringIO
from typing import Any

import pandas as pd
import pytest

import pudl.dagster.assets.core.glue as glue_assets

_UNSET = object()


def _crosswalk_row(
    plant_id_eia: int,
    emissions_unit_id_epa: str,
    generator_id: str,
    boiler_id: str | None = _UNSET,  # type: ignore[assignment]
) -> dict:
    """Build one crosswalk row, mirroring ``_core_epa__assn_eia_epacamd_unique``.

    In every test scenario ``plant_id_epa`` equals ``plant_id_eia`` and
    ``generator_id_epa`` equals ``generator_id``, and ``boiler_id`` usually just
    equals ``emissions_unit_id_epa`` -- so those are filled in automatically unless
    a test needs something different (e.g. a null ``boiler_id``).
    """
    return {
        "plant_id_epa": plant_id_eia,
        "emissions_unit_id_epa": emissions_unit_id_epa,
        "generator_id_epa": generator_id,
        "plant_id_eia": plant_id_eia,
        "boiler_id": emissions_unit_id_epa if boiler_id is _UNSET else boiler_id,
        "generator_id": generator_id,
    }


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


def _rows_mask(actual: pd.DataFrame, row: dict, id_cols: list[str]) -> pd.Series:
    mask = pd.Series(True, index=actual.index)
    for col in id_cols:
        mask &= actual[col] == row[col]
    return mask


def _assert_present_with_subplant_id(
    actual: pd.DataFrame, rows: list[dict], id_cols: list[str], entity_label: str
) -> None:
    """Assert every row's (id_cols) combination appears at least once, non-null."""
    for row in rows:
        matches = actual[_rows_mask(actual, row, id_cols)]
        assert not matches.empty, (
            f"{entity_label} {row} is missing from the subplant ID table entirely."
        )
        assert matches.subplant_id.notna().all(), (
            f"{entity_label} {row} appears with a null subplant_id: \n{matches}"
        )


def _assert_single_subplant_id_per_row(
    actual: pd.DataFrame, rows: list[dict], id_cols: list[str], entity_label: str
) -> None:
    """Assert every row's (id_cols) combination maps to exactly one subplant_id."""
    for row in rows:
        matches = actual[_rows_mask(actual, row, id_cols)]
        subplant_ids = matches.subplant_id.to_numpy()
        assert (subplant_ids == subplant_ids[0]).all(), (
            f"{entity_label} {row} maps to more than one subplant_id: \n{matches}"
        )


def _subplant_id_for(actual: pd.DataFrame, plant_id_eia: int, **id_filter: Any) -> int:
    """Look up the subplant_id for a single (plant_id_eia, id_col=id_value) row.

    ``id_filter`` must contain exactly one keyword, naming the id column to filter
    on (e.g. ``generator_id="G1"`` or ``emissions_unit_id_epa="U1"``).
    """
    [(id_col, id_value)] = id_filter.items()
    matches = actual[
        (actual.plant_id_eia == plant_id_eia) & (actual[id_col] == id_value)
    ]
    return matches.subplant_id.iloc[0]


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
        _crosswalk_row(1392, "1A", "1A"),
        _crosswalk_row(1392, "2A", "2A"),
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

    _assert_present_with_subplant_id(
        actual, generator_rows, ["plant_id_eia", "generator_id"], "Generator"
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
        _crosswalk_row(5000, "U1", "G1"),
        _crosswalk_row(5000, "U2", "G1"),
        _crosswalk_row(5000, "U3", "G2"),
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

    _assert_single_subplant_id_per_row(
        actual, generator_rows, ["plant_id_eia", "generator_id"], "Generator"
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
        _crosswalk_row(6000, "U1", "G1"),
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

    _assert_present_with_subplant_id(
        actual,
        emissions_unit_rows,
        ["plant_id_eia", "emissions_unit_id_epa"],
        "EPA unit",
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
        _crosswalk_row(7000, "U1", "G1"),
        _crosswalk_row(7000, "U1", "G2"),
        _crosswalk_row(7000, "U2", "G3"),
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

    _assert_single_subplant_id_per_row(
        actual,
        emissions_unit_rows,
        ["plant_id_eia", "emissions_unit_id_epa"],
        "EPA unit",
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
        _crosswalk_row(1392, "1A", "1A"),
        _crosswalk_row(8000, "U1", "G1"),
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

    EPA units that never match an EIA generator all end up with a null generator_id at
    the point make_subplant_ids builds its bipartite graph. Two such units with no other
    connection to each other must still land in different subplants.
    """
    generator_rows = [
        {"plant_id_eia": 9000, "generator_id": "G1"},
    ]
    crosswalk_rows = [
        _crosswalk_row(9000, "U1", "G1"),
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

    assert _subplant_id_for(
        actual, 9000, emissions_unit_id_epa="U_ORPHAN1"
    ) != _subplant_id_for(actual, 9000, emissions_unit_id_epa="U_ORPHAN2"), (
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
        _crosswalk_row(4002, "Ca", "Aa1"),
        _crosswalk_row(4002, "Ca", "Aa2"),
        # Combustor Cb feeds only Ab1 (unit_id_pudl=10, same unit as Aa1 above).
        _crosswalk_row(4002, "Cb", "Ab1"),
        # Combustor Cc feeds both Ac1 (unit_id_pudl=20) and Ac2 (no BGA match).
        _crosswalk_row(4002, "Cc", "Ac1"),
        _crosswalk_row(4002, "Cc", "Ac2"),
        # Combustor Cd feeds only Ad1 (unit_id_pudl=20, same unit as Ac1 above).
        _crosswalk_row(4002, "Cd", "Ad1"),
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

    assert _subplant_id_for(actual, 4002, generator_id="Ac1") == _subplant_id_for(
        actual, 4002, generator_id="Ad1"
    ), (
        "Ac1 and Ad1 share unit_id_pudl=20 but landed in different subplants: "
        f"\n{actual}"
    )
    assert _subplant_id_for(actual, 4002, generator_id="Ac1") != _subplant_id_for(
        actual, 4002, generator_id="Aa1"
    ), (
        "Ac1 (unit_id_pudl=20) was incorrectly merged into the unrelated "
        f"unit_id_pudl=10 group: \n{actual}"
    )


@pytest.mark.parametrize(
    "target_group_has_orphan,moving_group_has_orphan",
    [
        pytest.param(False, False, id="neither_group_has_an_orphan"),
        pytest.param(True, False, id="target_group_has_an_orphan"),
        pytest.param(False, True, id="moving_group_has_an_orphan"),
        pytest.param(True, True, id="both_groups_have_an_orphan"),
    ],
)
def test_unit_id_pudl_reconciliation_moves_orphaned_siblings_too(
    target_group_has_orphan: bool, moving_group_has_orphan: bool
):
    """A generator with no BGA match must move with its combustor's other generator.

    update_subplant_ids/connect_ids merges two make_subplant_ids groups together
    whenever EIA-860's boiler-generator association (BGA) table says two of their
    generators share a ``unit_id_pudl``, even though the EPA crosswalk didn't connect
    them directly. One of the two groups keeps its original ``subplant_id`` (the
    "target"); the other is remapped onto it (the "mover").

    Each group here has one generator with a real BGA match (``unit_id_pudl`` is not
    null) -- these are the two whose shared ``unit_id_pudl`` triggers the merge -- and
    optionally a second generator sharing the *same EPA combustor* as the first, but
    with no BGA match of its own (``unit_id_pudl`` is null, an "orphan"). Because
    make_subplant_ids already grouped the orphan with its BGA-matched sibling via
    their shared combustor, both must land in the same final ``subplant_id`` as
    everything else in the merge, regardless of which group is the target and which
    is the mover, and regardless of whether an orphan is present in neither, either,
    or both groups.

    This is exhaustive over the two dimensions that determine whether connect_ids's
    row-level merge key can leave an orphan behind: which group it's in (target vs.
    mover), and whether it's present at all. A prior version of connect_ids joined its
    replacement mapping on ``(id_to_update, connecting_id)`` -- the pair -- so a row
    whose own ``unit_id_pudl`` was null (having no BGA match) never matched that key
    and silently kept its stale, pre-merge ``subplant_id``, splitting what should have
    been one subplant into two. That bug could only surface when the *mover* group
    had an orphan (id="moving_group_has_an_orphan" and "both_groups_have_an_orphan"
    below) -- the fix is keyed on ``connecting_id`` alone so it no longer matters.
    """
    plant_id_eia = 5001
    generator_rows = [{"plant_id_eia": plant_id_eia, "generator_id": "A1"}]
    crosswalk_rows = [
        _crosswalk_row(plant_id_eia, "CA", "A1"),
        _crosswalk_row(plant_id_eia, "CB", "B1"),
    ]
    emissions_unit_rows = [
        {"plant_id_eia": plant_id_eia, "emissions_unit_id_epa": "CA"},
        {"plant_id_eia": plant_id_eia, "emissions_unit_id_epa": "CB"},
    ]
    bga_rows = [
        {"plant_id_eia": plant_id_eia, "generator_id": "A1", "unit_id_pudl": 99},
        {"plant_id_eia": plant_id_eia, "generator_id": "B1", "unit_id_pudl": 99},
    ]
    generator_ids = ["A1", "B1"]

    if target_group_has_orphan:
        generator_rows.append({"plant_id_eia": plant_id_eia, "generator_id": "A2"})
        crosswalk_rows.append(_crosswalk_row(plant_id_eia, "CA", "A2"))
        generator_ids.append("A2")

    if moving_group_has_orphan:
        generator_rows.append({"plant_id_eia": plant_id_eia, "generator_id": "B2"})
        crosswalk_rows.append(_crosswalk_row(plant_id_eia, "CB", "B2"))
        generator_ids.append("B2")

    actual = _make_subplant_ids(
        crosswalk_rows, generator_rows, emissions_unit_rows, bga_rows
    )

    subplant_ids = {
        generator_id: actual.loc[
            (actual.plant_id_eia == plant_id_eia)
            & (actual.generator_id == generator_id),
            "subplant_id",
        ].iloc[0]
        for generator_id in generator_ids
    }
    assert len(set(subplant_ids.values())) == 1, (
        "All generators connected (directly or via a shared unit_id_pudl) should "
        f"land in one subplant_id, but got {subplant_ids}: \n{actual}"
    )


def test_merged_groups_orphans_join_the_merged_group_not_each_other():
    """Orphans from two different merging groups must not form their own subplant.

    This is a second, distinct regression test for the same real-world plant
    (structured on plant_id_eia 2708) that motivated
    test_unit_id_pudl_reconciliation_moves_orphaned_siblings_too above, but that
    parametrized test's minimal two-generator-per-group scenario cannot, by itself,
    catch every way connect_ids/update_subplant_ids can leave an orphan behind: with
    only one real unit_id_pudl value (99) anywhere in the plant, the arithmetic in
    update_subplant_ids's fallback fill -- ``unit_id_pudl_connected.fillna(
    subplant_id_connected + unit_id_pudl_connected.max())`` -- can *coincidentally*
    reproduce that same real value for an orphan whose subplant_id_connected happens
    to be 0, masking a bug in that fallback formula itself (as opposed to the
    connect_ids merge-key bug that test targets).

    Here, two independent pairs of combustors each feed one BGA-matched generator
    (unit_id_pudl=1, shared, triggering a merge) and one orphan (no BGA match) that
    shares a combustor with the matched generator. Two more unrelated, unmerged
    generators (unit_id_pudl 2 and 3) are also present, exactly as in the real
    plant -- their presence changes what ``unit_id_pudl_connected.max()`` evaluates
    to, which is what makes this scenario expose the fallback-fill bug where the
    minimal version does not.

    The invariant: once make_subplant_ids has connected an orphan to a matched
    generator via a shared combustor, and that matched generator is later merged into
    a different subplant_id via unit_id_pudl, the orphan must move into that same
    merged subplant_id too -- not get stranded with the *other* plant's orphan in a
    subplant of their own.
    """
    plant_id_eia = 2708
    generator_ids = ["1", "1A", "1B", "2", "2A", "2B", "5", "6"]
    generator_rows = [
        {"plant_id_eia": plant_id_eia, "generator_id": g} for g in generator_ids
    ]
    crosswalk_rows = [
        # Combustor "1A" feeds both "1" (unit_id_pudl=1) and orphan "1A".
        _crosswalk_row(plant_id_eia, "1A", "1", boiler_id=None),
        _crosswalk_row(plant_id_eia, "1A", "1A", boiler_id=None),
        # Combustor "1B" feeds both "1" and orphan "1B".
        _crosswalk_row(plant_id_eia, "1B", "1", boiler_id=None),
        _crosswalk_row(plant_id_eia, "1B", "1B", boiler_id=None),
        # Combustor "2A" feeds both "2" (unit_id_pudl=1, same as "1" -- triggers the
        # merge) and orphan "2A".
        _crosswalk_row(plant_id_eia, "2A", "2", boiler_id=None),
        _crosswalk_row(plant_id_eia, "2A", "2A", boiler_id=None),
        # Combustor "2B" feeds both "2" and orphan "2B".
        _crosswalk_row(plant_id_eia, "2B", "2", boiler_id=None),
        _crosswalk_row(plant_id_eia, "2B", "2B", boiler_id=None),
        # Two unrelated, unmerged single-generator subplants -- present in the real
        # plant, and load-bearing for reproducing the bug (they change what
        # unit_id_pudl_connected.max() evaluates to).
        _crosswalk_row(plant_id_eia, "5", "5"),
        _crosswalk_row(plant_id_eia, "6", "6"),
    ]
    emissions_unit_rows = [
        {"plant_id_eia": plant_id_eia, "emissions_unit_id_epa": u}
        for u in ["1A", "1B", "2A", "2B", "5", "6"]
    ]
    bga_rows = [
        {"plant_id_eia": plant_id_eia, "generator_id": "1", "unit_id_pudl": 1},
        {"plant_id_eia": plant_id_eia, "generator_id": "2", "unit_id_pudl": 1},
        {"plant_id_eia": plant_id_eia, "generator_id": "5", "unit_id_pudl": 2},
        {"plant_id_eia": plant_id_eia, "generator_id": "6", "unit_id_pudl": 3},
    ]

    actual = _make_subplant_ids(
        crosswalk_rows, generator_rows, emissions_unit_rows, bga_rows
    )

    merged_group = {
        g: _subplant_id_for(actual, plant_id_eia, generator_id=g)
        for g in ["1", "1A", "1B", "2", "2A", "2B"]
    }
    assert len(set(merged_group.values())) == 1, (
        "generators 1/1A/1B/2/2A/2B are all connected -- directly by a shared "
        f"combustor, or transitively via unit_id_pudl=1 -- but landed in more than "
        f"one subplant_id: {merged_group}: \n{actual}"
    )
    assert (
        _subplant_id_for(actual, plant_id_eia, generator_id="5")
        not in merged_group.values()
    ), (
        f"generator 5 (unrelated, unit_id_pudl=2) was incorrectly merged into the "
        f"1/2 group: \n{actual}"
    )
    assert (
        _subplant_id_for(actual, plant_id_eia, generator_id="6")
        not in merged_group.values()
    ), (
        f"generator 6 (unrelated, unit_id_pudl=3) was incorrectly merged into the "
        f"1/2 group: \n{actual}"
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
        _crosswalk_row(6500, "U1", "G1"),
        _crosswalk_row(6500, "U2", "G2"),
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


def test_subplant_id_is_one_indexed_and_contiguous_per_plant():
    """subplant_id should start at 1 and have no gaps within each plant_id_eia.

    subplant_id is only meaningful in combination with plant_id_eia, so each plant's
    subplant_ids should independently form a contiguous 1..n sequence -- not just be
    non-negative, and not share a single contiguous sequence across plants.
    """
    generator_rows = [
        {"plant_id_eia": 8100, "generator_id": g} for g in ["G1", "G2", "G3"]
    ]
    # Three mutually disconnected generators/EPA units at the same plant, so they
    # should land in three distinct subplants.
    crosswalk_rows = [_crosswalk_row(8100, f"U{i}", f"G{i}") for i in (1, 2, 3)]
    emissions_unit_rows = [
        {"plant_id_eia": 8100, "emissions_unit_id_epa": f"U{i}"} for i in (1, 2, 3)
    ]
    bga_rows = [
        {"plant_id_eia": 8100, "generator_id": f"G{i}", "unit_id_pudl": i}
        for i in (1, 2, 3)
    ]

    actual = _make_subplant_ids(
        crosswalk_rows, generator_rows, emissions_unit_rows, bga_rows
    )

    subplant_ids = sorted(actual[actual.plant_id_eia == 8100].subplant_id.unique())
    assert subplant_ids == [1, 2, 3], (
        f"Expected subplant_id 1..3 with no gaps, got {subplant_ids}: \n{actual}"
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
