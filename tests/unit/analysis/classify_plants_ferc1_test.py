"""Unit tests for the deterministic parts of the FERC 1 plant ID assignment."""

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from pudl.analysis.record_linkage.classify_plants_ferc1 import (
    _assign_plant_ids,
    _canonicalize_plant_ids,
    merge_steam_fuel_dfs,
)


def _records() -> pd.DataFrame:
    """Six records making up three plants, with arbitrary clustering labels."""
    return pd.DataFrame(
        {
            "record_id": ["r1", "r2", "r3", "r4", "r5", "r6"],
            "report_year": [2000, 2001, 2000, 2001, 2000, 2001],
            "utility_id_ferc1": [1, 1, 1, 1, 2, 2],
            "plant_name_ferc1": ["a", "a", "b", "b", "c", "c"],
            "record_label": [7, 7, 3, 3, 0, 0],
        }
    )


def test_canonicalize_plant_ids_uses_earliest_record_position():
    """Each plant is identified by the sorted position of its earliest record."""
    df = _records()
    ids = _canonicalize_plant_ids(df, df["record_label"])
    # Sorted order is r1, r3, r5 (2000) then r2, r4, r6 (2001)
    assert ids.tolist() == [0, 0, 1, 1, 2, 2]


@pytest.mark.parametrize("seed", range(5))
def test_canonicalize_plant_ids_ignores_label_values_and_row_order(seed: int):
    """Renumbering labels and shuffling rows must not change the IDs by record."""
    df = _records()
    expected = dict(
        zip(
            df["record_id"],
            _canonicalize_plant_ids(df, df["record_label"]),
            strict=True,
        )
    )

    shuffled = df.sample(frac=1, random_state=seed).reset_index(drop=True)
    relabeled = shuffled["record_label"].map({7: 100, 3: 5, 0: 42})
    ids = _canonicalize_plant_ids(shuffled, relabeled)
    assert dict(zip(shuffled["record_id"], ids, strict=True)) == expected


def test_canonicalize_plant_ids_unrelated_change_does_not_shift_ids():
    """Splitting one plant must leave the IDs of the other plants alone."""
    df = _records()
    before = _canonicalize_plant_ids(df, df["record_label"])

    split = df.assign(record_label=[7, 7, 3, 99, 0, 0])
    after = _canonicalize_plant_ids(split, split["record_label"])

    unchanged = df["plant_name_ferc1"].isin(["a", "c"])
    assert before[unchanged].tolist() == after[unchanged].tolist()
    assert after[3] not in set(before)


def test_assign_plant_ids_matches_on_record_id():
    """IDs land on the right steam records even if row order differs."""
    input_df = _records()
    steam = pd.DataFrame({"record_id": ["r6", "r1", "r4", "r2", "r3", "r5"]})
    out = _assign_plant_ids(steam, input_df, input_df["record_label"])
    assert out["plant_id_ferc1"].tolist() == [2, 0, 1, 0, 1, 2]
    assert_frame_equal(out[["record_id"]], steam)


def test_merge_steam_fuel_dfs_sorts_by_record_id():
    """The clustering input is sorted by primary key and has a fresh RangeIndex."""
    steam = pd.DataFrame(
        {
            "record_id": ["r3", "r1", "r2"],
            "utility_id_ferc1": [1, 1, 1],
            "plant_name_ferc1": ["c", "a", "b"],
            "report_year": [2000, 2000, 2000],
            "plant_type": ["x", "x", "x"],
            "construction_type": ["y", "y", "y"],
        }
    )
    fuel = steam[["utility_id_ferc1", "plant_name_ferc1", "report_year"]].assign(
        coal_fraction_mmbtu=[0.1, 0.2, 0.3]
    )
    out = merge_steam_fuel_dfs(steam, fuel)
    assert out["record_id"].tolist() == ["r1", "r2", "r3"]
    assert out.index.equals(pd.RangeIndex(3))
    assert out["coal_fraction_mmbtu"].tolist() == [0.2, 0.3, 0.1]
