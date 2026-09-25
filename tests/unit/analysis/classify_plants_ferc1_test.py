"""Unit tests for the deterministic parts of the FERC 1 plant ID assignment."""

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from pudl.analysis.record_linkage.classify_plants_ferc1 import (
    _assign_plant_ids,
    _canonicalize_plant_ids,
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
    ids = _canonicalize_plant_ids(df)
    # Sorted order is r1, r3, r5 (2000) then r2, r4, r6 (2001); IDs start at 1.
    assert ids.tolist() == [1, 1, 2, 2, 3, 3]


@pytest.mark.parametrize("seed", range(5))
def test_canonicalize_plant_ids_ignores_label_values_and_row_order(seed: int):
    """Renumbering labels and shuffling rows must not change the IDs by record."""
    df = _records()
    expected = dict(zip(df["record_id"], _canonicalize_plant_ids(df), strict=True))

    shuffled = df.sample(frac=1, random_state=seed).reset_index(drop=True)
    shuffled["record_label"] = shuffled["record_label"].map({7: 100, 3: 5, 0: 42})
    ids = _canonicalize_plant_ids(shuffled)
    assert dict(zip(shuffled["record_id"], ids, strict=True)) == expected


def test_canonicalize_plant_ids_unrelated_change_does_not_shift_ids():
    """Splitting one plant must leave the IDs of the other plants alone."""
    df = _records()
    before = _canonicalize_plant_ids(df)

    split = df.assign(record_label=[7, 7, 3, 99, 0, 0])
    after = _canonicalize_plant_ids(split)

    unchanged = df["plant_name_ferc1"].isin(["a", "c"])
    assert before[unchanged].tolist() == after[unchanged].tolist()
    assert after[3] not in set(before)


def test_assign_plant_ids_matches_on_record_id():
    """IDs land on the right steam records even if row order differs."""
    labeled_df = _records()
    steam = pd.DataFrame({"record_id": ["r6", "r1", "r4", "r2", "r3", "r5"]})
    out = _assign_plant_ids(steam, labeled_df)
    assert out["plant_id_ferc1"].tolist() == [3, 1, 2, 1, 2, 3]
    assert_frame_equal(out[["record_id"]], steam)
