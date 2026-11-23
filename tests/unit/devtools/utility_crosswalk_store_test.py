"""Unit tests for the crosswalk labelling dashboard's storage and queue logic."""

import sys
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parents[3] / "devtools"))
import crosswalk_store as cs  # noqa: E402

FERC_NAMES = pd.Series({1: "Alpha Power", 2: "Beta Electric", 3: "Gamma Co"})
EIA_NAMES = pd.Series({10: "Alpha Power Co", 11: "Alpha Pwr", 20: "Beta Elec"})


def at(second: int) -> datetime:
    """A fixed timestamp ``second`` seconds into a day."""
    return datetime(2026, 1, 1, 0, 0, second, tzinfo=UTC)


def test_record_matched_sorts_and_dedupes_ids():
    decisions = cs.record_decision(
        cs.empty_decisions(), 1, "matched", [11, 10, 10], now=at(0)
    )
    assert decisions.loc[0, "utility_ids_eia"] == "10;11"


def test_record_replaces_earlier_decision():
    decisions = cs.record_decision(cs.empty_decisions(), 1, "skipped", now=at(0))
    decisions = cs.record_decision(decisions, 1, "matched", [10], now=at(1))
    assert len(decisions) == 1
    assert decisions.loc[0, "outcome"] == "matched"


@pytest.mark.parametrize(
    ("outcome", "ids"),
    [("matched", []), ("no_match", [10]), ("bogus", [])],
)
def test_record_rejects_inconsistent_decisions(outcome, ids):
    with pytest.raises(ValueError, match="outcome|EIA"):
        cs.record_decision(cs.empty_decisions(), 1, outcome, ids)


def test_crosswalk_has_one_row_per_pair():
    decisions = cs.record_decision(
        cs.empty_decisions(), 1, "matched", [10, 11], uncertain=True, now=at(0)
    )
    decisions = cs.record_decision(decisions, 2, "no_match", now=at(1))
    crosswalk = cs.build_crosswalk(decisions, FERC_NAMES, EIA_NAMES)
    assert crosswalk[["utility_id_ferc1", "utility_id_eia"]].to_numpy().tolist() == [
        [1, 10],
        [1, 11],
    ]
    assert crosswalk["uncertain"].all()
    assert crosswalk["name_eia"].tolist() == ["Alpha Power Co", "Alpha Pwr"]


def test_save_and_reload_round_trip(tmp_path):
    decisions = cs.record_decision(
        cs.empty_decisions(), 1, "matched", [10], notes="a note", now=at(0)
    )
    decisions = cs.record_decision(decisions, 3, "no_match", now=at(1))
    cs.save(
        decisions,
        tmp_path / "decisions.csv",
        tmp_path / "crosswalk.csv",
        FERC_NAMES,
        EIA_NAMES,
    )
    reloaded = cs.load_decisions(tmp_path / "decisions.csv")
    pd.testing.assert_frame_equal(reloaded, decisions)
    assert len(pd.read_csv(tmp_path / "crosswalk.csv")) == 1


def test_load_missing_file_is_empty(tmp_path):
    assert cs.load_decisions(tmp_path / "nope.csv").empty


def test_undo_removes_most_recent_decision():
    decisions = cs.record_decision(cs.empty_decisions(), 1, "matched", [10], now=at(0))
    decisions = cs.record_decision(decisions, 2, "no_match", now=at(5))
    decisions, undone = cs.undo_last(decisions)
    assert undone == 2
    assert decisions["utility_id_ferc1"].tolist() == [1]


def test_undo_on_empty_decisions_is_a_no_op():
    decisions, undone = cs.undo_last(cs.empty_decisions())
    assert decisions.empty
    assert undone is None


def test_queue_puts_skipped_after_undecided():
    decisions = cs.record_decision(cs.empty_decisions(), 1, "skipped", now=at(0))
    decisions = cs.record_decision(decisions, 2, "matched", [20], now=at(1))
    assert cs.pending_queue([1, 2, 3], decisions) == [3, 1]
    assert cs.pending_queue([1, 2, 3], decisions, include_skipped=False) == [3]
