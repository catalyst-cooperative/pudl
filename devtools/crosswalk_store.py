"""Persistence and queue logic for the FERC1-EIA utility crosswalk labelling dashboard.

``decisions.csv`` is the source of truth: one row per FERC utility that has been
decided, holding the outcome (``matched``, ``no_match`` or ``skipped``), the selected EIA
utility IDs, and review flags. The canonical crosswalk (``utility_crosswalk.csv``, one
row per accepted FERC1-EIA pair) is regenerated from it after every change, so a decision
can be revised or undone and the two files never disagree.

Files are written atomically (temp file, then rename) so closing the notebook mid-save
cannot corrupt them.
"""

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

OUTCOMES = ("matched", "no_match", "skipped")
DECISION_COLS = [
    "utility_id_ferc1",
    "outcome",
    "utility_ids_eia",
    "uncertain",
    "numeric_informed",
    "notes",
    "decided_at",
]
CROSSWALK_COLS = [
    "utility_id_ferc1",
    "name_ferc1",
    "utility_id_eia",
    "name_eia",
    "uncertain",
    "numeric_informed",
    "notes",
    "decided_at",
]


def _atomic_write(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(tmp, index=False)
    Path(tmp).replace(path)


def empty_decisions() -> pd.DataFrame:
    """An empty decisions table with the right columns and dtypes."""
    return pd.DataFrame(
        {
            "utility_id_ferc1": pd.Series(dtype="int64"),
            "outcome": pd.Series(dtype="string"),
            "utility_ids_eia": pd.Series(dtype="string"),
            "uncertain": pd.Series(dtype="bool"),
            "numeric_informed": pd.Series(dtype="bool"),
            "notes": pd.Series(dtype="string"),
            "decided_at": pd.Series(dtype="string"),
        }
    )


def _normalise(df: pd.DataFrame) -> pd.DataFrame:
    """Cast a decisions table to the canonical column order and dtypes."""
    return df[DECISION_COLS].astype(empty_decisions().dtypes.to_dict())


def load_decisions(path: Path) -> pd.DataFrame:
    """Read the decisions file, or return an empty table if it does not exist."""
    if not path.exists():
        return empty_decisions()
    df = pd.read_csv(path, dtype={"utility_ids_eia": "string", "notes": "string"})
    for col in ("uncertain", "numeric_informed"):
        df[col] = df[col].astype(str).str.lower() == "true"
    df["utility_ids_eia"] = df["utility_ids_eia"].fillna("")
    df["notes"] = df["notes"].fillna("")
    return _normalise(df)


def record_decision(
    decisions: pd.DataFrame,
    ferc_id: int,
    outcome: str,
    eia_ids: list[int] | None = None,
    *,
    uncertain: bool = False,
    numeric_informed: bool = False,
    notes: str = "",
    now: datetime | None = None,
) -> pd.DataFrame:
    """Return ``decisions`` with ``ferc_id`` decided (replacing any earlier decision)."""
    if outcome not in OUTCOMES:
        raise ValueError(f"outcome must be one of {OUTCOMES}, got {outcome!r}")
    eia_ids = sorted({int(i) for i in (eia_ids or [])})
    if outcome == "matched" and not eia_ids:
        raise ValueError("A matched decision needs at least one EIA utility ID.")
    if outcome != "matched" and eia_ids:
        raise ValueError(f"A {outcome} decision cannot have EIA utility IDs.")
    stamp = (now or datetime.now(UTC)).isoformat(timespec="seconds")
    row = pd.DataFrame(
        [
            {
                "utility_id_ferc1": int(ferc_id),
                "outcome": outcome,
                "utility_ids_eia": ";".join(str(i) for i in eia_ids),
                "uncertain": bool(uncertain),
                "numeric_informed": bool(numeric_informed),
                "notes": notes.strip(),
                "decided_at": stamp,
            }
        ]
    )
    kept = decisions[decisions["utility_id_ferc1"] != int(ferc_id)]
    return _normalise(pd.concat([kept, row], ignore_index=True))


def undo_last(decisions: pd.DataFrame) -> tuple[pd.DataFrame, int | None]:
    """Remove the most recent decision. Returns the new table and the FERC ID undone."""
    if decisions.empty:
        return decisions, None
    last = decisions["decided_at"].idxmax()
    # Ties (same second) resolve to the later row in file order.
    ties = decisions.index[decisions["decided_at"] == decisions.loc[last, "decided_at"]]
    last = ties[-1]
    ferc_id = int(decisions.loc[last, "utility_id_ferc1"])
    return decisions.drop(index=last).reset_index(drop=True), ferc_id


def build_crosswalk(
    decisions: pd.DataFrame, ferc_names: pd.Series, eia_names: pd.Series
) -> pd.DataFrame:
    """Expand matched decisions into one row per FERC1-EIA pair."""
    rows = []
    for row in decisions[decisions["outcome"] == "matched"].itertuples():
        for eia_id in str(row.utility_ids_eia).split(";"):
            rows.append(
                {
                    "utility_id_ferc1": row.utility_id_ferc1,
                    "name_ferc1": ferc_names.get(row.utility_id_ferc1, ""),
                    "utility_id_eia": int(eia_id),
                    "name_eia": eia_names.get(int(eia_id), ""),
                    "uncertain": row.uncertain,
                    "numeric_informed": row.numeric_informed,
                    "notes": row.notes,
                    "decided_at": row.decided_at,
                }
            )
    return pd.DataFrame(rows, columns=CROSSWALK_COLS).sort_values(
        ["utility_id_ferc1", "utility_id_eia"], ignore_index=True
    )


def save(
    decisions: pd.DataFrame,
    decisions_path: Path,
    crosswalk_path: Path,
    ferc_names: pd.Series,
    eia_names: pd.Series,
) -> None:
    """Write the decisions and the crosswalk derived from them."""
    _atomic_write(decisions, decisions_path)
    _atomic_write(build_crosswalk(decisions, ferc_names, eia_names), crosswalk_path)


def pending_queue(
    ferc_order: list[int], decisions: pd.DataFrame, *, include_skipped: bool = True
) -> list[int]:
    """FERC utilities still to review: undecided first, then skipped ones."""
    outcome = decisions.set_index("utility_id_ferc1")["outcome"]
    undecided = [i for i in ferc_order if i not in outcome.index]
    skipped = [i for i in ferc_order if outcome.get(i) == "skipped"]
    return undecided + (skipped if include_skipped else [])
