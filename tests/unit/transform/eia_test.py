"""Unit tests for the pudl.transform.eia module."""

import pandas as pd

from pudl.transform.eia import occurrence_consistency


def _tied_rows(codes: tuple[str, ...]) -> list[dict]:
    """Build 50 plants each reporting ``codes`` tied 1-1-...-1 across N tables.

    Insertion order of the codes rotates by plant so that a winner which merely
    tracked insertion order (rather than value) would not accidentally look
    alphabetical.
    """
    rows = []
    n = len(codes)
    for plant_id in range(50):
        shift = plant_id % n
        ordered = codes[shift:] + codes[:shift]
        for table_i, code in enumerate(ordered):
            rows.append(
                {
                    "plant_id_eia": plant_id,
                    "generator_id": "1",
                    "report_date": pd.Timestamp("2011-01-01"),
                    "prime_mover_code": code,
                    "table": f"table_{table_i}",
                }
            )
    return rows


def _winners(col_df: pd.DataFrame, cols_to_consit: list[str], col: str) -> pd.Series:
    """Pick the winning value per entity the way harvest_entity_tables does.

    harvest_entity_tables takes ``col_df[col_df["is_candidate"]]`` straight from
    :func:`occurrence_consistency` and keeps the first row per entity
    (``drop_duplicates``), relying on that function's documented consistent_rate/col
    sort to make the "first row" the alphabetically first tied value. This mirrors that
    same "first row wins" contract directly, instead of copying harvest_entity_tables'
    exact drop_duplicates/subset call, so this test doesn't need to change in lockstep
    with unrelated refactors of that function.
    """
    return (
        col_df[col_df["is_candidate"]]
        .groupby(cols_to_consit, observed=True)[col]
        .first()
    )


def test_occurrence_consistency_tiebreak_is_alphabetical():
    """Exactly-tied candidate values must resolve deterministically.

    When a column is harvested with ``strictness=0`` (e.g. ``prime_mover_code``), every
    distinct reported value can end up an equally-valid candidate. Real production data
    hits this: plant_id_eia=50973/generator_id="GN27" reported ``prime_mover_code`` as
    "CA" in one source table and "ST" in another for report_date 2011-01-01, tied 1-1.
    :func:`occurrence_consistency` should always resolve such ties to the alphabetically
    first value, per :ref:`entity_resolution`, rather than leaving the outcome to
    incidental pandas/numpy sort behavior.

    To prove the winner tracks the *value* rather than some incidental artifact of row
    order, we run the same tied entities twice: once with the original codes, and once
    with those codes relabeled (frequencies untouched) so the alphabetical order flips.
    The winner must flip too.
    """
    cols_to_consit = ["plant_id_eia", "generator_id", "report_date"]

    compiled_df = pd.DataFrame(_tied_rows(("CA", "ST")))
    col_df = occurrence_consistency(
        ["plant_id_eia", "generator_id"],
        compiled_df,
        "prime_mover_code",
        cols_to_consit,
        strictness=0.0,
    )
    winners = _winners(col_df, cols_to_consit, "prime_mover_code")
    assert (winners == "CA").all(), (
        f"Expected 'CA' (alphabetically first) to win every tie, got:\n{winners}"
    )

    # Generate a new dataframe with a different, larger set of tied values so the
    # alphabetically first one flips, without touching how often each value occurs.
    relabeled_df = pd.DataFrame(_tied_rows(("ZZ", "AA", "MM")))
    relabeled_col_df = occurrence_consistency(
        ["plant_id_eia", "generator_id"],
        relabeled_df,
        "prime_mover_code",
        cols_to_consit,
        strictness=0.0,
    )
    relabeled_winners = _winners(relabeled_col_df, cols_to_consit, "prime_mover_code")
    assert (relabeled_winners == "AA").all(), (
        "Expected the winner to track the alphabetically first value even "
        f"after relabeling, got:\n{relabeled_winners}"
    )
