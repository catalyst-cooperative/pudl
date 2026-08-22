"""Unit tests for the pudl.transform.eia module."""

import random

import pandas as pd

from pudl.transform.eia import occurrence_consistency

CODES_POOL = ["GT", "CA", "ST", "IC", "CT", "CS", "CE", "CP", "FC", "HY"]


def test_occurrence_consistency_tiebreak_is_alphabetical():
    """Exactly-tied candidate values must resolve deterministically.

    When a column is harvested with ``strictness=0`` (e.g. ``prime_mover_code``),
    every distinct reported value can end up an equally-valid candidate. Real
    production data hits this: plant_id_eia=50973/generator_id="GN27" reported
    ``prime_mover_code`` as "CA" in one source table and "ST" in another for
    report_date 2011-01-01, tied 1-1. :func:`occurrence_consistency` should
    always resolve such ties to the alphabetically first value, per
    :ref:`entity_resolution`, rather than leaving the outcome to incidental
    pandas/numpy sort behavior -- which only shows up with enough entities in
    play at once, hence generating a batch of them here rather than a single
    tied pair.
    """
    rng = random.Random(0)  # noqa: S311
    cols_to_consit = ["plant_id_eia", "generator_id", "report_date"]
    rows = [
        {
            "plant_id_eia": plant_id,
            "generator_id": "1",
            "report_date": pd.Timestamp("2011-01-01"),
            "prime_mover_code": code,
            "table": f"table_{table_i}",
        }
        for plant_id in range(50)
        for table_i, code in enumerate(rng.sample(CODES_POOL, 2))
    ]
    compiled_df = pd.DataFrame(rows)

    col_df = occurrence_consistency(
        ["plant_id_eia", "generator_id"],
        compiled_df,
        "prime_mover_code",
        cols_to_consit,
        strictness=0.0,
    )
    # Replicates the candidate-selection logic in harvest_entity_tables.
    winners = col_df[col_df["is_candidate"]].drop_duplicates(
        subset=(cols_to_consit + ["is_candidate"])
    )
    alpha_min = (
        compiled_df.groupby("plant_id_eia")["prime_mover_code"]
        .apply(lambda vals: min(vals.unique()))
        .rename("alpha_min")
    )
    merged = winners.merge(alpha_min, on="plant_id_eia")

    mismatches = merged[merged["prime_mover_code"] != merged["alpha_min"]]
    assert mismatches.empty, (
        "Expected the alphabetically-first tied value to always win, but got "
        f"mismatches for these entities:\n{mismatches}"
    )
