"""Unit tests for the pudl.transform.eia module."""

import random

import pandas as pd

from pudl.transform.eia import occurrence_consistency

CODES_POOL = [
    "GT",
    "CA",
    "ST",
    "IC",
    "CT",
    "CS",
    "CE",
    "CP",
    "FC",
    "HY",
    "OT",
    "PS",
    "PV",
    "WT",
    "BA",
]


def _tied_prime_mover_records(n_entities: int, seed: int) -> pd.DataFrame:
    """Build a compiled_df where every entity has two exactly-tied values.

    Each entity has exactly two reports of a value in ``col``, drawn from two
    different tables, each a different value. Since each is reported once,
    they're tied at a 50% consistent_rate. This mirrors real production data:
    e.g. plant_id_eia=50973/generator_id="GN27" reported
    ``prime_mover_code`` as "CA" in one source table and "ST" in another for
    report_date 2011-01-01, tied 1-1.
    """
    rng = random.Random(seed)  # noqa: S311
    rows = []
    for pid in range(n_entities):
        val_a, val_b = rng.sample(CODES_POOL, 2)
        for table_i, val in enumerate([val_a, val_b]):
            rows.append(
                {
                    "plant_id_eia": pid,
                    "generator_id": "1",
                    "report_date": pd.Timestamp("2011-01-01"),
                    "prime_mover_code": val,
                    "table": f"table_{table_i}",
                }
            )
    return pd.DataFrame(rows)


def _pick_candidates(col_df: pd.DataFrame, cols_to_consit: list[str]) -> pd.DataFrame:
    """Replicate the candidate-selection logic in harvest_entity_tables."""
    return col_df[col_df["is_candidate"]].drop_duplicates(
        subset=(cols_to_consit + ["is_candidate"])
    )


def test_occurrence_consistency_tiebreak_is_alphabetical_and_deterministic():
    """Exactly-tied candidate values must resolve the same way every time.

    Reproduces a bug where, when multiple reported values for an entity are
    exactly tied (as happens with ``strictness=0`` columns like
    ``prime_mover_code``), :func:`occurrence_consistency` sorted only by
    ``consistent_rate``, leaving the winner among tied values up to
    incidental pandas/numpy sort behavior. That behavior is consistent within
    a single environment, but isn't guaranteed to match across different
    pandas/numpy versions or platforms (e.g. the CI/nightly-build Linux
    runner vs. a developer's local machine), which produced different
    harvested values -- and thus different downstream row counts -- for the
    same input data on different machines.
    """
    cols_to_consit = ["plant_id_eia", "generator_id", "report_date"]
    compiled_df = _tied_prime_mover_records(n_entities=50, seed=2)

    col_df = occurrence_consistency(
        ["plant_id_eia", "generator_id"],
        compiled_df,
        "prime_mover_code",
        cols_to_consit,
        strictness=0.0,
    )
    winners = _pick_candidates(col_df, cols_to_consit)

    alpha_min = (
        compiled_df.groupby("plant_id_eia")["prime_mover_code"]
        .apply(lambda vals: min(vals.unique()))
        .rename("alpha_min")
    )
    merged = winners.merge(alpha_min, on="plant_id_eia")

    mismatches = merged[merged["prime_mover_code"] != merged["alpha_min"]]
    assert mismatches.empty, (
        "Expected the alphabetically-first tied value to always win, but "
        f"got mismatches for these entities:\n{mismatches}"
    )
