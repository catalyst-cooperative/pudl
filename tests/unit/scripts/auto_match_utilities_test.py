import pandas as pd
import pytest

from pudl.scripts import auto_match_utilities as amu


@pytest.mark.parametrize(
    "raw, expected_nonnull_count",
    [
        (
            [
                "ACME, Inc.",
                "  Acme inc ",
                "Acme, Incorporated",
                "",
                None,
            ],
            1,
        ),
        (
            [
                "The Quick-Brown_Fox (LLC)",
                "quick brown fox llc",
                "Quick-Brown Fox, L.L.C.",  # DEBUG ME!
                "",
            ],
            1,
        ),
    ],
    ids=["acme-variants", "quick-brown-variants"],
)
def test_clean_utility_name_variations_param(raw, expected_nonnull_count):
    """Multiple textual variants of the same company should clean to a single canonical form."""
    series = pd.Series(raw)
    cleaned = amu.clean_utility_name(series)

    non_null = cleaned.dropna().unique()
    # Expect the non-empty variants to collapse to a single canonical string
    assert len(non_null) == expected_nonnull_count
    # Canonical string should be stripped
    if len(non_null) > 0:
        assert non_null[0] == non_null[0].strip()
    # Ensure empty string and None become pd.NA somewhere in the result
    assert any(pd.isna(x) for x in cleaned.iloc[len(raw) - 2 :].to_numpy())


@pytest.mark.parametrize(
    "eia_rows, ferc_rows, false_matches, expected_names",
    [
        # case: 'acme' already matched (same pudl ID), 'globex' differs -> only globex returned
        (
            [
                {
                    "utility_id_eia": 10,
                    "utility_id_pudl": 1,
                    "cleaned_utility_name": "acme",
                },
                {
                    "utility_id_eia": 20,
                    "utility_id_pudl": 2,
                    "cleaned_utility_name": "globex",
                },
            ],
            [
                {
                    "utility_id_ferc1": 100,
                    "utility_id_pudl": 1,
                    "cleaned_utility_name": "acme",
                },
                {
                    "utility_id_ferc1": 200,
                    "utility_id_pudl": 3,
                    "cleaned_utility_name": "globex",
                },
            ],
            [
                {
                    "utility_id_eia": 999,
                    "utility_id_ferc1": 999,
                    "notes": "bad match!",
                }
            ],
            ["globex"],
        ),
        # case: globex matches but is labelled false match --> no matches
        (
            [
                {
                    "utility_id_eia": 20,
                    "utility_id_pudl": 2,
                    "cleaned_utility_name": "globex",
                },
            ],
            [
                {
                    "utility_id_ferc1": 200,
                    "utility_id_pudl": 3,
                    "cleaned_utility_name": "globex",
                },
            ],
            [
                {
                    "utility_id_eia": 20,
                    "utility_id_ferc1": 200,
                    "notes": "bad match!",
                }
            ],
            [],
        ),
        # case: one name present only in eia and one only in ferc -> no matches
        (
            [
                {
                    "utility_id_eia": 11,
                    "utility_id_pudl": 1,
                    "cleaned_utility_name": "solo_eia",
                },
            ],
            [
                {
                    "utility_id_ferc1": 222,
                    "utility_id_pudl": 2,
                    "cleaned_utility_name": "solo_ferc",
                },
            ],
            [
                {
                    "utility_id_eia": 999,
                    "utility_id_ferc1": 999,
                    "notes": "bad match!",
                }
            ],
            [],
        ),
    ],
    ids=["test-existing-match", "test-false-match", "test-no-match"],
)
def test_match_utility_names_param(eia_rows, ferc_rows, false_matches, expected_names):
    """Test match_utility_names with multiple scenarios via parameterization."""
    eia = pd.DataFrame(eia_rows)
    ferc = pd.DataFrame(ferc_rows)
    false_matches = pd.DataFrame(false_matches)

    matches = amu.match_utility_names(
        eia_df=eia, ferc_df=ferc, false_matches=false_matches
    )

    returned_names = (
        sorted(matches.cleaned_utility_name.unique().tolist())
        if not matches.empty
        else []
    )
    assert returned_names == sorted(expected_names)


@pytest.mark.parametrize(
    "entity, overrides_rows, matches_rows, remaining_expected_ids",
    [
        # Drop unmatched ferc (entity='ferc1')
        (
            "ferc1",
            [
                {"utility_id_ferc1": 100, "utility_id_eia": 10, "utility_id_pudl": 1},
                {
                    "utility_id_ferc1": 200,
                    "utility_id_eia": pd.NA,
                    "utility_id_pudl": 3,
                },
                {"utility_id_ferc1": pd.NA, "utility_id_eia": 20, "utility_id_pudl": 4},
            ],
            [
                {
                    "utility_id_ferc1": 200,
                    "utility_id_eia": 20,
                    "utility_id_pudl_ferc1": 3,
                    "utility_id_pudl_eia": 4,
                    "cleaned_utility_name": "globex",
                }
            ],
            [
                100,
                pd.NA,
                pd.NA,
            ],  # remaining utility_id_ferc1 values (approx; check absence of 200)
        ),
        # Drop unmatched eia (entity='eia')
        (
            "eia",
            [
                {"utility_id_ferc1": 101, "utility_id_eia": 11, "utility_id_pudl": 5},
                {"utility_id_ferc1": pd.NA, "utility_id_eia": 22, "utility_id_pudl": 6},
                {
                    "utility_id_ferc1": 303,
                    "utility_id_eia": pd.NA,
                    "utility_id_pudl": 7,
                },
            ],
            [
                {
                    "utility_id_ferc1": 303,
                    "utility_id_eia": 22,
                    "utility_id_pudl_ferc1": 7,
                    "utility_id_pudl_eia": 6,
                    "cleaned_utility_name": "alpha",
                }
            ],
            [
                101,
                303,
                pd.NA,
            ],  # check that 22 was used to drop the unmatched eia row (22 should be gone)
        ),
    ],
    ids=["drop-ferc-unmatched", "drop-eia-unmatched"],
)
def test_drop_records_with_matches_param(
    entity, overrides_rows, matches_rows, remaining_expected_ids
):
    """Parametrized tests ensuring drop_records_with_matches drops only the intended rows."""
    overrides = pd.DataFrame(overrides_rows)
    matches = pd.DataFrame(matches_rows)

    override_matches = overrides[
        overrides.utility_id_eia.notnull() & overrides.utility_id_ferc1.notnull()
    ]
    override_unmatched = overrides[
        ~(overrides.utility_id_eia.notnull() & overrides.utility_id_ferc1.notnull())
    ]

    remaining = amu.drop_records_with_matches(
        entity=entity,
        matches_new=matches,
        existing_glue_df=overrides,
        matches_existing=override_matches,
        unmatched_existing=override_unmatched,
    )
    # Ensure that the id from the matches input that should be dropped is not present in the remaining df.
    if entity == "ferc1":
        # For the first case we expected 200 to be dropped
        assert 200 not in remaining.utility_id_ferc1.dropna().to_numpy()
    else:
        # For the second case we expected the row with EIA id 22 to be dropped; ensure 22 not in utility_id_eia
        assert 22 not in remaining.utility_id_eia.dropna().to_numpy()


@pytest.mark.parametrize(
    "overrides_rows, matches_rows, expected_new_rows",
    [
        # Two matches in same group -> single group increment assignment
        (
            [
                {"utility_id_ferc1": 1, "utility_id_eia": pd.NA, "utility_id_pudl": 9},
                {"utility_id_ferc1": pd.NA, "utility_id_eia": 2, "utility_id_pudl": 10},
            ],
            [
                {
                    "utility_id_ferc1": 100,
                    "utility_id_eia": 200,
                    "utility_id_pudl_ferc1": pd.NA,
                    "utility_id_pudl_eia": pd.NA,
                    "cleaned_utility_name": "acme",
                },
                {
                    "utility_id_ferc1": 101,
                    "utility_id_eia": 201,
                    "utility_id_pudl_ferc1": pd.NA,
                    "utility_id_pudl_eia": pd.NA,
                    "cleaned_utility_name": "acme",
                },
            ],
            2,
        ),
        # Two matches in different groups -> two new groups / ids
        (
            [
                {"utility_id_ferc1": 10, "utility_id_eia": pd.NA, "utility_id_pudl": 5},
            ],
            [
                {
                    "utility_id_ferc1": 110,
                    "utility_id_eia": 210,
                    "utility_id_pudl_ferc1": pd.NA,
                    "utility_id_pudl_eia": pd.NA,
                    "cleaned_utility_name": "alpha",
                },
                {
                    "utility_id_ferc1": 111,
                    "utility_id_eia": 211,
                    "utility_id_pudl_ferc1": pd.NA,
                    "utility_id_pudl_eia": pd.NA,
                    "cleaned_utility_name": "beta",
                },
            ],
            2,
        ),
    ],
    ids=["same-group-two-matches", "two-groups-two-matches"],
)
def test_add_new_matches_assigns_new_ids_and_notes_param(
    overrides_rows, matches_rows, expected_new_rows
):
    """Parametrized test that add_new_matches_to_dataframe assigns new PUDL ids and appends rows with notes."""
    existing_glue_df = pd.DataFrame(overrides_rows)
    matches_new = pd.DataFrame(matches_rows)

    updated = amu.add_new_matches_to_dataframe(
        matches_new=matches_new, existing_glue_df=existing_glue_df
    )

    # Expect the updated dataframe length to equal original + number of new matches
    assert len(updated) == len(existing_glue_df) + expected_new_rows
    # Notes column should exist
    assert "notes" in updated.columns
    # The maximum utility_id_pudl should be >= previous max
    assert updated.utility_id_pudl.max() >= existing_glue_df.utility_id_pudl.max()


@pytest.mark.parametrize(
    "overrides_rows, matches_rows",
    [
        # FERC and EIA present in overrides with different PUDL ids -> raise
        (
            [
                {
                    "utility_id_ferc1": 100,
                    "utility_id_eia": pd.NA,
                    "utility_id_pudl": 1,
                },
                {
                    "utility_id_ferc1": pd.NA,
                    "utility_id_eia": 200,
                    "utility_id_pudl": 2,
                },
            ],
            [
                {
                    "utility_id_ferc1": 100,
                    "utility_id_eia": 200,
                    "utility_id_pudl_ferc1": 1,
                    "utility_id_pudl_eia": 2,
                    "cleaned_utility_name": "complicated inc",
                }
            ],
        ),
        # Swapped roles: still should raise
        (
            [
                {
                    "utility_id_ferc1": 500,
                    "utility_id_eia": pd.NA,
                    "utility_id_pudl": 7,
                },
                {
                    "utility_id_ferc1": pd.NA,
                    "utility_id_eia": 600,
                    "utility_id_pudl": 8,
                },
            ],
            [
                {
                    "utility_id_ferc1": 500,
                    "utility_id_eia": 600,
                    "utility_id_pudl_ferc1": 7,
                    "utility_id_pudl_eia": 8,
                    "cleaned_utility_name": "another complex",
                }
            ],
        ),
    ],
    ids=["complex-match-1", "complex-match-2"],
)
def test_add_new_matches_raises_on_complex_match_param(overrides_rows, matches_rows):
    """Parametrized cases where the match is complex and should raise a ValueError."""
    existing_glue_df = pd.DataFrame(overrides_rows)
    matches_new = pd.DataFrame(matches_rows)

    with pytest.raises(ValueError):
        amu.add_new_matches_to_dataframe(
            matches_new=matches_new, existing_glue_df=existing_glue_df
        )
