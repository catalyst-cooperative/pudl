"""A CLI tool for matching FERC and EIA utilities."""

import importlib
from typing import Literal

import click
import pandas as pd

from pudl.analysis.record_linkage import name_cleaner
from pudl.helpers import get_parquet_table
from pudl.logging_helpers import get_logger

logger = get_logger(__name__)

UTILITY_NAME_CLEANER = name_cleaner.CompanyNameCleaner(
    cleaning_rules_list=[
        "remove_word_the_from_the_end",
        "remove_word_the_from_the_beginning",
        "replace_ampersand_by_AND",
        "replace_hyphen_by_space",
        "replace_underscore_by_space",
        "remove_all_punctuation",
        "remove_math_symbols",
        "add_space_before_opening_parentheses",
        "add_space_after_closing_parentheses",
        "remove_parentheses",
        "remove_brackets",
        "remove_curly_brackets",
        "enforce_single_space_between_words",
    ],
    legal_term_location=2,
)


def clean_utility_name(col):
    """Apply standard cleaning steps to the utility name column."""
    col = col.fillna(pd.NA).str.strip().str.lower()
    col = UTILITY_NAME_CLEANER.apply_name_cleaning(col).str.strip()
    col = col.replace("", pd.NA)
    return col


def match_utility_names(eia_df: pd.DataFrame, ferc_df: pd.DataFrame):
    """Match FERC and EIA utilities based on their utility names.

    We note how many of these records are already matched to one another, and
    ignore these matches.
    """
    # Try an exact match on the cleaned name
    matches = ferc_df.merge(
        eia_df,
        on="cleaned_utility_name",
        how="inner",
        validate="m:m",
        suffixes=["_ferc1", "_eia"],
    )

    # Check to see how many of these are already matched.
    already_matched = matches[
        (matches.utility_id_pudl_ferc1 == matches.utility_id_pudl_eia)
    ]
    logger.info(f"{len(already_matched)} matches have already been made.")

    # Which utilities aren't currently matched but should be under this model?
    # This includes new utilities (not yet assigned a PUDL ID) or utilities where the FERC and EIA PUDL IDs are not the same.
    matches = matches[
        (matches.utility_id_pudl_ferc1 != matches.utility_id_pudl_eia)
        | (matches.utility_id_pudl_ferc1 is None)
        | (matches.utility_id_pudl_eia is None)
    ]
    return matches


def get_existing_overrides():
    """Read in the existing handmade glue spreadsheet."""
    return pd.read_csv(
        importlib.resources.files("pudl.package_data.glue") / "utility_id_pudl.csv"
    )


def drop_records_with_matches(
    entity: Literal["ferc1", "eia"],
    matches: pd.DataFrame,
    overrides: pd.DataFrame,
    override_matches: pd.DataFrame,
    override_unmatched: pd.DataFrame,
):
    """Drop records in the original dataframe where matches have been found.

    This takes the original overrides dataframe, and drops records which were
    previously unmatched and have been matched using the automated matching method.
    We do this in preparation for adding the new matches to the spreadsheet.
    """
    utility_column = f"utility_id_{entity}"
    utility_pudl_column = f"utility_id_pudl_{entity}"
    matching_utility_column = (
        "utility_id_ferc1" if entity == "eia" else "utility_id_eia"
    )

    # Get all records where:
    # 1) The utility ID of the entity is unmatched in the override CSV
    # 2) The PUDL-assigned utility ID isn't already used in any match.
    entity_matched = matches[
        matches[utility_column].isin(override_unmatched[utility_column])
        & (~matches[utility_pudl_column].isin(override_matches.utility_id_pudl))
    ]
    drop_records = overrides[
        overrides[utility_column].isin(entity_matched[utility_column])
    ]
    # Check that we aren't dropping any EIA matches
    assert drop_records[matching_utility_column].isnull().all()
    # Check that no other plants are mapped to this ID before we drop it
    pudl_ids = drop_records.utility_id_pudl
    assert overrides[
        (overrides.utility_id_pudl.isin(pudl_ids))
        & (overrides[matching_utility_column].notnull())
    ].empty
    # Drop these unmatched records so we can replace them with matches.
    logger.info(
        f"Dropping {len(drop_records)} unmatched {entity} records to replace with matches."
    )
    return overrides.drop(drop_records.index, axis=0)


def add_new_matches_to_spreadsheet(matches: pd.DataFrame, overrides: pd.DataFrame):
    """Add new matches to existing hand-mapped dataframe.

    We add new matches and assign them new PUDL utility IDs, following one of the
    following scenarios:
    1) If the PUDL utility ID already exists in another match, we keep that utility ID
    to avoid splitting up existing sets of matches.
    2) If the PUDL utility ID doesn't show up anywhere else (i.e., this record was
    previously unmatched to any other utility), we create a new auto-incremented PUDL ID.
    3) If the PUDL utility ID was previously matched to both another EIA and another
    FERC utility, this poses a challenge that needs to be resolved by hand and we raise
    a ValueError.

    Once all PUDL utility IDs are assigned, we flag all records where multiple FERC and
    multiple EIA utilities are matched together. These are uncommon cases and benefit
    from manual review to ensure that no unexpected connections have been created.
    """
    final_updates = []
    matches["utility_id_pudl"] = pd.NA

    for _i, match in matches.iterrows():
        if match.utility_id_ferc1 in set(overrides.utility_id_ferc1):
            if match.utility_id_eia in set(overrides.utility_id_eia):
                raise ValueError(
                    f"Found a complex match that connects to different FERC and EIA utilities. This will require manual debugging: {match}"
                )
            match.utility_id_pudl = match.utility_id_pudl_ferc1
        elif match.utility_id_eia in set(overrides.utility_id_eia):
            if match.utility_id_ferc1 in set(overrides.utility_id_ferc1):
                raise ValueError(
                    f"Found a complex match that connects to different FERC and EIA utilities. This will require manual debugging: {match}"
                )
            match.utility_id_pudl = match.utility_id_pudl_eia
        final_updates.append(match)

    updates_df = pd.DataFrame(final_updates)
    updates_df = updates_df.drop(
        columns=["utility_id_pudl_ferc1", "utility_id_pudl_eia"]
    )
    # Whichever records have a blank PUDL ID have no linkage to other existing records
    # and can be reassigned without impact.
    # Reassign the blank PUDL IDs by auto-incrementing from the current max record value.
    # First, assign a group ID for each group of cleaned utility names.
    updates_df = updates_df.assign(
        group_id=lambda x: x.groupby("cleaned_utility_name").ngroup()
    )
    # Then use this value to fill NAs in the utility ID PUDL.
    updates_df.utility_id_pudl = updates_df.utility_id_pudl.fillna(
        updates_df.group_id + max(overrides.utility_id_pudl)
    )
    updates_df = updates_df.drop(columns=["group_id", "cleaned_utility_name"])
    # Add a note to these records to help us quickly identify which were matched automatically.s
    updates_df["notes"] = (
        f"These records were automatically matched based on utility name on {pd.to_datetime('today').strftime('%m/%d/%Y')}"
    )
    logger.info(
        f"Adding {len(updates_df)} utility matches to the override spreadsheet. Remember to manually review these."
    )
    updated_overrides = pd.concat([overrides, updates_df]).sort_values(
        "utility_id_pudl"
    )

    # Run some sanity checks
    # Flag all records where one utility ID corresponds to more than one FERC AND more than one EIA utility.
    ids_per_utility = updated_overrides.groupby("utility_id_pudl")[
        ["utility_id_ferc1", "utility_id_eia"]
    ].nunique()
    multi_utility_ids = ids_per_utility[
        (ids_per_utility.utility_id_ferc1 > 1) & (ids_per_utility.utility_id_eia > 1)
    ].index
    logger.warning(
        "The following records have more than one FERC and more than one EIA utility matched to them. Please manually review!"
    )
    logger.warning(
        updated_overrides[updated_overrides.utility_id_pudl.isin(multi_utility_ids)]
    )
    return updated_overrides


def write_updated_matches(test_run: bool, dataframe: pd.DataFrame):
    """Write the updated matching spreadsheet to disk."""
    if test_run:
        csv_path = (
            importlib.resources.files("pudl.package_data.glue")
            / "utility_id_pudl_test_update.csv"
        )
    else:
        csv_path = (
            importlib.resources.files("pudl.package_data.glue") / "utility_id_pudl.csv"
        )
    logger.info(f"Writing matches to {csv_path}")
    dataframe.to_csv(csv_path, index=False)


@click.command(
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.option(
    "--test-run",
    is_flag=True,
    default=False,
    help="If passed, will save the updated spreadsheet to a test file rather than overwriting the existing package data.",
)
def main(test_run: bool):
    """Match EIA and FERC utilities based on utility name alone."""
    # Read in the data, keeping only utility IDs and name
    eia_df = get_parquet_table(
        "out_eia__yearly_utilities",
        columns=["utility_id_eia", "utility_id_pudl", "utility_name_eia"],
    ).drop_duplicates()
    ferc_df = get_parquet_table("core_pudl__assn_ferc1_pudl_utilities")
    # Clean the data
    eia_df["cleaned_utility_name"] = clean_utility_name(eia_df["utility_name_eia"])
    ferc_df["cleaned_utility_name"] = clean_utility_name(ferc_df["utility_name_ferc1"])
    # Exactly match the cleaned name
    matched_utilities = match_utility_names(eia_df=eia_df, ferc_df=ferc_df)

    # Warn about duplicated utilities
    logger.warning("The following utilities are matched to more than one FERC ID:")
    logger.info(
        matched_utilities[matched_utilities.utility_id_ferc1.duplicated(keep=False)]
    )
    logger.warning("The following utilities are matched to more than one EIA ID:")
    logger.info(
        matched_utilities[matched_utilities.utility_id_eia.duplicated(keep=False)]
    )

    # Get the override spreadsheet
    overrides = get_existing_overrides()
    ## Get matches from the CSV
    override_matches = overrides[
        overrides.utility_id_eia.notnull() & overrides.utility_id_ferc1.notnull()
    ]
    # Unmatched records
    unmatched = overrides[
        ~(overrides.utility_id_eia.notnull() & overrides.utility_id_ferc1.notnull())
    ]

    for entity in ["ferc1", "eia"]:
        overrides = drop_records_with_matches(
            entity=entity,
            matches=matched_utilities,
            overrides=overrides,
            override_matches=override_matches,
            override_unmatched=unmatched,
        )

    updated_spreadsheet = add_new_matches_to_spreadsheet(
        matches=matched_utilities, overrides=overrides
    )

    write_updated_matches(test_run=test_run, dataframe=updated_spreadsheet)


if __name__ == "__main__":
    main()
