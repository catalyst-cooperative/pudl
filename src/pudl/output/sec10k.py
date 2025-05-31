"""Denormalized output tables for the SEC 10-K assets.

These tables are created by joining the raw SEC 10-K tables with other data from the
PUDL database, and enriching them with additional information. The resulting tables are
more user-friendly and easier to work with than the normalized core tables.
"""

import dagster as dg
import pandas as pd

from pudl import logging_helpers
from pudl.transform.sec10k import _extract_filer_cik_from_filename

logger = logging_helpers.get_logger(__name__)


def _filename_sec10k_to_source_url(
    filename_sec10k: pd.Series,
) -> pd.Series:
    """Construct the source URL for SEC 10-K filings."""
    return "https://www.sec.gov/Archives/edgar/data/" + filename_sec10k + ".txt"


@dg.asset(
    io_manager_key="pudl_io_manager",
    group_name="out_sec10k",
)
def out_sec10k__quarterly_filings(
    core_sec10k__quarterly_filings: pd.DataFrame,
) -> pd.DataFrame:
    """Denormalized table for SEC 10-K quarterly filings.

    This table contains the basic information about the quarterly filings, including
    the filing date, report date, and the URL to the filing.
    """
    # Construct the source URL so people can see where the data came from.
    return core_sec10k__quarterly_filings.assign(
        source_url=lambda x: _filename_sec10k_to_source_url(x["filename_sec10k"])
    )


@dg.asset(
    io_manager_key="pudl_io_manager",
    group_name="out_sec10k",
)
def out_sec10k__quarterly_company_information(
    core_sec10k__quarterly_company_information: pd.DataFrame,
    core_sec10k__quarterly_filings: pd.DataFrame,
    core_sec10k__assn_sec10k_filers_and_eia_utilities: pd.DataFrame,
    core_eia__entity_utilities: pd.DataFrame,
) -> pd.DataFrame:
    """Company information extracted from SEC10k filings and matched to EIA utilities."""
    company_info = (
        pd.merge(
            left=core_sec10k__quarterly_company_information,
            right=core_sec10k__assn_sec10k_filers_and_eia_utilities.loc[
                :, ["central_index_key", "utility_id_eia"]
            ],
            how="left",
            on="central_index_key",
            validate="many_to_one",
        )
        .merge(
            core_eia__entity_utilities.loc[:, ["utility_id_eia", "utility_name_eia"]],
            how="left",
            on="utility_id_eia",
            validate="many_to_one",
        )
        .merge(
            core_sec10k__quarterly_filings.loc[
                :, ["filename_sec10k", "report_date", "filing_date"]
            ],
            how="left",
            on="filename_sec10k",
            validate="many_to_one",
        )
        .convert_dtypes()
    ).assign(source_url=lambda x: _filename_sec10k_to_source_url(x["filename_sec10k"]))
    return company_info


@dg.asset(
    io_manager_key="pudl_io_manager",
    group_name="out_sec10k",
)
def out_sec10k__changelog_company_name(
    core_sec10k__changelog_company_name: pd.DataFrame,
) -> pd.DataFrame:
    """Denormalized table for company name changes from SEC 10-K filings.

    The original data contains only the former name and date of the name change, leaving
    the current name out. This asset constructs a column with the new company name in
    it, by shifting the old name column by one row within each central_index_key group,
    and then fills in the last new company name value with the current company name.
    """
    name_changelog = core_sec10k__changelog_company_name.sort_values(
        ["central_index_key", "name_change_date"]
    )
    # Only a handdful of empty company names. Replace with the empty string to avoid
    # inappropriately filling them with the current name when constructing new name
    # for the most recent name change.
    assert name_changelog["company_name_old"].isna().sum() < 5
    name_changelog = name_changelog.assign(
        company_name_old=lambda x: x["company_name_old"].fillna(""),
        company_name_new=lambda x: x.groupby("central_index_key")["company_name_old"]
        .shift(-1)
        .fillna(x["company_name"]),
    )
    # Drop records where the "name change" is a no-op.
    name_changelog = name_changelog.loc[
        name_changelog["company_name_old"] != name_changelog["company_name_new"]
    ]
    return name_changelog


@dg.asset(
    io_manager_key="pudl_io_manager",
    group_name="out_sec10k",
)
def out_sec10k__parents_and_subsidiaries(
    core_sec10k__quarterly_exhibit_21_company_ownership: pd.DataFrame,
    out_sec10k__quarterly_company_information: pd.DataFrame,
    core_sec10k__assn_exhibit_21_subsidiaries_and_filers: pd.DataFrame,
    core_sec10k__assn_exhibit_21_subsidiaries_and_eia_utilities: pd.DataFrame,
    core_eia__entity_utilities: pd.DataFrame,
) -> pd.DataFrame:
    """Denormalized output table linking SEC 10-K company ownership to EIA Utilities."""
    # In order to merge parent company attributes onto the ownership records we need to
    # assume that the Ex. 21 attachment to a given filing describes the subsidiary
    # companies of the entity filing the 10-K. Given this assumption we can extract
    # the parent company's central index key from the first token of the filename.
    core_sec10k__quarterly_exhibit_21_company_ownership.loc[
        :, "parent_company_central_index_key"
    ] = _extract_filer_cik_from_filename(
        core_sec10k__quarterly_exhibit_21_company_ownership["filename_sec10k"]
    )
    # merge parent company attributes on
    parents_info_df = out_sec10k__quarterly_company_information.add_prefix(
        "parent_company_"
    ).rename(columns={"parent_company_company_name": "parent_company_name"})
    df = (
        core_sec10k__quarterly_exhibit_21_company_ownership.merge(
            parents_info_df,
            how="left",
            left_on=["filename_sec10k", "parent_company_central_index_key"],
            right_on=[
                "parent_company_filename_sec10k",
                "parent_company_central_index_key",
            ],
            validate="many_to_one",
        )
        .drop(columns=["parent_company_filename_sec10k"])
        .rename(
            columns={
                "parent_company_report_date": "report_date",
                "parent_company_filing_date": "filing_date",
            }
        )
    )

    # merge central index key on for subsidiaries that file a 10k
    df = df.merge(
        core_sec10k__assn_exhibit_21_subsidiaries_and_filers,
        how="left",
        on="subsidiary_company_id_sec10k",
        validate="many_to_one",
    ).rename(columns={"central_index_key": "subsidiary_company_central_index_key"})

    # merge utility_id_eia onto subsidiaries
    df = df.merge(
        core_sec10k__assn_exhibit_21_subsidiaries_and_eia_utilities,
        how="left",
        on="subsidiary_company_id_sec10k",
        validate="many_to_one",
    )

    # merge utility name onto subsidiaries
    df = df.merge(
        core_eia__entity_utilities,
        how="left",
        on="utility_id_eia",
        validate="many_to_one",
    ).rename(
        columns={
            "utility_id_eia": "sub_only_utility_id_eia",
            "utility_name_eia": "sub_only_utility_name_eia",
        }
    )
    subs_info_df = out_sec10k__quarterly_company_information.add_prefix(
        "subsidiary_company_"
    ).drop(columns=["subsidiary_company_company_name"])
    # merge subsidiary company attributes on
    df = df.merge(
        subs_info_df,
        how="left",
        left_on=[
            "filename_sec10k",
            "subsidiary_company_central_index_key",
        ],
        right_on=[
            "subsidiary_company_filename_sec10k",
            "subsidiary_company_central_index_key",
        ],
        validate="many_to_one",
    )
    # combine the sub-only and filing-subs EIA utility columns
    df["subsidiary_company_utility_id_eia"] = df[
        "subsidiary_company_utility_id_eia"
    ].fillna(df["sub_only_utility_id_eia"])
    df["subsidiary_company_utility_name_eia"] = df[
        "subsidiary_company_utility_name_eia"
    ].fillna(df["sub_only_utility_name_eia"])
    df = df.drop(columns=["sub_only_utility_id_eia", "sub_only_utility_name_eia"])

    return df
