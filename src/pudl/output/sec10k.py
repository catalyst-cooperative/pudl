"""Denormalized output tables for the SEC 10-K assets.

These tables are created by joining the raw SEC 10-K tables with other data from the
PUDL database, and enriching them with additional information. The resulting tables are
more user-friendly and easier to work with than the normalized core tables.
"""

import dagster as dg
import pandas as pd


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
