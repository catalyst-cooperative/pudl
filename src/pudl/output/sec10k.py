"""Denormalized output tables for the SEC 10-K assets."""

import dagster as dg
import pandas as pd

from pudl.extract.sec10k import _load_table_from_gcs


@dg.asset(
    # io_manager_key="pudl_io_manager",
    group_name="out_sec10k",
)
def out_sec10k__quarterly_company_information(
    core_sec10k__quarterly_company_information: pd.DataFrame,
    core_eia__entity_utilities: pd.DataFrame,
) -> pd.DataFrame:
    """Company information extracted from SEC10k filings and matched to EIA utilities."""
    matched_df = _load_table_from_gcs("out_sec10k__parents_and_subsidiaries")
    matched_df = (
        matched_df[["central_index_key", "utility_id_eia"]]
        .dropna()
        .drop_duplicates(
            subset="central_index_key"
        )  # matches should already be 1-to-1 but drop duplicates to ensure this is true
    )
    out_df = core_sec10k__quarterly_company_information.merge(
        matched_df, how="left", on="central_index_key"
    )
    # merge utility name on
    out_df = out_df.merge(core_eia__entity_utilities, how="left", on="utility_id_eia")
    return out_df


@dg.asset(
    # io_manager_key="pudl_io_manager",
    group_name="out_sec10k",
)
def out_sec10k__changelog_company_name(
    core_sec10k__changelog_company_name: pd.DataFrame,
) -> pd.DataFrame:
    """A denormalized output table for the SEC 10-K company name changelog."""
    return core_sec10k__changelog_company_name
