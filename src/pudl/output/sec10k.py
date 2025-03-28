"""Denormalized output tables for the SEC 10-K assets."""

import dagster as dg
import pandas as pd


@dg.asset(
    # io_manager_key="pudl_io_manager",
    group_name="out_sec10k",
)
def out_sec10k__quarterly_company_information(
    core_sec10k__quarterly_company_information: pd.DataFrame,
    core_sec10k__quarterly_filings: pd.DataFrame,
    core_sec10k__assn_sec10k_filers_and_eia_utilities: pd.DataFrame,
    core_eia__entity_utilities: pd.DataFrame,
) -> pd.DataFrame:
    """Company information extracted from SEC10k filings and matched to EIA utilities."""
    out_df = (
        pd.merge(
            left=core_sec10k__quarterly_company_information,
            right=core_sec10k__assn_sec10k_filers_and_eia_utilities,
            how="left",
            on="central_index_key",
        )
        .merge(
            core_eia__entity_utilities.loc[:, ["utility_id_eia", "utility_name_eia"]],
            how="left",
            on="utility_id_eia",
        )
        .merge(
            core_sec10k__quarterly_filings.loc[
                :, ["filename_sec10k", "report_date", "filing_date"]
            ],
            how="left",
            on="filename_sec10k",
            validate="one_to_many",
        )
        .assign(
            source_url=lambda x: f"https://www.sec.gov/Archives/edgar/data/{x.filename_sec10k}.txt"
        )
    )
    return out_df
