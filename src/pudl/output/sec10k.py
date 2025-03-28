"""Denormalized output tables for the SEC 10-K assets."""

import dagster as dg
import pandas as pd


@dg.asset(
    # io_manager_key="pudl_io_manager",
    io_manager_key="parquet_io_manager",
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
    )
    company_info["source_url"] = (
        "https://www.sec.gov/Archives/edgar/data/"
        + company_info["filename_sec10k"]
        + ".txt"
    )
    return company_info
