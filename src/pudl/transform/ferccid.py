"""Clean the FERC Company Identifier table."""

import pandas as pd
from dagster import asset

from pudl.helpers import convert_cols_dtypes


def clean_cid_string_cols(col: pd.Series) -> pd.Series:
    """Clean string columns: remove unicode, strip whitespace, enforce single spaces, standardize NAs."""
    col = (
        col.str.replace(r"[\x00-\x1f\x7f-\x9f]", "", regex=True)
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
        .replace(to_replace=r"^(?i:\s*|na|nan|none)$", value=pd.NA, regex=True)
    )
    return col


@asset(  # io_manager_key="pudl_io_manager"
)
def core_ferc__entity_companies(
    raw_ferc__entity_companies: pd.DataFrame,
) -> pd.DataFrame:
    """Clean the FERC Company Identifier table."""
    cid_df = raw_ferc__entity_companies
    cid_df.columns = cid_df.columns.str.lower()
    # override with field descriptions from data dictionary
    cid_df = cid_df.rename(
        columns={
            "cid": "company_id_ferc",
            "organization_name": "company_name",
            "address": "street_address",
            "address2": "address_2",
            "zip": "zip_code",
        }
    )
    # split out 4 digit zip code suffix from zip code
    reg = r"(\d{5})(?:-(\d{4}))*"
    cid_df[["zip_code", "zip_code_4"]] = cid_df["zip_code"].str.extract(reg)
    # remove unicode, strip whitespace, enforce single spaces, standardize NA values
    for col in ["company_name", "street_address", "address_2"]:
        cid_df[col] = clean_cid_string_cols(cid_df[col])
    cid_df = convert_cols_dtypes(cid_df)
    # Make CID the primary key by dropping
    # the CID values we know have duplicates that should be removed.
    known_dupe = cid_df.loc[
        (cid_df["company_id_ferc"] == "C003521")
        & (cid_df["company_name"] == "Chestnut Flats Wind Lessee, LLC")
    ]
    cid_df = cid_df.drop(known_dupe.index)
    return cid_df
