"""Clean the FERC Company Identifier table."""

from dagster import asset

from pudl.helpers import convert_cols_dtypes


@asset(  # io_manager_key="pudl_io_manager"
)
def core_ferccid__data(raw_ferccid__data):
    """Clean the FERC Company Identifier table."""
    cid_df = raw_ferccid__data.copy()
    cid_df.columns = cid_df.columns.str.lower()
    # override with field descriptions from data dictionary
    cid_df = cid_df.rename(
        columns={
            "organization_name": "company_name",
            "address": "street_address",
            "address2": "address_2",
            "zip": "zip_code",
        }
    )
    # split out 4 digit zip code suffix from zip code

    # na handling? is there an existing helper for string NA value handling?

    cid_df = convert_cols_dtypes(cid_df)
    # Make CID the primary key by dropping
    # the CID values we know have duplicates that should be removed.
    known_dupe = cid_df.loc[
        (cid_df["cid"] == "C003521")
        & (cid_df["company_name"] == "Chestnut Flats Wind Lessee, LLC")
    ]
    cid_df = cid_df.drop(known_dupe.index)
    if not cid_df["cid"].is_unique:
        raise ValueError(
            f"Duplicate CIDs found in core_ferccid_data: {cid_df[cid_df['cid'].duplicated(keep=False)]}"
        )

    # fk constraints to other tables with CID?

    # add regex constraints to company website in field metadata
    return cid_df
