"""Clean the FERC Company Identifier table."""

from dagster import asset


@asset(  # io_manager_key="pudl_io_manager"
)
def core_ferccid__data(raw_ferccid__data):
    """Clean the FERC Company Identifier table."""
    out_df = raw_ferccid__data.copy()
    out_df.columns = out_df.columns.str.lower()
    # dtypes
    # primary key
    # standardize field names
    return out_df
