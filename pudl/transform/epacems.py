
"""Routines specific to cleaning up EPA CEMS hourly data."""

import pandas as pd
import numpy as np
import pudl.transform.pudl
import pudl.constants as pc

###############################################################################
###############################################################################
# DATATABLE TRANSFORM FUNCTIONS
###############################################################################
###############################################################################


def fix_up_dates(df):
    """Fix the dates for the CEMS data

    Three date/datetime changes (not all implemented)
    - Make op_date a DATE type
    - Make an appropriate INTERVAL type (not implemented)
    - Add a UTC timestamp (not implemented)

    Args:
        df(pandas.DataFrame): A CEMS hourly dataframe for one year-month-state
    Output:
        pandas.DataFrame: The same data, with an op_datetime column added and
        the op_date and op_hour columns removed
    """
    # Convert to interval:
    # df = convert_time_to_interval(df)

    # Convert op_date and op_hour from string and integer to datetime:
    # Note that doing this conversion, rather than reading the CSV with
    # `parse_dates=True` is >10x faster.
    df["op_datetime"] = pd.to_datetime(
        df['op_date'].str.cat(df['op_hour'].astype(str), sep='-'),
        format="%m-%d-%Y-%H",
        exact=True,
        cache=True)
    del df['op_date']
    del df['op_hour']

    # Add UTC timestamp ... not done.
    return df


def convert_time_to_interval(df):
    """
    Reframe the CEMS hourly time as an interval -- NOT YET IMPLEMENTED

    NOTE: this function is currently written to be called *before* converting
    op_date to a Date.

    Args:
        df(pandas.DataFrame): A CEMS hourly dataframe for one year-month-state
    Output:
        pandas.DataFrame: The same data, with an op_interval column added.
    """
    raise NotImplementedError("This op_interval isn't included in the " +
        "SQLAlchemy model yet. Figure out what you want to do with interval " +
        "data first.")
    df['op_interval'] = pd.to_datetime(
        df['op_date'].str.cat(df['op_hour'].astype(str), sep='-'),
        format="%m-%d-%Y-%H",
        box=False,
        exact=True,
        cache=True).dt.to_period("H")
    return df


def harmonize_eia_epa_orispl(df):
    """
    Harmonize the ORISPL code to match the EIA data -- NOT YET IMPLEMENTED

    Args:
        df(pandas.DataFrame): A CEMS hourly dataframe for one year-month-state
    Output:
        pandas.DataFrame: The same data, with the ORISPL plant codes corrected
        to  match the EIA.

    The EIA plant IDs and CEMS ORISPL codes almost match, but not quite. See
    https://www.epa.gov/sites/production/files/2018-02/documents/egrid2016_technicalsupportdocument_0.pdf#page=104
    for an example.
    """
    # TODO: implement this.
    return df


def transform(epacems_raw_dfs, verbose=True):
    """Transform EPA CEMS hourly"""
    if verbose:
        print("Transforming tables from EPA CEMS:")
    # epacems_raw_dfs is a generator. Pull out one dataframe, run it through
    # a transformation pipeline, and yield it back as another generator.
    for raw_df_dict in epacems_raw_dfs:
        # There's currently only one dataframe in this dict at a time, but
        # that could be changed if you want.
        for yr_mo_st, raw_df in raw_df_dict.items():
            df = (
                raw_df.pipe(fix_up_dates)
                .pipe(harmonize_eia_epa_orispl)
            )
            yield {yr_mo_st: df}
