
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


def calc_heatrate(df):
    """
    Calculate heatrates and flag implausible values -- NOT YET IMPLEMENTED

    Args:
        df(pandas.DataFrame): A CEMS hourly dataframe for one year-month-state
    Output:
        pandas.DataFrame: The same data, with additional columns
        heatrate (float) and bad_heatrate (bool)
    """
    # TODO: actually implement this!
    df["heatrate"] = np.NaN
    df["bad_heatrate"] = True
    return df


def convert_time_to_interval(df):
    """
    Reframe the CEMS hourly time as an interval -- NOT YET IMPLEMENTED

    Args:
        df(pandas.DataFrame): A CEMS hourly dataframe for one year-month-state
    Output:
        pandas.DataFrame: The same data, with the hour timestamp converted to
        interval.
    """
    # TODO: implement this.
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
                raw_df.pipe(convert_time_to_interval)
                .pipe(calc_heatrate)
                .pipe(harmonize_eia_epa_orispl)
            )
            yield {yr_mo_st: df}
