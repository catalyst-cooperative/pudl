
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
    Calculate heatrates and flag implausible values

    Input:
        A CEMS hourly dataframe for one year-month-state
    Output:
        The same data, with additional columns heatrate (float) and
        bad_heatrate (bool)
    """
    # TODO: actually implement this!
    df['heatrate'] = 0.0
    df['bad_heatrate'] = False
    return df



def transform(epacems_raw_dfs, verbose=True):
    """Transform EPA CEMS hourly ."""
    if verbose:
        print("Transforming tables from EPA CEMS:")
    # epacems_raw_dfs is a generator. Pull out one dataframe, run it through
    # a transformation pipeline, and yield it back as another generator.
    for raw_df_dict in epacems_raw_dfs:
        # There's currently only one dataframe in this dict at a time, but
        # that could be changed
        for yr_mo_st, raw_df in raw_df_dict.items():
            yield {yr_mo_st: raw_df.pipe(calc_heatrate)}
