
"""Routines specific to cleaning up EPA CEMS hourly data."""

import pandas as pd
import numpy as np

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
    # Make an operating timestamp
    df["operating_datetime"] = (
        pd.to_datetime(df["op_date"], format=r"%m-%d-%Y", exact=True, cache=True) +
        pd.to_timedelta(df["op_hour"], unit="h")
    )
    del df["op_hour"], df["op_date"]

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
    raise NotImplementedError(
        "This op_interval isn't included in the " +
        "SQLAlchemy model yet. Figure out what you want to do with interval " +
        "data first."
    )
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


def add_facility_id_unit_id_epa(df):
    """Harmonize columns that are added later

    The load into Postgres checks for consistent column names, and these
    two columns aren't present before August 2008, so add them in.

    Args:
        df (pd.DataFrame): A CEMS dataframe
    Returns:
        The same DataFrame, guaranteed to have facility_id and unit_id_epa cols.
    """
    if "facility_id" not in df.columns:
        df["facility_id"] = np.NaN
    if "unit_id_epa" not in df.columns:
        df["unit_id_epa"] = np.NaN
    return df


def _all_na_or_values(series, values):
    """Test whether every element in the series is either missing or in values

    This is fiddly because isin() changes behavior if the series is totally NaN
    (because of type issues)
    Demo: x = pd.DataFrame({'a': ['x', np.NaN], 'b': [np.NaN, np.NaN]})
    x.isin({'x', np.NaN})

    Args:
        series (pd.Series): A data column
        values (set): A set of values
    Returns:
        True or False
    """
    series_excl_na = series[series.notna()]
    if not len(series_excl_na):
        out = True
    elif series_excl_na.isin(values).all():
        out = True
    else:
        out = False
    return out


def drop_calculated_rates(df):
    """Drop these calculated rates because they don't provide any information.

    If you want these, you can just use a view.

    It's always true that so2_rate_lbs_mmbtu == so2_mass_lbs / heat_content_mmbtu
    and co2_rate_tons_mmbtu == co2_mass_tons / heat_content_mmbtu,
    but the same does not hold for NOx.

    Args:
        df (pd.DataFrame): A CEMS dataframe
    Returns:
        The same DataFrame, without the variables so2_rate_measure_flg,
        so2_rate_lbs_mmbtu, co2_rate_measure_flg, or co2_rate_tons_mmbtu
    """

    if not _all_na_or_values(df["so2_rate_measure_flg"], {"Calculated"}):
        raise AssertionError()
    if not _all_na_or_values(df["co2_rate_measure_flg"], {"Calculated"}):
        raise AssertionError()

    del df["so2_rate_measure_flg"], df["so2_rate_lbs_mmbtu"]
    del df["co2_rate_measure_flg"], df["co2_rate_tons_mmbtu"]

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
        for yr_st, raw_df in raw_df_dict.items():
            df = (
                raw_df.pipe(add_facility_id_unit_id_epa)
                .pipe(drop_calculated_rates)
                .pipe(fix_up_dates)
                .pipe(harmonize_eia_epa_orispl)
                .pipe(add_facility_id_unit_id_epa)
            )
            yield {yr_st: df}
