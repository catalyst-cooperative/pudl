"""Routines specific to cleaning up EPA CEMS hourly data."""
import datetime
import logging
import pathlib

import numpy as np
import pandas as pd

import pudl

logger = logging.getLogger(__name__)
###############################################################################
###############################################################################
# DATATABLE TRANSFORM FUNCTIONS
###############################################################################
###############################################################################


def fix_up_dates(df, plant_utc_offset):
    """
    Fix the dates for the CEMS data.

    Args:
        df (pandas.DataFrame): A CEMS hourly dataframe for one year-month-state
        plant_utc_offset (pandas.DataFrame): A dataframe of plants' timezones

    Returns:
        pandas.DataFrame: The same data, with an op_datetime_utc column added
        and the op_date and op_hour columns removed

    """
    df = (
        df.assign(
            # Convert op_date and op_hour from string and integer to datetime:
            # Note that doing this conversion, rather than reading the CSV with
            # `parse_dates=True`, is >10x faster.
            op_datetime_naive=lambda x:
            # Read the date as a datetime, so all the dates are midnight
            # Mark as UTC (it's not true yet, but it will be once we add
            # utc_offsets, and it's easier to do here)
            pd.to_datetime(x.op_date, format=r"%m-%d-%Y",
                           exact=True, cache=True, utc=True) +
            # Add the hour
            pd.to_timedelta(x.op_hour, unit="h")
        )
        .merge(plant_utc_offset, how="left", on="plant_id_eia")
    )

    # Some of the timezones in the plants_entity_eia table may be missing,
    # but none of the CEMS plants should be.
    if not df["utc_offset"].notna().all():
        missing_plants = df.loc[df["utc_offset"].isna(),
                                "plant_id_eia"].unique()
        raise ValueError(
            f"utc_offset should never be missing for CEMS plants, but was "
            f"missing for these: {str(list(missing_plants))}"
        )
    # Add the offset from UTC. CEMS data don't have DST, so the offset is
    # always the same for a given plant.
    df["operating_datetime_utc"] = df["op_datetime_naive"] - df["utc_offset"]
    del df["op_date"], df["op_hour"], df["op_datetime_naive"], df["utc_offset"]
    return df


def _load_plant_utc_offset(datapkg_dir):
    """Load the UTC offset each EIA plant.

    CEMS times don't change for DST, so we get get the UTC offset by using the
    offset for the plants' timezones in January.

    Args:
        datapkg_dir (path-like) : Path to the directory of the datapackage
            which is currently being assembled.

    Returns:
        pandas.DataFrame: With columns plant_id_eia and utc_offset

    """
    import pytz

    jan1 = datetime.datetime(2011, 1, 1)  # year doesn't matter
    timezones = (
        pd.read_csv(
            pathlib.Path(datapkg_dir, 'data/plants_entity_eia.csv'),
            usecols=["plant_id_eia", "timezone"],
            dtype={"plant_id_eia": "Int64", "timezone": pd.StringDtype()})
        .replace(to_replace="None", value=pd.NA)
        .dropna()
    )

    timezones["utc_offset"] = (
        timezones["timezone"]
        .apply(lambda tz: pytz.timezone(tz).localize(jan1).utcoffset())
    )
    del timezones["timezone"]
    return timezones


def harmonize_eia_epa_orispl(df):
    """
    Harmonize the ORISPL code to match the EIA data -- NOT YET IMPLEMENTED.

    The EIA plant IDs and CEMS ORISPL codes almost match, but not quite. See
    https://www.epa.gov/sites/production/files/2018-02/documents/egrid2016_technicalsupportdocument_0.pdf#page=104
    for an example.

    Note that this transformation needs to be run *before* fix_up_dates,
    because fix_up_dates uses the plant ID to look up timezones.

    Args:
        df (pandas.DataFrame): A CEMS hourly dataframe for one year-month-state

    Returns:
        pandas.DataFrame: The same data, with the ORISPL plant codes corrected
        to match the EIA plant IDs.

    Todo:
        Actually implement the function...

    """
    return df


def add_facility_id_unit_id_epa(df):
    """
    Harmonize columns that are added later.

    The datapackage validation checks for consistent column names, and these
    two columns aren't present before August 2008, so this adds them in.

    Args:
        df (pandas.DataFrame): A CEMS dataframe

    Returns:
        pandas.Dataframe: The same DataFrame guaranteed to have int facility_id
        and unit_id_epa cols.

    """
    if ("facility_id" not in df.columns) or ("unit_id_epa" not in df.columns):
        # Can't just assign np.NaN and get an integer NaN, so make a new array
        # with the right shape:
        na_col = pd.array(np.full(df.shape[0], np.NaN), dtype="Int64")
        if "facility_id" not in df.columns:
            df["facility_id"] = na_col
        if "unit_id_epa" not in df.columns:
            df["unit_id_epa"] = na_col
    return df


def _all_na_or_values(series, values):
    """
    Test whether every element in the series is either missing or in values.

    This is fiddly because isin() changes behavior if the series is totally NaN
    (because of type issues)

    Example: x = pd.DataFrame({'a': ['x', np.NaN], 'b': [np.NaN, np.NaN]})
        x.isin({'x', np.NaN})

    Args:
        series (pd.Series): A data column
        values (set): A set of values

    Returns:
        bool: True or False, whether the elements are missing or in values

    """
    series_excl_na = series[series.notna()]
    if not len(series_excl_na):
        out = True
    elif series_excl_na.isin(values).all():
        out = True
    else:
        out = False
    return out


def correct_gross_load_mw(df):
    """
    Fix values of gross load that are wrong by orders of magnitude.

    Args:
        df (pandas.DataFrame): A CEMS dataframe

    Returns:
        pandas.DataFrame: The same DataFrame with corrected gross load values.

    """
    # Largest fossil plant is something like 3500 MW, and the largest unit
    # in the EIA 860 is less than 1500. Therefore, assume they've done it
    # wrong (by writing KWh) if they report more.
    # (There is a cogen unit, 54634 unit 1, that regularly reports around
    # 1700 MW. I'm assuming they're correct.)
    # This is rare, so don't bother most of the time.
    bad = df["gross_load_mw"] > 2000
    if bad.any():
        df.loc[bad, "gross_load_mw"] = df.loc[bad, "gross_load_mw"] / 1000
    return df


def transform(epacems_raw_dfs, datapkg_dir):
    """
    Transform EPA CEMS hourly data for use in datapackage export.

    Todo:
        Incomplete docstring.

    """
    # epacems_raw_dfs is a generator. Pull out one dataframe, run it through
    # a transformation pipeline, and yield it back as another generator.
    plant_utc_offset = _load_plant_utc_offset(datapkg_dir)
    for raw_df_dict in epacems_raw_dfs:
        # There's currently only one dataframe in this dict at a time, but
        # that could be changed if you want.
        # Also, the type conversion is being done here so that it happens
        # inside the generator -- rather than following the same pattern as
        # in the EIA type conversions.
        for yr_st, raw_df in raw_df_dict.items():
            df = (
                raw_df.fillna({
                    "gross_load_mw": 0.0,
                    "heat_content_mmbtu": 0.0
                })
                .pipe(harmonize_eia_epa_orispl)
                .pipe(fix_up_dates, plant_utc_offset=plant_utc_offset)
                .pipe(add_facility_id_unit_id_epa)
                .pipe(correct_gross_load_mw)
                .pipe(pudl.helpers.convert_cols_dtypes,
                      "epacems", "hourly_emissions_epacems")
            )
            yield {yr_st: df}
