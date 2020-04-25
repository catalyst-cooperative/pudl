"""Transformation of the FERC Form 714 data."""

import logging
import pathlib
import re

import geopandas
import numpy as np
import pandas as pd

import pudl
import pudl.constants as pc

logger = logging.getLogger(__name__)

##############################################################################
# Constants required for transforming FERC 714
##############################################################################
# Stand-in for the null values...
tz_nulls = {
    np.nan: "XXX",
    "": "XXX",
}

# More detailed fixes on a per respondent basis
tz_fixes = {
    102: {"CPT": "CST"},
    110: {"CPT": "EST"},
    115: {"MS": "MST"},
    118: {
        "CS": "CST",
        "CD": "CDT",
    },
    120: {
        "CTR": "CST",
        "CSR": "CST",
        "CPT": "CST",
        "DST": "CST",
        "XXX": "CST",
    },
    133: {
        "AKS": "AKST",
        "AST": "AKST",
        "AKD": "AKDT",
        "ADT": "AKDT",
    },
    134: {"XXX": "EST"},
    137: {"XXX": "CST"},
    140: {
        "1": "EST",
        "2": "EDT",
        "XXX": "EST",
    },
    141: {"XXX": "CST"},
    143: {"MS": "MST"},
    146: {"DST": "EST"},
    148: {"XXX": "CST"},
    151: {
        "DST": "CDT",
        "XXX": "CST",
    },
    153: {"XXX": "MST"},
    154: {"XXX": "MST"},
    156: {"XXX": "CST"},
    157: {"DST": "EDT"},
    161: {"CPT": "CST"},
    163: {"CPT": "CST"},
    164: {"XXX": "CST"},
    165: {"CS": "CST"},  # Uniform across the year.
    173: {
        "CPT": "CST",
        "XXX": "CST",
    },
    174: {
        "CS": "CDT",  # Only shows up in summer! Seems backwards.
        "CD": "CST",  # Only shows up in winter! Seems backwards.
        "433": "CDT",
    },
    176: {
        "E": "EST",
        "XXX": "EST",
    },
    186: {"EAS": "EST"},
    189: {"CPT": "CST"},
    190: {"CPT": "CST"},
    193: {
        "CS": "CST",
        "CD": "CDT",
    },
    194: {"PPT": "PST"},  # LADWP, constant across all years.
    195: {"CPT": "CST"},
    208: {"XXX": "CST"},
    211: {
        "206": "EST",
        "DST": "EDT",
        "XXX": "EST",
    },
    213: {"CDS": "CDT"},
    216: {"XXX": "CDT"},
    217: {
        "MPP": "MST",
        "MPT": "MST",
    },
    224: {"DST": "EST"},
    225: {
        "EDS": "EDT",
        "DST": "EDT",
        "EPT": "EST",
    },
    226: {"DST": "CDT"},
    230: {"EPT": "EST"},
    233: {"DST": "EDT"},
    234: {
        "1": "EST",
        "2": "EDT",
        "DST": "EDT",
    },
    # Constant across the year. Never another timezone seen.
    239: {"PPT": "PST"},
    243: {"DST": "PST"},
    245: {"CDS": "CDT"},
    248: {"DST": "EDT"},
    253: {"CPT": "CST"},
    254: {"DST": "CDT"},
    257: {"CPT": "CST"},
    259: {"DST": "CDT"},
    264: {"CDS": "CDT"},
    271: {"EDS": "EDT"},
    275: {"CPT": "CST"},
    277: {
        "CPT": "CST",
        "XXX": "CST",
    },
    281: {"CEN": "CST"},
    288: {"XXX": "EST"},
    293: {"XXX": "MST"},
    294: {"XXX": "EST"},
    296: {"CPT": "CST"},
    297: {"CPT": "CST"},
    298: {"CPT": "CST"},
    299: {"CPT": "CST"},
    307: {"PPT": "PST"},  # Pacificorp, constant across the whole year.
    308: {
        "DST": "EDT",
        "EDS": "EDT",
        "EPT": "EST",
    },
    328: {
        "EPT": "EST",
    },
}

spring_forward = [
    "2000-04-02T02:02:00", "2001-04-01T02:02:00",
    "2002-04-07T02:02:00", "2003-04-06T02:02:00",
    "2004-04-04T02:02:00", "2005-04-03T02:02:00",
    "2006-04-02T02:02:00", "2007-03-11T02:02:00",
    "2008-03-09T02:02:00", "2009-03-08T02:02:00",

    "2010-03-14T02:02:00", "2011-03-13T02:02:00",
    "2012-03-11T02:02:00", "2013-03-10T02:02:00",
    "2014-03-09T02:02:00", "2015-03-08T02:02:00",
    "2016-03-13T02:02:00", "2017-03-12T02:02:00",
    "2018-03-11T02:02:00", "2019-03-10T02:02:00",

    "2020-03-08T02:02:00",
]
fall_back = [
    "2000-10-29T02:02:00", "2001-10-28T02:02:00",
    "2002-10-27T02:02:00", "2003-10-26T02:02:00",
    "2004-10-31T02:02:00", "2005-10-30T02:02:00",
    "2006-10-29T02:02:00", "2007-11-04T02:02:00",
    "2008-11-02T02:02:00", "2009-11-01T02:02:00",

    "2010-11-07T02:02:00", "2011-11-06T02:02:00",
    "2012-11-04T02:02:00", "2013-11-03T02:02:00",
    "2014-11-02T02:02:00", "2015-11-01T02:02:00",
    "2016-11-06T02:02:00", "2017-11-05T02:02:00",
    "2018-11-04T02:02:00", "2019-11-03T02:02:00",

    "2020-11-01T02:02:00",
]
time_changes = (
    pd.DataFrame(data={
        "spring_forward": pd.to_datetime(spring_forward),
        "fall_back": pd.to_datetime(fall_back)
    })
    .assign(year=lambda x: x.spring_forward.dt.year)
    .set_index("year")
)

bad_respondents = [
    319,
    99991,
    99992,
    99993,
    99994,
    99995,
]

tz_fixes_by_date = [
    {
        "utility_id_ferc714": 139,
        "offset_col": "utc_offset_code",
        "start_time": pd.Timestamp("2006-01-01T00:00:00"),
        "end_time": pd.Timestamp("2006-12-31T23:00:00"),
        "std_val": "PST",
        "dst_val": "PDT",
    },
    {
        "utility_id_ferc714": 235,
        "offset_col": "utc_offset_code",
        "start_time": pd.Timestamp("2015-01-01T00:00:00"),
        "end_time": pd.Timestamp("2015-12-31T23:00:00"),
        "std_val": "MST",
        "dst_val": "MDT",
    },
    {
        "utility_id_ferc714": 289,
        "offset_col": "utc_offset_code",
        "start_time": pd.Timestamp("2011-01-01T00:00:00"),
        "end_time": pd.Timestamp("2011-12-31T23:00:00"),
        "std_val": "CST",
        "dst_val": "CDT",
    },
    {
        "utility_id_ferc714": 292,
        "offset_col": "utc_offset_code",
        "start_time": pd.Timestamp("2011-01-01T00:00:00"),
        "end_time": pd.Timestamp("2011-12-31T23:00:00"),
        "std_val": "CST",
        "dst_val": "CDT",
    }
]
"""Instances in which a UTC offset code is set based on date and respondent."""

inverted_demand_fixes = [
    {
        "utility_id_ferc714": 156,
        "start_time": pd.Timestamp("2006-01-01T00:00:00"),
        "end_time": pd.Timestamp("2009-12-31T23:00:00"),
    },
    {
        "utility_id_ferc714": 289,
        "start_time": pd.Timestamp("2006-01-01T00:00:00"),
        "end_time": pd.Timestamp("2010-12-31T23:00:00"),
    },
]
"""Instances in which a respondent recorded demand as a negative number."""

offset_codes = {
    "HST": pd.Timedelta(-10, unit="hours"),  # Hawaii Standard
    "AKST": pd.Timedelta(-9, unit="hours"),  # Alaska Standard
    "AKDT": pd.Timedelta(-9, unit="hours"),  # Alaska Daylight
    "PST": pd.Timedelta(-8, unit="hours"),  # Pacific Standard
    "PDT": pd.Timedelta(-8, unit="hours"),  # Pacific Daylight
    "MST": pd.Timedelta(-7, unit="hours"),  # Mountain Standard
    "MDT": pd.Timedelta(-7, unit="hours"),  # Mountain Daylight
    "CST": pd.Timedelta(-6, unit="hours"),  # Central Standard
    "CDT": pd.Timedelta(-6, unit="hours"),  # Central Daylight
    "EST": pd.Timedelta(-5, unit="hours"),  # Eastern Standard
    "EDT": pd.Timedelta(-5, unit="hours"),  # Eastern Daylight
}
"""
A mapping of timezone offset codes to Timedelta offsets from UTC.

Note that the FERC 714 instructions state that all hourly demand is to be
reported in STANDARD time for whatever timezone is being used. Even though
many respondents use daylight savings / standard time abbreviations, a large
majority do appear to conform to using a single UTC offset throughout the year.
There are 6 instances in which the timezone associated with reporting changed
from one year to the next, and these result in duplicate records, which are
dropped.
"""

tz_codes = {
    "EST": "America/New_York",
    "EDT": "America/New_York",
    "CST": "America/Chicago",
    "CDT": "America/Chicago",
    "MST": "America/Denver",
    "MDT": "America/Denver",
    "PST": "America/Los_Angeles",
    "PDT": "America/Los_Angeles",
    "AKST": "America/Anchorage",
    "AKDT": "America/Anchorage",
    "HST": "Pacific/Honolulu",
}

MISSING_UTILITY_ID_EIA = {
    307: 14354,  # PacifiCorp
    295: 40229,  # Old Dominion Electric Cooperative
    329: 39347,  # East Texas Electricity Cooperative
}
"""FERC respondents lacking EIA utility IDs that appear to exist."""

RENAME_COLS = {
    "respondent_id_ferc714": {
        "respondent_id": "utility_id_ferc714",
        "respondent_name": "utility_name_ferc714",
        "eia_code": "utility_id_eia",
    },
    "pa_demand_hourly_ferc714": {
        "report_yr": "report_year",
        "plan_date": "report_date",
        "respondent_id": "utility_id_ferc714",
        "timezone": "utc_offset_code",
    },
    "pa_description_ferc714": {
        "report_yr": "report_year",
        "respondent_id": "utility_id_ferc714",
        "elec_util_name": "utility_name_ferc714",
        "peak_summer": "peak_demand_summer_mw",
        "peak_winter": "peak_demand_winter_mw",
    },
    "id_certification_ferc714": {
        "report_yr": "report_year",
        "respondent_id": "utility_id_ferc714",
    },
    "ba_gen_plants_ferc714": {
        "report_yr": "report_year",
        "respondent_id": "utility_id_ferc714",
    },
    "ba_demand_monthly_ferc714": {
        "report_yr": "report_year",
        "respondent_id": "utility_id_ferc714",
    },
    "ba_net_energy_load_ferc714": {
        "report_yr": "report_year",
        "respondent_id": "utility_id_ferc714",
    },
    "adjacent_bas_ferc714": {
        "report_yr": "report_year",
        "respondent_id": "utility_id_ferc714",
    },
    "ba_interchange_ferc714": {
        "report_yr": "report_year",
        "respondent_id": "utility_id_ferc714",
    },
    "ba_lambda_hourly_ferc714": {
        "report_yr": "report_year",
        "respondent_id": "utility_id_ferc714",
    },
    "lambda_description_ferc714": {
        "report_yr": "report_year",
        "respondent_id": "utility_id_ferc714",
    },
    "pa_demand_forecast_ferc714": {
        "report_yr": "report_year",
        "respondent_id": "utility_id_ferc714",
    },
}

##############################################################################
# Internal helper functions.
##############################################################################


def _hours_to_ints(col_name):
    """A macro to rename hourly demand columns."""
    if re.match(r"^hour\d\d$", col_name):
        return int(col_name[4:])
    else:
        return col_name


def _standardize_offset_codes(df, tz_map, tz_fixes):
    """Convert to standardized timezone abbreviations.

    This function ensures that all of the 3-4 letter abbreviations used to
    indicate a timestamp's localized offset from UTC are starndardized, so that
    they can be used to convert the timestamps can be made timezone aware.
    Several different kinds of fixes are applied. Mappings from non-standard
    abbreviations to standard abbreviations have been defined for each
    respondent that used non-standard abbreviations. This mapping is tz_map.
    Other fixes only apply to a given respondent within a certain range of
    dates. These fixes are described by tz_fixes.

    This stuff shouldn't happen in this function:

    The small number of actual NA values for demand that show up originally are
    set to 0.0 and only records with either a non-zero demand or a real
    offset are returned.

    """
    logger.info("Standardizing UTC offset codes.")
    df = df.copy()
    df["utc_offset_code"] = df["utc_offset_code"].replace(to_replace=tz_map)
    # More specific fixes on a per-respondent basis:
    for rid in tz_fixes:
        for orig_tz in tz_fixes[rid]:
            df.loc[(
                (df.utility_id_ferc714 == rid) &
                (df["utc_offset_code"] == orig_tz)), "utc_offset_code"] = tz_fixes[rid][orig_tz]

    return df


def _fix_25th_hours(df, spring_forward, fall_back):
    """Deal with daylight savings time changes."""
    logger.info("Removing unfixable duplicate 25th hours.")

    fall_back_dates = pd.to_datetime(fall_back).date
    # The only valid reason for there to be 25 hours in a day is that the
    # data was collected on on of the "fall back" dates. It's unclear what
    # to do with the 25th hour data on any other day, so we are going to
    # discard it.
    df = df.loc[(df.hour != 25) | df.report_date.isin(fall_back_dates)]
    # However, it turns out that the vast majority of the 25th hours, even on
    # the "fall back" dates are just zero, because the whole day of data is
    # recorded in a single UTC offset, so there really are only 24 hours to
    # work with. But they don't have any real NA values, so they use zero. We
    # can safely remove the zero valued 25th hour demand:
    df = df.loc[(df.hour != 25) | (df.demand_mwh != 0.0)]

    # Select the full day's worth of records for the remaining 25 hour days,
    # so that we can adjust them in isolation.
    long_days = (
        df.query("hour==25")
        .loc[:, ["utility_id_ferc714", "report_date"]]
        .drop_duplicates()
        .merge(df, how="left", on=["utility_id_ferc714", "report_date"])
    )

    # Many of the remaining 25th hour values are actually daily demand totals.
    # We can check for those and remove them, since they aren't really hourly
    # data that we want to keep.
    daily_totals = (
        long_days.groupby(["utility_id_ferc714", "report_date"])[
            "demand_mwh"].sum().reset_index()
        .merge(long_days.loc[long_days.hour == 25,
                             ["utility_id_ferc714", "report_date", "demand_mwh"]],
               on=["utility_id_ferc714", "report_date"])
        .rename(columns={"demand_mwh_x": "daily_mwh", "demand_mwh_y": "hour25_mwh"})
        # If the hour 25 demand is half or more of daily demand, assume it is
        # an irrelevant daily total and can be discarded.
        .query("hour25_mwh>=0.5*daily_mwh")
        .loc[:, ["utility_id_ferc714", "report_date"]]
        .assign(hour=25)
    )
    # Drop the set of hour 25 records which we believe to be daily totals:
    idx_cols = ["utility_id_ferc714", "report_date", "hour"]
    df = (
        df.set_index(idx_cols)
        .drop(daily_totals.set_index(idx_cols).index)
        .reset_index()
    )
    # Convert the "hour" column into 0-23hr format and use it to set the time
    # portion of the local_time Datetime column. Report the number of resulting
    # duplicate hours.
    df["local_time"] = df.report_date + pd.to_timedelta(df.hour - 1, unit="h")
    log_dupes(df, ["utility_id_ferc714", "local_time"])
    # Drop the rows that pertained to the 25th hours, since they're unfixable:
    logger.info("Dropping remaining 25th hour timesteps for all respondents.")
    df = df.query("hour<25")

    # Now that we have a real datetime column, ditch "hour" and "report_date"
    df = df.drop(["hour", "report_date"], axis="columns")
    return df


def log_dupes(df, dupe_cols):
    """A macro to report the number of duplicate records."""
    n_dupes = len(df[df.duplicated(dupe_cols)])
    logger.info(f"Found {n_dupes} duplicated hours.")


def _fix_tz_by_date(df, time_changes, utility_id_ferc714,
                    start_time, end_time, offset_col, std_val, dst_val):
    """Assign UTC offset codes using date and respondent specific fixes."""
    df = df.copy()
    years = df.loc[(df.utility_id_ferc714 == utility_id_ferc714) &
                   (df.local_time >= start_time) &
                   (df.local_time <= end_time)].local_time.dt.year.unique()
    for yr in years:
        spring = time_changes.loc[yr, "spring_forward"]
        fall = time_changes.loc[yr, "fall_back"]
        std_times = (
            (df.utility_id_ferc714 == utility_id_ferc714) &
            (df.local_time.dt.year == yr) &
            ((df.local_time < spring) | (df.local_time >= fall))
        )
        dst_times = (
            (df.utility_id_ferc714 == utility_id_ferc714) &
            (df.local_time.dt.year == yr) &
            ((df.local_time >= spring) & (df.local_time < fall))
        )
        df.loc[std_times, offset_col] = std_val
        df.loc[dst_times, offset_col] = dst_val
    return df


def _apply_tz_fixes_by_date(df, fixes, time_changes):
    """A wrapper to assign many timezone fixes at once."""
    logger.info("Applying date and respondent specific UTC offset code fixes.")
    for fix in fixes:
        df = _fix_tz_by_date(df, time_changes=time_changes, **fix)
    return df


def _fix_inverted_demand(df, utility_id_ferc714, start_time, end_time):
    """Multiply the demand between start and end time by negative 1."""
    df = df.copy()
    idx_to_flip = df.loc[
        (df.utility_id_ferc714 == utility_id_ferc714) &
        (df.local_time >= start_time) &
        (df.local_time <= end_time)
    ].index
    df.loc[idx_to_flip, "demand_mwh"] = - 1 * df.loc[idx_to_flip, "demand_mwh"]
    return df


def _apply_inverted_demand_fixes(df, fixes):
    """A wrapper to fix reported demand sign errors."""
    for fix in fixes:
        df = _fix_inverted_demand(df, **fix)
    return df


def _to_utc_and_tz(df, offset_codes, tz_codes):
    """
    Use offset and timezone codes to turn local time into UTC.

    Args:
        df (pandas.DataFrame): A DataFrame with a 'local_time" column to be
            converted into UTC, and a 'utc_offset_code' column containing
            codes that indicate the local time offset from UTC.
        offset_codes (dict): A dictionary mapping the UTC offset codes to a
            pandas.Timedelta describing the offset of that code from UTC.
        tz_codes (dict): A dictionary mapping the UTC offset codes to the
            appropriate canonical Tiemzone name (e.g. "America/Denver").

    Returns:
        pandas.DataFrame: A dataframe with 'utc_datetime' and 'timezone'
        columns reflecting the UTC converted local time, and the timezone that
        it came from originally using canonical timezone names. The local time
        column is dropped.

    """
    log_dupes(df, ["utility_id_ferc714", "local_time"])
    logger.info("Converting local time + offset code to UTC + timezone.")
    df["utc_offset"] = df.utc_offset_code.replace(offset_codes)
    df["utc_datetime"] = df.local_time - df.utc_offset
    df["timezone"] = df.utc_offset_code.replace(tz_codes)
    df = df.drop(
        ["utc_offset", "utc_offset_code", "local_time"], axis="columns")
    log_dupes(df, ["utility_id_ferc714", "utc_datetime"])
    return df


def respondent_id(tf_dfs):
    """Table of FERC 714 respondents IDs, names, and EIA utility IDs."""
    df = (
        tf_dfs["respondent_id_ferc714"].astype({
            "utility_id_eia": pd.Int64Dtype(),
            "utility_id_ferc714": pd.Int64Dtype(),
            "utility_name_ferc714": pd.StringDtype(),
        })
        .assign(
            utility_name_ferc714=lambda x: x.utility_name_ferc714.str.strip(),
            utility_id_eia=lambda x: x.utility_id_eia.replace(
                to_replace=0, value=pd.NA)
        )
        # These excludes fake Test IDs -- not real planning areas
        .query("utility_id_ferc714 not in @bad_respondents")
    )
    # There are a few utilities that seem mappable, but missing:
    for rid in MISSING_UTILITY_ID_EIA:
        df.loc[df.utility_id_ferc714 == rid,
               "utility_id_eia"] = MISSING_UTILITY_ID_EIA[rid]
    tf_dfs["respondent_id_ferc714"] = df
    return tf_dfs


def pa_demand_hourly(tf_dfs):
    """Hourly demand time series by Planning Area."""
    logger.info("Converting dates into pandas Datetime types.")
    df = (
        tf_dfs["pa_demand_hourly_ferc714"].assign(
            report_date=lambda x: pd.to_datetime(x.report_date),
            utc_offset_code=lambda x: x.utc_offset_code.str.upper().str.strip(),
        )
    )

    logger.info("Melting daily FERC 714 records into hourly records.")
    df = (
        df.rename(columns=_hours_to_ints)
        .melt(
            id_vars=["report_year", "utility_id_ferc714",
                     "report_date", "utc_offset_code"],
            var_name="hour",
            value_name="demand_mwh")
    )
    log_dupes(df, ["utility_id_ferc714", "report_date", "hour"])

    df = (
        df.pipe(_standardize_offset_codes, tz_map=tz_nulls, tz_fixes=tz_fixes)
        # Discard records lacking *both* UTC offset code and non-zero demand
        # In practice, this should be *all* of the XXX records, but we're being
        # conservative, in case something changes / goes wrong. We want to
        # notice if for some reason later we find XXX records that *do* have
        # real demand associated with them.
        .query("utc_offset_code!='XXX' | demand_mwh!=0.0")
        .pipe(_fix_25th_hours, spring_forward, fall_back)
        .pipe(_apply_tz_fixes_by_date, tz_fixes_by_date, time_changes)
        .pipe(_apply_inverted_demand_fixes, inverted_demand_fixes)
        .pipe(_to_utc_and_tz, offset_codes=offset_codes, tz_codes=tz_codes)
        .loc[:, [
            "report_year",
            "utility_id_ferc714",
            "utc_datetime",
            "timezone",
            "demand_mwh"
        ]]
        .sort_values(["utility_id_ferc714", "utc_datetime"])
    )
    tf_dfs["pa_demand_hourly_ferc714"] = df
    return tf_dfs


def electricity_planning_areas(pudl_settings):
    """Electric Planning Area geometries from HIFLD."""
    # Sadly geopandas / fiona can't read a zipfile.Path
    shapepath = pathlib.Path(
        pudl_settings["data_dir"],
        "local",
        "hifld_electric_planning_areas",
        "data",
        "Electric_Planning_Areas.shp"
    )
    gdf = geopandas.read_file(shapepath)

    gdf = (
        gdf.assign(
            SOURCEDATE=lambda x: pd.to_datetime(x.SOURCEDATE),
            VAL_DATE=lambda x: pd.to_datetime(x.VAL_DATE),
            ID=lambda x: pd.to_numeric(x.ID),
            NAICS_CODE=lambda x: pd.to_numeric(x.NAICS_CODE),
            YEAR=lambda x: pd.to_numeric(x.YEAR),
        )
        .astype({
            "OBJECTID": pd.Int64Dtype(),
            "ID": pd.Int64Dtype(),
            "NAME": pd.StringDtype(),
            "COUNTRY": pd.StringDtype(),
            "NAICS_CODE": pd.Int64Dtype(),
            "NAICS_DESC": pd.StringDtype(),
            "SOURCE": pd.StringDtype(),
            "VAL_METHOD": pd.StringDtype(),
            "WEBSITE": pd.StringDtype(),
            "ABBRV": pd.StringDtype(),
            "YEAR": pd.Int64Dtype(),
            "PEAK_LOAD": float,
            "PEAK_RANGE": float,
            "SHAPE__Are": float,
            "SHAPE__Len": float,
        })
        .set_index("OBJECTID")
    )
    return gdf


def id_certification(tf_dfs):
    """A stub transform function."""
    return tf_dfs


def ba_gen_plants(tf_dfs):
    """A stub transform function."""
    return tf_dfs


def ba_demand_monthly(tf_dfs):
    """A stub transform function."""
    return tf_dfs


def ba_net_energy_load(tf_dfs):
    """A stub transform function."""
    return tf_dfs


def adjacent_bas(tf_dfs):
    """A stub transform function."""
    return tf_dfs


def ba_interchange(tf_dfs):
    """A stub transform function."""
    return tf_dfs


def ba_lambda_hourly(tf_dfs):
    """A stub transform function."""
    return tf_dfs


def lambda_description(tf_dfs):
    """A stub transform function."""
    return tf_dfs


def pa_description(tf_dfs):
    """A stub transform function."""
    return tf_dfs


def pa_demand_forecast(tf_dfs):
    """A stub transform function."""
    return tf_dfs


def pre_transform(raw_df):
    """
    A simple transform function for until the real ones are written.

    * Removes footnotes columns ending with _f
    * Drops report_prd, spplmnt_num, and row_num columns
    * Excludes records which pertain to bad (test) respondents.
    * Re-names report_yr to our standard report-year

    """
    logger.info("Removing unneeded columns and dropping bad respondents.")

    out_df = (
        raw_df.filter(regex=r"^(?!.*_f$).*")
        .drop(["report_prd", "spplmnt_num", "row_num"],
              axis="columns", errors="ignore")
        .query("utility_id_ferc714 not in @bad_respondents")
    )
    return out_df


def transform(raw_dfs, tables=pc.pudl_tables["ferc714"]):
    """
    Transform the raw FERC 714 dataframes into datapackage ready ouputs.

    Args:
        raw_dfs (dict): A dictionary of raw pandas.DataFrame objects,
            as read out of the original FERC 714 CSV files. Generated by the
            `pudl.extract.ferc714.extract()` function.
        tables (iterable): The set of PUDL tables within FERC 714 that
            we should process. Typically set to all of them, unless

    Returns:
        dict: A dictionary of pandas.DataFrame objects that are ready to be
        output in a data package / database table.

    """
    tf_funcs = {
        "respondent_id_ferc714": respondent_id,
        "pa_demand_hourly_ferc714": pa_demand_hourly,
        "pa_description_ferc714": pa_description,
        # These tables have yet to be cleaned up fully.
        "id_certification_ferc714": id_certification,
        "ba_gen_plants_ferc714": ba_gen_plants,
        "ba_demand_monthly_ferc714": ba_demand_monthly,
        "ba_net_energy_load_ferc714": ba_net_energy_load,
        "adjacent_bas_ferc714": adjacent_bas,
        "ba_interchange_ferc714": ba_interchange,
        "ba_lambda_hourly_ferc714": ba_lambda_hourly,
        "lambda_description_ferc714": lambda_description,
        "pa_demand_forecast_ferc714": pa_demand_forecast,
    }
    tf_dfs = {}
    for table in tables:
        if table not in pc.pudl_tables["ferc714"]:
            raise ValueError(
                f"No transform function found for requested FERC Form 714 "
                f"data table {table}!"
            )
        logger.info(f"Transforming {table}.")
        tf_dfs[table] = (
            raw_dfs[table]
            .rename(columns=RENAME_COLS[table])
            .pipe(pre_transform)
        )
        tf_dfs = tf_funcs[table](tf_dfs)
        tf_dfs[table] = (
            pudl.helpers.convert_cols_dtypes(tf_dfs[table], "ferc714", table)
        )
    return tf_dfs
