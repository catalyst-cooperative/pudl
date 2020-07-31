"""Transformation of the FERC Form 714 data."""
import logging
import re

import numpy as np
import pandas as pd

import pudl
from pudl import constants as pc

logger = logging.getLogger(__name__)

##############################################################################
# Constants required for transforming FERC 714
##############################################################################

# More detailed fixes on a per respondent basis
OFFSET_CODE_FIXES = {
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
    182: {"PPT": "PDT"},  # Imperial Irrigation District P looks like D
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

OFFSET_CODE_FIXES_BY_YEAR = [
    {
        "respondent_id_ferc714": 139,
        "report_year": 2006,
        "utc_offset_code": "PST"
    },
    {
        "respondent_id_ferc714": 235,
        "report_year": 2015,
        "utc_offset_code": "MST"
    },
    {
        "respondent_id_ferc714": 289,
        "report_year": 2011,
        "utc_offset_code": "CST"
    },
    {
        "respondent_id_ferc714": 292,
        "report_year": 2011,
        "utc_offset_code": "CST"
    },
]

BAD_RESPONDENTS = [
    319,
    99991,
    99992,
    99993,
    99994,
    99995,
]
"""Fake respondent IDs for database test entities."""

OFFSET_CODES = {
    "EST": pd.Timedelta(-5, unit="hours"),  # Eastern Standard
    "EDT": pd.Timedelta(-5, unit="hours"),  # Eastern Daylight
    "CST": pd.Timedelta(-6, unit="hours"),  # Central Standard
    "CDT": pd.Timedelta(-6, unit="hours"),  # Central Daylight
    "MST": pd.Timedelta(-7, unit="hours"),  # Mountain Standard
    "MDT": pd.Timedelta(-7, unit="hours"),  # Mountain Daylight
    "PST": pd.Timedelta(-8, unit="hours"),  # Pacific Standard
    "PDT": pd.Timedelta(-8, unit="hours"),  # Pacific Daylight
    "AKST": pd.Timedelta(-9, unit="hours"),  # Alaska Standard
    "AKDT": pd.Timedelta(-9, unit="hours"),  # Alaska Daylight
    "HST": pd.Timedelta(-10, unit="hours"),  # Hawaii Standard
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

TZ_CODES = {
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
"""Mapping between standardized time offset codes and canonical timezones."""

EIA_CODE_FIXES = {
    # FERC 714 Respondent ID: EIA BA or Utility ID
    125: 2775,  # EIA BA CAISO (fixing bad EIA Code of 229)
    134: 5416,  # Duke Energy Corp. (bad id was non-existent 3260)
    203: 12341,  # MidAmerican Energy Co. (fixes typo, from 12431)
    257: 59504,  # Southwest Power Pool (Fixing bad EIA Coding)
    292: 20382,  # City of West Memphis -- (fixes a typo, from 20383)
    295: 40229,  # Old Dominion Electric Cooperative (missing)
    301: 14725,  # PJM Interconnection Eastern Hub (missing)
    302: 14725,  # PJM Interconnection Western Hub (missing)
    303: 14725,  # PJM Interconnection Illinois Hub (missing)
    304: 14725,  # PJM Interconnection Northern Illinois Hub (missing)
    305: 14725,  # PJM Interconnection Dominion Hub (missing)
    306: 14725,  # PJM Interconnection AEP-Dayton Hub (missing)
    # PacifiCorp Utility ID is 14354. It ALSO has 2 BA IDs: (14378, 14379)
    # See https://github.com/catalyst-cooperative/pudl/issues/616
    307: 14379,  # Using this ID for now only b/c it's in the HIFLD geometry
    309: 12427,  # Michigan Power Pool / Power Coordination Center (missing)
    315: 56090,  # Griffith Energy (bad id was 55124)
    323: 58790,  # Gridforce Energy Management (missing)
    324: 58791,  # NaturEner Wind Watch LLC (Fixes bad ID 57995)
    329: 39347,  # East Texas Electricity Cooperative (missing)
}
"""Overrides of FERC 714 respondent IDs with wrong or missing EIA Codes"""

RENAME_COLS = {
    "respondent_id_ferc714": {
        "respondent_id": "respondent_id_ferc714",
        "respondent_name": "respondent_name_ferc714",
    },
    "demand_hourly_pa_ferc714": {
        "report_yr": "report_year",
        "plan_date": "report_date",
        "respondent_id": "respondent_id_ferc714",
        "timezone": "utc_offset_code",
    },
    "description_pa_ferc714": {
        "report_yr": "report_year",
        "respondent_id": "respondent_id_ferc714",
        "elec_util_name": "respondent_name_ferc714",
        "peak_summer": "peak_demand_summer_mw",
        "peak_winter": "peak_demand_winter_mw",
    },
    "id_certification_ferc714": {
        "report_yr": "report_year",
        "respondent_id": "respondent_id_ferc714",
    },
    "gen_plants_ba_ferc714": {
        "report_yr": "report_year",
        "respondent_id": "respondent_id_ferc714",
    },
    "demand_monthly_ba_ferc714": {
        "report_yr": "report_year",
        "respondent_id": "respondent_id_ferc714",
    },
    "net_energy_load_ba_ferc714": {
        "report_yr": "report_year",
        "respondent_id": "respondent_id_ferc714",
    },
    "adjacency_ba_ferc714": {
        "report_yr": "report_year",
        "respondent_id": "respondent_id_ferc714",
    },
    "interchange_ba_ferc714": {
        "report_yr": "report_year",
        "respondent_id": "respondent_id_ferc714",
    },
    "lambda_hourly_ba_ferc714": {
        "report_yr": "report_year",
        "respondent_id": "respondent_id_ferc714",
    },
    "lambda_description_ferc714": {
        "report_yr": "report_year",
        "respondent_id": "respondent_id_ferc714",
    },
    "demand_forecast_pa_ferc714": {
        "report_yr": "report_year",
        "respondent_id": "respondent_id_ferc714",
    },
}

##############################################################################
# Internal helper functions.
##############################################################################


def _hours_to_ints(col_name):
    """A macro to rename hourly demand columns."""
    if re.match(r"^hour\d\d$", col_name):
        col_name = int(col_name[4:])
    return col_name


def _standardize_offset_codes(df, offset_fixes):
    """
    Convert to standardized UTC offset abbreviations.

    This function ensures that all of the 3-4 letter abbreviations used to
    indicate a timestamp's localized offset from UTC are standardized, so that
    they can be used to make the timestamps timezone aware. The standard
    abbreviations we're using are:

    "HST": Hawaii Standard Time
    "AKST": Alaska Standard Time
    "AKDT": Alaska Daylight Time
    "PST": Pacific Standard Time
    "PDT": Pacific Daylight Time
    "MST": Mountain Standard Time
    "MDT": Mountain Daylight Time
    "CST": Central Standard Time
    "CDT": Central Daylight Time
    "EST": Eastern Standard Time
    "EDT": Eastern Daylight Time

    In some cases different respondents use the same non-standard abbreviations
    to indicate different offsets, and so the fixes are applied on a
    per-respondent basis, as defined by offset_fixes.

    UTC offset codes which are originally NA or the empty string are replaced
    with a temporary sentinel value, the string "XXX".

    Args:
        df (pandas.DataFrame): A DataFrame containing a utc_offset_code column
            that needs to be standardized.
        offset_fixes (dict): A dictionary with respondent_id_ferc714 values as the
            keys, and a dictionary mapping non-standard UTC offset codes to
            the standardized UTC offset codes as the value.

    Returns:
        pandas.DataFrame: The same as the input DataFrame, but with only
        standardized UTC offset codes in the ``utc_offset_code`` column.

    """
    logger.debug("Standardizing UTC offset codes.")
    df = df.copy()
    # Replace NaN and empty string values with a temporary placeholder "XXX"
    df["utc_offset_code"] = (
        df.utc_offset_code.replace(to_replace={np.nan: "XXX", "": "XXX"})
    )
    # Apply specific fixes on a per-respondent basis:
    for rid in offset_fixes:
        for orig_tz in offset_fixes[rid]:
            df.loc[(
                (df.respondent_id_ferc714 == rid) &
                (df["utc_offset_code"] == orig_tz)), "utc_offset_code"] = offset_fixes[rid][orig_tz]

    return df


def _log_dupes(df, dupe_cols):
    """A macro to report the number of duplicate hours found."""
    n_dupes = len(df[df.duplicated(dupe_cols)])
    logger.debug(f"Found {n_dupes} duplicated hours.")


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
    _log_dupes(df, ["respondent_id_ferc714", "local_time"])
    logger.debug("Converting local time + offset code to UTC + timezone.")
    df["utc_offset"] = df.utc_offset_code.replace(offset_codes)
    df["utc_datetime"] = df.local_time - df.utc_offset
    df["timezone"] = df.utc_offset_code.replace(tz_codes)
    df = df.drop(
        ["utc_offset", "utc_offset_code", "local_time"], axis="columns")
    _log_dupes(df, ["respondent_id_ferc714", "utc_datetime"])
    return df


def _complete_demand_timeseries(pa_demand):
    """
    Fill in missing planning area demand timesteps, leaving demand_mwh NaN.

    Before we can perform many kinds of time series analyses, including the
    identification of outliers and other anomolies, and the imputation of
    missing values, we need to ensure that we have a complete time series with
    all hourly timesteps for each respondent, and that there are no duplicate
    timesteps.

    This function does not attempt to impute or otherwise fill in missing
    demand_mwh values, but it does fill in the other non data fields for
    the newly created timesteps (respondent_id_ferc714, timezone, report_year).

    Args:
        pa_demand (pandas.DataFrame): A planning area demand time series from
            the FERC Form 714. May contain duplicate timesteps within a given
            respondent's data. May be missing some timesteps. Must already
            have a clean utc_datetime column and timezone column.

    Returns:
        pandas.DataFrame: A dataframe containing the same demand data as the
        input, but with no missing and no duplicate timesteps.

    """
    logger.debug("Ensuring that Planning Area demand time series are complete.")
    # Remove any lingering duplicate hours. There should be less than 10 of
    # these, resulting from changes a planning area's reporting timezone.
    pa_demand = pa_demand.drop_duplicates(
        ["respondent_id_ferc714", "utc_datetime"])
    _log_dupes(pa_demand, ["respondent_id_ferc714", "utc_datetime"])

    # We need to address the time series for each respondent independently, so
    # here we iterate over all of the respondent IDs one at a time:
    dfs = []
    for util_id in pa_demand.respondent_id_ferc714.unique():
        df = (
            pa_demand
            .loc[pa_demand.respondent_id_ferc714 == util_id]
            .set_index("utc_datetime")
        )

        # Reindex with a complete set of hourly timesteps and fill in the
        # resulting NA values where we can do so easily. The respondent IDs and
        # name do not vary within the time series for a particular respondent,
        # and the timezone is mainly important for internal consistency between
        # the utc_datetime value and the report year. So long as those values
        # are internally consistent, it can be eiter forward or backward filled
        # -- in reality no data was reported for these time steps, so either
        # one is equally "correct".
        df = (
            df.reindex(
                pd.date_range(
                    start=df.index.min(),
                    end=df.index.max(),
                    freq="1H"
                )
            )
            .assign(
                respondent_id_ferc714=lambda x: x.respondent_id_ferc714.fillna(
                    method="backfill"),
                timezone=lambda x: x.timezone.fillna(method="backfill"),
            )
        )
        df.index.name = "utc_datetime"
        dfs.append(df)
    # Bring all the individual per-respondent dataframes back together again:
    new_df = pd.concat(dfs).reset_index()
    logger.debug("Generating self-consistent report_year for new timesteps.")
    # Now we need to fill in the "report_year" value, which depends on the
    # local time not the UTC time, so we need to temporarily generate a
    # localized datetime column:
    new_df["utc_aware"] = new_df.utc_datetime.dt.tz_localize("UTC")
    new_df["local_datetime"] = (
        new_df
        .groupby("timezone")["utc_aware"]
        .transform(lambda x: x.dt.tz_convert(x.name))
    )

    # We can only use the Series.dt datetime accessor on a Series with
    # homogeneous timezone information, so we now have to iterate through each
    # of the timezones. If there's a better way to do this with groupby that
    # would be great...
    for tz in new_df.timezone.unique():
        rows_to_fix = (new_df.timezone == tz) & (new_df.report_year.isnull())
        if rows_to_fix.any():
            new_df.loc[rows_to_fix, "report_year"] = pd.to_datetime(
                new_df.loc[rows_to_fix, "local_datetime"]).dt.year
    # Remove temporary columns and return only the columns we started with.
    new_df = new_df.drop(["utc_aware", "local_datetime"], axis="columns")
    non_null = len(new_df[new_df.demand_mwh.notnull()]) / len(new_df)

    logger.debug(
        f"{non_null:.2%} of all planning area demand records have "
        "non-null values.")

    return new_df


def respondent_id(tfr_dfs):
    """
    Transform the FERC 714 respondent IDs, names, and EIA utility IDs.

    This consists primarily of dropping test respondents and manually
    assigning EIA utility IDs to a few FERC Form 714 respondents that report
    planning area demand, but which don't have their corresponding EIA utility
    IDs provided by FERC for some reason (including PacifiCorp).

    Args:
        tfr_dfs (dict): A dictionary of (partially) transformed dataframes,
            to be cleaned up.

    Returns:
        dict: The input dictionary of dataframes, but with a finished
        respondent_id_ferc714 dataframe.

    """
    df = (
        tfr_dfs["respondent_id_ferc714"].assign(
            respondent_name_ferc714=lambda x: x.respondent_name_ferc714.str.strip(),
            eia_code=lambda x: x.eia_code.replace(
                to_replace=0, value=pd.NA)
        )
        # These excludes fake Test IDs -- not real planning areas
        .query("respondent_id_ferc714 not in @BAD_RESPONDENTS")
    )
    # There are a few utilities that seem mappable, but missing:
    for rid in EIA_CODE_FIXES:
        df.loc[df.respondent_id_ferc714 == rid,
               "eia_code"] = EIA_CODE_FIXES[rid]
    tfr_dfs["respondent_id_ferc714"] = df
    return tfr_dfs


def demand_hourly_pa(tfr_dfs):
    """
    Transform the hourly demand time series by Planning Area.

    Args:
        tfr_dfs (dict): A dictionary of (partially) transformed dataframes,
            to be cleaned up.

    Returns:
        dict: The input dictionary of dataframes, but with a finished
        pa_demand_hourly_ferc714 dataframe.

    """
    logger.debug("Converting dates into pandas Datetime types.")
    df = (
        tfr_dfs["demand_hourly_pa_ferc714"].assign(
            report_date=lambda x: pd.to_datetime(x.report_date),
            utc_offset_code=lambda x: x.utc_offset_code.str.upper().str.strip(),
        )
    )

    logger.debug("Melting daily FERC 714 records into hourly records.")
    df = (
        df.rename(columns=_hours_to_ints)
        .melt(
            id_vars=["report_year", "respondent_id_ferc714",
                     "report_date", "utc_offset_code"],
            var_name="hour",
            value_name="demand_mwh")
    )
    _log_dupes(df, ["respondent_id_ferc714", "report_date", "hour"])

    df = (
        df.pipe(_standardize_offset_codes, offset_fixes=OFFSET_CODE_FIXES)
        # Discard records lacking *both* UTC offset code and non-zero demand
        # In practice, this should be *all* of the XXX records, but we're being
        # conservative, in case something changes / goes wrong. We want to
        # notice if for some reason later we find XXX records that *do* have
        # real demand associated with them.
        .query("utc_offset_code!='XXX' | demand_mwh!=0.0")
        # Almost all 25th hours are unusable (0.0 or daily totals),
        # and they shouldn't really exist at all based on FERC instructions.
        .query("hour!=25")
        # Switch to using 0-23 hour notation, and combine hour with date:
        .assign(
            local_time=lambda x:
                x.report_date + pd.to_timedelta(x.hour - 1, unit="h")
        )
    )

    for fix in OFFSET_CODE_FIXES_BY_YEAR:
        mask = (
            (df.report_year == fix["report_year"]) &
            (df.respondent_id_ferc714 == fix["respondent_id_ferc714"])
        )
        df.loc[mask, "utc_offset_code"] = fix["utc_offset_code"]

    # Flip the sign on two sections of demand which were reported as negative:
    mask1 = (df.report_year.isin([2006, 2007, 2008, 2009])) & (
        df.respondent_id_ferc714 == 156)
    df.loc[mask1, "demand_mwh"] = -1.0 * df.loc[mask1, "demand_mwh"]
    mask2 = (df.report_year.isin([2006, 2007, 2008, 2009, 2010])) & (
        df.respondent_id_ferc714 == 289)
    df.loc[mask2, "demand_mwh"] = -1.0 * df.loc[mask2, "demand_mwh"]

    df = (
        df.pipe(_to_utc_and_tz, offset_codes=OFFSET_CODES, tz_codes=TZ_CODES)
        .pipe(_complete_demand_timeseries)
        .loc[:, [
            "report_year",
            "respondent_id_ferc714",
            "utc_datetime",
            "timezone",
            "demand_mwh"
        ]]
        .sort_values(["respondent_id_ferc714", "utc_datetime"])
        .pipe(pudl.helpers.convert_to_date)
    )
    tfr_dfs["demand_hourly_pa_ferc714"] = df
    return tfr_dfs


def id_certification(tfr_dfs):
    """A stub transform function."""
    return tfr_dfs


def gen_plants_ba(tfr_dfs):
    """A stub transform function."""
    return tfr_dfs


def demand_monthly_ba(tfr_dfs):
    """A stub transform function."""
    return tfr_dfs


def net_energy_load_ba(tfr_dfs):
    """A stub transform function."""
    return tfr_dfs


def adjacency_ba(tfr_dfs):
    """A stub transform function."""
    return tfr_dfs


def interchange_ba(tfr_dfs):
    """A stub transform function."""
    return tfr_dfs


def lambda_hourly_ba(tfr_dfs):
    """A stub transform function."""
    return tfr_dfs


def lambda_description(tfr_dfs):
    """A stub transform function."""
    return tfr_dfs


def description_pa(tfr_dfs):
    """A stub transform function."""
    return tfr_dfs


def demand_forecast_pa(tfr_dfs):
    """A stub transform function."""
    return tfr_dfs


def _early_transform(raw_df):
    """
    A simple transform function for until the real ones are written.

    * Removes footnotes columns ending with _f
    * Drops report_prd, spplmnt_num, and row_num columns
    * Excludes records which pertain to bad (test) respondents.

    """
    logger.debug("Removing unneeded columns and dropping bad respondents.")

    out_df = (
        raw_df.filter(regex=r"^(?!.*_f$).*")
        .drop(["report_prd", "spplmnt_num", "row_num"],
              axis="columns", errors="ignore")
        .query("respondent_id_ferc714 not in @BAD_RESPONDENTS")
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
    tfr_funcs = {
        "respondent_id_ferc714": respondent_id,
        "demand_hourly_pa_ferc714": demand_hourly_pa,
        # These tables have yet to be fully transformed:
        "description_pa_ferc714": description_pa,
        "id_certification_ferc714": id_certification,
        "gen_plants_ba_ferc714": gen_plants_ba,
        "demand_monthly_ba_ferc714": demand_monthly_ba,
        "net_energy_load_ba_ferc714": net_energy_load_ba,
        "adjacency_ba_ferc714": adjacency_ba,
        "interchange_ba_ferc714": interchange_ba,
        "lambda_hourly_ba_ferc714": lambda_hourly_ba,
        "lambda_description_ferc714": lambda_description,
        "demand_forecast_pa_ferc714": demand_forecast_pa,
    }
    tfr_dfs = {}
    for table in tables:
        if table not in pc.pudl_tables["ferc714"]:
            raise ValueError(
                f"No transform function found for requested FERC Form 714 "
                f"data table {table}!"
            )
        logger.info(f"Transforming {table}.")
        tfr_dfs[table] = (
            raw_dfs[table]
            .rename(columns=RENAME_COLS[table])
            .pipe(_early_transform)
        )
        tfr_dfs = tfr_funcs[table](tfr_dfs)
        tfr_dfs[table] = (
            pudl.helpers.convert_cols_dtypes(tfr_dfs[table], "ferc714", table)
        )
    return tfr_dfs
