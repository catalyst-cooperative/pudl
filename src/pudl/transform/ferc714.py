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
        np.nan: "CST",
    },
    133: {
        "AKS": "AKST",
        "AST": "AKST",
        "AKD": "AKDT",
        "ADT": "AKDT",
    },
    134: {np.nan: "EST"},
    137: {np.nan: "CST"},
    140: {
        "1": "EST",
        "2": "EDT",
        np.nan: "EST",
    },
    141: {np.nan: "CST"},
    143: {"MS": "MST"},
    146: {"DST": "EST"},
    148: {np.nan: "CST"},
    151: {
        "DST": "CDT",
        np.nan: "CST",
    },
    153: {np.nan: "MST"},
    154: {np.nan: "MST"},
    156: {np.nan: "CST"},
    157: {"DST": "EDT"},
    161: {"CPT": "CST"},
    163: {"CPT": "CST"},
    164: {np.nan: "CST"},
    165: {"CS": "CST"},  # Uniform across the year.
    173: {
        "CPT": "CST",
        np.nan: "CST",
    },
    174: {
        "CS": "CDT",  # Only shows up in summer! Seems backwards.
        "CD": "CST",  # Only shows up in winter! Seems backwards.
        "433": "CDT",
    },
    176: {
        "E": "EST",
        np.nan: "EST",
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
    208: {np.nan: "CST"},
    211: {
        "206": "EST",
        "DST": "EDT",
        np.nan: "EST",
    },
    213: {"CDS": "CDT"},
    216: {np.nan: "CDT"},
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
        np.nan: "CST",
    },
    281: {"CEN": "CST"},
    288: {np.nan: "EST"},
    293: {np.nan: "MST"},
    294: {np.nan: "EST"},
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

from one year to the next, and these result in duplicate records, which are Note that
the FERC 714 instructions state that all hourly demand is to be reported in STANDARD
time for whatever timezone is being used. Even though many respondents use daylight
savings / standard time abbreviations, a large majority do appear to conform to using a
single UTC offset throughout the year. There are 6 instances in which the timezone
associated with reporting changed dropped.

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


def _standardize_offset_codes(df, offset_fixes):
    """
    Convert to standardized UTC offset abbreviations.

    This function ensures that all of the 3-4 letter abbreviations used to indicate a
    timestamp's localized offset from UTC are standardized, so that they can be used to
    make the timestamps timezone aware. The standard abbreviations we're using are:

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

    In some cases different respondents use the same non-standard abbreviations to
    indicate different offsets, and so the fixes are applied on a per-respondent basis,
    as defined by offset_fixes.

    Args:
        df (pandas.DataFrame): A DataFrame containing a utc_offset_code column
            that needs to be standardized.
        offset_fixes (dict): A dictionary with respondent_id_ferc714 values as the
            keys, and a dictionary mapping non-standard UTC offset codes to
            the standardized UTC offset codes as the value.

    Returns:
        Standardized UTC offset codes.
    """
    logger.debug("Standardizing UTC offset codes.")
    # Treat empty string as missing
    is_blank = df["utc_offset_code"] == ""
    code = df["utc_offset_code"].mask(is_blank)
    # Apply specific fixes on a per-respondent basis:
    return code.groupby(df['respondent_id_ferc714']).apply(
        lambda x: x.replace(offset_fixes[x.name]) if x.name in offset_fixes else x)


def _log_dupes(df, dupe_cols):
    """A macro to report the number of duplicate hours found."""
    n_dupes = len(df[df.duplicated(dupe_cols)])
    logger.debug(f"Found {n_dupes} duplicated hours.")


def respondent_id(tfr_dfs):
    """
    Transform the FERC 714 respondent IDs, names, and EIA utility IDs.

    This consists primarily of dropping test respondents and manually assigning EIA
    utility IDs to a few FERC Form 714 respondents that report planning area demand, but
    which don't have their corresponding EIA utility IDs provided by FERC for some
    reason (including PacifiCorp).

    Args:
        tfr_dfs (dict): A dictionary of (partially) transformed dataframes, to be
            cleaned up.

    Returns:
        dict: The input dictionary of dataframes, but with a finished
        respondent_id_ferc714 dataframe.

    """
    df = (
        tfr_dfs["respondent_id_ferc714"].assign(
            respondent_name_ferc714=lambda x: x.respondent_name_ferc714.str.strip(),
            eia_code=lambda x: x.eia_code.replace(to_replace=0, value=pd.NA))
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

    Transformations include:

    - Clean UTC offset codes.
    - Replace UTC offset codes with UTC offset and timezone.
    - Drop 25th hour rows.
    - Set records with 0 UTC code to 0 demand.
    - Drop duplicate rows.
    - Flip negative signs for reported demand.

    Args:
        tfr_dfs (dict): A dictionary of (partially) transformed dataframes, to be
            cleaned up.

    Returns:
        dict: The input dictionary of dataframes, but with a finished
        pa_demand_hourly_ferc714 dataframe.

    """
    logger.debug("Converting dates into pandas Datetime types.")
    df = tfr_dfs["demand_hourly_pa_ferc714"].copy()

    # Parse date strings
    # NOTE: Faster to ignore trailing 00:00:00 and use exact=False
    df["report_date"] = pd.to_datetime(
        df["report_date"], format="%m/%d/%Y", exact=False
    )

    # Assert that all respondents and years have complete and unique dates
    all_dates = {
        year: set(pd.date_range(f"{year}-01-01", f"{year}-12-31", freq="1D"))
        for year in range(df["report_year"].min(), df["report_year"].max() + 1)
    }
    assert df.groupby(["respondent_id_ferc714", "report_year"]).apply(
        lambda x: set(x["report_date"]) == all_dates[x.name[1]]
    ).all()

    # Clean UTC offset codes
    df["utc_offset_code"] = df["utc_offset_code"].str.strip().str.upper()
    df["utc_offset_code"] = df.pipe(
        _standardize_offset_codes, OFFSET_CODE_FIXES)
    # NOTE: Assumes constant timezone for entire year
    for fix in OFFSET_CODE_FIXES_BY_YEAR:
        mask = (
            (df["report_year"] == fix["report_year"]) &
            (df["respondent_id_ferc714"] == fix["respondent_id_ferc714"])
        )
        df.loc[mask, "utc_offset_code"] = fix["utc_offset_code"]

    # Replace UTC offset codes with UTC offset and timezone
    df["utc_offset"] = df["utc_offset_code"].map(OFFSET_CODES)
    df["timezone"] = df["utc_offset_code"].map(TZ_CODES)
    df.drop(columns="utc_offset_code", inplace=True)

    # Almost all 25th hours are unusable (0.0 or daily totals),
    # and they shouldn't really exist at all based on FERC instructions.
    df.drop(columns="hour25", inplace=True)

    # Melt daily rows with 24 demands to hourly rows with single demand
    logger.debug("Melting daily FERC 714 records into hourly records.")
    df.rename(
        columns=lambda x: int(re.sub(r"^hour", "", x)) -
        1 if "hour" in x else x,
        inplace=True
    )
    df = df.melt(
        id_vars=[
            "respondent_id_ferc714",
            "report_year",
            "report_date",
            "utc_offset",
            "timezone",
        ],
        value_vars=range(24),
        var_name="hour",
        value_name="demand_mwh"
    )

    # Assert that all records missing UTC offset have zero demand
    missing_offset = df["utc_offset"].isna()
    assert df.loc[missing_offset, "demand_mwh"].eq(0).all()
    # Drop these records
    df.query("~@missing_offset", inplace=True)

    # Construct UTC datetime
    logger.debug("Converting local time + offset code to UTC + timezone.")
    hour_timedeltas = {i: pd.to_timedelta(i, unit="h") for i in range(24)}
    df["report_date"] += df["hour"].map(hour_timedeltas)
    df["utc_datetime"] = df["report_date"] - df["utc_offset"]
    df.drop(columns=["hour", "utc_offset"], inplace=True)

    # Report and drop duplicated UTC datetimes
    # There should be less than 10 of these,
    # resulting from changes to a planning area's reporting timezone.
    duplicated = df.duplicated(["respondent_id_ferc714", "utc_datetime"])
    logger.debug(
        f"Found {np.count_nonzero(duplicated)} duplicate UTC datetimes.")
    df.query("~@duplicated", inplace=True)

    # Flip the sign on sections of demand which were reported as negative
    mask = (
        (
            df["report_year"].isin([2006, 2007, 2008, 2009]) &
            (df["respondent_id_ferc714"] == 156)
        ) |
        (
            df["report_year"].isin([2006, 2007, 2008, 2009, 2010]) &
            (df["respondent_id_ferc714"] == 289)
        )
    )
    df.loc[mask, "demand_mwh"] *= -1

    # Convert report_date to first day of year
    df["report_date"] = df["report_date"].astype("datetime64[Y]")

    # Format result
    columns = [
        "respondent_id_ferc714",
        "report_date",
        "utc_datetime",
        "timezone",
        "demand_mwh"
    ]
    df.drop(columns=set(df.columns) - set(columns), inplace=True)
    tfr_dfs["demand_hourly_pa_ferc714"] = df[columns]
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
        raw_dfs (dict): A dictionary of raw pandas.DataFrame objects, as read out of
            the original FERC 714 CSV files. Generated by the
            `pudl.extract.ferc714.extract()` function.
        tables (iterable): The set of PUDL tables within FERC 714 that we should
            process. Typically set to all of them, unless

    Returns:
        dict: A dictionary of pandas.DataFrame objects that are ready to be output in a
        data package / database table.

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
