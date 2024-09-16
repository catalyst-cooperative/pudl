"""Transformation of the FERC Form 714 data.

# TODO: add note about architecture and reusing form 1 stuff.
"""

import re
from dataclasses import dataclass
from typing import Literal

import numpy as np
import pandas as pd
from dagster import AssetCheckResult, AssetChecksDefinition, asset, asset_check

import pudl.logging_helpers
from pudl.metadata import PUDL_PACKAGE
from pudl.transform.classes import (
    RenameColumns,
    TransformParams,
    rename_columns,
)

logger = pudl.logging_helpers.get_logger(__name__)

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
    157: {
        "DST": "EDT",
        "EPT": "EST",
    },
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
    233: {
        "DST": "EDT",
        "EPT": "EST",
    },
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
    "C011454": {"CPT": "CST"},
    "C001555": {"CPT": "CST"},
    "C001552": {"CPT": "CST"},
    "C001553": {"CPT": "CST"},
    "C001556": {"CPT": "CST"},
    "C001554": {"CPT": "CST"},
    "C011510": {"PPT": "PST"},
    "C003677": {"PPT": "PST"},
    "C001646": {"PPT": "PST"},
    "C003850": {"EPT": "EST"},
    "C000135": {"EPT": "EST"},
    "C000290": {"EPT": "EST"},
    "C000136": {"EDT/EST": "EST", "EST/EDT": "EST"},  # this is duke.
    "C011542": {  # more recent years have CST & CDT. CDST correspond to DST months
        "CDST": "CDT"
    },
    "C011543": {"CDST": "CDT"},
    "C011100": {
        "AKT": "AKST",
        "1": "AKST",
        "2": "AKDT",
    },  # they swap from 1 - 2 in 2023
    "C011474": {"UTC": "EST"},  # city of Tallahassee
    "C011432": {"3": "MST"},  # black hills (CO). in year after this 3 its all MST
    "C011568": {np.nan: "PST"},  # just empty in 2021, other years is PST
    "C011431": {np.nan: "PST"},  # just empty in 2022, other years is PST
    "C000618": {np.nan: "EST"},  # this was just one lil empty guy
    "C011399": {np.nan: "PST"},  # this was just one lil empty guy
}

OFFSET_CODE_FIXES_BY_YEAR = [
    {"respondent_id_ferc714": 139, "report_year": 2006, "utc_offset_code": "PST"},
    {"respondent_id_ferc714": 235, "report_year": 2015, "utc_offset_code": "MST"},
    {"respondent_id_ferc714": 289, "report_year": 2011, "utc_offset_code": "CST"},
    {"respondent_id_ferc714": 292, "report_year": 2011, "utc_offset_code": "CST"},
]

BAD_RESPONDENTS = [
    2,
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
"""A mapping of timezone offset codes to Timedelta offsets from UTC.

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
"""Overrides of FERC 714 respondent IDs with wrong or missing EIA Codes."""

RENAME_COLS = {
    "core_ferc714__respondent_id": {
        "csv": {
            "respondent_id": "respondent_id_ferc714",
            "respondent_name": "respondent_name_ferc714",
        }
    },
    "out_ferc714__hourly_planning_area_demand": {
        "csv": {
            "report_yr": "report_year",
            "plan_date": "report_date",
            "respondent_id": "respondent_id_ferc714",  # TODO: change to respondent_id_ferc714_csv
            "timezone": "utc_offset_code",
        },
        "xbrl": {
            "entity_id": "respondent_id_ferc714",  # TODO: change to respondent_id_ferc714_xbrl
            "date": "report_date",
            "report_year": "report_year",
            "time_zone": "utc_offset_code",
            "planning_area_hourly_demand_megawatts": "demand_mwh",
        },
    },
    "core_ferc714__yearly_planning_area_demand_forecast": {
        "csv": {
            "respondent_id": "respondent_id_ferc714",
            "report_yr": "report_year",
            "plan_year": "forecast_year",
            "summer_forecast": "summer_peak_demand_mw",
            "winter_forecast": "winter_peak_demand_mw",
            "net_energy_forecast": "net_demand_mwh",
        }
    },
}


class RenameColumnsFerc714(TransformParams):
    """Dictionaries for renaming either XBRL or CSV derived FERC 714 columns.

    TODO: Determine if this is helpful/worth it. I think it'll only be if there are
    a bunch of share params to validate upfront.
    """

    csv: RenameColumns = RenameColumns()
    xbrl: RenameColumns = RenameColumns()


##############################################################################
# Internal helper functions.
##############################################################################
def pre_process_csv(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """A simple transform function for until the real ones are written.

    * Removes footnotes columns ending with _f
    * Drops report_prd, spplmnt_num, and row_num columns
    * Excludes records which pertain to bad (test) respondents.
    """
    logger.info("Removing unneeded columns and dropping bad respondents.")

    out_df = (
        rename_columns(
            df=df, params=RenameColumns(columns=RENAME_COLS[table_name]["csv"])
        )
        .filter(regex=r"^(?!.*_f$).*")
        .drop(["report_prd", "spplmnt_num", "row_num"], axis="columns", errors="ignore")
    )
    # Exclude fake Test IDs -- not real respondents
    out_df = out_df[~out_df.respondent_id_ferc714.isin(BAD_RESPONDENTS)]
    return out_df


def melt_hourx_columns_csv(df):
    """Melt hourX columns into hours."""
    # Almost all 25th hours are unusable (0.0 or daily totals),
    # and they shouldn't really exist at all based on FERC instructions.
    df = df.drop(columns="hour25")

    # Melt daily rows with 24 demands to hourly rows with single demand
    logger.info("Melting daily FERC 714 records into hourly records.")
    df = df.rename(
        columns=lambda x: int(re.sub(r"^hour", "", x)) - 1 if "hour" in x else x,
    )
    df = df.melt(
        id_vars=[
            "respondent_id_ferc714",
            "report_year",
            "report_date",
            "utc_offset_code",
        ],
        value_vars=range(24),
        var_name="hour",
        value_name="demand_mwh",
    )
    return df


def map_respondent_id_ferc714(df, source: Literal["csv", "xbrl"]):
    """TODO: Make this map!"""
    return df


def remove_yearly_records_duration_xbrl(duration_xbrl):
    """Convert a table with mostly daily records with some annuals into fully daily.

    Almost all of the records have a start_date that == the end_date
    which I'm assuming means the record spans the duration of one day
    there are a small handful of records which seem to span a full year.
    """
    duration_xbrl = duration_xbrl.astype(
        {"start_date": "datetime64[ns]", "end_date": "datetime64[ns]"}
    )
    one_day_mask = duration_xbrl.start_date == duration_xbrl.end_date
    duration_xbrl_one_day = duration_xbrl[one_day_mask]
    duration_xbrl_one_year = duration_xbrl[~one_day_mask]
    # ensure there are really only a few of these multi-day records
    assert len(duration_xbrl_one_year) / len(duration_xbrl_one_day) < 0.0005
    # ensure all of these records are one year records
    assert all(
        duration_xbrl_one_year.start_date
        + pd.DateOffset(years=1)
        - pd.DateOffset(days=1)
        == duration_xbrl_one_year.end_date
    )
    # these one-year records all show up as one-day records.
    idx = ["entity_id", "report_year", "start_date"]
    assert all(
        duration_xbrl_one_year.merge(
            duration_xbrl_one_day, on=idx, how="left", indicator=True
        )._merge
        == "both"
    )
    # all but two of them have the same timezone as the hourly data.
    # two of them have UTC instead of a local timezone reported in hourly data.
    # this leads me to think these are okay to just drop
    return duration_xbrl_one_day


def assign_report_day(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    """Add a report_day column."""
    return df.assign(
        report_day=pd.to_datetime(df[date_col], format="%Y-%m-%d", exact=False)
    )


def merge_instant_and_duration_tables_xbrl(
    instant_xbrl: pd.DataFrame, duration_xbrl: pd.DataFrame, table_name: str
) -> pd.DataFrame:
    """Merge XBRL instant and duration tables, reshaping instant as needed.

    FERC714 XBRL instant period signifies that it is true as of the reported date,
    while a duration fact pertains to the specified time period. The ``date`` column
    for an instant fact corresponds to the ``end_date`` column of a duration fact.

    Args:
        instant_xbrl: table representing XBRL instant facts.
        raw_xbrl_duration: table representing XBRL duration facts.

    Returns:
        A unified table combining the XBRL duration and instant facts, if both types
        of facts were present. If either input dataframe is empty, the other
        dataframe is returned unchanged, except that several unused columns are
        dropped. If both input dataframes are empty, an empty dataframe is returned.
    """
    drop_cols = ["filing_name", "index"]
    # Ignore errors in case not all drop_cols are present.
    instant = instant_xbrl.drop(columns=drop_cols, errors="ignore").pipe(
        assign_report_day, "date"
    )
    duration = duration_xbrl.drop(columns=drop_cols, errors="ignore").pipe(
        assign_report_day, "start_date"
    )

    merge_keys = ["entity_id", "report_year", "report_day", "sched_table_name"]
    # Merge instant into duration.
    out_df = pd.merge(
        instant,
        duration,
        how="left",
        on=merge_keys,
        validate="m:1",
    ).drop(columns=["report_day", "start_date", "end_date"])
    return out_df


def convert_dates_to_zero_offset_hours_xbrl(xbrl: pd.DataFrame) -> pd.DataFrame:
    """Convert all hours to: Hour (24-hour clock) as a zero-padded decimal number.

    Some but not all of the records start with hour 0, while other start with hour 1.
    It is not immediately clear whether or not hours 1-24 corresponds to 1-00 hours.
    """
    bad_24_hour_mask = xbrl.report_date.str.contains("T24:")

    xbrl.loc[bad_24_hour_mask, "report_date"] = pd.to_datetime(
        xbrl[bad_24_hour_mask].report_date.str.replace("T24:", "T23:"),
        format="%Y-%m-%dT%H:%M:%S",
    ) + np.timedelta64(1, "h")
    return xbrl


def parse_date_strings_csv(df, datetime_format):
    """Convert report_date into pandas Datetime types."""
    # Parse date strings
    # NOTE: Faster to ignore trailing 00:00:00 and use exact=False
    df["report_date"] = pd.to_datetime(
        df["report_date"], format=datetime_format, exact=False
    )
    # Assert that all respondents and years have complete and unique dates
    all_dates = {
        year: set(pd.date_range(f"{year}-01-01", f"{year}-12-31", freq="1D"))
        for year in range(df["report_year"].min(), df["report_year"].max() + 1)
    }
    assert (  # nosec B101
        df.groupby(["respondent_id_ferc714", "report_year"])
        .apply(lambda x: set(x["report_date"]) == all_dates[x.name[1]])
        .all()
    )
    return df


def convert_dates_to_zero_seconds_xbrl(xbrl: pd.DataFrame) -> pd.DataFrame:
    """Convert the last second of the day records to the first (0) second of the next day.

    There are a small amount of records which report the last "hour" of the day
    with as last second of the day, as opposed to T24 cleaned in
    :func:`convert_dates_to_zero_offset_hours_xbrl` or T00 which is standard for a
    numpy datetime. This function finds these records and adds one second of them and
    then ensures all of the records has 0's for seconds.
    """
    last_second_mask = xbrl.report_date.dt.second == 59

    xbrl.loc[last_second_mask, "report_date"] = xbrl.loc[
        last_second_mask, "report_date"
    ] + pd.Timedelta("1s")
    assert xbrl[xbrl.report_date.dt.second != 0].empty
    return xbrl


def ensure_dates_are_complete_and_unique(df):
    """Assert that all respondents and years have complete and unique dates."""
    df["gap"] = df[["respondent_id_ferc714", "report_date"]].sort_values(
        by=["respondent_id_ferc714", "report_date"]
    ).groupby("respondent_id_ferc714").diff() > pd.to_timedelta("1h")
    if not (gappy_dates := df[df.gap]).empty:
        raise AssertionError(
            "We expect there to be no gaps in the time series."
            f"but we found these gaps:\n{gappy_dates}"
        )
    return df.drop(columns=["gap"])


def parse_date_strings_xbrl(xbrl: pd.DataFrame) -> pd.DataFrame:
    """Convert report_date into pandas Datetime types."""
    xbrl = xbrl.astype({"report_date": "datetime64[ns]"}).pipe(
        ensure_dates_are_complete_and_unique
    )
    return xbrl


def clean_utc_code_offsets_and_set_timezone(df):
    """Clean UTC Codes and set timezone."""
    # Clean UTC offset codes
    df["utc_offset_code"] = df["utc_offset_code"].str.strip().str.upper()
    df["utc_offset_code"] = _standardize_offset_codes(df, OFFSET_CODE_FIXES)

    # NOTE: Assumes constant timezone for entire year
    for fix in OFFSET_CODE_FIXES_BY_YEAR:
        mask = (df["report_year"] == fix["report_year"]) & (
            df["respondent_id_ferc714"] == fix["respondent_id_ferc714"]
        )
        df.loc[mask, "utc_offset_code"] = fix["utc_offset_code"]

    # Replace UTC offset codes with UTC offset and timezone
    df["utc_offset"] = df["utc_offset_code"].map(OFFSET_CODES)
    df["timezone"] = df["utc_offset_code"].map(TZ_CODES)
    return df


def drop_missing_utc_offset(df):
    """Drop records with missing UTC offsets and zero demand."""
    # Assert that all records missing UTC offset have zero demand
    missing_offset = df["utc_offset"].isna()
    bad_offset_and_demand = df.loc[missing_offset & (df.demand_mwh != 0)]
    if not bad_offset_and_demand.empty:
        raise AssertionError(
            "We expect all of the records without a cleaned utc_offset "
            f"to not have any demand data, but we found {len(bad_offset_and_demand)} "
            "records.\nUncleaned Codes: "
            f"{bad_offset_and_demand.utc_offset_code.unique()}"
        )
    # Drop these records & then drop the original offset code
    df = df.query("~@missing_offset").drop(columns="utc_offset_code")
    return df


def construct_utc_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """Construct datetime_utc column."""
    # Construct UTC datetime
    logger.info("Converting local time + offset code to UTC + timezone.")
    hour_timedeltas = {i: pd.to_timedelta(i, unit="h") for i in range(24)}
    df["report_date"] += df["hour"].map(hour_timedeltas)
    df["datetime_utc"] = df["report_date"] - df["utc_offset"]
    df = df.drop(columns=["hour", "utc_offset"])

    # Report and drop duplicated UTC datetimes
    # There should be less than 10 of these,
    # resulting from changes to a planning area's reporting timezone.
    duplicated = df.duplicated(["respondent_id_ferc714", "datetime_utc"])
    # TODO: convert this into an error
    logger.info(f"Found {np.count_nonzero(duplicated)} duplicate UTC datetimes.")
    df = df.query("~@duplicated")
    return df


def spot_fix_values(df: pd.DataFrame) -> pd.DataFrame:
    """Spot fix values."""
    # Flip the sign on sections of demand which were reported as negative
    mask = (
        df["report_year"].isin([2006, 2007, 2008, 2009])
        & (df["respondent_id_ferc714"] == 156)
    ) | (
        df["report_year"].isin([2006, 2007, 2008, 2009, 2010])
        & (df["respondent_id_ferc714"] == 289)
    )
    df.loc[mask, "demand_mwh"] *= -1
    return df


def _post_process(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Uniform post-processing of FERC 714 tables.

    Applies standard data types and ensures that the tables generally conform to the
    schemas we have defined for them.

    Args:
        df: A dataframe to be post-processed.

    Returns:
        The post-processed dataframe.
    """
    return PUDL_PACKAGE.get_resource(table_name).enforce_schema(df)


def _standardize_offset_codes(df: pd.DataFrame, offset_fixes) -> pd.DataFrame:
    """Convert to standardized UTC offset abbreviations.

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
        df: DataFrame containing a utc_offset_code column that needs to be standardized.
        offset_fixes: A dictionary with respondent_id_ferc714 values as the keys, and a
            dictionary mapping non-standard UTC offset codes to the standardized UTC
            offset codes as the value.

    Returns:
        Standardized UTC offset codes.
    """
    logger.info("Standardizing UTC offset codes.")
    # We only need a couple of columns here:
    codes = df[["respondent_id_ferc714", "utc_offset_code"]].copy()
    # Set all blank "" missing UTC codes to np.nan
    codes["utc_offset_code"] = codes.utc_offset_code.mask(codes.utc_offset_code == "")
    # Apply specific fixes on a per-respondent basis:
    codes = codes.groupby("respondent_id_ferc714").transform(
        lambda x: x.replace(offset_fixes[x.name]) if x.name in offset_fixes else x
    )
    return codes


@asset(
    io_manager_key="pudl_io_manager",
    compute_kind="pandas",
)
def core_ferc714__respondent_id(
    raw_ferc714_csv__respondent_id: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the FERC 714 respondent IDs, names, and EIA utility IDs.

    Clean up FERC-714 respondent names and manually assign EIA utility IDs to a few FERC
    Form 714 respondents that report planning area demand, but which don't have their
    corresponding EIA utility IDs provided by FERC for some reason (including
    PacifiCorp).

    Args:
        raw_ferc714_csv__respondent_id: Raw table describing the FERC 714 Respondents.

    Returns:
        A clean(er) version of the FERC-714 respondents table.
    """
    df = pre_process_csv(
        raw_ferc714_csv__respondent_id, table_name="core_ferc714__respondent_id"
    )
    df["respondent_name_ferc714"] = df.respondent_name_ferc714.str.strip()
    df.loc[df.eia_code == 0, "eia_code"] = pd.NA
    # There are a few utilities that seem mappable, but missing:
    for rid in EIA_CODE_FIXES:
        df.loc[df.respondent_id_ferc714 == rid, "eia_code"] = EIA_CODE_FIXES[rid]
    return _post_process(df, table_name="core_ferc714__respondent_id")


@asset(
    io_manager_key="parquet_io_manager",
    op_tags={"memory-use": "high"},
    compute_kind="pandas",
)
def out_ferc714__hourly_planning_area_demand(  # noqa: C901
    raw_ferc714_csv__hourly_planning_area_demand: pd.DataFrame,
    raw_ferc714_xbrl__planning_area_hourly_demand_and_forecast_summer_and_winter_peak_demand_and_annual_net_energy_for_load_03_2_duration: pd.DataFrame,
    raw_ferc714_xbrl__planning_area_hourly_demand_and_forecast_summer_and_winter_peak_demand_and_annual_net_energy_for_load_03_2_instant: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the hourly demand time series by Planning Area.

    Transformations include:

    - Clean UTC offset codes.
    - Replace UTC offset codes with UTC offset and timezone.
    - Drop 25th hour rows.
    - Set records with 0 UTC code to 0 demand.
    - Drop duplicate rows.
    - Flip negative signs for reported demand.

    Args:
        raw_ferc714_csv__hourly_planning_area_demand: Raw table containing hourly demand
            time series by Planning Area.

    Returns:
        Clean(er) version of the hourly demand time series by Planning Area.
    """
    table_name = "out_ferc714__hourly_planning_area_demand"
    # CSV STUFF
    csv = (
        pre_process_csv(
            raw_ferc714_csv__hourly_planning_area_demand, table_name=table_name
        )
        .pipe(map_respondent_id_ferc714, "csv")
        .pipe(melt_hourx_columns_csv)
        .pipe(parse_date_strings_csv, datetime_format="%m/%d/%Y")
    )
    # XBRL STUFF
    duration_xbrl = remove_yearly_records_duration_xbrl(
        raw_ferc714_xbrl__planning_area_hourly_demand_and_forecast_summer_and_winter_peak_demand_and_annual_net_energy_for_load_03_2_duration
    )
    instant_xbrl = raw_ferc714_xbrl__planning_area_hourly_demand_and_forecast_summer_and_winter_peak_demand_and_annual_net_energy_for_load_03_2_instant
    xbrl = (
        merge_instant_and_duration_tables_xbrl(
            instant_xbrl, duration_xbrl, table_name=table_name
        )
        .pipe(
            rename_columns,
            params=RenameColumns(columns=RENAME_COLS[table_name]["xbrl"]),
        )
        .pipe(map_respondent_id_ferc714, "xbrl")
        .pipe(convert_dates_to_zero_offset_hours_xbrl)
        .pipe(parse_date_strings_xbrl)
        .pipe(convert_dates_to_zero_seconds_xbrl)
    )
    # CONCATED STUFF
    df = (
        pd.concat([csv, xbrl])
        .pipe(clean_utc_code_offsets_and_set_timezone)
        .pipe(drop_missing_utc_offset)
        .pipe(construct_utc_datetime)
        .pipe(spot_fix_values)
        # Convert report_date to first day of year
        .assign(report_date=lambda x: x.report_date.dt.to_period("Y").dt.to_timestamp())
        .pipe(_post_process, table_name=table_name)
    )
    return df


@asset(
    io_manager_key="pudl_io_manager",
    compute_kind="pandas",
)
def core_ferc714__yearly_planning_area_demand_forecast(
    raw_ferc714_csv__yearly_planning_area_demand_forecast: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the yearly planning area forecast data per Planning Area.

    Transformations include:

    - Drop/rename columns.
    - Remove duplicate rows and average out the metrics.

    Args:
        raw_ferc714_csv__yearly_planning_area_demand_forecast: Raw table containing,
            for each year and each planning area, the forecasted summer and winter peak demand,
            in megawatts, and annual net energy for load, in megawatthours, for the next
            ten years.

    Returns:
        Clean(er) version of the yearly forecasted demand by Planning Area.
    """
    # Clean up columns
    df = pre_process_csv(
        raw_ferc714_csv__yearly_planning_area_demand_forecast,
        table_name="core_ferc714__yearly_planning_area_demand_forecast",
    )

    # For any rows with non-unique respondent_id_ferc714/report_year/forecast_year,
    # group and take the mean measures
    # For the 2006-2020 data, there were only 20 such rows. In most cases, demand metrics were identical.
    # But for some, demand metrics were different - thus the need to take the average.
    logger.info(
        "Removing non-unique report rows and taking the average of non-equal metrics."
    )

    # Grab the number of rows before duplicate cleanup
    num_rows_before = len(df)

    df = (
        df.groupby(["respondent_id_ferc714", "report_year", "forecast_year"])[
            ["summer_peak_demand_mw", "winter_peak_demand_mw", "net_demand_mwh"]
        ]
        .mean()
        .reset_index()
    )

    # Capture the number of rows after grouping
    num_rows_after = len(df)

    # Add the number of duplicates removed as metadata
    num_duplicates_removed = num_rows_before - num_rows_after
    logger.info(f"Number of duplicate rows removed: {num_duplicates_removed}")
    # Assert that number of removed rows meets expectation
    assert (
        num_duplicates_removed <= 20
    ), f"Expected no more than 20 duplicates removed, but found {num_duplicates_removed}"

    # Check all data types and columns to ensure consistency with defined schema
    df = _post_process(
        df, table_name="core_ferc714__yearly_planning_area_demand_forecast"
    )
    return df


@dataclass
class Ferc714CheckSpec:
    """Define some simple checks that can run on FERC 714 assets."""

    name: str
    asset: str
    num_rows_by_report_year: dict[int, int]


check_specs = [
    Ferc714CheckSpec(
        name="yearly_planning_area_demand_forecast_check_spec",
        asset="core_ferc714__yearly_planning_area_demand_forecast",
        num_rows_by_report_year={
            2006: 1819,
            2007: 1570,
            2008: 1540,
            2009: 1269,
            2010: 1259,
            2011: 1210,
            2012: 1210,
            2013: 1192,
            2014: 1000,
            2015: 990,
            2016: 990,
            2017: 980,
            2018: 961,
            2019: 950,
            2020: 950,
        },
    )
]


def make_check(spec: Ferc714CheckSpec) -> AssetChecksDefinition:
    """Turn the Ferc714CheckSpec into an actual Dagster asset check."""

    @asset_check(asset=spec.asset, blocking=True)
    def _check(df):
        errors = []
        for year, expected_rows in spec.num_rows_by_report_year.items():
            if (num_rows := len(df.loc[df.report_year == year])) != expected_rows:
                errors.append(
                    f"Expected {expected_rows} for report year {year}, found {num_rows}"
                )

        if errors:
            return AssetCheckResult(passed=False, metadata={"errors": errors})

        return AssetCheckResult(passed=True)

    return _check


_checks = [make_check(spec) for spec in check_specs]
