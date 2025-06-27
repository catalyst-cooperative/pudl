"""Transformation of the FERC Form 714 data.

FERC Form 714 has two separate raw data sources - CSV and XBRL. For both sources
there is usually some specific processing that needs to happen before the two
data sources get concatenated together to create the full timeseries. We are
currently processing three tables from 714. Each one is processed using a similar
pattern: we've defined a class with a run classmethod as a coordinating method,
any table-specific transforms are defined as staticmethod's within the table
class and any generic 714 transforms are defined as internal module functions.
The table assets are created through a small function that calls the run method.
Any of the methods or functions that only apply to either of the raw data sources
should include a raw datasource suffix.
"""

import importlib
import re
from typing import Literal

import numpy as np
import pandas as pd
from dagster import AssetIn, asset

import pudl.logging_helpers
from pudl.extract.ferc714 import TABLE_NAME_MAP_FERC714
from pudl.settings import Ferc714Settings
from pudl.transform.classes import (
    RenameColumns,
    rename_columns,
)
from pudl.transform.ferc import filter_for_freshest_data_xbrl, get_primary_key_raw_xbrl

logger = pudl.logging_helpers.get_logger(__name__)

##############################################################################
# Constants required for transforming FERC 714
##############################################################################


# More detailed fixes on a per respondent basis
TIMEZONE_OFFSET_CODE_FIXES = {
    2: {"CPT": "CST"},
    10: {"CPT": "EST"},
    15: {"MS": "MST"},
    17: {"CS": "CST", "CD": "CDT"},
    19: {"CTR": "CST", "CSR": "CST", "CPT": "CST", "DST": "CST", np.nan: "CST"},
    27: {
        "AKS": "AKST",
        "AST": "AKST",
        "AKD": "AKDT",
        "ADT": "AKDT",
        "AKT": "AKST",
        "1": "AKST",  # they swap from 1 - 2 in 2023
        "2": "AKDT",
    },
    28: {np.nan: "EST"},
    31: {np.nan: "CST"},
    34: {"1": "EST", "2": "EDT", np.nan: "EST", "UTC": "EST"},  # city of Tallahassee
    35: {np.nan: "CST"},
    37: {"MS": "MST"},
    40: {"DST": "EST"},
    42: {np.nan: "CST"},
    45: {"DST": "CDT", np.nan: "CST"},
    47: {np.nan: "MST"},
    48: {np.nan: "MST"},
    50: {np.nan: "CST"},
    51: {"DST": "EDT", "EPT": "EST"},
    54: {"CPT": "CST"},
    56: {"CPT": "CST"},
    57: {np.nan: "CST"},
    58: {"CS": "CST"},  # Uniform across the year.
    65: {"CPT": "CST", np.nan: "CST"},
    66: {
        "CS": "CDT",  # Only shows up in summer! Seems backwards.
        "CD": "CST",  # Only shows up in winter! Seems backwards.
        "433": "CDT",
    },
    68: {"E": "EST", np.nan: "EST"},
    73: {"PPT": "PDT"},  # Imperial Irrigation District P looks like D
    77: {"EAS": "EST"},
    80: {"CPT": "CST"},
    81: {"CPT": "CST"},
    83: {"CS": "CST", "CD": "CDT"},
    84: {"PPT": "PST"},  # LADWP, constant across all years.
    85: {"CPT": "CST"},
    97: {np.nan: "CST"},
    100: {"206": "EST", "DST": "EDT", np.nan: "EST"},
    102: {"CDS": "CDT", "CDST": "CDT"},
    105: {np.nan: "CDT"},
    106: {"MPP": "MST", "MPT": "MST"},
    113: {"DST": "EST"},
    114: {"EDS": "EDT", "DST": "EDT", "EPT": "EST"},
    115: {"DST": "CDT"},
    119: {"EPT": "EST"},
    122: {"DST": "EDT", "EPT": "EST"},
    123: {"1": "EST", "2": "EDT", "DST": "EDT"},
    128: {"PPT": "PST"},  # Constant across the year. Never another timezone seen.
    132: {"DST": "PST", np.nan: "PST"},
    134: {"CDS": "CDT"},
    137: {"DST": "EDT"},
    142: {"CPT": "CST"},
    143: {"DST": "CDT"},
    146: {"CPT": "CST"},
    148: {"DST": "CDT"},
    153: {"CDS": "CDT"},
    159: {"EDS": "EDT"},
    163: {"CPT": "CST"},
    164: {"CPT": "CST", np.nan: "CST"},
    168: {"CEN": "CST"},
    175: {np.nan: "EST"},
    180: {np.nan: "MST"},
    181: {np.nan: "EST"},
    183: {"CPT": "CST"},
    184: {"CPT": "CST"},
    185: {"CPT": "CST"},
    186: {"CPT": "CST"},
    194: {"PPT": "PST"},  # Pacificorp, constant across the whole year.
    195: {"DST": "EDT", "EDS": "EDT", "EPT": "EST"},
    210: {"EPT": "EST"},
    217: {"CPT": "CST"},
    214: {"EPT": "EST"},
    215: {"EDT/EST": "EST", "EST/EDT": "EST"},  # this is duke.
    211: {  # more recent years have CST & CDT. CDST correspond to DST months
        "CDST": "CDT"
    },
    20: {"3": "MST"},  # black hills (CO). in year after this 3 its all MST
    95: {np.nan: "PST"},  # just empty in 2021, other years is PST
    29: {np.nan: "PST"},  # just empty in 2022, other years is PST
    101: {np.nan: "EST"},  # this was just one lil empty guy
}

TIMEZONE_OFFSET_CODE_FIXES_BY_YEAR = [
    {"respondent_id_ferc714": 33, "report_year": 2006, "utc_offset_code": "PST"},
    {"respondent_id_ferc714": 124, "report_year": 2015, "utc_offset_code": "MST"},
    {"respondent_id_ferc714": 176, "report_year": 2011, "utc_offset_code": "CST"},
    {"respondent_id_ferc714": 179, "report_year": 2011, "utc_offset_code": "CST"},
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

TIMEZONE_OFFSET_CODES = {
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

Note that the FERC 714 instructions state that all hourly demand is to be reported
in STANDARD time for whatever timezone is being used. Even though many respondents
use daylight savings / standard time abbreviations, a large majority do appear to
conform to using a single UTC offset throughout the year. There are 6 instances in
which the timezone associated with reporting changed dropped.
"""

TIMEZONE_CODES = {
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

EIA_CODE_FIXES: dict[Literal["combined", "csv", "xbrl"], dict[int | str], int] = {
    "combined": {
        # FERC 714 Respondent ID: EIA BA or Utility ID
        125: 2775,  # EIA BA CAISO (fixing bad EIA Code of 229)
        47: 56812,  # Duke Energy Control Area Services, LLC (Arlington Valley WECC AZ)
        146: 59504,  # Southwest Power Pool (Fixing bad EIA Coding)
        180: 32790,  # New Harquahala.
        # PacifiCorp Utility ID is 14354. It ALSO has 2 BA IDs: (14378, 14379)
        # See https://github.com/catalyst-cooperative/pudl/issues/616
        194: 14379,  # Using this ID for now only b/c it's in the HIFLD geometry
        206: 58791,  # NaturEner Wind Watch LLC (Fixes bad ID 57995)
        201: 56090,  # Griffith Energy (bad id was 55124)
        205: 58790,  # Gridforce Energy Management (missing or 11378 in xbrl)
        213: 64898,  # GridLiance (missing)
    },
    "xbrl": {
        # FERC 714 Respondent ID XBRL: EIA BA or Utility ID
        "C011373": 14610,  # Florida Municipal Power Pool (lines up with CSV code & is FL util)
        "C011421": 9617,  # JEA - lines up w/ CSV code and is EIA util
        "C002732": 56365,  # NaturEner Power Watch LLC: Fixes bad ID "57049, 57050"
        "C002447": 7004,  # Buckeye Power: was null or the entity_id
        "C001526": 14369,  # Avangrid Renewables: was null or the entity_id
        "C001132": 15248,  # PGE. Bad id was 43. New one lines up w/ CSV  and is EIA util
    },
    "csv": {
        # FERC 714 Respondent ID CSV: EIA BA or Utility ID
        134: 5416,  # Duke Energy Corp. (bad id was non-existent 3260)
        203: 12341,  # MidAmerican Energy Co. (fixes typo, from 12431)
        292: 20382,  # City of West Memphis -- (fixes a typo, from 20383)
        295: 40229,  # Old Dominion Electric Cooperative (missing)
        301: 14725,  # PJM Interconnection Eastern Hub (missing)
        302: 14725,  # PJM Interconnection Western Hub (missing)
        303: 14725,  # PJM Interconnection Illinois Hub (missing)
        304: 14725,  # PJM Interconnection Northern Illinois Hub (missing)
        305: 14725,  # PJM Interconnection Dominion Hub (missing)
        306: 14725,  # PJM Interconnection AEP-Dayton Hub (missing)
        309: 12427,  # Michigan Power Pool / Power Coordination Center (missing)
        312: 59435,  # NaturEner Glacier Wind (missing)
        329: 39347,  # East Texas Electricity Cooperative (missing)
    },
}
"""Overrides of FERC 714 respondent IDs with wrong or missing EIA Codes.

This is used in :meth:`RespondentId.spot_fix_eia_codes`. The dictionary
is organized by "source" keys ("combined", "csv", or "xbrl"). Each source's
value is a secondary dictionary which contains source respondent ID's as keys
and fixes for EIA codes as values.

We separated these fixes by either coming directly from the CSV data, the XBRL
data, or the combined data. We use the corresponding source or PUDL-derived
respondent ID to identify the EIA code to overwrite. We could have combined
these fixes all into one set of combined fixes identified by the PUDL-derived
``respondent_id_ferc714``, but this way we can do more targeted source-based
cleaning and test each source's EIA codes before the sources are concatenated
together.
"""

RENAME_COLS = {
    "core_ferc714__respondent_id": {
        "csv": {
            "respondent_id": "respondent_id_ferc714_csv",
            "respondent_name": "respondent_name_ferc714",
            "eia_code": "eia_code",
        },
        "xbrl": {
            "entity_id": "respondent_id_ferc714_xbrl",
            "respondent_legal_name": "respondent_name_ferc714",
            "respondent_identification_code": "eia_code",
        },
    },
    "core_ferc714__hourly_planning_area_demand": {
        "csv": {
            "report_yr": "report_year",
            "plan_date": "report_date",
            "respondent_id": "respondent_id_ferc714_csv",
            "timezone": "utc_offset_code",
        },
        "xbrl": {
            "entity_id": "respondent_id_ferc714_xbrl",
            "date": "report_date",
            "report_year": "report_year",
            "time_zone": "utc_offset_code",
            "planning_area_hourly_demand_megawatts": "demand_mwh",
        },
    },
    "core_ferc714__yearly_planning_area_demand_forecast": {
        "csv": {
            "respondent_id": "respondent_id_ferc714_csv",
            "report_yr": "report_year",
            "plan_year": "forecast_year",
            "summer_forecast": "summer_peak_demand_forecast_mw",
            "winter_forecast": "winter_peak_demand_forecast_mw",
            "net_energy_forecast": "net_demand_forecast_mwh",
        },
        "xbrl": {
            "entity_id": "respondent_id_ferc714_xbrl",
            "start_date": "start_date",
            "end_date": "end_date",
            "report_year": "report_year",
            "planning_area_hourly_demand_and_forecast_year": "forecast_year",
            "planning_area_hourly_demand_and_forecast_summer_forecast": "summer_peak_demand_forecast_mw",
            "planning_area_hourly_demand_and_forecast_winter_forecast": "winter_peak_demand_forecast_mw",
            "planning_area_hourly_demand_and_forecast_forecast_of_annual_net_energy_for_load": "net_demand_forecast_mwh",
        },
    },
}


##############################################################################
# Internal helper functions.
##############################################################################
def _pre_process_csv(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """A simple transform function for processing the CSV raw data.

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
    out_df = out_df[~out_df.respondent_id_ferc714_csv.isin(BAD_RESPONDENTS)]
    return out_df


def _assign_respondent_id_ferc714(
    df: pd.DataFrame, source: Literal["csv", "xbrl"]
) -> pd.DataFrame:
    """Assign the PUDL-assigned respondent_id_ferc714 based on the native respondent ID.

    We need to replace the natively reported respondent ID from each of the two FERC714
    sources with a PUDL-assigned respondent ID. The mapping between the native ID's and
    these PUDL-assigned ID's can be accessed in the database tables
    ``respondents_csv_ferc714`` and ``respondents_xbrl_ferc714``.

    Args:
        df: the input table with the native respondent ID column.
        source: the lower-case string name of the source of the FERC714 data. Either csv
        or xbrl.

    Returns:
        an augmented version of the input ``df`` with a new column that replaces
        the natively reported respondent ID with the PUDL-assigned respondent ID.
    """
    respondent_map_ferc714 = pd.read_csv(
        importlib.resources.files("pudl.package_data.glue")
        / "respondent_id_ferc714.csv"
    ).convert_dtypes()
    # use the source utility ID column to get a unique map and for merging
    resp_id_col = f"respondent_id_ferc714_{source}"
    resp_map_series = (
        respondent_map_ferc714.dropna(subset=[resp_id_col])
        .set_index(resp_id_col)
        .respondent_id_ferc714
    )

    df["respondent_id_ferc714"] = df[resp_id_col].map(resp_map_series)
    return df


def _filter_for_freshest_data_xbrl(
    raw_xbrl: pd.DataFrame,
    table_name: str,
    instant_or_duration: Literal["instant", "duration"],
):
    """Wrapper around filter_for_freshest_data_xbrl.

    Most of the specific stuff here is in just converting the core table name
    into the raw instant or duration XBRL table name.
    """
    table_name_raw_xbrl = (
        f"{TABLE_NAME_MAP_FERC714[table_name]['xbrl']}_{instant_or_duration}"
    )
    xbrl = filter_for_freshest_data_xbrl(
        raw_xbrl,
        get_primary_key_raw_xbrl(table_name_raw_xbrl, "ferc714"),
    )
    return xbrl


def _fillna_respondent_id_ferc714_source(
    df: pd.DataFrame, source: Literal["csv", "xbrl"]
) -> pd.DataFrame:
    """Fill missing CSV or XBRL respondent id.

    The source (CSV or XBRL) tables get assigned a PUDL-derived
    ``respondent_id_ferc714`` ID column (via :func:`_assign_respondent_id_ferc714`).
    After we concatenate the source tables, we sometimes backfill and
    forward-fill the source IDs (``respondent_id_ferc714_csv`` and
    ``respondent_id_ferc714_xbrl``). This way the older records from the CSV years
    will also have the XBRL ID's and vice versa. This will enable users to find
    the full timeseries of a respondent that given either source ID (instead of
    using the source ID to find the PUDL-derived ID and then finding the records).
    """
    respondent_map_ferc714 = pd.read_csv(
        importlib.resources.files("pudl.package_data.glue")
        / "respondent_id_ferc714.csv"
    ).convert_dtypes()
    # use the source utility ID column to get a unique map and for merging
    resp_id_col = f"respondent_id_ferc714_{source}"
    resp_map_series = respondent_map_ferc714.dropna(subset=[resp_id_col]).set_index(
        "respondent_id_ferc714"
    )[resp_id_col]

    df[resp_id_col] = df[resp_id_col].fillna(
        df["respondent_id_ferc714"].map(resp_map_series)
    )
    return df


def assign_report_day(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    """Add a report_day column."""
    return df.assign(
        report_day=pd.to_datetime(df[date_col], format="%Y-%m-%d", exact=False)
    )


class RespondentId:
    """Class for building the :ref:`core_ferc714__respondent_id` asset.

    Most of the methods in this class as staticmethods. The purpose of using a class
    in this instance is mostly for organizing the table specific transforms under the
    same name-space.
    """

    @classmethod
    def run(
        cls, raw_csv: pd.DataFrame, raw_xbrl_duration: pd.DataFrame
    ) -> pd.DataFrame:
        """Build the table for the :ref:`core_ferc714__respondent_id` asset.

        Process and combine the CSV and XBRL based data.

        There are two main threads of transforms happening here:

        * Table compatibility: The CSV raw table is static (does not even report years)
          while the xbrl table is reported annually. A lot of the downstream analysis
          expects this table to be static. So the first step was to check whether or not
          the columns that we have in the CSV years had consistent data over the few XBRL
          years that we have. There are a small number of eia_code's we needed to clean
          up, but besides that it was static. We then convert the XBRL data into a static
          table, then we concat-ed the tables and checked the static-ness again via
          :meth:`ensure_eia_code_uniqueness`.
        * eia_code cleaning: Clean up FERC-714 respondent names and manually assign EIA
          utility IDs to a few FERC Form 714 respondents that report planning area demand,
          but which don't have their corresponding EIA utility IDs provided by FERC for
          some reason (including PacifiCorp). Done all via :meth:`spot_fix_eia_codes` &
          EIA_CODE_FIXES.

        """
        table_name = "core_ferc714__respondent_id"
        # CSV STUFF
        csv = (
            _pre_process_csv(raw_csv, table_name)
            .pipe(_assign_respondent_id_ferc714, source="csv")
            .astype({"eia_code": pd.Int64Dtype()})
            .pipe(cls.spot_fix_eia_codes, "csv")
            .pipe(cls.ensure_eia_code_uniqueness, "csv")
            .assign(source="csv")
        )
        # XBRL STUFF
        xbrl = (
            _filter_for_freshest_data_xbrl(raw_xbrl_duration, table_name, "duration")
            .pipe(
                rename_columns,
                params=RenameColumns(columns=RENAME_COLS[table_name]["xbrl"]),
            )
            .pipe(_assign_respondent_id_ferc714, source="xbrl")
            .pipe(cls.clean_eia_codes_xbrl)
            .astype({"eia_code": pd.Int64Dtype()})
            .pipe(cls.spot_fix_eia_codes, "xbrl")
            .pipe(cls.ensure_eia_code_uniqueness, "xbrl")
            .pipe(cls.convert_into_static_table_xbrl)
            .assign(source="xbrl")
        )
        # CONCATED STUFF
        df = (
            pd.concat([csv, xbrl])
            .reset_index(drop=True)
            .convert_dtypes()
            .pipe(cls.spot_fix_eia_codes, "combined")
            .pipe(cls.ensure_eia_code_uniqueness, "combined")
            .pipe(cls.condense_into_one_source_table)
            .pipe(_fillna_respondent_id_ferc714_source, "csv")
            # the xbrl version of this is fillna is not *strictly necessary*
            # bc we are sorting the records grab the xbrl record if there is one
            # for each respondent during condense_into_one_source_table.
            .pipe(_fillna_respondent_id_ferc714_source, "xbrl")
        )
        return df

    @staticmethod
    def spot_fix_eia_codes(
        df: pd.DataFrame, source: Literal["csv", "xbrl", "combined"]
    ) -> pd.DataFrame:
        """Spot fix the eia_codes.

        Using the manually compiled fixes to the ``eia_code`` column stored in
        :py:const:`EIA_CODE_FIXES`, replace the reported values by respondent.
        """
        df.loc[df.eia_code == 0, "eia_code"] = pd.NA
        suffix = "" if source == "combined" else f"_{source}"
        # There are a few utilities that seem mappable, but missing:
        for rid, new_code in EIA_CODE_FIXES[source].items():
            df.loc[df[f"respondent_id_ferc714{suffix}"] == rid, "eia_code"] = new_code
        return df

    @staticmethod
    def ensure_eia_code_uniqueness(
        df: pd.DataFrame, source: Literal["csv", "xbrl", "combined"]
    ) -> pd.DataFrame:
        """Ensure there is only one unique eia_code for each respondent."""
        df["eia_code_count"] = (
            df.dropna(subset=["eia_code"])
            .groupby(["respondent_id_ferc714"])[["eia_code"]]
            .transform("nunique")
        )
        if not (
            multiple_eia_codes := df[(df.eia_code_count != 1) & (df.eia_code.notnull())]
        ).empty:
            raise AssertionError(
                "We expected 0 respondents with multiple different eia_code's "
                f"reported for each respondent in {source} data, "
                f"but we found {len(multiple_eia_codes)}"
            )
        return df.drop(columns=["eia_code_count"])

    @staticmethod
    def clean_eia_codes_xbrl(xbrl: pd.DataFrame) -> pd.DataFrame:
        """Make eia_code's cleaner coming from the XBRL data.

        Desired outcomes here include all respondents have only one non-null
        eia_code and all eia_codes that are actually the respondent_id_ferc714_xbrl
        are nulled.
        """
        # we expect all of these submissions to be from the last Q
        assert all(xbrl.report_period == "Q4")
        # first we are gonna null out all of the "EIA" codes that are really just the respondent id
        code_is_respondent_id_mask = xbrl.eia_code.str.startswith("C") & (
            xbrl.respondent_id_ferc714_xbrl == xbrl.eia_code
        )
        xbrl.loc[code_is_respondent_id_mask, "eia_code"] = pd.NA

        # lets null out some of the eia_code's from XBRL that we've manually culled
        # because they are were determined to be wrong. These respondents
        # had more than one value for their eia_code and one was always wrong
        respondent_id_xbrl_to_bad_eia_code = {
            "C002422": ["5776"],
            "C011374": ["8376"],
            "C002869": ["F720204"],
            "C002732": ["F720204", "57049, 57050"],
            "C011420": ["16606"],
        }
        for rid_xbrl, bad_eia_codes in respondent_id_xbrl_to_bad_eia_code.items():
            xbrl.loc[
                (xbrl.respondent_id_ferc714_xbrl == rid_xbrl)
                & (xbrl.eia_code.isin(bad_eia_codes)),
                "eia_code",
            ] = pd.NA
        return xbrl

    @staticmethod
    def convert_into_static_table_xbrl(xbrl: pd.DataFrame) -> pd.DataFrame:
        """Convert this annually reported table into a skinnier, static table.

        The CSV table is entirely static - it doesn't have any reported
        changes that vary over time. The XBRL table does have start and end
        dates in it. In order to merge these two sources, we are checking
        whether or not the shared variables change over time and then
        converting this table into a non-time-varying table.
        """
        # the CSV data does not vary by year, so we need to check if that is
        # also going to be the case for the XBRL data. we check the eia_codes
        # during ensure_eia_code_uniqueness. The name is less crucial but we
        # should still check.
        assert all(
            xbrl.groupby(["respondent_id_ferc714_xbrl"])[  # noqa: PD101
                ["respondent_name_ferc714"]
            ].nunique()
            == 1
        )
        cols_to_keep = [
            "respondent_id_ferc714",
            "respondent_id_ferc714_xbrl",
            "respondent_name_ferc714",
            "eia_code",
        ]
        # we are going to first sort by report year (descending) so the more recent
        # name is the name we get - just in case - we are checking for consistency of
        # the name above.
        return (
            xbrl.sort_values(["report_year"], ascending=False)[cols_to_keep]
            .sort_values(["respondent_id_ferc714", "eia_code"])
            .drop_duplicates(subset=["respondent_id_ferc714"], keep="first")
        )

    @staticmethod
    def condense_into_one_source_table(df):
        """Condense the CSV and XBRL records together into one record.

        We have two records coming from each of the two sources in this table.
        This method simply drops duplicates based on the PKs of the table.
        We know that the names are different in the CSV vs the XBRL source.
        We are going to grab the XBRL names because they are more recent.

        NOTE: We could have merged the data in :meth:`run` instead of concatenating
        along the index. We would have had to develop different methods for
        :meth:`ensure_eia_code_uniqueness`.
        """
        return df.sort_values(["source"], ascending=False).drop_duplicates(
            subset=["respondent_id_ferc714", "eia_code"], keep="first"
        )


@asset(
    io_manager_key="pudl_io_manager",
    ins={
        "raw_csv": AssetIn(key="raw_ferc714_csv__respondent_id"),
        "raw_xbrl_duration": AssetIn(
            key="raw_ferc714_xbrl__identification_and_certification_01_1_duration"
        ),
    },
    compute_kind="pandas",
)
def core_ferc714__respondent_id(
    raw_csv: pd.DataFrame, raw_xbrl_duration: pd.DataFrame
) -> pd.DataFrame:
    """Transform the FERC 714 respondent IDs, names, and EIA utility IDs.

    This is a light wrapper around :class:`RespondentId` because you need to
    build an asset from a function - not a staticmethod of a class.

    Args:
        raw_csv: Raw table describing the FERC 714 Respondents from the CSV years.
        raw_xbrl_duration: Raw table describing the FERC 714 Respondents from the
            XBRL years.

    Returns:
        A clean(er) version of the FERC-714 respondents table.
    """
    return RespondentId.run(raw_csv, raw_xbrl_duration)


class HourlyPlanningAreaDemand:
    """Class for building the :ref:`core_ferc714__hourly_planning_area_demand` asset.

    The :ref:`core_ferc714__hourly_planning_area_demand` table is an hourly time
    series of demand by Planning Area.

    Most of the methods in this class as staticmethods. The purpose of using a class
    in this instance is mostly for organizing the table specific transforms under the
    same name-space.
    """

    @classmethod
    def run(
        cls,
        raw_csv: pd.DataFrame,
        raw_xbrl_duration: pd.DataFrame,
        raw_xbrl_instant: pd.DataFrame,
    ) -> pd.DataFrame:
        """Build the :ref:`core_ferc714__hourly_planning_area_demand` asset.

        To transform this table we have to process the instant and duration xbrl
        tables so we can merge them together and process the XBRL data. We also
        have to process the CSV data so we can concatenate it with the XBLR data.
        Then we can process all of the data together.

        For both the CSV and XBRL data, the main transforms that are happening
        have to do with cleaning the timestamps in the data, resulting in
        timestamps that are in a datetime format and are nearly continuous
        for every respondent.

        Once the CSV and XBRL data is merged together, the transforms are mostly
        focused on cleaning the timezone codes reported to FERC
        and then using those timezone codes to convert all of timestamps into
        UTC datetime.

        The outcome here is nearly continuous and non-duplicative time series.
        """
        table_name = "core_ferc714__hourly_planning_area_demand"
        # XBRL STUFF
        duration_xbrl = _filter_for_freshest_data_xbrl(
            raw_xbrl_duration, table_name, "duration"
        ).pipe(cls.remove_yearly_records_duration_xbrl)
        instant_xbrl = _filter_for_freshest_data_xbrl(
            raw_xbrl_instant, table_name, "instant"
        )
        xbrl = (
            cls.merge_instant_and_duration_tables_xbrl(
                instant_xbrl, duration_xbrl, table_name=table_name
            )
            .pipe(
                rename_columns,
                params=RenameColumns(columns=RENAME_COLS[table_name]["xbrl"]),
            )
            .pipe(_assign_respondent_id_ferc714, "xbrl")
            .pipe(cls.convert_dates_to_zero_offset_hours_xbrl)
            .astype({"report_date": "datetime64[ns]"})
            .pipe(cls.convert_dates_to_zero_seconds_xbrl)
            .pipe(cls.spot_fix_records_xbrl)
            .pipe(cls.ensure_dates_are_continuous, source="xbrl")
        )
        # CSV STUFF
        csv = (
            _pre_process_csv(raw_csv, table_name=table_name)
            .pipe(_assign_respondent_id_ferc714, "csv")
            .pipe(cls.melt_hourx_columns_csv)
            .pipe(cls.parse_date_strings_csv)
            .pipe(cls.ensure_dates_are_continuous, source="csv")
        )
        # CONCATED STUFF
        df = (
            pd.concat([csv, xbrl])
            .reset_index(drop=True)
            .assign(
                utc_offset_code=lambda x: cls.standardize_offset_codes(
                    x, TIMEZONE_OFFSET_CODE_FIXES
                )
            )
            .pipe(cls.clean_utc_code_offsets_and_set_timezone)
            .pipe(cls.drop_missing_utc_offset)
            .pipe(cls.construct_utc_datetime)
            .pipe(cls.ensure_non_duplicated_datetimes)
            .pipe(cls.spot_fix_values)
            # Convert report_date to first day of year
            .assign(
                report_date=lambda x: x.report_date.dt.to_period("Y").dt.to_timestamp()
            )
            .pipe(_fillna_respondent_id_ferc714_source, "xbrl")
            .pipe(_fillna_respondent_id_ferc714_source, "csv")
            # sort so that the parquet files have all the repeating IDs are next
            # to each other for smoller storage
            .sort_values(by=["respondent_id_ferc714", "datetime_utc"])
        )
        return df

    @staticmethod
    def melt_hourx_columns_csv(df):
        """Melt hourX columns into hours.

        There are some instances of the CSVs with a 25th hour. We drop
        those entirely because almost all of them are unusable (0.0 or
        daily totals), and they shouldn't really exist at all based on
        FERC instructions.
        """
        df = df.drop(columns="hour25")

        # Melt daily rows with 24 demands to hourly rows with single demand
        logger.info("Melting daily FERC 714 records into hourly records.")
        df = df.rename(
            columns=lambda x: int(re.sub(r"^hour", "", x)) - 1 if "hour" in x else x,
        )
        df = df.melt(
            id_vars=[
                "respondent_id_ferc714",
                "respondent_id_ferc714_csv",
                "report_year",
                "report_date",
                "utc_offset_code",
            ],
            value_vars=range(24),
            var_name="hour",
            value_name="demand_mwh",
        )
        return df

    @staticmethod
    def parse_date_strings_csv(csv):
        """Convert report_date into pandas Datetime types.

        Make the report_date column from the daily string ``report_date`` and
        the integer ``hour`` column.
        """
        # Parse date strings
        hour_timedeltas = {i: pd.to_timedelta(i, unit="h") for i in range(24)}
        # NOTE: Faster to ignore trailing 00:00:00 and use exact=False
        csv["report_date"] = pd.to_datetime(
            csv["report_date"], format="%m/%d/%Y", exact=False
        ) + csv["hour"].map(hour_timedeltas)
        return csv.drop(columns=["hour"])

    @staticmethod
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

    @staticmethod
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

    @staticmethod
    def convert_dates_to_zero_offset_hours_xbrl(xbrl: pd.DataFrame) -> pd.DataFrame:
        """Convert all hours to: Hour (24-hour clock) as a zero-padded decimal number.

        The FERC 714 form includes columns for the hours of each day. Those columns are
        labeled with 1-24 to indicate the hours of the day. The XBRL filings themselves
        have time-like string associated with each of the facts. They include both a the
        year-month-day portion (formatted as %Y-%m-%d) as well as an hour-minute-second
        component (semi-formatted as T%H:%M:%S). Attempting to simply convert this
        timestamp information to a datetime using the format ``"%Y-%m-%dT%H:%M:%S"``
        fails because about a third of the records include hour 24 - which is not an
        accepted hour in standard datetime formats.

        The respondents that report hour 24 do not report hour 00. We have done some spot
        checking of values reported to FERC and have determined that hour 24 seems to
        correspond with hour 00 (of the next day). We have not gotten complete
        confirmation from FERC staff that this is always the case, but it seems like a
        decent assumption.

        So, this step converts all of the hour 24 records to be hour 00 of the next day.
        """
        bad_24_hour_mask = xbrl.report_date.str.contains("T24:")

        xbrl.loc[bad_24_hour_mask, "report_date"] = pd.to_datetime(
            xbrl[bad_24_hour_mask].report_date.str.replace("T24:", "T23:"),
            format="%Y-%m-%dT%H:%M:%S",
        ) + np.timedelta64(1, "h")
        return xbrl

    @staticmethod
    def convert_dates_to_zero_seconds_xbrl(xbrl: pd.DataFrame) -> pd.DataFrame:
        """Convert the last second of the day records to the first (0) second of the next day.

        There are a small amount of records which report the last "hour" of the day
        as last second of the day, as opposed to T24 cleaned in
        :meth:`convert_dates_to_zero_offset_hours_xbrl` or T00 which is standard for a
        datetime. This function finds these records and adds one second to them and
        then ensures all of the records has 0's for seconds.
        """
        last_second_mask = xbrl.report_date.dt.second == 59

        xbrl.loc[last_second_mask, "report_date"] = xbrl.loc[
            last_second_mask, "report_date"
        ] + pd.Timedelta("1s")
        assert xbrl[xbrl.report_date.dt.second != 0].empty
        return xbrl

    @staticmethod
    def spot_fix_records_xbrl(xbrl: pd.DataFrame):
        """Spot fix some specific XBRL records."""
        xbrl_years_mask = xbrl.report_date.dt.year >= min(Ferc714Settings().xbrl_years)
        if (len_csv_years := len(xbrl[~xbrl_years_mask])) > 25:
            raise AssertionError(
                "We expected less than 25 XBRL records that have timestamps "
                f"with years before the XBRL years, but we found {len_csv_years}"
            )
        return xbrl[xbrl_years_mask]

    @staticmethod
    def ensure_dates_are_continuous(df: pd.DataFrame, source: Literal["csv", "xbrl"]):
        """Assert that almost all respondents have continuous timestamps.

        In the xbrl data, we found 41 gaps in the timeseries! They are almost entirely
        on the hour in which daylight savings times goes into effect. The csv data
        had 10 gaps. Pretty good all in all!
        """
        df["gap"] = df[["respondent_id_ferc714", "report_date"]].sort_values(
            by=["respondent_id_ferc714", "report_date"]
        ).groupby("respondent_id_ferc714").diff() > pd.to_timedelta("1h")
        max_gaps = 41 if source == "xbrl" else 10
        if len(gappy_dates := df[df.gap]) > max_gaps:
            raise AssertionError(
                f"We expect there to be fewer than {max_gaps} gaps in the {source} time "
                f"series but we found these {len(gappy_dates)} gaps:\n{gappy_dates}"
            )
        return df.drop(columns=["gap"])

    @staticmethod
    def standardize_offset_codes(df: pd.DataFrame, offset_fixes) -> pd.Series:
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
        # Clean UTC offset codes
        df["utc_offset_code"] = df["utc_offset_code"].str.strip().str.upper()
        # We only need a couple of columns here:
        codes = df[["respondent_id_ferc714", "utc_offset_code"]].copy()
        # Set all blank "" missing UTC codes to np.nan
        codes["utc_offset_code"] = codes.utc_offset_code.mask(
            codes.utc_offset_code == ""
        )
        # Apply specific fixes on a per-respondent basis:
        codes = codes.groupby("respondent_id_ferc714").transform(
            lambda x: x.replace(offset_fixes[x.name]) if x.name in offset_fixes else x
        )
        return codes

    @staticmethod
    def clean_utc_code_offsets_and_set_timezone(df):
        """Clean UTC Codes and set timezone."""
        # NOTE: Assumes constant timezone for entire year
        for fix in TIMEZONE_OFFSET_CODE_FIXES_BY_YEAR:
            mask = (df["report_year"] == fix["report_year"]) & (
                df["respondent_id_ferc714"] == fix["respondent_id_ferc714"]
            )
            df.loc[mask, "utc_offset_code"] = fix["utc_offset_code"]

        # Replace UTC offset codes with UTC offset and timezone
        df["utc_offset"] = df["utc_offset_code"].map(TIMEZONE_OFFSET_CODES)
        df["timezone"] = df["utc_offset_code"].map(TIMEZONE_CODES)
        return df

    @staticmethod
    def drop_missing_utc_offset(df):
        """Drop records with missing UTC offsets and zero demand."""
        # Assert that all records missing UTC offset have zero demand
        missing_offset = df["utc_offset"].isna()
        bad_offset_and_demand = df.loc[missing_offset & (df.demand_mwh != 0)]
        # there are 12 of these bad guys just in the 2023 fast test.
        if len(bad_offset_and_demand) > 12:
            raise AssertionError(
                "We expect all but 12 of the records without a cleaned "
                "utc_offset to not have any demand data, but we found "
                f"{len(bad_offset_and_demand)} records.\nUncleaned Codes: "
                f"{bad_offset_and_demand.utc_offset_code.unique()}\n{bad_offset_and_demand}"
            )
        # Drop these records & then drop the original offset code
        df = df.query("~@missing_offset").drop(columns="utc_offset_code")
        return df

    @staticmethod
    def construct_utc_datetime(df: pd.DataFrame) -> pd.DataFrame:
        """Construct datetime_utc column."""
        # Construct UTC datetime
        logger.info("Converting local time + offset code to UTC + timezone.")
        df["datetime_utc"] = df["report_date"] - df["utc_offset"]
        df = df.drop(columns=["utc_offset"])
        return df

    @staticmethod
    def ensure_non_duplicated_datetimes(df):
        """Report and drop duplicated UTC datetimes."""
        # There should be less than 10 of these,
        # resulting from changes to a planning area's reporting timezone.
        duplicated = df.duplicated(["respondent_id_ferc714", "datetime_utc"])
        if (num_dupes := np.count_nonzero(duplicated)) > 10:
            raise AssertionError(
                f"Found {num_dupes} duplicate UTC datetimes, but we expected 10 or less.\n{df[duplicated]}"
            )
        df = df.query("~@duplicated")
        return df

    @staticmethod
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


@asset(
    ins={
        "raw_csv": AssetIn(key="raw_ferc714_csv__hourly_planning_area_demand"),
        "raw_xbrl_duration": AssetIn(
            key="raw_ferc714_xbrl__planning_area_hourly_demand_and_forecast_summer_and_winter_peak_demand_and_annual_net_energy_for_load_03_2_duration"
        ),
        "raw_xbrl_instant": AssetIn(
            key="raw_ferc714_xbrl__planning_area_hourly_demand_and_forecast_summer_and_winter_peak_demand_and_annual_net_energy_for_load_03_2_instant"
        ),
    },
    io_manager_key="parquet_io_manager",
    op_tags={"memory-use": "high"},
    compute_kind="pandas",
)
def core_ferc714__hourly_planning_area_demand(
    raw_csv: pd.DataFrame,
    raw_xbrl_duration: pd.DataFrame,
    raw_xbrl_instant: pd.DataFrame,
) -> pd.DataFrame:
    """Build the :ref:`core_ferc714__hourly_planning_area_demand`.

    This is a light wrapper around :class:`HourlyPlanningAreaDemand` because
    it seems you need to build an asset from a function - not a staticmethod of
    a class.
    """
    return HourlyPlanningAreaDemand.run(raw_csv, raw_xbrl_duration, raw_xbrl_instant)


class YearlyPlanningAreaDemandForecast:
    """Class for building the :ref:`core_ferc714__yearly_planning_area_demand_forecast` asset.

    The :ref:`core_ferc714__yearly_planning_area_demand_forecast` table is an annual, forecasted
    time series of demand by Planning Area.

    Most of the methods in this class as staticmethods. The purpose of using a class
    in this instance is mostly for organizing the table specific transforms under the
    same name-space.
    """

    @classmethod
    def run(
        cls,
        raw_csv: pd.DataFrame,
        raw_xbrl_duration: pd.DataFrame,
    ) -> pd.DataFrame:
        """Build the :ref:`core_ferc714__yearly_planning_area_demand_forecast` asset.

        To transform this table we have to process the CSV data and the XBRL duration data
        (this data has not instant table), merge together the XBRL and CSV data, and
        process the combined datasets.

        The main transforms include spot-fixing forecast years with
        :meth:`spot_fix_forecast_years_xbrl` and averaging out duplicate forecast values
        for duplicate primary key rows in the CSV table.

        """
        table_name = "core_ferc714__yearly_planning_area_demand_forecast"
        # XBRL STUFF
        xbrl = (
            _filter_for_freshest_data_xbrl(raw_xbrl_duration, table_name, "duration")
            .pipe(
                rename_columns,
                params=RenameColumns(columns=RENAME_COLS[table_name]["xbrl"]),
            )
            .pipe(_assign_respondent_id_ferc714, "xbrl")
            .pipe(cls.spot_fix_forecast_years_xbrl)
        )
        # CSV STUFF
        csv = (
            _pre_process_csv(raw_csv, table_name=table_name)
            .pipe(_assign_respondent_id_ferc714, "csv")
            .pipe(cls.average_duplicate_pks_csv)
        )
        # CONCATED STUFF
        df = pd.concat([csv, xbrl]).reset_index(drop=True)
        return df

    @staticmethod
    def spot_fix_forecast_years_xbrl(df):
        """Spot fix forecast year errors.

        This function fixes the following errors:

        - There's one record with an NA forecast_year value. This row
          also has no demand forecast values. Because forecast_year is a primary key
          we can't have any NA values. Because there are no substantive forecasts
          in this row, we can safely remove this row.
        - respondent_id_ferc714 number 107 reported their forecast_year
          as YY instead of YYYY values.
        - There's also at least one forecast year value reported as 3033 that should
          be 2033.

        This function also checks that the values for forecast year are within an
        expected range.
        """
        df = df.astype({"forecast_year": "Int64"})
        # Make sure there's only one NA forecast_year value and remove it
        if len(nulls := df[df["forecast_year"].isna()]) > 2:
            raise AssertionError(
                f"We expected one or 0 NA forecast year, but found:\n{nulls}"
            )
        df = df[df["forecast_year"].notna()]
        # Convert YY to YYYY for respondent 107 (the culprit).
        # The earliest forecast year reported as YY is 22. Any numbers
        # lower than that would signify a transition into 2100.
        mask = (df["respondent_id_ferc714"] == 107) & (df["forecast_year"] > 21)
        df.loc[mask, "forecast_year"] = df["forecast_year"] + 2000
        # Fix extraneus 3022 value from respondent 17
        mask = (
            (df["respondent_id_ferc714"] == 17)
            & (df["report_year"] == 2023)
            & (df["forecast_year"] == 3033)
        )
        df.loc[mask, "forecast_year"] = 2033
        # Make sure forecast_year values are expected
        assert df["forecast_year"].isin(range(2021, 2100)).all(), (
            "Forecast year values not in expected range"
        )
        return df

    @staticmethod
    def average_duplicate_pks_csv(df):
        """Average forecast values for duplicate primary keys.

        The XBRL data had duplicate primary keys, but it was easy to parse
        them by keeping rows with the most recent publication_time value.
        The CSVs have no such distinguishing column, despite having some
        duplicate primary keys.

        This function takes the average of the forecast values for rows
        with duplicate primary keys. There are only 6 respondent/report_year/
        forecast year rows where the forecast values differ. One of those is a
        pair where one forecast value is 0. We'll take the non-zero value here
        and average out the rest.
        """
        # Record original length of dataframe
        original_len = len(df)
        # Remove duplicate row with 0 forecast values
        error_mask = (
            (df["respondent_id_ferc714"] == 100)
            & (df["report_year"] == 2013)
            & (df["forecast_year"] == 2014)
            & (df["net_demand_forecast_mwh"] == 0)
        )
        if (len_dupes := len(df[error_mask])) > 1:
            raise AssertionError(
                f"We found {len_dupes} duplicate errors, but expected 1 or less:\n{df[error_mask]}"
            )
        df = df[~error_mask]
        # Take the average of duplicate PK forecast values.
        dupe_mask = df[
            ["respondent_id_ferc714", "report_year", "forecast_year"]
        ].duplicated(keep=False)
        deduped_df = (
            df[dupe_mask]
            .groupby(["respondent_id_ferc714", "report_year", "forecast_year"])[
                [
                    "summer_peak_demand_forecast_mw",
                    "winter_peak_demand_forecast_mw",
                    "net_demand_forecast_mwh",
                ]
            ]
            .mean()
            .reset_index()
        )
        df = pd.concat([df[~dupe_mask], deduped_df])
        # Make sure no more rows were dropped than expected
        assert original_len - len(df) <= 20, (
            f"dropped {original_len - len(df)} rows, expected 20"
        )
        return df


@asset(
    ins={
        "raw_csv": AssetIn(key="raw_ferc714_csv__yearly_planning_area_demand_forecast"),
        "raw_xbrl_duration": AssetIn(
            key="raw_ferc714_xbrl__planning_area_hourly_demand_and_forecast_summer_and_winter_peak_demand_and_annual_net_energy_for_load_table_03_2_duration"
        ),
    },
    io_manager_key="pudl_io_manager",
    compute_kind="pandas",
)
def core_ferc714__yearly_planning_area_demand_forecast(
    raw_csv: pd.DataFrame,
    raw_xbrl_duration: pd.DataFrame,
) -> pd.DataFrame:
    """Build the :ref:`core_ferc714__yearly_planning_area_demand_forecast`.

    This is a light wrapper around :class:`YearlyPlanningAreaDemandForecast` because
    it seems you need to build an asset from a function - not a staticmethod of
    a class.
    """
    return YearlyPlanningAreaDemandForecast.run(raw_csv, raw_xbrl_duration)
