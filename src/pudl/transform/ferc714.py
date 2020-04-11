"""Transformation of the FERC Form 714 data."""

import pathlib
import re
import zipfile

import geopandas
import numpy as np
import pandas as pd

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
        "respondent_id": 235,
        "tz_col": "timezone",
        "start_time": pd.Timestamp("2015-01-01T00:00:00"),
        "end_time": pd.Timestamp("2015-12-31T23:00:00"),
        "std_val": "MST",
        "dst_val": "MDT",
    },
    {
        "respondent_id": 289,
        "tz_col": "timezone",
        "start_time": pd.Timestamp("2011-01-01T00:00:00"),
        "end_time": pd.Timestamp("2011-12-31T23:00:00"),
        "std_val": "CST",
        "dst_val": "CDT",
    },
    {
        "respondent_id": 292,
        "tz_col": "timezone",
        "start_time": pd.Timestamp("2011-01-01T00:00:00"),
        "end_time": pd.Timestamp("2011-12-31T23:00:00"),
        "std_val": "CST",
        "dst_val": "CDT",
    }
]

# Once all of the timezone abbreviations have been set
# Transform them into actual offsets from UTC:
tz_to_offset = {
    "HST": -10,  # Hawaii Standard Time
    "AKST": -9,  # Alaska Standard Time
    "AKDT": -8,  # Alaska Daylight Time
    "PST": -8,  # Pacific Standard Time
    "PDT": -7,  # Pacific Daylight Time
    "MST": -7,  # Mountain Standard Time
    "MDT": -6,  # Mountain Daylight Time
    "CST": -6,  # Central Standard Time
    "CDT": -5,  # Central Daylight Time
    "EST": -5,  # Eastern Standard Time
    "EDT": -4,  # Eastern Daylight Time
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


def _fix_tz_ferc714(df, tz_map, tz_fixes):
    """Convert to standardized timezone abbreviations."""
    df = df.copy()
    df["timezone"] = df["timezone"].replace(to_replace=tz_map)
    # More specific fixes on a per-respondent basis:
    for rid in tz_fixes:
        for orig_tz in tz_fixes[rid]:
            df.loc[((df.respondent_id == rid) & (df["timezone"] ==
                                                 orig_tz)), "timezone"] = tz_fixes[rid][orig_tz]

    # The City of Tacoma, WA is especially messy... Brute-force overwrite::
    df.loc[((df.respondent_id == 139) & (df.plan_date >= "2006-01-01")
            & (df.plan_date < "2006-04-02")), "timezone"] = "PST"
    df.loc[((df.respondent_id == 139) & (df.plan_date >= "2006-04-02")
            & (df.plan_date < "2006-10-29")), "timezone"] = "PDT"
    df.loc[((df.respondent_id == 139) & (df.plan_date >= "2006-10-29")
            & (df.plan_date <= "2006-12-31")), "timezone"] = "PST"
    # Only keep records where we have at least a real demand number or a real timezone:
    df = (
        df.assign(demand_mwh=lambda x: x.demand_mwh.replace(
            to_replace=np.nan, value=0.0))
        .query("timezone!='XXX' | demand_mwh!=0.0")
    )

    return df


def _fix_dst(df, spring_forward, fall_back):
    """Deal with daylight savings time changes."""
    fall_back_dates = pd.to_datetime(fall_back).date  # noqa: F841
    fixed_df = (
        # Get rid of all 25th hours not on fall-back days.
        df.query("hour!=25 | plan_date in @fall_back_dates")
        # Get rid of all 25th hours with zero demand
        .query("hour!=25 | demand_mwh>0.0")
    )
    # Remove any 25th hours that are daily demand totals:
    weird_days = (
        fixed_df
        .query("hour==25")
        .loc[:, ["respondent_id", "plan_date"]]
        .drop_duplicates()
        .merge(fixed_df, how="left", on=["respondent_id", "plan_date"])
    )
    bad_totals = (
        weird_days.groupby(["respondent_id", "plan_date"])[
            "demand_mwh"].sum().reset_index()
        .merge(weird_days.loc[weird_days.hour == 25, ["respondent_id", "plan_date", "demand_mwh"]],
               on=["respondent_id", "plan_date"])
        .rename(columns={"demand_mwh_x": "daily_mwh", "demand_mwh_y": "hour25_mwh"})
        # Daily totals and null values
        .query("hour25_mwh>=0.5*daily_mwh | hour25_mwh==0.0")
        .loc[:, ["respondent_id", "plan_date"]]
        .assign(hour=25)
    )
    idx_cols = ["respondent_id", "plan_date", "hour"]
    fixed_df = (
        fixed_df.set_index(idx_cols)
        .drop(bad_totals.set_index(idx_cols).index)
        .reset_index()
        .set_index(["respondent_id", "plan_date"])
    )

    # Shift all the integer *after* the time change back by an hour, bu
    # only do it for the days & respondents with real 25th hour data.
    # Pandas can parse the duplicate hours appropriately in UTC conversion
    hours_to_shift = (
        fixed_df.loc[fixed_df.query("hour==25").index]
        .query("hour>2")
    )
    fixed_df.loc[hours_to_shift.index,
                 "hour"] = fixed_df.loc[hours_to_shift.index, "hour"] - 1
    # Use the hour field to create a datetime field, switch to 0-23hr notation.
    fixed_df = fixed_df.reset_index()
    fixed_df["local_time"] = fixed_df.plan_date + \
        pd.to_timedelta(fixed_df.hour - 1, unit="h")

    # Find and replace the 0.0 values at 2-3am on the spring_forward days.
    spring_forward_dates = list(pd.to_datetime(spring_forward).date)  # noqa: F841
    demand_gaps = (
        fixed_df
        .query("plan_date in @spring_forward_dates")
        .query("hour in [2,3]")
        .query("demand_mwh==0.0")
    )
    fixed_df.loc[demand_gaps.index, "demand_mwh"] = np.nan
    # Now that we have a real datetime column, ditch "hours" and "plan_date"
    fixed_df = fixed_df.loc[:, ["respondent_id", "local_time",
                                "timezone", "demand_mwh"]]

    return fixed_df


def _fix_tz_by_date(df, time_changes, respondent_id,
                    start_time, end_time, tz_col, std_val, dst_val):
    """Assign timezone abbreviations based on date."""
    df = df.copy()
    years = df.loc[(df.respondent_id == respondent_id) &
                   (df.local_time >= start_time) &
                   (df.local_time <= end_time)].local_time.dt.year.unique()
    for yr in years:
        spring = time_changes.loc[yr, "spring_forward"]
        fall = time_changes.loc[yr, "fall_back"]
        std_times = (
            (df.respondent_id == respondent_id) &
            (df.local_time.dt.year == yr) &
            ((df.local_time < spring) | (df.local_time >= fall))
        )
        dst_times = (
            (df.respondent_id == respondent_id) &
            (df.local_time.dt.year == yr) &
            ((df.local_time >= spring) & (df.local_time < fall))
        )
        df.loc[std_times, tz_col] = std_val
        df.loc[dst_times, tz_col] = dst_val
    return df


def _apply_tz_fixes_by_date(df, fixes, time_changes):
    """A wrapper assin many timezone fixes at once."""
    for fix in fixes:
        df = _fix_tz_by_date(df, time_changes=time_changes, **fix)
    return df


def respondents_ferc714(pudl_settings):
    """Table of FERC 714 respondents IDs, names, and EIA utility IDs."""
    zippath = zipfile.Path(
        pathlib.Path(pudl_settings["data_dir"], "ferc714/data/ferc714.zip"),
        "Respondent IDs.csv"
    )
    with zippath.open() as zp:
        df = pd.read_csv(zp)
    df = (
        df.astype({
            "eia_code": pd.Int64Dtype(),
            "respondent_id": pd.Int64Dtype(),
            "respondent_name": pd.StringDtype(),
        })
        .assign(
            respondent_name=lambda x: x.respondent_name.str.strip(),
            eia_code=lambda x: x.eia_code.replace(to_replace=0, value=pd.NA)
        )
        # These excludes fake Test IDs -- not real planning areas
        .query("respondent_id not in @bad_respondents")
    )
    return df


def planning_area_hourly_demand_ferc714(pudl_settings):
    """Hourly demand time series by Planning Area."""
    zippath = zipfile.Path(
        pathlib.Path(pudl_settings["data_dir"], "ferc714/data/ferc714.zip"),
        "Part 3 Schedule 2 - Planning Area Hourly Demand.csv"
    )
    with zippath.open() as zp:
        df = pd.read_csv(zp)

    df = (
        df.filter(regex=r"^(?!.*_f$).*")
        .drop(["report_yr", "report_prd", "spplmnt_num", "row_num"], axis="columns")
        # Fake test respondents
        .query("respondent_id not in @bad_respondents")
        .assign(
            plan_date=lambda x: pd.to_datetime(x.plan_date),
            timezone=lambda x: x.timezone.str.upper().str.strip(),
        )
        .rename(columns=_hours_to_ints)
        .melt(
            id_vars=["respondent_id", "plan_date", "timezone"],
            var_name="hour",
            value_name="demand_mwh"
        )
        .pipe(_fix_tz_ferc714, tz_map=tz_nulls, tz_fixes=tz_fixes)
        .pipe(_fix_dst, spring_forward, fall_back)
        .pipe(_apply_tz_fixes_by_date, tz_fixes_by_date, time_changes)
        .merge(
            respondents_ferc714(pudl_settings),
            how="left",
            on="respondent_id"
        )
        .sort_values(["respondent_id", "local_time"])
    )
    return df


def electricity_planning_areas(pudl_settings):
    """Electric Planning Area geometries from HIFLD."""
    # Sadly geopandas / fiona can't read a zipfile.Path
    shapepath = pathlib.Path(
        pudl_settings["data_dir"],
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
        .merge(
            respondents_ferc714(pudl_settings),
            how="left",
            left_on="ID",
            right_on="eia_code"
        )
    )
    return gdf
