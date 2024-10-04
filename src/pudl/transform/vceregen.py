"""Transformations of the Vibrant Clean Energy renewable generation profiles.

Wind and solar profiles are extracted separately, but concatenated into a single table
in this module, as they have exactly the same structure.
"""

import pandas as pd
from dagster import asset

import pudl
from pudl.helpers import cleanstrings_snake, simplify_columns, zero_pad_numeric_string
from pudl.metadata.dfs import POLITICAL_SUBDIVISIONS

logger = pudl.logging_helpers.get_logger(__name__)


def _prep_lat_long_fips_df(raw_vcegen__lat_lon_fips: pd.DataFrame) -> pd.DataFrame:
    """Prep the lat_long_fips table to merge into the capacity factor tables.

    Prep entails making sure the formatting and column names match those in the
    capacity factor tables, adding 0s to the beginning of FIPS codes with 4 values,
    and making separate county and state columns.
    """
    logger.info(
        "Preping Lat-Long-FIPS table for merging with the capacity factor tables"
    )
    # Get state name patterns from the POLITICAL_SUBDIVISIONS table
    ps_usa_df = POLITICAL_SUBDIVISIONS[POLITICAL_SUBDIVISIONS["country_code"] == "USA"]
    state_names = cleanstrings_snake(
        ps_usa_df, ["subdivision_name"]
    ).subdivision_name.tolist()
    state_pattern = "|".join(state_names)
    # Transform lat long fips table by making the county_state_names lowercase
    # to match the values in the capacity factor tables. Fix FIPS codes with
    # no leading zeros, and add a unique country and state field.
    lat_long_fips = (
        raw_vcegen__lat_lon_fips.pipe(simplify_columns)
        .assign(county_state_names=lambda x: x.county_state_names.str.lower())
        .assign(fips=lambda x: zero_pad_numeric_string(x.fips, 5))
        .assign(
            county=lambda x: x.county_state_names.str.extract(
                rf"(.+)_({state_pattern})"
            )[0]
        )
        .assign(
            state=lambda x: x.county_state_names.str.extract(
                rf"(.+)_({state_pattern})"
            )[1]
        )
        .rename(
            columns={
                "lat_county": "latitude",
                "long_county": "longitude",
                "fips": "county_id_fips",
            }
        )
    )

    return lat_long_fips


def _add_time_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Add datetime and year_hour columns.

    This function adds a datetime column and a year_hour column.
    The datetime column is important for merging the data with other
    data, and the year_hour 0-8769 column is important for modeling
    purposes. The report_year column is also helpful for filtering,
    so we keep all three!

    For leap years (2020), December 31st is excluded.

    TO-DO: decide whether to have the year_hour start at 0 or 1.
    and update the date_range accordingly.
    """
    logger.info("Adding datetime columns")
    # This data is compiled for modeling purposes and skips the last
    # day of a leap year. If we want to add a datetime column, we need
    # to make sure that we skip the 31st of December, 2020.
    # the [:-1] is because datetime will include both the 0th hour of the
    # start date and the 0th hour of the end date. We only want one!
    # Though we still need to decide which. This at least makes the math
    # right.
    to_leap_year_dates = pd.date_range(start="2019-01-01", end="2020-12-31", freq="h")[
        :-1
    ]
    post_leap_year_dates = pd.date_range(
        start="2021-01-01", end="2024-01-01", freq="h"
    )[:-1]
    # Add date sequence to pre and post leap year dates
    df.loc[:, "report_year"] = df.report_year.astype("Int32")
    df.loc[df["report_year"] < 2021, "hour"] = to_leap_year_dates
    df.loc[df["report_year"] > 2020, "hour"] = post_leap_year_dates
    # Add a year_date column for modeling purposes.
    # Right now each year should go from 0-8759. We
    # can decide to make that 1-8760 if we want to!
    df.loc[:, "year_hour"] = df.hour.dt.hour + (df.hour.dt.dayofyear - 1) * 24
    return df


def _stack_cap_fac_df(df, df_name: pd.DataFrame) -> pd.DataFrame:
    """Funciton to transform each capacity factor table individually to save memory.

    The main transforms are turning county columns into county rows and renaming columns
    to be more human-readable and compatible with the FIPs df that will get merged in.

    This function is intended to save memory by being applied to each individual
    capacity factor table rather than the giant combined one.
    """
    logger.info("Stacking the county columns")
    df_stacked = (
        df.set_index(["hour", "year_hour", "report_year"])
        .stack()
        .reset_index()
        .rename(
            columns={"level_3": "county_state_names", 0: f"capacity_factor_{df_name}"}
        )
        .assign(county_state_names=lambda x: pd.Categorical(x.county_state_names))
    )
    return df_stacked


def combine_all_cap_fac_dfs(cap_fac_dict: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Combine capacity factor tables."""
    logger.info("Merging all the capacity factor tables into one")
    merge_keys = ["report_year", "hour", "year_hour", "county_state_names"]
    # Can't merge all at once using reduce because that's too much memory
    # So we'll merge one at a time.
    mega_df = pd.merge(
        cap_fac_dict["solar_pv"],
        cap_fac_dict["onshore_wind"],
        on=merge_keys,
        how="outer",
    ).merge(cap_fac_dict["offshore_wind"], on=merge_keys, how="outer")
    # Make sure the merge didn't introduce any weird length issues
    if (
        not len(cap_fac_dict["solar_pv"])
        == len(cap_fac_dict["offshore_wind"])
        == len(cap_fac_dict["onshore_wind"])
        == len(mega_df)
    ):
        raise AssertionError("Merge issue causing a length difference.")

    return mega_df


def combine_cap_fac_with_fips_df(
    cap_fac_df: pd.DataFrame, fips_df: pd.DataFrame
) -> pd.DataFrame:
    """Combine the combined capacity factor df with the fips df."""
    logger.info(
        "Merging the combined capacity factor table with the Lat-Long-FIPS table"
    )
    combined_df = pd.merge(cap_fac_df, fips_df, on="county_state_names", how="left")
    if len(combined_df) != len(cap_fac_df):
        raise AssertionError("Merge erroniously altered table length.")
    return combined_df


@asset(
    io_manager_key="parquet_io_manager",
    compute_kind="Python",
    op_tags={"memory-use": "high"},
)
def out_vceregen__hourly_available_capacity_factor(
    raw_vcegen__lat_lon_fips: pd.DataFrame,
    raw_vceregen__fixed_solar_pv_lat_upv: pd.DataFrame,
    raw_vceregen__offshore_wind_power_140m: pd.DataFrame,
    raw_vceregen__onshore_wind_power_100m: pd.DataFrame,
) -> pd.DataFrame:
    """Transform raw Vibrant Clean Energy renewable generation profiles.

    Concatenates the solar and wind capacity factors into a single table and turns
    the columns for each county into a single county column.

    Note that this transform is a bit unusual, in that it is producing a highly
    processed output table. That's because we're working backwards from an archived
    finished product to be able to provide a minimum viable product. Our intent is to
    integrate or reimplement the steps required to produce this output table from
    less processed original inputs in the future.
    """
    logger.info("Transforming the hourly available capacity factor tables")
    # Apply the same transforms to all the capacity factor tables. This is slower
    # than doing it to a concatinated table but less memory intensive because
    # it doesn't need to process the ginormous table all at once.
    df_dict = {
        "solar_pv": raw_vceregen__fixed_solar_pv_lat_upv,
        "offshore_wind": raw_vceregen__offshore_wind_power_140m,
        "onshore_wind": raw_vceregen__onshore_wind_power_100m,
    }
    for df_name, df in df_dict.items():
        logger.info(f"Prepping the {df_name} table")
        df_dict[df_name] = (
            # Adding time columns before stacking saves memory!
            _add_time_cols(df).pipe(_stack_cap_fac_df, df_name)
        )
    # Make sure there's no funny business
    if len(df_dict) != 3:
        raise AssertionError("Incorrect number of dataframes in the df_dict")
    # Clean up the FIPS table
    fips_df = _prep_lat_long_fips_df(raw_vcegen__lat_lon_fips)
    # Combine the data!
    out_df = combine_all_cap_fac_dfs(df_dict).pipe(
        combine_cap_fac_with_fips_df, fips_df
    )
    return out_df
