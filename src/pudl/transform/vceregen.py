"""Transformations of the Vibrant Clean Energy renewable generation profiles.

Wind and solar profiles are extracted separately, but concatenated into a single table
in this module, as they have exactly the same structure.
"""

import pandas as pd
from dagster import AssetCheckResult, asset, asset_check

import pudl
from pudl.helpers import cleanstrings_snake, simplify_columns, zero_pad_numeric_string
from pudl.metadata.dfs import POLITICAL_SUBDIVISIONS

logger = pudl.logging_helpers.get_logger(__name__)


def _prep_lat_long_fips_df(raw_vcegen__lat_lon_fips: pd.DataFrame) -> pd.DataFrame:
    """Prep the lat_long_fips table to merge into the capacity factor tables.

    Prep entails making sure the formatting and column names match those in the
    capacity factor tables, adding 0s to the beginning of FIPS codes with 4 values,
    and making separate county/subregion and state columns.

    The county portion of the county_state column does not map directly to FIPS ID.
    Some of the county names are actually subregions like cities or lakes. For this
    reason we've named the column county_or_lake_name and it should be considered
    part of the primary key. There are several instances of multiple subregions that
    map to a single county_id_fips value.
    """
    logger.info(
        "Preping Lat-Long-FIPS table for merging with the capacity factor tables"
    )
    ps_usa_df = POLITICAL_SUBDIVISIONS[POLITICAL_SUBDIVISIONS["country_code"] == "USA"]
    state_names = cleanstrings_snake(
        ps_usa_df, ["subdivision_name"]
    ).subdivision_name.tolist()
    state_pattern = "|".join(state_names)
    # Transform lat long fips table by making the county_state_names lowercase
    # to match the values in the capacity factor tables. Fix FIPS codes with
    # no leading zeros, and add a county_or_lake_name and state field.
    lat_long_fips = (
        raw_vcegen__lat_lon_fips.pipe(simplify_columns)
        .assign(
            county_state_names=lambda x: x.county_state_names.str.lower().replace(
                {r"\.": "", "-": "_"}, regex=True
            )
        )
        .assign(county_id_fips=lambda x: zero_pad_numeric_string(x.fips, 5))
        .assign(state_id_fips=lambda x: x.county_id_fips.str.extract(r"(\d{2})"))
        .assign(
            county_or_lake_name=lambda x: x.county_state_names.str.extract(
                rf"(.+)_({state_pattern})$"
            )[0]
        )
        .merge(
            ps_usa_df[["state_id_fips", "subdivision_code"]],
            on=["state_id_fips"],
            how="left",
        )
        .rename(
            columns={
                "lat_county": "latitude",
                "long_county": "longitude",
                "subdivision_code": "state",
            }
        )
        .drop(columns=["state_id_fips", "fips"])
    )
    return lat_long_fips


def _add_time_cols(df: pd.DataFrame, df_name: str) -> pd.DataFrame:
    """Add datetime and hour_of_year columns.

    This function adds a datetime column and a hour_of_year column.
    The datetime column is important for merging the data with other
    data, and the hour_of_year 1-8760 column is important for modeling
    purposes. The report_year column is also helpful for filtering,
    so we keep all three!

    For leap years (2020), December 31st is excluded.
    """
    logger.info(f"Adding time columns for {df_name} table")
    # This data is compiled for modeling purposes and skips the last
    # day of a leap year. When adding a datetime column, we need
    # to make sure that we skip the 31st of December, 2020 and that
    # every year has exactly 8760 hours in it.
    all_years = df.report_year.unique()
    datetime8760_index = pd.DatetimeIndex(
        pd.concat(
            [
                pd.Series(pd.date_range(start=f"{year}-01-01", periods=8760, freq="h"))
                for year in all_years
            ]
        )
    )
    new_time_col = pd.DataFrame(
        {
            "datetime_utc": datetime8760_index,
        }
    )
    df = pd.concat(
        [df.reset_index(drop=True), new_time_col.reset_index(drop=True)], axis="columns"
    ).rename(columns={"unnamed_0": "hour_of_year"})
    return df


def _stack_cap_fac_df(df: pd.DataFrame, df_name: str) -> pd.DataFrame:
    """Funciton to transform each capacity factor table individually to save memory.

    The main transforms are turning count/subregion columns into county/subregion rows
    and renaming columns to be more human-readable and compatible with the FIPs df
    that will get merged in.

    This function is intended to save memory by being applied to each individual
    capacity factor table rather than the giant combined one.
    """
    logger.info(f"Stacking the county/subregion columns for {df_name} table.")
    df_stacked = (
        df.set_index(["datetime_utc", "hour_of_year", "report_year"])
        .stack()
        .reset_index()
        .rename(
            columns={"level_3": "county_state_names", 0: f"capacity_factor_{df_name}"}
        )
        .assign(county_state_names=lambda x: pd.Categorical(x.county_state_names))
    )
    return df_stacked


def _make_cap_fac_frac(df: pd.DataFrame, df_name: str) -> pd.DataFrame:
    """Make the capacity factor column a fraction instead of a percentage.

    This step happens before the table gets stacked to save memory.
    """
    logger.info(f"Converting capacity factor into a fraction for {df_name} table.")
    county_cols = [x for x in df.columns if x not in ["report_year", "unnamed_0"]]
    df[county_cols] = df[county_cols] / 100
    return df


def _check_for_valid_counties(
    df: pd.DataFrame, clean_fips_df: pd.DataFrame, df_name: str
) -> pd.DataFrame:
    """Make sure the state_county values show up in the fips table.

    This step happens before the table gets stacked to save memory.
    """
    logger.info(f"Checking for valid counties in the {df_name} table.")
    county_state_names_fips = clean_fips_df.county_state_names.unique().tolist()
    county_state_names_cap_fac = df.columns.tolist()
    expected_non_counties = ["report_year", "unnamed_0"]
    non_county_cols = [
        x
        for x in county_state_names_fips
        if x not in county_state_names_cap_fac + expected_non_counties
    ]
    if non_county_cols:
        raise AssertionError(f"""found unexpected columns that aren't in the FIPS table:
{non_county_cols}.""")
    return df


def _combine_all_cap_fac_dfs(cap_fac_dict: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Combine capacity factor tables."""
    logger.info("Merging all the capacity factor tables into one")
    merge_keys = ["report_year", "datetime_utc", "hour_of_year", "county_state_names"]
    for name, df in cap_fac_dict.items():
        cap_fac_dict[name] = df.set_index(merge_keys)
    mega_df = pd.concat(
        [
            cap_fac_dict["solar_pv"],
            cap_fac_dict["offshore_wind"],
            cap_fac_dict["onshore_wind"],
        ],
        axis="columns",
    ).reset_index()

    return mega_df


def _combine_cap_fac_with_fips_df(
    cap_fac_df: pd.DataFrame, fips_df: pd.DataFrame
) -> pd.DataFrame:
    """Combine the combined capacity factor df with the fips df."""
    logger.info(
        "Merging the combined capacity factor table with the Lat-Long-FIPS table"
    )
    combined_df = pd.merge(cap_fac_df, fips_df, on="county_state_names", how="left")
    return combined_df


def _combine_city_county_records(df: pd.DataFrame) -> pd.DataFrame:
    """Average cap fac values for city and county values with the same FIPS ID.

    There are several duplicate FIPS code values in the data. Most of them
    are lakes within a county, but some of them are cities. Only one of the
    cities, clifton forge city, has values, so we average those values
    with that of the county. The other city, XXX, has no values so we
    remove it.

    """
    bad_county_mask = df["county_state_names"].isin(
        ["alleghany_virginia", "clifton_forge_city_virginia"]
    )
    # Take the subset of the data that's relevant to the changes and sort the values
    # by county_state so that alleghany is first and those are the values that are
    # preserved by the groupby "first" below.
    subset_df = df[bad_county_mask].sort_values(by=["county_state_names"])
    cap_fac_cols = [x for x in subset_df.columns if "capacity_factor" in x]
    other_cols = [
        x for x in subset_df.columns if x not in cap_fac_cols + ["datetime_utc"]
    ]
    grouped_df = (
        subset_df.groupby(["datetime_utc"], sort=False)
        .agg(
            {
                **{col: "mean" for col in cap_fac_cols},
                **{col: "first" for col in other_cols},
            }
        )
        .reset_index()
    )
    out_df = pd.concat([df[~bad_county_mask], grouped_df])
    # Now lets remove the records for the other city with no values
    bad_county_mask_2 = out_df["county_state_names"] == "bedford_city_virginia"
    # Make sure there aren't any non-zero values
    if not (out_df[bad_county_mask_2][cap_fac_cols] == 0).all().all():
        raise AssertionError("Found non-zero values for bedford_city_virginia record.")
    return out_df[~bad_county_mask_2]


@asset(
    io_manager_key="parquet_io_manager",
    compute_kind="pandas",
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
    the columns for each county or subregion into a single county_or_lake_name column.
    """
    logger.info("Transforming the hourly available capacity factor tables")
    # Clean up the FIPS table
    fips_df = _prep_lat_long_fips_df(raw_vcegen__lat_lon_fips)
    # Apply the same transforms to all the capacity factor tables. This is slower
    # than doing it to a concatinated table but less memory intensive because
    # it doesn't need to process the ginormous table all at once.
    raw_dict = {
        "solar_pv": raw_vceregen__fixed_solar_pv_lat_upv,
        "offshore_wind": raw_vceregen__offshore_wind_power_140m,
        "onshore_wind": raw_vceregen__onshore_wind_power_100m,
    }
    clean_dict = {
        df_name: _check_for_valid_counties(df, fips_df, df_name)
        .pipe(_make_cap_fac_frac, df_name)
        .pipe(_add_time_cols, df_name)
        .pipe(_stack_cap_fac_df, df_name)
        for df_name, df in raw_dict.items()
    }
    # Combine the data!
    return (
        _combine_all_cap_fac_dfs(clean_dict)
        .pipe(_combine_city_county_records)
        .pipe(_combine_cap_fac_with_fips_df, fips_df)
    )


@asset_check(
    asset=out_vceregen__hourly_available_capacity_factor,
    blocking=True,
    description="Check that output table is as expected.",
)
def check_hourly_available_cap_fac_table(asset_df: pd.DataFrame):
    """Check that the final output table is as expected."""
    # Make sure the table is the expected length
    if (length := len(asset_df)) != 136437000:
        return AssetCheckResult(
            passed=False,
            description="Table unexpected length",
            metadata={"table_length": length, "expected_length": 136437000},
        )
    # Make sure there are no null values
    if asset_df.isna().any().any():
        return AssetCheckResult(
            passed=False, description="Found NA values when there should be none"
        )
    # Make sure the capacity_factor values are below the expected value
    # For right now there are some solar values that are slightly over 1
    # I'm not sure why...
    if (asset_df.iloc[:, asset_df.columns.str.contains("cap")] > 1.02).all().all():
        return AssetCheckResult(
            passed=False,
            description="Found capacity factor fraction values greater than 1.02",
        )
    # Make sure capacity_factor values are greater than or equal to 0
    if (asset_df.iloc[:, asset_df.columns.str.contains("cap")] < 0).all().all():
        return AssetCheckResult(
            passed=False,
            description="Found capacity factor fraction values less than 0",
        )
    # Make sure the highest hour_of_year values is 8760
    if asset_df["hour_of_year"].max() != 8760:
        return AssetCheckResult(
            passed=False, description="Found hour_of_year values larger than 8760"
        )
    # Make sure Dec 31/2029 is missing
    if not asset_df[asset_df["datetime_utc"] == pd.to_datetime("2020-12-31")].empty:
        return AssetCheckResult(
            passed=False,
            description="Found rows for December 31, 2020 which should not exist",
        )
    # Make sure the datetime aligns with the hour of the year
    asset_df["hour_from_date"] = (
        asset_df["datetime_utc"].dt.hour
        + (asset_df["datetime_utc"].dt.dayofyear - 1) * 24
        + 1
    )
    if not asset_df[asset_df["hour_from_date"] != asset_df["hour_of_year"]].empty:
        return AssetCheckResult(
            passed=False,
            description="hour_of_year values don't match date values",
        )
    return AssetCheckResult(passed=True)
