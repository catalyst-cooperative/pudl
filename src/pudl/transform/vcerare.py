"""Transformations of the Vibrant Clean Energy Resource Adequacy Renewable Energy (RARE) Power Dataset.

Wind and solar profiles are extracted separately, but concatenated into a single table
in this module, as they have exactly the same structure.
"""

import pandas as pd
from dagster import AssetCheckResult, asset, asset_check

import pudl
from pudl.helpers import cleanstrings_snake, simplify_columns, zero_pad_numeric_string
from pudl.metadata.dfs import POLITICAL_SUBDIVISIONS

logger = pudl.logging_helpers.get_logger(__name__)


def _prep_lat_long_fips_df(raw_vcerare__lat_lon_fips: pd.DataFrame) -> pd.DataFrame:
    """Prep the lat_long_fips table to merge into the capacity factor tables.

    Prep entails making sure the formatting and column names match those in the
    capacity factor tables, adding 0s to the beginning of FIPS codes with 4 values,
    and making separate county/subregion and state columns. Instead of pulling state
    from the county_state column, we use the first two digits of the county FIPS ID
    to pull in state code values from the census data stored in POLITICAL_SUBDIVISIONS.

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
    lat_long_fips = (
        # Making the county_state_names lowercase to match the values in the capacity factor tables
        raw_vcerare__lat_lon_fips.pipe(simplify_columns)
        .assign(
            county_state_names=lambda x: x.county_state_names.str.lower()
            .replace({r"\.": "", "-": "_"}, regex=True)
            .astype("category")
        )
        # Fix FIPS codes with no leading zeros
        .assign(
            county_id_fips=lambda x: zero_pad_numeric_string(x.fips, 5).astype(
                "category"
            )
        )
        # Add a state FIPS code so we can merge in the state code
        .assign(
            state_id_fips=lambda x: x.county_id_fips.str.extract(r"(\d{2})").astype(
                "category"
            )
        )
        # Extract the county or lake name from the county_state_name field
        .assign(
            county_or_lake_name=lambda x: x.county_state_names.str.extract(
                rf"([a-z_]+)_({state_pattern})$"
            )[0].astype("category")
        )
        # Add state column: e.g.: MA, RI, CA, TX
        .merge(
            ps_usa_df[["state_id_fips", "subdivision_code"]],
            on=["state_id_fips"],
            how="left",
            validate="m:1",
        )
        .rename(
            columns={
                "lat_county": "latitude",
                "long_county": "longitude",
            }
        )
        .assign(state=lambda x: x.subdivision_code.astype("category"))
        # Remove state FIPS code column in favor of the newly added state column.
        .drop(columns=["state_id_fips", "fips", "subdivision_code"])
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
    """Function to transform each capacity factor table individually to save memory.

    The main transforms are turning county/subregion columns into county/subregion rows
    and renaming columns to be more human-readable and compatible with the FIPS df
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
    county_cols = [
        x
        for x in df.columns
        if x not in ["report_year", "hour_of_year", "datetime_utc"]
    ]
    df[county_cols] = df[county_cols] / 100
    return df


def _check_for_valid_counties(
    df: pd.DataFrame, clean_fips_df: pd.DataFrame, df_name: str
) -> pd.DataFrame:
    """Make sure the state_county values show up in the FIPS table.

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
    """Combine the combined capacity factor df with the FIPS df."""
    logger.info(
        "Merging the combined capacity factor table with the Lat-Long-FIPS table"
    )
    combined_df = pd.merge(
        cap_fac_df, fips_df, on="county_state_names", how="left", validate="m:1"
    )
    return combined_df


def _drop_city_records(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows from cities.

    There are several duplicate FIPS code values in the data. Most of them
    are lakes within a county, but some of them are cities. Bedford City
    has no capacity factor values and Clifton Forge City has a few. We have
    no way to weight these cities by area to join them with the county
    values, so we drop them.

    """
    logger.info("Dropping city rows with duplicate county rows")
    return df.loc[
        ~df["county_state_names"].isin(
            ["clifton_forge_city_virginia", "bedford_city_virginia"]
        )
    ]


def _null_non_county_fips_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Null the county FIPS code for rows that aren't counties.

    There are a handful of rows that are not counties but rather
    cities or portions of lakes. We drop the city values in
    :func:`_drop_city_records`, but we keep the lake values.
    Because these are not within county geographies, we will
    not assign them FIPS codes.
    """
    logger.info("Nulling FIPS IDs for lake rows")
    lake_county_state_names = [
        "lake_erie_ohio",
        "lake_hurron_michigan",
        "lake_michigan_illinois",
        "lake_michigan_indiana",
        "lake_michigan_michigan",
        "lake_michigan_wisconsin",
        "lake_ontario_new_york",
        "lake_st_clair_michigan",
        "lake_superior_minnesota",
        "lake_superior_michigan",
        "lake_superior_wisconsin",
    ]
    # Null county_id_fips codes
    df.loc[df["county_state_names"].isin(lake_county_state_names), "county_id_fips"] = (
        pd.NA
    )
    return df


@asset(
    io_manager_key="parquet_io_manager",
    compute_kind="pandas",
    op_tags={"memory-use": "high"},
)
def out_vcerare__hourly_available_capacity_factor(
    raw_vcerare__lat_lon_fips: pd.DataFrame,
    raw_vcerare__fixed_solar_pv_lat_upv: pd.DataFrame,
    raw_vcerare__offshore_wind_power_140m: pd.DataFrame,
    raw_vcerare__onshore_wind_power_100m: pd.DataFrame,
) -> pd.DataFrame:
    """Transform raw Vibrant Clean Energy renewable generation profiles.

    Concatenates the solar and wind capacity factors into a single table and turns
    the columns for each county or subregion into a single county_or_lake_name column.
    """
    logger.info("Transforming the hourly available capacity factor tables")
    # Clean up the FIPS table
    fips_df = _prep_lat_long_fips_df(raw_vcerare__lat_lon_fips)
    # Apply the same transforms to all the capacity factor tables. This is slower
    # than doing it to a concatenated table but less memory intensive because
    # it doesn't need to process the ginormous table all at once.
    raw_dict = {
        "solar_pv": raw_vcerare__fixed_solar_pv_lat_upv,
        "offshore_wind": raw_vcerare__offshore_wind_power_140m,
        "onshore_wind": raw_vcerare__onshore_wind_power_100m,
    }
    clean_dict = {
        df_name: _check_for_valid_counties(df, fips_df, df_name)
        .pipe(_add_time_cols, df_name)
        .pipe(_make_cap_fac_frac, df_name)
        .pipe(_stack_cap_fac_df, df_name)
        for df_name, df in raw_dict.items()
    }
    # Combine the data and perform a few last cleaning mechanisms
    return (
        _combine_all_cap_fac_dfs(clean_dict)
        .pipe(_drop_city_records)
        .pipe(_combine_cap_fac_with_fips_df, fips_df)
        .pipe(_null_non_county_fips_rows)
    )


@asset_check(
    asset=out_vcerare__hourly_available_capacity_factor,
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
    # Make sure there are no null values (excluding FIPS code which should)
    if asset_df[asset_df.columns.difference(["county_id_fips"])].isna().any().any():
        return AssetCheckResult(
            passed=False,
            description="Found NA values when there should be none",
            metadata={
                "NA index values": asset_df[asset_df.isna().any(axis="columns")].index
            },
        )
    # Make sure the capacity_factor values are below the expected value
    # There are some solar values that are slightly over 1 due to colder
    # than average panel temperatures.
    if (asset_df.iloc[:, asset_df.columns.str.contains("cap")] > 1.02).any().any():
        return AssetCheckResult(
            passed=False,
            description="Found capacity factor fraction values greater than 1.02",
        )
    # Make sure capacity_factor values are greater than or equal to 0
    if (asset_df.iloc[:, asset_df.columns.str.contains("cap")] < 0).any().any():
        return AssetCheckResult(
            passed=False,
            description="Found capacity factor fraction values less than 0",
        )
    # Make sure the highest hour_of_year values is 8760
    if asset_df["hour_of_year"].max() != 8760:
        return AssetCheckResult(
            passed=False, description="Found hour_of_year values larger than 8760"
        )
    # Make sure Dec 31, 2020 is missing (leap year handling is correct)
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
    # Make sure there are no rows for Bedford City or Clifton Forge City
    if not asset_df[
        asset_df["county_or_lake_name"].isin(["bedford_city", "clifton_forge_city"])
    ].empty:
        return AssetCheckResult(
            passed=False,
            description="found records for bedford_city or clifton_forge_city that shouldn't exist",
        )
    # Make sure there are no duplicate county_id_fips values outside of NA
    notna_county_fips_df = asset_df[asset_df["county_id_fips"].notna()]
    if not notna_county_fips_df[
        notna_county_fips_df.duplicated(subset=["datetime_utc", "county_id_fips"])
    ].empty:
        return AssetCheckResult(
            passed=False,
            description="Found duplicate county_id_fips values",
            metadata={
                "duplciate county FIPS values": notna_county_fips_df[
                    notna_county_fips_df.duplicated(
                        subset=["datetime_utc", "county_id_fips"]
                    )
                ]
                .county_id_fips.unique()
                .tolist()
            },
        )
    return AssetCheckResult(passed=True)
