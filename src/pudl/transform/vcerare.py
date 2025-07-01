"""Transformations of the Vibrant Clean Energy Resource Adequacy Renewable Energy (RARE) Power Dataset.

Wind and solar profiles are extracted separately, but concatenated into a single table
in this module, as they have exactly the same structure.
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import (
    asset,
)

import pudl
from pudl.helpers import cleanstrings_snake, simplify_columns, zero_pad_numeric_string
from pudl.metadata.classes import Resource
from pudl.metadata.dfs import POLITICAL_SUBDIVISIONS
from pudl.metadata.fields import apply_pudl_dtypes
from pudl.workspace.setup import PudlPaths

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
    reason we've named the column place_name and it should be considered
    part of the primary key. There are several instances of multiple subregions that
    map to a single county_id_fips value.

    """
    logger.info(
        "Prepping Lat-Long-FIPS table for merging with the capacity factor tables"
    )
    ps_usa_df = POLITICAL_SUBDIVISIONS[POLITICAL_SUBDIVISIONS["country_code"] == "USA"]
    state_names = cleanstrings_snake(
        ps_usa_df, ["subdivision_name"]
    ).subdivision_name.tolist()

    # Handle west virginia as a special case
    state_names.remove("west_virginia")
    state_names.insert(0, "west-virginia")

    state_pattern = "|".join(state_names)
    lat_long_fips = (
        # Making the county_state_names lowercase to match the values in the capacity factor tables
        raw_vcerare__lat_lon_fips.pipe(simplify_columns)
        .assign(
            county_state_names=lambda x: x.county_state_names.str.lower()
            .replace({r"\.": "", "-": "_"}, regex=True)
            .pipe(_spot_fix_great_lakes_values)
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
            place_name=lambda x: x.county_state_names.str.replace(
                "west_virginia",
                "west-virginia",  # Temporary workaround to make sure we don't split 'west' from 'virginia'
            ).str.extract(rf"([a-z_]+)_({state_pattern})$")[0]
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

    logger.info("Nulling FIPS IDs for non-county regions.")
    lake_county_state_names = [
        "lake_erie_ohio",
        "lake_huron_michigan",
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
    lat_long_fips.loc[
        lat_long_fips.county_state_names.isin(lake_county_state_names), "county_id_fips"
    ] = pd.NA
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
    df.report_year = df.report_year.astype(int)  # Ensure this is getting read as an int

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


def _drop_city_cols(df: pd.DataFrame, df_name: str) -> pd.DataFrame:
    """Drop city columns from the capacity factor tables before stacking.

    We do this early since the columns can be dropped by name here, and we don't have to
    search through all of the stacked rows to find matching records.
    """
    city_cols = [
        x
        for x in ["bedford_city_virginia", "clifton_forge_city_virginia"]
        if x in df.columns
    ]
    logger.info(f"Dropping {city_cols} from {df_name} table.")
    return df.drop(columns=city_cols)


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
            columns={
                "level_3": "county_state_names",
                0: f"capacity_factor_{df_name}",
            }
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


def _standardize_census_names(vce_fips_df: pd.DataFrame, census_pep_data: pd.DataFrame):
    """Make sure that the VCE place names correspond to the latest census vintage.

    This function solves a problem of slight inconsistencies between Census PEP data and
    the county names provided by VCE RARE. We join the latest version of the Census PEP
    data onto the VCE RARE lat lon FIPS dataframe by FIPS ID, and then we take the
    Census PEP version of the county name wherever these values differ.

    Because the county_state_name column corresponds to the column names of each
    spreadsheet, we avoid altering it and only update the place_name column.
    In the final dataframe, we join all the dataframes on the original county_state_name
    value and drop this column, leaving only an updated place_name value in the final
    output.

    The function returns the cleaned VCE FIPS dataframe with updated place_name,
    as compared to the original VCE RARE values. Lakes and city names are not updated,
    as lakes don't have comparable values in the Census PEP data and we drop the city values.
    """
    census_fips = census_pep_data[
        ["county_id_fips", "area_name", "state"]
    ].drop_duplicates()

    census_fips["area_name"] = census_fips["area_name"].str.lower()
    # VCE RARE data does not include the place type,
    # so we drop these from the census data
    census_fips["area_name"] = (
        census_fips["area_name"]
        .str.replace("county", "")
        .str.replace("parish", "")
        .str.strip()
    )

    # Drop lakes and two cities we're going to remove from our dataset later
    vce_fips_df_sub = vce_fips_df.loc[
        ~vce_fips_df.county_state_names.isin(
            ["bedford_city_virginia", "clifton_forge_city_virginia"]
        )
    ].dropna(subset="county_id_fips")

    # Combine both dataframes on FIPS ID and state
    names_df = vce_fips_df_sub.merge(
        census_fips,
        on="county_id_fips",
        how="left",
        validate="one_to_one",
        suffixes=["", "_census"],
    )

    # Add back in our weirdos
    lakes_and_cities = vce_fips_df.loc[
        ~vce_fips_df.county_state_names.isin(names_df.county_state_names)
    ]
    names_df = pd.concat([names_df, lakes_and_cities])

    # Where there is no county data, fill in with VCE data
    names_df["area_name"] = names_df["area_name"].fillna(
        names_df["place_name"].astype(str)
    )
    names_df["area_name"] = names_df["area_name"].str.replace(
        "_", " "
    )  # Clean up the place name

    # Log differences, but only show ones that don't have a difference of "_"
    # to make this actually informative when debugging
    log_df = names_df.loc[
        names_df.place_name.str.replace("_", " ") != names_df.area_name,
        ["place_name", "area_name"],
    ].rename(columns={"place_name": "vce_place_name", "area_name": "census_place_name"})
    logger.debug(f"Updating the following place names:\n{log_df}")
    # Identified 74 replacements in 2025-06, expect this shouldn't change much.
    # If it does, manually inspect the debug log above and make sure name changes
    # are reasonable and expected.
    assert len(log_df) <= 74, f"Expected 74 replacements, found {len(log_df)}"

    names_df = (
        names_df.drop(columns=["place_name", "state_census"])
        .rename(
            columns={
                "area_name": "place_name",
            }
            # This gets converted to a string in the merge, so we reconvert to categorical
        )
        .assign(county_id_fips=lambda x: x.county_id_fips.astype("category"))
        .assign(place_name=lambda x: x.place_name.astype("category"))
    )

    return names_df


def _handle_2015_nulls(combined_df: pd.DataFrame, year: int):
    """Handle unexpected null values in 2015.

    In 2015, there are a few hundred null values for PV capacity factors that
    should be zeroed out, according to correspondence with the data provider.
    This function narrowly zeroes out these nulls, expecting that the rest of the
    data should conform to the expectation of no-null values.
    """
    if year == 2015:
        null_selector = (combined_df.capacity_factor_solar_pv.isnull()) & (
            combined_df.report_year == 2015
        )
        logger.info(
            f"{len(combined_df.loc[null_selector])} null PV capacity values found in the 2015 data. Zeroing out these values."
        )
        assert len(combined_df.loc[null_selector]) == 1320, (
            f"Expected 1320 null values but found {len(combined_df.loc[null_selector])}."
        )
        combined_df.loc[
            null_selector,
            "capacity_factor_solar_pv",
        ] = 0
    return combined_df


def _clip_unexpected_2016_pv_capacity(df: pd.DataFrame, df_name: str, year: int):
    """Handle unexpectedly large PV capacity values in 2016.

    In 2016, there are a few values for PV capacity factors that exceed the maximum
    allowed values noted in the read-me (110%).
    should be zeroed out, according to correspondence with the data provider.
    This function narrowly zeroes out these nulls, expecting that the rest of the
    data should conform to the expectation of no-null values.
    """
    if (year == 2016) and (df_name == "solar_pv"):
        logger.info(
            f"{len(df.loc[df.capacity_factor_solar_pv > 1.10])} out-of-bounds PV capacity factor values found in the 2016 data. Clipping these values."
        )
        assert len(df.loc[df.capacity_factor_solar_pv > 1.10]) == 365, (
            f"Found {len(df.loc[df.capacity_factor_solar_pv > 1.10])} solar capacity values over 1.10, expected 365."
        )
        df.loc[df.capacity_factor_solar_pv > 1.10, "capacity_factor_solar_pv"] = 1.10
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
    ).drop(columns="county_state_names")
    # We need the _vcerare columns earlier when we check that no counties are missing, but
    # at this point they are extraneous
    return combined_df


def _get_parquet_path():
    return PudlPaths().parquet_path("out_vcerare__hourly_available_capacity_factor")


def _spot_fix_great_lakes_values(sr: pd.Series) -> pd.Series:
    """Normalize spelling of great lakes in cell values."""
    return sr.replace("lake_hurron_michigan", "lake_huron_michigan")


def _spot_fix_great_lakes_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize spelling of great lakes in column names."""
    return df.rename(
        columns={
            "lake_hurron_michigan": "lake_huron_michigan",
        }
    )


def one_year_hourly_available_capacity_factor(
    year: int,
    raw_vcerare__lat_lon_fips: pd.DataFrame,
    raw_vcerare__fixed_solar_pv_lat_upv: pd.DataFrame,
    raw_vcerare__offshore_wind_power_140m: pd.DataFrame,
    raw_vcerare__onshore_wind_power_100m: pd.DataFrame,
    census_pep_data: pd.DataFrame,
) -> pd.DataFrame:
    """Transform raw Vibrant Clean Energy renewable generation profiles.

    Concatenates the solar and wind capacity factors into a single table and turns
    the columns for each county or subregion into a single place_name column.
    """
    logger.info(
        f"Transforming the VCE RARE hourly available capacity factor tables for {year}."
    )
    # Clean up the FIPS table and update state_county names to match Census data
    fips_df_census = _prep_lat_long_fips_df(raw_vcerare__lat_lon_fips).pipe(
        _standardize_census_names, census_pep_data
    )

    # Apply the same transforms to all the capacity factor tables. This is slower
    # than doing it to a concatenated table but less memory intensive because
    # it doesn't need to process the ginormous table all at once.
    raw_dict = {
        "solar_pv": raw_vcerare__fixed_solar_pv_lat_upv,
        "offshore_wind": raw_vcerare__offshore_wind_power_140m,
        "onshore_wind": raw_vcerare__onshore_wind_power_100m,
    }
    clean_dict = {
        df_name: _spot_fix_great_lakes_columns(df)
        .pipe(_check_for_valid_counties, fips_df_census, df_name)
        .pipe(_add_time_cols, df_name)
        .pipe(_drop_city_cols, df_name)
        .pipe(_make_cap_fac_frac, df_name)
        .pipe(_stack_cap_fac_df, df_name)
        .pipe(_clip_unexpected_2016_pv_capacity, df_name, year)
        for df_name, df in raw_dict.items()
    }

    # Combine the data and perform a few last cleaning mechanisms
    # Sort the data by primary key columns to produce compact row groups
    return apply_pudl_dtypes(
        _combine_all_cap_fac_dfs(clean_dict)
        .pipe(_handle_2015_nulls, year)
        .pipe(_combine_cap_fac_with_fips_df, fips_df_census)
        .sort_values(by=["state", "place_name", "datetime_utc"])
        .reset_index(drop=True)
    )


@asset(op_tags={"memory-use": "high"})
def out_vcerare__hourly_available_capacity_factor(
    context,
    raw_vcerare__lat_lon_fips: pd.DataFrame,
    raw_vcerare__fixed_solar_pv_lat_upv: pd.DataFrame,
    raw_vcerare__offshore_wind_power_140m: pd.DataFrame,
    raw_vcerare__onshore_wind_power_100m: pd.DataFrame,
    _core_censuspep__yearly_geocodes: pd.DataFrame,
):
    """Transform raw Vibrant Clean Energy renewable generation profiles.

    Concatenates the solar and wind capacity factors into a single table and turns
    the columns for each county or subregion into a single place_name column.
    Asset will process 1 year of data at a time to limit peak memory usage.
    """

    def _get_year(df, year):
        return df.loc[df["report_year"] == year]

    # Get census vintage to conform the data to.
    assert int(_core_censuspep__yearly_geocodes.report_year.max()) >= int(
        raw_vcerare__fixed_solar_pv_lat_upv["report_year"].max()
    )  # Check these are in sync

    # Only keep latest census data relating to the state-county level
    # See: https://www.census.gov/programs-surveys/geography/technical-documentation/naming-convention/cartographic-boundary-file/carto-boundary-summary-level.html
    census_pep_data = _core_censuspep__yearly_geocodes.loc[
        (
            _core_censuspep__yearly_geocodes.report_year
            == _core_censuspep__yearly_geocodes.report_year.max()
        )
        & (
            _core_censuspep__yearly_geocodes.fips_level == "050"
        )  # Only keep state-county level records
    ]

    parquet_path = _get_parquet_path()
    schema = Resource.from_id(
        "out_vcerare__hourly_available_capacity_factor"
    ).to_pyarrow()

    with pq.ParquetWriter(
        where=parquet_path, schema=schema, compression="snappy", version="2.6"
    ) as parquet_writer:
        for year in raw_vcerare__fixed_solar_pv_lat_upv["report_year"].unique():
            df = one_year_hourly_available_capacity_factor(
                year=int(year),
                raw_vcerare__lat_lon_fips=raw_vcerare__lat_lon_fips,
                raw_vcerare__fixed_solar_pv_lat_upv=_get_year(
                    raw_vcerare__fixed_solar_pv_lat_upv, year
                ),
                raw_vcerare__offshore_wind_power_140m=_get_year(
                    raw_vcerare__offshore_wind_power_140m, year
                ),
                raw_vcerare__onshore_wind_power_100m=_get_year(
                    raw_vcerare__onshore_wind_power_100m, year
                ),
                census_pep_data=census_pep_data,
            )
            parquet_writer.write_table(
                pa.Table.from_pandas(df, schema=schema, preserve_index=False)
            )
