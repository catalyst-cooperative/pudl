"""Transformations of the Vibrant Clean Energy Resource Adequacy Renewable Energy (RARE) Power Dataset.

Wind and solar profiles are extracted separately, but concatenated into a single table
in this module, as they have exactly the same structure.
"""

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    asset,
    asset_check,
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
    reason we've named the column county_or_lake_name and it should be considered
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
    state_pattern = "|".join(state_names)
    lat_long_fips = (
        # Making the county_state_names lowercase to match the values in the capacity factor tables
        raw_vcerare__lat_lon_fips.pipe(simplify_columns)
        .assign(
            county_state_names=lambda x: x.county_state_names.str.lower()
            .replace({r"\.": "", "-": "_"}, regex=True)
            .pipe(_spot_fix_great_lakes_values)
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

    logger.info("Spot check: fixed typos in great lakes.")

    logger.info("Nulling FIPS IDs for non-county regions.")
    lake_county_state_names = [
        "lake_erie_ohio",
        "lake_huron_michigan",
        "lake_michigan_illinois",
        "lake_michigan_indiana",
        "lake_michigan_michigan",
        "lake_michigan_wisconsin",
        "lake_ontario_new_york",
        "lake_saint_clair_michigan",
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

    We do this early since the columns can be droped by name here, and we don't have to
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


def _get_parquet_path():
    return PudlPaths().parquet_path("out_vcerare__hourly_available_capacity_factor")


def _spot_fix_great_lakes_values(sr: pd.Series) -> pd.Series:
    """Normalize spelling of great lakes in cell values."""
    return sr.replace("lake_hurron_michigan", "lake_huron_michigan").replace(
        "lake_st_clair_michigan", "lake_saint_clair_michigan"
    )


def _spot_fix_great_lakes_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize spelling of great lakes in column names."""
    return df.rename(
        columns={
            "lake_hurron_michigan": "lake_huron_michigan",
            "lake_st_clair_michigan": "lake_saint_clair_michigan",
        }
    )


def one_year_hourly_available_capacity_factor(
    year: int,
    raw_vcerare__lat_lon_fips: pd.DataFrame,
    raw_vcerare__fixed_solar_pv_lat_upv: pd.DataFrame,
    raw_vcerare__offshore_wind_power_140m: pd.DataFrame,
    raw_vcerare__onshore_wind_power_100m: pd.DataFrame,
) -> pd.DataFrame:
    """Transform raw Vibrant Clean Energy renewable generation profiles.

    Concatenates the solar and wind capacity factors into a single table and turns
    the columns for each county or subregion into a single county_or_lake_name column.
    """
    logger.info(
        f"Transforming the VCE RARE hourly available capacity factor tables for {year}."
    )
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
        df_name: _spot_fix_great_lakes_columns(df)
        .pipe(_check_for_valid_counties, fips_df, df_name)
        .pipe(_add_time_cols, df_name)
        .pipe(_drop_city_cols, df_name)
        .pipe(_make_cap_fac_frac, df_name)
        .pipe(_stack_cap_fac_df, df_name)
        for df_name, df in raw_dict.items()
    }
    # Combine the data and perform a few last cleaning mechanisms
    # Sort the data by primary key columns to produce compact row groups
    return apply_pudl_dtypes(
        _combine_all_cap_fac_dfs(clean_dict)
        .pipe(_combine_cap_fac_with_fips_df, fips_df)
        .sort_values(by=["state", "county_or_lake_name", "datetime_utc"])
        .reset_index(drop=True)
    )


@asset(op_tags={"memory-use": "high"})
def out_vcerare__hourly_available_capacity_factor(
    raw_vcerare__lat_lon_fips: pd.DataFrame,
    raw_vcerare__fixed_solar_pv_lat_upv: pd.DataFrame,
    raw_vcerare__offshore_wind_power_140m: pd.DataFrame,
    raw_vcerare__onshore_wind_power_100m: pd.DataFrame,
):
    """Transform raw Vibrant Clean Energy renewable generation profiles.

    Concatenates the solar and wind capacity factors into a single table and turns
    the columns for each county or subregion into a single county_or_lake_name column.
    Asset will process 1 year of data at a time to limit peak memory usage.
    """

    def _get_year(df, year):
        return df.loc[df["report_year"] == year]

    parquet_path = _get_parquet_path()
    schema = Resource.from_id(
        "out_vcerare__hourly_available_capacity_factor"
    ).to_pyarrow()

    with pq.ParquetWriter(
        where=parquet_path, schema=schema, compression="snappy", version="2.6"
    ) as parquet_writer:
        for year in raw_vcerare__fixed_solar_pv_lat_upv["report_year"].unique():
            df = one_year_hourly_available_capacity_factor(
                year=year,
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
            )
            parquet_writer.write_table(
                pa.Table.from_pandas(df, schema=schema, preserve_index=False)
            )


def _load_duckdb_table():
    """Load VCE RARE output table to duckdb for running asset checks."""
    parquet_path = str(_get_parquet_path())
    return duckdb.read_parquet(parquet_path)


@asset_check(
    asset=out_vcerare__hourly_available_capacity_factor,
    blocking=True,
    description="Check that row count matches expected.",
)
def check_rows(context: AssetCheckExecutionContext) -> AssetCheckResult:
    """Check rows."""
    logger.info("Check VCE RARE hourly table is the expected length")

    # Define row counts for report years
    row_counts_by_year = {
        2019: 27287400,
        2020: 27287400,
        2021: 27287400,
        2022: 27287400,
        2023: 27287400,
    }

    vce = _load_duckdb_table()  # noqa: F841
    errors = []
    for report_year, length in duckdb.query(
        "SELECT report_year, COUNT(*) FROM vce GROUP BY ALL"
    ).fetchall():
        if (expected_length := row_counts_by_year[report_year]) != length:
            errors.append(
                f"Expected {expected_length} for report year {report_year}, found {length}"
            )
    if errors:
        logger.warning(errors)
        return AssetCheckResult(
            passed=False,
            description="One or more report years have unexpected length",
            metadata={"errors": errors},
        )
    return AssetCheckResult(passed=True)


@asset_check(
    asset=out_vcerare__hourly_available_capacity_factor,
    blocking=True,
    description="Check for unexpected nulls.",
)
def check_nulls() -> AssetCheckResult:
    """Check nulls."""
    logger.info("Check there are no NA values in VCE RARE table (except FIPS)")
    vce = _load_duckdb_table()
    columns = [c for c in vce.columns if c != "county_id_fips"]
    null_columns = []
    for c in columns:
        nulls = duckdb.query(f"SELECT {c} FROM vce WHERE {c} IS NULL").fetchall()  # noqa: S608
        if len(nulls) > 0:
            null_columns.append(c)

    if len(null_columns) > 0:
        return AssetCheckResult(
            passed=False,
            description=f"Found NULL values in columns {', '.join(null_columns)}",
        )
    return AssetCheckResult(passed=True)


@asset_check(
    asset=out_vcerare__hourly_available_capacity_factor,
    blocking=True,
    description="Check for PV capacity factor above upper bound.",
)
def check_pv_capacity_factor_upper_bound() -> AssetCheckResult:
    """Check pv capacity upper bound."""
    # Make sure the capacity_factor values are below the expected value
    # There are some solar values that are slightly over 1 due to colder
    # than average panel temperatures.
    logger.info("Check capacity factors in VCE RARE table are between 0 and 1.")
    vce = _load_duckdb_table()  # noqa: F841
    cap_oob = duckdb.query(
        "SELECT * FROM vce WHERE capacity_factor_solar_pv > 1.02"
    ).fetchall()
    if len(cap_oob) > 0:
        return AssetCheckResult(
            passed=False,
            description="Found PV capacity factor values greater than 1.02",
        )
    return AssetCheckResult(passed=True)


@asset_check(
    asset=out_vcerare__hourly_available_capacity_factor,
    blocking=True,
    description="Check for wind capacity factor above upper bound.",
)
def check_wind_capacity_factor_upper_bound() -> AssetCheckResult:
    """Check wind capacity upper bound."""
    vce = _load_duckdb_table()
    columns = [c for c in vce.columns if c.endswith("wind")]
    cap_oob_columns = []
    for c in columns:
        cap_oob = duckdb.query(f"SELECT {c} FROM vce WHERE {c} > 1.0").fetchall()  # noqa: S608
        if len(cap_oob) > 0:  # noqa: S608
            cap_oob_columns.append(c)

    if len(cap_oob_columns) > 0:
        return AssetCheckResult(
            passed=False,
            description=f"Found wind capacity factor values greater than 1.0 in column {', '.join(cap_oob_columns)}",
        )
    return AssetCheckResult(passed=True)


@asset_check(
    asset=out_vcerare__hourly_available_capacity_factor,
    blocking=True,
    description="Check capacity factors below lower bound.",
)
def check_capacity_factor_lower_bound() -> AssetCheckResult:
    """Check capacity lower bound."""
    vce = _load_duckdb_table()
    # Make sure capacity_factor values are greater than or equal to 0
    columns = [c for c in vce.columns if c.startswith("capacity_factor")]
    cap_oob_columns = []
    for c in columns:
        cap_oob = duckdb.query(f"SELECT {c} FROM vce WHERE {c} < 0.0").fetchall()  # noqa: S608
        if len(cap_oob) > 0:
            cap_oob_columns.append(c)

    if len(cap_oob_columns) > 0:
        return AssetCheckResult(
            passed=False,
            description=f"Found capacity factor values less than 0 from column {', '.join(cap_oob_columns)}",
        )
    return AssetCheckResult(passed=True)


@asset_check(
    asset=out_vcerare__hourly_available_capacity_factor,
    blocking=True,
    description="Check max hour of year in VCE RARE table is 8760.",
)
def check_max_hour_of_year() -> AssetCheckResult:
    """Check max hour of year."""
    vce = _load_duckdb_table()  # noqa: F841
    logger.info("Check max hour of year in VCE RARE table is 8760.")
    (max_hour,) = duckdb.query("SELECT MAX(hour_of_year) FROM vce").fetchone()
    if max_hour != 8760:
        return AssetCheckResult(
            passed=False,
            description="Found hour_of_year values larger than 8760",
        )
    return AssetCheckResult(passed=True)


@asset_check(
    asset=out_vcerare__hourly_available_capacity_factor,
    blocking=True,
    description="Check for unexpected Dec 31st, 2020 dates in VCE RARE table.",
)
def check_unexpected_dates() -> AssetCheckResult:
    """Check unexpected dates."""
    vce = _load_duckdb_table()  # noqa: F841
    logger.info("Check for unexpected Dec 31st, 2020 dates in VCE RARE table.")
    unexpected_dates = duckdb.query(
        "SELECT datetime_utc FROM vce WHERE datetime_utc = make_date(2020, 12, 31)"
    ).fetchall()
    if len(unexpected_dates) > 0:
        return AssetCheckResult(
            passed=False,
            description="Found rows for December 31, 2020 which should not exist",
        )
    return AssetCheckResult(passed=True)


@asset_check(
    asset=out_vcerare__hourly_available_capacity_factor,
    blocking=True,
    description="Check hour from date and hour of year match in VCE RARE table.",
)
def check_hour_from_date() -> AssetCheckResult:
    """Check hour from date."""
    vce = _load_duckdb_table()  # noqa: F841
    logger.info("Check hour from date and hour of year match in VCE RARE table.")
    mismatched_hours = duckdb.query(
        "SELECT * FROM vce WHERE"
        "(datepart('hr', datetime_utc) +"
        "((datepart('dayofyear', datetime_utc)-1)*24)+1) != hour_of_year"
    ).fetchall()
    if len(mismatched_hours) > 0:
        return AssetCheckResult(
            passed=False,
            description="hour_of_year values don't match date values",
            metadata={"mismatched_hours": mismatched_hours},
        )
    return AssetCheckResult(passed=True)


@asset_check(
    asset=out_vcerare__hourly_available_capacity_factor,
    blocking=True,
    description="Check for rows for Bedford City or Clifton Forge City in VCE RARE table.",
)
def check_unexpected_counties() -> AssetCheckResult:
    """Check unexpected counties."""
    vce = _load_duckdb_table()  # noqa: F841
    logger.info(
        "Check for rows for Bedford City or Clifton Forge City in VCE RARE table."
    )
    unexpected_counties = duckdb.query(
        "SELECT * FROM vce "
        "WHERE county_or_lake_name in ("
        "'bedford_city','clifton_forge_city',"
        "'lake_hurron','lake_st_clair'"
        ")"
    ).fetchall()
    if len(unexpected_counties) > 0:
        return AssetCheckResult(
            passed=False,
            description="found records for bedford_city or clifton_forge_city that shouldn't exist",
            metadata={"unexpected_counties": unexpected_counties},
        )
    return AssetCheckResult(passed=True)
