"""Transformations of the Vibrant Clean Energy Resource Adequacy Renewable Energy (RARE) Power Dataset.

Wind and solar profiles are extracted separately, but concatenated into a single table
in this module, as they have exactly the same structure.
"""

import calendar

import pandas as pd
import polars as pl
from dagster import (
    asset,
)

import pudl.logging_helpers
from pudl.dagster.op_tags import COLD_PATH_OP_TAGS
from pudl.helpers import (
    ParquetData,
    cleanstrings_snake,
    lf_from_parquet,
    persist_table_as_parquet,
    simplify_columns,
    zero_pad_numeric_string,
)
from pudl.metadata.dfs import POLITICAL_SUBDIVISIONS

logger = pudl.logging_helpers.get_logger(__name__)

# Non-county columns in the wide raw capacity factor tables; everything else is a
# per-county/subregion capacity factor column.
HOURLY_ID_COLS = ["hour_of_year", "report_year"]


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

    Args:
        raw_vcerare__lat_lon_fips: The raw VCE RARE lat/lon/FIPS mapping table.

    Returns:
        The prepped table with ``county_state_names``, ``county_id_fips`` (nulled
        for lakes), ``place_name``, ``latitude``, ``longitude``, and ``state``
        columns.
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
            county_state_names=lambda x: (
                x.county_state_names.str.lower()
                .replace({r"\.": "", "-": "_"}, regex=True)
                .pipe(_spot_fix_great_lakes_fips)
            )
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


def _stack_cap_fac_df(
    lf: pl.LazyFrame,
    lf_name: str,
) -> pl.LazyFrame:
    """Reshape one wide capacity factor table from county columns to county rows.

    Each raw capacity factor table has one column per county/subregion. This
    unpivots those into a single ``county_state_names`` column (cast to
    Categorical) plus a ``capacity_factor_<lf_name>`` value column, keeping
    ``hour_of_year`` and ``report_year`` as identifiers. Applying this per table
    rather than to the concatenated table keeps peak memory down.

    Args:
        lf: One raw capacity factor table, wide, with ``hour_of_year`` and
            ``report_year`` columns plus one column per county/subregion.
        lf_name: Short table label (e.g. ``"solar_pv"``); names the value column
            and is used for logging.

    Returns:
        The table in long form with columns ``hour_of_year``, ``report_year``,
        ``county_state_names``, and ``capacity_factor_<lf_name>``.
    """
    logger.info(f"Stacking the county/subregion columns for {lf_name} table.")

    # Identify which columns are county columns (not metadata)
    county_cols = [
        col for col in lf.collect_schema().names() if col not in HOURLY_ID_COLS
    ]

    # Use Polars unpivot to convert wide → long
    lf_long = lf.unpivot(
        index=HOURLY_ID_COLS,
        on=county_cols,
        variable_name="county_state_names",
        value_name=f"capacity_factor_{lf_name}",
    ).with_columns(pl.col("county_state_names").cast(pl.Categorical))

    return lf_long


def _add_time_cols(lf: pl.LazyFrame, lf_name: str, year: int) -> pl.LazyFrame:
    """Add aligned ``datetime_utc`` and integer ``hour_of_year`` columns.

    The ``datetime_utc`` column matters for merging with other data; the integer
    ``hour_of_year`` (1-8760) matters for modeling; ``report_year`` is handy for
    filtering, so all three are kept.

    Older vintages publish an integer ``hour_of_year`` and we reconstruct
    ``datetime_utc`` from it. Vintages from 2024 on publish an hourly datetime column
    (arriving as ``hour_of_year``) and we derive the integer ``hour_of_year`` from that
    instead.

    On leap years December 31st is excluded so that every year has exactly 8760 hours.
    Older vintages already omit it; newer ones may include it, so it is clipped here
    based on whether ``year`` is a leap year.

    Args:
        lf: One capacity factor table, already stacked, with ``report_year`` and
            ``hour_of_year`` columns.
        lf_name: Short table label, used for logging.
        year: The report year of this table.

    Returns:
        The table with ``datetime_utc`` (datetime) and ``hour_of_year`` (Int32,
        1-8760) columns, clipped to 8760 hours on leap years.

    Raises:
        AssertionError: For ``year >= 2024``, if the incoming ``hour_of_year`` column
            is not a datetime column as expected.
    """
    logger.info(f"Adding time columns for {lf_name} table")

    # Convert report_year to int
    lf = lf.with_columns(report_year=pl.col("report_year").cast(pl.Int32))

    if year >= 2024:
        hour_of_year_dtype = lf.collect_schema()["hour_of_year"]
        assert hour_of_year_dtype == pl.Datetime, (
            f"Expected the {lf_name} {year} table to publish hour_of_year as a "
            f"datetime column (as VCE RARE has since 2024), got "
            f"{hour_of_year_dtype}."
        )
        lf = lf.rename({"hour_of_year": "datetime_utc"}).with_columns(
            hour_of_year=(pl.col("datetime_utc").dt.ordinal_day() - 1) * 24
            + pl.col("datetime_utc").dt.hour()
            + 1
        )  # Add 1 to conform to 0:00 as hour 1
    else:
        # This data is compiled for modeling purposes and skips the last
        # day of a leap year. When adding a datetime column, we need
        # to make sure that we skip the 31st of December on leap years and that
        # every year has exactly 8760 hours in it.

        # Compute datetime from year start + hours offset
        # hour_of_year ranges from 1-8760, so subtract 1 to get 0-based offset
        lf = lf.with_columns(
            datetime_utc=pl.datetime(pl.col("report_year"), 1, 1)
            + pl.duration(hours=pl.col("hour_of_year") - 1)
        )

    # Ensure hour of year has uniform dtype for all years.
    lf = lf.with_columns(hour_of_year=pl.col("hour_of_year").cast(pl.Int32))

    # VCE RARE is compiled for modeling and every year must have exactly 8760
    # hours, so December 31st is dropped on leap years. That day is ordinal day
    # 366, i.e. the only day whose hour_of_year exceeds 8760, so this clip is a
    # no-op when the raw data already excluded it (8760 hours) and drops it when
    # the raw data included it (8784 hours).
    if calendar.isleap(year):
        lf = lf.filter(pl.col("hour_of_year") <= 8760)

    return lf


def _drop_city_cols(lf: pl.LazyFrame, lf_name: str) -> pl.LazyFrame:
    """Drop the two Virginia independent-city rows from a capacity factor table.

    ``bedford_city_virginia`` and ``clifton_forge_city_virginia`` are reported by VCE
    RARE but are not counties, and are excluded from the output.

    Args:
        lf: A stacked capacity factor table with a ``county_state_names`` column.
        lf_name: Short table label, used for logging.

    Returns:
        The table with the two independent-city rows removed.
    """
    city_state_names = ["bedford_city_virginia", "clifton_forge_city_virginia"]
    logger.info(f"Dropping {city_state_names} from {lf_name} table.")

    return lf.filter(~pl.col("county_state_names").is_in(city_state_names))


def _make_cap_fac_frac(lf: pl.LazyFrame, lf_name: str) -> pl.LazyFrame:
    """Convert the capacity factor column from a percentage to a fraction.

    Args:
        lf: A stacked capacity factor table with a ``capacity_factor_<lf_name>``
            column expressed as a percentage (0-100).
        lf_name: Short table label; selects the value column and is used for logging.

    Returns:
        The table with ``capacity_factor_<lf_name>`` divided by 100.
    """
    logger.info(f"Converting capacity factor into a fraction for {lf_name} table.")

    cf_col = f"capacity_factor_{lf_name}"
    return lf.with_columns((pl.col(cf_col) / 100).alias(cf_col))


def _check_for_valid_counties(
    lf: pl.LazyFrame, clean_fips_df: pd.DataFrame, lf_name: str
) -> pl.LazyFrame:
    """Check that every place name in the data appears in the FIPS mapping table.

    Runs on the wide raw table so the place names can be read straight from the
    schema (they are the non-identifier column names), avoiding a data scan.

    Args:
        lf: A wide raw capacity factor table with one column per place name plus
            the ``HOURLY_ID_COLS`` identifier columns.
        clean_fips_df: The cleaned lat/lon/FIPS table, whose ``county_state_names``
            column is the set of expected place names.
        lf_name: Short table label, used for logging and the error message.

    Returns:
        ``lf`` unchanged; this is a validation pass-through.

    Raises:
        AssertionError: If the data contains any place name that is not present in
            ``clean_fips_df``.
    """
    logger.info(f"Checking for valid counties in the {lf_name} table.")

    # Fragile coupling: this runs before _drop_city_cols, so the city place names
    # (bedford_city_virginia, clifton_forge_city_virginia) are still present as
    # columns here. The check only passes because _standardize_census_names
    # deliberately adds those same city rows back into clean_fips_df. If the set of
    # non-county place names kept by the two functions ever drifts apart, this
    # raises.
    county_names_fips = clean_fips_df.county_state_names.unique().tolist()

    data_place_names = [
        col for col in lf.collect_schema().names() if col not in HOURLY_ID_COLS
    ]
    unexpected_place_names = [x for x in data_place_names if x not in county_names_fips]

    if unexpected_place_names:
        raise AssertionError(
            f"Found {len(unexpected_place_names)} place name(s) in the {lf_name} "
            f"table that aren't in the FIPS table: {unexpected_place_names}"
        )
    return lf


def _standardize_census_names(
    vce_fips_df: pd.DataFrame, census_pep_data: pd.DataFrame
) -> pd.DataFrame:
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

    Args:
        vce_fips_df: The prepped VCE RARE lat/lon/FIPS table from
            ``_prep_lat_long_fips_df``.
        census_pep_data: The latest-vintage state-county-level rows of
            ``_core_censuspep__yearly_geocodes``.

    Returns:
        ``vce_fips_df`` with ``place_name`` replaced by the Census PEP county name
        wherever the two differ, and the original VCE ``place_name`` column dropped.

    Raises:
        AssertionError: If more than 74 place names would be replaced, suggesting
            the Census data or VCE names have shifted and need manual review.
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


def _clip_unexpected_2016_pv_capacity(
    lf: pl.LazyFrame, lf_name: str, year: int
) -> pl.LazyFrame:
    """Clip a known set of out-of-bounds 2016 solar PV capacity factors.

    The 2016 solar PV data contains exactly 365 hourly capacity factor values that
    exceed the 110% maximum documented in the data's read-me. Per correspondence with
    the data provider these should be capped at 110%. This only touches the
    ``solar_pv`` table for ``year == 2016``; every other table/year combination passes
    through unchanged.

    Args:
        lf: A stacked capacity factor table.
        lf_name: Short table label; the clip only applies when this is ``"solar_pv"``.
        year: Report year; the clip only applies when this is 2016.

    Returns:
        The table with ``capacity_factor_solar_pv`` values above 1.10 clipped to 1.10
        for 2016 solar PV; otherwise ``lf`` unchanged.

    Raises:
        AssertionError: If the number of 2016 solar PV values exceeding 1.10 is not
            exactly 365, which would mean the data has changed and needs review.
    """
    if (year == 2016) and (lf_name == "solar_pv"):
        cf_col = "capacity_factor_solar_pv"
        outlier_count = (
            lf.filter(pl.col(cf_col) > 1.10).select(pl.len()).collect().item()
        )
        logger.info(
            f"{outlier_count} out-of-bounds PV capacity factor values found in 2016. Clipping these values."
        )

        assert outlier_count == 365, (
            f"Found {outlier_count} solar capacity values over 1.10, expected 365."
        )

        lf = lf.with_columns(
            pl.when(pl.col(cf_col) > 1.10)
            .then(1.10)
            .otherwise(pl.col(cf_col))
            .alias(cf_col)
        )

    return lf


def _spot_fix_great_lakes_fips(sr: pd.Series) -> pd.Series:
    """Normalize the misspelled Lake Huron place name in the lat/lon/FIPS table.

    VCE RARE spells Lake Huron as ``lake_hurron_michigan`` in the raw
    ``county_state_names`` values of the lat/lon/FIPS mapping table. This must be
    fixed so those values line up with the (separately fixed) capacity factor
    tables when they are joined on ``county_state_names`` in
    ``merge_all_vce_tables``. See ``_spot_fix_great_lakes_capacity_factor`` for
    the equivalent fix applied to the capacity factor data.

    Args:
        sr: The ``county_state_names`` Series from the raw lat/lon/FIPS table.

    Returns:
        The Series with ``lake_hurron_michigan`` replaced by ``lake_huron_michigan``.
    """
    return sr.replace("lake_hurron_michigan", "lake_huron_michigan")


def _spot_fix_great_lakes_capacity_factor(
    lf: pl.LazyFrame, lf_name: str
) -> pl.LazyFrame:
    """Normalize the misspelled Lake Huron place name in a capacity factor table.

    VCE RARE spells Lake Huron as ``lake_hurron_michigan`` in the raw capacity
    factor data, where each place name is a column. This is the same fix
    ``_spot_fix_great_lakes_fips`` applies to the lat/lon/FIPS table; both are
    needed because the misspelling appears in both raw sources and the two tables
    are joined on ``county_state_names`` in ``merge_all_vce_tables``. It runs
    before ``_check_for_valid_counties`` so the corrected name matches the FIPS
    table.

    Args:
        lf: A wide raw capacity factor table with one column per place name.
        lf_name: Label for the table, used only for logging by the caller.

    Returns:
        The LazyFrame with any ``lake_hurron_michigan`` column renamed to
        ``lake_huron_michigan``.
    """
    return lf.rename({"lake_hurron_michigan": "lake_huron_michigan"}, strict=False)


def one_year_hourly_available_capacity_factor(
    year: int,
    fips_df_census: pd.DataFrame,
    raw_vcerare__fixed_solar_pv_lat_upv: pl.LazyFrame,
    raw_vcerare__offshore_wind_power_140m: pl.LazyFrame,
    raw_vcerare__onshore_wind_power_100m: pl.LazyFrame,
) -> dict[str, ParquetData]:
    """Transform one year of raw VCE RARE capacity factor tables to parquet.

    Runs the same stack-and-clean pipeline over each of the three raw capacity
    factor tables (solar PV, offshore wind, onshore wind) separately rather than
    on a single concatenated table, to keep peak memory down, and writes each
    result to a partitioned ``_core_vcerare__*`` parquet file.

    Args:
        year: The report year being processed.
        fips_df_census: The cleaned lat/lon/FIPS table, used to validate place
            names.
        raw_vcerare__fixed_solar_pv_lat_upv: Raw solar PV capacity factors for
            ``year``.
        raw_vcerare__offshore_wind_power_140m: Raw offshore wind capacity factors
            for ``year``.
        raw_vcerare__onshore_wind_power_100m: Raw onshore wind capacity factors
            for ``year``.

    Returns:
        A dict mapping each ``_core_vcerare__<table>`` name to the ``ParquetData``
        describing its written partition for ``year``.
    """

    def _table_name(lf_name: str) -> str:
        return f"_core_vcerare__{lf_name}"

    logger.info(
        f"Transforming the VCE RARE hourly available capacity factor tables for {year}."
    )

    # Apply the same transforms to all the capacity factor tables. This is slower
    # than doing it to a concatenated table but less memory intensive because
    # it doesn't need to process the ginormous table all at once.
    raw_dict = {
        "solar_pv": raw_vcerare__fixed_solar_pv_lat_upv,
        "offshore_wind": raw_vcerare__offshore_wind_power_140m,
        "onshore_wind": raw_vcerare__onshore_wind_power_100m,
    }
    return {
        _table_name(lf_name): persist_table_as_parquet(
            lf.pipe(_spot_fix_great_lakes_capacity_factor, lf_name)
            .pipe(_check_for_valid_counties, fips_df_census, lf_name)
            .pipe(_stack_cap_fac_df, lf_name)
            .pipe(_add_time_cols, lf_name, year)
            .pipe(_drop_city_cols, lf_name)
            .pipe(_make_cap_fac_frac, lf_name)
            .pipe(_clip_unexpected_2016_pv_capacity, lf_name, year),
            table_name=_table_name(lf_name),
            partitions={"year": year},
        )
        for lf_name, lf in raw_dict.items()
    }


def merge_all_vce_tables(
    transformed_tables: dict[str, ParquetData],
    vce_fips_table: ParquetData,
    table_name: str,
    year: int,
) -> ParquetData:
    """Merge the cleaned VCE capacity factor tables into the final ``out`` table.

    Full-joins the three ``_core_vcerare__*`` tables on the hourly key, fills the
    solar PV capacity factor with 0 where wind-only rows introduced nulls, joins the
    lat/lon/FIPS metadata on ``county_state_names``, sorts, and writes the result to a
    partitioned parquet file for ``year``.

    Args:
        transformed_tables: The ``_core_vcerare__*`` -> ``ParquetData`` mapping
            returned by ``one_year_hourly_available_capacity_factor``.
        vce_fips_table: ``ParquetData`` for the cleaned lat/lon/FIPS table. Must
            carry an ``index`` column (see the note at the ``.drop`` call below).
        table_name: Name of the partitioned output table to write.
        year: The report year being merged.

    Returns:
        The ``ParquetData`` describing the written ``table_name`` partition for
        ``year``.
    """
    merge_keys = ["report_year", "datetime_utc", "hour_of_year", "county_state_names"]

    # Merge and write as partitioned parquet files to disk
    return persist_table_as_parquet(
        lf_from_parquet(transformed_tables["_core_vcerare__solar_pv"])
        .join(
            lf_from_parquet(transformed_tables["_core_vcerare__offshore_wind"]),
            on=merge_keys,
            how="full",
            coalesce=True,
        )
        .join(
            lf_from_parquet(transformed_tables["_core_vcerare__onshore_wind"]),
            on=merge_keys,
            how="full",
            coalesce=True,
        )
        .with_columns(pl.col("capacity_factor_solar_pv").fill_null(0))
        .join(
            lf_from_parquet(vce_fips_table),
            on="county_state_names",
            how="left",
            validate="m:1",
        )
        .sort(by=["state", "place_name", "datetime_utc"])
        # Note: this depends on ``vce_fips_table`` carrying an ``index`` column, which
        # is added by the ``reset_index()`` call in
        # ``out_vcerare__hourly_available_capacity_factor`` before that table is written
        # to parquet. The ``index`` column is dropped again below.
        .drop(["index", "county_state_names"]),
        table_name=table_name,
        partitions={"year": year},
    )


@asset(
    # VCE RARE is extracted but not yet integrated downstream (see extract_vcerare
    # in pudl.extract.vcerare, likewise deprioritized); nothing consumes this
    # table. Low priority so it acts as late-DAG filler rather than crowding the
    # critical path early.
    op_tags={"memory-use": "high"} | COLD_PATH_OP_TAGS,
    io_manager_key="parquet_io_manager",
)
def out_vcerare__hourly_available_capacity_factor(
    context,
    raw_vcerare__fixed_solar_pv_lat_upv: dict[int, ParquetData],
    raw_vcerare__offshore_wind_power_140m: dict[int, ParquetData],
    raw_vcerare__onshore_wind_power_100m: dict[int, ParquetData],
    raw_vcerare__lat_lon_fips: pd.DataFrame,
    _core_censuspep__yearly_geocodes: pd.DataFrame,
) -> pl.LazyFrame:
    """Transform raw Vibrant Clean Energy renewable generation profiles.

    Concatenates the solar and wind capacity factors into a single table and turns
    the columns for each county or subregion into a single place_name column. One
    year of data is processed at a time to limit peak memory usage, with each year
    written to its own partition of the output parquet file.

    Args:
        context: The Dagster asset execution context.
        raw_vcerare__fixed_solar_pv_lat_upv: Year -> ``ParquetData`` mapping of raw
            solar PV capacity factors.
        raw_vcerare__offshore_wind_power_140m: Year -> ``ParquetData`` mapping of raw
            offshore wind capacity factors.
        raw_vcerare__onshore_wind_power_100m: Year -> ``ParquetData`` mapping of raw
            onshore wind capacity factors.
        raw_vcerare__lat_lon_fips: The raw VCE RARE lat/lon/FIPS mapping table.
        _core_censuspep__yearly_geocodes: Census PEP geocodes, used to standardize
            county place names to the latest vintage.

    Returns:
        A LazyFrame scanning every written year partition of the merged
        ``out_vcerare__hourly_available_capacity_factor`` table.

    Raises:
        AssertionError: If the latest available Census PEP vintage predates the
            most recent VCE RARE report year.
    """
    report_years = raw_vcerare__fixed_solar_pv_lat_upv.keys()
    # Get census vintage to conform the data to.
    assert int(_core_censuspep__yearly_geocodes.report_year.max()) >= max(
        report_years
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

    # Clean up the FIPS table and update state_county names to match Census data
    fips_df_census = _prep_lat_long_fips_df(raw_vcerare__lat_lon_fips).pipe(
        _standardize_census_names, census_pep_data
    )

    # Write to disk for later merge with full VCE table
    fips_table = persist_table_as_parquet(
        fips_df_census.reset_index().astype({"county_state_names": "category"}),
        "_vce_fips_census",
    )

    # Intermediate table name to write partitioned output parquet files
    partitioned_output_table = "mega_vce"
    for year in report_years:
        transformed_tables = one_year_hourly_available_capacity_factor(
            year=year,
            fips_df_census=fips_df_census,
            raw_vcerare__fixed_solar_pv_lat_upv=lf_from_parquet(
                raw_vcerare__fixed_solar_pv_lat_upv[year],
            ),
            raw_vcerare__offshore_wind_power_140m=lf_from_parquet(
                raw_vcerare__offshore_wind_power_140m[year],
            ),
            raw_vcerare__onshore_wind_power_100m=lf_from_parquet(
                raw_vcerare__onshore_wind_power_100m[year],
            ),
        )

        # Merge table into final output table
        merge_all_vce_tables(
            transformed_tables, fips_table, partitioned_output_table, year
        )

    return lf_from_parquet(
        ParquetData(table_name=partitioned_output_table), use_all_partitions=True
    )
