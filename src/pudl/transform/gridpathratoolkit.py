"""Transformations of the GridPath RA Toolkit renewable generation profiles.

Wind and solar profiles are extracted separately, but concatenated into a single table
in this module, as they have exactly the same structure. The generator aggregation group
association tables for various technology types are also concatenated together.
"""

import pandas as pd
from dagster import asset

import pudl


def _transform_capacity_factors(
    capacity_factors: pd.DataFrame, utc_offset: pd.Timedelta
) -> pd.DataFrame:
    """Basic transformations that can be applied to many profiles.

    - Construct a datetime column and adjust it to be in UTC.
    - Reshape the table from wide to tidy format.
    - Name columns appropriately.
    """
    # Convert timestamp to UTC before stacking!
    # TODO: Ensure that we're using the same hour starting vs. ending convention
    # here as we use in the FERC-714 and EPA CEMS data. Should this be reflected in
    # the column names here and elsewhere, whatever the convention is?
    datetime_cols = ["year", "month", "day", "hour"]
    capacity_factors["datetime_utc"] = (
        pd.DatetimeIndex(pd.to_datetime(capacity_factors.loc[:, datetime_cols]))
        - utc_offset
    )
    capacity_factors = (
        capacity_factors.drop(columns=datetime_cols)
        .set_index("datetime_utc")
        .stack()
        .reset_index()
    )
    capacity_factors.columns = ["datetime_utc", "aggregation_group", "capacity_factor"]
    capacity_factors = capacity_factors.astype({"aggregation_group": "string"})
    return capacity_factors


@asset(
    io_manager_key="parquet_io_manager",
    compute_kind="Python",
    op_tags={"memory-use": "high"},
)
def out_gridpathratoolkit__hourly_available_capacity_factor(
    raw_gridpathratoolkit__aggregated_extended_solar_capacity: pd.DataFrame,
    raw_gridpathratoolkit__aggregated_extended_wind_capacity: pd.DataFrame,
) -> pd.DataFrame:
    """Transform raw GridPath RA Toolkit renewable generation profiles.

    Concatenates the solar and wind capacity factors into a single table and turns the
    aggregation key into a categorical column to save space.

    Note that this transform is a bit unusual, in that it is producing a highly
    processed output table. That's because we're working backwards from an archived
    finished product to be able to provide a minimum viable product. Our intent is to
    integrate or reimplement the steps required to produce this output table from
    less processed original inputs in the future.
    """
    pacific_standard_time = pd.Timedelta("-8h")
    return pd.concat(
        [
            _transform_capacity_factors(
                capacity_factors=raw_df, utc_offset=pacific_standard_time
            )
            for raw_df in [
                raw_gridpathratoolkit__aggregated_extended_solar_capacity,
                raw_gridpathratoolkit__aggregated_extended_wind_capacity,
            ]
            if not raw_df.empty
        ]
    ).astype({"aggregation_group": pd.CategoricalDtype()})


def _transform_aggs(raw_agg: pd.DataFrame) -> pd.DataFrame:
    """Transform raw GridPath RA Toolkit generator aggregations.

    - split EIA_UniqueID into plant + generator IDs
    - rename columns to use PUDL conventions
    - verify that split-out plant IDs always match reported plant IDs
    - Set column dtypes
    """
    # Get rid of non-alphanumeric characters, and convert to lowercase
    agg = pudl.helpers.simplify_columns(raw_agg)
    # Split the eia_uniqueid into plant and generator IDs
    # Should always be an integer (plant id) followed by an underscore, with anything
    # after the underscore being the generator ID (a string).
    separate_ids = agg["eia_uniqueid"].str.split("_", n=1, expand=True)
    separate_ids.columns = ["plant_id_eia", "generator_id"]
    separate_ids = separate_ids.astype({"plant_id_eia": int, "generator_id": "string"})
    agg = pd.concat([agg, separate_ids], axis="columns")
    # Ensure no mismatch between EIA plant IDs. Only applies to wind data.
    if "eia_plantcode" in agg.columns:
        assert agg.loc[agg.eia_plantcode != agg.plant_id_eia].empty
    agg = (
        agg.drop(
            columns=[
                col for col in ["eia_plantcode", "eia_uniqueid"] if col in agg.columns
            ]
        )
        .rename(
            columns={
                "gp_aggregation": "aggregation_group",
                "eia_nameplatecap": "capacity_mw",
                "include": "include_generator",
            }
        )
        .astype({"include_generator": "boolean", "aggregation_group": "string"})
        .pipe(
            pudl.helpers.remove_leading_zeros_from_numeric_strings,
            col_name="generator_id",
        )
    )
    # Plant 64272 does not exist in the PUDL data. However, plant 64481 is the only
    # plant that has a generator_id of "AEC1", and its solar, and has the same capacity
    # as the GridPath RA Toolkit expects for plant 64272, and also has generator AEC2
    # which shows up in the GridPath RA Toolkit data in association with plant 64481.
    agg.loc[
        (agg.plant_id_eia == 64272) & (agg.generator_id == "AEC1"), "plant_id_eia"
    ] = 64481
    return agg


@asset(
    io_manager_key="pudl_io_manager",
    compute_kind="Python",
)
def core_gridpathratoolkit__assn_generator_aggregation_group(
    raw_gridpathratoolkit__wind_capacity_aggregations: pd.DataFrame,
    raw_gridpathratoolkit__solar_capacity_aggregations: pd.DataFrame,
) -> pd.DataFrame:
    """Transform and combine raw GridPath RA Toolkit generator aggregations."""
    return pd.concat(
        [
            _transform_aggs(agg_df)
            for agg_df in [
                raw_gridpathratoolkit__wind_capacity_aggregations,
                raw_gridpathratoolkit__solar_capacity_aggregations,
            ]
            if not agg_df.empty
        ]
    )
