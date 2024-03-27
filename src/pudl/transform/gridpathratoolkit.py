"""Transformations of the GridPath RA Toolkit renewable generation profiles."""

import pandas as pd
from dagster import asset

import pudl


def _transform(
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
    capacity_factors["utc_datetime"] = (
        pd.DatetimeIndex(pd.to_datetime(capacity_factors.loc[:, datetime_cols]))
        - utc_offset
    )
    capacity_factors = (
        capacity_factors.drop(columns=datetime_cols)
        .set_index("utc_datetime")
        .stack()
        .reset_index()
    )
    capacity_factors.columns = ["utc_datetime", "aggregation_key", "capacity_factor"]
    capacity_factors = capacity_factors.astype({"aggregation_key": "string"})
    return capacity_factors


@asset(io_manager_key="pudl_io_manager")
def core_gridpathratoolkit__hourly_aggregated_extended_capacity_factors(
    raw_gridpathratoolkit__aggregated_extended_solar_capacity: pd.DataFrame,
    raw_gridpathratoolkit__aggregated_extended_wind_capacity: pd.DataFrame,
) -> pd.DataFrame:
    """Transform raw GridPath RA Toolkit renewable generation profiles.

    Concatenates the solar and wind capacity factors into a single table and turns the
    aggregation key into a categorical column to save space.
    """
    pacific_standard_time = pd.Timedelta("-8h")
    return pd.concat(
        [
            _transform(
                capacity_factors=raw_gridpathratoolkit__aggregated_extended_solar_capacity,
                utc_offset=pacific_standard_time,
            ),
            _transform(
                capacity_factors=raw_gridpathratoolkit__aggregated_extended_wind_capacity,
                utc_offset=pacific_standard_time,
            ),
        ]
    ).astype({"aggregation_key": pd.CategoricalDtype()})


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
                "gp_aggregation": "aggregation_key",
                "eia_nameplatecap": "capacity_mw",
                "include": "include_generator",
            }
        )
        .astype({"include_generator": "boolean", "aggregation_key": "string"})
    )
    return agg


@asset(io_manager_key="pudl_io_manager")
def core_gridpathratoolkit__capacity_factor_aggregations(
    raw_gridpathratoolkit__wind_capacity_aggregations: pd.DataFrame,
    raw_gridpathratoolkit__solar_capacity_aggregations: pd.DataFrame,
) -> pd.DataFrame:
    """Transform and combine raw GridPath RA Toolkit generator aggregations."""
    return pd.concat(
        [
            _transform_aggs(raw_gridpathratoolkit__wind_capacity_aggregations),
            _transform_aggs(raw_gridpathratoolkit__solar_capacity_aggregations),
        ]
    )
