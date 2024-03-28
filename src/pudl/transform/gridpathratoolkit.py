"""Transformations of the GridPath RA Toolkit renewable generation profiles."""

import pandas as pd
from dagster import asset


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
