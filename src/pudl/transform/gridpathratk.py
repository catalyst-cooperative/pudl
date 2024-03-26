"""Transformations of the GridPath RA Toolkit renewable generation profiles."""

import pandas as pd
from dagster import asset


def _transform_profiles(
    profiles: pd.DataFrame, utc_offset: pd.Timedelta
) -> pd.DataFrame:
    """Basic transformations that can be applied to many profiles.

    - Construct a datetime column and adjust it to be in UTC.
    - Reshape the table from wide to tidy format.
    - Rename columns.
    - Use categorical strings to represent the aggregation keys.

    """
    # Convert timestamp to UTC before stacking!
    # TODO: Ensure that we're using the same hour starting vs. ending convention
    # here as we use in the FERC-714 and EPA CEMS data. Should this be reflected in
    # the column names here and elsewhere, whatever the convention is?
    datetime_cols = ["year", "month", "day", "hour"]
    profiles["utc_datetime"] = (
        pd.DatetimeIndex(pd.to_datetime(profiles.loc[:, datetime_cols])) - utc_offset
    )
    profiles = (
        profiles.drop(columns=datetime_cols)
        .set_index("utc_datetime")
        .stack()
        .reset_index()
    )
    profiles.columns = ["utc_datetime", "aggregation_key", "capacity_factor"]
    profiles = profiles.astype({"aggregation_key": "string"})
    return profiles


@asset(io_manager_key="pudl_io_manager")
def core_gridpathratk__aggregated_extended_profiles(
    raw_gridpathratk__aggregated_extended_solar: pd.DataFrame,
    raw_gridpathratk__aggregated_extended_wind: pd.DataFrame,
) -> pd.DataFrame:
    """Transform raw GridPath RA Toolkit renewable generation profiles."""
    pacific_standard_time = pd.Timedelta("-8h")
    return pd.concat(
        [
            _transform_profiles(
                profiles=raw_gridpathratk__aggregated_extended_solar,
                utc_offset=pacific_standard_time,
            ),
            _transform_profiles(
                profiles=raw_gridpathratk__aggregated_extended_wind,
                utc_offset=pacific_standard_time,
            ),
        ]
    ).astype({"aggregation_key": pd.CategoricalDtype()})
