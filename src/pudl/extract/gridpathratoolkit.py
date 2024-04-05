"""Extract GridPath RA Toolkit renewable energy generation profiles from zipped CSVs.

These hourly time are organized by technology type: Wind and Solar; and level of
processing: original, aggregated, and extended. This module currently only extracts the
data which is both aggregated and extended, since it is the closest to being analysis
ready. In the future we may extract the other versions, or re-implement the entire
aggregation and extension process within PUDL, so the outputs are being derived from the
more granular original data, and so a variety of different aggregations can be provided.

Alongside the Wind + Solar time series, there are also aggregation tables that describe
which of the original plants & generators were combined to make the aggregated datasets.
These are stored as single CSVs.

In addition there's a table of daily weather data, which is also stored as a single CSV.
"""

from io import BytesIO
from pathlib import Path

import pandas as pd
from dagster import AssetsDefinition, Output, asset

import pudl
from pudl.settings import DataSource
from pudl.workspace.datastore import Datastore

logger = pudl.logging_helpers.get_logger(__name__)


def _extract_csv(part: str, ds: Datastore) -> pd.DataFrame:
    return pd.read_csv(BytesIO(ds.get_unique_resource("gridpathratoolkit", part=part)))


def _extract_capacity_factor(part: str, ds: Datastore) -> pd.DataFrame:
    zf = ds.get_zipfile_resource("gridpathratoolkit", part=part)
    profiles = pd.read_csv(BytesIO(zf.read("timestamps.csv"))).rename(
        columns={"HE": "hour"}
    )

    files = [fn for fn in zf.namelist() if "timestamps" not in fn]
    for fn in files:
        aggregation_group = Path(fn).stem
        new_profile = pd.read_csv(
            BytesIO(zf.read(fn)), header=None, names=[aggregation_group]
        )
        profiles = pd.concat([profiles, new_profile], axis="columns")
    return profiles


def raw_gridpathratoolkit_asset_factory(part: str) -> AssetsDefinition:
    """An asset factory for GridPath RA Toolkit hourly generation profiles.

    This factory works on the processed hourly profiles that store one capacity factor
    time series per file with the time index stored in a separate file named
    timestamps.csv. We extract the timestamps first and use them as the index of the
    dataframe, concatenating the capacity factor time series as separate columns in a
    (temporarily) wide-format dataframe.

    The stems of the filenames are used as column labels, which are later transformed
    into the ``aggregation_group`` field, indicating which generators were aggregated to
    produce the time series based on the wind and solar capacity aggregation tables.
    """
    asset_kwargs = {
        "name": f"raw_gridpathratoolkit__{part}",
        "required_resource_keys": {"datastore", "dataset_settings"},
        "compute_kind": "Python",
    }
    if part == "aggregated_extended_solar_capacity":
        asset_kwargs["op_tags"] = {"memory-use": "high"}

    @asset(**asset_kwargs)
    def _extract(context):
        """Extract raw GridPath RA Toolkit renewable energy generation profiles.

        Args:
            context: dagster keyword that provides access to resources and config.
        """
        gpratk_settings = context.resources.dataset_settings.gridpathratoolkit
        ds = context.resources.datastore
        csv_parts = [
            "daily_weather",
            "solar_capacity_aggregations",
            "wind_capacity_aggregations",
        ]
        capacity_factor_parts = [
            "aggregated_extended_solar_capacity",
            "aggregated_extended_wind_capacity",
        ]

        if part in gpratk_settings.parts:
            logger.info(f"Extracting {part} from GridPath RA Toolkit Data")
            if part in csv_parts:
                extracted_df = _extract_csv(part, ds)
            elif part in capacity_factor_parts:
                extracted_df = _extract_capacity_factor(part, ds)
            else:
                raise ValueError(f"Unrecognized part: {part}")  # pragma: no cover
        else:
            # If the settings say we don't process a table, return an empty dataframe
            extracted_df = pd.DataFrame()
        return Output(value=extracted_df)

    return _extract


raw_gridpathratoolkit_assets = [
    raw_gridpathratoolkit_asset_factory(part)
    for part in DataSource.from_id("gridpathratoolkit").working_partitions["parts"]
]
