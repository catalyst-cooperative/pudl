"""Extract GridPath RA Toolkit renewable energy generation profiles from CSVs.

There are several time series available from the GridPath Resource Adequacy Toolkit.

Solar:

* Hourly Solar by Generator
* Hourly Solar by Zone
* Hourly Solar by Zone (Extended)
* Solar Aggregations

Wind:

* Hourly Wind by Plant
* Hourly Wind by Zone
* Hourly Wind by Zone (Extended)
* Wind Aggregations

Thermal Derates:

* Hourly Thermal Derates by Generator
* Hourly Thermal Derates by Generator (Extended)

Misc:

* Daily Weather Data

"""

from io import BytesIO
from pathlib import Path

import pandas as pd
from dagster import AssetsDefinition, Output, asset

import pudl
from pudl.settings import DataSource
from pudl.workspace.datastore import Datastore

logger = pudl.logging_helpers.get_logger(__name__)


def raw_gridpathratoolkit_asset_factory(part: str) -> AssetsDefinition:
    """An asset factory for GridPath RA Toolkit hourly generation profiles.

    This factory works on the processed hourly profiles that store one capacity factor
    time series per file with the time index stored in a separate file named
    timestamps.csv. We extract the timestamps first and use them as the index of the
    dataframe, concatenating the capacity factor time series as separate columns in a
    (temporarily) wide-format dataframe.

    The stems of the filenames are used as column labels, which are later transformed
    into the ``aggregation_key`` field, indicating which generators were aggregated to
    produce the time series based on the wind and solar capacity aggregation tables.
    """

    def _extract_csv(part: str, ds: Datastore) -> pd.DataFrame:
        return pd.read_csv(
            BytesIO(ds.get_unique_resource("gridpathratoolkit", part=part))
        )

    def _extract_capacity_factor(part: str, ds: Datastore) -> pd.DataFrame:
        zf = ds.get_zipfile_resource("gridpathratoolkit", part=part)
        profiles = pd.read_csv(BytesIO(zf.read("timestamps.csv"))).rename(
            columns={"HE": "hour"}
        )

        files = [fn for fn in zf.namelist() if "timestamps" not in fn]
        for fn in files:
            aggregation_key = Path(fn).stem
            new_profile = pd.read_csv(
                BytesIO(zf.read(fn)), header=None, names=[aggregation_key]
            )
            profiles = pd.concat([profiles, new_profile], axis="columns")
        return profiles

    @asset(
        name=f"raw_gridpathratoolkit__{part}",
        required_resource_keys={"datastore", "dataset_settings"},
    )
    def _extract(context):
        """Extract raw GridPath RA Toolkit renewable energy generation profiles.

        Args:
            context: dagster keyword that provides access to resources and config.
        """
        gpratk_settings = context.resources.dataset_settings.gridpathratoolkit
        ds = context.resources.datastore

        parts = {
            "daily_weather": (_extract_csv, gpratk_settings.daily_weather),
            "aggregated_extended_solar_capacity": (
                _extract_capacity_factor,
                "solar" in gpratk_settings.technology_types
                and "extended" in gpratk_settings.processing_levels,
            ),
            "aggregated_extended_wind_capacity": (
                _extract_capacity_factor,
                "wind" in gpratk_settings.technology_types
                and "extended" in gpratk_settings.processing_levels,
            ),
            "solar_capacity_aggregations": (
                _extract_csv,
                "solar" in gpratk_settings.technology_types
                and (
                    "extended" in gpratk_settings.processing_levels
                    or "aggregated" in gpratk_settings.processing_levels
                ),
            ),
            "wind_capacity_aggregations": (
                _extract_csv,
                "wind" in gpratk_settings.technology_types
                and (
                    "extended" in gpratk_settings.processing_levels
                    or "aggregated" in gpratk_settings.processing_levels
                ),
            ),
        }

        if part not in parts:
            raise ValueError(f"Unable to process {part}!")

        func, condition = parts[part]
        if condition:
            logger.info(f"Extracting {part} from GridPath RA Toolkit Data")
            return Output(value=func(part, ds))

        # If the settings say we don't process a table, return an empty dataframe
        return Output(value=pd.DataFrame())

    return _extract


raw_gridpathratoolkit_capacity_factor_assets = [
    raw_gridpathratoolkit_asset_factory(part)
    for part in DataSource.from_id("gridpathratoolkit").working_partitions["parts"]
]
