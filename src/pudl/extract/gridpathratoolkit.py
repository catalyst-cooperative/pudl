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


def raw_gridpathratoolkit_capacity_factor_asset_factory(part: str) -> AssetsDefinition:
    """An asset factory for GridPath RA Toolkit hourly generation profiles.

    This factory works on the processed hourly profiles that store one capacity factor
    time series per file with the time index stored in a separate file named
    timestamps.csv. The stems of the filenames are used to identify the timeseries for
    later processing.
    """

    @asset(
        name=f"raw_gridpathratoolkit__{part}",
        required_resource_keys={"datastore", "dataset_settings"},
    )
    def _extract(context):
        """Extract raw GridPath RA Toolkit renewable energy generation profiles.

        Args:
            context: dagster keyword that provides access to resources and config.
        """
        ds = context.resources.datastore
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
        return Output(value=profiles)

    return _extract


def raw_gridpathratoolkit__csv_asset_factory(part: str) -> AssetsDefinition:
    """An asset factory for extracting standalone CSVs from the GridPath RA Toolkit.

    These are simple CSVs which indicate how to combine individual wind plants or solar
    generators into larger blocks of capacity, as well as some weather data.
    """

    @asset(
        name=f"raw_gridpathratoolkit__{part}",
        required_resource_keys={"datastore", "dataset_settings"},
    )
    def _extract(context):
        """Extract raw GridPath RA Toolkit CSV files.

        Args:
            context: dagster keyword that provides access to resources and config.
        """
        ds = context.resources.datastore
        df = pd.read_csv(
            BytesIO(ds.get_unique_resource("gridpathratoolkit", part=part))
        )
        return Output(value=df)

    return _extract


raw_gridpathratoolkit_capacity_factor_assets = [
    raw_gridpathratoolkit_capacity_factor_asset_factory(part)
    for part in [
        "aggregated_extended_solar_capacity",
        "aggregated_extended_wind_capacity",
    ]
]

raw_gridpathratoolkit_csvs = [
    raw_gridpathratoolkit__csv_asset_factory(part)
    for part in [
        "wind_capacity_aggregations",
        "solar_capacity_aggregations",
        "daily_weather",
    ]
]
