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

# TODO: Reorganize the archive to use asset names for partitions directly
GRIDPATHRATK_PARTS = {
    "hourly-solar-zonal-capacity-synth": {
        "table_name": "aggregated_extended_solar",
    },
    "hourly-wind-zonal-capacity-synth": {
        "table_name": "aggregated_extended_wind",
    },
}


def raw_gridpathratk_profile_asset_factory(part: str) -> AssetsDefinition:
    """An asset factory for GridPath RA Toolkit hourly generation profiles.

    This factory works on the processed hourly profiles that store one capacity factor
    time series per file with a the time index stored in a separate timestamps.csv. The
    stems of the filenames are used to identify the timeseries for later processing.
    """

    @asset(
        name=f"raw_gridpathratk__{GRIDPATHRATK_PARTS[part]["table_name"]}",
        required_resource_keys={"datastore", "dataset_settings"},
    )
    def _extract_raw_gridpathratk(context):
        """Extract raw GridPath RA Toolkit renewable energy generation profiles.

        Args:
            context: dagster keyword that provides access to resources and config.
        """
        ds = context.resources.datastore
        zf = ds.get_zipfile_resource("gridpathratk", part=part)
        profiles = pd.read_csv(BytesIO(zf.read(f"{part}/timestamps.csv"))).rename(
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

    return _extract_raw_gridpathratk


raw_gridpathratk_assets = [
    raw_gridpathratk_profile_asset_factory(part) for part in GRIDPATHRATK_PARTS
]
