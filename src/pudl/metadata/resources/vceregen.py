"""Table definitions for data coming from the Vibrant Clean Energy Renewable Generation Profiles."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "out_vceregen__hourly_available_capacity_factor": {
        "description": (
            "The data in this table were produced by Vibrant Clean Energy, and are "
            "licensed to the public under the Creative Commons Attribution 4.0 International "
            "license (CC-BY-4.0). The table consists of hourly, county-level renewable "
            "generation profiles in the continental United States and are compiled based on "
            "outputs from the NOAA HRRR weather model. Profiles are stated as a capacity "
            "factor (a fraction of nameplate capacity) and exist for onshore wind, offshore "
            "wind, and fixed-tilt solar generation types."
        ),
        "schema": {
            "fields": [
                "datetime_utc",
                "hour_of_year",
                "report_year",
                "county_id_fips",
                "county",
                "state",
                "latitude",
                "longitude",
                "capacity_factor_solar_pv",
                "capacity_factor_onshore_wind",
                "capacity_factor_offshore_wind",
            ],
            "primary_key": ["datetime_utc", "county_id_fips", "county"],
        },
        "sources": ["vceregen"],
        "field_namespace": "vceregen",
        "etl_group": "vceregen",
        "create_database_schema": False,
    },
}
