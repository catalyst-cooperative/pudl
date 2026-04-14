"""Table definitions for data coming from the Vibrant Clean Energy Renewable Generation Profiles."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "out_vcerare__hourly_available_capacity_factor": {
        "description": {
            "additional_summary_text": (
                "estimated county-averaged "
                "capacity factors for wind and solar generating facilities across the contiguous "
                "United States (US), to be used as a tool and input for resource adequacy modeling "
                "and planning."
            ),
            "usage_warnings": [
                {
                    "type": "custom",
                    "description": (
                        "The hourly capacity factors are normalized to unity for maximal power "
                        "output. To convert to units of power, the user must multiply by the installed capacity "
                        "within the county."
                    ),
                },
                {
                    "type": "custom",
                    "description": (
                        "Hourly capacity factors are spatially averaged across each county over the contiguous "
                        "USA. There are a handful of counties that are too small to pick up representation on "
                        "the HRRR operational forecast grid. As such, these counties will have no wind or solar "
                        "power production curves."
                    ),
                },
                {
                    "type": "custom",
                    "description": (
                        "Due to power production "
                        "performance being correlated with panel temperatures, during cold sunny periods, "
                        "some solar capacity factor values are greater than 1 (but less that 1.1)."
                    ),
                },
            ],
            "additional_details_text": (
                "The data in this table were produced by Vibrant Clean Energy, and are "
                "licensed to the public under the Creative Commons Attribution 4.0 International "
                "license (CC-BY-4.0).\n\n"
                "The technologies provided are:\n\n"
                "1) Onshore wind assuming a 100m hub height and 120m rotor diameter;\n\n"
                "2) Offshore wind assuming a 140m hub height and 120m rotor diameter;\n\n"
                "3) Utility solar assuming a fixed axis panel tilted at latitude.\n\n"
                "The foundation of the capacity factors provided here is the NOAA HRRR "
                "operational numerical weather prediction model. The HRRR covers the entire "
                "contiguous US at a horizontal resolution of 3 km. Forecasts are initialized each "
                "hour of the year. Forecast hour two (2) is used as the input data for the power "
                "algorithms. This forecast hour is chosen to trade-off the impact of the measurement "
                "and data assimilation procedure of the HRRR with the physics of the model to derive "
                "the most complete picture of the atmosphere at the forecast time horizon. "
                "\n\n"
                "For wind capacity factors: vertical slices of the atmosphere are considered across "
                "the defined rotor swept area. Bringing together wind speed, density, temperature and "
                "icing information, a power capacity is estimated using a representative power coefficient "
                "(Cp) curve to determine the power from a given wind speed, atmospheric density and "
                "temperature. There is no wake modeling included in the dataset.\n\n"
                "For solar capacity factors: pertinent surface weather variables are pulled such as "
                "incoming short wave radiation, direct normal irradiance (calculated in the HRRR 2016 "
                "forward), surface temperature and other parameters. These are used in a non-linear "
                "I-V curve translation to power capacity factors."
            ),
        },
        "schema": {
            "fields": [
                "state",
                "place_name",
                "datetime_utc",
                "report_year",
                "hour_of_year",
                "county_id_fips",
                "latitude",
                "longitude",
                "capacity_factor_solar_pv",
                "capacity_factor_onshore_wind",
                "capacity_factor_offshore_wind",
            ],
            "primary_key": ["state", "place_name", "datetime_utc"],
        },
        "sources": ["vcerare"],
        "field_namespace": "vcerare",
        "etl_group": "vcerare",
        "create_database_schema": False,
    },
}
