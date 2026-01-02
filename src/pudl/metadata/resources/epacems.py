"""Table definitions for the EPA CEMS data group."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_epacems__hourly_emissions": {
        "description": {
            "additional_summary_text": "emissions and plant operational data as required by 40 CFR Part 75.",
            "usage_warnings": ["scale_hazard", "incomplete_id_coverage"],
            "additional_details_text": """Continuous Emissions Monitoring Systems
(CEMS) are used to determine the rate of gas or particulate matter exiting a point
source of emissions. The EPA Clean Air Markets Division (CAMD) has collected data on
power plant emissions from CEMS units stretching back to 1995. The CEMS dataset
includes hourly gross load, SO2, CO2, and NOx emissions associated with a given
emissions-unit during startup, shutdown, and instances of malfunction. An EPA CEMS
emissions-unit or smokestack unit is not the same as an EIA unit. See
:ref:`core_epa__assn_eia_epacamd` for details about how to connect CEMS data to
corresponding EIA units.

Only fossil-combustion units over 25 MW are required to install and use CEMS, so
there are some units that do report in EIA-860 or EIA-923 that do not have any
CEMS data.
""",
        },
        "schema": {
            "fields": [
                "plant_id_eia",
                "plant_id_epa",
                "emissions_unit_id_epa",
                "operating_datetime_utc",
                "year",
                "state",
                "operating_time_hours",
                "gross_load_mw",
                "heat_content_mmbtu",
                "steam_load_lbs",
                "so2_mass_lbs",
                "so2_mass_measurement_code",
                "nox_mass_lbs",
                "nox_mass_measurement_code",
                "co2_mass_tons",
                "co2_mass_measurement_code",
            ],
            "primary_key": [
                "plant_id_epa",
                "emissions_unit_id_epa",
                "operating_datetime_utc",
            ],
        },
        "sources": ["eia860", "epacems"],
        "field_namespace": "epacems",
        "etl_group": "epacems",
        "create_database_schema": False,
    },
}
"""EPA CEMS resource attributes by PUDL identifier (``resource.name``).

Keys are in alphabetical order.

See :func:`pudl.metadata.helpers.build_foreign_keys` for the expected format of
``foreign_key_rules``.
"""
