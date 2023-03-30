"""Table definitions for the EPA CEMS data group."""
from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "hourly_emissions_epacems": {
        "description": "Hourly emissions and plant operational data reported via Continuous Emissions Monitoring Systems as required by 40 CFR Part 75.",
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
                "steam_load_1000_lbs",
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
        "include_in_database": False,
    },
}
"""EPA CEMS resource attributes by PUDL identifier (``resource.name``).

Keys are in alphabetical order.

See :func:`pudl.metadata.helpers.build_foreign_keys` for the expected format of
``foreign_key_rules``.
"""
