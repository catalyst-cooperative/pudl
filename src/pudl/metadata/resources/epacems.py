"""Table definitions for the EPA CEMS data group."""
from typing import Any, Dict

RESOURCE_METADATA: Dict[str, Dict[str, Any]] = {
    "hourly_emissions_epacems": {
        "description": "Hourly emissions and plant operational data reported via Continuous Emissions Monitoring Systems as required by 40 CFR Part 75.",
        "schema": {
            "fields": [
                "state",
                "plant_id_eia",
                "unitid",
                "operating_datetime_utc",
                "operating_time_hours",
                "gross_load_mw",
                "steam_load_1000_lbs",
                "so2_mass_lbs",
                "so2_mass_measurement_code",
                "nox_rate_lbs_mmbtu",
                "nox_rate_measurement_code",
                "nox_mass_lbs",
                "nox_mass_measurement_code",
                "co2_mass_tons",
                "co2_mass_measurement_code",
                "heat_content_mmbtu",
                "facility_id",
                "unit_id_epa",
                "year",
            ],
            "primary_key": ["plant_id_eia", "unitid", "operating_datetime_utc"],
        },
        "sources": ["eia860", "epacems"],
        "field_namespace": "epacems",
        "etl_group": "epacems",
    },
}
"""
EPA CEMS resource attributes by PUDL identifier (``resource.name``).

Keys are in alphabetical order.

See :func:`pudl.metadata.helpers.build_foreign_keys` for the expected format of
``foreign_key_rules``.
"""
