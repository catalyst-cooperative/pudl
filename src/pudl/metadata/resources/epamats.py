"""Table definitions for the EPA MATS data group."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_epamats__hourly_emissions": {
        "description": {
            "additional_summary_text": (
                "hourly emissions of mercury (Hg), hydrogen chloride (HCl), and "
                "hydrogen fluoride (HF) from coal-fired power plants."
            ),
            "usage_warnings": ["incomplete_id_coverage"],
            "availability_text": "2026",
            "additional_details_text": """The EPA Mercury and Air Toxics Standards
(MATS) dataset provides detailed information on emissions of hazardous air
pollutants, specifically mercury (Hg), hydrogen chloride (HCl), and hydrogen
fluoride (HF), from coal- and oil-fired power plants in the United States.
Established under the Clean Air Act, MATS requires affected units to install
and operate continuous emissions monitoring systems (CEMS) to track pollutant
output rates and mass emissions.

Only coal- and oil-fired units above a certain capacity threshold are required
to comply with MATS, so not all plants that report to EIA have corresponding
MATS monitoring data. The data covers roughly 2015 onward and is reported
hourly.

Units in the MATS data are identified by their EPA emissions unit IDs, which
may differ from EIA generator IDs. Use the core_epa__assn_eia_epacamd
crosswalk to connect MATS emissions units to EIA plant IDs and generators.
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
                "plant_name_epa",
                "operating_time_hours",
                "gross_load_mw",
                "heat_content_mmbtu",
                "steam_load_lbs",
                "hg_output_rate_lb_per_gwh",
                "hg_input_rate_lb_per_tbtu",
                "hg_mass_lbs",
                "hg_mass_measurement_code",
                "hcl_output_rate_lb_per_mwh",
                "hcl_input_rate_lb_per_mmbtu",
                "hcl_mass_lbs",
                "hcl_mass_measurement_code",
                "associated_stacks",
                "primary_fuel_type",
                "secondary_fuel_type",
                "unit_type",
                "so2_controls",
                "nox_controls",
                "pm_controls",
                "hg_controls",
            ],
            "primary_key": [
                "plant_id_epa",
                "emissions_unit_id_epa",
                "operating_datetime_utc",
            ],
        },
        "sources": ["eia860", "epamats"],
        "field_namespace": "epamats",
        "etl_group": "epamats",
        "create_database_schema": False,
    },
}
"""EPA MATS resource attributes by PUDL identifier (``resource.name``).

Keys are in alphabetical order.

See :func:`pudl.metadata.helpers.build_foreign_keys` for the expected format of
``foreign_key_rules``.
"""
