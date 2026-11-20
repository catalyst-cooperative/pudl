"""Table definitions for the EPA MATS data group."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_epamats__hourly_emissions": {
        "description": {
            "additional_summary_text": (
                "hourly emissions of mercury (Hg) and hydrogen chloride (HCl) "
                "from coal- and oil-fired power plants."
            ),
            "usage_warnings": ["incomplete_id_coverage"],
            "additional_details_text": """Not all plants that report to EIA are
represented here; see `Notable Irregularities
<https://docs.catalyst.coop/pudl/en/latest/data_sources/epamats.html#notable-irregularities>`__
for details.

MATS does not require reporting of hourly hydrogen fluoride (HF) emissions.
In the raw data from MATS, the four HF columns are entirely null throughout,
so they are dropped from this table.
Only the mercury (Hg) and hydrogen chloride (HCl) columns contain data.

Units in the MATS data are identified by their EPA emissions unit IDs, which may differ
from EIA generator IDs. This table already contains an EIA plant ID, but you can use the
``core_epa__assn_eia_epacamd`` crosswalk to connect MATS emissions units to EIA
boilers and generators.
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
"""
