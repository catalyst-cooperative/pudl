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
            "pk_check_chunk_field": "operating_datetime_utc",
        },
        "sources": ["eia860", "epacems"],
        "field_namespace": "epacems",
        "etl_group": "epacems",
        "create_database_schema": False,
    },
    "out_epacems__yearly_operational_characteristics": {
        "description": {
            "additional_summary_text": (
                "estimated operational characteristics for EPA CEMS emissions units."
            ),
            "usage_warnings": [
                "estimated_values",
                "experimental_wip",
                {
                    "type": "custom",
                    "description": "This table estimates values for each calendar year from a configurable trailing window of EPA CEMS quarters ending in that year (12 quarters, i.e. the most recent three full years, in production). Builds that only have a limited number of EPA CEMS quarters available, such as the fast ETL and CI, will produce estimates for a single partial year from a shorter, less accurate window.",
                },
            ],
            "additional_details_text": """This table summarizes several inferred
operational characteristics for each EPA CEMS emissions unit, for every calendar
year that has a full trailing window of usable EPA CEMS quarters available, using
hourly CEMS gross load and fuel heat content over that window. In production the
window is the three full years (12 quarters) ending in each reported year, so the
earliest reported year is limited by how far back that trailing window can reach.
EPA CEMS's first three years of reporting (1995-1997) are excluded as unusable
due to poor and inconsistent unit coverage, so the earliest reported year in
production is 2000, not 1997.

The values are not directly reported to source agencies. They are derived from observed
hourly operations. These variables should be treated as an analytical estimate rather
than as reported plant characteristics.

For methodological details, see :doc:`/methodology/operational_characteristics`.
""",
        },
        "schema": {
            "fields": [
                "report_year",
                "plant_id_epa",
                "emissions_unit_id_epa",
                "plant_id_eia",
                "state",
                "max_gross_load_mw",
                "min_stable_load_factor",
                "min_up_time_hours",
                "min_down_time_hours",
                "heat_rate_at_max_load_factor_mmbtu_per_mwh",
                "heat_rate_at_min_stable_load_factor_mmbtu_per_mwh",
                "ramp_up_rate_per_min",
                "ramp_down_rate_per_min",
            ],
            "primary_key": ["report_year", "plant_id_epa", "emissions_unit_id_epa"],
        },
        "sources": ["epacems"],
        "field_namespace": "epacems",
        "etl_group": "epacems",
    },
}
"""EPA CEMS resource attributes by PUDL identifier (``resource.name``).

Keys are in alphabetical order.

See :func:`pudl.metadata.helpers.build_foreign_keys` for the expected format of
``foreign_key_rules``.
"""
