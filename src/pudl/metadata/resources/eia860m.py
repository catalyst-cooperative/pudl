"""Definitions of data tables primarily coming from EIA-860m."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_eia860m__changelog_generators": {
        "description": (
            """This table is a changelog of the monthly reported EIA-860M data. EIA-860M includes
generator tables with the most up-to-date catalog of EIA generators and their
operational status and other generator characteristics.  EIA-860M is reported monthly,
although for the vast majority of the generators nothing changes month-to-month. This
table is a changelog of that monthly reported generator data. There is a record
corresponding to the first instance of a generator and associated characteristics with a
report_date column and a valid_until_date column. Whenever any of the reported EIA-860M
data was changed for a record, there will be a new changelog record with a new
report_date."""
        ),
        "schema": {
            "fields": [
                "report_date",
                "valid_until_date",
                "plant_id_eia",
                "plant_name_eia",
                "utility_id_eia",
                "utility_name_eia",
                "generator_id",
                "balancing_authority_code_eia",
                "capacity_mw",
                "county",
                "current_planned_generator_operating_date",
                "data_maturity",
                "energy_source_code_1",
                "energy_storage_capacity_mwh",
                "fuel_type_code_pudl",
                "generator_operating_date",
                "generator_retirement_date",
                "latitude",
                "longitude",
                "net_capacity_mwdc",
                "operational_status",
                "operational_status_code",
                "planned_derate_date",
                "planned_generator_retirement_date",
                "planned_net_summer_capacity_derate_mw",
                "planned_net_summer_capacity_uprate_mw",
                "planned_uprate_date",
                "prime_mover_code",
                "sector_id_eia",
                "state",
                "summer_capacity_mw",
                "technology_description",
                "winter_capacity_mw",
            ],
            "primary_key": ["plant_id_eia", "generator_id", "report_date"],
        },
        "field_namespace": "eia",
        "sources": ["eia860"],
        "etl_group": "eia860",
    },
}
