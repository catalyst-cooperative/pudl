"""Definitions of data tables primarily coming from EIA-923."""
from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "boiler_fuel_eia923": {
        "description": "EIA-923 Monthly Boiler Fuel Consumption and Emissions Time Series. From EIA-923 Schedule 3.",
        "schema": {
            "fields": [
                "plant_id_eia",
                "boiler_id",
                "energy_source_code",
                "prime_mover_code",
                "fuel_type_code_pudl",
                "report_date",
                "fuel_consumed_units",
                "fuel_mmbtu_per_unit",
                "sulfur_content_pct",
                "ash_content_pct",
            ],
            "primary_key": [
                "plant_id_eia",
                "boiler_id",
                "energy_source_code",
                "prime_mover_code",
                "report_date",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia923"],
        "etl_group": "eia923",
    },
    "coalmine_eia923": {
        "description": "Coal mine attributes originally reported within the Fuel Receipts and Costs table via EIA-923 Schedule 2, Part C.",
        "schema": {
            "fields": [
                "mine_id_pudl",
                "mine_name",
                "mine_type_code",
                "state",
                "county_id_fips",
                "mine_id_msha",
                "data_maturity",
            ],
            "primary_key": ["mine_id_pudl"],
            "foreign_key_rules": {"fields": [["mine_id_pudl"]]},
        },
        "field_namespace": "eia",
        "sources": ["eia923"],
        "etl_group": "eia923",
    },
    "fuel_receipts_costs_eia923": {
        "description": "Monthly fuel contract information, purchases, and costs reported in EIA-923 Schedule 2, Part A.",
        "schema": {
            "fields": [
                "plant_id_eia",
                "report_date",
                "contract_type_code",
                "contract_expiration_date",
                "energy_source_code",
                "fuel_type_code_pudl",
                "fuel_group_code",
                "mine_id_pudl",
                "supplier_name",
                "fuel_received_units",
                "fuel_mmbtu_per_unit",
                "sulfur_content_pct",
                "ash_content_pct",
                "mercury_content_ppm",
                "fuel_cost_per_mmbtu",
                "primary_transportation_mode_code",
                "secondary_transportation_mode_code",
                "natural_gas_transport_code",
                "natural_gas_delivery_contract_type_code",
                "moisture_content_pct",
                "chlorine_content_ppm",
                "data_maturity",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia923"],
        "etl_group": "eia923",
    },
    "generation_eia923": {
        "description": "EIA-923 Monthly Generating Unit Net Generation Time Series. From EIA-923 Schedule 3.",
        "schema": {
            "fields": [
                "plant_id_eia",
                "generator_id",
                "report_date",
                "net_generation_mwh",
                "data_maturity",
            ],
            "primary_key": ["plant_id_eia", "generator_id", "report_date"],
        },
        "field_namespace": "eia",
        "sources": ["eia923"],
        "etl_group": "eia923",
    },
    "generation_fuel_eia923": {
        "description": "EIA-923 Monthly Generation and Fuel Consumption Time Series. From EIA-923 Schedule 3. Monthly electricity generation and fuel consumption reported for each combination of fuel and prime mover within a plant. This table does not include data from nuclear plants as they report at the generation unit level, rather than the plant level. See the generation_fuel_nuclear_eia923 table for nuclear electricity generation and fuel consumption.",
        "schema": {
            "fields": [
                "plant_id_eia",
                "report_date",
                "energy_source_code",
                "fuel_type_code_pudl",
                "fuel_type_code_aer",
                "prime_mover_code",
                "fuel_consumed_units",
                "fuel_consumed_for_electricity_units",
                "fuel_mmbtu_per_unit",
                "fuel_consumed_mmbtu",
                "fuel_consumed_for_electricity_mmbtu",
                "net_generation_mwh",
                "data_maturity",
            ],
            "primary_key": [
                "plant_id_eia",
                "report_date",
                "energy_source_code",
                "prime_mover_code",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia923"],
        "etl_group": "eia923",
    },
    "generation_fuel_nuclear_eia923": {
        "description": "EIA-923 Monthly Generation and Fuel Consumption Time Series. From EIA-923 Schedule 3. Monthly electricity generation and fuel consumption reported for each combination of fuel and prime mover within a nuclear generation unit.",
        "schema": {
            "fields": [
                "plant_id_eia",
                "report_date",
                "nuclear_unit_id",
                "energy_source_code",
                "fuel_type_code_pudl",
                "fuel_type_code_aer",
                "prime_mover_code",
                "fuel_consumed_units",
                "fuel_consumed_for_electricity_units",
                "fuel_mmbtu_per_unit",
                "fuel_consumed_mmbtu",
                "fuel_consumed_for_electricity_mmbtu",
                "net_generation_mwh",
                "data_maturity",
            ],
            "primary_key": [
                "plant_id_eia",
                "report_date",
                "nuclear_unit_id",
                "energy_source_code",
                "prime_mover_code",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia923"],
        "etl_group": "eia923",
    },
}
"""EIA-923 resource attributes organized by PUDL identifier (``resource.name``).

Keys are in alphabetical order.

See :func:`pudl.metadata.helpers.build_foreign_keys` for the expected format of
``foreign_key_rules``.
"""
