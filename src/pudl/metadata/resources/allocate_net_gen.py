"""Resource metadata for the allocate_net_gen tables."""
from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "generation_fuel_by_generator_energy_source_monthly_eia923": {
        "description": (
            "Monthly estimated net generation and fuel consumption associated with "
            "each combination of generator, energy source, and prime mover."
        ),
        "schema": {
            "fields": [
                "report_date",
                "plant_id_eia",
                "generator_id",
                "prime_mover_code",
                "energy_source_code",
                "energy_source_code_num",
                "net_generation_mwh",
                "fuel_consumed_mmbtu",
                "fuel_consumed_for_electricity_mmbtu",
            ],
            "primary_key": [
                "report_date",
                "plant_id_eia",
                "generator_id",
                "prime_mover_code",
                "energy_source_code",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia923", "eia860"],
        "etl_group": "outputs",
    },
    "generation_fuel_by_generator_energy_source_yearly_eia923": {
        "description": (
            "Yearly estimated net generation and fuel consumption associated with "
            "each combination of generator, energy source, and prime mover."
        ),
        "schema": {
            "fields": [
                "report_date",
                "plant_id_eia",
                "generator_id",
                "prime_mover_code",
                "energy_source_code",
                "energy_source_code_num",
                "net_generation_mwh",
                "fuel_consumed_mmbtu",
                "fuel_consumed_for_electricity_mmbtu",
            ],
            "primary_key": [
                "report_date",
                "plant_id_eia",
                "generator_id",
                "prime_mover_code",
                "energy_source_code",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia923", "eia860"],
        "etl_group": "outputs",
    },
    "generation_fuel_by_generator_monthly_eia923": {
        "description": (
            "Monthly estimated net generation by generator. Based on net "
            "generation reported in the EIA-923 generation and generation_fuel tables."
        ),
        "schema": {
            "fields": [
                "report_date",
                "plant_id_eia",
                "plant_id_pudl",
                "plant_name_eia",
                "utility_id_eia",
                "utility_id_pudl",
                "utility_name_eia",
                "generator_id",
                "unit_id_pudl",
                "fuel_consumed_for_electricity_mmbtu",
                "fuel_consumed_mmbtu",
                "net_generation_mwh",
            ],
            "primary_key": [
                "report_date",
                "plant_id_eia",
                "generator_id",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia923", "eia860"],
        "etl_group": "outputs",
    },
    "generation_fuel_by_generator_yearly_eia923": {
        "description": (
            "Yearly estimated net generation and fuel consumption by generator. Based "
            "on net generation reported in the EIA-923 generation and generation_fuel "
            "tables."
        ),
        "schema": {
            "fields": [
                "report_date",
                "plant_id_eia",
                "plant_id_pudl",
                "plant_name_eia",
                "utility_id_eia",
                "utility_id_pudl",
                "utility_name_eia",
                "generator_id",
                "unit_id_pudl",
                "fuel_consumed_for_electricity_mmbtu",
                "fuel_consumed_mmbtu",
                "net_generation_mwh",
            ],
            "primary_key": [
                "report_date",
                "plant_id_eia",
                "generator_id",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia923", "eia860"],
        "etl_group": "outputs",
    },
    "generation_fuel_by_generator_energy_source_owner_monthly_eia923": {
        "description": (
            "Monthly estimated net generation and fuel consumption for each generator, "
            "broken down by energy source, prime mover, and owner. Based on data "
            "reported in the EIA-923 generation and generation_fuel tables."
        ),
        "schema": {
            "fields": [
                "report_date",
                "plant_id_eia",
                "generator_id",
                "prime_mover_code",
                "energy_source_code",
                "utility_id_eia",
                "ownership_record_type",
                "fraction_owned",
                "capacity_mw",
                "energy_source_code_num",
                "net_generation_mwh",
                "fuel_consumed_mmbtu",
                "fuel_consumed_for_electricity_mmbtu",
            ],
            "primary_key": [
                "report_date",
                "plant_id_eia",
                "generator_id",
                "prime_mover_code",
                "energy_source_code",
                "utility_id_eia",  # This is the OWNER not the operator.
                "ownership_record_type",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia923", "eia860"],
        "etl_group": "outputs",
    },
    "generation_fuel_by_generator_energy_source_owner_yearly_eia923": {
        "description": (
            "Yearly estimated net generation and fuel consumption for each generator, "
            "broken down by energy source, prime mover, and owner. Based on data "
            "reported in the EIA-923 generation and generation_fuel tables."
        ),
        "schema": {
            "fields": [
                "report_date",
                "plant_id_eia",
                "generator_id",
                "prime_mover_code",
                "energy_source_code",
                "utility_id_eia",
                "ownership_record_type",
                "fraction_owned",
                "capacity_mw",
                "energy_source_code_num",
                "net_generation_mwh",
                "fuel_consumed_mmbtu",
                "fuel_consumed_for_electricity_mmbtu",
            ],
            "primary_key": [
                "report_date",
                "plant_id_eia",
                "generator_id",
                "prime_mover_code",
                "energy_source_code",
                "utility_id_eia",  # This is the OWNER not the operator.
                "ownership_record_type",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia923", "eia860"],
        "etl_group": "outputs",
    },
}
