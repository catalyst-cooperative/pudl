"""Resource metadata for the allocate_net_gen tables."""
from typing import Any

AGG_FREQS = ["yearly", "monthly"]

RESOURCE_METADATA: dict[str, dict[str, Any]] = (
    {
        f"generation_fuel_by_generator_energy_source_{freq}_eia923": {
            "description": (
                f"{freq.title()} estimated net generation and fuel consumption "
                "associated with each combination of generator, energy source, and "
                "prime mover. First, the net electricity generation and fuel consumption "
                "reported in the EIA-923 generation fuel are allocated to individual "
                "generators. Then, these allocations are aggregated to unique generator, "
                "prime mover, and energy source code combinations. This process does not "
                "distinguish between primary and secondary energy_sources for generators. "
                "Net generation is allocated equally between energy source codes, so if a "
                "plant has multiple generators with the same prime_mover_code but different "
                "energy source codes the generation_fuel_eia923 records will be associated "
                "similarly between these two generators. Allocated net generation will still "
                "be proportional to each generator's net generation or capacity."
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
        }
        for freq in AGG_FREQS
    }
    | {
        f"generation_fuel_by_generator_{freq}_eia923": {
            "description": (
                f"{freq.title()} estimated net generation by generator. Based on net "
                "generation reported in the EIA-923 generation and generation_fuel tables."
                "The net electricity generation and fuel consumption reported in the EIA-923 "
                "generation fuel are allocated to individual generators. This process does not "
                "distinguish between primary and secondary energy_sources for generators. Net "
                "generation is allocated equally between energy source codes, so if a "
                "plant has multiple generators with the same prime_mover_code but different "
                "energy source codes the generation_fuel_eia923 records will be associated "
                "similarly between these two generators. Allocated net generation will still "
                "be proportional to each generator's net generation or capacity."
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
        }
        for freq in AGG_FREQS
    }
    | {
        "generation_fuel_by_generator_energy_source_owner_yearly_eia923": {
            "description": (
                "Yearly estimated net generation and fuel consumption for each generator, "
                "broken down by energy source, prime mover, and owner. Based on data "
                "reported in the EIA-923 generation and generation_fuel tables. Note "
                "that the utility_id_eia in this table refers to the OWNER of the generator, "
                "not the operator. To create these estimates, the net electricity generation "
                "and fuel consumption reported in the EIA-923 generation fuel are allocated "
                "to individual generators. Then, these allocations are aggregated to unique "
                "generator, prime mover, energy source code, and owner combinations. This "
                "process does not distinguish between primary and secondary energy_sources "
                "for generators. Net generation is allocated equally between energy source "
                "codes, so if a plant has multiple generators with the same prime_mover_code "
                "but different energy source codes the generation_fuel_eia923 records will be "
                "associated similarly between these two generators. Allocated net generation "
                "will still be proportional to each generator's net generation or capacity."
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
                    "utility_id_eia",
                    "ownership_record_type",
                ],
            },
            "field_namespace": "eia",
            "sources": ["eia923", "eia860"],
            "etl_group": "outputs",
        },
    }
)
