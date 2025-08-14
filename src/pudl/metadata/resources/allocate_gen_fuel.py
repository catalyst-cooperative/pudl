"""Resource metadata for the allocate_gen_fuel tables."""

from typing import Any

AGG_FREQS = ["yearly", "monthly"]

USAGE_WARNING_DRAWBACK = {
    "type": "custom",
    "description": "This downscaling process used to create this table does not distinguish between primary and secondary energy_sources for generators (see below for implications).",
}
KNOWN_DRAWBACKS_DESCRIPTION = (
    "This process does not distinguish between primary and secondary energy_sources for generators. "
    "Net generation is allocated equally between energy source codes, so if a "
    "plant has multiple generators with the same prime_mover_code but different "
    "energy source codes the core_eia923__monthly_generation_fuel records will be associated "
    "similarly between these two generators. Allocated net generation will still "
    "be proportional to each generator's net generation or capacity."
)

RESOURCE_METADATA: dict[str, dict[str, Any]] = (
    {
        f"out_eia923__{freq}_generation_fuel_by_generator_energy_source": {
            "description": {
                "additional_summary_text": "of estimated net generation and fuel consumption associated with each combination of generator, energy source, and prime mover.",
                "additional_source_text": "(Schedule 3)",
                "usage_warnings": [
                    "estimated_values",
                    USAGE_WARNING_DRAWBACK,
                    "month_as_date",
                    {
                        "type": "custom",
                        "description": "A small number of respondents only report annual fuel consumption, and all of it is reported in December.",
                    },
                ],
                "additional_details_text": (
                    f"First, the net electricity generation and fuel consumption "
                    "reported in the EIA-923 generation fuel are allocated to individual "
                    "generators. Then, these allocations are aggregated to unique generator, "
                    f"prime mover, and energy source code combinations.\n\n"
                    f"{KNOWN_DRAWBACKS_DESCRIPTION}"
                ),
            },
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
        f"out_eia923__{freq}_generation_fuel_by_generator": {
            "description": {
                "additional_summary_text": "of estimated net generation and fuel consumption by generator.",
                "additional_source_text": "(Schedule 3)",
                "usage_warnings": [
                    "estimated_values",
                    USAGE_WARNING_DRAWBACK,
                    "month_as_date",
                    {
                        "type": "custom",
                        "description": "A small number of respondents only report annual fuel consumption, and all of it is reported in December.",
                    },
                ],
                "additional_details_text": (
                    "Based on allocating net electricity generation and fuel consumption reported "
                    "in the EIA-923 generation and generation_fuel tables to individual generators.\n\n"
                    f"{KNOWN_DRAWBACKS_DESCRIPTION}"
                ),
            },
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
        "out_eia923__yearly_generation_fuel_by_generator_energy_source_owner": {
            "description": {
                "additional_summary_text": (
                    "of estimated net generation and fuel consumption for each generator, "
                    "associated with each combination of generator, energy source, prime mover, and owner."
                ),
                "additional_source_text": "(Schedule 3)",
                "usage_warnings": [
                    "estimated_values",
                    USAGE_WARNING_DRAWBACK,
                    "month_as_date",
                ],
                "additional_details_text": (
                    "First, the net electricity generation and fuel consumption "
                    "reported in the EIA-923 generation fuel are allocated to individual "
                    "generators. Then, these allocations are aggregated to unique generator, "
                    "prime mover, energy source code, and owner combinations. Note that the "
                    "utility_id_eia in this table refers to the OWNER of the generator, not the "
                    f"operator.\n\n{KNOWN_DRAWBACKS_DESCRIPTION}"
                ),
            },
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
