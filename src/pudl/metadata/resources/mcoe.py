"""Resource metadata for the MCOE (marginal cost of electricity) tables."""
from typing import Any

AGG_FREQS = ["yearly", "monthly"]

FILTERING_WARNING = (
    "Note that the values in this table are unfiltered and we "
    "expect some of the values are unreasonable and out of bounds."
    "This table should not be used without filtering values to within "
    "logical boundaries."
)
RESOURCE_METADATA: dict[str, dict[str, Any]] = (
    {
        f"heat_rate_by_unit_{freq}": {
            "description": (
                f"{freq.title()} heat rate estimates by generation unit. Generation "
                "units are identified by ``unit_id_pudl`` and are composed of a set of "
                f"interconnected boilers and generators. {FILTERING_WARNING}"
            ),
            "schema": {
                "fields": [
                    "report_date",
                    "plant_id_eia",
                    "unit_id_pudl",
                    "net_generation_mwh",
                    "fuel_consumed_for_electricity_mmbtu",
                    "heat_rate_mmbtu_mwh",
                ],
                "primary_key": [
                    "report_date",
                    "plant_id_eia",
                    "unit_id_pudl",
                ],
            },
            "field_namespace": "eia",
            "sources": ["eia923", "eia860"],
            "etl_group": "outputs",
        }
        for freq in AGG_FREQS
    }
    | {
        f"heat_rate_by_generator_{freq}": {
            "description": (
                f"{freq.title()} heat rate estimates by generator. These are actually "
                "just generation unit level heat rates, which have been broadcast "
                "across all constituent generator IDs, since heat rates really only "
                "have a well-defined meaning in the context of a generation unit."
                f"{FILTERING_WARNING}"
            ),
            "schema": {
                "fields": [
                    "report_date",
                    "plant_id_eia",
                    "unit_id_pudl",
                    "generator_id",
                    "heat_rate_mmbtu_mwh",
                    "fuel_type_code_pudl",
                    "fuel_type_count",
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
        f"capacity_factor_by_generator_{freq}": {
            "description": (
                f"{freq.title()} estimates of generator capacity factor. Capacity "
                "factor is calculated based on reported generator capacity and the "
                "allocated net generation reported in the generation and generation "
                f"fuel tables. {FILTERING_WARNING}"
            ),
            "schema": {
                "fields": [
                    "report_date",
                    "plant_id_eia",
                    "generator_id",
                    "net_generation_mwh",
                    "capacity_mw",
                    "capacity_factor",
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
        f"fuel_cost_by_generator_{freq}": {
            "description": (
                f"{freq.title()} estimate of per-generator fuel costs both per MMBTU "
                "and per MWh. These calculations are based on the allocation of net "
                "generation and fuel consumption as well as plant-level delivered fuel prices "
                "reported in the fuel receipts and cost table. The intermediary heat rate "
                "calculation depends on having the unit ID filled in, which means fuel cost "
                "coverage is low. The fuel costs are also currently aggregated to coarse fuel "
                "categories rather than using the more detailed energy source codes."
                f"{FILTERING_WARNING}"
            ),
            "schema": {
                "fields": [
                    "report_date",
                    "plant_id_eia",
                    "generator_id",
                    "unit_id_pudl",
                    "plant_name_eia",
                    "plant_id_pudl",
                    "utility_id_eia",
                    "utility_name_eia",
                    "utility_id_pudl",
                    "fuel_type_count",
                    "fuel_type_code_pudl",
                    "fuel_cost_from_eiaapi",
                    "fuel_cost_per_mmbtu",
                    "heat_rate_mmbtu_mwh",
                    "fuel_cost_per_mwh",
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
        f"mcoe_{freq}": {
            "description": (
                f"{freq.title()} generator capacity factor, heat rate, fuel cost per MMBTU and fuel cost "
                "per MWh. These calculations are based on the allocation of net generation reported on "
                "the basis of plant, prime mover and energy source to individual generators. Heat rates "
                "by generator-month are estimated by using allocated estimates for per-generator net "
                "generation and fuel consumption as well as the :ref:`boiler_fuel_eia923` table, which "
                "reports fuel consumed by boiler. Heat rates are necessary to estimate the amount of fuel "
                "consumed by a generation unit, and thus the fuel cost per MWh generated. Plant specific "
                "fuel prices are taken from the :ref:`fuel_receipts_costs_eia923` table, which only has "
                "~70% coverage, leading to some generators with heat rate estimates still lacking fuel "
                "cost estimates."
            ),
            "schema": {
                "fields": [
                    "plant_id_eia",
                    "generator_id",
                    "report_date",
                    "capacity_factor",
                    "fuel_cost_from_eiaapi",
                    "fuel_cost_per_mmbtu",
                    "fuel_cost_per_mwh",
                    "heat_rate_mmbtu_mwh",
                    "net_generation_mwh",
                    "total_fuel_cost",
                    "total_mmbtu",
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
        f"mcoe_generators_{freq}": {
            "description": (
                f"The marginal cost of energy table with {freq.title()} generator capacity factor, "
                "heat rate, fuel cost per MMBTU and fuel cost, but with additional potentially "
                "useful generator attributes merged on."
                "See the description in the MCOE table for more explanation of these calculations."
            ),
            "schema": {
                "fields": [
                    "plant_id_eia",
                    "generator_id",
                    "report_date",
                    "unit_id_pudl",
                    "plant_id_pudl",
                    "plant_name_eia",
                    "utility_id_eia",
                    "utility_id_pudl",
                    "utility_name_eia",
                    "technology_description",
                    "energy_source_code_1",
                    "prime_mover_code",
                    "generator_operating_date",
                    "generator_retirement_date",
                    "operational_status",
                    "capacity_mw",
                    "fuel_type_code_pudl",
                    "planned_generator_retirement_date",
                    "capacity_factor",
                    "fuel_cost_from_eiaapi",
                    "fuel_cost_per_mmbtu",
                    "fuel_cost_per_mwh",
                    "heat_rate_mmbtu_mwh",
                    "net_generation_mwh",
                    "total_fuel_cost",
                    "total_mmbtu",
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
)
