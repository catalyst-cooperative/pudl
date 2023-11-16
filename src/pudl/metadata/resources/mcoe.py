"""Resource metadata for the generator table with derived attributes."""
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
        f"_out_eia__{freq}_heat_rate_by_unit": {
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
                    "unit_heat_rate_mmbtu_per_mwh",
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
        f"_out_eia__{freq}_heat_rate_by_generator": {
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
                    "unit_heat_rate_mmbtu_per_mwh",
                    "fuel_type_code_pudl",
                    "fuel_type_count",
                    "prime_mover_code",
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
        f"_out_eia__{freq}_capacity_factor_by_generator": {
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
        f"_out_eia__{freq}_fuel_cost_by_generator": {
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
                    "unit_heat_rate_mmbtu_per_mwh",
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
        f"_out_eia__{freq}_derived_generator_attributes": {
            "description": (
                f"{freq.title()} generator capacity factor, heat rate, fuel cost per MMBTU and fuel cost "
                "per MWh. These calculations are based on the allocation of net generation reported on "
                "the basis of plant, prime mover and energy source to individual generators. Heat rates "
                "by generator-month are estimated by using allocated estimates for per-generator net "
                "generation and fuel consumption as well as the :ref:`core_eia923__monthly_boiler_fuel` table, which "
                "reports fuel consumed by boiler. Heat rates are necessary to estimate the amount of fuel "
                "consumed by a generation unit, and thus the fuel cost per MWh generated. Plant specific "
                "fuel prices are taken from the :ref:`core_eia923__monthly_fuel_receipts_costs` table, which only has "
                "~70% coverage, leading to some generators with heat rate estimates still lacking fuel "
                "cost estimates."
            ),
            "schema": {
                "fields": [
                    "plant_id_eia",
                    "generator_id",
                    "unit_id_pudl",
                    "report_date",
                    "capacity_factor",
                    "fuel_cost_from_eiaapi",
                    "fuel_cost_per_mmbtu",
                    "fuel_cost_per_mwh",
                    "unit_heat_rate_mmbtu_per_mwh",
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
        f"out_eia__{freq}_generators": {
            "description": (
                f"{freq.title()} all generator attributes including calculated capacity factor, "
                "heat rate, fuel cost per MMBTU and fuel cost."
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
                    "unit_heat_rate_mmbtu_per_mwh",
                    "net_generation_mwh",
                    "total_fuel_cost",
                    "total_mmbtu",
                    "associated_combined_heat_power",
                    "bga_source",
                    "bypass_heat_recovery",
                    "carbon_capture",
                    "city",
                    "cofire_fuels",
                    "county",
                    "current_planned_generator_operating_date",
                    "data_maturity",
                    "deliver_power_transgrid",
                    "distributed_generation",
                    "duct_burners",
                    "energy_source_1_transport_1",
                    "energy_source_1_transport_2",
                    "energy_source_1_transport_3",
                    "energy_source_2_transport_1",
                    "energy_source_2_transport_2",
                    "energy_source_2_transport_3",
                    "energy_source_code_2",
                    "energy_source_code_3",
                    "energy_source_code_4",
                    "energy_source_code_5",
                    "energy_source_code_6",
                    "energy_storage_capacity_mwh",
                    "ferc_qualifying_facility",
                    "fluidized_bed_tech",
                    "fuel_type_count",
                    "latitude",
                    "longitude",
                    "minimum_load_mw",
                    "multiple_fuels",
                    "nameplate_power_factor",
                    "net_capacity_mwdc",
                    "operating_switch",
                    "operational_status_code",
                    "original_planned_generator_operating_date",
                    "other_combustion_tech",
                    "other_modifications_date",
                    "other_planned_modifications",
                    "owned_by_non_utility",
                    "ownership_code",
                    "planned_derate_date",
                    "planned_energy_source_code_1",
                    "planned_modifications",
                    "planned_net_summer_capacity_derate_mw",
                    "planned_net_summer_capacity_uprate_mw",
                    "planned_net_winter_capacity_derate_mw",
                    "planned_net_winter_capacity_uprate_mw",
                    "planned_new_capacity_mw",
                    "planned_new_prime_mover_code",
                    "planned_repower_date",
                    "planned_uprate_date",
                    "previously_canceled",
                    "pulverized_coal_tech",
                    "reactive_power_output_mvar",
                    "rto_iso_lmp_node_id",
                    "rto_iso_location_wholesale_reporting_id",
                    "solid_fuel_gasification",
                    "startup_source_code_1",
                    "startup_source_code_2",
                    "startup_source_code_3",
                    "startup_source_code_4",
                    "state",
                    "stoker_tech",
                    "street_address",
                    "subcritical_tech",
                    "summer_capacity_estimate",
                    "summer_capacity_mw",
                    "summer_estimated_capability_mw",
                    "supercritical_tech",
                    "switch_oil_gas",
                    "syncronized_transmission_grid",
                    "time_cold_shutdown_full_load_code",
                    "timezone",
                    "topping_bottoming_code",
                    "turbines_inverters_hydrokinetics",
                    "turbines_num",
                    "ultrasupercritical_tech",
                    "uprate_derate_completed_date",
                    "uprate_derate_during_year",
                    "winter_capacity_estimate",
                    "winter_capacity_mw",
                    "winter_estimated_capability_mw",
                    "zip_code",
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
