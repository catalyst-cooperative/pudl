"""Definitions of data tables primarily coming from EIA-860."""
from typing import Any, Dict

RESOURCE_METADATA: Dict[str, Dict[str, Any]] = {
    "boiler_generator_assn_eia860": {
        "description": "Associations between boilers and generators as reported in EIA-860 Schedule 6, Part A. Augmented with various heuristics within PUDL.",
        "schema": {
            "fields": [
                "plant_id_eia",
                "report_date",
                "generator_id",
                "boiler_id",
                "unit_id_eia",
                "unit_id_pudl",
                "bga_source",
            ],
            "primary_key": ["plant_id_eia", "report_date", "generator_id", "boiler_id"],
        },
        "field_namespace": "eia",
        "sources": ["eia860", "eia923"],
        "etl_group": "eia860",
    },
    "generators_eia860": {
        "description": "Annually varying generator attributes compiled from across EIA-860 and EIA-923 data.",
        "schema": {
            "fields": [
                "plant_id_eia",
                "generator_id",
                "utility_id_eia",
                "report_date",
                "operational_status_code",
                "operational_status",
                "ownership_code",
                "capacity_mw",
                "summer_capacity_mw",
                "summer_capacity_estimate",
                "winter_capacity_mw",
                "winter_capacity_estimate",
                "energy_source_code_1",
                "energy_source_code_2",
                "energy_source_code_3",
                "energy_source_code_4",
                "energy_source_code_5",
                "energy_source_code_6",
                "energy_source_1_transport_1",
                "energy_source_1_transport_2",
                "energy_source_1_transport_3",
                "energy_source_2_transport_1",
                "energy_source_2_transport_2",
                "energy_source_2_transport_3",
                "fuel_type_code_pudl",
                "multiple_fuels",
                "deliver_power_transgrid",
                "distributed_generation",
                "syncronized_transmission_grid",
                "turbines_num",
                "planned_modifications",
                "planned_net_summer_capacity_uprate_mw",
                "planned_net_winter_capacity_uprate_mw",
                "planned_uprate_date",
                "planned_net_summer_capacity_derate_mw",
                "planned_net_winter_capacity_derate_mw",
                "planned_derate_date",
                "planned_new_prime_mover_code",
                "planned_energy_source_code_1",
                "planned_repower_date",
                "other_planned_modifications",
                "other_modifications_date",
                "planned_retirement_date",
                "carbon_capture",
                "startup_source_code_1",
                "startup_source_code_2",
                "startup_source_code_3",
                "startup_source_code_4",
                "technology_description",
                "turbines_inverters_hydrokinetics",
                "time_cold_shutdown_full_load_code",
                "planned_new_capacity_mw",
                "cofire_fuels",
                "switch_oil_gas",
                "nameplate_power_factor",
                "minimum_load_mw",
                "uprate_derate_during_year",
                "uprate_derate_completed_date",
                "current_planned_operating_date",
                "summer_estimated_capability_mw",
                "winter_estimated_capability_mw",
                "retirement_date",
                "owned_by_non_utility",
                "reactive_power_output_mvar",
                "data_source",
            ],
            "primary_key": ["plant_id_eia", "generator_id", "report_date"],
            "foreign_key_rules": {
                "fields": [["plant_id_eia", "generator_id", "report_date"]],
                # TODO: Excluding monthly data tables since their report_date
                # values don't match up with generators_eia860, which is annual,
                # so non-january records violate the constraint.
                # See: https://github.com/catalyst-cooperative/pudl/issues/1196
                "exclude": [
                    "boiler_fuel_eia923",
                    "fuel_receipts_costs_eia923",
                    "generation_eia923",
                    "generation_fuel_eia923",
                ]
            },
        },
        "field_namespace": "eia",
        "sources": ["eia860", "eia923"],
        "etl_group": "eia860",
    },
    "ownership_eia860": {
        "description": "Generator Ownership, reported in EIA-860 Schedule 4. Includes only jointly or third-party owned generators.",
        "schema": {
            "fields": [
                "report_date",
                "utility_id_eia",
                "plant_id_eia",
                "generator_id",
                "owner_utility_id_eia",
                "owner_name",
                "owner_state",
                "owner_city",
                "owner_street_address",
                "owner_zip_code",
                "fraction_owned",
            ],
            "primary_key": [
                "report_date", "plant_id_eia", "generator_id", "owner_utility_id_eia"
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia860"],
        "etl_group": "eia860",
    },
    "plants_eia860": {
        "description": "Annually varying plant attributes, compiled from across all EIA-860 and EIA-923 data.",
        "schema": {
            "fields": [
                "plant_id_eia",
                "report_date",
                "ash_impoundment",
                "ash_impoundment_lined",
                "ash_impoundment_status",
                "datum",
                "energy_storage",
                "ferc_cogen_docket_no",
                "ferc_exempt_wholesale_generator_docket_no",
                "ferc_small_power_producer_docket_no",
                "liquefied_natural_gas_storage",
                "natural_gas_local_distribution_company",
                "natural_gas_storage",
                "natural_gas_pipeline_name_1",
                "natural_gas_pipeline_name_2",
                "natural_gas_pipeline_name_3",
                "nerc_region",
                "net_metering",
                "pipeline_notes",
                "regulatory_status_code",
                "respondent_frequency",
                "service_area",
                "transmission_distribution_owner_id",
                "transmission_distribution_owner_name",
                "transmission_distribution_owner_state",
                "utility_id_eia",
                "water_source",
            ],
            "primary_key": ["plant_id_eia", "report_date"],
            "foreign_key_rules": {
                "fields": [["plant_id_eia", "report_date"]],
                # TODO: Excluding monthly data tables since their report_date
                # values don't match up with plants_eia860, which is annual, so
                # non-january records fail.
                # See: https://github.com/catalyst-cooperative/pudl/issues/1196
                "exclude": [
                    "boiler_fuel_eia923",
                    "fuel_receipts_costs_eia923",
                    "generation_eia923",
                    "generation_fuel_eia923",
                    "generation_fuel_nuclear_eia923",
                ]
            },
        },
        "field_namespace": "eia",
        "sources": ["eia860", "eia923"],
        "etl_group": "eia860",
    },
    "utilities_eia860": {
        "description": "Annually varying utility attributes, compiled from all EIA data.",
        "schema": {
            "fields": [
                "utility_id_eia",
                "report_date",
                "street_address",
                "city",
                "state",
                "zip_code",
                "plants_reported_owner",
                "plants_reported_operator",
                "plants_reported_asset_manager",
                "plants_reported_other_relationship",
                "entity_type",
                "attention_line",
                "address_2",
                "zip_code_4",
                "contact_firstname",
                "contact_lastname",
                "contact_title",
                "phone_number",
                "phone_extension",
                "contact_firstname_2",
                "contact_lastname_2",
                "contact_title_2",
                "phone_number_2",
                "phone_extension_2",
            ],
            "primary_key": ["utility_id_eia", "report_date"],
            "foreign_key_rules": {
                "fields": [
                    ["utility_id_eia", "report_date"],
                    # Failing because this column is not harvested in the old
                    # system. TODO: re-enable when we switch to new system.
                    # https://github.com/catalyst-cooperative/pudl/issues/1196
                    # ["owner_utility_id_eia", "report_date"],
                ],
            },
        },
        "field_namespace": "eia",
        "sources": ["eia860", "eia923"],
        "etl_group": "eia860",
    },
}
"""
EIA-860 resource attributes organized by PUDL identifier (``resource.name``).

Keys are in alphabetical order.

See :func:`pudl.metadata.helpers.build_foreign_keys` for the expected format of
``foreign_key_rules``.
"""
