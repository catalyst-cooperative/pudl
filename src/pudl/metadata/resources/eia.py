"""Definitions of data tables primarily coming from EIA 860/861/923."""
from typing import Any, Dict

from pudl.metadata.codes import (CONTRACT_TYPES_EIA, ENERGY_SOURCES_EIA,
                                 FUEL_TRANSPORTATION_MODES_EIA,
                                 FUEL_TYPES_AER_EIA, PRIME_MOVERS_EIA,
                                 SECTOR_CONSOLIDATED_EIA)

RESOURCE_METADATA: Dict[str, Dict[str, Any]] = {
    "boiler_fuel_eia923": {
        "schema": {
            "fields": [
                "plant_id_eia",
                "boiler_id",
                "energy_source_code",
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
                "report_date"
            ],
        },
        "sources": ["eia923"],
    },
    "boiler_generator_assn_eia860": {
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
        "sources": ["eia860"],
    },
    "boilers_entity_eia": {
        "schema": {
            "fields": ["plant_id_eia", "boiler_id", "prime_mover_code"],
            "primary_key": ["plant_id_eia", "boiler_id"],
            "foreign_key_rules": {"fields": [["plant_id_eia", "boiler_id"]]},
        },
    },
    "coalmine_eia923": {
        "schema": {
            "fields": [
                "mine_id_pudl",
                "mine_name",
                "mine_type",
                "state",
                "county_id_fips",
                "mine_id_msha",
            ],
            "primary_key": ["mine_id_pudl"],
            "foreign_key_rules": {"fields": [["mine_id_pudl"]]},
        },
        "sources": ["eia923"],
    },
    "contract_types_eia": {
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["contract_type_code"]]}
        },
        "encoder": CONTRACT_TYPES_EIA,
        "sources": ["eia923"],
    },
    "datasets": {
        "schema": {"fields": ["datasource", "active"], "primary_key": ["datasource"]},
    },
    "energy_sources_eia": {
        "schema": {
            "fields": [
                "code",
                "label",
                "fuel_units",
                "min_fuel_mmbtu_per_unit",
                "max_fuel_mmbtu_per_unit",
                "fuel_group_eia",
                "fuel_derived_from",
                "fuel_phase",
                "fuel_type_code_pudl",
                "description",
            ],
            "primary_key": ["code"],
            "foreign_key_rules": {
                "fields": [
                    ["energy_source_code"],
                    ["energy_source_code_1"],
                    ["energy_source_code_2"],
                    ["energy_source_code_3"],
                    ["energy_source_code_4"],
                    ["energy_source_code_5"],
                    ["energy_source_code_6"],
                    ["startup_source_code_1"],
                    ["startup_source_code_2"],
                    ["startup_source_code_3"],
                    ["startup_source_code_4"],
                    ["planned_energy_source_code_1"],
                ],
            },
        },
        "encoder": ENERGY_SOURCES_EIA,
        "sources": ["eia923"],
    },
    "fuel_receipts_costs_eia923": {
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
            ],
        },
        "sources": ["eia923"],
    },
    "fuel_transportation_modes_eia": {
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {
                "fields": [
                    ["energy_source_1_transport_1"],
                    ["energy_source_1_transport_2"],
                    ["energy_source_1_transport_3"],
                    ["energy_source_2_transport_1"],
                    ["energy_source_2_transport_2"],
                    ["energy_source_2_transport_3"],
                    ["primary_transportation_mode_code"],
                    ["secondary_transportation_mode_code"],
                ]
            }
        },
        "encoder": FUEL_TRANSPORTATION_MODES_EIA,
        "sources": ["eia923"],
    },
    "fuel_types_aer_eia": {
        "schema": {
            "fields": ["code", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["fuel_type_code_aer"]]},
        },
        "encoder": FUEL_TYPES_AER_EIA,
        "sources": ["eia923"],
    },
    "generation_eia923": {
        "schema": {
            "fields": [
                "plant_id_eia",
                "generator_id",
                "report_date",
                "net_generation_mwh",
            ],
            "primary_key": ["plant_id_eia", "generator_id", "report_date"],
        },
        "sources": ["eia923"],
    },
    "generation_fuel_eia923": {
        "description": "Monthly electricity generation and fuel consumption reported for each combination of fuel and prime mover within a plant. Note that this table does not include data from nuclear plants as they report at the generation unit level, rather than the plant level. See the generation_fuel_nuclear_eia923 table for nuclear electricity generation and fuel consumption.",

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
            ],
            "primary_key": [
                "plant_id_eia",
                "report_date",
                "energy_source_code",
                "prime_mover_code"
            ],
        },
        "sources": ["eia923"],
    },
    "generation_fuel_nuclear_eia923": {
        "description": "Monthly electricity generation and fuel consumption reported for each combination of fuel and prime mover within a nuclear generation unit.",
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
            ],
            "primary_key": [
                "plant_id_eia",
                "report_date",
                "nuclear_unit_id",
                "energy_source_code",
                "prime_mover_code"
            ],
        },
        "sources": ["eia923"],
    },
    "generators_eia860": {
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
        "sources": ["eia860"],
    },
    "generators_entity_eia": {
        "schema": {
            "fields": [
                "plant_id_eia",
                "generator_id",
                "prime_mover_code",
                "duct_burners",
                "operating_date",
                "topping_bottoming_code",
                "solid_fuel_gasification",
                "pulverized_coal_tech",
                "fluidized_bed_tech",
                "subcritical_tech",
                "supercritical_tech",
                "ultrasupercritical_tech",
                "stoker_tech",
                "other_combustion_tech",
                "bypass_heat_recovery",
                "rto_iso_lmp_node_id",
                "rto_iso_location_wholesale_reporting_id",
                "associated_combined_heat_power",
                "original_planned_operating_date",
                "operating_switch",
                "previously_canceled",
            ],
            "primary_key": ["plant_id_eia", "generator_id"],
            "foreign_key_rules": {"fields": [["plant_id_eia", "generator_id"]]},
        },
    },
    "natural_gas_transport_eia923": {
        "schema": {
            "fields": ["code", "status"],
            "primary_key": ["code"]
        },
        "sources": ["eia923"],
    },
    "ownership_eia860": {
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
        "sources": ["eia860"],
    },
    "plants_eia": {
        "schema": {
            "fields": ["plant_id_eia", "plant_name_eia", "plant_id_pudl"],
            "primary_key": ["plant_id_eia"],
        },
    },
    "plants_eia860": {
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
        "sources": ["eia860"],
    },
    "plants_entity_eia": {
        "schema": {
            "fields": [
                "plant_id_eia",
                "plant_name_eia",
                "balancing_authority_code_eia",
                "balancing_authority_name_eia",
                "city",
                "county",
                "ferc_cogen_status",
                "ferc_exempt_wholesale_generator",
                "ferc_small_power_producer",
                "grid_voltage_kv",
                "grid_voltage_2_kv",
                "grid_voltage_3_kv",
                "iso_rto_code",
                "latitude",
                "longitude",
                "primary_purpose_id_naics",
                "sector_name_eia",
                "sector_id_eia",
                "state",
                "street_address",
                "zip_code",
                "timezone",
            ],
            "primary_key": ["plant_id_eia"],
            "foreign_key_rules": {
                "fields": [["plant_id_eia"]],
                # Excluding plants_eia because it's static and manually compiled
                # so it has plants from *all* years of data, even when only a
                # restricted set of data is processed, leading to constraint
                # violations.
                # See: https://github.com/catalyst-cooperative/pudl/issues/1196
                "exclude": ["plants_eia"],
            },
        },
    },
    "prime_movers_eia": {
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {
                "fields": [
                    ["prime_mover_code"],
                    ["planned_new_prime_mover_code"],
                ]
            },
        },
        "encoder": PRIME_MOVERS_EIA,
        "sources": ["eia923", "eia860"],
    },
    "sector_consolidated_eia": {
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["sector_id_eia"]]},
        },
        "encoder": SECTOR_CONSOLIDATED_EIA,
        "sources": ["eia923"],
    },
    "utilities_eia": {
        "schema": {
            "fields": ["utility_id_eia", "utility_name_eia", "utility_id_pudl"],
            "primary_key": ["utility_id_eia"],
        },
    },
    "utilities_eia860": {
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
        "sources": ["eia860"],
    },
    "utilities_entity_eia": {
        "schema": {
            "fields": ["utility_id_eia", "utility_name_eia"],
            "primary_key": ["utility_id_eia"],
            "foreign_key_rules": {
                "fields": [
                    ["utility_id_eia"],
                    # Results in constraint failures because this column is not
                    # harvested in the old system. See:
                    # https://github.com/catalyst-cooperative/pudl/issues/1196
                    # ["owner_utility_id_eia"]
                ],
                # Excluding utilities_eia b/c it's static and manually compiled
                # so it has utilities from *all* years of data, even when only a
                # restricted set of data is processed, leading to constraint
                # violations.
                # See: https://github.com/catalyst-cooperative/pudl/issues/1196
                "exclude": ["utilities_eia"],
            },
        },
    },
}
"""
EIA (860/861/923) resource attributes by PUDL identifier (``resource.name``).

Keys are in alphabetical order.

See :func:`pudl.metadata.helpers.build_foreign_keys` for the expected format of
``foreign_key_rules``.
"""
