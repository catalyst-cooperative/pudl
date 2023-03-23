"""Definitions of data tables primarily coming from EIA 860/861/923."""
from typing import Any

from pudl.metadata.codes import CODE_METADATA

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "balancing_authorities_eia": {
        "description": "A coding table describing balancing authorities in EIA-860 and EIA-923.",
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["balancing_authority_code_eia"]]},
        },
        "encoder": CODE_METADATA["balancing_authorities_eia"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "boilers_entity_eia": {
        "description": "Static boiler attributes compiled from the EIA-860 and EIA-923 data.",
        "schema": {
            "fields": [
                "plant_id_eia",
                "boiler_id",
                "boiler_manufacturer",
                "boiler_manufacturer_code",
            ],
            "primary_key": ["plant_id_eia", "boiler_id"],
            "foreign_key_rules": {"fields": [["plant_id_eia", "boiler_id"]]},
        },
        "sources": ["eia860", "eia923"],
        "etl_group": "entity_eia",
        "field_namespace": "eia",
    },
    "boiler_generator_assn_types_eia": {
        "description": "A coding table describing different types of boiler-generator associations in the EIA-860.",
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["boiler_generator_assn_type_code"]]},
        },
        "encoder": CODE_METADATA["boiler_generator_assn_types_eia"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "boiler_status_eia": {
        "description": "A coding table describing different types of boiler status in the EIA-860.",
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["boiler_status"]]},
        },
        "encoder": CODE_METADATA["boiler_status_eia"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "boiler_types_eia": {
        "description": "A coding table describing different types of boiler regulatory types in the EIA-860.",
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["boiler_type"]]},
        },
        "encoder": CODE_METADATA["boiler_types_eia"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "environmental_equipment_manufacturers_eia": {
        "description": "A coding table describing manufacturers of boilers and environmental control equipment in the EIA-860.",
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {
                "fields": [
                    ["boiler_manufacturer_code"],
                    ["nox_control_manufacturer_code"],
                ]
            },
        },
        "encoder": CODE_METADATA["environmental_equipment_manufacturers_eia"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "firing_types_eia": {
        "description": "A coding table describing different boiler firing types in the EIA-860.",
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {
                "fields": [["firing_type_1"], ["firing_type_2"], ["firing_type_3"]]
            },
        },
        "encoder": CODE_METADATA["firing_types_eia"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "nox_compliance_strategies_eia": {
        "description": "A coding table describing different compliance strategies used to control nitrogen oxide in the EIA-860.",
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {
                "fields": [
                    ["nox_control_existing_caaa_compliance_strategy_1"],
                    ["nox_control_existing_caaa_compliance_strategy_2"],
                    ["nox_control_existing_caaa_compliance_strategy_3"],
                    ["nox_control_out_of_compliance_strategy_1"],
                    ["nox_control_out_of_compliance_strategy_2"],
                    ["nox_control_out_of_compliance_strategy_3"],
                    ["nox_control_planned_caaa_compliance_strategy_1"],
                    ["nox_control_planned_caaa_compliance_strategy_2"],
                    ["nox_control_planned_caaa_compliance_strategy_3"],
                ]
            },
        },
        "encoder": CODE_METADATA["nox_compliance_strategies_eia"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "nox_control_status_eia": {
        "description": "A coding table describing the operational status of nitrogen oxide control units associated with boilers in the EIA-860 data.",
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {
                "fields": [
                    ["nox_control_status_code"],
                ]
            },
        },
        "encoder": CODE_METADATA["nox_control_status_eia"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "nox_units_eia": {
        "description": "A coding table describing different units of measurement for nitrogen oxide in the EIA-860.",
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["unit_nox"]]},
        },
        "encoder": CODE_METADATA["nox_units_eia"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "mercury_compliance_strategies_eia": {
        "description": "A coding table describing different compliance strategies used to control mercury in the EIA-860.",
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {
                "fields": [
                    ["mercury_control_proposed_strategy_1"],
                    ["mercury_control_proposed_strategy_2"],
                    ["mercury_control_proposed_strategy_3"],
                    ["mercury_control_existing_strategy_1"],
                    ["mercury_control_existing_strategy_2"],
                    ["mercury_control_existing_strategy_3"],
                    ["mercury_control_existing_strategy_4"],
                    ["mercury_control_existing_strategy_5"],
                    ["mercury_control_existing_strategy_6"],
                ]
            },
        },
        "encoder": CODE_METADATA["mercury_compliance_strategies_eia"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "averaging_periods_eia": {
        "description": "A coding table describing the averaging period specified by emissions statutes and regulation for in the EIA-860.",
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {
                "fields": [["period_nox"], ["period_particulate"], ["period_so2"]]
            },
        },
        "encoder": CODE_METADATA["averaging_periods_eia"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "particulate_compliance_strategies_eia": {
        "description": "A coding table describing different compliance strategies used to control particulate matter in the EIA-860.",
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {
                "fields": [
                    ["particulate_control_out_of_compliance_strategy_1"],
                    ["particulate_control_out_of_compliance_strategy_2"],
                    ["particulate_control_out_of_compliance_strategy_3"],
                ]
            },
        },
        "encoder": CODE_METADATA["particulate_compliance_strategies_eia"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "particulate_units_eia": {
        "description": "A coding table describing different units of measurement for particulate matter in the EIA-860.",
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["unit_particulate"]]},
        },
        "encoder": CODE_METADATA["particulate_units_eia"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "regulations_eia": {
        "description": "A coding table describing the different levels of statutes and codes under which boilers operate in the EIA-860.",
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {
                "fields": [
                    ["regulation_nox"],
                    ["regulation_particulate"],
                    ["regulation_so2"],
                    ["regulation_mercury"],
                ]
            },
        },
        "encoder": CODE_METADATA["regulations_eia"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "so2_compliance_strategies_eia": {
        "description": "A coding table describing different compliance strategies used to control sulfur dioxide in the EIA-860.",
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {
                "fields": [
                    ["so2_control_existing_caaa_compliance_strategy_1"],
                    ["so2_control_existing_caaa_compliance_strategy_2"],
                    ["so2_control_existing_caaa_compliance_strategy_3"],
                    ["so2_control_out_of_compliance_strategy_1"],
                    ["so2_control_out_of_compliance_strategy_2"],
                    ["so2_control_out_of_compliance_strategy_3"],
                    ["so2_control_planned_caaa_compliance_strategy_1"],
                    ["so2_control_planned_caaa_compliance_strategy_2"],
                    ["so2_control_planned_caaa_compliance_strategy_3"],
                ]
            },
        },
        "encoder": CODE_METADATA["so2_compliance_strategies_eia"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "wet_dry_bottom_eia": {
        "description": "A coding table describing whether boiler has a wet or dry bottom in the EIA-860.",
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["wet_dry_bottom"]]},
        },
        "encoder": CODE_METADATA["wet_dry_bottom_eia"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "so2_units_eia": {
        "description": "A coding table describing different units of measurement for sulfur dioxide in the EIA-860.",
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["unit_so2"]]},
        },
        "encoder": CODE_METADATA["so2_units_eia"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "steam_plant_types_eia": {
        "description": "A coding table describing different types of steam plants in the EIA-860.",
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["steam_plant_type_code"]]},
        },
        "encoder": CODE_METADATA["steam_plant_types_eia"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "reporting_frequencies_eia": {
        "description": "A coding table describing different types of reporting frequencies in plants in the EIA-923.",
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["reporting_frequency_code"]]},
        },
        "encoder": CODE_METADATA["reporting_frequencies_eia"],
        "sources": ["eia923"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "coalmine_types_eia": {
        "description": "A coding table describing different types of coalmines reported as fuel sources in the EIA-923.",
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["mine_type_code"]]},
        },
        "encoder": CODE_METADATA["coalmine_types_eia"],
        "sources": ["eia923"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "contract_types_eia": {
        "description": "A coding table describing the various types of fuel supply contracts reported in EIA-923.",
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["contract_type_code"]]},
        },
        "encoder": CODE_METADATA["contract_types_eia"],
        "sources": ["eia923"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "operational_status_eia": {
        "description": "Codes and metadata pertaining to operational status reported to EIA. Compiled from EIA-860 instructions and EIA-923 file layout spreadsheets.",
        "schema": {
            "fields": [
                "code",
                "label",
                "description",
                "operational_status",
            ],
            "primary_key": ["code"],
            "foreign_key_rules": {
                "fields": [["operational_status_code"]],
            },
        },
        "encoder": CODE_METADATA["operational_status_eia"],
        "sources": ["eia860", "eia923"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "data_maturities": {
        "description": "Level of maturities of data records. Some data sources report less-than-final data. PUDL sometimes includes this data, but use at your own risk.",
        "schema": {
            "fields": ["code", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {
                "fields": [["data_maturity"]],
            },
        },
        "encoder": CODE_METADATA["data_maturities"],
        "sources": ["eia860", "eia923"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "energy_sources_eia": {
        "description": "Codes and metadata pertaining to energy sources reported to EIA. Compiled from EIA-860 instructions and EIA-923 file layout spreadsheets.",
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
                    ["boiler_fuel_code_1"],
                    ["boiler_fuel_code_2"],
                    ["boiler_fuel_code_3"],
                    ["boiler_fuel_code_4"],
                ],
            },
        },
        "encoder": CODE_METADATA["energy_sources_eia"],
        "sources": ["eia860", "eia923"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "entity_types_eia": {
        "description": "Descriptive labels for EIA entity type and ownership codes, taken from the EIA-861 form instructions, valid through 2023-05-31.",
        "schema": {"fields": ["code", "label", "description"], "primary_key": ["code"]},
        "encoder": CODE_METADATA["entity_types_eia"],
        "sources": ["eia861"],
        "etl_group": "static_eia_disabled",  # currently not being loaded into the db
        "field_namespace": "eia",
    },
    "fuel_transportation_modes_eia": {
        "description": "Long descriptions of the fuel transportation modes reported in the EIA-860 and EIA-923.",
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
            },
        },
        "encoder": CODE_METADATA["fuel_transportation_modes_eia"],
        "sources": ["eia860", "eia923"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "fuel_types_aer_eia": {
        "description": "Descriptive labels for aggregated fuel types used in the Annual Energy Review. See EIA-923 Fuel Code table for additional information.",
        "schema": {
            "fields": ["code", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["fuel_type_code_aer"]]},
        },
        "encoder": CODE_METADATA["fuel_types_aer_eia"],
        "sources": ["eia923"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "generators_entity_eia": {
        "description": "Static generator attributes compiled from across the EIA-860 and EIA-923 data.",
        "schema": {
            "fields": [
                "plant_id_eia",
                "generator_id",
                "duct_burners",
                "generator_operating_date",
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
                "original_planned_generator_operating_date",
                "operating_switch",
                "previously_canceled",
            ],
            "primary_key": ["plant_id_eia", "generator_id"],
            "foreign_key_rules": {"fields": [["plant_id_eia", "generator_id"]]},
        },
        "sources": ["eia860", "eia923"],
        "etl_group": "entity_eia",
        "field_namespace": "eia",
    },
    "momentary_interruptions_eia": {
        "description": "A coding table for utility definitions of momentary service interruptions.",
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["momentary_interruption_definition"]]},
        },
        "encoder": CODE_METADATA["momentary_interruptions_eia"],
        "sources": ["eia861"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "plants_eia": {
        "description": "Association between EIA Plant IDs and manually assigned PUDL Plant IDs",
        "schema": {
            "fields": ["plant_id_eia", "plant_name_eia", "plant_id_pudl"],
            "primary_key": ["plant_id_eia"],
        },
        "sources": ["eia860", "eia923"],
        "etl_group": "glue",
        "field_namespace": "eia",
    },
    "plants_entity_eia": {
        "description": "Static plant attributes, compiled from across all EIA-860 and EIA-923 data.",
        "schema": {
            "fields": [
                "plant_id_eia",
                "plant_name_eia",
                "city",
                "county",
                "latitude",
                "longitude",
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
        "sources": ["eia860", "eia923"],
        "etl_group": "entity_eia",
        "field_namespace": "eia",
    },
    "plant_parts_eia": {
        "description": "Output table with the aggregation of all EIA plant parts. For use with matching to FERC 1.",
        "schema": {
            "fields": [
                "plant_id_eia",
                "report_date",
                "plant_part",
                "generator_id",
                "unit_id_pudl",
                "prime_mover_code",
                "energy_source_code_1",
                "technology_description",
                "ferc_acct_name",
                "utility_id_eia",
                "true_gran",
                "appro_part_label",
                "appro_record_id_eia",
                "capacity_eoy_mw",
                "capacity_factor",
                "capacity_mw",
                "construction_year",
                "fraction_owned",
                "fuel_cost_per_mmbtu",
                "fuel_cost_per_mwh",
                "fuel_type_code_pudl",
                "generator_retirement_date",
                "heat_rate_mmbtu_mwh",
                "installation_year",
                "net_generation_mwh",
                "generator_operating_year",
                "operational_status",
                "operational_status_pudl",
                "ownership_record_type",
                "ownership_dupe",
                "planned_generator_retirement_date",
                "plant_id_pudl",
                "plant_name_eia",
                "plant_name_ppe",
                "plant_part_id_eia",
                "record_count",
                "total_fuel_cost",
                "total_mmbtu",
                "utility_id_pudl",
                "report_year",
                "plant_id_report_year",
            ]
        },
        "sources": ["eia860", "eia923"],
        "etl_group": "outputs",
        "field_namespace": "ppe",
    },
    "prime_movers_eia": {
        "description": "Long descriptions explaining the short prime mover codes reported in the EIA-860 and EIA-923.",
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
        "encoder": CODE_METADATA["prime_movers_eia"],
        "sources": ["eia923", "eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "sector_consolidated_eia": {
        "description": "Long descriptions for the EIA consolidated NAICS sector codes. Codes and descriptions taken from the EIA-923 File Layout spreadsheet.",
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["sector_id_eia"]]},
        },
        "encoder": CODE_METADATA["sector_consolidated_eia"],
        "sources": ["eia860", "eia923"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "utilities_eia": {
        "description": "Associations between the EIA Utility IDs and the manually assigned PUDL Utility IDs.",
        "schema": {
            "fields": ["utility_id_eia", "utility_name_eia", "utility_id_pudl"],
            "primary_key": ["utility_id_eia"],
        },
        "sources": ["eia860", "eia923"],
        "etl_group": "glue",
        "field_namespace": "eia",
    },
    "utilities_entity_eia": {
        "description": "Static attributes of utilities, compiled from all EIA data.",
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
        "sources": ["eia860", "eia923"],
        "etl_group": "entity_eia",
        "field_namespace": "eia",
    },
}
"""Generic EIA resource attributes organized by PUDL identifier (``resource.name``).

Keys are in alphabetical order.

See :func:`pudl.metadata.helpers.build_foreign_keys` for the expected format of
``foreign_key_rules``.
"""
