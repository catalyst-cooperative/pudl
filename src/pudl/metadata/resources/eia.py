"""Definitions of data tables primarily coming from EIA 860/861/923."""

from typing import Any

from pudl.metadata.codes import CODE_METADATA

AGG_FREQS = ["yearly", "monthly"]

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_eia__codes_balancing_authorities": {
        "description": {
            "additional_summary_text": "balancing authorities in EIA 860, EIA 923, and EIA 930.",
        },
        "schema": {
            "fields": [
                "code",
                "label",
                "description",
                "balancing_authority_region_code_eia",
                "balancing_authority_region_name_eia",
                "report_timezone",
                "balancing_authority_retirement_date",
                "is_generation_only",
                "interconnect_code_eia",
            ],
            "primary_key": ["code"],
            "foreign_key_rules": {
                "fields": [
                    ["balancing_authority_code_eia"],
                    ["balancing_authority_code_adjacent_eia"],
                ],
                "exclude": [
                    "core_eia861__yearly_advanced_metering_infrastructure",
                    "core_eia861__yearly_balancing_authority",
                    "out_eia861__yearly_balancing_authority_service_territory",
                    "core_eia861__yearly_demand_response",
                    "core_eia861__yearly_demand_response_water_heater",
                    "core_eia861__yearly_dynamic_pricing",
                    "core_eia861__yearly_energy_efficiency",
                    "out_ferc714__respondents_with_fips",
                    "core_eia861__yearly_net_metering_customer_fuel_class",
                    "core_eia861__yearly_net_metering_misc",
                    "core_eia861__yearly_non_net_metering_customer_fuel_class",
                    "core_eia861__yearly_non_net_metering_misc",
                    "core_eia861__yearly_reliability",
                    "core_eia861__yearly_sales",
                    "out_ferc714__summarized_demand",
                    "core_eia861__yearly_short_form",
                ],
            },
        },
        "encoder": CODE_METADATA["core_eia__codes_balancing_authorities"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "core_eia__codes_balancing_authority_subregions": {
        "description": {
            "additional_summary_text": "balancing authority subregions in EIA 930.",
        },
        "schema": {
            "fields": [
                "balancing_authority_code_eia",
                "balancing_authority_subregion_code_eia",
                "balancing_authority_subregion_name_eia",
            ],
            "primary_key": [
                "balancing_authority_code_eia",
                "balancing_authority_subregion_code_eia",
            ],
            "foreign_key_rules": {
                "fields": [
                    [
                        "balancing_authority_code_eia",
                        "balancing_authority_subregion_code_eia",
                    ],
                ]
            },
        },
        "sources": ["eia930"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "core_eia__codes_cooling_water_sources": {
        "description": {
            "additional_summary_text": "cooling water sources in EIA-860.",
        },
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["boiler_type"]]},
        },
        "encoder": CODE_METADATA["core_eia__codes_cooling_water_sources"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "core_eia__codes_sorbent_types": {
        "description": {
            "additional_summary_text": "flue gas desulfurization sorbent types used in EIA-860.",
        },
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {
                "fields": [
                    ["sorbent_type_1"],
                    ["sorbent_type_2"],
                    ["sorbent_type_3"],
                    ["sorbent_type_4"],
                ]
            },
        },
        "encoder": CODE_METADATA["core_eia__codes_sorbent_types"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "core_eia__entity_boilers": {
        "description": {
            "additional_summary_text": "boilers compiled from the EIA-860 and EIA-923.",
        },
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
    "core_eia__codes_boiler_generator_assn_types": {
        "description": {
            "additional_summary_text": "boiler-generator associations in the EIA 860.",
        },
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["boiler_generator_assn_type_code"]]},
        },
        "encoder": CODE_METADATA["core_eia__codes_boiler_generator_assn_types"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "core_eia__codes_boiler_status": {
        "description": {
            "additional_summary_text": "boiler status in the EIA 860.",
        },
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["boiler_status"]]},
        },
        "encoder": CODE_METADATA["core_eia__codes_boiler_status"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "core_eia__codes_boiler_types": {
        "description": {
            "additional_summary_text": "boiler regulatory types in the EIA 860.",
        },
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["boiler_type"]]},
        },
        "encoder": CODE_METADATA["core_eia__codes_boiler_types"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "core_eia__codes_environmental_equipment_manufacturers": {
        "description": {
            "additional_summary_text": "manufacturers of boilers and environmental control equipment in the EIA-860.",
        },
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {
                "fields": [
                    ["boiler_manufacturer_code"],
                    ["fgd_manufacturer_code"],
                    ["nox_control_manufacturer_code"],
                ]
            },
        },
        "encoder": CODE_METADATA[
            "core_eia__codes_environmental_equipment_manufacturers"
        ],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "core_eia__codes_emission_control_equipment_types": {
        "description": {
            "additional_summary_text": "emissions control equipment installed on a boiler.",
        },
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {
                "fields": [
                    ["emission_control_equipment_type_code"],
                    ["so2_equipment_type_1"],
                    ["so2_equipment_type_2"],
                    ["so2_equipment_type_3"],
                    ["so2_equipment_type_4"],
                ],
            },
        },
        "encoder": CODE_METADATA["core_eia__codes_emission_control_equipment_types"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "core_eia__codes_firing_types": {
        "description": {
            "additional_summary_text": "boiler firing types in the EIA-860.",
        },
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {
                "fields": [["firing_type_1"], ["firing_type_2"], ["firing_type_3"]]
            },
        },
        "encoder": CODE_METADATA["core_eia__codes_firing_types"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "core_eia__codes_nox_compliance_strategies": {
        "description": {
            "additional_summary_text": "compliance strategies used to control nitrogen oxide in the EIA-860.",
        },
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
        "encoder": CODE_METADATA["core_eia__codes_nox_compliance_strategies"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "core_eia__codes_nox_control_status": {
        "description": {
            "additional_summary_text": "the operational status of nitrogen oxide control units associated with boilers in the EIA-860 data.",
        },
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {
                "fields": [
                    ["nox_control_status_code"],
                ]
            },
        },
        "encoder": CODE_METADATA["core_eia__codes_nox_control_status"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "core_eia__codes_nox_units": {
        "description": {
            "additional_summary_text": "units of measurement for nitrogen oxide in the EIA-860.",
        },
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["unit_nox"]]},
        },
        "encoder": CODE_METADATA["core_eia__codes_nox_units"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "core_eia__codes_mercury_compliance_strategies": {
        "description": {
            "additional_summary_text": "compliance strategies used to control mercury in the EIA-860.",
        },
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
        "encoder": CODE_METADATA["core_eia__codes_mercury_compliance_strategies"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "core_eia__codes_averaging_periods": {
        "description": {
            "additional_summary_text": "the averaging period specified by emissions statutes and regulations for the EIA 860.",
        },
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {
                "fields": [["period_nox"], ["period_particulate"], ["period_so2"]]
            },
        },
        "encoder": CODE_METADATA["core_eia__codes_averaging_periods"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "core_eia__codes_particulate_compliance_strategies": {
        "description": {
            "additional_summary_text": "compliance strategies used to control particulate matter in the EIA-860.",
        },
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
        "encoder": CODE_METADATA["core_eia__codes_particulate_compliance_strategies"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "core_eia__codes_particulate_units": {
        "description": {
            "additional_summary_text": "units of measurement for particulate matter in the EIA-860.",
        },
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["unit_particulate"]]},
        },
        "encoder": CODE_METADATA["core_eia__codes_particulate_units"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "core_eia__codes_regulations": {
        "description": {
            "additional_summary_text": "levels of statutes and codes under which boilers operate in the EIA-860.",
        },
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
        "encoder": CODE_METADATA["core_eia__codes_regulations"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "core_eia__codes_so2_compliance_strategies": {
        "description": {
            "additional_summary_text": "compliance strategies used to control sulfur dioxide in the EIA-860.",
        },
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
        "encoder": CODE_METADATA["core_eia__codes_so2_compliance_strategies"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "core_eia__codes_wet_dry_bottom": {
        "description": {
            "additional_summary_text": "boiler bottoms in the EIA-860.",
        },
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["wet_dry_bottom"]]},
        },
        "encoder": CODE_METADATA["core_eia__codes_wet_dry_bottom"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "core_eia__codes_so2_units": {
        "description": {
            "additional_summary_text": "units of measurement for sulfur dioxide in the EIA-860.",
        },
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["unit_so2"]]},
        },
        "encoder": CODE_METADATA["core_eia__codes_so2_units"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "core_eia__codes_steam_plant_types": {
        "description": {
            "additional_summary_text": "steam plants in the EIA-860.",
        },
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["steam_plant_type_code"]]},
        },
        "encoder": CODE_METADATA["core_eia__codes_steam_plant_types"],
        "sources": ["eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "core_eia__codes_reporting_frequencies": {
        "description": {
            "additional_summary_text": "the reporting frequencies used by plants in the EIA-923.",
        },
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["reporting_frequency_code"]]},
        },
        "encoder": CODE_METADATA["core_eia__codes_reporting_frequencies"],
        "sources": ["eia923"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "core_eia__codes_coalmine_types": {
        "description": {
            "additional_summary_text": "coalmines reported as fuel sources in the EIA-923.",
        },
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["mine_type_code"]]},
        },
        "encoder": CODE_METADATA["core_eia__codes_coalmine_types"],
        "sources": ["eia923"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "core_eia__codes_contract_types": {
        "description": {
            "additional_summary_text": "fuel supply contracts reported in EIA-923.",
        },
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["contract_type_code"]]},
        },
        "encoder": CODE_METADATA["core_eia__codes_contract_types"],
        "sources": ["eia923"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "core_eia__codes_operational_status": {
        "description": {
            "additional_summary_text": "operational status reported to EIA.",
            "additional_details_text": "Compiled from EIA-860 instructions and EIA-923 file layout spreadsheets.",
        },
        "schema": {
            "fields": [
                "code",
                "label",
                "description",
                "operational_status",
            ],
            "primary_key": ["code"],
            "foreign_key_rules": {
                "fields": [
                    ["operational_status_code"],
                    ["fgd_operational_status_code"],
                    ["cooling_status_code"],
                ],
            },
        },
        "encoder": CODE_METADATA["core_eia__codes_operational_status"],
        "sources": ["eia860", "eia923"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "core_pudl__codes_data_maturities": {
        "description": {
            "additional_summary_text": "maturity levels of data records. Some data sources report less-than-final data. PUDL sometimes includes this data, but use at your own risk.",
        },
        "schema": {
            "fields": ["code", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {
                "fields": [["data_maturity"]],
            },
        },
        "encoder": CODE_METADATA["core_pudl__codes_data_maturities"],
        "sources": ["eia860", "eia923"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "core_eia__codes_energy_sources": {
        "description": {
            "additional_summary_text": "energy sources reported to EIA.",
            "additional_details_text": "Compiled from EIA-860 instructions and EIA-923 file layout spreadsheets.",
        },
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
                    ["energy_source_code_7"],
                    ["energy_source_code_8"],
                    ["energy_source_code_9"],
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
        "encoder": CODE_METADATA["core_eia__codes_energy_sources"],
        "sources": ["eia860", "eia923"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    # The entity types were never fully reconciled. Preserving for future reference.
    # See https://github.com/catalyst-cooperative/pudl/issues/1392
    # "core_eia__codes_entity_types": {
    #    "description": "Descriptive labels for EIA entity type and ownership codes, taken from the EIA-861 form instructions, valid through 2023-05-31.",
    #    "schema": {"fields": ["code", "label", "description"], "primary_key": ["code"]},
    #    "encoder": CODE_METADATA["core_eia__codes_entity_types"],
    #    "sources": ["eia861"],
    #    "etl_group": "static_eia_disabled",  # currently not being loaded into the db
    #    "field_namespace": "eia",
    #    "create_database_schema": False,
    # },
    "core_eia__codes_fuel_transportation_modes": {
        "description": {
            "additional_summary_text": "fuel transportation modes reported in the EIA-860 and EIA-923.",
        },
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
        "encoder": CODE_METADATA["core_eia__codes_fuel_transportation_modes"],
        "sources": ["eia860", "eia923"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "core_eia__codes_fuel_types_agg": {
        "description": {
            "additional_summary_text": "aggregated fuel types used in the Annual Energy Review or Monthly Energy Review.",
            "additional_details_text": "See EIA-923 Fuel Code table for additional information.",
        },
        "schema": {
            "fields": ["code", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["fuel_type_code_agg"]]},
        },
        "encoder": CODE_METADATA["core_eia__codes_fuel_types_agg"],
        "sources": ["eia923"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "core_eia__entity_generators": {
        "description": {
            "additional_summary_text": "generators compiled from across the EIA-860 and EIA-923.",
        },
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
                "can_switch_when_operating",
                "previously_canceled",
            ],
            "primary_key": ["plant_id_eia", "generator_id"],
            "foreign_key_rules": {
                "fields": [
                    ["plant_id_eia", "generator_id"],
                    ["plant_id_eia_direct_support_1", "generator_id_direct_support_1"],
                    ["plant_id_eia_direct_support_2", "generator_id_direct_support_2"],
                    ["plant_id_eia_direct_support_3", "generator_id_direct_support_3"],
                ],
                # exclude core_epa__assn_eia_epacamd_subplant_ids bc there are generator ids in this
                # glue table that come only from epacamd
                # also exclude the 860 changelog table bc that table doesn't get harvested
                # and therefor there are a few straggler generators that don't end up in this table
                "exclude": [
                    "core_epa__assn_eia_epacamd_subplant_ids",
                    "core_eia860m__changelog_generators",
                ],
            },
        },
        "sources": ["eia860", "eia923"],
        "etl_group": "entity_eia",
        "field_namespace": "eia",
    },
    "core_eia__codes_momentary_interruptions": {
        "description": {
            "additional_summary_text": "utility definitions of momentary service interruptions.",
        },
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["momentary_interruption_definition"]]},
        },
        "encoder": CODE_METADATA["core_eia__codes_momentary_interruptions"],
        "sources": ["eia861"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "core_pudl__assn_eia_pudl_plants": {
        "description": {
            "additional_summary_text": "EIA plant IDs and manually assigned PUDL plant IDs.",
        },
        "schema": {
            "fields": ["plant_id_eia", "plant_name_eia", "plant_id_pudl"],
            "primary_key": ["plant_id_eia"],
        },
        "sources": ["eia860", "eia923"],
        "etl_group": "glue",
        "field_namespace": "eia",
    },
    "core_eia__entity_plants": {
        "description": {
            "additional_summary_text": "plants, compiled from across all EIA-860 and EIA-923 data.",
        },
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
                # Excluding core_pudl__assn_eia_pudl_plants because it's static and manually compiled
                # so it has plants from *all* years of data, even when only a
                # restricted set of data is processed, leading to constraint
                # violations.
                # See: https://github.com/catalyst-cooperative/pudl/issues/1196
                # Exclude the core_epa__assn_eia_epacamd_subplant_ids table
                # also exclude the 860 changelog table bc that table doesn't get harvested
                # and therefor there are a few straggler generators that don't end up in this table
                "exclude": [
                    "core_pudl__assn_eia_pudl_plants",
                    "core_epa__assn_eia_epacamd_subplant_ids",
                    "core_eia860m__changelog_generators",
                ],
            },
        },
        "sources": ["eia860", "eia923"],
        "etl_group": "entity_eia",
        "field_namespace": "eia",
    },
    "core_eia__codes_prime_movers": {
        "description": {
            "additional_summary_text": "prime movers reported in the EIA-860 and EIA-923.",
        },
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
        "encoder": CODE_METADATA["core_eia__codes_prime_movers"],
        "sources": ["eia923", "eia860"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "core_eia__codes_sector_consolidated": {
        "description": {
            "additional_summary_text": "EIA consolidated NAICS sectors.",
            "additional_details_text": "Codes and descriptions taken from the EIA-923 File Layout spreadsheet.",
        },
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["sector_id_eia"]]},
        },
        "encoder": CODE_METADATA["core_eia__codes_sector_consolidated"],
        "sources": ["eia860", "eia923"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "core_pudl__assn_eia_pudl_utilities": {
        "description": {
            "additional_summary_text": "EIA utility IDs and manually assigned PUDL utility IDs.",
        },
        "schema": {
            "fields": ["utility_id_eia", "utility_name_eia", "utility_id_pudl"],
            "primary_key": ["utility_id_eia"],
        },
        "sources": ["eia860", "eia923"],
        "etl_group": "glue",
        "field_namespace": "eia",
    },
    "core_eia__entity_utilities": {
        "description": {
            "additional_summary_text": "utilities, compiled from all EIA data.",
        },
        "schema": {
            "fields": ["utility_id_eia", "utility_name_eia"],
            "primary_key": ["utility_id_eia"],
            "foreign_key_rules": {
                "fields": [["utility_id_eia"], ["owner_utility_id_eia"]],
                # Excluding utilities_eia b/c it's static and manually compiled
                # so it has utilities from *all* years of data, even when only a
                # restricted set of data is processed, leading to constraint
                # violations.
                # See: https://github.com/catalyst-cooperative/pudl/issues/1196
                # Excluding EIA-861 because they haven't been harvested/normalized.
                "exclude": [
                    "core_pudl__assn_eia_pudl_utilities",
                    "core_eia861__yearly_advanced_metering_infrastructure",
                    "core_eia861__assn_balancing_authority",
                    "out_eia861__yearly_utility_service_territory",
                    "core_eia861__yearly_demand_response",
                    "core_eia861__yearly_demand_response_water_heater",
                    "core_eia861__yearly_demand_side_management_ee_dr",
                    "core_eia861__yearly_demand_side_management_misc",
                    "core_eia861__yearly_demand_side_management_sales",
                    "core_eia861__yearly_distributed_generation_fuel",
                    "core_eia861__yearly_distributed_generation_misc",
                    "core_eia861__yearly_distributed_generation_tech",
                    "core_eia861__yearly_distribution_systems",
                    "core_eia861__yearly_dynamic_pricing",
                    "core_eia861__yearly_energy_efficiency",
                    "out_ferc714__respondents_with_fips",
                    "core_eia861__yearly_green_pricing",
                    "core_eia861__yearly_mergers",
                    "core_eia861__yearly_net_metering_customer_fuel_class",
                    "core_eia861__yearly_net_metering_misc",
                    "core_eia861__yearly_non_net_metering_customer_fuel_class",
                    "core_eia861__yearly_non_net_metering_misc",
                    "core_eia861__yearly_operational_data_misc",
                    "core_eia861__yearly_operational_data_revenue",
                    "core_eia861__yearly_reliability",
                    "core_eia861__yearly_sales",
                    "core_eia861__yearly_service_territory",
                    "out_ferc714__summarized_demand",
                    "core_eia861__assn_utility",
                    "core_eia861__yearly_utility_data_misc",
                    "core_eia861__yearly_utility_data_nerc",
                    "core_eia861__yearly_utility_data_rto",
                    "core_eia861__yearly_short_form",
                    # Utility IDs in this table are owners, not operators, and we are
                    # not yet harvesting owner_utility_id_eia from core_eia860__scd_ownership.
                    # See https://github.com/catalyst-cooperative/pudl/issues/1393
                    "out_eia923__yearly_generation_fuel_by_generator_energy_source_owner",
                    # also exclude the 860 changelog table bc that table doesn't get harvested
                    # and therefor there are a few straggler generators that don't end up in this table
                    "core_eia860m__changelog_generators",
                    # These have to be excluded because the record-linkage between SEC
                    # companies and EIA utilities is not time-dependent -- if any SEC
                    # CIK is ever matched with an EIA utility ID, that utility ID shows
                    # up across all years of data, even if that utility doesn't show up
                    # in the EIA data for that year.
                    "core_sec10k__assn_sec10k_filers_and_eia_utilities",
                    "out_sec10k__quarterly_company_information",
                ],
            },
        },
        "sources": ["eia860", "eia923"],
        "etl_group": "entity_eia",
        "field_namespace": "eia",
    },
    "out_eia__yearly_utilities": {
        "description": {
            "additional_summary_text": "all EIA utility attributes.",
        },
        "schema": {
            "fields": [
                "utility_id_eia",
                "utility_id_pudl",
                "utility_name_eia",
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
                "data_maturity",
            ],
            "primary_key": ["utility_id_eia", "report_date"],
        },
        "field_namespace": "eia",
        "sources": ["eia860", "eia923"],
        "etl_group": "outputs",
    },
    "out_eia__yearly_plants": {
        "description": {
            "additional_summary_text": "all EIA plant attributes.",
        },
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
                "report_date",
                "ash_impoundment",
                "ash_impoundment_lined",
                "ash_impoundment_status",
                "balancing_authority_code_eia",
                "balancing_authority_name_eia",
                "datum",
                "energy_storage",
                "ferc_cogen_docket_no",
                "ferc_cogen_status",
                "ferc_exempt_wholesale_generator_docket_no",
                "ferc_exempt_wholesale_generator",
                "ferc_small_power_producer_docket_no",
                "ferc_small_power_producer",
                "ferc_qualifying_facility_docket_no",
                "grid_voltage_1_kv",
                "grid_voltage_2_kv",
                "grid_voltage_3_kv",
                "iso_rto_code",
                "liquefied_natural_gas_storage",
                "natural_gas_local_distribution_company",
                "natural_gas_storage",
                "natural_gas_pipeline_name_1",
                "natural_gas_pipeline_name_2",
                "natural_gas_pipeline_name_3",
                "nerc_region",
                "has_net_metering",
                "pipeline_notes",
                "primary_purpose_id_naics",
                "regulatory_status_code",
                "reporting_frequency_code",
                "sector_id_eia",
                "sector_name_eia",
                "service_area",
                "transmission_distribution_owner_id",
                "transmission_distribution_owner_name",
                "transmission_distribution_owner_state",
                "utility_id_eia",
                "water_source",
                "data_maturity",
                "plant_id_pudl",
                "utility_name_eia",
                "utility_id_pudl",
                "balancing_authority_code_eia_consistent_rate",
            ],
            "primary_key": ["plant_id_eia", "report_date"],
        },
        "field_namespace": "eia",
        "sources": ["eia860", "eia923"],
        "etl_group": "outputs",
    },
    "out_eia__yearly_boilers": {
        "description": {
            "additional_summary_text": "all EIA boiler attributes.",
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
                "boiler_id",
                "air_flow_100pct_load_cubic_feet_per_minute",
                "boiler_fuel_code_1",
                "boiler_fuel_code_2",
                "boiler_fuel_code_3",
                "boiler_fuel_code_4",
                "boiler_manufacturer",
                "boiler_manufacturer_code",
                "boiler_operating_date",
                "boiler_retirement_date",
                "boiler_status",
                "boiler_type",
                "city",
                "compliance_year_mercury",
                "compliance_year_nox",
                "compliance_year_particulate",
                "compliance_year_so2",
                "county",
                "data_maturity",
                "efficiency_100pct_load",
                "efficiency_50pct_load",
                "firing_rate_using_coal_tons_per_hour",
                "firing_rate_using_gas_mcf_per_hour",
                "firing_rate_using_oil_bbls_per_hour",
                "firing_rate_using_other_fuels",
                "firing_type_1",
                "firing_type_2",
                "firing_type_3",
                "fly_ash_reinjection",
                "hrsg",
                "latitude",
                "longitude",
                "max_steam_flow_1000_lbs_per_hour",
                "mercury_control_existing_strategy_1",
                "mercury_control_existing_strategy_2",
                "mercury_control_existing_strategy_3",
                "mercury_control_existing_strategy_4",
                "mercury_control_existing_strategy_5",
                "mercury_control_existing_strategy_6",
                "mercury_control_proposed_strategy_1",
                "mercury_control_proposed_strategy_2",
                "mercury_control_proposed_strategy_3",
                "new_source_review",
                "new_source_review_date",
                "new_source_review_permit",
                "nox_control_existing_caaa_compliance_strategy_1",
                "nox_control_existing_caaa_compliance_strategy_2",
                "nox_control_existing_caaa_compliance_strategy_3",
                "nox_control_existing_strategy_1",
                "nox_control_existing_strategy_2",
                "nox_control_existing_strategy_3",
                "nox_control_manufacturer",
                "nox_control_manufacturer_code",
                "nox_control_out_of_compliance_strategy_1",
                "nox_control_out_of_compliance_strategy_2",
                "nox_control_out_of_compliance_strategy_3",
                "nox_control_planned_caaa_compliance_strategy_1",
                "nox_control_planned_caaa_compliance_strategy_2",
                "nox_control_planned_caaa_compliance_strategy_3",
                "nox_control_proposed_strategy_1",
                "nox_control_proposed_strategy_2",
                "nox_control_proposed_strategy_3",
                "nox_control_status_code",
                "particulate_control_out_of_compliance_strategy_1",
                "particulate_control_out_of_compliance_strategy_2",
                "particulate_control_out_of_compliance_strategy_3",
                "regulation_mercury",
                "regulation_nox",
                "regulation_particulate",
                "regulation_so2",
                "so2_control_existing_caaa_compliance_strategy_1",
                "so2_control_existing_caaa_compliance_strategy_2",
                "so2_control_existing_caaa_compliance_strategy_3",
                "so2_control_existing_strategy_1",
                "so2_control_existing_strategy_2",
                "so2_control_existing_strategy_3",
                "so2_control_out_of_compliance_strategy_1",
                "so2_control_out_of_compliance_strategy_2",
                "so2_control_out_of_compliance_strategy_3",
                "so2_control_planned_caaa_compliance_strategy_1",
                "so2_control_planned_caaa_compliance_strategy_2",
                "so2_control_planned_caaa_compliance_strategy_3",
                "so2_control_proposed_strategy_1",
                "so2_control_proposed_strategy_2",
                "so2_control_proposed_strategy_3",
                "standard_nox_rate",
                "standard_particulate_rate",
                "standard_so2_percent_scrubbed",
                "standard_so2_rate",
                "state",
                "street_address",
                "timezone",
                "turndown_ratio",
                "unit_id_pudl",
                "unit_nox",
                "unit_particulate",
                "unit_so2",
                "waste_heat_input_mmbtu_per_hour",
                "wet_dry_bottom",
                "zip_code",
            ],
            "primary_key": ["plant_id_eia", "boiler_id", "report_date"],
        },
        "field_namespace": "eia",
        "sources": ["eia860", "eia923"],
        "etl_group": "outputs",
    },
    "core_eia__codes_cooling_tower_types": {
        "description": {
            "additional_summary_text": "cooling towers.",
        },
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {
                "fields": [
                    ["tower_type_1"],
                    ["tower_type_2"],
                    ["tower_type_3"],
                    ["tower_type_4"],
                ]
            },
        },
        "encoder": CODE_METADATA["core_eia__codes_cooling_tower_types"],
        "field_namespace": "eia",
        "sources": ["eia860"],
        "etl_group": "static_eia",
    },
    "core_eia__codes_cooling_water_types": {
        "description": {
            "additional_summary_text": "cooling water.",
        },
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["water_type_code"]]},
        },
        "encoder": CODE_METADATA["core_eia__codes_cooling_water_types"],
        "field_namespace": "eia",
        "sources": ["eia860"],
        "etl_group": "static_eia",
    },
    "core_eia__codes_cooling_system_types": {
        "description": {
            "additional_summary_text": "cooling systems.",
        },
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {
                "fields": [
                    ["cooling_type"],
                    ["cooling_type_1"],
                    ["cooling_type_2"],
                    ["cooling_type_3"],
                    ["cooling_type_4"],
                ]
            },
        },
        "encoder": CODE_METADATA["core_eia__codes_cooling_system_types"],
        "field_namespace": "eia",
        "sources": ["eia860", "eia923"],
        "etl_group": "static_eia",
    },
    "core_eia__codes_wind_quality_class": {
        "description": {
            "additional_summary_text": "wind quality classes.",
        },
        "schema": {
            "fields": [
                "code",
                "label",
                "description",
                "wind_speed_avg_ms",
                "extreme_fifty_year_gust_ms",
                "turbulence_intensity_a",
                "turbulence_intensity_b",
            ],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["wind_quality_class"]]},
        },
        "encoder": CODE_METADATA["core_eia__codes_wind_quality_class"],
        "field_namespace": "eia",
        "sources": ["eia860"],
        "etl_group": "static_eia",
    },
    "core_eia__codes_storage_enclosure_types": {
        "description": {
            "additional_summary_text": "energy storage enclosures.",
        },
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["storage_enclosure_code"]]},
        },
        "encoder": CODE_METADATA["core_eia__codes_storage_enclosure_types"],
        "field_namespace": "eia",
        "sources": ["eia860"],
        "etl_group": "static_eia",
    },
    "core_eia__codes_storage_technology_types": {
        "description": {
            "additional_summary_text": "energy storage technologies.",
        },
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {
                "fields": [
                    ["storage_technology_code_1"],
                    ["storage_technology_code_2"],
                    ["storage_technology_code_3"],
                    ["storage_technology_code_4"],
                ]
            },
        },
        "encoder": CODE_METADATA["core_eia__codes_storage_technology_types"],
        "field_namespace": "eia",
        "sources": ["eia860"],
        "etl_group": "static_eia",
    },
} | {
    # out_eia__yearly_generators and out_eia__monthly_generators are defined here
    f"out_eia__{freq}_generators": {
        "description": {
            "additional_summary_text": "all generator attributes including calculated capacity factor, heat rate, fuel cost per MMBTU and fuel cost per MWh.",
            "additional_details_text": """These calculations are based on
the allocation of net generation reported on the basis of plant, prime mover and energy
source to individual generators. Heat rates by generator-month are estimated by using
allocated estimates for per-generator net generation and fuel consumption as well as the
:ref:`core_eia923__monthly_boiler_fuel` table, which reports fuel consumed by boiler.
Heat rates are necessary to estimate the amount of fuel consumed by a generation unit,
and thus the fuel cost per MWh generated.

Plant specific fuel prices are taken from the
:ref:`core_eia923__monthly_fuel_receipts_costs` table, which only has ~70% coverage,
leading to some generators with heat rate estimates still lacking fuel cost
estimates.""",
            "usage_warnings": [
                "estimated_values",
                {
                    "type": "custom",
                    "description": "Due to coverage problems in other tables, some generators have heat rate estimates but not fuel cost estimates.",
                },
                {
                    "type": "custom",
                    "description": "Not all columns are originally reported in or calculable from the input tables. Expect nulls.",
                },
            ],
        },
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
                "balancing_authority_code_eia",
                "balancing_authority_name_eia",
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
                "fuel_cost_per_mmbtu_source",
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
                "can_cofire_fuels",
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
                "can_burn_multiple_fuels",
                "nameplate_power_factor",
                "net_capacity_mwdc",
                "can_switch_when_operating",
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
                "can_switch_oil_gas",
                "synchronized_transmission_grid",
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
"""Generic EIA resource attributes organized by PUDL identifier (``resource.name``).

See :func:`pudl.metadata.helpers.build_foreign_keys` for the expected format of
``foreign_key_rules``.
"""
