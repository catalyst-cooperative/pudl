"""Definitions of data tables primarily coming from EIA 860/861/923."""
from typing import Any, Dict

from pudl.metadata.codes import CODE_METADATA

RESOURCE_METADATA: Dict[str, Dict[str, Any]] = {
    "boilers_entity_eia": {
        "description": "Static boiler attributes compiled from the EIA-860 and EIA-923 data.",
        "schema": {
            "fields": ["plant_id_eia", "boiler_id", "prime_mover_code"],
            "primary_key": ["plant_id_eia", "boiler_id"],
            "foreign_key_rules": {"fields": [["plant_id_eia", "boiler_id"]]},
        },
        "sources": ["eia860", "eia923"],
        "etl_group": "entity_eia",
        "field_namespace": "eia",
    },
    "coalmine_types_eia": {
        "description": "A coding table describing different types of coalmines reported as fuel sources in the EIA-923.",
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["mine_type_code"]]}
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
            "foreign_key_rules": {"fields": [["contract_type_code"]]}
        },
        "encoder": CODE_METADATA["contract_types_eia"],
        "sources": ["eia923"],
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
                ],
            },
        },
        "encoder": CODE_METADATA['energy_sources_eia'],
        "sources": ["eia860", "eia923"],
        "etl_group": "static_eia",
        "field_namespace": "eia",
    },
    "entity_types_eia": {
        "description": "Descriptive labels for EIA entity type and ownership codes, taken from the EIA-861 form instructions, valid through 2023-05-31.",
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"]
        },
        "encoder": CODE_METADATA["entity_types_eia"],
        "sources": ["eia861"],
        "etl_group": "static_eia",
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
            }
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
        "sources": ["eia860", "eia923"],
        "etl_group": "entity_eia",
        "field_namespace": "eia",
    },
    "momentary_interruptions_eia": {
        "description": "A coding table for utility definitions of momentary service interruptions.",
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["momentary_interruption_definition"]]}
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
        "sources": ["eia860", "eia923"],
        "etl_group": "entity_eia",
        "field_namespace": "eia",
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
"""
Generic EIA resource attributes organized by PUDL identifier (``resource.name``).

Keys are in alphabetical order.

See :func:`pudl.metadata.helpers.build_foreign_keys` for the expected format of
``foreign_key_rules``.
"""
