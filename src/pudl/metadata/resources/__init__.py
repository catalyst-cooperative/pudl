"""A subpackage to define and organize PUDL database tables by data group."""

import importlib
import pkgutil
from typing import Dict, List

from pudl.metadata.helpers import build_foreign_keys

RESOURCE_METADATA = {}
for module_info in pkgutil.iter_modules(__path__):
    module = importlib.import_module(f"{__name__}.{module_info.name}")
    if module.__name__ == "pudl.metadata.resources.eia861":
        continue
    resources = module.RESOURCE_METADATA
    RESOURCE_METADATA.update(resources)

FOREIGN_KEYS: Dict[str, List[dict]] = build_foreign_keys(RESOURCE_METADATA)
"""
Generated foreign key constraints by resource name.

See :func:`pudl.metadata.helpers.build_foreign_keys`.
"""

ENTITIES: Dict[str, Dict[str, List[str]]] = {
    'plants': {
        "id_cols": ['plant_id_eia'],
        "static_cols": [
            'balancing_authority_code_eia',
            'balancing_authority_name_eia',
            'city',
            'county',
            'ferc_cogen_status',
            'ferc_exempt_wholesale_generator',
            'ferc_small_power_producer',
            'grid_voltage_2_kv',
            'grid_voltage_3_kv',
            'grid_voltage_kv',
            'iso_rto_code',
            'latitude',
            'longitude',
            'plant_name_eia',
            'primary_purpose_id_naics',
            'sector_id_eia',
            'sector_name_eia',
            'state',
            'street_address',
            'zip_code',
        ],
        "annual_cols": [
            'ash_impoundment',
            'ash_impoundment_lined',
            'ash_impoundment_status',
            'datum',
            'energy_storage',
            'ferc_cogen_docket_no',
            'water_source',
            'ferc_exempt_wholesale_generator_docket_no',
            'ferc_small_power_producer_docket_no',
            'liquefied_natural_gas_storage',
            'natural_gas_local_distribution_company',
            'natural_gas_storage',
            'natural_gas_pipeline_name_1',
            'natural_gas_pipeline_name_2',
            'natural_gas_pipeline_name_3',
            'nerc_region',
            'net_metering',
            'pipeline_notes',
            'regulatory_status_code',
            'respondent_frequency',
            'service_area',
            'transmission_distribution_owner_id',
            'transmission_distribution_owner_name',
            'transmission_distribution_owner_state',
            'utility_id_eia',
        ],
    },
    'generators': {
        "id_cols": ['plant_id_eia', 'generator_id'],
        "static_cols": [
            'prime_mover_code',
            'duct_burners',
            'operating_date',
            'topping_bottoming_code',
            'solid_fuel_gasification',
            'pulverized_coal_tech',
            'fluidized_bed_tech',
            'subcritical_tech',
            'supercritical_tech',
            'ultrasupercritical_tech',
            'stoker_tech',
            'other_combustion_tech',
            'bypass_heat_recovery',
            'rto_iso_lmp_node_id',
            'rto_iso_location_wholesale_reporting_id',
            'associated_combined_heat_power',
            'original_planned_operating_date',
            'operating_switch',
            'previously_canceled',
        ],
        "annual_cols": [
            'capacity_mw',
            'fuel_type_code_pudl',
            'multiple_fuels',
            'ownership_code',
            'owned_by_non_utility',
            'deliver_power_transgrid',
            'summer_capacity_mw',
            'winter_capacity_mw',
            'summer_capacity_estimate',
            'winter_capacity_estimate',
            'minimum_load_mw',
            'distributed_generation',
            'technology_description',
            'reactive_power_output_mvar',
            'energy_source_code_1',
            'energy_source_code_2',
            'energy_source_code_3',
            'energy_source_code_4',
            'energy_source_code_5',
            'energy_source_code_6',
            'energy_source_1_transport_1',
            'energy_source_1_transport_2',
            'energy_source_1_transport_3',
            'energy_source_2_transport_1',
            'energy_source_2_transport_2',
            'energy_source_2_transport_3',
            'startup_source_code_1',
            'startup_source_code_2',
            'startup_source_code_3',
            'startup_source_code_4',
            'time_cold_shutdown_full_load_code',
            'syncronized_transmission_grid',
            'turbines_num',
            'operational_status_code',
            'operational_status',
            'planned_modifications',
            'planned_net_summer_capacity_uprate_mw',
            'planned_net_winter_capacity_uprate_mw',
            'planned_new_capacity_mw',
            'planned_uprate_date',
            'planned_net_summer_capacity_derate_mw',
            'planned_net_winter_capacity_derate_mw',
            'planned_derate_date',
            'planned_new_prime_mover_code',
            'planned_energy_source_code_1',
            'planned_repower_date',
            'other_planned_modifications',
            'other_modifications_date',
            'planned_retirement_date',
            'carbon_capture',
            'cofire_fuels',
            'switch_oil_gas',
            'turbines_inverters_hydrokinetics',
            'nameplate_power_factor',
            'uprate_derate_during_year',
            'uprate_derate_completed_date',
            'current_planned_operating_date',
            'summer_estimated_capability_mw',
            'winter_estimated_capability_mw',
            'retirement_date',
            'utility_id_eia',
            'data_source',
        ],
    },
    # utilities must come after plants. plant location needs to be
    # removed before the utility locations are compiled
    'utilities': {
        "id_cols": ['utility_id_eia'],
        "static_cols": ['utility_name_eia'],
        "annual_cols": [
            'street_address',
            'city',
            'state',
            'zip_code',
            'entity_type',
            'plants_reported_owner',
            'plants_reported_operator',
            'plants_reported_asset_manager',
            'plants_reported_other_relationship',
            'attention_line',
            'address_2',
            'zip_code_4',
            'contact_firstname',
            'contact_lastname',
            'contact_title',
            'contact_firstname_2',
            'contact_lastname_2',
            'contact_title_2',
            'phone_extension',
            'phone_extension_2',
            'phone_number',
            'phone_number_2',
        ],
    },
    'boilers': {
        "id_cols": ['plant_id_eia', 'boiler_id'],
        "static_cols": ['prime_mover_code'],
        "annual_cols": [],
    }
}
"""
Columns kept for either entity or annual EIA tables in the harvesting process.

For each entity type (key), the ID columns, static columns, and annual columns,

The order of the entities matters. Plants must be harvested before utilities,
since plant location must be removed before the utility locations are harvested.
"""
