"""
A warehouse for constant values required to initilize the PUDL Database.

This constants module stores and organizes a bunch of constant values which are
used throughout PUDL to populate static lists within the data packages or for
data cleaning purposes.
"""

from typing import Dict, List, Tuple, TypedDict

from pudl.metadata.enums import EPACEMS_STATES

ENTITIES: Dict[str, Tuple[List[str], List[str], List[str], Dict[str, str]]] = {
    'plants': (
        # base cols
        ['plant_id_eia'],
        # static cols
        ['balancing_authority_code_eia', 'balancing_authority_name_eia',
         'city', 'county', 'ferc_cogen_status',
         'ferc_exempt_wholesale_generator', 'ferc_small_power_producer',
         'grid_voltage_2_kv', 'grid_voltage_3_kv', 'grid_voltage_kv',
         'iso_rto_code', 'latitude', 'longitude',
         'plant_name_eia', 'primary_purpose_id_naics',
         'sector_id_eia', 'sector_name_eia', 'state', 'street_address', 'zip_code'],
        # annual cols
        ['ash_impoundment', 'ash_impoundment_lined', 'ash_impoundment_status',
         'datum', 'energy_storage', 'ferc_cogen_docket_no', 'water_source',
         'ferc_exempt_wholesale_generator_docket_no',
         'ferc_small_power_producer_docket_no',
         'liquefied_natural_gas_storage',
         'natural_gas_local_distribution_company', 'natural_gas_storage',
         'natural_gas_pipeline_name_1', 'natural_gas_pipeline_name_2',
         'natural_gas_pipeline_name_3', 'nerc_region', 'net_metering',
         'pipeline_notes', 'regulatory_status_code', 'respondent_frequency',
         'service_area',
         'transmission_distribution_owner_id',
         'transmission_distribution_owner_name',
         'transmission_distribution_owner_state', 'utility_id_eia'],
        # need type fixing
        {},
    ),
    'generators': (
        # base cols
        ['plant_id_eia', 'generator_id'],
        # static cols
        ['prime_mover_code', 'duct_burners', 'operating_date',
         'topping_bottoming_code', 'solid_fuel_gasification',
         'pulverized_coal_tech', 'fluidized_bed_tech', 'subcritical_tech',
         'supercritical_tech', 'ultrasupercritical_tech', 'stoker_tech',
         'other_combustion_tech', 'bypass_heat_recovery',
         'rto_iso_lmp_node_id', 'rto_iso_location_wholesale_reporting_id',
         'associated_combined_heat_power', 'original_planned_operating_date',
         'operating_switch', 'previously_canceled'],
        # annual cols
        ['capacity_mw', 'fuel_type_code_pudl', 'multiple_fuels',
         'ownership_code', 'owned_by_non_utility', 'deliver_power_transgrid',
         'summer_capacity_mw', 'winter_capacity_mw', 'summer_capacity_estimate',
         'winter_capacity_estimate', 'minimum_load_mw', 'distributed_generation',
         'technology_description', 'reactive_power_output_mvar',
         'energy_source_code_1', 'energy_source_code_2',
         'energy_source_code_3', 'energy_source_code_4',
         'energy_source_code_5', 'energy_source_code_6',
         'energy_source_1_transport_1', 'energy_source_1_transport_2',
         'energy_source_1_transport_3', 'energy_source_2_transport_1',
         'energy_source_2_transport_2', 'energy_source_2_transport_3',
         'startup_source_code_1', 'startup_source_code_2',
         'startup_source_code_3', 'startup_source_code_4',
         'time_cold_shutdown_full_load_code', 'syncronized_transmission_grid',
         'turbines_num', 'operational_status_code', 'operational_status',
         'planned_modifications', 'planned_net_summer_capacity_uprate_mw',
         'planned_net_winter_capacity_uprate_mw', 'planned_new_capacity_mw',
         'planned_uprate_date', 'planned_net_summer_capacity_derate_mw',
         'planned_net_winter_capacity_derate_mw', 'planned_derate_date',
         'planned_new_prime_mover_code', 'planned_energy_source_code_1',
         'planned_repower_date', 'other_planned_modifications',
         'other_modifications_date', 'planned_retirement_date',
         'carbon_capture', 'cofire_fuels', 'switch_oil_gas',
         'turbines_inverters_hydrokinetics', 'nameplate_power_factor',
         'uprate_derate_during_year', 'uprate_derate_completed_date',
         'current_planned_operating_date', 'summer_estimated_capability_mw',
         'winter_estimated_capability_mw', 'retirement_date',
         'utility_id_eia', 'data_source'],
        # need type fixing
        {}
    ),
    # utilities must come after plants. plant location needs to be
    # removed before the utility locations are compiled
    'utilities': (
        # base cols
        ['utility_id_eia'],
        # static cols
        ['utility_name_eia'],
        # annual cols
        ['street_address', 'city', 'state', 'zip_code', 'entity_type',
         'plants_reported_owner', 'plants_reported_operator',
         'plants_reported_asset_manager', 'plants_reported_other_relationship',
         'attention_line', 'address_2', 'zip_code_4',
         'contact_firstname', 'contact_lastname', 'contact_title',
         'contact_firstname_2', 'contact_lastname_2', 'contact_title_2',
         'phone_extension', 'phone_extension_2', 'phone_number',
         'phone_number_2'],
        # need type fixing
        {'utility_id_eia': 'int64'},
    ),
    'boilers': (
        # base cols
        ['plant_id_eia', 'boiler_id'],
        # static cols
        ['prime_mover_code'],
        # annual cols
        [],
        # need type fixing
        {},
    )
}
"""
Columns kept for either entity or annual EIA tables in the harvesting process.

For each entity type (key), the ID columns, static columns, and annual columns,
followed by any custom data type fixes.

The order of the entities matters. Plants must be harvested before utilities,
since plant location must be removed before the utility locations are harvested.
"""


class Partition(TypedDict, total=False):
    """Data partition."""

    years: Tuple[int, ...]
    year_month: str
    states: Tuple[str, ...]


WORKING_PARTITIONS: Dict[str, Partition] = {
    'eia860': {
        'years': tuple(range(2001, 2021))
    },
    'eia860m': {
        'year_month': '2021-08'
    },
    'eia861': {
        'years': tuple(range(2001, 2021))
    },
    'eia923': {
        'years': tuple(range(2001, 2021))
    },
    'epacems': {
        'years': tuple(range(1995, 2021)),
        'states': tuple(EPACEMS_STATES),
    },
    'ferc1': {
        'years': tuple(range(1994, 2021))
    },
    'ferc714': {},
}
"""
Per-dataset descriptions of what raw input data partitions can be processed.

Most of our datasets are distributed in chunks that correspond to a given year,
state, or other logical partition. Not all available partitions of the raw have
data have been integrated into PUDL. The sub-keys within each dataset partition
dictionary refer to metadata in the data packages we have archived on Zenodo,
which contain the original raw input data.

Note: ferc714 is not partitioned by year and is available only as a single file
containing all data.
"""

PUDL_TABLES: Dict[str, Tuple[str]] = {
    'eia860': (
        'boiler_generator_assn_eia860',
        'utilities_eia860',
        'plants_eia860',
        'generators_eia860',
        'ownership_eia860',
    ),
    'eia861': (
        "service_territory_eia861",
        "balancing_authority_eia861",
        "sales_eia861",
        "advanced_metering_infrastructure_eia861",
        "demand_response_eia861",
        "demand_side_management_eia861",
        "distributed_generation_eia861",
        "distribution_systems_eia861",
        "dynamic_pricing_eia861",
        "energy_efficiency_eia861",
        "green_pricing_eia861",
        "mergers_eia861",
        "net_metering_eia861",
        "non_net_metering_eia861",
        "operational_data_eia861",
        "reliability_eia861",
        "utility_data_eia861",
    ),
    'eia923': (
        'generation_fuel_eia923',
        'boiler_fuel_eia923',
        'generation_eia923',
        'coalmine_eia923',
        'fuel_receipts_costs_eia923',
    ),
    'entity_tables': (
        'utilities_entity_eia',
        'plants_entity_eia',
        'generators_entity_eia',
        'boilers_entity_eia',
    ),
    'epacems': (
        "hourly_emissions_epacems",
    ),
    'ferc1': (
        'fuel_ferc1',
        'plants_steam_ferc1',
        'plants_small_ferc1',
        'plants_hydro_ferc1',
        'plants_pumped_storage_ferc1',
        'purchased_power_ferc1',
        'plant_in_service_ferc1',
    ),
    'ferc714': (
        "respondent_id_ferc714",
        "id_certification_ferc714",
        "gen_plants_ba_ferc714",
        "demand_monthly_ba_ferc714",
        "net_energy_load_ba_ferc714",
        "adjacency_ba_ferc714",
        "interchange_ba_ferc714",
        "lambda_hourly_ba_ferc714",
        "lambda_description_ferc714",
        "description_pa_ferc714",
        "demand_forecast_pa_ferc714",
        "demand_hourly_pa_ferc714",
    ),
    'glue': (
        'plants_eia',
        'plants_ferc',
        'plants',
        'utilities_eia',
        'utilities_ferc',
        'utilities',
        'utility_plant_assn',
    )
}
"""Core PUDL DB tables by data source. Used to validate ETL inputs."""
