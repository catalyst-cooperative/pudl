"""
A warehouse for constant values required to initilize the PUDL Database.

This constants module stores and organizes a bunch of constant values which are
used throughout PUDL to populate static lists within the data packages or for
data cleaning purposes.
"""

from typing import Any, Dict, List, Tuple, TypedDict

import pandas as pd

from pudl.metadata.enums import (CUSTOMER_CLASSES, EPACEMS_MEASUREMENT_CODES,
                                 EPACEMS_STATES, FUEL_CLASSES, NERC_REGIONS,
                                 RELIABILITY_STANDARDS, REVENUE_CLASSES,
                                 TECH_CLASSES)
from pudl.metadata.labels import (COALMINE_TYPES_EIA, ENTITY_TYPES,
                                  ESTIMATED_OR_ACTUAL, MOMENTARY_INTERRUPTIONS)

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

COLUMN_DTYPES: Dict[str, Dict[str, Any]] = {
    "ferc1": {  # Obviously this is not yet a complete list...
        "construction_year": pd.Int64Dtype(),
        "installation_year": pd.Int64Dtype(),
        "plant_id_ferc1": pd.Int64Dtype(),
        "plant_id_pudl": pd.Int64Dtype(),
        "report_date": "datetime64[ns]",
        "report_year": pd.Int64Dtype(),
        "utility_id_ferc1": pd.Int64Dtype(),
        "utility_id_pudl": pd.Int64Dtype(),
    },
    "ferc714": {  # INCOMPLETE
        "demand_mwh": float,
        "demand_annual_mwh": float,
        "eia_code": pd.Int64Dtype(),
        "peak_demand_summer_mw": float,
        "peak_demand_winter_mw": float,
        "report_date": "datetime64[ns]",
        "respondent_id_ferc714": pd.Int64Dtype(),
        "respondent_name_ferc714": pd.StringDtype(),
        "respondent_type": pd.CategoricalDtype(
            categories=["utility", "balancing_authority"]
        ),
        "timezone": pd.CategoricalDtype(
            categories=[
                "America/New_York",
                "America/Chicago",
                "America/Denver",
                "America/Los_Angeles",
                "America/Anchorage",
                "Pacific/Honolulu",
            ]
        ),
        "utc_datetime": "datetime64[ns]",
    },
    "epacems": {
        'state': pd.CategoricalDtype(categories=EPACEMS_STATES),
        'plant_id_eia': "int32",
        'unitid': pd.StringDtype(),
        'operating_datetime_utc': "datetime64[ns]",
        'operating_time_hours': "float32",
        'gross_load_mw': "float32",
        'steam_load_1000_lbs': "float32",
        'so2_mass_lbs': "float32",
        'so2_mass_measurement_code': pd.CategoricalDtype(
            categories=EPACEMS_MEASUREMENT_CODES
        ),
        'nox_rate_lbs_mmbtu': "float32",
        'nox_rate_measurement_code': pd.CategoricalDtype(
            categories=EPACEMS_MEASUREMENT_CODES
        ),
        'nox_mass_lbs': "float32",
        'nox_mass_measurement_code': pd.CategoricalDtype(
            categories=EPACEMS_MEASUREMENT_CODES
        ),
        'co2_mass_tons': "float32",
        'co2_mass_measurement_code': pd.CategoricalDtype(
            categories=EPACEMS_MEASUREMENT_CODES
        ),
        'heat_content_mmbtu': "float32",
        'facility_id': pd.Int32Dtype(),  # Nullable Integer
        'unit_id_epa': pd.Int32Dtype(),  # Nullable Integer
    },
    "eia": {
        'actual_peak_demand_savings_mw': float,  # Added by AES for DR table
        'address_2': pd.StringDtype(),  # Added by AES for 860 utilities table
        'advanced_metering_infrastructure': pd.Int64Dtype(),  # Added by AES for AMI table
        # Added by AES for UD misc table
        'alternative_fuel_vehicle_2_activity': pd.BooleanDtype(),
        'alternative_fuel_vehicle_activity': pd.BooleanDtype(),
        'annual_indirect_program_cost': float,
        'annual_total_cost': float,
        'ash_content_pct': float,
        'ash_impoundment': pd.BooleanDtype(),
        'ash_impoundment_lined': pd.BooleanDtype(),
        # TODO: convert this field to more descriptive words
        'ash_impoundment_status': pd.StringDtype(),
        'associated_combined_heat_power': pd.BooleanDtype(),
        'attention_line': pd.StringDtype(),
        'automated_meter_reading': pd.Int64Dtype(),  # Added by AES for AMI table
        'backup_capacity_mw': float,  # Added by AES for NNM & DG misc table
        'balancing_authority_code_eia': pd.CategoricalDtype(),
        'balancing_authority_id_eia': pd.Int64Dtype(),
        'balancing_authority_name_eia': pd.StringDtype(),
        'bga_source': pd.StringDtype(),
        'boiler_id': pd.StringDtype(),
        'bunded_activity': pd.BooleanDtype(),
        'business_model': pd.CategoricalDtype(
            categories=["retail", "energy_services"]
        ),
        'buy_distribution_activity': pd.BooleanDtype(),
        'buying_transmission_activity': pd.BooleanDtype(),
        'bypass_heat_recovery': pd.BooleanDtype(),
        'caidi_w_major_event_days_minus_loss_of_service_minutes': float,
        'caidi_w_major_event_dats_minutes': float,
        'caidi_wo_major_event_days_minutes': float,
        'capacity_mw': float,
        'carbon_capture': pd.BooleanDtype(),
        'chlorine_content_ppm': float,
        'circuits_with_voltage_optimization': pd.Int64Dtype(),
        'city': pd.StringDtype(),
        'cofire_fuels': pd.BooleanDtype(),
        'consumed_by_facility_mwh': float,
        'consumed_by_respondent_without_charge_mwh': float,
        'contact_firstname': pd.StringDtype(),
        'contact_firstname_2': pd.StringDtype(),
        'contact_lastname': pd.StringDtype(),
        'contact_lastname_2': pd.StringDtype(),
        'contact_title': pd.StringDtype(),
        'contact_title_2': pd.StringDtype(),
        'contract_expiration_date': 'datetime64[ns]',
        'contract_type_code': pd.StringDtype(),
        'county': pd.StringDtype(),
        'county_id_fips': pd.StringDtype(),  # Must preserve leading zeroes
        'credits_or_adjustments': float,
        'critical_peak_pricing': pd.BooleanDtype(),
        'critical_peak_rebate': pd.BooleanDtype(),
        'current_planned_operating_date': 'datetime64[ns]',
        'customers': float,
        'customer_class': pd.CategoricalDtype(categories=CUSTOMER_CLASSES),
        'customer_incentives_cost': float,
        'customer_incentives_incremental_cost': float,
        'customer_incentives_incremental_life_cycle_cost': float,
        'customer_other_costs_incremental_life_cycle_cost': float,
        'daily_digital_access_customers': pd.Int64Dtype(),
        'data_observed': pd.BooleanDtype(),
        'datum': pd.StringDtype(),
        'deliver_power_transgrid': pd.BooleanDtype(),
        'delivery_customers': float,
        'direct_load_control_customers': pd.Int64Dtype(),
        'distributed_generation': pd.BooleanDtype(),
        'distributed_generation_owned_capacity_mw': float,
        'distribution_activity': pd.BooleanDtype(),
        'distribution_circuits': pd.Int64Dtype(),
        'duct_burners': pd.BooleanDtype(),
        'energy_displaced_mwh': float,
        'energy_efficiency_annual_cost': float,
        'energy_efficiency_annual_actual_peak_reduction_mw': float,
        'energy_efficiency_annual_effects_mwh': float,
        'energy_efficiency_annual_incentive_payment': float,
        'energy_efficiency_incremental_actual_peak_reduction_mw': float,
        'energy_efficiency_incremental_effects_mwh': float,
        'energy_savings_estimates_independently_verified': pd.BooleanDtype(),
        'energy_savings_independently_verified': pd.BooleanDtype(),
        'energy_savings_mwh': float,
        'energy_served_ami_mwh': float,
        'energy_source_1_transport_1': pd.StringDtype(),
        'energy_source_1_transport_2': pd.StringDtype(),
        'energy_source_1_transport_3': pd.StringDtype(),
        'energy_source_2_transport_1': pd.StringDtype(),
        'energy_source_2_transport_2': pd.StringDtype(),
        'energy_source_2_transport_3': pd.StringDtype(),
        'energy_source_code': pd.StringDtype(),
        'energy_source_code_1': pd.StringDtype(),
        'energy_source_code_2': pd.StringDtype(),
        'energy_source_code_3': pd.StringDtype(),
        'energy_source_code_4': pd.StringDtype(),
        'energy_source_code_5': pd.StringDtype(),
        'energy_source_code_6': pd.StringDtype(),
        'energy_storage': pd.BooleanDtype(),
        'entity_type': pd.CategoricalDtype(categories=ENTITY_TYPES.values()),
        'estimated_or_actual_capacity_data': pd.CategoricalDtype(
            categories=ESTIMATED_OR_ACTUAL.values()
        ),
        'estimated_or_actual_fuel_data': pd.CategoricalDtype(
            categories=ESTIMATED_OR_ACTUAL.values()
        ),
        'estimated_or_actual_tech_data': pd.CategoricalDtype(
            categories=ESTIMATED_OR_ACTUAL.values()
        ),
        'exchange_energy_delivered_mwh': float,
        'exchange_energy_recieved_mwh': float,
        'ferc_cogen_docket_no': pd.StringDtype(),
        'ferc_cogen_status': pd.BooleanDtype(),
        'ferc_exempt_wholesale_generator': pd.BooleanDtype(),
        'ferc_exempt_wholesale_generator_docket_no': pd.StringDtype(),
        'ferc_small_power_producer': pd.BooleanDtype(),
        'ferc_small_power_producer_docket_no': pd.StringDtype(),
        'fluidized_bed_tech': pd.BooleanDtype(),
        'fraction_owned': float,
        'fuel_class': pd.CategoricalDtype(categories=FUEL_CLASSES),
        'fuel_consumed_for_electricity_mmbtu': float,
        'fuel_consumed_for_electricity_units': float,
        'fuel_consumed_mmbtu': float,
        'fuel_consumed_units': float,
        'fuel_cost_per_mmbtu': float,
        'fuel_group_code': pd.StringDtype(),
        'fuel_group_code_simple': pd.StringDtype(),
        'fuel_mmbtu_per_unit': float,
        'fuel_pct': float,
        'fuel_received_units': float,
        'fuel_type_code_aer': pd.StringDtype(),
        'fuel_type_code_pudl': pd.StringDtype(),
        'furnished_without_charge_mwh': float,
        'generation_activity': pd.BooleanDtype(),
        # this is a mix of integer-like values (2 or 5) and strings like AUGSF
        'generator_id': pd.StringDtype(),
        'generators_number': float,
        'generators_num_less_1_mw': float,
        'green_pricing_revenue': float,
        'grid_voltage_2_kv': float,
        'grid_voltage_3_kv': float,
        'grid_voltage_kv': float,
        'highest_distribution_voltage_kv': float,
        'home_area_network': pd.Int64Dtype(),
        'inactive_accounts_included': pd.BooleanDtype(),
        'incremental_energy_savings_mwh': float,
        'incremental_life_cycle_energy_savings_mwh': float,
        'incremental_life_cycle_peak_reduction_mwh': float,
        'incremental_peak_reduction_mw': float,
        'iso_rto_code': pd.StringDtype(),
        'latitude': float,
        'liquefied_natural_gas_storage': pd.BooleanDtype(),
        'load_management_annual_cost': float,
        'load_management_annual_actual_peak_reduction_mw': float,
        'load_management_annual_effects_mwh': float,
        'load_management_annual_incentive_payment': float,
        'load_management_annual_potential_peak_reduction_mw': float,
        'load_management_incremental_actual_peak_reduction_mw': float,
        'load_management_incremental_effects_mwh': float,
        'load_management_incremental_potential_peak_reduction_mw': float,
        'longitude': float,
        'major_program_changes': pd.BooleanDtype(),
        'mercury_content_ppm': float,
        'merge_address': pd.StringDtype(),
        'merge_city': pd.StringDtype(),
        'merge_company': pd.StringDtype(),
        'merge_date': 'datetime64[ns]',
        'merge_state': pd.StringDtype(),
        'mine_id_msha': pd.Int64Dtype(),
        'mine_id_pudl': pd.Int64Dtype(),
        'mine_name': pd.StringDtype(),
        'mine_type': pd.CategoricalDtype(
            categories=COALMINE_TYPES_EIA.values()
        ),
        'minimum_load_mw': float,
        'moisture_content_pct': float,
        'momentary_interruption_definition': pd.CategoricalDtype(
            categories=MOMENTARY_INTERRUPTIONS.values()
        ),
        'multiple_fuels': pd.BooleanDtype(),
        'nameplate_power_factor': float,
        'natural_gas_delivery_contract_type_code': pd.StringDtype(),
        'natural_gas_local_distribution_company': pd.StringDtype(),
        'natural_gas_pipeline_name_1': pd.StringDtype(),
        'natural_gas_pipeline_name_2': pd.StringDtype(),
        'natural_gas_pipeline_name_3': pd.StringDtype(),
        'natural_gas_storage': pd.BooleanDtype(),
        'natural_gas_transport_code': pd.StringDtype(),
        'nerc_region': pd.CategoricalDtype(categories=NERC_REGIONS),
        'nerc_regions_of_operation': pd.CategoricalDtype(categories=NERC_REGIONS),
        'net_generation_mwh': float,
        'net_metering': pd.BooleanDtype(),
        'net_power_exchanged_mwh': float,
        'net_wheeled_power_mwh': float,
        'new_parent': pd.StringDtype(),
        'non_amr_ami': pd.Int64Dtype(),
        'nuclear_unit_id': pd.StringDtype(),
        'operates_generating_plant': pd.BooleanDtype(),
        'operating_date': 'datetime64[ns]',
        'operating_switch': pd.StringDtype(),
        # TODO: double check this for early 860 years
        'operational_status': pd.StringDtype(),
        'operational_status_code': pd.StringDtype(),
        'original_planned_operating_date': 'datetime64[ns]',
        'other': float,
        'other_combustion_tech': pd.BooleanDtype(),
        'other_costs': float,
        'other_costs_incremental_cost': float,
        'other_modifications_date': 'datetime64[ns]',
        'other_planned_modifications': pd.BooleanDtype(),
        'outages_recorded_automatically': pd.BooleanDtype(),
        'owned_by_non_utility': pd.BooleanDtype(),
        'owner_city': pd.StringDtype(),
        'owner_name': pd.StringDtype(),
        'owner_state': pd.StringDtype(),
        'owner_street_address': pd.StringDtype(),
        'owner_utility_id_eia': pd.Int64Dtype(),
        'owner_zip_code': pd.StringDtype(),
        # we should transition these into readable codes, not a one letter thing
        'ownership_code': pd.StringDtype(),
        'phone_extension': pd.StringDtype(),
        'phone_extension_2': pd.StringDtype(),
        'phone_number': pd.StringDtype(),
        'phone_number_2': pd.StringDtype(),
        'pipeline_notes': pd.StringDtype(),
        'planned_derate_date': 'datetime64[ns]',
        'planned_energy_source_code_1': pd.StringDtype(),
        'planned_modifications': pd.BooleanDtype(),
        'planned_net_summer_capacity_derate_mw': float,
        'planned_net_summer_capacity_uprate_mw': float,
        'planned_net_winter_capacity_derate_mw': float,
        'planned_net_winter_capacity_uprate_mw': float,
        'planned_new_capacity_mw': float,
        'planned_new_prime_mover_code': pd.StringDtype(),
        'planned_repower_date': 'datetime64[ns]',
        'planned_retirement_date': 'datetime64[ns]',
        'planned_uprate_date': 'datetime64[ns]',
        'plant_id_eia': pd.Int64Dtype(),
        'plant_id_epa': pd.Int64Dtype(),
        'plant_id_pudl': pd.Int64Dtype(),
        'plant_name_eia': pd.StringDtype(),
        'plants_reported_asset_manager': pd.BooleanDtype(),
        'plants_reported_operator': pd.BooleanDtype(),
        'plants_reported_other_relationship': pd.BooleanDtype(),
        'plants_reported_owner': pd.BooleanDtype(),
        'point_source_unit_id_epa': pd.StringDtype(),
        'potential_peak_demand_savings_mw': float,
        'pulverized_coal_tech': pd.BooleanDtype(),
        'previously_canceled': pd.BooleanDtype(),
        'price_responsive_programes': pd.BooleanDtype(),
        'price_responsiveness_customers': pd.Int64Dtype(),
        'primary_transportation_mode_code': pd.StringDtype(),
        'primary_purpose_id_naics': pd.Int64Dtype(),
        'prime_mover_code': pd.StringDtype(),
        'pv_current_flow_type': pd.CategoricalDtype(categories=['AC', 'DC']),
        'reactive_power_output_mvar': float,
        'real_time_pricing_program': pd.BooleanDtype(),
        'rec_revenue': float,
        'rec_sales_mwh': float,
        'regulatory_status_code': pd.StringDtype(),
        'report_date': 'datetime64[ns]',
        'reported_as_another_company': pd.StringDtype(),
        'respondent_frequency': pd.CategoricalDtype(categories=["A", "M", "AM"]),
        'retail_marketing_activity': pd.BooleanDtype(),
        'retail_sales': float,
        'retail_sales_mwh': float,
        'retirement_date': 'datetime64[ns]',
        'revenue_class': pd.CategoricalDtype(categories=REVENUE_CLASSES),
        'rto_iso_lmp_node_id': pd.StringDtype(),
        'rto_iso_location_wholesale_reporting_id': pd.StringDtype(),
        'rtos_of_operation': pd.StringDtype(),
        'saidi_w_major_event_dats_minus_loss_of_service_minutes': float,
        'saidi_w_major_event_days_minutes': float,
        'saidi_wo_major_event_days_minutes': float,
        'saifi_w_major_event_days_customers': float,
        'saifi_w_major_event_days_minus_loss_of_service_customers': float,
        'saifi_wo_major_event_days_customers': float,
        'sales_for_resale': float,
        'sales_for_resale_mwh': float,
        'sales_mwh': float,
        'sales_revenue': float,
        'sales_to_ultimate_consumers_mwh': float,
        'secondary_transportation_mode_code': pd.StringDtype(),
        'sector_id_eia': pd.Int64Dtype(),
        'sector_name_eia': pd.StringDtype(),
        'service_area': pd.StringDtype(),
        'service_type': pd.CategoricalDtype(
            categories=["bundled", "energy", "delivery"]
        ),
        'short_form': pd.BooleanDtype(),
        'sold_to_utility_mwh': float,
        'solid_fuel_gasification': pd.BooleanDtype(),
        'data_source': pd.StringDtype(),
        'standard': pd.CategoricalDtype(categories=RELIABILITY_STANDARDS),
        'startup_source_code_1': pd.StringDtype(),
        'startup_source_code_2': pd.StringDtype(),
        'startup_source_code_3': pd.StringDtype(),
        'startup_source_code_4': pd.StringDtype(),
        'state': pd.StringDtype(),
        'state_id_fips': pd.StringDtype(),  # Must preserve leading zeroes
        'street_address': pd.StringDtype(),
        'stoker_tech': pd.BooleanDtype(),
        'storage_capacity_mw': float,
        'storage_customers': pd.Int64Dtype(),
        'subcritical_tech': pd.BooleanDtype(),
        'sulfur_content_pct': float,
        'summer_capacity_mw': float,
        'summer_capacity_estimate': pd.BooleanDtype(),
        # TODO: check if there is any data pre-2016
        'summer_estimated_capability_mw': float,
        'summer_peak_demand_mw': float,
        'supercritical_tech': pd.BooleanDtype(),
        'supplier_name': pd.StringDtype(),
        'switch_oil_gas': pd.BooleanDtype(),
        'syncronized_transmission_grid': pd.BooleanDtype(),
        # Added by AES for NM & DG tech table (might want to consider merging with another fuel label)
        'tech_class': pd.CategoricalDtype(categories=TECH_CLASSES),
        'technology_description': pd.StringDtype(),
        'time_cold_shutdown_full_load_code': pd.StringDtype(),
        'time_of_use_pricing_program': pd.BooleanDtype(),
        'time_responsive_programs': pd.BooleanDtype(),
        'time_responsiveness_customers': pd.Int64Dtype(),
        'timezone': pd.StringDtype(),
        'topping_bottoming_code': pd.StringDtype(),
        'total': float,
        'total_capacity_less_1_mw': float,
        'total_meters': pd.Int64Dtype(),
        'total_disposition_mwh': float,
        'total_energy_losses_mwh': float,
        'total_sources_mwh': float,
        'transmission': float,
        'transmission_activity': pd.BooleanDtype(),
        'transmission_by_other_losses_mwh': float,
        'transmission_distribution_owner_id': pd.Int64Dtype(),
        'transmission_distribution_owner_name': pd.StringDtype(),
        'transmission_distribution_owner_state': pd.StringDtype(),
        'turbines_inverters_hydrokinetics': float,
        'turbines_num': pd.Int64Dtype(),  # TODO: check if any turbines show up pre-2016
        'ultrasupercritical_tech': pd.BooleanDtype(),
        'unbundled_revenues': float,
        'unit_id_eia': pd.StringDtype(),
        'unit_id_pudl': pd.Int64Dtype(),
        'uprate_derate_completed_date': 'datetime64[ns]',
        'uprate_derate_during_year': pd.BooleanDtype(),
        'utility_id_eia': pd.Int64Dtype(),
        'utility_id_pudl': pd.Int64Dtype(),
        'utility_name_eia': pd.StringDtype(),
        'utility_owned_capacity_mw': float,  # Added by AES for NNM table
        'variable_peak_pricing_program': pd.BooleanDtype(),  # Added by AES for DP table
        'virtual_capacity_mw': float,  # Added by AES for NM table
        'virtual_customers': pd.Int64Dtype(),  # Added by AES for NM table
        'water_heater': pd.Int64Dtype(),  # Added by AES for DR table
        'water_source': pd.StringDtype(),
        'weighted_average_life_years': float,
        'wheeled_power_delivered_mwh': float,
        'wheeled_power_recieved_mwh': float,
        'wholesale_marketing_activity': pd.BooleanDtype(),
        'wholesale_power_purchases_mwh': float,
        'winter_capacity_mw': float,
        'winter_capacity_estimate': pd.BooleanDtype(),
        'winter_estimated_capability_mw': float,
        'winter_peak_demand_mw': float,
        # 'with_med': float,
        # 'with_med_minus_los': float,
        # 'without_med': float,
        'zip_code': pd.StringDtype(),
        'zip_code_4': pd.StringDtype()
    },
    'depreciation': {
        'utility_id_ferc1': pd.Int64Dtype(),
        'utility_id_pudl': pd.Int64Dtype(),
        'plant_id_pudl': pd.Int64Dtype(),
        # 'plant_name': pd.StringDtype(),
        'note': pd.StringDtype(),
        'report_year': int,
        'report_date': 'datetime64[ns]',
        'common': pd.BooleanDtype(),
        'plant_balance': float,
        'book_reserve': float,
        'unaccrued_balance': float,
        'reserve_pct': float,
        # 'survivor_curve_type': pd.StringDtype(),
        'service_life_avg': float,
        'net_salvage_pct': float,
        'net_salvage_rate_type_pct': pd.BooleanDtype(),
        'net_removal': float,
        'net_removal_pct': float,
        'remaining_life_avg': float,
        # 'retirement_date': 'datetime64[ns]',
        'depreciation_annual_epxns': float,
        'depreciation_annual_pct': float,
        'depreciation_annual_rate_type_pct': pd.BooleanDtype(),
        # 'data_source': pd.StringDtype(),
    }
}
