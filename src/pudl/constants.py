"""
A warehouse for constant values required to initilize the PUDL Database.

This constants module stores and organizes a bunch of constant values which are
used throughout PUDL to populate static lists within the data packages or for
data cleaning purposes.
"""

from typing import Any, Dict, List, Optional, Tuple, TypedDict

import pandas as pd

from pudl.metadata import Resource
from pudl.metadata.enums import (CUSTOMER_CLASSES, EPACEMS_MEASUREMENT_CODES,
                                 FUEL_CLASSES, NERC_REGIONS,
                                 RELIABILITY_STANDARDS, REVENUE_CLASSES,
                                 TECH_CLASSES)
from pudl.metadata.labels import (ENTITY_TYPES, ESTIMATED_OR_ACTUAL,
                                  FUEL_TRANSPORTATION_MODES_EIA,
                                  MOMENTARY_INTERRUPTIONS)

US_STATES_TERRITORIES: Dict[str, str] = {
    'AK': 'Alaska',
    'AL': 'Alabama',
    'AR': 'Arkansas',
    'AS': 'American Samoa',
    'AZ': 'Arizona',
    'CA': 'California',
    'CO': 'Colorado',
    'CT': 'Connecticut',
    'DC': 'District of Columbia',
    'DE': 'Delaware',
    'FL': 'Florida',
    'GA': 'Georgia',
    'GU': 'Guam',
    'HI': 'Hawaii',
    'IA': 'Iowa',
    'ID': 'Idaho',
    'IL': 'Illinois',
    'IN': 'Indiana',
    'KS': 'Kansas',
    'KY': 'Kentucky',
    'LA': 'Louisiana',
    'MA': 'Massachusetts',
    'MD': 'Maryland',
    'ME': 'Maine',
    'MI': 'Michigan',
    'MN': 'Minnesota',
    'MO': 'Missouri',
    'MP': 'Northern Mariana Islands',
    'MS': 'Mississippi',
    'MT': 'Montana',
    'NA': 'National',
    'NC': 'North Carolina',
    'ND': 'North Dakota',
    'NE': 'Nebraska',
    'NH': 'New Hampshire',
    'NJ': 'New Jersey',
    'NM': 'New Mexico',
    'NV': 'Nevada',
    'NY': 'New York',
    'OH': 'Ohio',
    'OK': 'Oklahoma',
    'OR': 'Oregon',
    'PA': 'Pennsylvania',
    'PR': 'Puerto Rico',
    'RI': 'Rhode Island',
    'SC': 'South Carolina',
    'SD': 'South Dakota',
    'TN': 'Tennessee',
    'TX': 'Texas',
    'UT': 'Utah',
    'VA': 'Virginia',
    'VI': 'Virgin Islands',
    'VT': 'Vermont',
    'WA': 'Washington',
    'WI': 'Wisconsin',
    'WV': 'West Virginia',
    'WY': 'Wyoming'
}
"""Mapping of US state and territory abbreviations to their full names."""

CANADA_PROVINCES_TERRITORIES: Dict[str, str] = {
    'AB': 'Alberta',
    'BC': 'British Columbia',
    'CN': 'Canada',
    'MB': 'Manitoba',
    'NB': 'New Brunswick',
    'NS': 'Nova Scotia',
    'NL': 'Newfoundland and Labrador',
    'NT': 'Northwest Territories',
    'NU': 'Nunavut',
    'ON': 'Ontario',
    'PE': 'Prince Edwards Island',
    'QC': 'Quebec',
    'SK': 'Saskatchewan',
    'YT': 'Yukon Territory',
}
"""Mapping of Canadian province and territory abbreviations to their full names"""

EPACEMS_STATES: Dict[str, str] = {
    k: v for k, v in US_STATES_TERRITORIES.items()
    if k not in {'AK', 'AS', 'GU', 'HI', 'MP', 'PR', 'VI', 'NA'}
}
"""The US states and territories that are present in the EPA CEMS dataset."""

APPROXIMATE_TIMEZONES: Dict[str, str] = {
    "AK": "US/Alaska",            # Alaska; Not in CEMS
    "AL": "US/Central",           # Alabama
    "AR": "US/Central",           # Arkansas
    "AS": "Pacific/Pago_Pago",    # American Samoa; Not in CEMS
    "AZ": "US/Arizona",           # Arizona
    "CA": "US/Pacific",           # California
    "CO": "US/Mountain",          # Colorado
    "CT": "US/Eastern",           # Connecticut
    "DC": "US/Eastern",           # District of Columbia
    "DE": "US/Eastern",           # Delaware
    "FL": "US/Eastern",           # Florida (split state)
    "GA": "US/Eastern",           # Georgia
    "GU": "Pacific/Guam",         # Guam; Not in CEMS
    "HI": "US/Hawaii",            # Hawaii; Not in CEMS
    "IA": "US/Central",           # Iowa
    "ID": "US/Mountain",          # Idaho (split state)
    "IL": "US/Central",           # Illinois
    "IN": "US/Eastern",           # Indiana (split state)
    "KS": "US/Central",           # Kansas (split state)
    "KY": "US/Eastern",           # Kentucky (split state)
    "LA": "US/Central",           # Louisiana
    "MA": "US/Eastern",           # Massachusetts
    "MD": "US/Eastern",           # Maryland
    "ME": "US/Eastern",           # Maine
    "MI": "America/Detroit",      # Michigan (split state)
    "MN": "US/Central",           # Minnesota
    "MO": "US/Central",           # Missouri
    "MP": "Pacific/Saipan",       # Northern Mariana Islands; Not in CEMS
    "MS": "US/Central",           # Mississippi
    "MT": "US/Mountain",          # Montana
    "NC": "US/Eastern",           # North Carolina
    "ND": "US/Central",           # North Dakota (split state)
    "NE": "US/Central",           # Nebraska (split state)
    "NH": "US/Eastern",           # New Hampshire
    "NJ": "US/Eastern",           # New Jersey
    "NM": "US/Mountain",          # New Mexico
    "NV": "US/Pacific",           # Nevada
    "NY": "US/Eastern",           # New York
    "OH": "US/Eastern",           # Ohio
    "OK": "US/Central",           # Oklahoma
    "OR": "US/Pacific",           # Oregon (split state)
    "PA": "US/Eastern",           # Pennsylvania
    "PR": "America/Puerto_Rico",  # Puerto Rico; Not in CEMS
    "RI": "US/Eastern",           # Rhode Island
    "SC": "US/Eastern",           # South Carolina
    "SD": "US/Central",           # South Dakota (split state)
    "TN": "US/Central",           # Tennessee
    "TX": "US/Central",           # Texas
    "UT": "US/Mountain",          # Utah
    "VA": "US/Eastern",           # Virginia
    "VI": "America/Puerto_Rico",  # Virgin Islands; Not in CEMS
    "VT": "US/Eastern",           # Vermont
    "WA": "US/Pacific",           # Washington
    "WI": "US/Central",           # Wisconsin
    "WV": "US/Eastern",           # West Virginia
    "WY": "US/Mountain",          # Wyoming
    # Canada (none of these are in CEMS)
    "AB": "America/Edmonton",     # Alberta
    "BC": "America/Vancouver",    # British Columbia (split province)
    "MB": "America/Winnipeg",     # Manitoba
    "NB": "America/Moncton",      # New Brunswick
    "NS": "America/Halifax",      # Nova Scotia
    "NL": "America/St_Johns",     # Newfoundland and Labrador  (split province)
    "NT": "America/Yellowknife",  # Northwest Territories (split province)
    "NU": "America/Iqaluit",      # Nunavut (split province)
    "ON": "America/Toronto",      # Ontario (split province)
    "PE": "America/Halifax",      # Prince Edwards Island
    "QC": "America/Montreal",     # Quebec (split province)
    "SK": "America/Regina",       # Saskatchewan  (split province)
    "YT": "America/Whitehorse",   # Yukon Territory
}
"""
Approximate mapping of US & Canadian jurisdictions to canonical timezones

This is imperfect for states that have split timezones. See:
https://en.wikipedia.org/wiki/List_of_time_offsets_by_U.S._state_and_territory
For states that are split, the timezone that has more people in it.
List of timezones in pytz.common_timezones
Canada: https://en.wikipedia.org/wiki/Time_in_Canada#IANA_time_zone_database
"""

FUEL_TYPE_EIA923_GEN_FUEL_SIMPLE_MAP: Dict[str, Tuple[str, ...]] = {
    'coal': ('ant', 'bit', 'cbl', 'lig', 'pc', 'rc', 'sc', 'sub', 'wc'),
    'oil': ('dfo', 'rfo', 'wo', 'jf', 'ker'),
    'gas': ('bfg', 'lfg', 'ng', 'og', 'obg', 'pg', 'sgc', 'sgp'),
    'solar': ('sun', ),
    'wind': ('wnd', ),
    'hydro': ('wat', ),
    'nuclear': ('nuc', ),
    'waste': ('ab', 'blq', 'msb', 'msn', 'msw', 'obl', 'obs', 'slw', 'tdf', 'wdl', 'wds'),
    'other': ('geo', 'mwh', 'oth', 'pur', 'wh'),
}
"""Simplified grouping of fuel codes found in the generation_fuel_eia923 table."""

FUEL_TYPE_EIA923_BOILER_FUEL_SIMPLE_MAP: Dict[str, Tuple[str, ...]] = {
    'coal': ('ant', 'bit', 'lig', 'pc', 'rc', 'sc', 'sub', 'wc'),
    'oil': ('dfo', 'rfo', 'wo', 'jf', 'ker'),
    'gas': ('bfg', 'lfg', 'ng', 'og', 'obg', 'pg', 'sgc', 'sgp'),
    'waste': ('ab', 'blq', 'msb', 'msn', 'obl', 'obs', 'slw', 'tdf', 'wdl', 'wds'),
    'other': ('oth', 'pur', 'wh'),
}
"""Simplified grouping of fuel codes found in the boiler_fuel_eia923 table."""

AER_FUEL_TYPE_STRINGS: Dict[str, Tuple[str, ...]] = {
    'coal': ('col', 'woc', 'pc'),
    'gas': ('mlg', 'ng', 'oog'),
    'oil': ('dfo', 'rfo', 'woo'),
    'solar': ('sun', ),
    'wind': ('wnd', ),
    'hydro': ('hps', 'hyc'),
    'nuclear': ('nuc', ),
    'waste': ('www', ),
    'other': ('geo', 'orw', 'oth'),
}
"""
Consolidation of AER fuel types into energy_sources_eia' categories.

These classifications are not currently used, as the EIA fuel type and energy
source designations provide more detailed information.
"""

FUEL_TYPE_EIA860_SIMPLE_MAP: Dict[str, Tuple[str, ...]] = {
    'coal': ('ant', 'bit', 'cbl', 'lig', 'pc', 'rc', 'sc', 'sub', 'wc', 'coal', 'petroleum coke', 'col', 'woc'),
    'oil': ('dfo', 'jf', 'ker', 'rfo', 'wo', 'woo', 'petroleum'),
    'gas': ('bfg', 'lfg', 'mlg', 'ng', 'obg', 'og', 'pg', 'sgc', 'sgp', 'natural gas', 'other gas', 'oog', 'sg'),
    'solar': ('sun', 'solar'),
    'wind': ('wnd', 'wind', 'wt'),
    'hydro': ('wat', 'hyc', 'hps', 'hydro'),
    'nuclear': ('nuc', 'nuclear'),
    'waste': ('ab', 'blq', 'bm', 'msb', 'msn', 'obl', 'obs', 'slw', 'tdf', 'wdl', 'wds', 'biomass', 'msw', 'www'),
    'other': ('mwh', 'oth', 'pur', 'wh', 'geo', 'none', 'orw', 'other')
}
"""Simplified grouping of fuel codes found in the generators_eia860 table."""

ENTITIES: Dict[str, Tuple[List[str], List[str], List[str], Dict[str, str]]] = {}
"""
Columns kept for either entity or annual EIA tables in the harvesting process.

For each entity type (key), the ID columns, static columns, and annual columns,
followed by any custom data type fixes.

The order of the entities matters. Plants must be harvested before utilities,
since plant location must be removed before the utility locations are harvested.
"""

for entity in ('plants', 'generators', 'utilities', 'boilers'):
    # Static
    resource = Resource.from_id(f'{entity}_entity_eia')
    ids = resource.schema.primary_key
    static = [
        field.name for field in resource.schema.fields
        if field.name not in resource.schema.primary_key + ['timezone']
    ]
    # Annual
    try:
        resource = Resource.from_id(f'{entity}_eia860')
        annual = [
            field.name for field in resource.schema.fields
            if field.name not in resource.schema.primary_key
        ]
    except KeyError:
        annual = []
    dtypes = {'utility_id_eia': 'int64'} if entity == 'utilities' else {}
    ENTITIES[entity] = ids, static, annual, dtypes

DATA_YEARS: Dict[str, Optional[Tuple[int, ...]]] = {
    'eia860': tuple(range(2001, 2020)),
    'eia861': tuple(range(1990, 2020)),
    'eia923': tuple(range(2001, 2020)),
    'epacems': tuple(range(1995, 2021)),
    'ferc1': tuple(range(1994, 2020)),
    'ferc714': None,
}
"""
What years of raw input data are available for download from each dataset.

Note: ferc714 is not partitioned by year and is available only as a single file
containing all data.
"""


class Partition(TypedDict, total=False):
    """Data partition."""

    years: Tuple[int, ...]
    year_month: str
    states: Tuple[str, ...]


WORKING_PARTITIONS: Dict[str, Partition] = {
    'eia860': {
        'years': tuple(range(2001, 2020))
    },
    'eia860m': {
        'year_month': '2020-11'
    },
    'eia861': {
        'years': tuple(range(2001, 2020))
    },
    'eia923': {
        'years': tuple(range(2001, 2020))
    },
    'epacems': {
        'years': tuple(range(1995, 2021)),
        'states': tuple(EPACEMS_STATES.keys())},
    'ferc1': {
        'years': tuple(range(1994, 2020))
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

PUDL_TABLES: Dict[str, Tuple[str, ...]] = {
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
        "hourly_emissions_epacems"
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
    ),
}
"""Tables that are available in the PUDL DB, organized by data source."""

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
        'state': pd.CategoricalDtype(categories=EPACEMS_STATES.keys()),
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
        'energy_source_1_transport_1': pd.CategoricalDtype(
            categories=set(FUEL_TRANSPORTATION_MODES_EIA.values())
        ),
        'energy_source_1_transport_2': pd.CategoricalDtype(
            categories=set(FUEL_TRANSPORTATION_MODES_EIA.values())
        ),
        'energy_source_1_transport_3': pd.CategoricalDtype(
            categories=set(FUEL_TRANSPORTATION_MODES_EIA.values())
        ),
        'energy_source_2_transport_1': pd.CategoricalDtype(
            categories=set(FUEL_TRANSPORTATION_MODES_EIA.values())
        ),
        'energy_source_2_transport_2': pd.CategoricalDtype(
            categories=set(FUEL_TRANSPORTATION_MODES_EIA.values())
        ),
        'energy_source_2_transport_3': pd.CategoricalDtype(
            categories=set(FUEL_TRANSPORTATION_MODES_EIA.values())
        ),
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
        'fuel_qty_units': float,
        # are fuel_type and fuel_type_code the same??
        # fuel_type includes 40 code-like things.. WAT, SUN, NUC, etc.
        'fuel_type': pd.StringDtype(),
        # from the boiler_fuel_eia923 table, there are 30 code-like things, like NG, BIT, LIG
        'fuel_type_code': pd.StringDtype(),
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
        'heat_content_mmbtu_per_unit': float,
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
        'mine_type_code': pd.StringDtype(),
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
        'nuclear_unit_id': pd.Int64Dtype(),
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
        'primary_transportation_mode_code': pd.CategoricalDtype(
            categories=set(FUEL_TRANSPORTATION_MODES_EIA.values())
        ),
        'primary_purpose_naics_id': pd.Int64Dtype(),
        'prime_mover_code': pd.StringDtype(),
        'pv_current_flow_type': pd.CategoricalDtype(categories=['AC', 'DC']),
        'reactive_power_output_mvar': float,
        'real_time_pricing_program': pd.BooleanDtype(),
        'rec_revenue': float,
        'rec_sales_mwh': float,
        'regulatory_status_code': pd.StringDtype(),
        'report_date': 'datetime64[ns]',
        'reported_as_another_company': pd.StringDtype(),
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
        'secondary_transportation_mode_code': pd.CategoricalDtype(
            categories=set(FUEL_TRANSPORTATION_MODES_EIA.values())
        ),
        'sector_id': pd.Int64Dtype(),
        'sector_name': pd.StringDtype(),
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
