"""Table definitions for EIA AEO resources."""

from typing import Any

# 2024-04-24: workaround so we can *define* a bunch of schemas without
# documenting them or expecting assets to exist
_STAGING_RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_eiaaeo__generation_electric_sector": {
        "description": (
            "Projected generation capacity & total generation in the electric "
            "sector."
        ),
        "schema": {
            "fields": [
                "report_year",
                "electricity_market_module_region",
                "aeo_case",
                "projection_date",
                "aeo_electricity_sector_fuel_type",
                "net_summer_capacity_mw",  # should this be capacity_mw_eia?
                # should we include "first modeled year" as a field, to help interpret all the cumulative fields?
                "net_summer_capacity_cumulative_planned_additions_mw",
                "net_summer_capacity_cumulative_unplanned_additions_mw",
                "net_summer_capacity_cumulative_total_additions_mw",  # this is just planned + unplanned - do we even care about publishing this?
                "net_summer_capacity_cumulative_retirements_mw",
                "gross_generation_mwh",  # chose gross bc the numbers add up to "total generation" which then equals "sales to customers + generation for own use" - but... is that right?
            ],
            "primary_key": [
                "report_year",
                "electricity_market_module_region",
                "aeo_case",
                "projection_date",
                "aeo_electricity_sector_fuel_type",
            ],
        },
        "sources": ["eiaaeo"],
        "field_namespace": "eiaaeo",
        "etl_group": "eiaaeo",
    },
    "core_eiaaeo__generation_end_use_sector": {
        "description": (
            "Projected generation capacity & total generation in the end-use "
            "sector.\n"
            "Includes combined-heat-and-power plants and electricity-only "
            "plants in the commercial and industrial sectors; and small on-site "
            "generating systems in the residential, commercial, and industrial "
            "sectors used primarily for own-use generation, but which may also "
            "sell some power to the grid. Solar photovoltaic capacity portion of "
            "Renewable Sources in gigawatts direct current; other technologies "
            "in gigawatts alternating current."
        ),
        "schema": {
            "fields": [
                "report_year",
                "electricity_market_module_region",
                "aeo_case",
                "projection_date",
                "aeo_end_use_sector_fuel_type",
                "net_summer_capacity_mw",  # should this be capacity_mw_eia?
                "gross_generation_mwh",
            ],
            "primary_key": [
                "report_year",
                "electricity_market_module_region",
                "aeo_case",
                "projection_date",
                "aeo_end_use_sector_fuel_type",
            ],
        },
        "sources": ["eiaaeo"],
        "field_namespace": "eiaaeo",
        "etl_group": "eiaaeo",
    },
    "core_eiaaeo__electricity_sales": {
        "description": "Projected electricity sales.",
        "schema": {
            "fields": [
                "report_year",
                "electricity_market_module_region",
                "aeo_case",
                "projection_date",
                "aeo_sales_sector",
                "sales_mwh",
            ],
            "primary_key": [
                "report_year",
                "electricity_market_module_region",
                "aeo_case",
                "projection_date",
                "aeo_sales_sector",
            ],
        },
        "sources": ["eiaaeo"],
        "field_namespace": "eiaaeo",
        "etl_group": "eiaaeo",
    },
    "core_eiaaeo__net_energy_for_load": {
        "description": "Projected net energy for load.",
        "schema": {
            "fields": [
                "report_year",
                "electricity_market_module_region",
                "aeo_case",
                "projection_date",
                "aeo_net_energy_for_load_sector",
                "net_energy_for_load_mwh",
            ],
            "primary_key": [
                "report_year",
                "electricity_market_module_region",
                "aeo_case",
                "projection_date",
                "aeo_net_energy_for_load_sector",
            ],
        },
        "sources": ["eiaaeo"],
        "field_namespace": "eiaaeo",
        "etl_group": "eiaaeo",
    },
    "core_eiaaeo__end_use_prices_by_sector": {
        "description": "Projected electricity cost to the end user by sector.",
        "schema": {
            "fields": [
                "report_year",
                "electricity_market_module_region",
                "aeo_case",
                "projection_date",
                "aeo_sales_sector",
                "real_value_base_year",
                "end_use_price_per_mwh",
            ],
            "primary_key": [
                "report_year",
                "electricity_market_module_region",
                "aeo_case",
                "projection_date",
                "aeo_sales_sector",
                "real_value_base_year",
            ],
        },
        "sources": ["eiaaeo"],
        "field_namespace": "eiaaeo",
        "etl_group": "eiaaeo",
    },
    "core_eiaaeo__end_use_prices_by_service_category": {
        "description": (
            "Projected electricity cost to the end user by service category."
        ),
        "schema": {
            "fields": [
                "report_year",
                "electricity_market_module_region",
                "aeo_case",
                "projection_date",
                "aeo_service_category",
                "real_value_base_year",
                "end_use_price_per_mwh",
            ],
            "primary_key": [
                "report_year",
                "electricity_market_module_region",
                "aeo_case",
                "projection_date",
                "aeo_service_category",
                "real_value_base_year",
            ],
        },
        "sources": ["eiaaeo"],
        "field_namespace": "eiaaeo",
        "etl_group": "eiaaeo",
    },
    "core_eiaaeo__fuel_consumption_by_type": {
        "description": (
            "Projected fuel consumption by the electric power sector, "
            "including electricity-only and combined-heat-and-power plants "
            "that have a regulatory status."
        ),
        "schema": {
            "fields": [
                "report_year",
                "electricity_market_module_region",
                "aeo_case",
                "projection_date",
                "aeo_fuel_consumption_type",
                "fuel_consumed_mmbtu",
            ],
            "primary_key": [
                "report_year",
                "electricity_market_module_region",
                "aeo_case",
                "projection_date",
                "aeo_fuel_consumption_type",
            ],
        },
        "sources": ["eiaaeo"],
        "field_namespace": "eiaaeo",
        "etl_group": "eiaaeo",
    },
    "core_eiaaeo__electric_sector_fuel_cost_by_type": {
        "description": (
            "Projected fuel prices to the electric power sector, including "
            "electricity-only and combined-heat-and-power plants that have a "
            "regulatory status."
        ),
        "schema": {
            "fields": [
                "report_year",
                "electricity_market_module_region",
                "aeo_case",
                "projection_date",
                "aeo_fuel_cost_type",
                "real_value_base_year",
                "fuel_cost_mmbtu",  # this is *nominal* dollars, but we probably want to standardize to some sort of constant-dollar value?
            ],
            "primary_key": [
                "report_year",
                "electricity_market_module_region",
                "aeo_case",
                "projection_date",
                "aeo_fuel_cost_type",
                "real_value_base_year",
            ],
        },
        "sources": ["eiaaeo"],
        "field_namespace": "eiaaeo",
        "etl_group": "eiaaeo",
    },
    "core_eiaaeo__electric_power_sector_emissions": {
        "description": (
            "Projected emissions from the electric power sector, including "
            "electricity-only and combined-heat-and-power plants that have a "
            "regulatory status."
        ),
        "schema": {
            "fields": [
                "report_year",
                "electricity_market_module_region",
                "aeo_case",
                "projection_date",
                "carbon_mass_tons",
                "co2_mass_tons",
                "hg_mass_lb",
                "nox_mass_lb",
                "so2_mass_lb",
            ],
            "primary_key": [
                "report_year",
                "electricity_market_module_region",
                "aeo_case",
                "projection_date",
            ],
        },
        "sources": ["eiaaeo"],
        "field_namespace": "eiaaeo",
        "etl_group": "eiaaeo",
    },
}

# 2024-04-24: A holding pen so it's easy to see what the field definitions are.
# Once we "promote" schemas we need to move the fields to the real
# pudl.metadata.fields.FIELD_METADATA dictionary. What a frickin pain.
_STAGING_FIELD_METADATA: dict[str, dict[str, Any]] = {
    "report_year": {
        "type": "integer",
        "description": "ALREADY EXISTS, HERE FOR CONVENIENCE!",
    },
    "projection_date": {
        "type": "date",
        "description": "Date at which this data is projected to be true.",
    },
    "real_value_base_year": {
        "type": "integer",
        "description": "Nominal monetary values are converted to values from this year.",
    },
    "electricity_market_module_region": {
        "type": "string",
        "description": "AEO projection region. Maybe the same as NERC region?",
        "constraints": {
            "enum": [
                "florida_reliability_coordinating_council",
                "midcontinent_central",
                "midcontinent_east",
                "midcontinent_south",
                "midcontinent_west",
                "northeast_power_coordinating_council_new_england",
                "northeast_power_coordinating_council_new_york_city_and_long_island",
                "northeast_power_coordinating_council_upstate_new_york",
                "pjm_commonwealth_edison",
                "pjm_dominion",
                "pjm_east",
                "pjm_west",
                "serc_reliability_corporation_central",
                "serc_reliability_corporation_east",
                "serc_reliability_corporation_southeastern",
                "southwest_power_pool_central",
                "southwest_power_pool_north",
                "southwest_power_pool_south",
                "texas_reliability_entity",
                "united_states",
                "western_electricity_coordinating_council_basin",
                "western_electricity_coordinating_council_california_north",
                "western_electricity_coordinating_council_california_south",
                "western_electricity_coordinating_council_northwest_power_pool_area",
                "western_electricity_coordinating_council_rockies",
                "western_electricity_coordinating_council_southwest",
            ]
        },
    },
    "aeo_case": {
        "type": "string",
        "description": "AEO modelling case.",
        "constraints": {
            "enum": [
                "aeo2022",
                "high_economic_growth",
                "high_macro_and_high_zero_carbon_technology_cost",
                "high_macro_and_low_zero_carbon_technology_cost",
                "high_oil_and_gas_supply",
                "high_oil_price",
                "high_uptake_of_inflation_reduction_act",
                "high_zero_carbon_technology_cost",
                "low_economic_growth",
                "low_macro_and_high_zero_carbon_technology_cost",
                "low_macro_and_low_zero_carbon_technology_cost",
                "low_oil_and_gas_supply",
                "low_oil_price",
                "low_uptake_of_inflation_reduction_act",
                "low_zero_carbon_technology_cost",
                "no_inflation_reduction_act",
                "reference",
            ]
        },
    },
    "aeo_electricity_sector_fuel_type": {
        "type": "string",
        "description": "Fuel type reported for AEO electricity sector generation data.",
        "constraints": {
            "enum": [
                "coal",
                "combined_cycle",
                "combustion_turbine_diesel",
                "distributed_generation",
                "diurnal_storage",
                "fuel_cells",
                "nuclear",
                "oil_and_natural_gas_steam",
                "pumped_storage",
                "renewable_sources",
                "total",  # this one isn't *really* a fuel type - should we even keep it around?
                "sales_to_customers",  # this is only used for *total* generation, not *capacity*. And also isn't *really* a fuel type...
                "generation_for_own_use",  # this is only used for *total* generation, not *capacity*. And also isn't *really* a fuel type...
            ]
        },
    },
    "aeo_end_use_sector_fuel_type": {
        "type": "string",
        "description": ("Fuel type reported for AEO end-use sector generation data."),
        "constraints": {
            "enum": [
                "coal",
                "petroleum",
                "natural_gas",
                "other_gaseous_fuels",
                "renewable_sources",
                "other",
                "total",  # this one isn't *really* a fuel type - should we even keep it around?
                "sales_to_the_grid",  # this is only used for *total* generation, not *capacity*. And also isn't *really* a fuel type...
                "generation_for_own_use",  # this is only used for *total* generation, not *capacity*. And also isn't *really* a fuel type...
            ]
        },
    },
    "aeo_fuel_consumption_type": {
        "type": "string",
        "description": ("Fuel type reported for AEO fuel consumption data."),
        "constraints": {
            "enum": [
                "coal",
                "natural_gas",
                "oil",
                "total",  # this one isn't *really* a fuel type - should we even keep it around?
            ]
        },
    },
    "aeo_fuel_cost_type": {
        "type": "string",
        "description": ("Fuel type reported for AEO fuel consumption data."),
        "constraints": {
            "enum": [
                "coal",
                "natural_gas",
                "distillate_fuel_oil",
                "residual_fuel_oil",
            ]
        },
    },
    "aeo_net_energy_for_load_sector": {
        "type": "string",
        "description": ("Sector reported for net energy for load."),
        "constraints": {
            "enum": [
                "gross_international_imports",
                "gross_international_exports",
                "gross_interregional_electricity_imports",
                "gross_interregional_electricty_exports",
                "purchases_from_combined_heat_and_power",
                "electric_power_sector_generation_for_customer",
                "total",  # not a real sector / calculatable from everything else, should we keep this?
            ]
        },
    },
    "aeo_sales_sector": {
        "type": "string",
        "description": ("Sector reported for electricity sales & end-use prices."),
        "constraints": {
            "enum": [
                "residential",
                "commercial",
                "industrial",
                "transportation",
                "average",  # not a real sector, and only present for prices
                "total",  # not a real sector, only present for sales
            ]
        },
    },
    "aeo_service_category": {
        "type": "string",
        "description": ("Service category reported for electricity end-use prices."),
        "constraints": {
            "enum": [
                "generation",
                "transmission",
                "distribution",
            ]
        },
    },
    "carbon_mass_tons": {
        "type": "number",
        "description": "Total carbon emissions in short tons.",
        "unit": "short_tons",
    },
    "co2_mass_tons": {
        "type": "number",
        "description": "ALREADY EXISTS, HERE FOR CONVENIENCE!",
    },
    "hg_mass_lbs": {
        "type": "number",
        "description": "Total mercury emissions in pounds.",
        "unit": "lb",
    },
    "nox_mass_lbs": {
        "type": "number",
        "description": "ALREADY EXISTS, HERE FOR CONVENIENCE!",
    },
    "so2_mass_lbs": {
        "type": "number",
        "description": "ALREADY EXISTS, HERE FOR CONVENIENCE!",
    },
    "end_use_price_per_mwh": {
        "type": "number",
        "description": (
            "End-use sector electricity prices (standardized to the year "
            "``real_value_base_year``)."
        ),
        "unit": "USD/MWh",
    },
    "sales_mwh": {
        "type": "number",
        "description": "ALREADY EXISTS, JUST HERE FOR CONVENIENCE",
        "unit": "MWh",
    },
    "net_summer_capacity_mw": {
        "type": "number",
        "description": "The steady hourly output that generating equipment is expected to apply to system load.",
        "unit": "mw",
    },  # should this be capacity_mw_eia?
    "net_summer_capacity_cumulative_planned_additions_mw": {
        "type": "number",
        "description": (
            "The total planned additions to generating capacity since the "
            "beginning of the AEO period."
        ),
        "unit": "mw",
    },
    "net_summer_capacity_cumulative_unplanned_additions_mw": {
        "type": "number",
        "description": (
            "The total unplanned additions to generating capacity since the "
            "beginning of the AEO period."
        ),
        "unit": "mw",
    },
    "net_summer_capacity_cumulative_total_additions_mw": {  # this is just planned + unplanned - do we even care about publishing this?
        "type": "number",
        "description": (
            "The total additions to generating capacity since the beginning "
            "of the AEO period."
        ),
        "unit": "mw",
    },
    "net_summer_capacity_cumulative_retirements_mw": {
        "type": "number",
        "description": (
            "The total retirements from generating capacity since the beginning "
            "of the AEO period."
        ),
        "unit": "mw",
    },
}  # noqa:W0612

# 2024-04-24: to "promote" the schemas, we can add them to this set and move
# the field definitions
RESOURCE_METADATA = {
    key: value for key, value in _STAGING_RESOURCE_METADATA.items() if key in {}
}
