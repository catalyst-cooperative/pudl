"""Table definitions for EIA AEO resources."""

from typing import Any

# 2024-04-24: workaround so we can *define* a bunch of schemas without
# documenting them or expecting assets to exist
_STAGING_RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_eiaaeo__yearly_projected_generation_in_electric_sector_by_technology": {
        "description": (
            "Projected generation capacity & total generation in the electric "
            "sector, broken out by technology."
        ),
        "schema": {
            "fields": [
                "report_year",
                "electricity_market_module_region_eiaaeo",
                "model_case_eiaaeo",
                "projection_year",
                "technology_description_eiaaeo",
                "summer_capacity_mw",
                "summer_capacity_planned_additions_mw",
                "summer_capacity_unplanned_additions_mw",
                "summer_capacity_retirements_mw",
                "gross_generation_mwh",
            ],
            "primary_key": [
                "report_year",
                "electricity_market_module_region_eiaaeo",
                "model_case_eiaaeo",
                "projection_year",
                "technology_description_eiaaeo",
            ],
        },
        "sources": ["eiaaeo"],
        "field_namespace": "eiaaeo",
        "etl_group": "eiaaeo",
    },
    "core_eiaaeo__yearly_projected_generation_in_end_use_sectors_by_fuel_type": {
        "description": (
            "Projected generation capacity and total generation in the end-use "
            "sector, broken out by fuel type.\n"
            "Includes combined-heat-and-power plants and electricity-only "
            "plants in the commercial and industrial sectors; and small on-site "
            "generating systems in the residential, commercial, and industrial "
            "sectors used primarily for own-use generation, but which may also "
            "sell some power to the grid. Solar photovoltaic capacity portion of "
            "Renewable Sources in megawatts direct current; other technologies "
            "in megawatts alternating current."
        ),
        "schema": {
            "fields": [
                "report_year",
                "electricity_market_module_region_eiaaeo",
                "model_case_eiaaeo",
                "projection_year",
                "fuel_type_eiaaeo",
                "summer_capacity_mw",  # should this be capacity_mw_eia?
                "gross_generation_mwh",
            ],
            "primary_key": [
                "report_year",
                "electricity_market_module_region_eiaaeo",
                "model_case_eiaaeo",
                "projection_year",
                "fuel_type_eiaaeo",
            ],
        },
        "sources": ["eiaaeo"],
        "field_namespace": "eiaaeo",
        "etl_group": "eiaaeo",
    },
    "core_eiaaeo__yearly_projected_generation_in_electric_sector": {
        "description": (
            "Projected total generation in the electric sector, across all "
            "technologies."
        ),
        "schema": {
            "fields": [
                "report_year",
                "electricity_market_module_region_eiaaeo",
                "model_case_eiaaeo",
                "projection_year",
                "gross_generation_mwh",
                "sales_mwh",
                "generation_for_own_use_mwh",
            ],
            "primary_key": [
                "report_year",
                "electricity_market_module_region_eiaaeo",
                "model_case_eiaaeo",
                "projection_year",
            ],
        },
        "sources": ["eiaaeo"],
        "field_namespace": "eiaaeo",
        "etl_group": "eiaaeo",
    },
    "core_eiaaeo__yearly_projected_generation_in_end_use_sectors": {
        "description": (
            "Projected total generation in the end-use sector, across all "
            "fuel types.\n"
            "Includes combined-heat-and-power plants and electricity-only "
            "plants in the commercial and industrial sectors; and small on-site "
            "generating systems in the residential, commercial, and industrial "
            "sectors used primarily for own-use generation, but which may also "
            "sell some power to the grid. Solar photovoltaic capacity portion of "
            "Renewable Sources in megawatts direct current; other technologies "
            "in megawatts alternating current."
        ),
        "schema": {
            "fields": [
                "report_year",
                "electricity_market_module_region_eiaaeo",
                "model_case_eiaaeo",
                "projection_year",
                "gross_generation_mwh",
                "sales_mwh",
                "generation_for_own_use_mwh",
            ],
            "primary_key": [
                "report_year",
                "electricity_market_module_region_eiaaeo",
                "model_case_eiaaeo",
                "projection_year",
            ],
        },
        "sources": ["eiaaeo"],
        "field_namespace": "eiaaeo",
        "etl_group": "eiaaeo",
    },
    "core_eiaaeo__yearly_projected_electric_sales": {
        "description": "Projected electricity sales by region and customer class.",
        "schema": {
            "fields": [
                "report_year",
                "electricity_market_module_region_eiaaeo",
                "model_case_eiaaeo",
                "projection_year",
                "customer_class",
                "sales_mwh",
            ],
            "primary_key": [
                "report_year",
                "electricity_market_module_region_eiaaeo",
                "model_case_eiaaeo",
                "projection_year",
                "customer_class",
            ],
        },
        "sources": ["eiaaeo"],
        "field_namespace": "eiaaeo",
        "etl_group": "eiaaeo",
    },
    "core_eiaaeo__yearly_projected_net_energy_for_load": {
        "description": "Projected net energy for load.",
        "schema": {
            "fields": [
                "report_year",
                "electricity_market_module_region_eiaaeo",
                "model_case_eiaaeo",
                "projection_year",
                "generation_source_or_sink_type_eiaaeo",
                "net_load_mwh",
            ],
            "primary_key": [
                "report_year",
                "electricity_market_module_region_eiaaeo",
                "model_case_eiaaeo",
                "projection_year",
                "generation_source_or_sink_type_eiaaeo",
            ],
        },
        "sources": ["eiaaeo"],
        "field_namespace": "eiaaeo",
        "etl_group": "eiaaeo",
    },
    "core_eiaaeo__yearly_projected_end_use_prices_by_sector": {
        "description": "Projected electricity cost to the end user by sector.",
        "schema": {
            "fields": [
                "report_year",
                "electricity_market_module_region_eiaaeo",
                "model_case_eiaaeo",
                "projection_year",
                "customer_class",
                "nominal_price_per_mwh",
                "real_price_per_mwh",
            ],
            "primary_key": [
                "report_year",
                "electricity_market_module_region_eiaaeo",
                "model_case_eiaaeo",
                "projection_year",
                "customer_class",
            ],
        },
        "sources": ["eiaaeo"],
        "field_namespace": "eiaaeo",
        "etl_group": "eiaaeo",
    },
    "core_eiaaeo__yearly_projected_end_use_prices_by_service_category": {
        "description": (
            "Projected electricity cost to the end user by service category."
        ),
        "schema": {
            "fields": [
                "report_year",
                "electricity_market_module_region_eiaaeo",
                "model_case_eiaaeo",
                "projection_year",
                "service_category_eiaaeo",
                "nominal_price_per_mwh",
                "real_price_per_mwh",
            ],
            "primary_key": [
                "report_year",
                "electricity_market_module_region_eiaaeo",
                "model_case_eiaaeo",
                "projection_year",
                "service_category_eiaaeo",
            ],
        },
        "sources": ["eiaaeo"],
        "field_namespace": "eiaaeo",
        "etl_group": "eiaaeo",
    },
    "core_eiaaeo__yearly_projected_fuel_consumption_by_type": {
        "description": (
            "Projected fuel consumption by the electric power sector, "
            "including electricity-only and combined-heat-and-power plants "
            "that have a regulatory status."
        ),
        "schema": {
            "fields": [
                "report_year",
                "electricity_market_module_region_eiaaeo",
                "model_case_eiaaeo",
                "projection_year",
                "fuel_type_eiaaeo",
                "fuel_consumed_mmbtu",
            ],
            "primary_key": [
                "report_year",
                "electricity_market_module_region_eiaaeo",
                "model_case_eiaaeo",
                "projection_year",
                "fuel_type_eiaaeo",
            ],
        },
        "sources": ["eiaaeo"],
        "field_namespace": "eiaaeo",
        "etl_group": "eiaaeo",
    },
    "core_eiaaeo__yearly_projected_fuel_cost_in_electric_sector_by_type": {
        "description": (
            "Projected fuel prices to the electric power sector, including "
            "electricity-only and combined-heat-and-power plants that have a "
            "regulatory status."
        ),
        "schema": {
            "fields": [
                "report_year",
                "electricity_market_module_region_eiaaeo",
                "model_case_eiaaeo",
                "projection_year",
                "fuel_type_eiaaeo",
                "fuel_cost_per_mmbtu",
                "fuel_cost_real_per_mmbtu_eiaaeo",
            ],
            "primary_key": [
                "report_year",
                "electricity_market_module_region_eiaaeo",
                "model_case_eiaaeo",
                "projection_year",
                "fuel_type_eiaaeo",
            ],
        },
        "sources": ["eiaaeo"],
        "field_namespace": "eiaaeo",
        "etl_group": "eiaaeo",
    },
    "core_eiaaeo__yearly_projected_emissions_in_electric_sector": {
        "description": (
            "Projected emissions from the electric power sector, including "
            "electricity-only and combined-heat-and-power plants that have a "
            "regulatory status."
        ),
        "schema": {
            "fields": [
                "report_year",
                "electricity_market_module_region_eiaaeo",
                "model_case_eiaaeo",
                "projection_year",
                "carbon_mass_tons",
                "co2_mass_tons",
                "hg_mass_lb",
                "nox_mass_lb",
                "so2_mass_lb",
            ],
            "primary_key": [
                "report_year",
                "electricity_market_module_region_eiaaeo",
                "model_case_eiaaeo",
                "projection_year",
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
    "aeo_fuel_consumption_type": {  # TODO: turn this into a resource-specific enum for fuel_type_eiaaeo when promoting.
        "type": "string",
        "description": ("Fuel type reported for AEO fuel consumption data."),
        "constraints": {
            "enum": [
                "coal",
                "natural_gas",
                "oil",
            ]
        },
    },
    "aeo_fuel_cost_type": {  # TODO: turn this into a resource-specific enum for fuel_type_eiaaeo when promoting.
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
    "carbon_mass_tons": {
        "type": "number",
        "description": "Total carbon emissions in short tons.",
        "unit": "short_tons",
    },
    "fuel_cost_real_per_mmbtu_eiaaeo": {
        "type": "number",
        "description": (
            "Average fuel cost per mmBTU of heat content in real USD, "
            "standardized to the value of a USD in the year before the report "
            "year."
        ),
        "unit": "USD_per_MMBtu",
    },
    "generation_for_own_use_mwh": {
        "type": "number",
        "description": "Amount of generation that is used for generation instead of sold.",
        "unit": "MWh",
    },
    "generation_source_or_sink_type_eiaaeo": {
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
            ]
        },
    },
    "hg_mass_lbs": {
        "type": "number",
        "description": "Total mercury emissions in pounds.",
        "unit": "lb",
    },
    "price_per_mwh": {
        "type": "number",
        "description": ("Nominal electricity price per MWh."),
        "unit": "USD/MWh",
    },
    "price_real_per_mwh": {
        "type": "number",
        "description": (
            "Real electricity price per MWh, standardized to "
            "the year before the ``report_year``."
        ),
        "unit": "USD/MWh",
    },
    "service_category_eiaaeo": {
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
}  # noqa:W0612

# 2024-04-24: to "promote" the schemas, we can add them to this set and move
# the field definitions
RESOURCE_METADATA = {
    key: value
    for key, value in _STAGING_RESOURCE_METADATA.items()
    if key
    in {
        "core_eiaaeo__yearly_projected_generation_in_electric_sector_by_technology",
        "core_eiaaeo__yearly_projected_generation_in_end_use_sectors_by_fuel_type",
        "core_eiaaeo__yearly_projected_electric_sales",
    }
}
