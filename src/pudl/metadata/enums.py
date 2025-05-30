"""Enumerations of valid field values."""

from pudl.metadata.dfs import IMPUTATION_REASON_CODES, POLITICAL_SUBDIVISIONS

IMPUTATION_CODES: set[str] = set(IMPUTATION_REASON_CODES.code)
COUNTRY_CODES_ISO3166: set[str] = set(POLITICAL_SUBDIVISIONS.country_code)
SUBDIVISION_CODES_ISO3166: set[str] = set(POLITICAL_SUBDIVISIONS.subdivision_code)
EPACEMS_STATES: set[str] = set(
    POLITICAL_SUBDIVISIONS.loc[
        POLITICAL_SUBDIVISIONS.is_epacems_state, "subdivision_code"
    ]
)
DIVISION_CODES_US_CENSUS: set[str] = set(
    POLITICAL_SUBDIVISIONS.division_code_us_census.dropna()
)

APPROXIMATE_TIMEZONES: dict[str, str] = {
    x.subdivision_code: x.timezone_approx for x in POLITICAL_SUBDIVISIONS.itertuples()
}
"""Mapping of political subdivision code to the most common timezone in that area.

This is imperfect for states that have split timezones. See:
https://en.wikipedia.org/wiki/List_of_time_offsets_by_U.S._state_and_territory

For states that are split, we chose the timezone with a larger population. List of
timezones in pytz.common_timezones Canada:
https://en.wikipedia.org/wiki/Time_in_Canada#IANA_time_zone_database
"""

NERC_REGIONS: list[str] = [
    "BASN",  # ASSESSMENT AREA Basin (WECC)
    "CALN",  # ASSESSMENT AREA California (WECC)
    "CALS",  # ASSESSMENT AREA California (WECC)
    "DSW",  # ASSESSMENT AREA Desert Southwest (WECC)
    "ASCC",  # Alaska
    "ISONE",  # ISO New England (NPCC)
    "ERCOT",  # lumped under TRE in 2017 Form instructions
    "NORW",  # ASSESSMENT AREA Northwest (WECC)
    "NYISO",  # ISO (NPCC)
    "PJM",  # RTO
    "ROCK",  # ASSESSMENT AREA Rockies (WECC)
    "ECAR",  # OLD RE Now part of RFC and SERC
    "FRCC",  # included in 2017 Form instructions, recently joined with SERC
    "HICC",  # Hawaii
    "MAAC",  # OLD RE Now part of RFC
    "MAIN",  # OLD RE Now part of SERC, RFC, MRO
    "MAPP",  # OLD/NEW RE Became part of MRO, resurfaced in 2010
    "MRO",  # RE included in 2017 Form instructions
    "NPCC",  # RE included in 2017 Form instructions
    "RFC",  # RE included in 2017 Form instructions
    "SERC",  # RE included in 2017 Form instructions
    "SPP",  # RE included in 2017 Form instructions
    "TRE",  # RE included in 2017 Form instructions (included ERCOT)
    "WECC",  # RE included in 2017 Form instructions
    "WSCC",  # OLD RE pre-2002 version of WECC
    "MISO",  # ISO unclear whether technically a regional entity, but lots of entries
    "ECAR_MAAC",
    "MAPP_WECC",
    "RFC_SERC",
    "SPP_WECC",
    "MRO_WECC",
    "ERCOT_SPP",
    "SPP_TRE",
    "ERCOT_TRE",
    "MISO_TRE",
    "FRCC_SERC",
    "VI",  # Virgin Islands
    "GU",  # Guam
    "PR",  # Puerto Rico
    "AS",  # American Samoa
    "UNK",
]
"""North American Reliability Corporation (NERC) regions.

See https://www.eia.gov/electricity/data/eia411/#tabs_NERC-3.
"""

US_TIMEZONES: list[str] = [
    "America/Anchorage",
    "America/Chicago",
    "America/Denver",
    "America/Los_Angeles",
    "America/New_York",
    "America/Phoenix",
    "Pacific/Honolulu",
]

GENERATION_ENERGY_SOURCES_EIA930 = [
    "coal",
    "gas",
    "hydro",
    "nuclear",
    "oil",
    "other",
    "solar",
    "unknown",
    "wind",
    "battery_storage",
    "geothermal",
    "hydro_excluding_pumped_storage",
    "other_energy_storage",
    "pumped_storage",
    "solar_w_integrated_battery_storage",
    "solar_wo_integrated_battery_storage",
    "unknown_energy_storage",
    "wind_w_integrated_battery_storage",
    "wind_wo_integrated_battery_storage",
]
"""Energy sources used to categorize generation in the EIA 930 data."""

ELECTRICITY_MARKET_MODULE_REGIONS: list[str] = [
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
"""Regions that the EIA uses in their Electricity Market Module analysis.

According to EIA:

The Electricity Market Module (EMM) in the National Energy Modeling System
(NEMS) is made up of four primary submodules: electricity load and demand,
electricity capacity planning, electricity fuel dispatching, and electricity
finance and pricing, as well as the ReStore submodule which interfaces with
both the renewable and electricity modules The EMM also includes nonutility
capacity and generation as well as electricity transmission and trade.

We use 25 electricity supply regions to represent U.S. power markets. The
regions follow North American Electric Reliability Corporation (NERC)
assessment region boundaries and independent system operator (ISO) and regional
transmission organization (RTO) region boundaries (as of early 2019).
Subregions are based on regional pricing zones.

https://www.eia.gov/outlooks/aeo/assumptions/pdf/EMM_Assumptions.pdf
"""


CUSTOMER_CLASSES: list[str] = [
    "commercial",
    "industrial",
    "direct_connection",
    "other",
    "residential",
    "total",
    "transportation",
    "commercial_other",  # commercial *OR* other - used in EIA AEO only.
]

TECH_CLASSES: list[str] = [
    "backup",  # WHERE Is this used? because removed from DG table b/c not a real component
    "chp_cogen",
    "combustion_turbine",
    "fuel_cell",
    "hydro",
    "internal_combustion",
    "other",
    "pv",
    "steam",
    "storage_pv",
    "storage_nonpv",
    "all_storage",  # need 'all' as prefix so as not to confuse with other storage category
    "total",
    "virtual_pv",
    "virtual_pv_under_1mw",  # Broken out in EIA 861 in 2023
    "virtual_pv_over_1mw",  # Broken out in EIA 861 in 2023
    "wind",
]

REVENUE_CLASSES: list[str] = [
    "credits_or_adjustments",
    "delivery_customers",
    "other",
    "retail_sales",
    "sales_for_resale",
    "total",
    "transmission",
    "unbundled",
]

RELIABILITY_STANDARDS: list[str] = ["ieee_standard", "other_standard"]

FUEL_CLASSES: list[str] = [
    "gas",
    "oil",
    "other",
    "renewable",
    "water",
    "wind",
    "wood",
]

RTO_CLASSES: list[str] = [
    "caiso",
    "ercot",
    "isone",
    "miso",
    "nyiso",
    "other",
    "pjm",
    "spp",
]

EPACEMS_MEASUREMENT_CODES: list[str] = [
    "Calculated",
    "LME",
    "Measured",
    "Measured and Substitute",
    "Other",  # ¿Should be replaced with NA?
    "Substitute",
    "Undetermined",  # Should be replaced with NA
    "Unknown Code",  # Should be replaced with NA
]
"""Valid emissions measurement codes for the EPA CEMS hourly data."""

TECH_DESCRIPTIONS: set[str] = {
    "Conventional Hydroelectric",
    "Conventional Steam Coal",
    "Natural Gas Steam Turbine",
    "Natural Gas Fired Combustion Turbine",
    "Natural Gas Internal Combustion Engine",
    "Nuclear",
    "Natural Gas Fired Combined Cycle",
    "Petroleum Liquids",
    "Hydroelectric Pumped Storage",
    "Solar Photovoltaic",
    "Batteries",
    "Geothermal",
    "Municipal Solid Waste",
    "Wood/Wood Waste Biomass",
    "Onshore Wind Turbine",
    "Coal Integrated Gasification Combined Cycle",
    "Other Gases",
    "Landfill Gas",
    "All Other",
    "Other Waste Biomass",
    "Petroleum Coke",
    "Solar Thermal without Energy Storage",
    "Solar Thermal with Energy Storage",
    "Other Natural Gas",
    "Flywheels",
    "Offshore Wind Turbine",
    "Natural Gas with Compressed Air Storage",
    "Hydrokinetic",
}
"""Valid technology descriptions from the EIA plant parts list."""

PLANT_PARTS: set[str] = {
    "plant",
    "plant_unit",
    "plant_prime_mover",
    "plant_technology",
    "plant_prime_fuel",
    "plant_ferc_acct",
    "plant_operating_year",
    "plant_gen",
    "plant_match_ferc1",
}
"""The plant parts in the EIA plant parts list."""

TECH_DESCRIPTIONS_NRELATB: set[str] = {
    "AEO",
    "Biopower",
    "CSP",
    "Coal_FE",
    "Coal_Retrofits",
    "CommPV",
    "Commercial Battery Storage",
    "DistributedWind",
    "Geothermal",
    "Hydropower",
    "LandbasedWind",
    "NaturalGas_FE",
    "NaturalGas_Retrofits",
    "Nuclear",
    "OffShoreWind",
    "Pumped Storage Hydropower",
    "ResPV",
    "Residential Battery Storage",
    "Utility-Scale Battery Storage",
    "Utility-Scale PV-Plus-Battery",
    "UtilityPV",
}
"""NREL ATB technology descriptions."""


TECH_DESCRIPTIONS_EIAAEO: list[str] = [
    "coal",
    "combined_cycle",
    "combustion_turbine_diesel",
    "distributed_generation",
    "diurnal_storage",
    "fuel_cells",
    "natural_gas",
    "nuclear",
    "oil_and_natural_gas_steam",
    "petroleum",
    "pumped_storage",
    "pumped_storage_other",
    "renewable_sources",
]
"""Types of generation technology reported in EIA AEO."""

FUEL_TYPES_EIAAEO: list[str] = [
    "coal",
    "distillate_fuel_oil",
    "residual_fuel_oil",
    "petroleum",
    "natural_gas",
    "other_gaseous_fuels",
    "renewable_sources",
    "other",
]
"""Type of fuel used for generation reported in EIA AEO."""

MODEL_CASES_EIAAEO: list[str] = [
    "aeo2022",
    "fast_builds_plus_high_lng_price",
    "high_economic_growth",
    "high_lng_price",
    "high_macro_and_high_zero_carbon_technology_cost",
    "high_macro_and_low_zero_carbon_technology_cost",
    "high_oil_and_gas_supply",
    "high_oil_price",
    "high_uptake_of_inflation_reduction_act",
    "high_zero_carbon_technology_cost",
    "low_economic_growth",
    "low_lng_price",
    "low_macro_and_high_zero_carbon_technology_cost",
    "low_macro_and_low_zero_carbon_technology_cost",
    "low_oil_and_gas_supply",
    "low_oil_price",
    "low_uptake_of_inflation_reduction_act",
    "low_zero_carbon_technology_cost",
    "no_inflation_reduction_act",
    "reference",
]
"""Modeling cases for EIA AEO 2023.

See https://www.eia.gov/outlooks/archive/aeo23/assumptions/case_descriptions.php .

EIA's browser (https://www.eia.gov/outlooks/aeo/data/browser/#/?id=2-AEO2023) and
data API also include the AEO2022 Reference case, which is not listed on the case
descriptions page.
"""

ENERGY_USE_TYPES_EIAAEO: list[str] = [
    "biofuels_heat_and_coproducts",
    "coal",
    "coal_subtotal",
    "coal_to_liquids_heat_and_power",
    "delivered_energy",
    "distillate_fuel_oil",
    "e85",
    "electricity",
    "electricity_imports",
    "electricity_related_losses",
    "hydrocarbon_gas_liquids",
    "hydrogen",
    "jet_fuel",
    "kerosene",
    "lease_and_plant_fuel",
    "liquefaction",
    "liquid_fuels_subtotal",
    "liquefied_petroleum_gases",
    "metallurgical_coal",
    "motor_gasoline",
    "natural_gas",
    "natural_gas_subtotal",
    "natural_gas_to_liquids_heat_and_power",
    "net_coal_coke_imports",
    "non_biogenic_municipal_waste",
    "nuclear",
    "other_industrial_coal",
    "other_petroleum",
    "other_coal",
    "petrochemical_feedstocks",
    "pipeline_fuel_natural_gas",
    "pipeline_natural_gas",
    "propane",
    "renewable_energy",
    "residual_fuel_oil",
    "steam_coal",
    "total",
]
"""Energy use types from Table 2 of EIA AEO 2023.

These are from the series titles, not the display titles in the EIA's data browser tool,
which may show different text."""


ENERGY_SOURCE_TYPES_FERC1: list[str] = [
    "steam_generation",
    "net_energy_generation",
    "sources_of_energy",
    "nuclear_generation",
    "hydro_conventional_generation",
    "megawatt_hours_purchased",
    "energy_received_through_power_exchanges",
    "energy_delivered_through_power_exchanges",
    "net_energy_through_power_exchanges",
    "electric_power_wheeling_energy_received",
    "electric_power_wheeling_energy_delivered",
    "net_transmission_energy_for_others_electric_power_wheeling",
    "other_energy_generation",
    "hydro_pumped_storage_generation",
    "pumping_energy",
    "transmission_losses_by_others_electric_power_wheeling",
    "megawatt_hours_purchased_for_energy_storage",
    "megawatt_hours_purchased_other_than_storage",
]
"""Energy source types for FERC Form 1 data."""

ENERGY_DISPOSITION_TYPES_FERC1: list[str] = [
    "megawatt_hours_sold_non_requirements_sales",
    "disposition_of_energy",
    "megawatt_hours_sold_sales_to_ultimate_consumers",
    "megawatt_hours_sold_requirements_sales",
    "non_charged_energy",
    "internal_use_energy",
    "energy_losses",
    "energy_stored",
]
"""Energy disposition types for FERC Form 1 data."""

UTILITY_PLANT_ASSET_TYPES_FERC1: list[str] = [
    "abandonment_of_leases",
    "accumulated_provision_for_depreciation_amortization_and_depletion_of_plant_utility",
    "accumulated_provision_for_depreciation_amortization_and_depletion_of_plant_utility_reported",
    "amortization_and_depletion_of_producing_natural_gas_land_and_land_rights_utility_plant_in_service",
    "amortization_and_depletion_utility_plant_leased_to_others",
    "amortization_of_other_utility_plant_utility_plant_in_service",
    "amortization_of_plant_acquisition_adjustment",
    "amortization_of_underground_storage_land_and_land_rights_utility_plant_in_service",
    "amortization_utility_plant_held_for_future_use",
    "construction_work_in_progress",
    "depreciation_amortization_and_depletion_utility_plant_in_service",
    "depreciation_amortization_and_depletion_utility_plant_leased_to_others",
    "depreciation_and_amortization_utility_plant_held_for_future_use",
    "depreciation_utility_plant_held_for_future_use",
    "depreciation_utility_plant_in_service",
    "depreciation_utility_plant_leased_to_others",
    "utility_plant_acquisition_adjustment",
    "utility_plant_and_construction_work_in_progress",
    "utility_plant_held_for_future_use",
    "utility_plant_in_service_classified",
    "utility_plant_in_service_classified_and_property_under_capital_leases",
    "utility_plant_in_service_classified_and_unclassified",
    "utility_plant_in_service_completed_construction_not_classified",
    "utility_plant_in_service_experimental_plant_unclassified",
    "utility_plant_in_service_plant_purchased_or_sold",
    "utility_plant_in_service_property_under_capital_leases",
    "utility_plant_leased_to_others",
    "utility_plant_net",
]
"""Utility plant asset types for FERC Form 1 data."""

LIABILITY_TYPES_FERC1: list[str] = [
    "accounts_payable",
    "accounts_payable_to_associated_companies",
    "accumulated_deferred_income_taxes",
    "accumulated_deferred_income_taxes_accelerated_amortization_property",
    "accumulated_deferred_income_taxes_other",
    "accumulated_deferred_income_taxes_other_property",
    "accumulated_deferred_investment_tax_credits",
    "accumulated_miscellaneous_operating_provisions",
    "accumulated_other_comprehensive_income",
    "accumulated_provision_for_injuries_and_damages",
    "accumulated_provision_for_pensions_and_benefits",
    "accumulated_provision_for_property_insurance",
    "accumulated_provision_for_rate_refunds",
    "advances_from_associated_companies",
    "asset_retirement_obligations",
    "bonds",
    "capital_stock_expense",
    "capital_stock_subscribed",
    "common_stock_issued",
    "current_and_accrued_liabilities",
    "customer_advances_for_construction",
    "customer_deposits",
    "deferred_credits",
    "deferred_gains_from_disposition_of_utility_plant",
    "derivative_instrument_liabilities_hedges",
    "derivatives_instrument_liabilities",
    "discount_on_capital_stock",
    "dividends_declared",
    "installments_received_on_capital_stock",
    "interest_accrued",
    "less_long_term_portion_of_derivative_instrument_liabilities",
    "less_long_term_portion_of_derivative_instrument_liabilities_hedges",
    "liabilities_and_other_credits",
    "long_term_debt",
    "long_term_portion_of_derivative_instrument_liabilities",
    "long_term_portion_of_derivative_instrument_liabilities_hedges",
    "matured_interest",
    "matured_long_term_debt",
    "miscellaneous_current_and_accrued_liabilities",
    "noncorporate_proprietorship",
    "notes_payable",
    "notes_payable_to_associated_companies",
    "obligations_under_capital_lease_noncurrent",
    "obligations_under_capital_leases_current",
    "other_deferred_credits",
    "other_long_term_debt",
    "other_noncurrent_liabilities",
    "other_paid_in_capital",
    "other_regulatory_liabilities",
    "preferred_stock_issued",
    "premium_on_capital_stock",
    "proprietary_capital",
    "reacquired_bonds",
    "reacquired_capital_stock",
    "retained_earnings",
    "stock_liability_for_conversion",
    "tax_collections_payable",
    "taxes_accrued",
    "unamortized_discount_on_long_term_debt_debit",
    "unamortized_gain_on_reacquired_debt",
    "unamortized_premium_on_long_term_debt",
    "unappropriated_undistributed_subsidiary_earnings",
]
"""Liability types for FERC Form 1 data."""

ASSET_TYPES_FERC1: list[str] = [
    "accounts_receivable_from_associated_companies",
    "accrued_utility_revenues",
    "accumulated_deferred_income_taxes",
    "accumulated_provision_for_amortization_of_nuclear_fuel_assemblies",
    "accumulated_provision_for_depreciation_and_amortization_of_nonutility_property",
    "accumulated_provision_for_uncollectible_accounts_credit",
    "advances_for_gas",
    "allowance_inventory_and_withheld",
    "amortization_fund_federal",
    "cash",
    "cash_and_working_funds",
    "clearing_accounts",
    "current_and_accrued_assets",
    "customer_accounts_receivable",
    "deferred_debits",
    "deferred_losses_from_disposition_of_utility_plant",
    "depreciation_fund",
    "derivative_instrument_assets",
    "derivative_instrument_assets_hedges",
    "derivative_instrument_assets_hedges_long_term",
    "derivative_instrument_assets_long_term",
    "extraordinary_property_losses",
    "fuel_stock",
    "fuel_stock_expenses_undistributed",
    "gas_stored_current",
    "gas_stored_underground_noncurrent",
    "interest_and_dividends_receivable",
    "investment_in_associated_companies",
    "investment_in_subsidiary_companies",
    "less_derivative_instrument_assets_hedges_long_term",
    "less_derivative_instrument_assets_long_term",
    "less_noncurrent_portion_of_allowances",
    "liquefied_natural_gas_stored_and_held_for_processing",
    "merchandise",
    "miscellaneous_current_and_accrued_assets",
    "miscellaneous_deferred_debits",
    "noncurrent_portion_of_allowances",
    "nonutility_property",
    "notes_receivable",
    "notes_receivable_from_associated_companies",
    "nuclear_fuel",
    "nuclear_fuel_assemblies_in_reactor_major_only",
    "nuclear_fuel_in_process_of_refinement_conversion_enrichment_and_fabrication",
    "nuclear_fuel_materials_and_assemblies_stock_account_major_only",
    "nuclear_fuel_net",
    "nuclear_fuel_under_capital_leases",
    "nuclear_materials_held_for_sale",
    "other_accounts_receivable",
    "other_electric_plant_adjustments",
    "other_investments",
    "other_materials_and_supplies",
    "other_preliminary_survey_and_investigation_charges",
    "other_property_and_investments",
    "other_regulatory_assets",
    "other_special_funds",
    "plant_materials_and_operating_supplies",
    "preliminary_natural_gas_and_other_survey_and_investigation_charges",
    "preliminary_natural_gas_survey_and_investigation_charges",
    "preliminary_survey_and_investigation_charges",
    "prepayments",
    "rents_receivable",
    "research_development_and_demonstration_expenditures",
    "residuals",
    "sinking_funds",
    "special_deposits",
    "special_funds",
    "special_funds_all",
    "spent_nuclear_fuel_major_only",
    "stores_expense_undistributed",
    "temporary_cash_investments",
    "temporary_facilities",
    "unamortized_debt_expense",
    "unamortized_loss_on_reacquired_debt",
    "unrecovered_plant_and_regulatory_study_costs",
    "unrecovered_purchased_gas_costs",
    "utility_plant_and_nuclear_fuel_net",
    "utility_plant_net",
    "working_funds",
]
"""Asset types for FERC Form 1 data."""

INCOME_TYPES_FERC1: list[str] = [
    "accretion_expense",
    "allowance_for_borrowed_funds_used_during_construction_credit",
    "allowance_for_other_funds_used_during_construction",
    "amortization_and_depletion_of_utility_plant",
    "amortization_of_conversion_expenses",
    "amortization_of_debt_discount_and_expense",
    "amortization_of_electric_plant_acquisition_adjustments",
    "amortization_of_gain_on_reacquired_debt_credit",
    "amortization_of_loss_on_reacquired_debt",
    "amortization_of_premium_on_debt_credit",
    "amortization_of_property_losses_unrecovered_plant_and_regulatory_study_costs",
    "costs_and_expenses_of_merchandising_jobbing_and_contract_work",
    "depreciation_expense",
    "depreciation_expense_for_asset_retirement_costs",
    "donations",
    "equity_in_earnings_of_subsidiary_companies",
    "expenditures_for_certain_civic_political_and_related_activities",
    "expenses_of_nonutility_operations",
    "extraordinary_deductions",
    "extraordinary_income",
    "extraordinary_items_after_taxes",
    "gain_on_disposition_of_property",
    "gains_from_disposition_of_allowances",
    "gains_from_disposition_of_plant",
    "income_before_extraordinary_items",
    "income_taxes_extraordinary_items",
    "income_taxes_federal",
    "income_taxes_operating_income",
    "income_taxes_other",
    "income_taxes_utility_operating_income_other",
    "interest_and_dividend_income",
    "interest_on_debt_to_associated_companies",
    "interest_on_long_term_debt",
    "investment_tax_credit_adjustments",
    "investment_tax_credit_adjustments_nonutility_operations",
    "investment_tax_credits",
    "life_insurance",
    "loss_on_disposition_of_property",
    "losses_from_disposition_of_allowances",
    "losses_from_disposition_of_service_company_plant",
    "maintenance_expense",
    "miscellaneous_amortization",
    "miscellaneous_deductions",
    "miscellaneous_nonoperating_income",
    "net_extraordinary_items",
    "net_income_loss",
    "net_interest_charges",
    "net_other_income_and_deductions",
    "net_utility_operating_income",
    "nonoperating_rental_income",
    "operating_revenues",
    "operation_expense",
    "other_deductions",
    "other_income",
    "other_income_deductions",
    "other_interest_expense",
    "penalties",
    "provision_for_deferred_income_taxes_credit_operating_income",
    "provision_for_deferred_income_taxes_credit_other_income_and_deductions",
    "provision_for_deferred_income_taxes_other_income_and_deductions",
    "provisions_for_deferred_income_taxes_utility_operating_income",
    "regulatory_credits",
    "regulatory_debits",
    "revenues_from_merchandising_jobbing_and_contract_work",
    "revenues_from_nonutility_operations",
    "taxes_on_other_income_and_deductions",
    "taxes_other_than_income_taxes_other_income_and_deductions",
    "taxes_other_than_income_taxes_utility_operating_income",
    "utility_operating_expenses",
]
"""Income types for FERC Form 1 data."""
