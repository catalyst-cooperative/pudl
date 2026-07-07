# pudl.metadata.enums

Enumerations of valid field values.

## Attributes

| [`IMPUTATION_CODES`](#pudl.metadata.enums.IMPUTATION_CODES)                                             |                                                                                 |
|---------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------|
| [`COUNTRY_CODES_ISO3166`](#pudl.metadata.enums.COUNTRY_CODES_ISO3166)                                   |                                                                                 |
| [`SUBDIVISION_CODES_ISO3166`](#pudl.metadata.enums.SUBDIVISION_CODES_ISO3166)                           |                                                                                 |
| [`EPACEMS_STATES`](#pudl.metadata.enums.EPACEMS_STATES)                                                 |                                                                                 |
| [`DIVISION_CODES_US_CENSUS`](#pudl.metadata.enums.DIVISION_CODES_US_CENSUS)                             |                                                                                 |
| [`APPROXIMATE_TIMEZONES`](#pudl.metadata.enums.APPROXIMATE_TIMEZONES)                                   | Mapping of political subdivision code to the most common timezone in that area. |
| [`EIA191_STORAGE_REGIONS`](#pudl.metadata.enums.EIA191_STORAGE_REGIONS)                                 | EIA storage regions for underground natural gas storage fields (Form 191).      |
| [`NERC_REGIONS`](#pudl.metadata.enums.NERC_REGIONS)                                                     | North American Reliability Corporation (NERC) regions.                          |
| [`US_TIMEZONES`](#pudl.metadata.enums.US_TIMEZONES)                                                     |                                                                                 |
| [`GENERATION_ENERGY_SOURCES_EIA930`](#pudl.metadata.enums.GENERATION_ENERGY_SOURCES_EIA930)             | Energy sources used to categorize generation in the EIA 930 data.               |
| [`ELECTRICITY_MARKET_MODULE_REGIONS`](#pudl.metadata.enums.ELECTRICITY_MARKET_MODULE_REGIONS)           | Regions that the EIA uses in their Electricity Market Module analysis.          |
| [`CUSTOMER_CLASSES`](#pudl.metadata.enums.CUSTOMER_CLASSES)                                             |                                                                                 |
| [`CUSTOMER_CLASSES_EIA176`](#pudl.metadata.enums.CUSTOMER_CLASSES_EIA176)                               |                                                                                 |
| [`SUPPLY_TYPES_EIA176`](#pudl.metadata.enums.SUPPLY_TYPES_EIA176)                                       |                                                                                 |
| [`TECH_CLASSES`](#pudl.metadata.enums.TECH_CLASSES)                                                     |                                                                                 |
| [`REVENUE_CLASSES_EIA861`](#pudl.metadata.enums.REVENUE_CLASSES_EIA861)                                 |                                                                                 |
| [`REVENUE_CLASSES_EIA176`](#pudl.metadata.enums.REVENUE_CLASSES_EIA176)                                 |                                                                                 |
| [`SUPPLEMENTAL_GASEOUS_FUEL_TYPES_EIA176`](#pudl.metadata.enums.SUPPLEMENTAL_GASEOUS_FUEL_TYPES_EIA176) |                                                                                 |
| [`OTHER_DISPOSITION_TYPES_EIA176`](#pudl.metadata.enums.OTHER_DISPOSITION_TYPES_EIA176)                 |                                                                                 |
| [`RELIABILITY_STANDARDS`](#pudl.metadata.enums.RELIABILITY_STANDARDS)                                   |                                                                                 |
| [`FUEL_CLASSES`](#pudl.metadata.enums.FUEL_CLASSES)                                                     |                                                                                 |
| [`RTO_CLASSES`](#pudl.metadata.enums.RTO_CLASSES)                                                       |                                                                                 |
| [`EPACEMS_MEASUREMENT_CODES`](#pudl.metadata.enums.EPACEMS_MEASUREMENT_CODES)                           | Valid emissions measurement codes for the EPA CEMS hourly data.                 |
| [`TECH_DESCRIPTIONS`](#pudl.metadata.enums.TECH_DESCRIPTIONS)                                           | Valid technology descriptions from the EIA plant parts list.                    |
| [`PLANT_PARTS`](#pudl.metadata.enums.PLANT_PARTS)                                                       | The plant parts in the EIA plant parts list.                                    |
| [`TECH_DESCRIPTIONS_NRELATB`](#pudl.metadata.enums.TECH_DESCRIPTIONS_NRELATB)                           | NREL ATB technology descriptions.                                               |
| [`TECH_DESCRIPTIONS_EIAAEO`](#pudl.metadata.enums.TECH_DESCRIPTIONS_EIAAEO)                             | Types of generation technology reported in EIA AEO.                             |
| [`FUEL_TYPES_EIAAEO`](#pudl.metadata.enums.FUEL_TYPES_EIAAEO)                                           | Type of fuel used for generation reported in EIA AEO.                           |
| [`MODEL_CASES_EIAAEO`](#pudl.metadata.enums.MODEL_CASES_EIAAEO)                                         | Modeling cases for EIA AEO 2023.                                                |
| [`ENERGY_USE_TYPES_EIAAEO`](#pudl.metadata.enums.ENERGY_USE_TYPES_EIAAEO)                               | Energy use types from Table 2 of EIA AEO 2023-2025.                             |
| [`ENERGY_SOURCE_TYPES_FERC1`](#pudl.metadata.enums.ENERGY_SOURCE_TYPES_FERC1)                           | Energy source types for FERC Form 1 data.                                       |
| [`ENERGY_DISPOSITION_TYPES_FERC1`](#pudl.metadata.enums.ENERGY_DISPOSITION_TYPES_FERC1)                 | Energy disposition types for FERC Form 1 data.                                  |
| [`UTILITY_PLANT_ASSET_TYPES_FERC1`](#pudl.metadata.enums.UTILITY_PLANT_ASSET_TYPES_FERC1)               | Utility plant asset types for FERC Form 1 data.                                 |
| [`LIABILITY_TYPES_FERC1`](#pudl.metadata.enums.LIABILITY_TYPES_FERC1)                                   | Liability types for FERC Form 1 data.                                           |
| [`ASSET_TYPES_FERC1`](#pudl.metadata.enums.ASSET_TYPES_FERC1)                                           | Asset types for FERC Form 1 data.                                               |
| [`INCOME_TYPES_FERC1`](#pudl.metadata.enums.INCOME_TYPES_FERC1)                                         | Income types for FERC Form 1 data.                                              |
| [`FUNCTIONAL_STATUS_CODES_CENSUS`](#pudl.metadata.enums.FUNCTIONAL_STATUS_CODES_CENSUS)                 | Functional status codes for Census geographic entities.                         |
| [`MATERIAL_TYPES_PHMSAGAS`](#pudl.metadata.enums.MATERIAL_TYPES_PHMSAGAS)                               |                                                                                 |
| [`MAIN_PIPE_SIZES_PHMSAGAS`](#pudl.metadata.enums.MAIN_PIPE_SIZES_PHMSAGAS)                             |                                                                                 |
| [`LEAK_SOURCE_PHMSAGAS`](#pudl.metadata.enums.LEAK_SOURCE_PHMSAGAS)                                     |                                                                                 |
| [`DAMAGE_TYPES_PHMSAGAS`](#pudl.metadata.enums.DAMAGE_TYPES_PHMSAGAS)                                   |                                                                                 |
| [`DAMAGE_SUB_TYPES_PHMSAGAS`](#pudl.metadata.enums.DAMAGE_SUB_TYPES_PHMSAGAS)                           |                                                                                 |
| [`ASSET_TYPES_RUS7`](#pudl.metadata.enums.ASSET_TYPES_RUS7)                                             |                                                                                 |
| [`LIABILITY_TYPES_RUS7`](#pudl.metadata.enums.LIABILITY_TYPES_RUS7)                                     |                                                                                 |
| [`ASSET_TYPES_RUS12`](#pudl.metadata.enums.ASSET_TYPES_RUS12)                                           |                                                                                 |
| [`LIABILITY_TYPES_RUS12`](#pudl.metadata.enums.LIABILITY_TYPES_RUS12)                                   |                                                                                 |
| [`PRIME_MOVER_TYPES_RUS12`](#pudl.metadata.enums.PRIME_MOVER_TYPES_RUS12)                               |                                                                                 |
| [`RENEWABLE_FUEL_TYPES_RUS12`](#pudl.metadata.enums.RENEWABLE_FUEL_TYPES_RUS12)                         |                                                                                 |
| [`PLANT_TYPE_RUS12`](#pudl.metadata.enums.PLANT_TYPE_RUS12)                                             |                                                                                 |
| [`SOURCE_OF_ENERGY_RUS12`](#pudl.metadata.enums.SOURCE_OF_ENERGY_RUS12)                                 |                                                                                 |
| [`PLANT_COST_TYPES_RUS12`](#pudl.metadata.enums.PLANT_COST_TYPES_RUS12)                                 |                                                                                 |
| [`LOAN_STATUS_TYPES_RUS7`](#pudl.metadata.enums.LOAN_STATUS_TYPES_RUS7)                                 |                                                                                 |
| [`LOAN_UNIT_TYPES_RUS7`](#pudl.metadata.enums.LOAN_UNIT_TYPES_RUS7)                                     |                                                                                 |
| [`SERVICE_INTERRUPTION_TYPES_RUS7`](#pudl.metadata.enums.SERVICE_INTERRUPTION_TYPES_RUS7)               |                                                                                 |
| [`SERVICE_INTERRUPTION_PERIODS_RUS7`](#pudl.metadata.enums.SERVICE_INTERRUPTION_PERIODS_RUS7)           |                                                                                 |
| [`SERVICE_STATUS_RUS7`](#pudl.metadata.enums.SERVICE_STATUS_RUS7)                                       |                                                                                 |
| [`TRANSMISSION_DISTRIBUTION_TYPES_RUS7`](#pudl.metadata.enums.TRANSMISSION_DISTRIBUTION_TYPES_RUS7)     |                                                                                 |
| [`UTILITY_PLANT_GROUP_RUS7`](#pudl.metadata.enums.UTILITY_PLANT_GROUP_RUS7)                             |                                                                                 |
| [`UTILITY_PLANT_ITEM_RUS7`](#pudl.metadata.enums.UTILITY_PLANT_ITEM_RUS7)                               |                                                                                 |
| [`UTILITY_PLANT_GROUP_RUS12`](#pudl.metadata.enums.UTILITY_PLANT_GROUP_RUS12)                           |                                                                                 |
| [`UTILITY_PLANT_ITEM_RUS12`](#pudl.metadata.enums.UTILITY_PLANT_ITEM_RUS12)                             |                                                                                 |
| [`DEPRECIATION_CHANGES_GROUP_RUS12`](#pudl.metadata.enums.DEPRECIATION_CHANGES_GROUP_RUS12)             |                                                                                 |
| [`DEPRECIATION_CHANGES_ITEMS_RUS12`](#pudl.metadata.enums.DEPRECIATION_CHANGES_ITEMS_RUS12)             |                                                                                 |
| [`DEPRECIATION_ITEMS_MISC_RUS12`](#pudl.metadata.enums.DEPRECIATION_ITEMS_MISC_RUS12)                   |                                                                                 |

## Module Contents

### pudl.metadata.enums.IMPUTATION_CODES *: [set](https://docs.python.org/3/library/stdtypes.html#set)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

### pudl.metadata.enums.COUNTRY_CODES_ISO3166 *: [set](https://docs.python.org/3/library/stdtypes.html#set)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

### pudl.metadata.enums.SUBDIVISION_CODES_ISO3166 *: [set](https://docs.python.org/3/library/stdtypes.html#set)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

### pudl.metadata.enums.EPACEMS_STATES *: [set](https://docs.python.org/3/library/stdtypes.html#set)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

### pudl.metadata.enums.DIVISION_CODES_US_CENSUS *: [set](https://docs.python.org/3/library/stdtypes.html#set)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

### pudl.metadata.enums.APPROXIMATE_TIMEZONES *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]*

Mapping of political subdivision code to the most common timezone in that area.

This is imperfect for states that have split timezones. See:
[https://en.wikipedia.org/wiki/List_of_time_offsets_by_U.S._state_and_territory](https://en.wikipedia.org/wiki/List_of_time_offsets_by_U.S._state_and_territory)

For states that are split, we chose the timezone with a larger population. List of
timezones in pytz.common_timezones Canada:
[https://en.wikipedia.org/wiki/Time_in_Canada#IANA_time_zone_database](https://en.wikipedia.org/wiki/Time_in_Canada#IANA_time_zone_database)

### pudl.metadata.enums.EIA191_STORAGE_REGIONS *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= ['alaska', 'east region', 'midwest region', 'mountain region', 'pacific region', 'south central region']*

EIA storage regions for underground natural gas storage fields (Form 191).

### pudl.metadata.enums.NERC_REGIONS *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= ['BASN', 'CALN', 'CALS', 'DSW', 'ASCC', 'ISONE', 'ERCOT', 'NORW', 'NYISO', 'PJM', 'ROCK',...*

North American Reliability Corporation (NERC) regions.

See [https://www.eia.gov/electricity/data/eia411/#tabs_NERC-3](https://www.eia.gov/electricity/data/eia411/#tabs_NERC-3).

### pudl.metadata.enums.US_TIMEZONES *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= ['America/Anchorage', 'America/Chicago', 'America/Denver', 'America/Los_Angeles',...*

### pudl.metadata.enums.GENERATION_ENERGY_SOURCES_EIA930 *= ['coal', 'gas', 'hydro', 'nuclear', 'oil', 'other', 'solar', 'unknown', 'wind',...*

Energy sources used to categorize generation in the EIA 930 data.

### pudl.metadata.enums.ELECTRICITY_MARKET_MODULE_REGIONS *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= ['florida_reliability_coordinating_council', 'midcontinent_central', 'midcontinent_east',...*

Regions that the EIA uses in their Electricity Market Module analysis.

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

[https://www.eia.gov/outlooks/aeo/assumptions/pdf/EMM_Assumptions.pdf](https://www.eia.gov/outlooks/aeo/assumptions/pdf/EMM_Assumptions.pdf)

### pudl.metadata.enums.CUSTOMER_CLASSES *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= ['commercial', 'industrial', 'direct_connection', 'other', 'residential', 'total',...*

### pudl.metadata.enums.CUSTOMER_CLASSES_EIA176 *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= ['commercial', 'electric_power', 'industrial', 'other', 'residential', 'vehicle_fuel']*

### pudl.metadata.enums.SUPPLY_TYPES_EIA176 *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= ['natural_gas_production', 'synthetic_gas_production', 'underground_storage_withdrawals',...*

### pudl.metadata.enums.TECH_CLASSES *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= ['backup', 'chp_cogen', 'combustion_turbine', 'fuel_cell', 'hydro', 'internal_combustion',...*

### pudl.metadata.enums.REVENUE_CLASSES_EIA861 *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= ['credits_or_adjustments', 'delivery_customers', 'other', 'retail_sales', 'sales_for_resale',...*

### pudl.metadata.enums.REVENUE_CLASSES_EIA176 *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= ['sales', 'transport']*

### pudl.metadata.enums.SUPPLEMENTAL_GASEOUS_FUEL_TYPES_EIA176 *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= ['air_injection', 'biomass', 'biomass_gas', 'blast_furnace_gas', 'coke_oven_gas', 'gas_holders',...*

### pudl.metadata.enums.OTHER_DISPOSITION_TYPES_EIA176 *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= ['line_pressure', 'other', 'vented_flared', 'plant_fuel', 'plant_thermal_reduction',...*

### pudl.metadata.enums.RELIABILITY_STANDARDS *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= ['ieee_standard', 'other_standard']*

### pudl.metadata.enums.FUEL_CLASSES *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= ['gas', 'oil', 'other', 'renewable', 'water', 'wind', 'wood']*

### pudl.metadata.enums.RTO_CLASSES *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= ['caiso', 'ercot', 'isone', 'miso', 'nyiso', 'other', 'pjm', 'spp']*

### pudl.metadata.enums.EPACEMS_MEASUREMENT_CODES *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= ['Calculated', 'LME', 'Measured', 'Measured and Substitute', 'Other', 'Substitute']*

Valid emissions measurement codes for the EPA CEMS hourly data.

### pudl.metadata.enums.TECH_DESCRIPTIONS *: [set](https://docs.python.org/3/library/stdtypes.html#set)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

Valid technology descriptions from the EIA plant parts list.

### pudl.metadata.enums.PLANT_PARTS *: [set](https://docs.python.org/3/library/stdtypes.html#set)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

The plant parts in the EIA plant parts list.

### pudl.metadata.enums.TECH_DESCRIPTIONS_NRELATB *: [set](https://docs.python.org/3/library/stdtypes.html#set)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

NREL ATB technology descriptions.

### pudl.metadata.enums.TECH_DESCRIPTIONS_EIAAEO *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= ['coal', 'combined_cycle', 'combustion_turbine_diesel', 'distributed_generation',...*

Types of generation technology reported in EIA AEO.

### pudl.metadata.enums.FUEL_TYPES_EIAAEO *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= ['coal', 'distillate_fuel_oil', 'residual_fuel_oil', 'petroleum', 'natural_gas',...*

Type of fuel used for generation reported in EIA AEO.

### pudl.metadata.enums.MODEL_CASES_EIAAEO *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= ['aeo2022', 'aeo2023', 'alternative_electricity', 'alternative_transportation',...*

Modeling cases for EIA AEO 2023.

See [https://www.eia.gov/outlooks/archive/aeo23/assumptions/case_descriptions.php](https://www.eia.gov/outlooks/archive/aeo23/assumptions/case_descriptions.php) .

EIA’s browser ([https://www.eia.gov/outlooks/aeo/data/browser/#/](https://www.eia.gov/outlooks/aeo/data/browser/#/)?id=2-AEO2023) and
data API also include the AEO2022 Reference case, which is not listed on the case
descriptions page.

### pudl.metadata.enums.ENERGY_USE_TYPES_EIAAEO *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= ['biofuels_heat_and_coproducts', 'byproduct_hydrogen', 'coal', 'coal_subtotal',...*

Energy use types from Table 2 of EIA AEO 2023-2025.

These are from the series titles, not the display titles in the EIA’s data browser tool,
which may show different text.

### pudl.metadata.enums.ENERGY_SOURCE_TYPES_FERC1 *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= ['steam_generation', 'net_energy_generation', 'sources_of_energy', 'nuclear_generation',...*

Energy source types for FERC Form 1 data.

### pudl.metadata.enums.ENERGY_DISPOSITION_TYPES_FERC1 *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= ['megawatt_hours_sold_non_requirements_sales', 'disposition_of_energy',...*

Energy disposition types for FERC Form 1 data.

### pudl.metadata.enums.UTILITY_PLANT_ASSET_TYPES_FERC1 *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= ['abandonment_of_leases',...*

Utility plant asset types for FERC Form 1 data.

### pudl.metadata.enums.LIABILITY_TYPES_FERC1 *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= ['accounts_payable', 'accounts_payable_to_associated_companies',...*

Liability types for FERC Form 1 data.

### pudl.metadata.enums.ASSET_TYPES_FERC1 *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= ['accounts_receivable_from_associated_companies', 'accrued_utility_revenues',...*

Asset types for FERC Form 1 data.

### pudl.metadata.enums.INCOME_TYPES_FERC1 *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= ['accretion_expense', 'allowance_for_borrowed_funds_used_during_construction_credit',...*

Income types for FERC Form 1 data.

### pudl.metadata.enums.FUNCTIONAL_STATUS_CODES_CENSUS *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= ['A', 'B', 'C', 'E', 'F', 'G', 'I', 'L', 'M', 'N', 'S', 'T']*

Functional status codes for Census geographic entities.

[https://www.census.gov/library/reference/code-lists/functional-status-codes.html](https://www.census.gov/library/reference/code-lists/functional-status-codes.html)

### pudl.metadata.enums.MATERIAL_TYPES_PHMSAGAS *= ['unprotected_steel_bare', 'unprotected_steel_coated', 'cathodically_protected_steel_bare',...*

### pudl.metadata.enums.MAIN_PIPE_SIZES_PHMSAGAS *= ['0.5_in_or_less', '0.5_to_1_in', '1_in_or_less', '1_to_2_in', '2_in_or_less', '2_to_4_in',...*

### pudl.metadata.enums.LEAK_SOURCE_PHMSAGAS *= ['construction_defect', 'corrosion_failure', 'equipment_failure', 'excavation_damage',...*

### pudl.metadata.enums.DAMAGE_TYPES_PHMSAGAS *= ['notification', 'locating', 'excavation', 'other', 'total']*

### pudl.metadata.enums.DAMAGE_SUB_TYPES_PHMSAGAS *= ['deteriorated_facility', 'dug_after_expiry', 'failed_clearance',...*

### pudl.metadata.enums.ASSET_TYPES_RUS7 *: [set](https://docs.python.org/3/library/stdtypes.html#set)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

### pudl.metadata.enums.LIABILITY_TYPES_RUS7 *: [set](https://docs.python.org/3/library/stdtypes.html#set)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

### pudl.metadata.enums.ASSET_TYPES_RUS12 *: [set](https://docs.python.org/3/library/stdtypes.html#set)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

### pudl.metadata.enums.LIABILITY_TYPES_RUS12 *: [set](https://docs.python.org/3/library/stdtypes.html#set)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

### pudl.metadata.enums.PRIME_MOVER_TYPES_RUS12 *: [set](https://docs.python.org/3/library/stdtypes.html#set)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

### pudl.metadata.enums.RENEWABLE_FUEL_TYPES_RUS12 *: [set](https://docs.python.org/3/library/stdtypes.html#set)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

### pudl.metadata.enums.PLANT_TYPE_RUS12 *: [set](https://docs.python.org/3/library/stdtypes.html#set)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

### pudl.metadata.enums.SOURCE_OF_ENERGY_RUS12 *: [set](https://docs.python.org/3/library/stdtypes.html#set)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

### pudl.metadata.enums.PLANT_COST_TYPES_RUS12 *: [set](https://docs.python.org/3/library/stdtypes.html#set)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

### pudl.metadata.enums.LOAN_STATUS_TYPES_RUS7 *= ['loan_default', 'loan_delinquency']*

### pudl.metadata.enums.LOAN_UNIT_TYPES_RUS7 *= ['actual_pct', 'anticipated_pct', 'ytd_dollars']*

### pudl.metadata.enums.SERVICE_INTERRUPTION_TYPES_RUS7

### pudl.metadata.enums.SERVICE_INTERRUPTION_PERIODS_RUS7 *= ['five_year_average', 'annual']*

### pudl.metadata.enums.SERVICE_STATUS_RUS7 *= ['connected_this_year', 'retired_this_year', 'total_in_place', 'idle_in_place']*

### pudl.metadata.enums.TRANSMISSION_DISTRIBUTION_TYPES_RUS7 *= ['distribution_overhead', 'distribution_underground', 'transmission_line', 'total']*

### pudl.metadata.enums.UTILITY_PLANT_GROUP_RUS7 *= ['utility_plant_in_service', 'total_utility_plant']*

### pudl.metadata.enums.UTILITY_PLANT_ITEM_RUS7 *= ['construction_work_in_progress', 'total', 'all_other', 'distribution', 'general',...*

### pudl.metadata.enums.UTILITY_PLANT_GROUP_RUS12 *= ['intangible_plant', 'production_plant', 'transmission_plant', 'distribution_plant',...*

### pudl.metadata.enums.UTILITY_PLANT_ITEM_RUS12 *= ['land_and_land_rights', 'other', 'station_equipment', 'structures_and_improvements', 'total',...*

### pudl.metadata.enums.DEPRECIATION_CHANGES_GROUP_RUS12 *= ['electric_plant_in_service', 'provision_for_depreciation_and_amortization']*

### pudl.metadata.enums.DEPRECIATION_CHANGES_ITEMS_RUS12 *= ['depreciation_distribution_plant', 'depreciation_general_plant',...*

### pudl.metadata.enums.DEPRECIATION_ITEMS_MISC_RUS12 *= ['annual_accrual_charged_to_expense', 'annual_accrual_charged_to_other_accounts',...*
