"""Enumerations of valid field values."""

from pudl.metadata.dfs import POLITICAL_SUBDIVISIONS

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
]
"""Energy sources used to categorize generation in the EIA 930 data.

These strings are used to construct a multi-index for stacking the net generation data
and must not contain underscores, as that character is used to split the longer column
names into different parts.
"""
for energy_source in GENERATION_ENERGY_SOURCES_EIA930:
    assert "_" not in energy_source

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
    "all_storage",  # need 'all' as prefix so as not to confuse with other storage category
    "total",
    "virtual_pv",
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
"""Modeling cases for EIA AEO.

See https://eia.gov/outlooks/aeo/assumptions/case_descriptions.php .
"""
