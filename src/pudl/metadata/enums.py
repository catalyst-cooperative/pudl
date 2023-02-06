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

For states that are split, we chose the timezone with a larger population.
List of timezones in pytz.common_timezones
Canada: https://en.wikipedia.org/wiki/Time_in_Canada#IANA_time_zone_database
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

CUSTOMER_CLASSES: list[str] = [
    "commercial",
    "industrial",
    "direct_connection",
    "other",
    "residential",
    "total",
    "transportation",
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
}

"""The plant parts in the EIA plant parts list."""

EIA860_EMISSION_CONTROL_EQUIPMENT_TYPE_CODES: list[str] = [
    "JB",  # Jet bubbling reactor (wet) scrubber
    "MA",  # Mechanically aided type (wet) scrubber
    "PA",  # Packed type (wet) scrubber
    "SP",  # Spray type (wet) scrubber
    "TR",  # Tray type (wet) scrubber
    "VE",  # Venturi type (wet) scrubber
    "BS",  # Baghouse (fabric filter), shake and deflate
    "BP",  # Baghouse (fabric filter), pulse
    "BR",  # Baghouse (fabric filter), reverse air
    "EC",  # Electrostatic precipitator, cold side, with flue gas conditioning
    "EH",  # Electrostatic precipitator, hot side, with flue gas conditioning
    "EK",  # Electrostatic precipitator, cold side, without flue gas conditioning
    "EW",  # Electrostatic precipitator, hot side, without flue gas conditioning
    "MC",  # Multiple cyclone
    "SC",  # Single cyclone
    "CD",  # Circulating dry scrubber
    "SD",  # Spray dryer type / dry FGD / semi-dry FGD
    "DSI",  # Dry sorbent (powder) injection type (DSI)
    "ACI",  # Activated carbon injection system
    "SN",  # Selective noncatalytic reduction
    "SR",  # Selective catalytic reduction
    "OT ",  # Other equipment (Specify in SCHEDULE 7)
]
"""Valid equipment type codes for emission control equipment reported in EIA860."""
