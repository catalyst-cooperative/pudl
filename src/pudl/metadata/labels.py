"""Descriptive labels for coded field values."""
from typing import Dict

ENTITY_TYPES: Dict[str, str] = {
    'M': 'Municipal',
    'C': 'Cooperative',
    'R': 'Retail Power Marketer',
    'I': 'Investor Owned',
    'P': 'Political Subdivision',
    'T': 'Transmission',
    'S': 'State',
    'W': 'Wholesale Power Marketer',
    'F': 'Federal',
    'A': 'Municipal Marketing Authority',
    'G': 'Community Choice Aggregator',
    'D': 'Nonutility DSM Administrator',
    'B': 'Behind the Meter',
    'Q': 'Independent Power Producer',
    'IND': 'Industrial',
    'COM': 'Commercial',
    'PR': 'Private',  # Added by AES for OD table (Arbitrary moniker)
    'PO': 'Power Marketer',  # Added by AES for OD table
    'U': 'Unknown',  # Added by AES for OD table
    'O': 'Other',  # Added by AES for OD table
}
"""
Descriptive labels for EIA entity type codes.
"""

ESTIMATED_OR_ACTUAL: Dict[str, str] = {'E': 'Estimated', 'A': 'Actual'}
"""
Descriptive labels for EIA estimated or actual codes.
"""

MOMENTARY_INTERRUPTIONS: Dict[str, str] = {
    'L': 'Less than 1 minute',
    'F': 'Less than or equal to 5 minutes',
    'O': 'Other',
}
"""Descriptive labels for EIA momentary interruption codes."""

POWER_PURCHASE_TYPES_FERC1: Dict[str, str] = {
    'RQ': 'requirement',
    'LF': 'long_firm',
    'IF': 'intermediate_firm',
    'SF': 'short_firm',
    'LU': 'long_unit',
    'IU': 'intermediate_unit',
    'EX': 'electricity_exchange',
    'OS': 'other_service',
    'AD': 'adjustment'
}
"""Descriptive labels for FERC 1 power purchase type codes."""

CENSUS_REGIONS: Dict[str, str] = {
    'NEW': 'New England',
    'MAT': 'Middle Atlantic',
    'SAT': 'South Atlantic',
    'ESC': 'East South Central',
    'WSC': 'West South Central',
    'ENC': 'East North Central',
    'WNC': 'West North Central',
    'MTN': 'Mountain',
    'PACC': 'Pacific Contiguous (OR, WA, CA)',
    'PACN': 'Pacific Non-Contiguous (AK, HI)',
}
"""Descriptive labels for Census Region codes."""

RTO_ISO: Dict[str, str] = {
    'CAISO': 'California ISO',
    'ERCOT': 'Electric Reliability Council of Texas',
    'ISONE': 'ISO New England',
    'MISO': 'Midcontinent ISO',
    'NYISO': 'New York ISO',
    'PJM': 'PJM Interconnection',
    'SPP': 'Southwest Power Pool'
}
"""Descriptive labels for RTO/ISO short codes."""

NAICS_SECTOR_CONSOLIDATED_EIA: Dict[str, str] = {
    # traditional regulated electric utilities
    '1': 'Electric Utility',
    # Independent power producers which are not cogenerators
    '2': 'NAICS-22 Non-Cogen',
    # Independent power producers which are cogenerators, but whose
    # primary business purpose is the sale of electricity to the public
    '3': 'NAICS-22 Cogen',
    # Commercial non-cogeneration facilities that produce electric power,
    # are connected to the gird, and can sell power to the public
    '4': 'Commercial NAICS Non-Cogen',
    # Commercial cogeneration facilities that produce electric power, are
    # connected to the grid, and can sell power to the public
    '5': 'Commercial NAICS Cogen',
    # Industrial non-cogeneration facilities that produce electric power, are
    # connected to the gird, and can sell power to the public
    '6': 'Industrial NAICS Non-Cogen',
    # Industrial cogeneration facilities that produce electric power, are
    # connected to the gird, and can sell power to the public
    '7': 'Industrial NAICS Cogen'
}
"""
Descriptive labels for EIA consolidated NAICS sector codes.

For internal purposes, EIA consolidates NAICS categories into seven groups.
These codes and descriptions are listed on Page 7 of EIA Form 923 EIA’s internal
consolidated NAICS sectors.

"""

PRIME_MOVERS_EIA: Dict[str, str] = {
    'BA': 'Energy Storage, Battery',
    'BT': 'Turbines Used in a Binary Cycle. Including those used for geothermal applications',
    'CA': 'Combined-Cycle -- Steam Part',
    'CC': 'Combined-Cycle, Total Unit',
    'CE': 'Energy Storage, Compressed Air',
    'CP': 'Energy Storage, Concentrated Solar Power',
    'CS': 'Combined-Cycle Single-Shaft Combustion Turbine and Steam Turbine share of single',
    'CT': 'Combined-Cycle Combustion Turbine Part',
    'ES': 'Energy Storage, Other (Specify on Schedule 9, Comments)',
    'FC': 'Fuel Cell',
    'FW': 'Energy Storage, Flywheel',
    'GT': 'Combustion (Gas) Turbine. Including Jet Engine design',
    'HA': 'Hydrokinetic, Axial Flow Turbine',
    'HB': 'Hydrokinetic, Wave Buoy',
    'HK': 'Hydrokinetic, Other',
    'HY': 'Hydraulic Turbine. Including turbines associated with delivery of water by pipeline.',
    'IC': 'Internal Combustion (diesel, piston, reciprocating) Engine',
    'PS': 'Energy Storage, Reversible Hydraulic Turbine (Pumped Storage)',
    'OT': 'Other',
    'ST': 'Steam Turbine. Including Nuclear, Geothermal, and Solar Steam (does not include Combined Cycle).',
    'PV': 'Photovoltaic',
    'WT': 'Wind Turbine, Onshore',
    'WS': 'Wind Turbine, Offshore',
    'UNK': 'Unkown Prime Mover'
}
"""Descriptive labels for EIA prime mover codes."""

FUEL_UNITS_EIA: Dict[str, str] = {
    'mcf': 'Thousands of cubic feet (for gases)',
    'short_tons': 'Short tons (for solids)',
    'barrels': 'Barrels (for liquids)'
}
"""
Descriptive labels for the units of measure EIA uses for fuels.

The physical units fuel consumption is reported in.  All consumption is reported
in either short tons for solids, thousands of cubic feet for gases, and barrels
for liquids.
"""

ENERGY_SOURCES_EIA: Dict[str, str] = {
    'AB': 'Agricultural By-Products',
    'ANT': 'Anthracite Coal',
    'BFG': 'Blast Furnace Gas',
    'BIT': 'Bituminous Coal',
    'BLQ': 'Black Liquor',
    'BM': 'Biomass',
    'CBL': 'Coal, Blended',
    'DFO': 'Distillate Fuel Oil. Including diesel, No. 1, No. 2, and No. 4 fuel oils.',
    'GEO': 'Geothermal',
    'JF': 'Jet Fuel',
    'KER': 'Kerosene',
    'LFG': 'Landfill Gas',
    'LIG': 'Lignite Coal',
    'MSB': 'Biogenic Municipal Solid Waste',
    'MSN': 'Non-biogenic Municipal Solid Waste',
    'MSW': 'Municipal Solid Waste',
    'MWH': 'Electricity used for energy storage',
    'NG': 'Natural Gas',
    'NUC': 'Nuclear. Including Uranium, Plutonium, and Thorium.',
    'OBG': 'Other Biomass Gas. Including digester gas, methane, and other biomass gases.',
    'OBL': 'Other Biomass Liquids',
    'OBS': 'Other Biomass Solids',
    'OG': 'Other Gas',
    'OTH': 'Other Fuel',
    'PC': 'Petroleum Coke',
    'PG': 'Gaseous Propane',
    'PUR': 'Purchased Steam',
    'RC': 'Refined Coal',
    'RFO': 'Residual Fuel Oil. Including No. 5 & 6 fuel oils and bunker C fuel oil.',
    'SC': 'Coal-based Synfuel. Including briquettes, pellets, or extrusions, which are formed by binding materials or processes that recycle materials.',
    'SGC': 'Coal-Derived Synthesis Gas',
    'SGP': 'Synthesis Gas from Petroleum Coke',
    'SLW': 'Sludge Waste',
    'SUB': 'Subbituminous Coal',
    'SUN': 'Solar',
    'TDF': 'Tire-derived Fuels',
    'WAT': 'Water at a Conventional Hydroelectric Turbine and water used in Wave Buoy Hydrokinetic Technology, current Hydrokinetic Technology, Tidal Hydrokinetic Technology, and Pumping Energy for Reversible (Pumped Storage) Hydroelectric Turbines.',
    'WC': 'Waste/Other Coal. Including anthracite culm, bituminous gob, fine coal, lignite waste, waste coal.',
    'WDL': 'Wood Waste Liquids, excluding Black Liquor. Including red liquor, sludge wood, spent sulfite liquor, and other wood-based liquids.',
    'WDS': 'Wood/Wood Waste Solids. Including paper pellets, railroad ties, utility polies, wood chips, bark, and other wood waste solids.',
    'WH': 'Waste Heat not directly attributed to a fuel source',
    'WND': 'Wind',
    'WO': 'Waste/Other Oil. Including crude oil, liquid butane, liquid propane, naphtha, oil waste, re-refined moto oil, sludge oil, tar oil, or other petroleum-based liquid wastes.'
}
"""Descriptive labels for energy sources and fuel types reported in EIA 923."""

FUEL_TYPES_AER_EIA: Dict[str, str] = {
    'SUN': 'Solar PV and thermal',
    'COL': 'Coal',
    'DFO': 'Distillate Petroleum',
    'GEO': 'Geothermal',
    'HPS': 'Hydroelectric Pumped Storage',
    'HYC': 'Hydroelectric Conventional',
    'MLG': 'Biogenic Municipal Solid Waste and Landfill Gas',
    'NG': 'Natural Gas',
    'NUC': 'Nuclear',
    'OOG': 'Other Gases',
    'ORW': 'Other Renewables',
    'OTH': 'Other (including nonbiogenic MSW)',
    'PC': 'Petroleum Coke',
    'RFO': 'Residual Petroleum',
    'WND': 'Wind',
    'WOC': 'Waste Coal',
    'WOO': 'Waste Oil',
    'WWW': 'Wood and Wood Waste'
}
"""
Descriptive labels for aggregated fuel types used in the Annual Energy Review.

See the EIA 923 Fuel Code table (Table 5) for additional information.
"""

CONTRACT_TYPES_EIA923: Dict[str, str] = {
    'C': 'Contract - Fuel received under a purchase order or contract with a term of one year or longer.  Contracts with a shorter term are considered spot purchases ',
    'NC': 'New Contract - Fuel received under a purchase order or contract with duration of one year or longer, under which deliveries were first made during the reporting month',
    'N': 'New Contract - see NC code. This abbreviation existed only in 2008 before being replaced by NC.',
    'S': 'Spot Purchase',
    'T': 'Tolling Agreement – Fuel received under a tolling agreement (bartering arrangement of fuel for generation)'
}
"""
Descriptive labels for fuel supply contract type codes reported in EIA 923.

The purchase type under which fuel receipts occurred in the reporting month.
"""

COALMINE_TYPES_EIA: Dict[str, str] = {
    'P': 'Preparation Plant',
    'S': 'Surface',
    'U': 'Underground',
    'US': 'Both an underground and surface mine with most coal extracted from underground',
    'SU': 'Both an underground and surface mine with most coal extracted from surface',
}
"""
Descriptive labels for coal mine type codes used in EIA 923 reporting.

These codes and descriptions come from Page 7 of the EIA 923.
"""

FUEL_TRANSPORTATION_MODES_EIA: Dict[str, str] = {
    # Shipments of fuel moved to consumers by rail (private or
    # public/commercial). Included is coal hauled to or away from a railroad
    # siding by truck if the truck did not use public roads.
    'RR': 'railroad',

    # Shipments of fuel moved to consumers via river by barge.  Not included are
    # shipments to Great Lakes coal loading docks, tidewater piers, or coastal
    # ports.
    'RV': 'river',

    # Shipments of coal moved to consumers via the Great Lakes. These shipments
    # are moved via the Great Lakes coal loading docks, which are identified by
    # name and location as follows: Conneaut Coal Storage & Transfer, Conneaut,
    # Ohio; NS Coal Dock (Ashtabula Coal Dock), Ashtabula, Ohio; Sandusky Coal
    # Pier, Sandusky, Ohio;  Toledo Docks, Toledo, Ohio; KCBX Terminals Inc.,
    # Chicago, Illinois; Superior Midwest Energy Terminal, Superior, Wisconsin',
    'GL': 'great_lakes',

    # Shipments of fuel moved to consumers by truck.  Not included is fuel
    # hauled to or away from a railroad siding by truck on non-public roads.
    'TK': 'truck',
    'TR': 'truck',

    # Shipments of coal moved to Tidewater Piers and Coastal Ports for further
    # shipments to consumers via coastal water or ocean.  The Tidewater Piers
    # and Coastal Ports are identified by name and location as follows:
    # Dominion Terminal Associates, Newport News, Virginia; McDuffie Coal
    # Terminal, Mobile, Alabama; IC Railmarine Terminal, Convent, Louisiana;
    # International Marine Terminals, Myrtle Grove, Louisiana; Cooper/T. Smith
    # Stevedoring Co. Inc., Darrow, Louisiana; Seward Terminal Inc., Seward,
    # Alaska;  Los Angeles Export Terminal, Inc., Los Angeles, California;
    # Levin-Richmond Terminal Corp., Richmond, California; Baltimore Terminal,
    # Baltimore, Maryland; Norfolk Southern Lamberts Point P-6, Norfolk,
    # Virginia; Chesapeake Bay Piers, Baltimore, Maryland;  Pier IX Terminal
    # Company, Newport News, Virginia;  Electro-Coal Transport Corp., Davant,
    # Louisiana
    'TP': 'coastal_ports',

    'WT': 'other_waterways',
    'WA': 'other_waterways',

    # Shipments of fuel moved to consumers by tramway or conveyor.
    'TC': 'tramway_conveyor',
    'CV': 'tramway_conveyor',

    # Slurry Pipeline: Shipments of coal moved to consumers by slurry pipeline.
    'SP': 'slurry_pipeline',

    # Shipments of fuel moved to consumers by pipeline
    'PL': 'pipeline',

    # Should be replaced with NA
    'UN': 'unknown',
}
"""Descriptive labels for fuel transport modes reported in the EIA 923."""
