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

FUEL_UNITS_EIA: Dict[str, str] = {
    'mcf': 'Thousands of cubic feet (for gases)',
    'short_tons': 'Short tons (for solids)',
    'barrels': 'Barrels (for liquids)',
    'mwh': 'Megawatt hours (for electricity)',
}
"""
Descriptive labels for the units of measure EIA uses for fuels.

The physical units fuel consumption is reported in.  All consumption is reported
in either short tons for solids, thousands of cubic feet for gases, and barrels
for liquids.
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
