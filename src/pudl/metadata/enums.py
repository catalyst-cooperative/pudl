"""Enumerations of valid field values."""
from typing import List

NERC_REGIONS: List[str] = [
    'BASN',  # ASSESSMENT AREA Basin (WECC)
    'CALN',  # ASSESSMENT AREA California (WECC)
    'CALS',  # ASSESSMENT AREA California (WECC)
    'DSW',  # ASSESSMENT AREA Desert Southwest (WECC)
    'ASCC',  # Alaska
    'ISONE',  # ISO New England (NPCC)
    'ERCOT',  # lumped under TRE in 2017 Form instructions
    'NORW',  # ASSESSMENT AREA Northwest (WECC)
    'NYISO',  # ISO (NPCC)
    'PJM',  # RTO
    'ROCK',  # ASSESSMENT AREA Rockies (WECC)
    'ECAR',  # OLD RE Now part of RFC and SERC
    'FRCC',  # included in 2017 Form instructions, recently joined with SERC
    'HICC',  # Hawaii
    'MAAC',  # OLD RE Now part of RFC
    'MAIN',  # OLD RE Now part of SERC, RFC, MRO
    'MAPP',  # OLD/NEW RE Became part of MRO, resurfaced in 2010
    'MRO',  # RE included in 2017 Form instructions
    'NPCC',  # RE included in 2017 Form instructions
    'RFC',  # RE included in 2017 Form instructions
    'SERC',  # RE included in 2017 Form instructions
    'SPP',  # RE included in 2017 Form instructions
    'TRE',  # RE included in 2017 Form instructions (included ERCOT)
    'WECC',  # RE included in 2017 Form instructions
    'WSCC',  # OLD RE pre-2002 version of WECC
    'MISO',  # ISO unclear whether technically a regional entity, but lots of entries
    'ECAR_MAAC',
    'MAPP_WECC',
    'RFC_SERC',
    'SPP_WECC',
    'MRO_WECC',
    'ERCOT_SPP',
    'SPP_TRE',
    'ERCOT_TRE',
    'MISO_TRE',
    'VI',  # Virgin Islands
    'GU',  # Guam
    'PR',  # Puerto Rico
    'AS',  # American Samoa
    'UNK',
]
"""
North American Reliability Corporation (NERC) regions.

See https://www.eia.gov/electricity/data/eia411/#tabs_NERC-3.
"""

CUSTOMER_CLASSES: List[str] = [
    "commercial",
    "industrial",
    "direct_connection",
    "other",
    "residential",
    "total",
    "transportation"
]

TECH_CLASSES: List[str] = [
    'backup',  # WHERE Is this used? because removed from DG table b/c not a real component
    'chp_cogen',
    'combustion_turbine',
    'fuel_cell',
    'hydro',
    'internal_combustion',
    'other',
    'pv',
    'steam',
    'storage_pv',
    'all_storage',  # need 'all' as prefix so as not to confuse with other storage category
    'total',
    'virtual_pv',
    'wind',
]

REVENUE_CLASSES: List[str] = [
    'credits_or_adjustments',
    'delivery_customers',
    'other',
    'retail_sales',
    'sales_for_resale',
    'total',
    'transmission',
    'unbundled',
]

RELIABILITY_STANDARDS: List[str] = [
    'ieee_standard',
    'other_standard'
]

FUEL_CLASSES: List[str] = [
    'gas',
    'oil',
    'other',
    'renewable',
    'water',
    'wind',
    'wood',
]

RTO_CLASSES: List[str] = [
    'caiso',
    'ercot',
    'isone',
    'miso',
    'nyiso',
    'other'
    'pjm',
    'spp',
]

EPACEMS_MEASUREMENT_CODES: List[str] = [
    "Calculated",
    "LME",
    "Measured",
    "Measured and Substitute",
    "Other",  # ¿Should be replaced with NA?
    "Substitute",
    "Undetermined",  # Should be replaced with NA
    "Unknown Code",  # Should be replaced with NA
    "",  # Should be replaced with NA
]
"""Valid emissions measurement codes for the EPA CEMS hourly data."""
