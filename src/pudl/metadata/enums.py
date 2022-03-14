"""Enumerations of valid field values."""
from typing import Dict, List

US_STATES: Dict[str, str] = {
    'AK': 'Alaska',
    'AL': 'Alabama',
    'AR': 'Arkansas',
    'AZ': 'Arizona',
    'CA': 'California',
    'CO': 'Colorado',
    'CT': 'Connecticut',
    'DE': 'Delaware',
    'FL': 'Florida',
    'GA': 'Georgia',
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
    'MS': 'Mississippi',
    'MT': 'Montana',
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
    'RI': 'Rhode Island',
    'SC': 'South Carolina',
    'SD': 'South Dakota',
    'TN': 'Tennessee',
    'TX': 'Texas',
    'UT': 'Utah',
    'VA': 'Virginia',
    'VT': 'Vermont',
    'WA': 'Washington',
    'WI': 'Wisconsin',
    'WV': 'West Virginia',
    'WY': 'Wyoming',
}
"""Mapping of US state abbreviations to their full names."""

US_TERRITORIES: Dict[str, str] = {
    'AS': 'American Samoa',
    'DC': 'District of Columbia',
    'GU': 'Guam',
    'MP': 'Northern Mariana Islands',
    'PR': 'Puerto Rico',
    'VI': 'Virgin Islands',
}
"""Mapping of US territory abbreviations to their full names."""

US_STATES_TERRITORIES: Dict[str, str] = {**US_STATES, **US_TERRITORIES}

EPACEMS_STATES: List[str] = [
    state for state in US_STATES_TERRITORIES
    # AK and PR have data but only a few years, and that breaks the Datastore.
    # See https://github.com/catalyst-cooperative/pudl/issues/1264
    if state not in {'AK', 'AS', 'GU', 'HI', 'MP', 'PR', 'VI'}
]
"""The US states and territories that are present in the EPA CEMS dataset."""

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
    'other',
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
]
"""Valid emissions measurement codes for the EPA CEMS hourly data."""
