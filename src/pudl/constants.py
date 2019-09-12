"""
A warehouse for constant values required to initilize the PUDL Database.

This constants module stores and organizes a bunch of constant values which are
used throughout PUDL to populate static lists within the data packages or for
data cleaning purposes.
"""

import importlib.resources

import pandas as pd
import sqlalchemy as sa

######################################################################
# Constants used within the init.py module.
######################################################################
prime_movers = [
    'steam_turbine',
    'gas_turbine',
    'hydro',
    'internal_combustion',
    'solar_pv',
    'wind_turbine'
]
"""list: A list of the types of prime movers"""

rto_iso = {
    'CAISO': 'California ISO',
    'ERCOT': 'Electric Reliability Council of Texas',
    'MISO': 'Midcontinent ISO',
    'ISO-NE': 'ISO New England',
    'NYISO': 'New York ISO',
    'PJM': 'PJM Interconnection',
    'SPP': 'Southwest Power Pool'
}
"""dict: A dictionary containing ISO/RTO abbreviations (keys) and names (values)
"""

us_states = {
    'AK': 'Alaska',
    'AL': 'Alabama',
    'AR': 'Arkansas',
    'AS': 'American Samoa',
    'AZ': 'Arizona',
    'CA': 'California',
    'CO': 'Colorado',
    'CT': 'Connecticut',
    'DC': 'District of Columbia',
    'DE': 'Delaware',
    'FL': 'Florida',
    'GA': 'Georgia',
    'GU': 'Guam',
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
    'MP': 'Northern Mariana Islands',
    'MS': 'Mississippi',
    'MT': 'Montana',
    'NA': 'National',
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
    'PR': 'Puerto Rico',
    'RI': 'Rhode Island',
    'SC': 'South Carolina',
    'SD': 'South Dakota',
    'TN': 'Tennessee',
    'TX': 'Texas',
    'UT': 'Utah',
    'VA': 'Virginia',
    'VI': 'Virgin Islands',
    'VT': 'Vermont',
    'WA': 'Washington',
    'WI': 'Wisconsin',
    'WV': 'West Virginia',
    'WY': 'Wyoming'
}
"""dict: A dictionary containing US state abbreviations (keys) and names
    (values)
"""
canada_prov_terr = {
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
"""dict: A dictionary containing Canadian provinces' and territories'
    abbreviations (keys) and names (values)
"""

cems_states = {k: v for k, v in us_states.items() if v not in
               {'Alaska',
                'American Samoa',
                'Guam',
                'Hawaii',
                'Northern Mariana Islands',
                'National',
                'Puerto Rico',
                'Virgin Islands'}
               }
"""dict: A dictionary containing US state abbreviations (keys) and names
    (values) that are present in the CEMS dataset
"""

# This is imperfect for states that have split timezones. See:
# https://en.wikipedia.org/wiki/List_of_time_offsets_by_U.S._state_and_territory
# For states that are split, I went with where there seem to be more people
# List of timezones in pytz.common_timezones
# Canada: https://en.wikipedia.org/wiki/Time_in_Canada#IANA_time_zone_database
state_tz_approx = {
    "AK": "US/Alaska",            # Alaska; Not in CEMS
    "AL": "US/Central",           # Alabama
    "AR": "US/Central",           # Arkansas
    "AS": "Pacific/Pago_Pago",    # American Samoa; Not in CEMS
    "AZ": "US/Arizona",           # Arizona
    "CA": "US/Pacific",           # California
    "CO": "US/Mountain",          # Colorado
    "CT": "US/Eastern",           # Connecticut
    "DC": "US/Eastern",           # District of Columbia
    "DE": "US/Eastern",           # Delaware
    "FL": "US/Eastern",           # Florida (split state)
    "GA": "US/Eastern",           # Georgia
    "GU": "Pacific/Guam",         # Guam; Not in CEMS
    "HI": "US/Hawaii",            # Hawaii; Not in CEMS
    "IA": "US/Central",           # Iowa
    "ID": "US/Mountain",          # Idaho (split state)
    "IL": "US/Central",           # Illinois
    "IN": "US/Eastern",           # Indiana (split state)
    "KS": "US/Central",           # Kansas (split state)
    "KY": "US/Eastern",           # Kentucky (split state)
    "LA": "US/Central",           # Louisiana
    "MA": "US/Eastern",           # Massachusetts
    "MD": "US/Eastern",           # Maryland
    "ME": "US/Eastern",           # Maine
    "MI": "America/Detroit",      # Michigan (split state)
    "MN": "US/Central",           # Minnesota
    "MO": "US/Central",           # Missouri
    "MP": "Pacific/Saipan",       # Northern Mariana Islands; Not in CEMS
    "MS": "US/Central",           # Mississippi
    "MT": "US/Mountain",          # Montana
    "NC": "US/Eastern",           # North Carolina
    "ND": "US/Central",           # North Dakota (split state)
    "NE": "US/Central",           # Nebraska (split state)
    "NH": "US/Eastern",           # New Hampshire
    "NJ": "US/Eastern",           # New Jersey
    "NM": "US/Mountain",          # New Mexico
    "NV": "US/Pacific",           # Nevada
    "NY": "US/Eastern",           # New York
    "OH": "US/Eastern",           # Ohio
    "OK": "US/Central",           # Oklahoma
    "OR": "US/Pacific",           # Oregon (split state)
    "PA": "US/Eastern",           # Pennsylvania
    "PR": "America/Puerto_Rico",  # Puerto Rico; Not in CEMS
    "RI": "US/Eastern",           # Rhode Island
    "SC": "US/Eastern",           # South Carolina
    "SD": "US/Central",           # South Dakota (split state)
    "TN": "US/Central",           # Tennessee
    "TX": "US/Central",           # Texas
    "UT": "US/Mountain",          # Utah
    "VA": "US/Eastern",           # Virginia
    "VI": "America/Puerto_Rico",  # Virgin Islands; Not in CEMS
    "VT": "US/Eastern",           # Vermont
    "WA": "US/Pacific",           # Washington
    "WI": "US/Central",           # Wisconsin
    "WV": "US/Eastern",           # West Virginia
    "WY": "US/Mountain",          # Wyoming
    # Canada (none of these are in CEMS)
    "AB": "America/Edmonton",     # Alberta
    "BC": "America/Vancouver",    # British Columbia (split province)
    "MB": "America/Winnipeg",     # Manitoba
    "NB": "America/Moncton",      # New Brunswick
    "NS": "America/Halifax",      # Nova Scotia
    "NL": "America/St_Johns",     # Newfoundland and Labrador  (split province)
    "NT": "America/Yellowknife",  # Northwest Territories (split province)
    "NU": "America/Iqaluit",      # Nunavut (split province)
    "ON": "America/Toronto",      # Ontario (split province)
    "PE": "America/Halifax",      # Prince Edwards Island
    "QC": "America/Montreal",     # Quebec (split province)
    "SK": "America/Regina",       # Saskatchewan  (split province)
    "YT": "America/Whitehorse",   # Yukon Territory
}
"""dict: A dictionary containing US and Canadian state/territory abbreviations
    (keys) and timezones (values)
"""

travis_ci_ferc1_years = (2017, )
"""tuple: A tuple containing years of FERC1 data to use with Travis continous
    integration.
"""
travis_ci_eia860_years = (2017, )
"""tuple: A tuple containing years of EIA 860 data to use with Travis continuous
    integration.
"""
travis_ci_eia923_years = (2017, )
"""
tuple: A tuple containing years of EIA 923 data to use with Travis continuous
    integration.
"""
travis_ci_epacems_years = (2017, )
"""tuple: A tuple containing years of EPA CEMS data to use with Travis
    continuous integration.
"""
travis_ci_epacems_states = ('ID', )
"""tuple: A tuple containing states whose EPA CEMS data are used with Travis
    continuous integration.
"""
# Construct a dictionary mapping a canonical fuel name to a list of strings
# which are used to represent that fuel in the FERC Form 1 Reporting. Case is
# ignored, as all fuel strings can be converted to a lower case in the data
# set.
# Previous categories of ferc1_biomass_strings and ferc1_stream_strings have
# been deleted and their contents redistributed to ferc1_waste_strings and
# ferc1_other_strings

ferc1_coal_strings = \
    ['coal', 'coal-subbit', 'lignite', 'coal(sb)', 'coal (sb)', 'coal-lignite',
     'coke', 'coa', 'lignite/coal', 'coal - subbit', 'coal-subb', 'coal-sub',
     'coal-lig', 'coal-sub bit', 'coals', 'ciak', 'petcoke']
"""
list: A list of strings which are used to represent coal fuel in FERC Form 1
    reporting.
"""

ferc1_oil_strings = \
    ['oil', '#6 oil', '#2 oil', 'fuel oil', 'jet', 'no. 2 oil', 'no.2 oil',
     'no.6& used', 'used oil', 'oil-2', 'oil (#2)', 'diesel oil',
     'residual oil', '# 2 oil', 'resid. oil', 'tall oil', 'oil/gas',
     'no.6 oil', 'oil-fuel', 'oil-diesel', 'oil / gas', 'oil bbls', 'oil bls',
     'no. 6 oil', '#1 kerosene', 'diesel', 'no. 2 oils', 'blend oil',
     '#2oil diesel', '#2 oil-diesel', '# 2  oil', 'light oil', 'heavy oil',
     'gas.oil', '#2', '2', '6', 'bbl', 'no 2 oil', 'no 6 oil', '#1 oil', '#6',
     'oil-kero', 'oil bbl', 'biofuel', 'no 2', 'kero', '#1 fuel oil',
     'no. 2  oil', 'blended oil', 'no 2. oil', '# 6 oil', 'nno. 2 oil',
     '#2 fuel', 'oill', 'oils', 'gas/oil', 'no.2 oil gas', '#2 fuel oil',
     'oli', 'oil (#6)', 'oil/diesel', '2 Oil']
"""
list: A list of strings which are used to represent oil fuel in FERC Form 1
    reporting.
"""

ferc1_gas_strings = \
    ['gas', 'gass', 'methane', 'natural gas', 'blast gas', 'gas mcf',
     'propane', 'prop', 'natural  gas', 'nat.gas', 'nat gas',
     'nat. gas', 'natl gas', 'ga', 'gas`', 'syngas', 'ng', 'mcf',
     'blast gaa', 'nat  gas', 'gac', 'syngass', 'prop.', 'natural', 'coal.gas']
"""
list: A list of strings which are used to represent gas fuel in FERC Form 1
    reporting.
"""

ferc1_solar_strings = []

ferc1_wind_strings = []

ferc1_hydro_strings = []

ferc1_nuke_strings = \
    ['nuclear', 'grams of uran', 'grams of', 'grams of  ura',
     'grams', 'nucleur', 'nulear', 'nucl', 'nucleart']
"""
list: A list of strings which are used to represent nuclear fuel in FERC Form
    1 reporting.
"""

ferc1_waste_strings = ['tires', 'tire', 'refuse', 'switchgrass',
                       'wood waste', 'woodchips', 'biomass', 'wood',
                       'wood chips', 'rdf']
"""
list: A list of strings which are used to represent waste fuel in FERC Form 1
    reporting.
"""

ferc1_other_strings = ['steam', 'purch steam',
                       'purch. steam', 'other', 'composite', 'composit',
                       'mbtus']
"""list: A list of strings which are used to represent other fuels in FERC Form
    1 reporting.
"""

# There are also a bunch of other weird and hard to categorize strings
# that I don't know what to do with... hopefully they constitute only a
# small fraction of the overall generation.

ferc1_fuel_strings = {'coal': ferc1_coal_strings,
                      'oil': ferc1_oil_strings,
                      'gas': ferc1_gas_strings,
                      'solar': ferc1_solar_strings,
                      'wind': ferc1_wind_strings,
                      'hydro': ferc1_hydro_strings,
                      'nuclear': ferc1_nuke_strings,
                      'waste': ferc1_waste_strings,
                      'other': ferc1_other_strings
                      }
"""dict: A dictionary linking fuel types (keys) to lists of various strings
    representing that fuel (values)
"""
# Similarly, dictionary for cleaning up fuel unit strings
ferc1_ton_strings = ['toms', 'taons', 'tones', 'col-tons', 'toncoaleq', 'coal',
                     'tons coal eq', 'coal-tons', 'ton', 'tons', 'tons coal',
                     'coal-ton', 'tires-tons']
"""list: A list of fuel unit strings for tons."""

ferc1_mcf_strings = \
    ['mcf', "mcf's", 'mcfs', 'mcf.', 'gas mcf', '"gas" mcf', 'gas-mcf',
     'mfc', 'mct', ' mcf', 'msfs', 'mlf', 'mscf', 'mci', 'mcl', 'mcg',
     'm.cu.ft.']
"""list: A list of fuel unit strings for thousand cubic feet."""

ferc1_bbl_strings = \
    ['barrel', 'bbls', 'bbl', 'barrels', 'bbrl', 'bbl.', 'bbls.',
     'oil 42 gal', 'oil-barrels', 'barrrels', 'bbl-42 gal',
     'oil-barrel', 'bb.', 'barrells', 'bar', 'bbld', 'oil- barrel',
     'barrels    .', 'bbl .', 'barels', 'barrell', 'berrels', 'bb',
     'bbl.s', 'oil-bbl', 'bls', 'bbl:', 'barrles', 'blb', 'propane-bbl']
"""list: A list of fuel unit strings for barrels."""

ferc1_gal_strings = ['gallons', 'gal.', 'gals', 'gals.', 'gallon', 'gal']
"""list: A list of fuel unit strings for gallons."""

ferc1_1kgal_strings = ['oil(1000 gal)', 'oil(1000)', 'oil (1000)', 'oil(1000']
"""list: A list of fuel unit strings for thousand gallons."""

ferc1_gramsU_strings = [  # noqa: N816 (U-ranium is capitalized...)
    'gram', 'grams', 'gm u', 'grams u235', 'grams u-235', 'grams of uran',
    'grams: u-235', 'grams:u-235', 'grams:u235', 'grams u308', 'grams: u235',
    'grams of'
]
"""list: A list of fuel unit strings for grams."""

ferc1_kgU_strings = [  # noqa: N816 (U-ranium is capitalized...)
    'kg of uranium', 'kg uranium', 'kilg. u-235', 'kg u-235', 'kilograms-u23',
    'kg', 'kilograms u-2', 'kilograms', 'kg of'
]
"""list: A list of fuel unit strings for thousand grams."""

ferc1_mmbtu_strings = ['mmbtu', 'mmbtus',
                       "mmbtu's", 'nuclear-mmbtu', 'nuclear-mmbt']
"""list: A list of fuel unit strings for million British Thermal Units."""

ferc1_mwdth_strings = \
    ['mwd therman', 'mw days-therm', 'mwd thrml', 'mwd thermal',
     'mwd/mtu', 'mw days', 'mwdth', 'mwd', 'mw day']
"""list: A list of fuel unit strings for megawatt days thermal."""

ferc1_mwhth_strings = ['mwh them', 'mwh threm',
                       'nwh therm', 'mwhth', 'mwh therm', 'mwh']
"""list: A list of fuel unit strings for megawatt hours thermal."""

ferc1_fuel_unit_strings = {'ton': ferc1_ton_strings,
                           'mcf': ferc1_mcf_strings,
                           'bbl': ferc1_bbl_strings,
                           'gal': ferc1_gal_strings,
                           '1kgal': ferc1_1kgal_strings,
                           'gramsU': ferc1_gramsU_strings,
                           'kgU': ferc1_kgU_strings,
                           'mmbtu': ferc1_mmbtu_strings,
                           'mwdth': ferc1_mwdth_strings,
                           'mwhth': ferc1_mwhth_strings
                           }
"""
dict: A dictionary linking fuel units (keys) to lists of various strings
    representing those fuel units (values)
"""

# Categorizing the strings from the FERC Form 1 Plant Kind (plant_kind) field
# into lists. There are many strings that weren't categorized,
# Solar and Solar Project were not classified as these do not indicate if they
# are solar thermal or photovoltaic. Variants on Steam (e.g. "steam 72" and
# "steam and gas") were classified based on additional research of the plants
# on the Internet.

ferc1_plant_kind_steam_turbine = \
    ['coal', 'steam', 'steam units 1 2 3', 'steam units 4 5',
     'steam fossil', 'steam turbine', 'steam a', 'steam 100',
     'steam units 1 2 3', 'steams', 'steam 1', 'steam retired 2013', 'stream']
"""
list: A list of strings from FERC Form 1 for the steam turbine plant kind.
"""

ferc1_plant_kind_combustion_turbine = \
    ['combustion turbine', 'gt', 'gas turbine',
     'gas turbine # 1', 'gas turbine', 'gas turbine (note 1)',
     'gas turbines', 'simple cycle', 'combustion turbine',
     'comb.turb.peak.units', 'gas turbine', 'combustion turbine',
     'com turbine peaking', 'gas turbine peaking', 'comb turb peaking',
     'combustine turbine', 'comb. turine', 'conbustion turbine',
     'combustine turbine', 'gas turbine (leased)', 'combustion tubine',
     'gas turb', 'gas turbine peaker', 'gtg/gas', 'simple cycle turbine',
     'gas-turbine', 'gas turbine-simple', 'gas turbine - note 1',
     'gas turbine #1', 'simple cycle', 'gasturbine', 'combustionturbine',
     'gas turbine (2)', 'comb turb peak units', 'jet engine']
"""list: A list of strings from FERC Form 1 for the combustion turbine plant
    kind.
"""

ferc1_plant_kind_combined_cycle = \
    ['Combined cycle', 'combined cycle', 'combined',
     'gas turb. & heat rec', 'combined cycle', 'com. cyc', 'com. cycle',
     'gas turb-combined cy', 'combined cycle ctg', 'combined cycle - 40%',
     'com cycle gas turb', 'combined cycle oper', 'gas turb/comb. cyc',
     'combine cycle', 'cc', 'comb. cycle', 'gas turb-combined cy',
     'steam and cc', 'steam cc', 'gas steam', 'ctg steam gas',
     'steam comb cycle', ]
"""
list: A list of strings from FERC Form 1 for the combined cycle plant kind.
"""

ferc1_plant_kind_nuke = ['nuclear', 'nuclear (3)']
"""list: A list of strings from FERC Form 1 for the nuclear plant kind."""

ferc1_plant_kind_geothermal = ['steam - geothermal', 'steam_geothermal']
"""list: A list of strings from FERC Form 1 for the geothermal plant kind."""

ferc_1_plant_kind_internal_combustion = \
    ['ic', 'internal combustion',
     'diesel turbine', 'int combust (note 1)', 'int. combust (note1)',
     'int.combustine', 'comb. cyc', 'internal comb', 'diesel', 'diesel engine',
     'internal combustion', 'int combust - note 1', 'int. combust - note1',
     'internal comb recip', 'reciprocating engine', 'comb. turbine']
"""
list: A list of strings from FERC Form 1 for the internal combustion plant
    kind.
"""

ferc1_plant_kind_wind = ['wind', 'wind energy',
                         'wind turbine', 'wind - turbine']
"""list: A list of strings from FERC Form 1 for the wind plant kind."""

ferc1_plant_kind_photovoltaic = ['solar photovoltaic', 'photovoltaic']
"""list: A list of strings from FERC Form 1 for the photovoltaic plant kind."""

ferc1_plant_kind_solar_thermal = ['solar thermal']
"""
list: A list of strings from FERC Form 1 for the solar thermal plant kind.
"""

# Making a dictionary of lists from the lists of plant_fuel strings to create
# a dictionary of plant fuel lists.

ferc1_plant_kind_strings = {
    'steam': ferc1_plant_kind_steam_turbine,
    'combustion_turbine': ferc1_plant_kind_combustion_turbine,
    'combined_cycle': ferc1_plant_kind_combined_cycle,
    'nuclear': ferc1_plant_kind_nuke,
    'geothermal': ferc1_plant_kind_geothermal,
    'internal_combustion': ferc_1_plant_kind_internal_combustion,
    'wind': ferc1_plant_kind_wind,
    'photovoltaic': ferc1_plant_kind_photovoltaic,
    'solar_thermal': ferc1_plant_kind_solar_thermal
}
"""
dict: A dictionary of plant kinds (keys) and associated lists of plant_fuel
    strings (values).
"""

# This is an alternative set of strings for simplifying the plant kind field
# from Uday & Laura at CPI. For the moment we have reverted to using our own
# categorizations which are more detailed, but these are preserved here for
# comparison and testing, if need be.
cpi_diesel_strings = ['DIESEL', 'Diesel Engine', 'Diesel Turbine', ]
"""
list: A list of strings for fuel type diesel compiled by Climate Policy
    Initiative.
"""
cpi_geothermal_strings = ['Steam - Geothermal', ]
"""
list: A list of strings for fuel type geothermal compiled by Climate Policy
    Initiative.
"""
cpi_natural_gas_strings = [
    'Combined Cycle', 'Combustion Turbine', 'GT',
    'GAS TURBINE', 'Comb. Turbine', 'Gas Turbine #1', 'Combine Cycle Oper',
    'Combustion', 'Combined', 'Gas Turbine/Steam', 'Gas Turbine Peaker',
    'Gas Turbine - Note 1', 'Resp Share Gas Note3', 'Gas Turbines',
    'Simple Cycle', 'Gas / Steam', 'GasTurbine', 'Combine Cycle',
    'CTG/Steam-Gas', 'GTG/Gas', 'CTG/Steam -Gas', 'Steam/Gas Turbine',
    'CombustionTurbine', 'Gas Turbine-Simple', 'STEAM & GAS TURBINE',
    'Gas & Steam Turbine', 'Gas', 'Gas Turbine (2)', 'COMBUSTION AND GAS',
    'Com Turbine Peaking', 'Gas Turbine Peaking', 'Comb Turb Peaking',
    'JET ENGINE', 'Comb. Cyc', 'Com. Cyc', 'Com. Cycle',
    'GAS TURB-COMBINED CY', 'Gas Turb', 'Combined Cycle - 40%',
    'IGCC/Gas Turbine', 'CC', 'Combined Cycle Oper', 'Simple Cycle Turbine',
    'Steam and CC', 'Com Cycle Gas Turb', 'I.C.E/  Gas Turbine',
    'Combined Cycle CTG', 'GAS-TURBINE', 'Gas Expander Turbine',
    'Gas Turbine (Leased)', 'Gas Turbine # 1', 'Gas Turbine (Note 1)',
    'COMBUSTINE TURBINE', 'Gas Turb, Int. Comb.', 'Combined Turbine',
    'Comb Turb Peak Units', 'Combustion Tubine', 'Comb. Cycle',
    'COMB.TURB.PEAK.UNITS', 'Steam  and  CC', 'I.C.E. /Gas Turbine',
    'Conbustion Turbine', 'Gas Turbine/Int Comb', 'Steam & CC',
    'GAS TURB. & HEAT REC', 'Gas Turb/Comb. Cyc', 'Comb. Turine',
]
"""list: A list of strings for fuel type gas compiled by Climate Policy
    Initiative.
"""
cpi_nuclear_strings = ['Nuclear', 'Nuclear (3)', ]
"""list: A list of strings for fuel type nuclear compiled by Climate Policy
    Initiative.
"""
cpi_other_strings = [
    'IC', 'Internal Combustion', 'Int Combust - Note 1',
    'Resp. Share - Note 2', 'Int. Combust - Note1', 'Resp. Share - Note 4',
    'Resp Share - Note 5', 'Resp. Share - Note 7', 'Internal Comb Recip',
    'Reciprocating Engine', 'Internal Comb', 'Resp. Share - Note 8',
    'Resp. Share - Note 9', 'Resp Share - Note 11', 'Resp. Share - Note 6',
    'INT.COMBUSTINE', 'Steam (Incl I.C.)', 'Other', 'Int Combust (Note 1)',
    'Resp. Share (Note 2)', 'Int. Combust (Note1)', 'Resp. Share (Note 8)',
    'Resp. Share (Note 9)', 'Resp Share (Note 11)', 'Resp. Share (Note 4)',
    'Resp. Share (Note 6)', 'Plant retired- 2013', 'Retired - 2013',
]
"""list: A list of strings for fuel type other compiled by Climate Policy
    Initiative.
"""
cpi_steam_strings = [
    'Steam', 'Steam Units 1, 2, 3', 'Resp Share St Note 3',
    'Steam Turbine', 'Steam-Internal Comb', 'IGCC', 'Steam- 72%', 'Steam (1)',
    'Steam (1)', 'Steam Units 1,2,3', 'Steam/Fossil', 'Steams', 'Steam - 72%',
    'Steam - 100%', 'Stream', 'Steam Units 4, 5', 'Steam - 64%', 'Common',
    'Steam (A)', 'Coal', 'Steam;Retired - 2013', 'Steam Units 4 & 6',
]
"""list: A list of strings for fuel type steam compiled by Climate Policy
    Initiative.
"""
cpi_wind_strings = ['Wind', 'Wind Turbine', 'Wind - Turbine', 'Wind Energy', ]
"""list: A list of strings for fuel type wind compiled by Climate Policy
    Initiative.
"""
cpi_solar_strings = [
    'Solar Photovoltaic', 'Solar Thermal', 'SOLAR PROJECT', 'Solar',
    'Photovoltaic',
]
"""list: A list of strings for fuel type photovoltaic compiled by Climate Policy
    Initiative.
"""
cpi_plant_kind_strings = {
    'natural_gas': cpi_natural_gas_strings,
    'diesel': cpi_diesel_strings,
    'geothermal': cpi_geothermal_strings,
    'nuclear': cpi_nuclear_strings,
    'steam': cpi_steam_strings,
    'wind': cpi_wind_strings,
    'solar': cpi_solar_strings,
    'other': cpi_other_strings,
}
"""dict: A dictionary linking fuel types (keys) to lists of strings associated
    by Climate Policy Institute with those fuel types (values).
"""
# Categorizing the strings from the FERC Form 1 Type of Plant Construction
# (construction_type) field into lists.
# There are many strings that weren't categorized, including crosses between
# conventional and outdoor, PV, wind, combined cycle, and internal combustion.
# The lists are broken out into the two types specified in Form 1:
# conventional and outdoor. These lists are inclusive so that variants of
# conventional (e.g. "conventional full") and outdoor (e.g. "outdoor full"
# and "outdoor hrsg") are included.

ferc1_const_type_outdoor = [
    'outdoor', 'outdoor boiler', 'full outdoor', 'outdoor boiler',
    'outdoor boilers', 'outboilers', 'fuel outdoor', 'full outdoor',
    'outdoors', 'outdoor', 'boiler outdoor& full', 'boiler outdoor&full',
    'outdoor boiler& full', 'full -outdoor', 'outdoor steam',
    'outdoor boiler', 'ob', 'outdoor automatic', 'outdoor repower',
    'full outdoor boiler', 'fo', 'outdoor boiler & ful', 'full-outdoor',
    'fuel outdoor', 'outoor', 'outdoor', 'outdoor  boiler&full',
    'boiler outdoor &full', 'outdoor boiler &full', 'boiler outdoor & ful',
    'outdoor-boiler', 'outdoor - boiler', 'outdoor const.',
    '4 outdoor boilers', '3 outdoor boilers', 'full outdoor', 'full outdoors',
    'full oudoors', 'outdoor (auto oper)', 'outside boiler',
    'outdoor boiler&full', 'outdoor hrsg', 'outdoor hrsg', 'semi-outdoor',
    'semi - outdoor'
]
"""list: A list of strings from FERC Form 1 associated with the outdoor
    construction type.
"""

ferc1_const_type_conventional = [
    'conventional', 'conventional', 'conventional boiler', 'conv-b',
    'conventionall', 'convention', 'conventional', 'coventional',
    'conven full boiler', 'c0nventional', 'conventtional', 'convential'
    'underground', 'conventional bulb', 'conventrional'
]
"""list: A list of strings from FERC Form 1 associated with the conventional
    construction type.
"""

# Making a dictionary of lists from the lists of construction_type strings to
# create a dictionary of construction type lists.

ferc1_const_type_strings = {
    'outdoor': ferc1_const_type_outdoor,
    'conventional': ferc1_const_type_conventional
}
"""dict: A dictionary of construction types (keys) and lists of construction
    type strings associated with each type (values) from FERC Form 1.
"""

ferc1_power_purchase_type = {
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
"""dict: A dictionary of abbreviations (keys) and types (values) for power
    purchase agreements from FERC Form 1.
"""

# Dictionary mapping DBF files (w/o .DBF file extension) to DB table names
ferc1_dbf2tbl = {
    'F1_1': 'f1_respondent_id',
    'F1_2': 'f1_acb_epda',
    'F1_3': 'f1_accumdepr_prvsn',
    'F1_4': 'f1_accumdfrrdtaxcr',
    'F1_5': 'f1_adit_190_detail',
    'F1_6': 'f1_adit_190_notes',
    'F1_7': 'f1_adit_amrt_prop',
    'F1_8': 'f1_adit_other',
    'F1_9': 'f1_adit_other_prop',
    'F1_10': 'f1_allowances',
    'F1_11': 'f1_bal_sheet_cr',
    'F1_12': 'f1_capital_stock',
    'F1_13': 'f1_cash_flow',
    'F1_14': 'f1_cmmn_utlty_p_e',
    'F1_15': 'f1_comp_balance_db',
    'F1_16': 'f1_construction',
    'F1_17': 'f1_control_respdnt',
    'F1_18': 'f1_co_directors',
    'F1_19': 'f1_cptl_stk_expns',
    'F1_20': 'f1_csscslc_pcsircs',
    'F1_21': 'f1_dacs_epda',
    'F1_22': 'f1_dscnt_cptl_stk',
    'F1_23': 'f1_edcfu_epda',
    'F1_24': 'f1_elctrc_erg_acct',
    'F1_25': 'f1_elctrc_oper_rev',
    'F1_26': 'f1_elc_oper_rev_nb',
    'F1_27': 'f1_elc_op_mnt_expn',
    'F1_28': 'f1_electric',
    'F1_29': 'f1_envrnmntl_expns',
    'F1_30': 'f1_envrnmntl_fclty',
    'F1_31': 'f1_fuel',
    'F1_32': 'f1_general_info',
    'F1_33': 'f1_gnrt_plant',
    'F1_34': 'f1_important_chg',
    'F1_35': 'f1_incm_stmnt_2',
    'F1_36': 'f1_income_stmnt',
    'F1_37': 'f1_miscgen_expnelc',
    'F1_38': 'f1_misc_dfrrd_dr',
    'F1_39': 'f1_mthly_peak_otpt',
    'F1_40': 'f1_mtrl_spply',
    'F1_41': 'f1_nbr_elc_deptemp',
    'F1_42': 'f1_nonutility_prop',
    'F1_43': 'f1_note_fin_stmnt',  # 37% of DB
    'F1_44': 'f1_nuclear_fuel',
    'F1_45': 'f1_officers_co',
    'F1_46': 'f1_othr_dfrrd_cr',
    'F1_47': 'f1_othr_pd_in_cptl',
    'F1_48': 'f1_othr_reg_assets',
    'F1_49': 'f1_othr_reg_liab',
    'F1_50': 'f1_overhead',
    'F1_51': 'f1_pccidica',
    'F1_52': 'f1_plant_in_srvce',
    'F1_53': 'f1_pumped_storage',
    'F1_54': 'f1_purchased_pwr',
    'F1_55': 'f1_reconrpt_netinc',
    'F1_56': 'f1_reg_comm_expn',
    'F1_57': 'f1_respdnt_control',
    'F1_58': 'f1_retained_erng',
    'F1_59': 'f1_r_d_demo_actvty',
    'F1_60': 'f1_sales_by_sched',
    'F1_61': 'f1_sale_for_resale',
    'F1_62': 'f1_sbsdry_totals',
    'F1_63': 'f1_schedules_list',
    'F1_64': 'f1_security_holder',
    'F1_65': 'f1_slry_wg_dstrbtn',
    'F1_66': 'f1_substations',
    'F1_67': 'f1_taxacc_ppchrgyr',
    'F1_68': 'f1_unrcvrd_cost',
    'F1_69': 'f1_utltyplnt_smmry',
    'F1_70': 'f1_work',
    'F1_71': 'f1_xmssn_adds',
    'F1_72': 'f1_xmssn_elc_bothr',
    'F1_73': 'f1_xmssn_elc_fothr',
    'F1_74': 'f1_xmssn_line',
    'F1_75': 'f1_xtraordnry_loss',
    'F1_76': 'f1_codes_val',
    'F1_77': 'f1_sched_lit_tbl',
    'F1_78': 'f1_audit_log',
    'F1_79': 'f1_col_lit_tbl',
    'F1_80': 'f1_load_file_names',
    'F1_81': 'f1_privilege',
    'F1_82': 'f1_sys_error_log',
    'F1_83': 'f1_unique_num_val',
    'F1_84': 'f1_row_lit_tbl',
    'F1_85': 'f1_footnote_data',
    'F1_86': 'f1_hydro',
    'F1_87': 'f1_footnote_tbl',  # 52% of DB
    'F1_88': 'f1_ident_attsttn',
    'F1_89': 'f1_steam',
    'F1_90': 'f1_leased',
    'F1_91': 'f1_sbsdry_detail',
    'F1_92': 'f1_plant',
    'F1_93': 'f1_long_term_debt',
    'F1_106_2009': 'f1_106_2009',
    'F1_106A_2009': 'f1_106a_2009',
    'F1_106B_2009': 'f1_106b_2009',
    'F1_208_ELC_DEP': 'f1_208_elc_dep',
    'F1_231_TRN_STDYCST': 'f1_231_trn_stdycst',
    'F1_324_ELC_EXPNS': 'f1_324_elc_expns',
    'F1_325_ELC_CUST': 'f1_325_elc_cust',
    'F1_331_TRANSISO': 'f1_331_transiso',
    'F1_338_DEP_DEPL': 'f1_338_dep_depl',
    'F1_397_ISORTO_STL': 'f1_397_isorto_stl',
    'F1_398_ANCL_PS': 'f1_398_ancl_ps',
    'F1_399_MTH_PEAK': 'f1_399_mth_peak',
    'F1_400_SYS_PEAK': 'f1_400_sys_peak',
    'F1_400A_ISO_PEAK': 'f1_400a_iso_peak',
    'F1_429_TRANS_AFF': 'f1_429_trans_aff',
    'F1_ALLOWANCES_NOX': 'f1_allowances_nox',
    'F1_CMPINC_HEDGE_A': 'f1_cmpinc_hedge_a',
    'F1_CMPINC_HEDGE': 'f1_cmpinc_hedge',
    'F1_EMAIL': 'f1_email',
    'F1_RG_TRN_SRV_REV': 'f1_rg_trn_srv_rev',
    'F1_S0_CHECKS': 'f1_s0_checks',
    'F1_S0_FILING_LOG': 'f1_s0_filing_log',
    'F1_SECURITY': 'f1_security'
    # 'F1_PINS': 'f1_pins',  # private data, not publicized.
    # 'F1_FREEZE': 'f1_freeze', # private data, not publicized
}
"""dict: A dictionary mapping FERC Form 1 DBF files (w/o .DBF file extension)
    (keys) to database table names (values).
"""

ferc1_huge_tables = {
    'f1_footnote_tbl',
    'f1_footnote_data',
    'f1_note_fin_stmnt',
}
"""set: A set containing large FERC Form 1 tables.
"""

# Invert the map above so we can go either way as needed
ferc1_tbl2dbf = {v: k for k, v in ferc1_dbf2tbl.items()}

"""dict: A dictionary mapping database table names (keys) to FERC Form 1 DBF
    files (w/o .DBF file extension) (values).
"""
# This is a dictionary of respondents (respondent_id: respondent_name) that are
# missing from the ferc respondent table but show up in other tables. We are
# inserting them into the respondent table and hope to go back to determine
# what the real names of the respondents are to insert them.
missing_respondents_ferc1 = {514: 'respondent_514',
                             515: 'respondent_515',
                             516: 'respondent_516',
                             517: 'respondent_517',
                             518: 'respondent_518',
                             519: 'respondent_519',
                             522: 'respondent_522'}
"""dict: A dictionary of missing FERC Form 1 respondent IDs (keys) and names
    (values).
"""

# This dictionary maps the strings which are used to denote field types in the
# DBF objects to the corresponding generic SQLAlchemy Column types:
# These definitions come from a combination of the dbfread example program
# dbf2sqlite and this DBF file format documentation page:
# http://www.dbase.com/KnowledgeBase/int/db7_file_fmt.htm
# Un-mapped types left as 'XXX' which should obviously make an error...
dbf_typemap = {
    'C': sa.String,
    'D': sa.Date,
    'F': sa.Float,
    'I': sa.Integer,
    'L': sa.Boolean,
    'M': sa.Text,  # 10 digit .DBT block number, stored as a string...
    'N': sa.Float,
    'T': sa.DateTime,
    '0': sa.Integer,  # based on dbf2sqlite mapping
    'B': 'XXX',  # .DBT block number, binary string
    '@': 'XXX',  # Timestamp... Date = Julian Day, Time is in milliseconds?
    '+': 'XXX',  # Autoincrement (e.g. for IDs)
    'O': 'XXX',  # Double, 8 bytes
    'G': 'XXX',  # OLE 10 digit/byte number of a .DBT block, stored as string
}
"""dict: A dictionary mapping field types in the DBF objects (keys) to the
    corresponding generic SQLAlchemy Column types.
"""
# These are the FERC Form 1 DB tables that we're focusing on initially.
ferc1_default_tables = (
    'f1_respondent_id',
    'f1_fuel',
    'f1_steam',
    'f1_gnrt_plant',
    'f1_hydro',
    'f1_pumped_storage',
    'f1_plant_in_srvce',
    'f1_purchased_pwr',
    'f1_accumdepr_prvsn',
    'f1_general_info',  # For Uday Varadarajan / RMI
    'f1_row_lit_tbl',  # For Uday Varadarajan / RMI
    'f1_edcfu_epda',  # For Uday Varadarajan / RMI
    'f1_dacs_epda',  # For Uday Varadarajan / RMI
    'f1_sales_by_sched',  # For Uday Varadarajan / RMI
    'f1_sale_for_resale',  # For Uday Varadarajan / RMI
    'f1_elctrc_oper_rev',  # For Uday Varadarajan / RMI
    'f1_elctrc_erg_acct',  # For Uday Varadarajan / RMI
    'f1_elc_op_mnt_expn',  # For Uday Varadarajan / RMI
    'f1_slry_wg_dstrbtn',  # For Uday Varadarajan / RMI
    'f1_utltyplnt_smmry',  # For Uday Varadarajan / RMI
    'f1_399_mth_peak',  # For Uday Varadarajan / RMI
    'f1_398_ancl_ps',  # For Uday Varadarajan / RMI
    'f1_325_elc_cust',  # For Uday Varadarajan / RMI
    'f1_400_sys_peak',  # For Uday Varadarajan / RMI
    'f1_400a_iso_peak',  # For Uday Varadarajan / RMI
    'f1_397_isorto_stl',  # For Uday Varadarajan / RMI
)
"""tuple: A tuple containing the FERC Form 1 columns PUDL is initially focused
    on.
"""

# This is the set of tables which have been successfully integrated into PUDL:
ferc1_pudl_tables = ('fuel_ferc1',
                     'plants_steam_ferc1',
                     'plants_small_ferc1',
                     'plants_hydro_ferc1',
                     'plants_pumped_storage_ferc1',
                     'plant_in_service_ferc1',
                     'purchased_power_ferc1',
                     'accumulated_depreciation_ferc1')
"""tuple: A tuple containing the FERC Form 1 tables that can be successfully
    integrated into PUDL.
"""

table_map_ferc1_pudl = {'fuel_ferc1': 'f1_fuel',
                        'plants_steam_ferc1': 'f1_steam',
                        'plants_small_ferc1': 'f1_gnrt_plant',
                        'plants_hydro_ferc1': 'f1_hydro',
                        'plants_pumped_storage_ferc1': 'f1_pumped_storage',
                        'plant_in_service_ferc1': 'f1_plant_in_srvce',
                        'purchased_power_ferc1': 'f1_purchased_pwr',
                        'accumulated_depreciation_ferc1': 'f1_accumdepr_prvsn'}
"""dict: A dictionary mapping PUDL table names (keys) to the corresponding FERC
    Form 1 DBF table names.
"""

# This is the list of EIA923 tables that can be successfully pulled into PUDL
eia923_pudl_tables = ('generation_fuel_eia923',
                      'boiler_fuel_eia923',
                      'generation_eia923',
                      'coalmine_eia923',
                      'fuel_receipts_costs_eia923')
"""tuple: A tuple containing the EIA923 tables that can be successfully
    integrated into PUDL.
"""

epaipm_pudl_tables = (
    'transmission_single_epaipm',
    'transmission_joint_epaipm',
    'load_curves_epaipm',
    'plant_region_map_epaipm',
)
"""tuple: A tuple containing the EPA IPM tables that can be successfully
    integrated into PUDL.
"""

# List of entity tables
entity_tables = ['utilities_entity_eia',
                 'plants_entity_eia',
                 'generators_entity_eia',
                 'boilers_entity_eia',
                 'regions_entity_epaipm', ]
"""list: A list of PUDL entity tables.
"""

# 'stocks_eia923'
xlsx_maps_pkg = 'pudl.package_data.meta.xlsx_maps'
"""type?:

    Todo:
        Return to
"""

##############################################################################
# EIA 923 Spreadsheet Metadata
##############################################################################
tab_map_eia923 = pd.read_csv(
    importlib.resources.open_text(xlsx_maps_pkg, 'tab_map_eia923.csv'),
    index_col=0, comment='#')
"""pandas.DataFrame: A DataFrame of metadata from the EIA 923 tab map.
"""
skiprows_eia923 = pd.read_csv(
    importlib.resources.open_text(xlsx_maps_pkg, 'skiprows_eia923.csv'),
    index_col=0, comment='#')
"""pandas.DataFrame: A DataFrame of metadata from the EIA 923 skiprows map.
"""
generation_fuel_map_eia923 = pd.read_csv(
    importlib.resources.open_text(
        xlsx_maps_pkg, 'generation_fuel_map_eia923.csv'),
    index_col=0, comment='#')
"""pandas.DataFrame: A DataFrame of metadata from EIA 923 Generation Fuel.
"""
stocks_map_eia923 = pd.read_csv(
    importlib.resources.open_text(xlsx_maps_pkg, 'stocks_map_eia923.csv'),
    index_col=0, comment='#')
"""pandas.DataFrame: A DataFrame of metadata from EIA 923 Stocks.
"""
boiler_fuel_map_eia923 = pd.read_csv(
    importlib.resources.open_text(xlsx_maps_pkg, 'boiler_fuel_map_eia923.csv'),
    index_col=0, comment='#')
"""pandas.DataFrame: A DataFrame of metadata from EIA 923 Boiler Fuel.
"""
generator_map_eia923 = pd.read_csv(
    importlib.resources.open_text(xlsx_maps_pkg, 'generator_map_eia923.csv'),
    index_col=0, comment='#')
"""pandas.DataFrame: A DataFrame of metadata from EIA 923 Generators.
"""
fuel_receipts_costs_map_eia923 = pd.read_csv(
    importlib.resources.open_text(
        xlsx_maps_pkg, 'fuel_receipts_costs_map_eia923.csv'),
    index_col=0, comment='#')
"""pandas.DataFrame: A DataFrame of metadata from EIA 923 Fuel Receipts and
    Costs.
"""
plant_frame_map_eia923 = pd.read_csv(
    importlib.resources.open_text(xlsx_maps_pkg, 'plant_frame_map_eia923.csv'),
    index_col=0, comment='#')
"""pandas.DataFrame: A DataFrame of metadata from EIA 923 Plant Frame.
"""

# patterns for matching columns to months:
month_dict_eia923 = {1: '_january$',
                     2: '_february$',
                     3: '_march$',
                     4: '_april$',
                     5: '_may$',
                     6: '_june$',
                     7: '_july$',
                     8: '_august$',
                     9: '_september$',
                     10: '_october$',
                     11: '_november$',
                     12: '_december$'}
"""dict: A dictionary mapping column numbers (keys) to months (values).
"""

##############################################################################
# EIA 860 Spreadsheet Metadata
##############################################################################

# list of eia860 file names
files_eia860 = ('enviro_assn', 'utilities',
                'plants', 'generators', 'ownership')
"""tuple: A tuple containing EIA 860 file names.
"""

# file names to glob file pattern (used in get_eia860_file)
files_dict_eia860 = {'utilities': '*Utility*',
                     'plants': '*Plant*',
                     'generators': '*Generat*',
                     'wind': '*Wind*',
                     'solar': '*Solar*',
                     'multi_fuel': '*Multi*',
                     'ownership': '*Owner*',
                     'enviro_assn': '*EnviroAssoc*',
                     'envrio_equipment': '*EnviroEquip*'}
"""dict: A dictionary containing file names (keys) and file name patterns to
    glob (values) for EIA 860.
"""

# files to list of tabs
file_pages_eia860 = {'enviro_assn': ['boiler_generator_assn', ],
                     'utilities': ['utility', ],
                     'plants': ['plant', ],
                     'generators': ['generator_existing', 'generator_proposed',
                                    'generator_retired'],
                     'ownership': ['ownership', ]}
"""dict: A dictionary containing file names (keys) and lists of tab names to
    read (values) for EIA 860.
"""

# This is the list of EIA860 tables that can be successfully pulled into PUDL
eia860_pudl_tables = (
    'boiler_generator_assn_eia860',
    'utilities_eia860',
    'plants_eia860',
    'generators_eia860',
    'ownership_eia860'
)
"""tuple: A tuple containing the list of EIA 860 tables that can be successfully
    pulled into PUDL.
"""

tab_map_eia860 = pd.read_csv(
    importlib.resources.open_text(xlsx_maps_pkg, 'tab_map_eia860.csv'),
    index_col=0, comment='#')
"""pandas.DataFrame: A DataFrame of metadata from EIA 860 tab map.
"""

skiprows_eia860 = pd.read_csv(
    importlib.resources.open_text(xlsx_maps_pkg, 'skiprows_eia860.csv'),
    index_col=0, comment='#')
"""pandas.DataFrame: A DataFrame of metadata from EIA 860 skiprows map.
"""

boiler_generator_assn_map_eia860 = pd.read_csv(
    importlib.resources.open_text(
        xlsx_maps_pkg, 'boiler_generator_assn_map_eia860.csv'),
    index_col=0, comment='#')
"""pandas.DataFrame: A DataFrame of metadata from EIA 860 Boiler Generator
    Association.
"""

utility_assn_map_eia860 = pd.read_csv(
    importlib.resources.open_text(
        xlsx_maps_pkg, 'utility_assn_map_eia860.csv'),
    index_col=0, comment='#')
"""pandas.DataFrame: A DataFrame of metadata from EIA 860 Utility.
"""

ownership_assn_map_eia860 = pd.read_csv(
    importlib.resources.open_text(
        xlsx_maps_pkg, 'ownership_assn_map_eia860.csv'),
    index_col=0, comment='#')
"""pandas.DataFrame: A DataFrame of metadata from EIA 860 Ownership.
"""

plant_assn_map_eia860 = pd.read_csv(
    importlib.resources.open_text(xlsx_maps_pkg, 'plant_assn_map_eia860.csv'),
    index_col=0, comment='#')
"""pandas.DataFrame: A DataFrame of metadata from EIA 860 Plant.
"""

generator_assn_map_eia860 = pd.read_csv(
    importlib.resources.open_text(
        xlsx_maps_pkg, 'generator_assn_map_eia860.csv'),
    index_col=0, comment='#')
"""pandas.DataFrame: A DataFrame of metadata from EIA 860 Generator.
"""

generator_proposed_assn_map_eia860 = pd.read_csv(
    importlib.resources.open_text(
        xlsx_maps_pkg, 'generator_proposed_assn_map_eia860.csv'),
    index_col=0, comment='#')
"""pandas.DataFrame: A DataFrame of metadata from EIA 860 Generator Proposed.
"""

generator_retired_assn_map_eia860 = pd.read_csv(
    importlib.resources.open_text(
        xlsx_maps_pkg, 'generator_retired_assn_map_eia860.csv'),
    index_col=0, comment='#')
"""pandas.DataFrame: A DataFrame of metadata from EIA 860 Generator Retired.
"""

# The set of FERC Form 1 tables that have the same composite primary keys: [
# respondent_id, report_year, report_prd, row_number, spplmnt_num ].
# TODO: THIS ONLY PERTAINS TO 2015 AND MAY NEED TO BE ADJUSTED BY YEAR...
ferc1_data_tables = (
    'f1_acb_epda', 'f1_accumdepr_prvsn', 'f1_accumdfrrdtaxcr',
    'f1_adit_190_detail', 'f1_adit_190_notes', 'f1_adit_amrt_prop',
    'f1_adit_other', 'f1_adit_other_prop', 'f1_allowances', 'f1_bal_sheet_cr',
    'f1_capital_stock', 'f1_cash_flow', 'f1_cmmn_utlty_p_e',
    'f1_comp_balance_db', 'f1_construction', 'f1_control_respdnt',
    'f1_co_directors', 'f1_cptl_stk_expns', 'f1_csscslc_pcsircs',
    'f1_dacs_epda', 'f1_dscnt_cptl_stk', 'f1_edcfu_epda', 'f1_elctrc_erg_acct',
    'f1_elctrc_oper_rev', 'f1_elc_oper_rev_nb', 'f1_elc_op_mnt_expn',
    'f1_electric', 'f1_envrnmntl_expns', 'f1_envrnmntl_fclty', 'f1_fuel',
    'f1_general_info', 'f1_gnrt_plant', 'f1_important_chg', 'f1_incm_stmnt_2',
    'f1_income_stmnt', 'f1_miscgen_expnelc', 'f1_misc_dfrrd_dr',
    'f1_mthly_peak_otpt', 'f1_mtrl_spply', 'f1_nbr_elc_deptemp',
    'f1_nonutility_prop', 'f1_note_fin_stmnt', 'f1_nuclear_fuel',
    'f1_officers_co', 'f1_othr_dfrrd_cr', 'f1_othr_pd_in_cptl',
    'f1_othr_reg_assets', 'f1_othr_reg_liab', 'f1_overhead', 'f1_pccidica',
    'f1_plant_in_srvce', 'f1_pumped_storage', 'f1_purchased_pwr',
    'f1_reconrpt_netinc', 'f1_reg_comm_expn', 'f1_respdnt_control',
    'f1_retained_erng', 'f1_r_d_demo_actvty', 'f1_sales_by_sched',
    'f1_sale_for_resale', 'f1_sbsdry_totals', 'f1_schedules_list',
    'f1_security_holder', 'f1_slry_wg_dstrbtn', 'f1_substations',
    'f1_taxacc_ppchrgyr', 'f1_unrcvrd_cost', 'f1_utltyplnt_smmry', 'f1_work',
    'f1_xmssn_adds', 'f1_xmssn_elc_bothr', 'f1_xmssn_elc_fothr',
    'f1_xmssn_line', 'f1_xtraordnry_loss',
    'f1_hydro', 'f1_steam', 'f1_leased', 'f1_sbsdry_detail',
    'f1_plant', 'f1_long_term_debt', 'f1_106_2009', 'f1_106a_2009',
    'f1_106b_2009', 'f1_208_elc_dep', 'f1_231_trn_stdycst', 'f1_324_elc_expns',
    'f1_325_elc_cust', 'f1_331_transiso', 'f1_338_dep_depl',
    'f1_397_isorto_stl', 'f1_398_ancl_ps', 'f1_399_mth_peak',
    'f1_400_sys_peak', 'f1_400a_iso_peak', 'f1_429_trans_aff',
    'f1_allowances_nox', 'f1_cmpinc_hedge_a', 'f1_cmpinc_hedge',
    'f1_rg_trn_srv_rev')
"""tuple: A tuple containing the FERC Form 1 tables that have the same composite
    primary keys: [respondent_id, report_year, report_prd, row_number,
    spplmnt_num ].
"""
# Line numbers, and corresponding FERC account number
# from FERC Form 1 pages 204-207, Electric Plant in Service.
# Descriptions from: https://www.law.cornell.edu/cfr/text/18/part-101
ferc_electric_plant_accounts = pd.DataFrame.from_records([
    # 1. Intangible Plant
    (2, '301', 'Intangible: Organization'),
    (3, '302', 'Intangible: Franchises and consents'),
    (4, '303', 'Intangible: Miscellaneous intangible plant'),
    (5, 'subtotal_intangible', 'Subtotal: Intangible Plant'),
    # 2. Production Plant
    #  A. steam production
    (8, '310', 'Steam production: Land and land rights'),
    (9, '311', 'Steam production: Structures and improvements'),
    (10, '312', 'Steam production: Boiler plant equipment'),
    (11, '313', 'Steam production: Engines and engine-driven generators'),
    (12, '314', 'Steam production: Turbogenerator units'),
    (13, '315', 'Steam production: Accessory electric equipment'),
    (14, '316', 'Steam production: Miscellaneous power plant equipment'),
    (15, '317', 'Steam production: Asset retirement costs for steam production\
                                   plant'),
    (16, 'subtotal_steam_production', 'Subtotal: Steam Production Plant'),
    #  B. nuclear production
    (18, '320', 'Nuclear production: Land and land rights (Major only)'),
    (19, '321', 'Nuclear production: Structures and improvements (Major\
                                     only)'),
    (20, '322', 'Nuclear production: Reactor plant equipment (Major only)'),
    (21, '323', 'Nuclear production: Turbogenerator units (Major only)'),
    (22, '324', 'Nuclear production: Accessory electric equipment (Major\
                                     only)'),
    (23, '325', 'Nuclear production: Miscellaneous power plant equipment\
                                     (Major only)'),
    (24, '326', 'Nuclear production: Asset retirement costs for nuclear\
                                     production plant (Major only)'),
    (25, 'subtotal_nuclear_produciton', 'Subtotal: Nuclear Production Plant'),
    #  C. hydraulic production
    (27, '330', 'Hydraulic production: Land and land rights'),
    (28, '331', 'Hydraulic production: Structures and improvements'),
    (29, '332', 'Hydraulic production: Reservoirs, dams, and waterways'),
    (30, '333', 'Hydraulic production: Water wheels, turbines and generators'),
    (31, '334', 'Hydraulic production: Accessory electric equipment'),
    (32, '335', 'Hydraulic production: Miscellaneous power plant equipment'),
    (33, '336', 'Hydraulic production: Roads, railroads and bridges'),
    (34, '337', 'Hydraulic production: Asset retirement costs for hydraulic\
                                       production plant'),
    (35, 'subtotal_hydraulic_production', 'Subtotal: Hydraulic Production\
                                                     Plant'),
    #  D. other production
    (37, '340', 'Other production: Land and land rights'),
    (38, '341', 'Other production: Structures and improvements'),
    (39, '342', 'Other production: Fuel holders, producers, and accessories'),
    (40, '343', 'Other production: Prime movers'),
    (41, '344', 'Other production: Generators'),
    (42, '345', 'Other production: Accessory electric equipment'),
    (43, '346', 'Other production: Miscellaneous power plant equipment'),
    (44, '347', 'Other production: Asset retirement costs for other production\
                                   plant'),
    (None, '348', 'Other production: Energy Storage Equipment'),
    (45, 'subtotal_other_production', 'Subtotal: Other Production Plant'),
    (46, 'subtotal_production', 'Subtotal: Production Plant'),
    # 3. Transmission Plant,
    (48, '350', 'Transmission: Land and land rights'),
    (None, '351', 'Transmission: Energy Storage Equipment'),
    (49, '352', 'Transmission: Structures and improvements'),
    (50, '353', 'Transmission: Station equipment'),
    (51, '354', 'Transmission: Towers and fixtures'),
    (52, '355', 'Transmission: Poles and fixtures'),
    (53, '356', 'Transmission: Overhead conductors and devices'),
    (54, '357', 'Transmission: Underground conduit'),
    (55, '358', 'Transmission: Underground conductors and devices'),
    (56, '359', 'Transmission: Roads and trails'),
    (57, '359.1', 'Transmission: Asset retirement costs for transmission\
                                 plant'),
    (58, 'subtotal_transmission', 'Subtotal: Transmission Plant'),
    # 4. Distribution Plant
    (60, '360', 'Distribution: Land and land rights'),
    (61, '361', 'Distribution: Structures and improvements'),
    (62, '362', 'Distribution: Station equipment'),
    (63, '363', 'Distribution: Storage battery equipment'),
    (64, '364', 'Distribution: Poles, towers and fixtures'),
    (65, '365', 'Distribution: Overhead conductors and devices'),
    (66, '366', 'Distribution: Underground conduit'),
    (67, '367', 'Distribution: Underground conductors and devices'),
    (68, '368', 'Distribution: Line transformers'),
    (69, '369', 'Distribution: Services'),
    (70, '370', 'Distribution: Meters'),
    (71, '371', 'Distribution: Installations on customers\' premises'),
    (72, '372', 'Distribution: Leased property on customers\' premises'),
    (73, '373', 'Distribution: Street lighting and signal systems'),
    (74, '374', 'Distribution: Asset retirement costs for distribution plant'),
    (75, 'subtotal_distribution', 'Subtotal: Distribution Plant'),
    # 5. Regional Transmission and Market Operation Plant
    (77, '380', 'Regional transmission: Land and land rights'),
    (78, '381', 'Regional transmission: Structures and improvements'),
    (79, '382', 'Regional transmission: Computer hardware'),
    (80, '383', 'Regional transmission: Computer software'),
    (81, '384', 'Regional transmission: Communication Equipment'),
    (82, '385', 'Regional transmission: Miscellaneous Regional Transmission\
                                        and Market Operation Plant'),
    (83, '386', 'Regional transmission: Asset Retirement Costs for Regional\
                                        Transmission and Market Operation\
                                        Plant'),
    (84, 'subtotal_regional_transmission', 'Subtotal: Transmission and Market\
                                                      Operation Plant'),
    (None, '387', 'Regional transmission: [Reserved]'),
    # 6. General Plant
    (86, '389', 'General: Land and land rights'),
    (87, '390', 'General: Structures and improvements'),
    (88, '391', 'General: Office furniture and equipment'),
    (89, '392', 'General: Transportation equipment'),
    (90, '393', 'General: Stores equipment'),
    (91, '394', 'General: Tools, shop and garage equipment'),
    (92, '395', 'General: Laboratory equipment'),
    (93, '396', 'General: Power operated equipment'),
    (94, '397', 'General: Communication equipment'),
    (95, '398', 'General: Miscellaneous equipment'),
    (96, 'subtotal_general', 'Subtotal: General Plant'),
    (97, '399', 'General: Other tangible property'),
    (98, '399.1', 'General: Asset retirement costs for general plant'),
    (99, 'total_general', 'TOTAL General Plant'),
    (100, '101_and_106', 'Electric plant in service (Major only)'),
    (101, '102_purchased', 'Electric plant purchased'),
    (102, '102_sold', 'Electric plant sold'),
    (103, '103', 'Experimental plant unclassified'),
    (104, 'total_electric_plant', 'TOTAL Electric Plant in Service')],
    columns=['row_number', 'ferc_account_id', 'ferc_account_description'])
"""list: A list of tuples containing row numbers, FERC account IDs, and FERC
    account descriptions from FERC Form 1 pages 204-207, Electric Plant in
    Service.
"""

# Line numbers, and corresponding FERC account number
# from FERC Form 1 page 219, ACCUMULATED PROVISION FOR DEPRECIATION
# OF ELECTRIC UTILITY PLANT (Account 108).

ferc_accumulated_depreciation = pd.DataFrame.from_records([

    # Section A. Balances and Changes During Year
    (1, 'balance_beginning_of_year', 'Balance Beginning of Year'),
    (3, 'depreciation_expense', '(403) Depreciation Expense'),
    (4, 'depreciation_expense_asset_retirement', \
     '(403.1) Depreciation Expense for Asset Retirement Costs'),
    (5, 'expense_electric_plant_leased_to_others', \
     '(413) Exp. of Elec. Plt. Leas. to Others'),
    (6, 'transportation_expenses_clearing',\
     'Transportation Expenses-Clearing'),
    (7, 'other_clearing_accounts', 'Other Clearing Accounts'),
    (8, 'other_accounts_specified',\
     'Other Accounts (Specify, details in footnote):'),
    # blank: might also be other charges like line 17.
    (9, 'other_charges', 'Other Charges:'),
    (10, 'total_depreciation_provision_for_year',\
     'TOTAL Deprec. Prov for Year (Enter Total of lines 3 thru 9)'),
    (11, 'net_charges_for_plant_retired', 'Net Charges for Plant Retired:'),
    (12, 'book_cost_of_plant_retired', 'Book Cost of Plant Retired'),
    (13, 'cost_of_removal', 'Cost of Removal'),
    (14, 'salvage_credit', 'Salvage (Credit)'),
    (15, 'total_net_charges_for_plant_retired',\
     'TOTAL Net Chrgs. for Plant Ret. (Enter Total of lines 12 thru 14)'),
    (16, 'other_debit_or_credit_items',\
     'Other Debit or Cr. Items (Describe, details in footnote):'),
    # blank: can be "Other Charges", e.g. in 2012 for PSCo.
    (17, 'other_charges_2', 'Other Charges 2'),
    (18, 'book_cost_or_asset_retirement_costs_retired',\
     'Book Cost or Asset Retirement Costs Retired'),
    (19, 'balance_end_of_year', \
     'Balance End of Year (Enter Totals of lines 1, 10, 15, 16, and 18)'),
    # Section B. Balances at End of Year According to Functional Classification
    (20, 'steam_production_end_of_year', 'Steam Production'),
    (21, 'nuclear_production_end_of_year', 'Nuclear Production'),
    (22, 'hydraulic_production_end_of_year',\
     'Hydraulic Production-Conventional'),
    (23, 'pumped_storage_end_of_year', 'Hydraulic Production-Pumped Storage'),
    (24, 'other_production', 'Other Production'),
    (25, 'transmission', 'Transmission'),
    (26, 'distribution', 'Distribution'),
    (27, 'regional_transmission_and_market_operation',
     'Regional Transmission and Market Operation'),
    (28, 'general', 'General'),
    (29, 'total', 'TOTAL (Enter Total of lines 20 thru 28)')],

    columns=['row_number', 'line_id', 'ferc_account_description'])
"""list: A list of tuples containing row numbers, FERC account IDs, and FERC
    account descriptions from FERC Form 1 page 219, Accumulated Provision for
    Depreciation of electric utility plant (Account 108).
"""
######################################################################
# Constants from EIA From 923 used within init.py module
######################################################################

# From Page 7 of EIA Form 923, Census Region a US state is located in
census_region = {
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
"""dict: A dictionary mapping Census Region abbreviations (keys) to Census
    Region names (values).
"""

# From Page 7 of EIA Form923
# Static list of NERC (North American Electric Reliability Corporation)
# regions, used for where plant is located
nerc_region = {
    'NPCC': 'Northeast Power Coordinating Council',
    'ASCC': 'Alaska Systems Coordinating Council',
    'HICC': 'Hawaiian Islands Coordinating Council',
    'MRO': 'Midwest Reliability Organization',
    'SERC': 'SERC Reliability Corporation',
    'RFC': 'Reliability First Corporation',
    'SPP': 'Southwest Power Pool',
    'TRE': 'Texas Regional Entity',
    'FRCC': 'Florida Reliability Coordinating Council',
    'WECC': 'Western Electricity Coordinating Council'
}
"""dict: A dictionary mapping NERC Region abbreviations (keys) to NERC
    Region names (values).
"""

# From Page 7 of EIA Form 923 EIA’s internal consolidated NAICS sectors.
# For internal purposes, EIA consolidates NAICS categories into seven groups.
sector_eia = {
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
"""dict: A dictionary mapping EIA numeric codes (keys) to EIA’s internal
    consolidated NAICS sectors (values).
"""

# EIA 923: EIA Type of prime mover:
prime_movers_eia923 = {
    'BA': 'Energy Storage, Battery',
    'BT': 'Turbines Used in a Binary Cycle. Including those used for geothermal applications',
    'CA': 'Combined-Cycle -- Steam Part',
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
    'WS': 'Wind Turbine, Offshore'
}
"""dict: A dictionary mapping EIA 923 prime mover codes (keys) and prime mover
    names/descriptions (values).
"""

# EIA 923: The fuel code reported to EIA.Two or three letter alphanumeric:
fuel_type_eia923 = {
    'AB': 'Agricultural By-Products',
    'ANT': 'Anthracite Coal',
    'BFG': 'Blast Furnace Gas',
    'BIT': 'Bituminous Coal',
    'BLQ': 'Black Liquor',
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
"""dict: A dictionary mapping EIA 923 fuel type codes (keys) and fuel type
    names/descriptions (values).
"""

# Fuel type strings for EIA 923 generator fuel table

fuel_type_eia923_gen_fuel_coal_strings = [
    'ant', 'bit', 'cbl', 'lig', 'pc', 'rc', 'sc', 'sub', 'wc', ]
"""list: The list of EIA 923 Generation Fuel strings associated with coal fuel.
"""

fuel_type_eia923_gen_fuel_oil_strings = [
    'dfo', 'rfo', 'wo', 'jf', 'ker', ]
"""list: The list of EIA 923 Generation Fuel strings associated with oil fuel.
"""

fuel_type_eia923_gen_fuel_gas_strings = [
    'bfg', 'lfg', 'ng', 'og', 'obg', 'pg', 'sgc', 'sgp', ]
"""list: The list of EIA 923 Generation Fuel strings associated with gas fuel.
"""

fuel_type_eia923_gen_fuel_solar_strings = ['sun', ]
"""list: The list of EIA 923 Generation Fuel strings associated with solar
    power.
"""

fuel_type_eia923_gen_fuel_wind_strings = ['wnd', ]
"""list: The list of EIA 923 Generation Fuel strings associated with wind
    power.
"""
fuel_type_eia923_gen_fuel_hydro_strings = ['wat', ]
"""list: The list of EIA 923 Generation Fuel strings associated with hydro
    power.
"""
fuel_type_eia923_gen_fuel_nuclear_strings = ['nuc', ]
"""list: The list of EIA 923 Generation Fuel strings associated with nuclear
    power.
"""
fuel_type_eia923_gen_fuel_waste_strings = [
    'ab', 'blq', 'msb', 'msn', 'msw', 'obl', 'obs', 'slw', 'tdf', 'wdl', 'wds']
"""list: The list of EIA 923 Generation Fuel strings associated with solid waste
    fuel.
"""
fuel_type_eia923_gen_fuel_other_strings = ['geo', 'mwh', 'oth', 'pur', 'wh', ]
"""list: The list of EIA 923 Generation Fuel strings associated with geothermal
    power.
"""


fuel_type_eia923_gen_fuel_simple_map = {
    'coal': fuel_type_eia923_gen_fuel_coal_strings,
    'oil': fuel_type_eia923_gen_fuel_oil_strings,
    'gas': fuel_type_eia923_gen_fuel_gas_strings,
    'solar': fuel_type_eia923_gen_fuel_solar_strings,
    'wind': fuel_type_eia923_gen_fuel_wind_strings,
    'hydro': fuel_type_eia923_gen_fuel_hydro_strings,
    'nuclear': fuel_type_eia923_gen_fuel_nuclear_strings,
    'waste': fuel_type_eia923_gen_fuel_waste_strings,
    'other': fuel_type_eia923_gen_fuel_other_strings,
}
"""dict: A dictionary mapping EIA 923 Generation Fuel fuel types (keys) to lists
    of strings associated with that fuel type (values).
"""

# Fuel type strings for EIA 923 boiler fuel table

fuel_type_eia923_boiler_fuel_coal_strings = [
    'ant', 'bit', 'lig', 'pc', 'rc', 'sc', 'sub', 'wc', ]
"""list: A list of strings from EIA 923 Boiler Fuel associated with fuel type
    coal.
"""

fuel_type_eia923_boiler_fuel_oil_strings = ['dfo', 'rfo', 'wo', 'jf', 'ker', ]
"""list: A list of strings from EIA 923 Boiler Fuel associated with fuel type
    oil.
"""
fuel_type_eia923_boiler_fuel_gas_strings = [
    'bfg', 'lfg', 'ng', 'og', 'obg', 'pg', 'sgc', 'sgp', ]
"""list: A list of strings from EIA 923 Boiler Fuel associated with fuel type
    gas.
"""
fuel_type_eia923_boiler_fuel_waste_strings = [
    'ab', 'blq', 'msb', 'msn', 'obl', 'obs', 'slw', 'tdf', 'wdl', 'wds', ]
"""list: A list of strings from EIA 923 Boiler Fuel associated with fuel type
    waste.
"""
fuel_type_eia923_boiler_fuel_other_strings = ['oth', 'pur', 'wh', ]
"""list: A list of strings from EIA 923 Boiler Fuel associated with fuel type
    other.
"""

fuel_type_eia923_boiler_fuel_simple_map = {
    'coal': fuel_type_eia923_boiler_fuel_coal_strings,
    'oil': fuel_type_eia923_boiler_fuel_oil_strings,
    'gas': fuel_type_eia923_boiler_fuel_gas_strings,
    'waste': fuel_type_eia923_boiler_fuel_waste_strings,
    'other': fuel_type_eia923_boiler_fuel_other_strings,
}
"""dict: A dictionary mapping EIA 923 Boiler Fuel fuel types (keys) to lists
    of strings associated with that fuel type (values).
"""

# PUDL consolidation of EIA923 AER fuel type strings into same categories as
# 'energy_source_eia923' plus additional renewable and nuclear categories.
# These classifications are not currently used, as the EIA fuel type and energy
# source designations provide more detailed information.

aer_coal_strings = ['col', 'woc', 'pc']
"""list: A list of EIA 923 AER fuel type strings associated with coal.
"""

aer_gas_strings = ['mlg', 'ng', 'oog']
"""list: A list of EIA 923 AER fuel type strings associated with gas.
"""

aer_oil_strings = ['dfo', 'rfo', 'woo']
"""list: A list of EIA 923 AER fuel type strings associated with oil.
"""

aer_solar_strings = ['sun']
"""list: A list of EIA 923 AER fuel type strings associated with solar power.
"""

aer_wind_strings = ['wnd']
"""list: A list of EIA 923 AER fuel type strings associated with wind power.
"""

aer_hydro_strings = ['hps', 'hyc']
"""list: A list of EIA 923 AER fuel type strings associated with hydro power.
"""

aer_nuclear_strings = ['nuc']
"""list:  A list of EIA 923 AER fuel type strings associated with nuclear power.
"""

aer_waste_strings = ['www']
"""list: A list of EIA 923 AER fuel type strings associated with waste.
"""

aer_other_strings = ['geo', 'orw', 'oth']
"""list: A list of EIA 923 AER fuel type strings associated with other fuel.
"""

aer_fuel_type_strings = {
    'coal': aer_coal_strings,
    'gas': aer_gas_strings,
    'oil': aer_oil_strings,
    'solar': aer_solar_strings,
    'wind': aer_wind_strings,
    'hydro': aer_hydro_strings,
    'nuclear': aer_nuclear_strings,
    'waste': aer_waste_strings,
    'other': aer_other_strings
}
"""dict: A dictionary mapping EIA 923 AER fuel types (keys) to lists
    of strings associated with that fuel type (values).
"""


# EIA 923: A partial aggregation of the reported fuel type codes into
# larger categories used by EIA in, for example,
# the Annual Energy Review (AER).Two or three letter alphanumeric.
# See the Fuel Code table (Table 5), below:
fuel_type_aer_eia923 = {
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
"""dict: A dictionary mapping EIA 923 AER fuel types (keys) to lists
    of strings associated with that fuel type (values).
"""

fuel_type_eia860_coal_strings = ['ant', 'bit', 'cbl', 'lig', 'pc', 'rc', 'sc',
                                 'sub', 'wc', 'coal', 'petroleum coke', 'col',
                                 'woc']
"""list: A list of strings from EIA 860 associated with fuel type coal.
"""

fuel_type_eia860_oil_strings = ['dfo', 'jf', 'ker', 'rfo', 'wo', 'woo',
                                'petroleum']
"""list: A list of strings from EIA 860 associated with fuel type oil.
"""

fuel_type_eia860_gas_strings = ['bfg', 'lfg', 'mlg', 'ng', 'obg', 'og', 'pg',
                                'sgc', 'sgp', 'natural gas', 'other gas',
                                'oog', 'sg']
"""list: A list of strings from EIA 860 associated with fuel type gas.
"""

fuel_type_eia860_solar_strings = ['sun', 'solar']
"""list: A list of strings from EIA 860 associated with solar power.
"""

fuel_type_eia860_wind_strings = ['wnd', 'wind', 'wt']
"""list: A list of strings from EIA 860 associated with wind power.
"""

fuel_type_eia860_hydro_strings = ['wat', 'hyc', 'hps', 'hydro']
"""list: A list of strings from EIA 860 associated with hydro power.
"""

fuel_type_eia860_nuclear_strings = ['nuc', 'nuclear']
"""list:  A list of strings from EIA 860 associated with nuclear power.
"""

fuel_type_eia860_waste_strings = ['ab', 'blq', 'bm', 'msb', 'msn', 'obl',
                                  'obs', 'slw', 'tdf', 'wdl', 'wds', 'biomass',
                                  'msw', 'www']
"""list: A list of strings from EIA 860 associated with fuel type waste.
"""

fuel_type_eia860_other_strings = ['mwh', 'oth', 'pur', 'wh', 'geo', 'none',
                                  'orw', 'other']
"""list:  A list of strings from EIA 860 associated with fuel type other.
"""

fuel_type_eia860_simple_map = {
    'coal': fuel_type_eia860_coal_strings,
    'oil': fuel_type_eia860_oil_strings,
    'gas': fuel_type_eia860_gas_strings,
    'solar': fuel_type_eia860_solar_strings,
    'wind': fuel_type_eia860_wind_strings,
    'hydro': fuel_type_eia860_hydro_strings,
    'nuclear': fuel_type_eia860_nuclear_strings,
    'waste': fuel_type_eia860_waste_strings,
    'other': fuel_type_eia860_other_strings,
}
"""dict: A dictionary mapping EIA 860 fuel types (keys) to lists
    of strings associated with that fuel type (values).
"""

# EIA 923/860: Lumping of energy source categories.
energy_source_eia_simple_map = {
    'coal': ['ANT', 'BIT', 'LIG', 'PC', 'SUB', 'WC', 'RC'],
    'oil': ['DFO', 'JF', 'KER', 'RFO', 'WO'],
    'gas': ['BFG', 'LFG', 'NG', 'OBG', 'OG', 'PG', 'SG', 'SGC', 'SGP'],
    'solar': ['SUN'],
    'wind': ['WND'],
    'hydro': ['WAT'],
    'nuclear': ['NUC'],
    'waste': ['AB', 'BLQ', 'MSW', 'OBL', 'OBS', 'SLW', 'TDF', 'WDL', 'WDS'],
    'other': ['GEO', 'MWH', 'OTH', 'PUR', 'WH']
}
"""dict: A dictionary mapping EIA fuel types (keys) to fuel codes (values).
"""

fuel_group_eia923_simple_map = {
    'coal': ['coal', 'petroleum coke'],
    'oil': ['petroleum'],
    'gas': ['natural gas', 'other gas']
}
"""dict: A dictionary mapping EIA 923 simple fuel types ("oil", "coal", "gas")
    (keys) to fuel types (values).
"""

# EIA 923: The type of physical units fuel consumption is reported in.
# All consumption is reported in either short tons for solids,
# thousands of cubic feet for gases, and barrels for liquids.
fuel_units_eia923 = {
    'mcf': 'Thousands of cubic feet (for gases)',
    'short_tons': 'Short tons (for solids)',
    'barrels': 'Barrels (for liquids)'
}
"""dict: A dictionary mapping EIA 923 fuel units (keys) to fuel unit
    descriptions (values).
"""

# EIA 923: Designates the purchase type under which receipts occurred
# in the reporting month. One or two character alphanumeric:
contract_type_eia923 = {
    'C': 'Contract - Fuel received under a purchase order or contract with a term of one year or longer.  Contracts with a shorter term are considered spot purchases ',
    'NC': 'New Contract - Fuel received under a purchase order or contract with duration of one year or longer, under which deliveries were first made during the reporting month',
    'S': 'Spot Purchase',
    'T': 'Tolling Agreement – Fuel received under a tolling agreement (bartering arrangement of fuel for generation)'
}
"""dict: A dictionary mapping EIA 923 contract codes (keys) to contract
    descriptions (values) for each month in the Fuel Receipts and Costs table.
"""

# EIA 923: The fuel code associated with the fuel receipt.
# Defined on Page 7 of EIA Form 923
# Two or three character alphanumeric:
energy_source_eia923 = {
    'ANT': 'Anthracite Coal',
    'BFG': 'Blast Furnace Gas',
    'BM': 'Biomass',
    'BIT': 'Bituminous Coal',
    'DFO': 'Distillate Fuel Oil. Including diesel, No. 1, No. 2, and No. 4 fuel oils.',
    'JF': 'Jet Fuel',
    'KER': 'Kerosene',
    'LIG': 'Lignite Coal',
    'NG': 'Natural Gas',
    'PC': 'Petroleum Coke',
    'PG': 'Gaseous Propone',
    'OG': 'Other Gas',
    'RC': 'Refined Coal',
    'RFO': 'Residual Fuel Oil. Including No. 5 & 6 fuel oils and bunker C fuel oil.',
    'SG': 'Synhtesis Gas from Petroleum Coke',
    'SGP': 'Petroleum Coke Derived Synthesis Gas',
    'SC': 'Coal-based Synfuel. Including briquettes, pellets, or extrusions, which are formed by binding materials or processes that recycle materials.',
    'SUB': 'Subbituminous Coal',
    'WC': 'Waste/Other Coal. Including anthracite culm, bituminous gob, fine coal, lignite waste, waste coal.',
    'WO': 'Waste/Other Oil. Including crude oil, liquid butane, liquid propane, naphtha, oil waste, re-refined moto oil, sludge oil, tar oil, or other petroleum-based liquid wastes.',
}
"""dict: A dictionary mapping fuel codes (keys) to fuel descriptions (values)
    for each fuel receipt from the EIA 923 Fuel Receipts and Costs table.
"""

# EIA 923 Fuel Group, from Page 7 EIA Form 923
# Groups fossil fuel energy sources into fuel groups that are located in the
# Electric Power Monthly:  Coal, Natural Gas, Petroleum, Petroleum Coke.
fuel_group_eia923 = (
    'coal',
    'natural_gas',
    'petroleum',
    'petroleum_coke',
    'other_gas'
)
"""tuple: A tuple containing EIA 923 fuel groups.
"""

# EIA 923: Type of Coal Mine as defined on Page 7 of EIA Form 923
coalmine_type_eia923 = {
    'P': 'Preparation Plant',
    'S': 'Surface',
    'U': 'Underground',
    'US': 'Both an underground and surface mine with most coal extracted from underground',
    'SU': 'Both an underground and surface mine with most coal extracted from surface',
}
"""dict: A dictionary mapping EIA 923 coal mine type codes (keys) to
    descriptions (values).
"""

# EIA 923: State abbreviation related to coal mine location.
# Country abbreviations are also used in this category, but they are
# non-standard because of collisions with US state names. Instead of using
# the provided non-standard names, we convert to ISO-3166-1 three letter
# country codes https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3
coalmine_country_eia923 = {
    'AU': 'AUS',  # Australia
    'CL': 'COL',  # Colombia
    'CN': 'CAN',  # Canada
    'IS': 'IDN',  # Indonesia
    'PL': 'POL',  # Poland
    'RS': 'RUS',  # Russia
    'UK': 'GBR',  # United Kingdom of Great Britain
    'VZ': 'VEN',  # Venezuela
    'OC': 'other_country',
    'IM': 'unknown'
}
"""dict: A dictionary mapping coal mine country codes (keys) to ISO-3166-1 three
    letter country codes (values).
"""

# EIA 923: Mode for the longest / second longest distance.
transport_modes_eia923 = {
    'RR': 'Rail: Shipments of fuel moved to consumers by rail \
        (private or public/commercial). Included is coal hauled to or \
        away from a railroad siding by truck if the truck did not use public\
        roads.',
    'RV': 'River:  Shipments of fuel moved to consumers via river by barge.  \
        Not included are shipments to Great Lakes coal loading docks, \
        tidewater piers, or coastal ports.',
    'GL': 'Great Lakes:  Shipments of coal moved to consumers via \
        the Great Lakes. These shipments are moved via the Great Lakes \
        coal loading docks, which are identified by name and location as \
        follows: Conneaut Coal Storage & Transfer, Conneaut, Ohio;  \
        NS Coal Dock (Ashtabula Coal Dock), Ashtabula, Ohio;  \
        Sandusky Coal Pier, Sandusky, Ohio;  Toledo Docks, Toledo, Ohio; \
        KCBX Terminals Inc., Chicago, Illinois;  \
        Superior Midwest Energy Terminal, Superior, Wisconsin',
    'TP': 'Tidewater Piers and Coastal Ports:  Shipments of coal moved to \
        Tidewater Piers and Coastal Ports for further shipments to consumers \
        via coastal water or ocean.  The Tidewater Piers and Coastal Ports \
        are identified by name and location as follows:  Dominion Terminal \
        Associates, Newport News, Virginia; McDuffie Coal Terminal, Mobile, \
        Alabama; IC Railmarine Terminal, Convent, Louisiana;  \
        International Marine Terminals, Myrtle Grove, Louisiana;  \
        Cooper/T. Smith Stevedoring Co. Inc., Darrow, Louisiana;  \
        Seward Terminal Inc., Seward, Alaska;  Los Angeles Export Terminal, \
        Inc., Los Angeles, California;  Levin-Richmond Terminal Corp., \
        Richmond, California; Baltimore Terminal, Baltimore, Maryland;  \
        Norfolk Southern Lamberts Point P-6, Norfolk, Virginia;  \
        Chesapeake Bay Piers, Baltimore, Maryland;  Pier IX Terminal Company, \
        Newport News, Virginia;  Electro-Coal Transport Corp., Davant, \
        Louisiana',
    'WT': 'Water: Shipments of fuel moved to consumers by other waterways.',
    'TR': 'Truck: Shipments of fuel moved to consumers by truck.  \
        Not included is fuel hauled to or away from a railroad siding by \
        truck on non-public roads.',
    'tr': 'Truck: Shipments of fuel moved to consumers by truck.  \
        Not included is fuel hauled to or away from a railroad siding by \
        truck on non-public roads.',
    'TC': 'Tramway/Conveyor: Shipments of fuel moved to consumers \
        by tramway or conveyor.',
    'SP': 'Slurry Pipeline: Shipments of coal moved to consumers \
        by slurry pipeline.',
    'PL': 'Pipeline: Shipments of fuel moved to consumers by pipeline'
}
"""dict: A dictionary mapping primary and secondary transportation mode codes
    (keys) to descriptions (values).
"""

# we need to include all of the columns which we want to keep for either the
# entity or annual tables. The order here matters. We need to harvest the plant
# location before harvesting the location of the utilites for example.
entities = {
    'plants': [  # base cols
        ['plant_id_eia'],
        # static cols
        ['balancing_authority_code', 'balancing_authority_name',
         'city', 'county', 'ferc_cogen_status',
         'ferc_exempt_wholesale_generator', 'ferc_small_power_producer',
         'grid_voltage_2_kv', 'grid_voltage_3_kv', 'grid_voltage_kv',
         'iso_rto_code', 'iso_rto_name', 'latitude', 'longitude',
         'nerc_region', 'plant_name', 'primary_purpose_naics_id', 'sector_id',
         'sector_name', 'state', 'street_address', 'zip_code'],
        # annual cols
        ['ash_impoundment', 'ash_impoundment_lined', 'ash_impoundment_status',
         'energy_storage', 'ferc_cogen_docket_no', 'water_source',
         'ferc_exempt_wholesale_generator_docket_no',
         'ferc_small_power_producer_docket_no',
         'liquefied_natural_gas_storage',
         'natural_gas_local_distribution_company', 'natural_gas_storage',
         'natural_gas_pipeline_name_1', 'natural_gas_pipeline_name_2',
         'natural_gas_pipeline_name_3', 'net_metering', 'pipeline_notes',
         'regulatory_status_code', 'transmission_distribution_owner_id',
         'transmission_distribution_owner_name',
         'transmission_distribution_owner_state', 'utility_id_eia'],
        # need type fixing
        {'plant_id_eia': 'int64',
         'grid_voltage_2_kv': 'float64',
         'grid_voltage_3_kv': 'float64',
         'grid_voltage_kv': 'float64',
         'longitude': 'float64',
         'latitude': 'float64',
         'primary_purpose_naics_id': 'float64',
         'sector_id': 'float64',
         'zip_code': 'float64',
         'utility_id_eia': 'float64'}, ],
    'generators': [  # base cols
        ['plant_id_eia', 'generator_id'],
        # static cols
        ['prime_mover_code', 'duct_burners', 'operating_date',
         'topping_bottoming_code', 'solid_fuel_gasification',
         'pulverized_coal_tech', 'fluidized_bed_tech', 'subcritical_tech',
         'supercritical_tech', 'ultrasupercritical_tech', 'stoker_tech',
         'other_combustion_tech', 'heat_bypass_recovery',
         'rto_iso_lmp_node_id', 'rto_iso_location_wholesale_reporting_id',
         'associated_combined_heat_power', 'original_planned_operating_date',
         'operating_switch', 'previously_canceled'],
        # annual cols
        ['capacity_mw', 'fuel_type_code_pudl', 'multiple_fuels',
         'ownership_code', 'deliver_power_transgrid', 'summer_capacity_mw',
         'winter_capacity_mw', 'minimum_load_mw', 'technology_description',
         'energy_source_code_1', 'energy_source_code_2',
         'energy_source_code_3', 'energy_source_code_4',
         'energy_source_code_5', 'energy_source_code_6',
         'startup_source_code_1', 'startup_source_code_2',
         'startup_source_code_3', 'startup_source_code_4',
         'time_cold_shutdown_full_load_code', 'syncronized_transmission_grid',
         'turbines_num', 'operational_status_code', 'operational_status',
         'planned_modifications', 'planned_net_summer_capacity_uprate_mw',
         'planned_net_winter_capacity_uprate_mw', 'planned_new_capacity_mw',
         'planned_uprate_date', 'planned_net_summer_capacity_derate_mw',
         'planned_net_winter_capacity_derate_mw', 'planned_derate_date',
         'planned_new_prime_mover_code', 'planned_energy_source_code_1',
         'planned_repower_date', 'other_planned_modifications',
         'other_modifications_date', 'planned_retirement_date',
         'carbon_capture', 'cofire_fuels', 'switch_oil_gas',
         'turbines_inverters_hydrokinetics', 'nameplate_power_factor',
         'uprate_derate_during_year', 'uprate_derate_completed_date',
         'current_planned_operating_date', 'summer_estimated_capability_mw',
         'winter_estimated_capability_mw', 'retirement_date'],
        # need type fixing
        {'plant_id_eia': 'int64',
         'generator_id': 'str'}, ],
    # utilities must come after plants. plant location needs to be
    # removed before the utility locations are compiled
    'utilities': [  # base cols
        ['utility_id_eia'],
        # static cols
        ['utility_name',
         'entity_type'],
        # annual cols
        ['street_address', 'city', 'state', 'zip_code',
         'plants_reported_owner', 'plants_reported_operator',
         'plants_reported_asset_manager', 'plants_reported_other_relationship',
         ],
        # need type fixing
        {'utility_id_eia': 'int64', }, ],
    'boilers': [  # base cols
        ['plant_id_eia', 'boiler_id'],
        # static cols
        ['prime_mover_code'],
        # annual cols
        [],
        # need type fixing
        {'plant_id_eia': 'int64',
         'boiler_id': 'str', }, ]}
"""dict: A dictionary containing table name strings (keys) and lists of columns
    to keep for those tables (values).
"""

# EPA CEMS constants #####

epacems_rename_dict = {
    "STATE": "state",
    # "FACILITY_NAME": "plant_name",  # Not reading from CSV
    "ORISPL_CODE": "plant_id_eia",
    "UNITID": "unitid",
    # These op_date, op_hour, and op_time variables get converted to
    # operating_date, operating_datetime and operating_time_interval in
    # transform/epacems.py
    "OP_DATE": "op_date",
    "OP_HOUR": "op_hour",
    "OP_TIME": "operating_time_hours",
    "GLOAD (MW)": "gross_load_mw",
    "GLOAD": "gross_load_mw",
    "SLOAD (1000 lbs)": "steam_load_1000_lbs",
    "SLOAD (1000lb/hr)": "steam_load_1000_lbs",
    "SLOAD": "steam_load_1000_lbs",
    "SO2_MASS (lbs)": "so2_mass_lbs",
    "SO2_MASS": "so2_mass_lbs",
    "SO2_MASS_MEASURE_FLG": "so2_mass_measurement_code",
    # "SO2_RATE (lbs/mmBtu)": "so2_rate_lbs_mmbtu",  # Not reading from CSV
    # "SO2_RATE": "so2_rate_lbs_mmbtu",  # Not reading from CSV
    # "SO2_RATE_MEASURE_FLG": "so2_rate_measure_flg",  # Not reading from CSV
    "NOX_RATE (lbs/mmBtu)": "nox_rate_lbs_mmbtu",
    "NOX_RATE": "nox_rate_lbs_mmbtu",
    "NOX_RATE_MEASURE_FLG": "nox_rate_measurement_code",
    "NOX_MASS (lbs)": "nox_mass_lbs",
    "NOX_MASS": "nox_mass_lbs",
    "NOX_MASS_MEASURE_FLG": "nox_mass_measurement_code",
    "CO2_MASS (tons)": "co2_mass_tons",
    "CO2_MASS": "co2_mass_tons",
    "CO2_MASS_MEASURE_FLG": "co2_mass_measurement_code",
    # "CO2_RATE (tons/mmBtu)": "co2_rate_tons_mmbtu",  # Not reading from CSV
    # "CO2_RATE": "co2_rate_tons_mmbtu",  # Not reading from CSV
    # "CO2_RATE_MEASURE_FLG": "co2_rate_measure_flg",  # Not reading from CSV
    "HEAT_INPUT (mmBtu)": "heat_content_mmbtu",
    "HEAT_INPUT": "heat_content_mmbtu",
    "FAC_ID": "facility_id",
    "UNIT_ID": "unit_id_epa",
}
"""dict: A dictionary containing EPA CEMS column names (keys) and replacement
    names to use when reading those columns into PUDL (values).
"""
# Any column that exactly matches one of these won't be read
epacems_columns_to_ignore = {
    "FACILITY_NAME",
    "SO2_RATE (lbs/mmBtu)",
    "SO2_RATE",
    "SO2_RATE_MEASURE_FLG",
    "CO2_RATE (tons/mmBtu)",
    "CO2_RATE",
    "CO2_RATE_MEASURE_FLG",
}
"""set: The set of EPA CEMS columns to ignore when reading data.
"""
# Specify dtypes to for reading the CEMS CSVs
epacems_csv_dtypes = {
    "STATE": str,
    # "FACILITY_NAME": str,  # Not reading from CSV
    "ORISPL_CODE": int,
    "UNITID": str,
    # These op_date, op_hour, and op_time variables get converted to
    # operating_date, operating_datetime and operating_time_interval in
    # transform/epacems.py
    "OP_DATE": str,
    "OP_HOUR": int,
    "OP_TIME": float,
    "GLOAD (MW)": float,
    "GLOAD": float,
    "SLOAD (1000 lbs)": float,
    "SLOAD (1000lb/hr)": float,
    "SLOAD": float,
    "SO2_MASS (lbs)": float,
    "SO2_MASS": float,
    "SO2_MASS_MEASURE_FLG": str,
    # "SO2_RATE (lbs/mmBtu)": float,  # Not reading from CSV
    # "SO2_RATE": float,  # Not reading from CSV
    # "SO2_RATE_MEASURE_FLG": str,  # Not reading from CSV
    "NOX_RATE (lbs/mmBtu)": float,
    "NOX_RATE": float,
    "NOX_RATE_MEASURE_FLG": str,
    "NOX_MASS (lbs)": float,
    "NOX_MASS": float,
    "NOX_MASS_MEASURE_FLG": str,
    "CO2_MASS (tons)": float,
    "CO2_MASS": float,
    "CO2_MASS_MEASURE_FLG": str,
    # "CO2_RATE (tons/mmBtu)": float,  # Not reading from CSV
    # "CO2_RATE": float,  # Not reading from CSV
    # "CO2_RATE_MEASURE_FLG": str,  # Not reading from CSV
    "HEAT_INPUT (mmBtu)": float,
    "HEAT_INPUT": float,
    "FAC_ID": int,
    "UNIT_ID": int,
}
"""dict: A dictionary containing column names (keys) and data types (values) for
    EPA CEMS.
"""
epacems_columns_fill_na_dict = {
    "gross_load_mw": 0.0,
    "heat_content_mmbtu": 0.0
}
"""set: the set of EPA CEMS columns to

    Todo:
        Return to
"""

epacems_tables = ("hourly_emissions_epacems")
"""tuple: A tuple containing tables of EPA CEMS data to pull into PUDL.
"""

epacems_additional_plant_info_file = importlib.resources.open_text(
    'pudl.package_data.epa.cems', 'plant_info_for_additional_cems_plants.csv')
"""typing.TextIO:

    Todo:
        Return to
"""

files_dict_epaipm = {
    'transmission_single_epaipm': '*table_3-21*',
    'transmission_joint_epaipm': '*transmission_joint_ipm*',
    'load_curves_epaipm': '*table_2-2_*',
    'plant_region_map_epaipm': '*needs_v6*',
}
"""dict: A dictionary of EPA IPM tables and strings that files of those tables
    contain.
"""

epaipm_url_ext = {
    'transmission_single_epaipm': 'table_3-21_annual_transmission_capabilities_of_u.s._model_regions_in_epa_platform_v6_-_2021.xlsx',
    'load_curves_epaipm': 'table_2-2_load_duration_curves_used_in_epa_platform_v6.xlsx',
    'plant_region_map_epaipm': 'needs_v6_november_2018_reference_case_0.xlsx',
}
"""dict: A dictionary of EPA IPM tables and associated URLs extensions for
    downloading that table's data.
"""

read_excel_epaipm_dict = {
    'transmission_single_epaipm': dict(
        skiprows=3,
        usecols='B:F',
        index_col=[0, 1],
    ),
    'transmission_joint_epaipm': {},
    'load_curves_epaipm': dict(
        skiprows=3,
        usecols='B:AB',
    ),
    'plant_region_map_epaipm_active': dict(
        sheet_name='NEEDS v6_Active',
        usecols='C,I',
    ),
    'plant_region_map_epaipm_retired': dict(
        sheet_name='NEEDS v6_Retired_Through2021',
        usecols='C,I',
    ),
}
"""
dict: A dictionary of dictionaries containing EPA IPM tables and associated
    information for reading those tables into PUDL (values).
"""

epaipm_region_names = [
    'ERC_PHDL', 'ERC_REST', 'ERC_FRNT', 'ERC_GWAY', 'ERC_WEST',
    'FRCC', 'NENG_CT', 'NENGREST', 'NENG_ME', 'MIS_AR', 'MIS_IL',
    'MIS_INKY', 'MIS_IA', 'MIS_MIDA', 'MIS_LA', 'MIS_LMI', 'MIS_MNWI',
    'MIS_D_MS', 'MIS_MO', 'MIS_MAPP', 'MIS_AMSO', 'MIS_WOTA',
    'MIS_WUMS', 'NY_Z_A', 'NY_Z_B', 'NY_Z_C&E', 'NY_Z_D', 'NY_Z_F',
    'NY_Z_G-I', 'NY_Z_J', 'NY_Z_K', 'PJM_West', 'PJM_AP', 'PJM_ATSI',
    'PJM_COMD', 'PJM_Dom', 'PJM_EMAC', 'PJM_PENE', 'PJM_SMAC',
    'PJM_WMAC', 'S_C_KY', 'S_C_TVA', 'S_D_AECI', 'S_SOU', 'S_VACA',
    'SPP_NEBR', 'SPP_N', 'SPP_SPS', 'SPP_WEST', 'SPP_KIAM', 'SPP_WAUE',
    'WECC_AZ', 'WEC_BANC', 'WECC_CO', 'WECC_ID', 'WECC_IID',
    'WEC_LADW', 'WECC_MT', 'WECC_NM', 'WEC_CALN', 'WECC_NNV',
    'WECC_PNW', 'WEC_SDGE', 'WECC_SCE', 'WECC_SNV', 'WECC_UT',
    'WECC_WY', 'CN_AB', 'CN_BC', 'CN_NL', 'CN_MB', 'CN_NB', 'CN_NF',
    'CN_NS', 'CN_ON', 'CN_PE', 'CN_PQ', 'CN_SK',
]
"""list: A list of EPA IPM region names."""

epaipm_region_aggregations = {
    'PJM': [
        'PJM_AP', 'PJM_ATSI', 'PJM_COMD', 'PJM_Dom',
        'PJM_EMAC', 'PJM_PENE', 'PJM_SMAC', 'PJM_WMAC'
    ],
    'NYISO': [
        'NY_Z_A', 'NY_Z_B', 'NY_Z_C&E', 'NY_Z_D',
        'NY_Z_F', 'NY_Z_G-I', 'NY_Z_J', 'NY_Z_K'
    ],
    'ISONE': ['NENG_CT', 'NENGREST', 'NENG_ME'],
    'MISO': [
        'MIS_AR', 'MIS_IL', 'MIS_INKY', 'MIS_IA',
        'MIS_MIDA', 'MIS_LA', 'MIS_LMI', 'MIS_MNWI', 'MIS_D_MS',
        'MIS_MO', 'MIS_MAPP', 'MIS_AMSO', 'MIS_WOTA', 'MIS_WUMS'
    ],
    'SPP': [
        'SPP_NEBR', 'SPP_N', 'SPP_SPS', 'SPP_WEST', 'SPP_KIAM', 'SPP_WAUE'
    ],
    'WECC_NW': [
        'WECC_CO', 'WECC_ID', 'WECC_MT', 'WECC_NNV',
        'WECC_PNW', 'WECC_UT', 'WECC_WY'
    ]

}
"""
dict: A dictionary containing EPA IPM regions (keys) and lists of their
    associated abbreviations (values).
"""

epaipm_rename_dict = {
    'transmission_single_epaipm': {
        'From': 'region_from',
        'To': 'region_to',
        'Capacity TTC (MW)': 'firm_ttc_mw',
        'Energy TTC (MW)': 'nonfirm_ttc_mw',
        'Transmission Tariff (2016 mills/kWh)': 'tariff_mills_kwh',
    },
    'load_curves_epaipm': {
        'day': 'day_of_year',
        'region': 'region_id_epaipm',
    },
    'plant_region_map_epaipm': {
        'ORIS Plant Code': 'plant_id_eia',
        'Region Name': 'region',
    },
}
glue_pudl_tables = ('plants_eia', 'plants_ferc', 'plants', 'utilities_eia',
                    'utilities_ferc', 'utilities', 'utility_plant_assn')
"""
dict: A dictionary of dictionaries containing EPA IPM tables (keys) and items
    for each table to be renamed along with the replacement name (values).
"""

data_sources = (
    'eia860',
    'eia923',
    'epacems',
    'ferc1',
    'epaipm',
    # 'pudl'
)
"""tuple: A tuple containing the data sources we are able to pull into PUDL."""

# All the years for which we ought to be able to download these data sources
data_years = {
    'eia860': tuple(range(2001, 2019)),
    'eia923': tuple(range(2001, 2019)),
    'epacems': tuple(range(1995, 2019)),
    'ferc1': tuple(range(1994, 2019)),
    'epaipm': (None, ),
}
"""
dict: A dictionary of data sources (keys) and tuples containing the years
    that we expect to be able to download for each data source (values).
"""

# The full set of years we currently expect to be able to ingest, per source:
working_years = {
    'eia860': tuple(range(2011, 2018)),
    'eia923': tuple(range(2009, 2018)),
    'epacems': tuple(range(1995, 2019)),
    'ferc1': tuple(range(2004, 2018)),
    'epaipm': (None, ),
}
"""
dict: A dictionary of data sources (keys) and tuples containing the years for
    each data source that are able to be ingested into PUDL.
"""

pudl_tables = {
    'eia860': eia860_pudl_tables,
    'eia923': eia923_pudl_tables,
    'ferc1': ferc1_pudl_tables,
    'epacems': epacems_tables,
    'epaipm': epaipm_pudl_tables,
    'glue': glue_pudl_tables,
}
"""
dict: A dictionary containing data sources (keys) and the list of associated
    tables from that datasource that can be pulled into PUDL (values).
"""

base_data_urls = {
    'eia860': 'https://www.eia.gov/electricity/data/eia860',
    'eia861': 'https://www.eia.gov/electricity/data/eia861/zip',
    'eia923': 'https://www.eia.gov/electricity/data/eia923',
    'epacems': 'ftp://newftp.epa.gov/dmdnload/emissions/hourly/monthly',
    'ferc1': 'ftp://eforms1.ferc.gov/f1allyears',
    'ferc714': 'https://www.ferc.gov/docs-filing/forms/form-714/data',
    'ferceqr': 'ftp://eqrdownload.ferc.gov/DownloadRepositoryProd/BulkNew/CSV',
    'msha': 'https://arlweb.msha.gov/OpenGovernmentData/DataSets',
    'epaipm': 'https://www.epa.gov/sites/production/files/2019-03',
    'pudl': 'https://catalyst.coop/pudl/'
}
"""
dict: A dictionary containing data sources (keys) and their base data URLs
    (values).
"""

need_fix_inting = {
    # 'generators_eia860': ('turbines_num',),
    'coalmine_eia923': ('mine_id_msha', 'county_id_fips'),
    'fuel_receipts_costs_eia923': ('mine_id_pudl',),
    'generation_fuel_eia923': ('nuclear_unit_id',),
    'plants_steam_ferc1': ('construction_year', 'installation_year'),
    'plants_small_ferc1': ('construction_year', 'ferc_license_id'),
    'plants_hydro_ferc1': ('construction_year', 'installation_year',),
    'plants_pumped_storage_ferc1': ('construction_year', 'installation_year',),
    'hourly_emissions_epacems': ('facility_id', 'unit_id_epa',),
    'plants_eia860': ('utility_id_eia',),
    'generators_eia860': ('turbines_num',),
    'plants_entity_eia': ('zip_code',),
}
"""
dict: A dictionary containing tables (keys) and column names (values)
    containing integer-type columns whose null values need fixing.
"""

contributors = {
    "catalyst-cooperative": {
        "title": "Catalyst Cooperative",
        "path": "https://catalyst.coop/",
        "role": "publisher",
        "email": "pudl@catalyst.coop",
        "organization": "Catalyst Cooperative",
    },
    "zane-selvans": {
        "title": "Zane Selvans",
        "email": "zane.selvans@catalyst.coop",
        "path": "https://amateurearthling.org/",
        "role": "wrangler",
        "organization": "Catalyst Cooperative"
    },
    "christina-gosnell": {
        "title": "Christina Gosnell",
        "email": "christina.gosnell@catalyst.coop",
        "role": "contributor",
        "organization": "Catalyst Cooperative",
    },
    "steven-winter": {
        "title": "Steven Winter",
        "email": "steven.winter@catalyst.coop",
        "role": "contributor",
        "organization": "Catalyst Cooperative",
    },
    "alana-wilson": {
        "title": "Alana Wilson",
        "email": "alana.wilson@catalyst.coop",
        "role": "contributor",
        "organization": "Catalyst Cooperative",
    },
    "karl-dunkle-werner": {
        "title": "Karl Dunkle Werner",
        "email": "karldw@berkeley.edu",
        "path": "https://karldw.org/",
        "role": "contributor",
        "organization": "UC Berkeley",
    },
    "climate-policy-initiative": {
        "title": "Climate Policy Initiative",
        "path": "https://climatepolicyinitiative.org/",
        "role": "contributor",
        "organization": "Climate Policy Initiative",
    },
    'greg-schivley': {
        "title": "Greg Schivley",
        "role": "contributor",
    },
}
"""
dict: A dictionary of dictionaries containing organization names (keys) and
    their attributes (values).
"""

data_source_info = {
    "pudl": {
        "title": "Public Utility Data Liberation Project (PUDL)",
        "path": "https://catalyst.coop/pudl/",
        "email": "pudl@catalyst.coop",
    },
    "eia923": {
        "title": "EIA Form 923",
        "path": "https://www.eia.gov/electricity/data/eia923/",
    },
    "eia860": {
        "title": "EIA Form 860",
        "path": "https://www.eia.gov/electricity/data/eia860/",
    },
    "eia861": {
        "title": "EIA Form 861",
    },
    "eiawater": {
        "title": "EIA Water Use for Power",
    },
    "ferc1": {
        "title": "FERC Form 1",
        "path": "https://www.ferc.gov/docs-filing/forms/form-1/data.asp",
    },
    "epacems": {
        "title": "EPA Air Markets Program Data",
        "path": "https://ampd.epa.gov/ampd/",
    },
    "msha": {
        "title": "Mining Safety and Health Administration",
    },
    "phmsa": {
        "title": "Pipelines and Hazardous Materials Safety Administration",
    },
    "ferceqr": {
        "title": "FERC Electric Quarterly Report",
    },
    "ferc714": {
        "title": "FERC Form 714",
    }
}
"""
dict: A dictionary of dictionaries containing datasources (keys) and
    associated attributes (values)
"""

contributors_by_source = {
    "pudl": [
        "catalyst-cooperative",
        "zane-selvans",
        "christina-gosnell",
        "steven-winter",
        "alana-wilson",
        "karl-dunkle-werner",
        "climate-policy-initiative",
    ],
    "eia923": [
        "catalyst-cooperative",
        "zane-selvans",
        "christina-gosnell",
        "steven-winter",
    ],
    "eia860": [
        "catalyst-cooperative",
        "zane-selvans",
        "christina-gosnell",
        "steven-winter",
        "alana-wilson",
    ],
    "ferc1": [
        "catalyst-cooperative",
        "zane-selvans",
        "christina-gosnell",
        "steven-winter",
        "alana-wilson",
    ],
    "epacems": [
        "catalyst-cooperative",
        "karl-dunkle-werner",
        "zane-selvans",
    ],
    "epaipm": [
        "greg-schivley",
    ],
}
"""
dict: A dictionary of data sources (keys) and lists of contributors (values).
"""

licenses = {
    "cc-by-4.0": {
        "name": "CC-BY-4.0",
        "title": "Creative Commons Attribution 4.0",
        "path": "https://creativecommons.org/licenses/by/4.0/"
    },
    "us-govt": {
        "name": "other-pd",
        "title": "U.S. Government Work",
        "path": "http://www.usa.gov/publicdomain/label/1.0/",
    }
}
"""
dict: A dictionary of dictionaries containing license types and their
    attributes.
"""

output_formats = [
    'sqlite',
    'parquet',
    'datapackage',
    'notebook',
]
"""list: A list of types of PUDL output formats."""
