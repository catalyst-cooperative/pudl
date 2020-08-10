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

# Construct a dictionary mapping a canonical fuel name to a list of strings
# which are used to represent that fuel in the FERC Form 1 Reporting. Case is
# ignored, as all fuel strings can be converted to a lower case in the data
# set.
# Previous categories of ferc1_biomass_strings and ferc1_stream_strings have
# been deleted and their contents redistributed to ferc1_waste_strings and
# ferc1_other_strings

ferc1_coal_strings = [
    'coal', 'coal-subbit', 'lignite', 'coal(sb)', 'coal (sb)', 'coal-lignite',
    'coke', 'coa', 'lignite/coal', 'coal - subbit', 'coal-subb', 'coal-sub',
    'coal-lig', 'coal-sub bit', 'coals', 'ciak', 'petcoke', 'coal.oil',
    'coal/gas', 'bit coal', 'coal-unit #3', 'coal-subbitum', 'coal tons',
    'coal mcf', 'coal unit #3', 'pet. coke', 'coal-u3', 'coal&coke', 'tons'
]
"""
list: A list of strings which are used to represent coal fuel in FERC Form 1
    reporting.
"""

ferc1_oil_strings = [
    'oil', '#6 oil', '#2 oil', 'fuel oil', 'jet', 'no. 2 oil', 'no.2 oil',
    'no.6& used', 'used oil', 'oil-2', 'oil (#2)', 'diesel oil',
    'residual oil', '# 2 oil', 'resid. oil', 'tall oil', 'oil/gas',
    'no.6 oil', 'oil-fuel', 'oil-diesel', 'oil / gas', 'oil bbls', 'oil bls',
    'no. 6 oil', '#1 kerosene', 'diesel', 'no. 2 oils', 'blend oil',
    '#2oil diesel', '#2 oil-diesel', '# 2  oil', 'light oil', 'heavy oil',
    'gas.oil', '#2', '2', '6', 'bbl', 'no 2 oil', 'no 6 oil', '#1 oil', '#6',
    'oil-kero', 'oil bbl', 'biofuel', 'no 2', 'kero', '#1 fuel oil',
    'no. 2  oil', 'blended oil', 'no 2. oil', '# 6 oil', 'nno. 2 oil',
    '#2 fuel', 'oill', 'oils', 'gas/oil', 'no.2 oil gas', '#2 fuel oil',
    'oli', 'oil (#6)', 'oil/diesel', '2 oil', '#6 hvy oil', 'jet fuel',
    'diesel/compos', 'oil-8', 'oil {6}', 'oil-unit #1', 'bbl.', 'oil.',
    'oil #6', 'oil (6)', 'oil(#2)', 'oil-unit1&2', 'oil-6', '#2 fue oil',
    'dielel oil', 'dielsel oil', '#6 & used', 'barrels', 'oil un 1 & 2',
    'jet oil', 'oil-u1&2', 'oiul', 'pil', 'oil - 2', '#6 & used', 'oial'
]
"""
list: A list of strings which are used to represent oil fuel in FERC Form 1
    reporting.
"""

ferc1_gas_strings = [
    'gas', 'gass', 'methane', 'natural gas', 'blast gas', 'gas mcf',
    'propane', 'prop', 'natural  gas', 'nat.gas', 'nat gas',
    'nat. gas', 'natl gas', 'ga', 'gas`', 'syngas', 'ng', 'mcf',
    'blast gaa', 'nat  gas', 'gac', 'syngass', 'prop.', 'natural', 'coal.gas',
    'n. gas', 'lp gas', 'natuaral gas', 'coke gas', 'gas #2016', 'propane**',
    '* propane', 'propane **', 'gas expander', 'gas ct', '# 6 gas', '#6 gas',
    'coke oven gas'
]
"""
list: A list of strings which are used to represent gas fuel in FERC Form 1
    reporting.
"""

ferc1_solar_strings = []

ferc1_wind_strings = []

ferc1_hydro_strings = []

ferc1_nuke_strings = [
    'nuclear', 'grams of uran', 'grams of', 'grams of  ura',
    'grams', 'nucleur', 'nulear', 'nucl', 'nucleart', 'nucelar',
    'gr.uranium', 'grams of urm', 'nuclear (9)', 'nulcear', 'nuc',
    'gr. uranium', 'nuclear mw da', 'grams of ura'
]
"""
list: A list of strings which are used to represent nuclear fuel in FERC Form
    1 reporting.
"""

ferc1_waste_strings = [
    'tires', 'tire', 'refuse', 'switchgrass', 'wood waste', 'woodchips',
    'biomass', 'wood', 'wood chips', 'rdf', 'tires/refuse', 'tire refuse',
    'waste oil', 'waste', 'woodships', 'tire chips'
]
"""
list: A list of strings which are used to represent waste fuel in FERC Form 1
    reporting.
"""

ferc1_other_strings = [
    'steam', 'purch steam', 'all', 'tdf', 'n/a', 'purch. steam', 'other',
    'composite', 'composit', 'mbtus', 'total', 'avg', 'avg.', 'blo',
    'all fuel', 'comb.', 'alt. fuels', 'na', 'comb', '/#=2\x80â\x91?',
    'kã\xadgv¸\x9d?', "mbtu's", 'gas, oil', 'rrm', '3\x9c', 'average',
    'furfural', '0', 'watson bng', 'toal', 'bng', '# 6 & used', 'combined',
    'blo bls', 'compsite', '*', 'compos.', 'gas / oil', 'mw days', 'g', 'c',
    'lime', 'all fuels', 'at right', '20', '1', 'comp oil/gas', 'all fuels to',
    'the right are', 'c omposite', 'all fuels are', 'total pr crk',
    'all fuels =', 'total pc', 'comp', 'alternative', 'alt. fuel', 'bio fuel',
    'total prairie', ''
]
"""list: A list of strings which are used to represent other fuels in FERC Form
    1 reporting.
"""

# There are also a bunch of other weird and hard to categorize strings
# that I don't know what to do with... hopefully they constitute only a
# small fraction of the overall generation.

ferc1_fuel_strings = {"coal": ferc1_coal_strings,
                      "oil": ferc1_oil_strings,
                      "gas": ferc1_gas_strings,
                      "solar": ferc1_solar_strings,
                      "wind": ferc1_wind_strings,
                      "hydro": ferc1_hydro_strings,
                      "nuclear": ferc1_nuke_strings,
                      "waste": ferc1_waste_strings,
                      "other": ferc1_other_strings
                      }
"""dict: A dictionary linking fuel types (keys) to lists of various strings
    representing that fuel (values)
"""
# Similarly, dictionary for cleaning up fuel unit strings
ferc1_ton_strings = ['toms', 'taons', 'tones', 'col-tons', 'toncoaleq', 'coal',
                     'tons coal eq', 'coal-tons', 'ton', 'tons', 'tons coal',
                     'coal-ton', 'tires-tons', 'coal tons -2 ',
                     'coal tons 200', 'ton-2000', 'coal tons -2', 'coal tons',
                     'coal-tone', 'tire-ton', 'tire-tons', 'ton coal eqv']
"""list: A list of fuel unit strings for tons."""

ferc1_mcf_strings = \
    ['mcf', "mcf's", 'mcfs', 'mcf.', 'gas mcf', '"gas" mcf', 'gas-mcf',
     'mfc', 'mct', ' mcf', 'msfs', 'mlf', 'mscf', 'mci', 'mcl', 'mcg',
     'm.cu.ft.', 'kcf', '(mcf)', 'mcf *(4)', 'mcf00', 'm.cu.ft..']
"""list: A list of fuel unit strings for thousand cubic feet."""

ferc1_bbl_strings = \
    ['barrel', 'bbls', 'bbl', 'barrels', 'bbrl', 'bbl.', 'bbls.',
     'oil 42 gal', 'oil-barrels', 'barrrels', 'bbl-42 gal',
     'oil-barrel', 'bb.', 'barrells', 'bar', 'bbld', 'oil- barrel',
     'barrels    .', 'bbl .', 'barels', 'barrell', 'berrels', 'bb',
     'bbl.s', 'oil-bbl', 'bls', 'bbl:', 'barrles', 'blb', 'propane-bbl',
     'barriel', 'berriel', 'barrile', '(bbl.)', 'barrel *(4)', '(4) barrel',
     'bbf', 'blb.', '(bbl)', 'bb1', 'bbsl', 'barrrel', 'barrels 100%',
     'bsrrels', "bbl's", '*barrels', 'oil - barrels', 'oil 42 gal ba', 'bll',
     'boiler barrel', 'gas barrel', '"boiler" barr', '"gas" barrel',
     '"boiler"barre', '"boiler barre', 'barrels .']
"""list: A list of fuel unit strings for barrels."""

ferc1_gal_strings = ['gallons', 'gal.', 'gals', 'gals.', 'gallon', 'gal',
                     'galllons']
"""list: A list of fuel unit strings for gallons."""

ferc1_1kgal_strings = ['oil(1000 gal)', 'oil(1000)', 'oil (1000)', 'oil(1000',
                       'oil(1000ga)']
"""list: A list of fuel unit strings for thousand gallons."""

ferc1_gramsU_strings = [  # noqa: N816 (U-ranium is capitalized...)
    'gram', 'grams', 'gm u', 'grams u235', 'grams u-235', 'grams of uran',
    'grams: u-235', 'grams:u-235', 'grams:u235', 'grams u308', 'grams: u235',
    'grams of', 'grams - n/a', 'gms uran', 's e uo2 grams', 'gms uranium',
    'grams of urm', 'gms. of uran', 'grams (100%)', 'grams v-235',
    'se uo2 grams'
]
"""list: A list of fuel unit strings for grams."""

ferc1_kgU_strings = [  # noqa: N816 (U-ranium is capitalized...)
    'kg of uranium', 'kg uranium', 'kilg. u-235', 'kg u-235', 'kilograms-u23',
    'kg', 'kilograms u-2', 'kilograms', 'kg of', 'kg-u-235', 'kilgrams',
    'kilogr. u235', 'uranium kg', 'kg uranium25', 'kilogr. u-235',
    'kg uranium 25', 'kilgr. u-235', 'kguranium 25', 'kg-u235'
]
"""list: A list of fuel unit strings for thousand grams."""

ferc1_mmbtu_strings = ['mmbtu', 'mmbtus', 'mbtus', '(mmbtu)',
                       "mmbtu's", 'nuclear-mmbtu', 'nuclear-mmbt']
"""list: A list of fuel unit strings for million British Thermal Units."""

ferc1_mwdth_strings = \
    ['mwd therman', 'mw days-therm', 'mwd thrml', 'mwd thermal',
     'mwd/mtu', 'mw days', 'mwdth', 'mwd', 'mw day', 'dth', 'mwdaysthermal',
     'mw day therml', 'mw days thrml', 'nuclear mwd', 'mmwd', 'mw day/therml'
     'mw days/therm', 'mw days (th', 'ermal)']
"""list: A list of fuel unit strings for megawatt days thermal."""

ferc1_mwhth_strings = ['mwh them', 'mwh threm', 'nwh therm', 'mwhth',
                       'mwh therm', 'mwh', 'mwh therms.', 'mwh term.uts',
                       'mwh thermal', 'mwh thermals', 'mw hr therm',
                       'mwh therma', 'mwh therm.uts']
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

ferc1_plant_kind_steam_turbine = [
    'coal', 'steam', 'steam units 1 2 3', 'steam units 4 5',
    'steam fossil', 'steam turbine', 'steam a', 'steam 100',
    'steam units 1 2 3', 'steams', 'steam 1', 'steam retired 2013', 'stream',
    'steam units 1,2,3', 'steam units 4&5', 'steam units 4&6',
    'steam conventional', 'unit total-steam', 'unit total steam',
    '*resp. share steam', 'resp. share steam', 'steam (see note 1,',
    'steam (see note 3)', 'mpc 50%share steam', '40% share steam'
    'steam (2)', 'steam (3)', 'steam (4)', 'steam (5)', 'steam (6)',
    'steam (7)', 'steam (8)', 'steam units 1 and 2', 'steam units 3 and 4',
    'steam (note 1)', 'steam (retired)', 'steam (leased)', 'coal-fired steam',
    'oil-fired steam', 'steam/fossil', 'steam (a,b)', 'steam (a)', 'stean',
    'steam-internal comb', 'steam (see notes)', 'steam units 4 & 6',
    'resp share stm note3' 'mpc50% share steam', 'mpc40%share steam',
    'steam - 64%', 'steam - 100%', 'steam (1) & (2)', 'resp share st note3',
    'mpc 50% shares steam', 'steam-64%', 'steam-100%', 'steam (see note 1)',
    'mpc 50% share steam', 'steam units 1, 2, 3', 'steam units 4, 5',
    'steam (2)', 'steam (1)', 'steam 4, 5', 'steam - 72%', 'steam (incl i.c.)',
    'steam- 72%', 'steam;retired - 2013', "respondent's sh.-st.",
    "respondent's sh-st", '40% share steam', 'resp share stm note3',
    'mpc50% share steam', 'resp share st note 3', '\x02steam (1)',
]
"""
list: A list of strings from FERC Form 1 for the steam turbine plant kind.
"""

ferc1_plant_kind_combustion_turbine = [
    'combustion turbine', 'gt', 'gas turbine',
    'gas turbine # 1', 'gas turbine', 'gas turbine (note 1)',
    'gas turbines', 'simple cycle', 'combustion turbine',
    'comb.turb.peak.units', 'gas turbine', 'combustion turbine',
    'com turbine peaking', 'gas turbine peaking', 'comb turb peaking',
    'combustine turbine', 'comb. turine', 'conbustion turbine',
    'combustine turbine', 'gas turbine (leased)', 'combustion tubine',
    'gas turb', 'gas turbine peaker', 'gtg/gas', 'simple cycle turbine',
    'gas-turbine', 'gas turbine-simple', 'gas turbine - note 1',
    'gas turbine #1', 'simple cycle', 'gasturbine', 'combustionturbine',
    'gas turbine (2)', 'comb turb peak units', 'jet engine',
    'jet powered turbine', '*gas turbine', 'gas turb.(see note5)',
    'gas turb. (see note', 'combutsion turbine', 'combustion turbin',
    'gas turbine-unit 2', 'gas - turbine', 'comb turbine peaking',
    'gas expander turbine', 'jet turbine', 'gas turbin (lease',
    'gas turbine (leased', 'gas turbine/int. cm', 'comb.turb-gas oper.',
    'comb.turb.gas/oil op', 'comb.turb.oil oper.', 'jet', 'comb. turbine (a)',
    'gas turb.(see notes)', 'gas turb(see notes)', 'comb. turb-gas oper',
    'comb.turb.oil oper', 'gas turbin (leasd)', 'gas turbne/int comb',
    'gas turbine (note1)', 'combution turbin', '* gas turbine',
    'add to gas turbine', 'gas turbine (a)', 'gas turbinint comb',
    'gas turbine (note 3)', 'resp share gas note3', 'gas trubine',
    '*gas turbine(note3)', 'gas turbine note 3,6', 'gas turbine note 4,6',
    'gas turbine peakload', 'combusition turbine', 'gas turbine (lease)',
    'comb. turb-gas oper.', 'combution turbine', 'combusion turbine',
    'comb. turb. oil oper', 'combustion burbine', 'combustion and gas',
    'comb. turb.', 'gas turbine (lease', 'gas turbine (leasd)',
    'gas turbine/int comb', '*gas turbine(note 3)', 'gas turbine (see nos',
    'i.c.e./gas turbine', 'gas turbine/intcomb', 'cumbustion turbine',
    'gas turb, int. comb.', 'gas turb, diesel', 'gas turb, int. comb',
    'i.c.e/gas turbine', 'diesel turbine', 'comubstion turbine',
    'i.c.e. /gas turbine', 'i.c.e/ gas turbine', 'i.c.e./gas tubine',
]
"""list: A list of strings from FERC Form 1 for the combustion turbine plant
    kind.
"""

ferc1_plant_kind_combined_cycle = [
    'Combined cycle', 'combined cycle', 'combined', 'gas & steam turbine',
    'gas turb. & heat rec', 'combined cycle', 'com. cyc', 'com. cycle',
    'gas turb-combined cy', 'combined cycle ctg', 'combined cycle - 40%',
    'com cycle gas turb', 'combined cycle oper', 'gas turb/comb. cyc',
    'combine cycle', 'cc', 'comb. cycle', 'gas turb-combined cy',
    'steam and cc', 'steam cc', 'gas steam', 'ctg steam gas',
    'steam comb cycle', 'gas/steam comb. cycl', 'steam (comb. cycle)'
    'gas turbine/steam', 'steam & gas turbine', 'gas trb & heat rec',
    'steam & combined ce', 'st/gas turb comb cyc', 'gas tur & comb cycl',
    'combined cycle (a,b)', 'gas turbine/ steam', 'steam/gas turb.',
    'steam & comb cycle', 'gas/steam comb cycle', 'comb cycle (a,b)', 'igcc',
    'steam/gas turbine', 'gas turbine / steam', 'gas tur & comb cyc',
    'comb cyc (a) (b)', 'comb cycle', 'comb cyc', 'combined turbine',
    'combine cycle oper', 'comb cycle/steam tur', 'cc / gas turb',
    'steam (comb. cycle)', 'steam & cc', 'gas turbine/steam',
    'gas turb/cumbus cycl', 'gas turb/comb cycle', 'gasturb/comb cycle',
    'gas turb/cumb. cyc', 'igcc/gas turbine', 'gas / steam', 'ctg/steam-gas',
    'ctg/steam -gas'
]
"""
list: A list of strings from FERC Form 1 for the combined cycle plant kind.
"""

ferc1_plant_kind_nuke = [
    'nuclear', 'nuclear (3)', 'steam(nuclear)', 'nuclear(see note4)'
    'nuclear steam', 'nuclear turbine', 'nuclear - steam',
    'nuclear (a)(b)(c)', 'nuclear (b)(c)', '* nuclear', 'nuclear (b) (c)',
    'nuclear (see notes)', 'steam (nuclear)', '* nuclear (note 2)',
    'nuclear (note 2)', 'nuclear (see note 2)', 'nuclear(see note4)',
    'nuclear steam', 'nuclear(see notes)', 'nuclear-steam',
    'nuclear (see note 3)'
]
"""list: A list of strings from FERC Form 1 for the nuclear plant kind."""

ferc1_plant_kind_geothermal = [
    'steam - geothermal', 'steam_geothermal', 'geothermal'
]
"""list: A list of strings from FERC Form 1 for the geothermal plant kind."""

ferc_1_plant_kind_internal_combustion = [
    'ic', 'internal combustion', 'internal comb.', 'internl combustion'
    'diesel turbine', 'int combust (note 1)', 'int. combust (note1)',
    'int.combustine', 'comb. cyc', 'internal comb', 'diesel', 'diesel engine',
    'internal combustion', 'int combust - note 1', 'int. combust - note1',
    'internal comb recip', 'reciprocating engine', 'comb. turbine',
    'internal combust.', 'int. combustion (1)', '*int combustion (1)',
    "*internal combust'n", 'internal', 'internal comb.', 'steam internal comb',
    'combustion', 'int. combustion', 'int combust (note1)', 'int. combustine',
    'internl combustion', '*int. combustion (1)'
]
"""
list: A list of strings from FERC Form 1 for the internal combustion plant
    kind.
"""

ferc1_plant_kind_wind = [
    'wind', 'wind energy', 'wind turbine', 'wind - turbine', 'wind generation'
]
"""list: A list of strings from FERC Form 1 for the wind plant kind."""

ferc1_plant_kind_photovoltaic = [
    'solar photovoltaic', 'photovoltaic', 'solar', 'solar project'
]
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
"""list: A list of strings for fuel type photovoltaic compiled by Climate
Policy Initiative.
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
    'outdoor boiler&full', 'outdoor hrsg', 'outdoor hrsg',
    'outdoor-steel encl.', 'boiler-outdr & full',
    'con.& full outdoor', 'partial outdoor', 'outdoor (auto. oper)',
    'outdoor (auto.oper)', 'outdoor construction', '1 outdoor boiler',
    '2 outdoor boilers', 'outdoor enclosure', '2 outoor boilers',
    'boiler outdr.& full', 'boiler outdr. & full', 'ful outdoor',
    'outdoor-steel enclos', 'outdoor (auto oper.)', 'con. & full outdoor',
    'outdore', 'boiler & full outdor', 'full & outdr boilers',
    'outodoor (auto oper)', 'outdoor steel encl.', 'full outoor',
    'boiler & outdoor ful', 'otdr. blr. & f. otdr', 'f.otdr & otdr.blr.',
    'oudoor (auto oper)', 'outdoor constructin', 'f. otdr. & otdr. blr',

]
"""list: A list of strings from FERC Form 1 associated with the outdoor
    construction type.
"""

ferc1_const_type_semioutdoor = [
    'more than 50% outdoo', 'more than 50% outdos', 'over 50% outdoor',
    'over 50% outdoors', 'semi-outdoor', 'semi - outdoor', 'semi outdoor',
    'semi-enclosed', 'semi-outdoor boiler', 'semi outdoor boiler',
    'semi- outdoor', 'semi - outdoors', 'semi -outdoor'
    'conven & semi-outdr', 'conv & semi-outdoor', 'conv & semi- outdoor',
    'convent. semi-outdr', 'conv. semi outdoor', 'conv(u1)/semiod(u2)',
    'conv u1/semi-od u2', 'conv-one blr-semi-od', 'convent semioutdoor',
    'conv. u1/semi-od u2', 'conv - 1 blr semi od', 'conv. ui/semi-od u2',
    'conv-1 blr semi-od', 'conven. semi-outdoor', 'conv semi-outdoor',
    'u1-conv./u2-semi-od', 'u1-conv./u2-semi -od', 'convent. semi-outdoo',
    'u1-conv. / u2-semi', 'conven & semi-outdr', 'semi -outdoor',
    'outdr & conventnl', 'conven. full outdoor', 'conv. & outdoor blr',
    'conv. & outdoor blr.', 'conv. & outdoor boil', 'conv. & outdr boiler',
    'conv. & out. boiler', 'convntl,outdoor blr', 'outdoor & conv.',
    '2 conv., 1 out. boil', 'outdoor/conventional', 'conv. boiler outdoor',
    'conv-one boiler-outd', 'conventional outdoor', 'conventional outdor',
    'conv. outdoor boiler', 'conv.outdoor boiler', 'conventional outdr.',
    'conven,outdoorboiler', 'conven full outdoor', 'conven,full outdoor',
    '1 out boil, 2 conv', 'conv. & full outdoor', 'conv. & outdr. boilr',
    'conv outdoor boiler', 'convention. outdoor', 'conv. sem. outdoor',
    'convntl, outdoor blr', 'conv & outdoor boil', 'conv & outdoor boil.',
    'outdoor & conv', 'conv. broiler outdor', '1 out boilr, 2 conv',
    'conv.& outdoor boil.', 'conven,outdr.boiler', 'conven,outdr boiler',
    'outdoor & conventil', '1 out boilr 2 conv', 'conv & outdr. boilr',
    'conven, full outdoor', 'conven full outdr.', 'conven, full outdr.',
    'conv/outdoor boiler', "convnt'l outdr boilr", '1 out boil 2 conv',
    'conv full outdoor', 'conven, outdr boiler', 'conventional/outdoor',
    'conv&outdoor boiler', 'outdoor & convention', 'conv & outdoor boilr',
    'conv & full outdoor', 'convntl. outdoor blr', 'conv - ob',
    "1conv'l/2odboilers", "2conv'l/1odboiler", 'conv-ob', 'conv.-ob',
    '1 conv/ 2odboilers', '2 conv /1 odboilers', 'conv- ob', 'conv -ob',
    'con sem outdoor', 'cnvntl, outdr, boilr', 'less than 50% outdoo',
    'under 50% outdoor', 'under 50% outdoors', '1cnvntnl/2odboilers',
    '2cnvntnl1/1odboiler', 'con & ob', 'combination (b)', 'indoor & outdoor',
    'conven. blr. & full', 'conv. & otdr. blr.', 'combination',
    'indoor and outdoor', 'conven boiler & full', "2conv'l/10dboiler",
    '4 indor/outdr boiler', '4 indr/outdr boilerr', '4 indr/outdr boiler',
    'indoor & outdoof',
]
"""list: A list of strings from FERC Form 1 associated with the semi - outdoor
    construction type, or a mix of conventional and outdoor construction.
"""

ferc1_const_type_conventional = [
    'conventional', 'conventional', 'conventional boiler', 'conv-b',
    'conventionall', 'convention', 'conventional', 'coventional',
    'conven full boiler', 'c0nventional', 'conventtional', 'convential'
    'underground', 'conventional bulb', 'conventrional',
    '*conventional', 'convential', 'convetional', 'conventioanl',
    'conventioinal', 'conventaional', 'indoor construction', 'convenional',
    'conventional steam', 'conventinal', 'convntional', 'conventionl',
    'conventionsl', 'conventiional', 'convntl steam plants', 'indoor const.',
    'full indoor', 'indoor', 'indoor automatic', 'indoor boiler',
    '(peak load) indoor', 'conventionl,indoor', 'conventionl, indoor',
    'conventional, indoor', 'comb. cycle indoor', '3 indoor boiler',
    '2 indoor boilers', '1 indoor boiler', '2 indoor boiler',
    '3 indoor boilers', 'fully contained', 'conv - b', 'conventional/boiler',
    'cnventional', 'comb. cycle indooor', 'sonventional',
]
"""list: A list of strings from FERC Form 1 associated with the conventional
    construction type.
"""

# Making a dictionary of lists from the lists of construction_type strings to
# create a dictionary of construction type lists.

ferc1_const_type_strings = {
    'outdoor': ferc1_const_type_outdoor,
    'semioutdoor': ferc1_const_type_semioutdoor,
    'conventional': ferc1_const_type_conventional,
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
"""dict: A dictionary mapping FERC Form 1 DBF files(w / o .DBF file extension)
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
    files(w / o .DBF file extension) (values).
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

# This is the set of tables which have been successfully integrated into PUDL:
ferc1_pudl_tables = (
    'fuel_ferc1',  # Plant-level data, linked to plants_steam_ferc1
    'plants_steam_ferc1',  # Plant-level data
    'plants_small_ferc1',  # Plant-level data
    'plants_hydro_ferc1',  # Plant-level data
    'plants_pumped_storage_ferc1',  # Plant-level data
    'purchased_power_ferc1',  # Inter-utility electricity transactions
    'plant_in_service_ferc1',  # Row-mapped plant accounting data.
    # 'accumulated_depreciation_ferc1'  # Requires row-mapping to be useful.
)
"""tuple: A tuple containing the FERC Form 1 tables that can be successfully
    integrated into PUDL.
"""


table_map_ferc1_pudl = {
    'fuel_ferc1': 'f1_fuel',
    'plants_steam_ferc1': 'f1_steam',
    'plants_small_ferc1': 'f1_gnrt_plant',
    'plants_hydro_ferc1': 'f1_hydro',
    'plants_pumped_storage_ferc1': 'f1_pumped_storage',
    'plant_in_service_ferc1': 'f1_plant_in_srvce',
    'purchased_power_ferc1': 'f1_purchased_pwr',
    # 'accumulated_depreciation_ferc1': 'f1_accumdepr_prvsn'
}
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

xlsx_maps_pkg = 'pudl.package_data.meta.xlsx_maps'
"""string: The location of the xlsx maps within the PUDL package data."""

##############################################################################
# EIA 923 Spreadsheet Metadata
##############################################################################
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
"""dict: A dictionary mapping column numbers (keys) to months (values)."""

##############################################################################
# EIA 860 Spreadsheet Metadata
##############################################################################

# This is the list of EIA860 tables that can be successfully pulled into PUDL
eia860_pudl_tables = (
    'boiler_generator_assn_eia860',
    'utilities_eia860',
    'plants_eia860',
    'generators_eia860',
    'ownership_eia860'
)
"""tuple: A tuple enumerating EIA 860 tables for which PUDL's ETL works."""


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
    spplmnt_num].
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
    account descriptions from FERC Form 1 pages 204 - 207, Electric Plant in
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
    Depreciation of electric utility plant(Account 108).
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
    names / descriptions (values).
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
    names / descriptions (values).
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
"""dict: A dictionary mapping EIA 923 simple fuel types("oil", "coal", "gas")
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
    'SG': 'Synthesis Gas from Petroleum Coke',
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
    'plants': [
        # base cols
        ['plant_id_eia'],
        # static cols
        ['balancing_authority_code_eia', 'balancing_authority_name_eia',
         'city', 'county', 'ferc_cogen_status',
         'ferc_exempt_wholesale_generator', 'ferc_small_power_producer',
         'grid_voltage_2_kv', 'grid_voltage_3_kv', 'grid_voltage_kv',
         'iso_rto_code', 'latitude', 'longitude',
         'nerc_region', 'plant_name_eia', 'primary_purpose_naics_id',
         'sector_id', 'sector_name', 'state', 'street_address', 'zip_code'],
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
        {},
    ],
    'generators': [
        # base cols
        ['plant_id_eia', 'generator_id'],
        # static cols
        ['prime_mover_code', 'duct_burners', 'operating_date',
         'topping_bottoming_code', 'solid_fuel_gasification',
         'pulverized_coal_tech', 'fluidized_bed_tech', 'subcritical_tech',
         'supercritical_tech', 'ultrasupercritical_tech', 'stoker_tech',
         'other_combustion_tech', 'bypass_heat_recovery',
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
         'winter_estimated_capability_mw', 'retirement_date', 'utility_id_eia'],
        # need type fixing
        {}
    ],
    # utilities must come after plants. plant location needs to be
    # removed before the utility locations are compiled
    'utilities': [
        # base cols
        ['utility_id_eia'],
        # static cols
        ['utility_name_eia',
         'entity_type'],
        # annual cols
        ['street_address', 'city', 'state', 'zip_code',
         'plants_reported_owner', 'plants_reported_operator',
         'plants_reported_asset_manager', 'plants_reported_other_relationship',
         ],
        # need type fixing
        {'utility_id_eia': 'int64', }, ],
    'boilers': [
        # base cols
        ['plant_id_eia', 'boiler_id'],
        # static cols
        ['prime_mover_code'],
        # annual cols
        [],
        # need type fixing
        {},
    ]
}
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
    "STATE": pd.StringDtype(),
    # "FACILITY_NAME": str,  # Not reading from CSV
    "ORISPL_CODE": pd.Int64Dtype(),
    "UNITID": pd.StringDtype(),
    # These op_date, op_hour, and op_time variables get converted to
    # operating_date, operating_datetime and operating_time_interval in
    # transform/epacems.py
    "OP_DATE": pd.StringDtype(),
    "OP_HOUR": pd.Int64Dtype(),
    "OP_TIME": float,
    "GLOAD (MW)": float,
    "GLOAD": float,
    "SLOAD (1000 lbs)": float,
    "SLOAD (1000lb/hr)": float,
    "SLOAD": float,
    "SO2_MASS (lbs)": float,
    "SO2_MASS": float,
    "SO2_MASS_MEASURE_FLG": pd.StringDtype(),
    # "SO2_RATE (lbs/mmBtu)": float,  # Not reading from CSV
    # "SO2_RATE": float,  # Not reading from CSV
    # "SO2_RATE_MEASURE_FLG": str,  # Not reading from CSV
    "NOX_RATE (lbs/mmBtu)": float,
    "NOX_RATE": float,
    "NOX_RATE_MEASURE_FLG": pd.StringDtype(),
    "NOX_MASS (lbs)": float,
    "NOX_MASS": float,
    "NOX_MASS_MEASURE_FLG": pd.StringDtype(),
    "CO2_MASS (tons)": float,
    "CO2_MASS": float,
    "CO2_MASS_MEASURE_FLG": pd.StringDtype(),
    # "CO2_RATE (tons/mmBtu)": float,  # Not reading from CSV
    # "CO2_RATE": float,  # Not reading from CSV
    # "CO2_RATE_MEASURE_FLG": str,  # Not reading from CSV
    "HEAT_INPUT (mmBtu)": float,
    "HEAT_INPUT": float,
    "FAC_ID": pd.Int64Dtype(),
    "UNIT_ID": pd.Int64Dtype(),
}
"""dict: A dictionary containing column names (keys) and data types (values)
for EPA CEMS.
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
    'eia861',
    'eia923',
    'epacems',
    'epaipm',
    'ferc1',
    'ferc714',
    # 'pudl'
)
"""tuple: A tuple containing the data sources we are able to pull into PUDL."""

# All the years for which we ought to be able to download these data sources
data_years = {
    'eia860': tuple(range(2001, 2019)),
    'eia861': tuple(range(1990, 2019)),
    'eia923': tuple(range(2001, 2020)),
    'epacems': tuple(range(1995, 2019)),
    'epaipm': (None, ),
    'ferc1': tuple(range(1994, 2019)),
    'ferc714': (None, ),
}
"""
dict: A dictionary of data sources (keys) and tuples containing the years
    that we expect to be able to download for each data source (values).
"""

# The full set of years we currently expect to be able to ingest, per source:
working_years = {
    'eia860': tuple(range(2009, 2019)),
    'eia861': tuple(range(2001, 2019)),
    'eia923': tuple(range(2009, 2019)),
    'epacems': tuple(range(1995, 2019)),
    'epaipm': (None, ),
    'ferc1': tuple(range(1994, 2019)),
    'ferc714': (None, ),
}
"""
dict: A dictionary of data sources (keys) and tuples containing the years for
    each data source that are able to be ingested into PUDL.
"""

pudl_tables = {
    'eia860': eia860_pudl_tables,
    'eia861': (
        "service_territory_eia861",
        "balancing_authority_eia861",
        "sales_eia861",
        "advanced_metering_infrastructure_eia861",
        "demand_response_eia861",
        # "demand_side_management_eia861",
        # "distributed_generation_eia861",
        "distribution_systems_eia861",
        "dynamic_pricing_eia861",
        "green_pricing_eia861",
        "mergers_eia861",
        "net_metering_eia861",
        "non_net_metering_eia861",
        "operational_data_eia861",
        "reliability_eia861",
        "utility_data_eia861"
    ),
    'eia923': eia923_pudl_tables,
    'epacems': epacems_tables,
    'epaipm': epaipm_pudl_tables,
    'ferc1': ferc1_pudl_tables,
    'ferc714': (
        "respondent_id_ferc714",
        "id_certification_ferc714",
        "gen_plants_ba_ferc714",
        "demand_monthly_ba_ferc714",
        "net_energy_load_ba_ferc714",
        "adjacency_ba_ferc714",
        "interchange_ba_ferc714",
        "lambda_hourly_ba_ferc714",
        "lambda_description_ferc714",
        "description_pa_ferc714",
        "demand_forecast_pa_ferc714",
        "demand_hourly_pa_ferc714",
    ),
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
    'plants_steam_ferc1': ('construction_year', 'installation_year'),
    'plants_small_ferc1': ('construction_year', 'ferc_license_id'),
    'plants_hydro_ferc1': ('construction_year', 'installation_year',),
    'plants_pumped_storage_ferc1': ('construction_year', 'installation_year',),
    'hourly_emissions_epacems': ('facility_id', 'unit_id_epa',),
}
"""
dict: A dictionary containing tables (keys) and column names (values)
    containing integer - type columns whose null values need fixing.
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
    "eia860": {
        "title": "EIA Form 860",
        "path": "https://www.eia.gov/electricity/data/eia860/",
    },
    "eia861": {
        "title": "EIA Form 861",
        "path": "https://www.eia.gov/electricity/data/eia861/",
    },
    "eia923": {
        "title": "EIA Form 923",
        "path": "https://www.eia.gov/electricity/data/eia923/",
    },
    "eiawater": {
        "title": "EIA Water Use for Power",
        "path": "https://www.eia.gov/electricity/data/water/",
    },
    "epacems": {
        "title": "EPA Air Markets Program Data",
        "path": "https://ampd.epa.gov/ampd/",
    },
    "epaipm": {
        "title": "EPA Integrated Planning Model",
        "path": "https://www.epa.gov/airmarkets/national-electric-energy-data-system-needs-v6",
    },
    "ferc1": {
        "title": "FERC Form 1",
        "path": "https://www.ferc.gov/docs-filing/forms/form-1/data.asp",
    },
    "ferc714": {
        "title": "FERC Form 714",
        "path": "https://www.ferc.gov/docs-filing/forms/form-714/data.asp",
    },
    "ferceqr": {
        "title": "FERC Electric Quarterly Report",
        "path": "https://www.ferc.gov/docs-filing/eqr.asp",
    },
    "msha": {
        "title": "Mining Safety and Health Administration",
        "path": "https://www.msha.gov/mine-data-retrieval-system",
    },
    "phmsa": {
        "title": "Pipelines and Hazardous Materials Safety Administration",
        "path": "https://www.phmsa.dot.gov/data-and-statistics/pipeline/data-and-statistics-overview",
    },
    "pudl": {
        "title": "The Public Utility Data Liberation Project (PUDL)",
        "path": "https://catalyst.coop/pudl/",
        "email": "pudl@catalyst.coop",
    },
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
    'datapkg',
    'notebook',
]
"""list: A list of types of PUDL output formats."""


keywords_by_data_source = {
    'pudl': [
        'us', 'electricity',
    ],
    'eia860': [
        'electricity', 'electric', 'boiler', 'generator', 'plant', 'utility',
        'fuel', 'coal', 'natural gas', 'prime mover', 'eia860', 'retirement',
        'capacity', 'planned', 'proposed', 'energy', 'hydro', 'solar', 'wind',
        'nuclear', 'form 860', 'eia', 'annual', 'gas', 'ownership', 'steam',
        'turbine', 'combustion', 'combined cycle', 'eia',
        'energy information administration'
    ],
    'eia923': [
        'fuel', 'boiler', 'generator', 'plant', 'utility', 'cost', 'price',
        'natural gas', 'coal', 'eia923', 'energy', 'electricity', 'form 923',
        'receipts', 'generation', 'net generation', 'monthly', 'annual', 'gas',
        'fuel consumption', 'MWh', 'energy information administration', 'eia',
        'mercury', 'sulfur', 'ash', 'lignite', 'bituminous', 'subbituminous',
        'heat content'
    ],
    'epacems': [
        'epa', 'us', 'emissions', 'pollution', 'ghg', 'so2', 'co2', 'sox',
        'nox', 'load', 'utility', 'electricity', 'plant', 'generator', 'unit',
        'generation', 'capacity', 'output', 'power', 'heat content', 'mmbtu',
        'steam', 'cems', 'continuous emissions monitoring system', 'hourly'
        'environmental protection agency', 'ampd', 'air markets program data',
    ],
    'ferc1': [
        'electricity', 'electric', 'utility', 'plant', 'steam', 'generation',
        'cost', 'expense', 'price', 'heat content', 'ferc', 'form 1',
        'federal energy regulatory commission', 'capital', 'accounting',
        'depreciation', 'finance', 'plant in service', 'hydro', 'coal',
        'natural gas', 'gas', 'opex', 'capex', 'accounts', 'investment',
        'capacity'
    ],
    'epaipm': [
        'epaipm', 'integrated planning',
    ]
}
"""dict: A dictionary of datasets (keys) and keywords (values). """

ENTITY_TYPE_DICT = {
    'M': 'Municipal',
    'C': 'Cooperative',
    'R': 'Retail Power Marketer',
    'I': 'Investor Owned',
    'P': 'Political Subdivision',
    'T': 'Transmission',
    'S': 'State',
    'W': 'Wholesale Power Marketer',
    'F': 'Federal',
    'A': 'Municipal Mktg Authority',
    'G': 'Community Choice Aggregator',
    'D': 'Nonutility DSM Administrator',
    'B': 'Behind the Meter',
    'Q': 'Independent Power Producer',
    'IND': 'Industrial',
    'COM': 'Commercial',
    'PR': 'Private',  # Added by AES for OD table (Arbitrary moniker)
    'PO': 'Power Marketer',  # Added by AES for OD table
    'U': 'Unknown',  # Added by AES for OD table
    'O': 'Other'  # Added by AES for OD table
}

# Confirm these designations -- educated guess based on the form instructions
MOMENTARY_INTERRUPTION_DEF = {  # Added by AES for R table
    'L': 'Less than 1 minute',
    'F': 'Less than or equal to 5 minutes',
    'O': 'Other',
}

# https://www.eia.gov/electricity/data/eia411/#tabs_NERC-3
RECOGNIZED_NERC_REGIONS = [
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

CUSTOMER_CLASSES = [
    "commercial",
    "industrial",
    "direct_connection",
    "other",
    "residential",
    "total",
    "transportation"
]

TECH_CLASSES = [
    'pv',
    'storage_pv',
    'virtual_pv',
    'wind',
    'chp_cogen',
    'combustion_turbine',
    'fuel_cell',
    'hydro',
    'internal_combustion',
    'steam',
    'other',
    'total'
]

REVENUE_CLASSES = [
    'retail_sales',
    'unbundled',
    'delivery_customers',
    'sales_for_resale',
    'credits_or_adjustments',
    'other',
    'total'
]

RELIABILITY_STANDARDS = [
    'ieee_standard',
    'other_standard'
]

"""dict: A dictionary of datasets (keys) and keywords (values). """

column_dtypes = {
    "ferc1": {  # Obviously this is not yet a complete list...
        "construction_year": pd.Int64Dtype(),
        "installation_year": pd.Int64Dtype(),
        "plant_id_ferc1": pd.Int64Dtype(),
        "plant_id_pudl": pd.Int64Dtype(),
        "report_date": "datetime64[ns]",
        "report_year": pd.Int64Dtype(),
        "utility_id_ferc1": pd.Int64Dtype(),
        "utility_id_pudl": pd.Int64Dtype(),
    },
    "ferc714": {  # INCOMPLETE
        "demand_mwh": float,
        "demand_annual_mwh": float,
        "eia_code": pd.Int64Dtype(),
        "peak_demand_summer_mw": float,
        "peak_demand_winter_mw": float,
        "report_date": "datetime64[ns]",
        "respondent_id_ferc714": pd.Int64Dtype(),
        "respondent_name_ferc714": pd.StringDtype(),
        "respondent_type": pd.CategoricalDtype(categories=[
            "utility", "balancing_authority",
        ]),
        "timezone": pd.CategoricalDtype(categories=[
            "America/New_York", "America/Chicago", "America/Denver",
            "America/Los_Angeles", "America/Anchorage", "Pacific/Honolulu"]),
        "utc_datetime": "datetime64[ns]",
    },
    "epacems": {
        'state': pd.StringDtype(),
        'plant_id_eia': pd.Int64Dtype(),  # Nullable Integer
        'unitid': pd.StringDtype(),
        'operating_datetime_utc': "datetime64[ns]",
        'operating_time_hours': float,
        'gross_load_mw': float,
        'steam_load_1000_lbs': float,
        'so2_mass_lbs': float,
        'so2_mass_measurement_code': pd.StringDtype(),
        'nox_rate_lbs_mmbtu': float,
        'nox_rate_measurement_code': pd.StringDtype(),
        'nox_mass_lbs': float,
        'nox_mass_measurement_code': pd.StringDtype(),
        'co2_mass_tons': float,
        'co2_mass_measurement_code': pd.StringDtype(),
        'heat_content_mmbtu': float,
        'facility_id': pd.Int64Dtype(),  # Nullable Integer
        'unit_id_epa': pd.Int64Dtype(),  # Nullable Integer
    },
    "eia": {
        'actual_peak_demand_savings_mw': float,  # Added by AES for DR table
        'advanced_metering_infrastructure': float,  # Added by AES for AMI table
        # Added by AES for UD misc table
        'alternative_fuel_vehicle_2_activity': pd.BooleanDtype(),
        # Added by AES for UD misc table
        'alternative_fuel_vehicle_activity': pd.BooleanDtype(),
        'ash_content_pct': float,
        'ash_impoundment': pd.BooleanDtype(),
        'ash_impoundment_lined': pd.BooleanDtype(),
        # TODO: convert this field to more descriptive words
        'ash_impoundment_status': pd.StringDtype(),
        'associated_combined_heat_power': pd.BooleanDtype(),
        'automated_meter_reading': float,  # Added by AES for AMI table
        'backup_capacity_mw': float,  # Added by AES for NNM table
        'balancing_authority_code_eia': pd.CategoricalDtype(),
        'balancing_authority_id_eia': pd.Int64Dtype(),
        'balancing_authority_name_eia': pd.StringDtype(),
        'bga_source': pd.StringDtype(),
        'boiler_id': pd.StringDtype(),
        'bunded_activity': pd.BooleanDtype(),  # Added by AES for UD misc table
        'business_model': pd.CategoricalDtype(categories=[
            "retail", "energy_services"]),
        'buy_distribution_activity': pd.BooleanDtype(),  # Added by AES for UD misc table
        # Added by AES for UD misc table
        'buying_transmission_activity': pd.BooleanDtype(),
        'bypass_heat_recovery': pd.BooleanDtype(),
        # Added by AES for R table
        'caidi_w_major_event_days_minus_loss_of_service_minutes': float,
        'caidi_w_major_event_dats_minutes': float,  # Added by AES for R table
        'caidi_wo_major_event_days_minutes': float,  # Added by AES for R table
        'capacity_mw': float,  # Used by AES for NNM table
        'carbon_capture': pd.BooleanDtype(),
        'chlorine_content_ppm': float,
        'circuits_with_voltage_optimization': pd.Int64Dtype(),  # Added by AES for DS table
        'city': pd.StringDtype(),
        'cofire_fuels': pd.BooleanDtype(),
        'consumed_by_facility_mwh': float,  # Added by AES for OD table
        'consumed_by_respondent_without_charge_mwh': float,  # Added by AES for OD table
        'contact_firstname': pd.StringDtype(),
        'contact_firstname2': pd.StringDtype(),
        'contact_lastname': pd.StringDtype(),
        'contact_lastname2': pd.StringDtype(),
        'contact_title': pd.StringDtype(),
        'contact_title2': pd.StringDtype(),
        'contract_expiration_date': 'datetime64[ns]',
        'contract_type_code': pd.StringDtype(),
        'county': pd.StringDtype(),
        'county_id_fips': pd.StringDtype(),  # Must preserve leading zeroes
        'credits_or_adjustments': float,  # Added by AES for OD Revenue table
        'critical_peak_pricing': pd.BooleanDtype(),  # Added by AES for DP table
        'critical_peak_rebate': pd.BooleanDtype(),  # Added by AES for DP table
        'current_planned_operating_date': 'datetime64[ns]',
        'customers': float,  # pd.Int64Dtype(),  # Used by AES for NM table
        'customer_class': pd.CategoricalDtype(categories=CUSTOMER_CLASSES),
        'customer_incentives_cost': float,  # Added by AES for DR table
        'daily_digital_access_customers': float,  # Added by AES for AMI table
        'data_observed': pd.BooleanDtype(),  # Used by AES for OD table
        'deliver_power_transgrid': pd.BooleanDtype(),
        'delivery_customers': float,  # Added by AES for OD Revenue table
        'direct_load_control_customers': float,  # Added by AES for AMI table
        'distribution_activity': pd.BooleanDtype(),  # Added by AES for UD misc table
        'distribution_circuits': pd.Int64Dtype(),  # Added by AES for DS table
        'duct_burners': pd.BooleanDtype(),
        'energy_displaced_mwh': float,  # Added by AES for NM table
        'energy_savings_mwh': float,  # Added by AES for DR table
        'energy_served_ami_mwh': float,  # Added by AES for AMI table
        'energy_source_code': pd.StringDtype(),
        'energy_source_code_1': pd.StringDtype(),
        'energy_source_code_2': pd.StringDtype(),
        'energy_source_code_3': pd.StringDtype(),
        'energy_source_code_4': pd.StringDtype(),
        'energy_source_code_5': pd.StringDtype(),
        'energy_source_code_6': pd.StringDtype(),
        'energy_storage': pd.BooleanDtype(),
        # Modified by AES for Merger, OD, and R tables
        'entity_type': pd.CategoricalDtype(categories=ENTITY_TYPE_DICT.values()),
        'exchange_energy_delivered_mwh': float,  # Added by AES for OD table
        'exchange_energy_recieved_mwh': float,  # Added by AES for OD table
        'ferc_cogen_docket_no': pd.StringDtype(),
        'ferc_cogen_status': pd.BooleanDtype(),
        'ferc_exempt_wholesale_generator': pd.BooleanDtype(),
        'ferc_exempt_wholesale_generator_docket_no': pd.StringDtype(),
        'ferc_small_power_producer': pd.BooleanDtype(),
        'ferc_small_power_producer_docket_no': pd.StringDtype(),
        'fluidized_bed_tech': pd.BooleanDtype(),
        'fraction_owned': float,
        'fuel_consumed_for_electricity_mmbtu': float,
        'fuel_consumed_for_electricity_units': float,
        'fuel_consumed_mmbtu': float,
        'fuel_consumed_units': float,
        'fuel_cost_per_mmbtu': float,
        'fuel_group_code': pd.StringDtype(),
        'fuel_group_code_simple': pd.StringDtype(),
        'fuel_mmbtu_per_unit': float,
        'fuel_qty_units': float,
        # are fuel_type and fuel_type_code the same??
        # fuel_type includes 40 code-like things.. WAT, SUN, NUC, etc.
        'fuel_type': pd.StringDtype(),
        # from the boiler_fuel_eia923 table, there are 30 code-like things, like NG, BIT, LIG
        'fuel_type_code': pd.StringDtype(),
        'fuel_type_code_aer': pd.StringDtype(),
        'fuel_type_code_pudl': pd.StringDtype(),
        'furnished_without_charge_mwh': float,  # Added by AES for OD table
        'generation_activity': pd.BooleanDtype(),  # Added by AES for UD misc table
        # this is a mix of integer-like values (2 or 5) and strings like AUGSF
        'generator_id': pd.StringDtype(),
        'generators_number': pd.Int64Dtype(),  # Added by AES for NNM table
        # Added by AES for GP table (added green pricing prefix for now)
        'green_pricing_revenue': float,
        'grid_voltage_2_kv': float,
        'grid_voltage_3_kv': float,
        'grid_voltage_kv': float,
        'heat_content_mmbtu_per_unit': float,
        'highest_distribution_voltage_kv': float,  # Added by AES for R table
        'home_area_network': float,  # Added by AES for AMI table
        'inactive_accounts_included': pd.BooleanDtype(),  # Added by AES for R table
        'iso_rto_code': pd.StringDtype(),
        'latitude': float,
        'liquefied_natural_gas_storage': pd.BooleanDtype(),
        'longitude': float,
        'mercury_content_ppm': float,
        'merge_address': pd.StringDtype(),  # Added by AES for Mergers table
        'merge_city': pd.StringDtype(),  # Added by AES for Mergers table
        'merge_company': pd.StringDtype(),  # Added by AES for Mergers table
        'merge_date': 'datetime64[ns]',  # Added by AES for Mergers table
        'merge_state': pd.StringDtype(),  # Added by AES for Mergers table
        'merge_zip_4': pd.StringDtype(),  # Added by AES for Mergers table
        'merge_zip_5': pd.StringDtype(),  # Added by AES for Mergers tables
        'mine_id_msha': pd.Int64Dtype(),
        'mine_id_pudl': pd.Int64Dtype(),
        'mine_name': pd.StringDtype(),
        'mine_type_code': pd.StringDtype(),
        'minimum_load_mw': float,
        'moisture_content_pct': float,
        # Added by AES for R table
        'momentary_interruption_definition': pd.CategoricalDtype(categories=MOMENTARY_INTERRUPTION_DEF.values()),
        'multiple_fuels': pd.BooleanDtype(),
        'nameplate_power_factor': float,
        'natural_gas_delivery_contract_type_code': pd.StringDtype(),
        'natural_gas_local_distribution_company': pd.StringDtype(),
        'natural_gas_pipeline_name_1': pd.StringDtype(),
        'natural_gas_pipeline_name_2': pd.StringDtype(),
        'natural_gas_pipeline_name_3': pd.StringDtype(),
        'natural_gas_storage': pd.BooleanDtype(),
        'natural_gas_transport_code': pd.StringDtype(),
        'nerc_region': pd.CategoricalDtype(categories=RECOGNIZED_NERC_REGIONS),
        'nerc_regions_of_operation': pd.StringDtype(),  # Added by AES for UD nerc table
        'net_generation_mwh': float,  # Used by AES for OD table
        'net_metering': pd.BooleanDtype(),
        'net_power_exchanged_mwh': float,  # Added by AES for OD table
        'net_wheeled_power_mwh': float,  # Added by AES for OD table
        'new_parent': pd.StringDtype(),  # Added by AES for Mergers table
        'non_amr_ami': float,  # Added by AES for AMI table
        'nuclear_unit_id': pd.Int64Dtype(),
        'operates_generating_plant': pd.BooleanDtype(),  # Added by AES for UD misc table
        'operating_date': 'datetime64[ns]',
        'operating_switch': pd.StringDtype(),
        # TODO: double check this for early 860 years
        'operational_status': pd.StringDtype(),
        'operational_status_code': pd.StringDtype(),
        'original_planned_operating_date': 'datetime64[ns]',
        'other': float,  # Added by AES for OD Revenue table
        'other_combustion_tech': pd.BooleanDtype(),
        'other_costs': float,  # Added by AES for DR table
        'other_modifications_date': 'datetime64[ns]',
        'other_planned_modifications': pd.BooleanDtype(),
        'outages_recorded_automatically': pd.BooleanDtype(),  # Added by AES for R table
        'owner_city': pd.StringDtype(),
        'owner_name': pd.StringDtype(),
        'owner_state': pd.StringDtype(),
        'owner_street_address': pd.StringDtype(),
        'owner_utility_id_eia': pd.Int64Dtype(),
        'owner_zip_code': pd.StringDtype(),  # Must preserve leading zeroes.
        # we should transition these into readable codes, not a one letter thing
        'ownership_code': pd.StringDtype(),
        'pipeline_notes': pd.StringDtype(),
        'planned_derate_date': 'datetime64[ns]',
        'planned_energy_source_code_1': pd.StringDtype(),
        'planned_modifications': pd.BooleanDtype(),
        'planned_net_summer_capacity_derate_mw': float,
        'planned_net_summer_capacity_uprate_mw': float,
        'planned_net_winter_capacity_derate_mw': float,
        'planned_net_winter_capacity_uprate_mw': float,
        'planned_new_capacity_mw': float,
        'planned_new_prime_mover_code': pd.StringDtype(),
        'planned_repower_date': 'datetime64[ns]',
        'planned_retirement_date': 'datetime64[ns]',
        'planned_uprate_date': 'datetime64[ns]',
        'plant_id_eia': pd.Int64Dtype(),
        'plant_id_pudl': pd.Int64Dtype(),
        'plant_name_eia': pd.StringDtype(),
        'plants_reported_asset_manager': pd.BooleanDtype(),
        'plants_reported_operator': pd.BooleanDtype(),
        'plants_reported_other_relationship': pd.BooleanDtype(),
        'plants_reported_owner': pd.BooleanDtype(),
        'potential_peak_demand_savings_mw': float,  # Added by AES for DR table
        'pulverized_coal_tech': pd.BooleanDtype(),
        'previously_canceled': pd.BooleanDtype(),
        'primary_transportation_mode_code': pd.StringDtype(),
        'primary_purpose_naics_id': pd.Int64Dtype(),
        'prime_mover_code': pd.StringDtype(),
        # Added by AES for NM table; used for NM & NNM table
        'pv_current_flow_type': pd.CategoricalDtype(categories=['AC', 'DC']),
        'real_time_pricing_program': pd.BooleanDtype(),  # Added by AES for DP table
        'rec_revenue': float,  # Added by AES for GP table
        'rec_sales_mwh': float,  # Added by AES for GP table
        'regulatory_status_code': pd.StringDtype(),
        'report_date': 'datetime64[ns]',
        'retail_marketing_activity': pd.BooleanDtype(),  # Added by AES for UD misc table
        'retail_sales': float,  # Added by AES for OD Revenue table
        'retail_sales_mwh': float,  # Added by AES for OD table
        'retirement_date': 'datetime64[ns]',
        # Added by AES for OD table
        'revenue_class': pd.CategoricalDtype(categories=REVENUE_CLASSES),
        'rto_iso_lmp_node_id': pd.StringDtype(),
        'rto_iso_location_wholesale_reporting_id': pd.StringDtype(),
        'rtos_of_operation': pd.StringDtype(),  # Added by AES for UD rto table
        # Added by AES for R table
        'saidi_w_major_event_dats_minus_loss_of_service_minutes': float,
        'saidi_w_major_event_days_minutes': float,  # Added by AES for R table
        'saidi_wo_major_event_days_minutes': float,  # Added by AES for R table
        'saifi_w_major_event_days_customers': float,  # Added by AES for R table
        # Added by AES for R table
        'saifi_w_major_event_days_minus_loss_of_service_customers': float,
        'saifi_wo_major_event_days_customers': float,  # Added by AES for R table
        'sales_for_resale': float,  # Added by AES for OD Revenue table
        'sales_for_resale_mwh': float,  # Added by AES for OD table
        'sales_mwh': float,
        'sales_revenue': float,  # Added sales prefix for now
        'secondary_transportation_mode_code': pd.StringDtype(),
        'sector_id': pd.Int64Dtype(),
        'sector_name': pd.StringDtype(),
        'service_type': pd.CategoricalDtype(categories=[
            "bundled", "energy", "delivery",
        ]),
        'sold_to_utility_mwh': float,  # Added by AES for NM table
        'solid_fuel_gasification': pd.BooleanDtype(),
        # Added by AES for R table
        'standard': pd.CategoricalDtype(categories=RELIABILITY_STANDARDS),
        'startup_source_code_1': pd.StringDtype(),
        'startup_source_code_2': pd.StringDtype(),
        'startup_source_code_3': pd.StringDtype(),
        'startup_source_code_4': pd.StringDtype(),
        'state': pd.StringDtype(),
        'state_id_fips': pd.StringDtype(),  # Must preserve leading zeroes
        'street_address': pd.StringDtype(),
        'stoker_tech': pd.BooleanDtype(),
        'storage_capacity_mw': float,  # Added by AES for NM table
        'storage_customers': pd.Int64Dtype(),  # Added by AES for NM table
        'subcritical_tech': pd.BooleanDtype(),
        'sulfur_content_pct': float,
        'summer_capacity_mw': float,
        # TODO: check if there is any data pre-2016
        'summer_estimated_capability_mw': float,
        'summer_peak_demand_mw': float,  # Added by AES for OD table
        'supercritical_tech': pd.BooleanDtype(),
        'supplier_name': pd.StringDtype(),
        'switch_oil_gas': pd.BooleanDtype(),
        'syncronized_transmission_grid': pd.BooleanDtype(),
        # Added by AES for NM table (might want to consider merging with another fuel label)
        'tech_class': pd.CategoricalDtype(categories=TECH_CLASSES),
        'technology_description': pd.StringDtype(),
        'time_cold_shutdown_full_load_code': pd.StringDtype(),
        'time_of_use_pricing_program': pd.BooleanDtype(),  # Added by AES for DP table
        'timezone': pd.StringDtype(),
        'topping_bottoming_code': pd.StringDtype(),
        'total': float,  # Added by AES for OD Revenue table
        'total_meters': float,  # Added by AES for AMI table
        'total_disposition_mwh': float,  # Added by AES for OD table
        'total_energy_losses_mwh': float,  # Added by AES for OD table
        'total_sources_mwh': float,  # Added by AES for OD table
        'transmission': float,  # Added by AES for OD Revenue table
        'transmission_activity': pd.BooleanDtype(),  # Added by AES for UD misc table
        'transmission_by_other_losses_mwh': float,  # Added by AES for OD table
        'transmission_distribution_owner_id': pd.Int64Dtype(),
        'transmission_distribution_owner_name': pd.StringDtype(),
        'transmission_distribution_owner_state': pd.StringDtype(),
        'turbines_inverters_hydrokinetics': float,
        'turbines_num': pd.Int64Dtype(),  # TODO: check if any turbines show up pre-2016
        'ultrasupercritical_tech': pd.BooleanDtype(),
        'unbundled_revenues': float,  # Added by AES for OD table
        'unit_id_eia': pd.StringDtype(),
        'unit_id_pudl': pd.Int64Dtype(),
        'uprate_derate_completed_date': 'datetime64[ns]',
        'uprate_derate_during_year': pd.BooleanDtype(),
        'utility_attn': pd.StringDtype(),
        'utility_id_eia': pd.Int64Dtype(),
        'utility_id_pudl': pd.Int64Dtype(),
        'utility_name_eia': pd.StringDtype(),
        'utility_owned_capacity_mw': float,  # Added by AES for NNM table
        'utility_pobox': pd.StringDtype(),
        'utility_zip4': pd.StringDtype(),
        'variable_peak_pricing_program': pd.BooleanDtype(),  # Added by AES for DP table
        'virtual_capacity_mw': float,  # Added by AES for NM table
        'virtual_customers': pd.Int64Dtype(),  # Added by AES for NM table
        'water_heater': pd.Int64Dtype(),  # Added by AES for DR table
        'water_source': pd.StringDtype(),
        'wheeled_power_delivered_mwh': float,  # Added by AES for OD table
        'wheeled_power_recieved_mwh': float,  # Added by AES for OD table
        # Added by AES for UD misc table
        'wholesale_marketing_activity': pd.BooleanDtype(),
        'wholesale_power_purchases_mwh': float,  # Added by AES for OD table
        'winter_capacity_mw': float,
        'winter_estimated_capability_mw': float,
        'winter_peak_demand_mw': float,  # Added by AES for OD table
        # 'with_med': float,  # Added by AES for R table
        # 'with_med_minus_los': float,  # Added by AES for R table
        # 'without_med': float,  # Added by AES for R table
        'zip_code': pd.StringDtype(),
    },
    'depreciation': {
        'utility_id_ferc1': pd.Int64Dtype(),
        'utility_id_pudl': pd.Int64Dtype(),
        'plant_id_pudl': pd.Int64Dtype(),
        # 'plant_name': pd.StringDtype(),
        'report_year': pd.Int64Dtype(),
        'report_date': 'datetime64[ns]',
        'common': pd.BooleanDtype(),
        'plant_balance': float,
        'book_reserve': float,
        'unaccrued_balance': float,
        'reserve_pct': float,
        # 'survivor_curve_type': pd.StringDtype(),
        'service_life_avg': float,
        'net_salvage_pct': float,
        'net_salvage_num_or_pct': pd.BooleanDtype(),
        'net_removal': float,
        'net_removal_pct': float,
        'remaining_life_avg': float,
        'retirement_date': 'datetime64[ns]',
        'depreciation_annual_epxns': float,
        'depreciation_annual_pct': float,
        'depreciation_annual_num_or_pct': pd.BooleanDtype(),
        # 'data_source': pd.StringDtype(),
    }
}
