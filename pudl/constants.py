"""
A warehouse for constant values required to initilize the PUDL Database.

This constants module stores and organizes a bunch of constant values which are
used throughout PUDL to populate static lists within the DB or for data
cleaning purposes.

We may want to migrate these values into a Data Package as specified by:

http://frictionlessdata.io/guides/data-package/
"""

# These imports are necessary for the DBF to SQL type map.
from sqlalchemy import String, Date, Float, Integer
from sqlalchemy import Boolean, Text, Float, DateTime
from pandas import DataFrame

######################################################################
# Constants used within the pudl.py module.
######################################################################
prime_movers = ['steam_turbine', 'gas_turbine', 'hydro', 'internal_combustion',
                'solar_pv', 'wind_turbine']

rto_iso = {
    'CAISO': 'California ISO',
    'ERCOT': 'Electric Reliability Council of Texas',
    'MISO': 'Midcontinent ISO',
    'ISO-NE': 'ISO New England',
    'NYISO': 'New York ISO',
    'PJM': 'PJM Interconnection',
    'SPP': 'Southwest Power Pool'
}

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

# Construct a dictionary mapping a canonical fuel name to a list of strings
# which are used to represent that fuel in the FERC Form 1 Reporting. Case is
# ignored, as all fuel strings can be converted to a lower case in the data
# set.
ferc1_coal_strings = \
    ['coal', 'coal-subbit', 'lignite', 'coal(sb)', 'coal (sb)', 'coal-lignite',
     'coke', 'coa', 'lignite/coal', 'coal - subbit', 'coal-subb', 'coal-sub',
     'coal-lig', 'coal-sub bit', 'coals', 'ciak', 'petcoke']

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
     'oli', 'oil (#6)']

ferc1_gas_strings = \
    ['gas', 'methane', 'natural gas', 'blast gas', 'gas mcf',
     'propane', 'prop', 'natural  gas', 'nat.gas', 'nat gas',
     'nat. gas', 'natl gas', 'ga', 'gas`', 'syngas', 'ng', 'mcf',
     'blast gaa', 'nat  gas', 'gac', 'syngass', 'prop.', 'natural']

ferc1_nuke_strings = \
    ['nuclear', 'grams of uran', 'grams of', 'grams of  ura',
     'grams', 'nucleur', 'nulear', 'nucl', 'nucleart']

ferc1_biomass_strings = \
    ['switchgrass', 'wood waste', 'woodchips', 'biomass', 'wood', 'wood chips']

ferc1_waste_strings = ['tires', 'tire', 'refuse']

ferc1_steam_strings = ['steam', 'purch steam', 'purch. steam']

# There are also a bunch of other weird and hard to categorize strings
# that I don't know what to do with... hopefully they constitute only a
# small fraction of the overall generation.

ferc1_fuel_strings = {'coal': ferc1_coal_strings,
                      'gas': ferc1_gas_strings,
                      'oil': ferc1_oil_strings,
                      'nuke': ferc1_nuke_strings,
                      'biomass': ferc1_biomass_strings,
                      'waste': ferc1_waste_strings,
                      'steam': ferc1_steam_strings
                      }

# Similarly, dictionary for cleaning up fuel unit strings
ferc1_ton_strings = ['toms', 'taons', 'tones', 'col-tons', 'toncoaleq', 'coal',
                     'tons coal eq', 'coal-tons', 'ton', 'tons', 'tons coal',
                     'coal-ton', 'tires-tons']

ferc1_mcf_strings = \
    ['mcf', "mcf's", 'mcfs', 'mcf.', 'gas mcf', '"gas" mcf', 'gas-mcf',
     'mfc', 'mct', ' mcf', 'msfs', 'mlf', 'mscf', 'mci', 'mcl', 'mcg',
     'm.cu.ft.']

ferc1_bbl_strings = \
    ['barrel', 'bbls', 'bbl', 'barrels', 'bbrl', 'bbl.', 'bbls.',
     'oil 42 gal', 'oil-barrels', 'barrrels', 'bbl-42 gal',
     'oil-barrel', 'bb.', 'barrells', 'bar', 'bbld', 'oil- barrel',
     'barrels    .', 'bbl .', 'barels', 'barrell', 'berrels', 'bb',
     'bbl.s', 'oil-bbl', 'bls', 'bbl:', 'barrles', 'blb', 'propane-bbl']

ferc1_gal_strings = ['gallons', 'gal.', 'gals', 'gals.', 'gallon', 'gal']

ferc1_1kgal_strings = ['oil(1000 gal)', 'oil(1000)', 'oil (1000)', 'oil(1000']

ferc1_gramsU_strings = ['gram', 'grams', 'gm u', 'grams u235', 'grams u-235',
                        'grams of uran', 'grams: u-235', 'grams:u-235',
                        'grams:u235', 'grams u308', 'grams: u235', 'grams of']

ferc1_kgU_strings = \
    ['kg of uranium', 'kg uranium', 'kilg. u-235', 'kg u-235',
     'kilograms-u23', 'kg', 'kilograms u-2', 'kilograms', 'kg of']

ferc1_mmbtu_strings = ['mmbtu', 'mmbtus',
                       "mmbtu's", 'nuclear-mmbtu', 'nuclear-mmbt']

ferc1_mwdth_strings = \
    ['mwd therman', 'mw days-therm', 'mwd thrml', 'mwd thermal',
     'mwd/mtu', 'mw days', 'mwdth', 'mwd', 'mw day']

ferc1_mwhth_strings = ['mwh them', 'mwh threm',
                       'nwh therm', 'mwhth', 'mwh therm', 'mwh']

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

# Categorizing the strings from the FERC Form 1 Plant Kind (plant_kind) field
# into lists. There are many strings that weren't categorized,
# including variants on Steam. Solar and Solar Project also were not
# classified as these do not indicate if they are solar thermal or
# photovoltaic.

ferc1_plant_kind_coal = ['Coal']

ferc1_plant_kind_combustion_turbine = \
    ['Combustion Turbine', 'GT', 'Gas Turbine',
     'Gas Turbine # 1', 'Gas turbine', 'Gas Turbine (Note 1)',
     'Gas Turbines', 'Simple Cycle', 'COMBUSTION TURBINE',
     'COMB.TURB.PEAK.UNITS', 'GAS TURBINE', 'Combustion turbine',
     'Com Turbine Peaking', 'Gas Turbine Peaking', 'Comb Turb Peaking',
     'COMBUSTINE TURBINE', 'Comb. Turine', 'Conbustion Turbine',
     'Combustine Turbine', 'Gas Turbine (Leased)', 'Combustion Tubine',
     'Gas Turb', 'Gas Turbine Peaker', 'GTG/Gas', 'Simple Cycle Turbine',
     'GAS-TURBINE', 'Gas Turbine-Simple', 'Gas Turbine - Note 1',
     'Gas Turbine #1', 'SIMPLE CYCLE', 'GasTurbine', 'CombustionTurbine',
     'Gas Turbine (2)', 'Comb Turb Peak Units', 'JET ENGINE']

ferc1_plant_kind_combined_cycle = \
    ['COMBINED CYCLE', 'Combined Cycle', 'Combined',
     'GAS TURB. & HEAT REC', 'Combined cycle', 'Com. Cyc', 'Com. Cycle',
     'GAS TURB-COMBINED CY', 'Combined Cycle CTG', 'Combined Cycle - 40%',
     'Com Cycle Gas Turb', 'Combined Cycle Oper', 'Gas Turb/Comb. Cyc',
     'Combine Cycle', 'CC', 'Comb. Cycle', 'Gas Turb-Combined Cy']

ferc1_plant_kind_nuke = ['Nuclear', 'NUCLEAR', 'Nuclear (3)']

ferc1_plant_kind_geothermal = ['Steam - Geothermal']

ferc_1_plant_kind_internal_combustion = \
    ['IC', 'Internal Combustion',
     'Diesel Turbine', 'Int Combust (Note 1)', 'Int. Combust (Note1)',
     'INT.COMBUSTINE', 'Comb. Cyc', 'Internal Comb', 'DIESEL', 'Diesel Engine',
     'INTERNAL COMBUSTION', 'Int Combust - Note 1', 'Int. Combust - Note1',
     'Internal Comb Recip', 'Reciprocating Engine', 'Comb. Turbine']

ferc1_plant_kind_wind = ['Wind', 'Wind Energy',
                         'Wind Turbine', 'Wind - Turbine']

ferc1_plant_kind_photovoltaic = ['Solar Photovoltaic', 'Photovoltaic']

ferc1_plant_kind_solar_thermal = ['Solar Thermal']

# Making a dictionary of lists from the lists of plant_fuel strings to create
# a dictionary of plant fuel lists.

ferc1_plant_kind_strings = {
    'coal': ferc1_plant_kind_coal,
    'combustion_turbine': ferc1_plant_kind_combustion_turbine,
    'combined_cycle': ferc1_plant_kind_combined_cycle,
    'nuclear': ferc1_plant_kind_nuke,
    'geothermal': ferc1_plant_kind_geothermal,
    'internal_combustion': ferc_1_plant_kind_internal_combustion,
    'wind': ferc1_plant_kind_wind,
    'photovoltaic': ferc1_plant_kind_photovoltaic,
    'solar_thermal': ferc1_plant_kind_solar_thermal
}

# Categorizing the strings from the FERC Form 1 Type of Plant Construction
# (type_const) field into lists.
# There are many strings that weren't categorized, including crosses between
# conventional and outdoor, PV, wind, combined cycle, and internal combustion.
# The lists are broken out into the two types specified in Form 1:
# conventional and outdoor. These lists are inclusive so that variants of
# conventional (e.g. "conventional full") and outdoor (e.g. "outdoor full"
# and "outdoor hrsg") are included.

ferc1_construction_type_outdoor = \
    ['Outdoor', 'OUTDOOR BOILER', 'Full Outdoor', 'Outdoor Boiler',
     'Outdoor Boilers', 'Outboilers', 'Fuel outdoor', 'FULL OUTDOOR',
     'Outdoors', 'OUTDOOR', 'Boiler Outdoor& Full', 'Boiler Outdoor&Full',
     'Outdoor Boiler& Full', 'Full -Outdoor', 'Outdoor Steam',
     'Outdoor boiler', 'OB', 'Outdoor Automatic', 'OUTDOOR REPOWER',
     'FULL OUTDOOR BOILER', 'FO', 'Outdoor Boiler & Ful', 'Full-Outdoor',
     'Fuel Outdoor', 'Outoor', 'outdoor', 'Outdoor  Boiler&Full',
     'Boiler Outdoor &Full', 'Outdoor Boiler &Full', 'Boiler Outdoor & Ful',
     'Outdoor-Boiler', 'Outdoor - Boiler', 'Outdoor Const.',
     '4 Outdoor Boilers', '3 Outdoor Boilers', 'Full outdoor', 'Full Outdoors',
     'Full Oudoors', 'Outdoor (Auto Oper)', 'Outside Boiler',
     'Outdoor Boiler&Full', 'OUTDOOR HRSG', 'Outdoor HRSG']

ferc1_construction_type_conventional = \
    ['Conventional', 'CONVENTIONAL', 'Conventional Boiler', 'Conv-B',
     'Conventionall', 'CONVENTION', 'conventional', 'Coventional',
     'Conven Full Boiler', 'C0NVENTIONAL', 'Conventtional', 'Convential']

# Making a dictionary of lists from the lists of type_const strings to create
# a dictionary of construction type lists.

ferc1_construction_type_strings = {
    'outdoor': ferc1_construction_type_outdoor,
    'conventional': ferc1_construction_type_conventional
}

# Dictionary mapping DBF files (w/o .DBF file extension) to DB table names
ferc1_dbf2tbl = {
    'F1_1':  'f1_respondent_id',    # GET THIS ONE
    'F1_2':  'f1_acb_epda',
    'F1_3':  'f1_accumdepr_prvsn',
    'F1_4':  'f1_accumdfrrdtaxcr',
    'F1_5':  'f1_adit_190_detail',
    'F1_6':  'f1_adit_190_notes',
    'F1_7':  'f1_adit_amrt_prop',
    'F1_8':  'f1_adit_other',
    'F1_9':  'f1_adit_other_prop',
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
    'F1_31': 'f1_fuel',            # GET THIS ONE
    'F1_32': 'f1_general_info',
    'F1_33': 'f1_gnrt_plant',      # GET THIS ONE
    'F1_34': 'f1_important_chg',
    'F1_35': 'f1_incm_stmnt_2',
    'F1_36': 'f1_income_stmnt',
    'F1_37': 'f1_miscgen_expnelc',
    'F1_38': 'f1_misc_dfrrd_dr',
    'F1_39': 'f1_mthly_peak_otpt',
    'F1_40': 'f1_mtrl_spply',
    'F1_41': 'f1_nbr_elc_deptemp',
    'F1_42': 'f1_nonutility_prop',
    'F1_43': 'f1_note_fin_stmnt',
    'F1_44': 'f1_nuclear_fuel',
    'F1_45': 'f1_officers_co',
    'F1_46': 'f1_othr_dfrrd_cr',
    'F1_47': 'f1_othr_pd_in_cptl',
    'F1_48': 'f1_othr_reg_assets',
    'F1_49': 'f1_othr_reg_liab',
    'F1_50': 'f1_overhead',
    'F1_51': 'f1_pccidica',
    'F1_52': 'f1_plant_in_srvce',  # GET THIS ONE
    'F1_53': 'f1_pumped_storage',  # GET THIS ONE
    'F1_54': 'f1_purchased_pwr',  # GET THIS ONE
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
    'F1_70': 'f1_work',            # GET THIS ONE
    'F1_71': 'f1_xmssn_adds',      # GET THIS ONE
    'F1_72': 'f1_xmssn_elc_bothr',
    'F1_73': 'f1_xmssn_elc_fothr',
    'F1_74': 'f1_xmssn_line',
    'F1_75': 'f1_xtraordnry_loss',
    'F1_76': 'f1_codes_val',
    'F1_77': 'f1_sched_lit_tbl',  # GET THIS ONE
    'F1_78': 'f1_audit_log',
    'F1_79': 'f1_col_lit_tbl',    # GET THIS ONE
    'F1_80': 'f1_load_file_names',
    'F1_81': 'f1_privilege',
    'F1_82': 'f1_sys_error_log',
    'F1_83': 'f1_unique_num_val',
    'F1_84': 'f1_row_lit_tbl',    # GET THIS ONE
    'F1_85': 'f1_footnote_data',
    'F1_86': 'f1_hydro',          # GET THIS ONE
    'F1_87': 'f1_footnote_tbl',
    'F1_88': 'f1_ident_attsttn',
    'F1_89': 'f1_steam',          # GET THIS ONE
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
    'F1_398_ANCL_PS': 'f1_398_ancl_ps',  # GET THIS ONE
    'F1_399_MTH_PEAK': 'f1_399_mth_peak',
    'F1_400_SYS_PEAK': 'f1_400_sys_peak',
    'F1_400A_ISO_PEAK': 'f1_400a_iso_peak',
    'F1_429_TRANS_AFF': 'f1_429_trans_aff',
    'F1_ALLOWANCES_NOX': 'f1_allowances_nox',
    'F1_CMPINC_HEDGE_A': 'f1_cmpinc_hedge_a',
    'F1_CMPINC_HEDGE': 'f1_cmpinc_hedge',
    'F1_EMAIL': 'f1_email',
    'F1_FREEZE': 'f1_freeze',
    'F1_PINS': 'f1_pins',
    'F1_RG_TRN_SRV_REV': 'f1_rg_trn_srv_rev',
    'F1_S0_CHECKS': 'f1_s0_checks',
    'F1_S0_FILING_LOG': 'f1_s0_filing_log',  # GET THIS ONE
    'F1_SECURITY': 'f1_security'
}
# Invert the map above so we can go either way as needed
ferc1_tbl2dbf = {v: k for k, v in ferc1_dbf2tbl.items()}

# This dictionary maps the strings which are used to denote field types in the
# DBF objects to the corresponding generic SQLAlchemy Column types:
# These definitions come from a combination of the dbfread example program
# dbf2sqlite and this DBF file format documentation page:
# http://www.dbase.com/KnowledgeBase/int/db7_file_fmt.htm
# Un-mapped types left as 'XXX' which should obviously make an error...
dbf_typemap = {
    'C': String,
    'D': Date,
    'F': Float,
    'I': Integer,
    'L': Boolean,
    'M': Text,  # 10 digit .DBT block number, stored as a string...
    'N': Float,
    'T': DateTime,
    'B': 'XXX',  # .DBT block number, binary string
    '@': 'XXX',  # Timestamp... Date = Julian Day, Time is in milliseconds?
    '+': 'XXX',  # Autoincrement (e.g. for IDs)
    'O': 'XXX',  # Double, 8 bytes
    'G': 'XXX',  # OLE 10 digit/byte number of a .DBT block, stored as string
    '0': 'XXX'  # Integer? based on dbf2sqlite mapping
}

# We still don't understand the primary keys for these tables, and so they
# can't be inserted yet...
dbfs_bad_pk = ['F1_84', 'F1_S0_FILING_LOG']

# These are the FERC Form 1 DB tables that we're focusing on initially.
ferc1_default_tables = ['f1_respondent_id',
                        'f1_fuel',
                        'f1_steam',
                        'f1_gnrt_plant',
                        'f1_hydro',
                        'f1_pumped_storage',
                        'f1_plant_in_srvce',
                        'f1_purchased_pwr',
                        'f1_accumdepr_prvsn']

# This is the set of tables which have been successfully integrated into PUDL:
ferc1_pudl_tables = ['f1_fuel',
                     'f1_steam',
                     'f1_hydro',
                     'f1_plant_in_srvce',
                     'f1_pumped_storage',
                     'f1_accumdepr_prvsn',
                     'f1_purchased_pwr']


# This is the full set of tables that currently ingestible by the ferc1 DB:
ferc1_working_tables = ['f1_respondent_id',
                        'f1_fuel',
                        'f1_steam',
                        'f1_gnrt_plant',
                        'f1_hydro',
                        'f1_pumped_storage',
                        'f1_plant_in_srvce',
                        'f1_purchased_pwr',
                        'f1_accumdepr_prvsn']

# The full set of years we expect to be able to pull into the FERC1 DB.
ferc1_working_years = range(2004, 2016)

# The full set of years we currently expect to be able to ingest from EIA923.
eia923_working_years = range(2014, 2017)

# This is the list of EIA923 tables that can be successfully pulled into PUDL
eia923_pudl_tables = ['plant_info_eia923',
                      'generation_fuel_eia923',
                      'boiler_fuel_eia923',
                      'generation_eia923',
                      'fuel_receipts_costs_eia923']
# 'generation_eia923',
# ,
# 'stocks_eia923']

# these are tabname inputs mapping to excel spreadsheet tabs
pagemap_eia923 = DataFrame.from_records(
    [('generation_fuel', 0, 5),
     ('stocks', 1, 5),
     ('boiler_fuel', 2, 5),
     ('generator', 3, 5),
     ('fuel_receipts_costs', 4, 4),
     ('plant_frame', 5, 4)],
    columns=['page', 'sheetname', 'skiprows'],
    index='page')

tab_map_eia923 = DataFrame.from_records([
    (2009, 0, 1, 2, 3, 4),
    (2010, 0, 1, 2, 3, 4),
    (2011, 0, 1, 2, 3, 4),
    (2012, 0, 1, 2, 3, 4),
    (2013, 0, 1, 2, 3, 4),
    (2014, 0, 1, 2, 3, 4),
    (2015, 0, 1, 2, 3, 4),
    (2016, 0, 1, 2, 3, 4)],
    columns=['year_index', 'gen_fuel', 'stocks', 'boiler_fuel', 'generator',
             'fuel_receipts_costs'],
    index='year_index')

skiprows_eia923 = DataFrame.from_records([
    (2008, 7, 7, 7, 7, 7),
    (2009, 7, 7, 7, 7, 6),
    (2010, 7, 7, 7, 7, 7),
    (2011, 5, 5, 5, 5, 4),
    (2012, 5, 5, 5, 5, 4),
    (2013, 5, 5, 5, 5, 4),
    (2014, 5, 5, 5, 5, 4),
    (2015, 5, 5, 5, 5, 4),
    (2016, 5, 5, 5, 5, 4)],
    columns=['year_index', 'gen_fuel', 'stocks', 'boiler_fuel', 'generator',
             'fuel_receipts_costs'],
    index='year_index')

gen_fuel_map_eia923 = DataFrame.from_records([
    (2009, 'plant_id', 'combined_heat_power_plant', 'nuclear_unit_i_d',
     'plant_name', 'operator_name', 'operator_id', 'state', 'census_region',
     'nerc_region', 'reserved', 'naics_code', 'eia_sector_number',
     'sector_name', 'reported_prime_mover', 'reported_fuel_type_code',
     'aer_fuel_type_code', 'reserved', 'reserved', 'physical_unit_label',
     'quantity_jan', 'quantity_feb', 'quantity_mar', 'quantity_apr',
     'quantity_may', 'quantity_jun', 'quantity_jul', 'quantity_aug',
     'quantity_sep', 'quantity_oct', 'quantity_nov', 'quantity_dec',
     'elec_quantity_jan', 'elec_quantity_feb', 'elec_quantity_mar',
     'elec_quantity_apr', 'elec_quantity_may', 'elec_quantity_jun',
     'elec_quantity_jul', 'elec_quantity_aug', 'elec_quantity_sep',
     'elec_quantity_oct', 'elec_quantity_nov', 'elec_quantity_dec',
     'mmbtu_per_unit_jan', 'mmbtu_per_unit_feb', 'mmbtu_per_unit_mar',
     'mmbtu_per_unit_apr', 'mmbtu_per_unit_may', 'mmbtu_per_unit_jun',
     'mmbtu_per_unit_jul', 'mmbtu_per_unit_aug', 'mmbtu_per_unit_sep',
     'mmbtu_per_unit_oct', 'mmbtu_per_unit_nov', 'mmbtu_per_unit_dec',
     'tot_mmbtu_jan', 'tot_mmbtu_feb', 'tot_mmbtu_mar', 'tot_mmbtu_apr',
     'tot_mmbtu_may', 'tot_mmbtu_jun', 'tot_mmbtu_jul', 'tot_mmbtu_aug',
     'tot_mmbtu_sep', 'tot_mmbtu_oct', 'tot_mmbtu_nov', 'tot_mmbtu_dec',
     'elec_mmbtus_jan', 'elec_mmbtus_feb',	'elec_mmbtus_mar',
     'elec_mmbtus_apr', 'elec_mmbtus_may', 'elec_mmbtus_jun',
     'elec_mmbtus_jul', 'elec_mmbtus_aug', 'elec_mmbtus_sep',
     'elec_mmbtus_oct', 'elec_mmbtus_nov', 'elec_mmbtus_dec', 'netgen_jan',
     'netgen_feb', 'netgen_mar', 'netgen_apr', 'netgen_may', 'netgen_jun',
     'netgen_jul', 'netgen_aug', 'netgen_sep', 'netgen_oct', 'netgen_nov',
     'netgen_dec', 'total_fuel_consumption_quantity',
     'electric_fuel_consumption_quantity', 'total_fuel_consumption_mmbtus',
     'elec_fuel_consumption_mmbtus', 'net_generation_megawatthours', 'year'),
    (2010, 'plant_id', 'combined_heat_power_plant', 'nuclear_unit_i_d',
     'plant_name', 'operator_name', 'operator_id', 'state', 'census_region',
     'nerc_region', 'reserved', 'naics_code', 'eia_sector_number',
     'sector_name', 'reported_prime_mover', 'reported_fuel_type_code',
     'aer_fuel_type_code', 'reserved', 'reserved', 'physical_unit_label',
     'quantity_jan', 'quantity_feb', 'quantity_mar', 'quantity_apr',
     'quantity_may', 'quantity_jun', 'quantity_jul', 'quantity_aug',
     'quantity_sep', 'quantity_oct', 'quantity_nov', 'quantity_dec',
     'elec_quantity_jan', 'elec_quantity_feb', 'elec_quantity_mar',
     'elec_quantity_apr', 'elec_quantity_may', 'elec_quantity_jun',
     'elec_quantity_jul', 'elec_quantity_aug', 'elec_quantity_sep',
     'elec_quantity_oct', 'elec_quantity_nov', 'elec_quantity_dec',
     'mmbtu_per_unit_jan', 'mmbtu_per_unit_feb', 'mmbtu_per_unit_mar',
     'mmbtu_per_unit_apr', 'mmbtu_per_unit_may', 'mmbtu_per_unit_jun',
     'mmbtu_per_unit_jul', 'mmbtu_per_unit_aug', 'mmbtu_per_unit_sep',
     'mmbtu_per_unit_oct', 'mmbtu_per_unit_nov', 'mmbtu_per_unit_dec',
     'tot_mmbtu_jan', 'tot_mmbtu_feb', 'tot_mmbtu_mar', 'tot_mmbtu_apr',
     'tot_mmbtu_may', 'tot_mmbtu_jun', 'tot_mmbtu_jul', 'tot_mmbtu_aug',
     'tot_mmbtu_sep', 'tot_mmbtu_oct', 'tot_mmbtu_nov', 'tot_mmbtu_dec',
     'elec_mmbtus_jan', 'elec_mmbtus_feb', 'elec_mmbtus_mar',
     'elec_mmbtus_apr', 'elec_mmbtus_may', 'elec_mmbtus_jun',
     'elec_mmbtus_jul', 'elec_mmbtus_aug', 'elec_mmbtus_sep',
     'elec_mmbtus_oct', 'elec_mmbtus_nov', 'elec_mmbtus_dec', 'netgen_jan',
     'netgen_feb', 'netgen_mar', 'netgen_apr', 'netgen_may', 'netgen_jun',
     'netgen_jul', 'netgen_aug', 'netgen_sep', 'netgen_oct', 'netgen_nov',
     'netgen_dec', 'total_fuel_consumption_quantity',
     'electric_fuel_consumption_quantity', 'total_fuel_consumption_mmbtus',
     'elec_fuel_consumption_mmbtus', 'net_generation_megawatthours', 'year'),
    (2011, 'plant_id', 'combined_heat_power_plant', 'nuclear_unit_id',
     'plant_name', 'operator_name', 'operator_id', 'state', 'census_region',
     'nerc_region', 'reserved', 'naics_code', 'eia_sector_number',
     'sector_name', 'reported_prime_mover', 'reported_fuel_type_code',
     'aer_fuel_type_code', 'reserved', 'reserved', 'physical_unit_label',
     'quantity_jan', 'quantity_feb', 'quantity_mar', 'quantity_apr',
     'quantity_may', 'quantity_jun', 'quantity_jul', 'quantity_aug',
     'quantity_sep', 'quantity_oct', 'quantity_nov', 'quantity_dec',
     'elec_quantity_jan', 'elec_quantity_feb', 'elec_quantity_mar',
     'elec_quantity_apr', 'elec_quantity_may', 'elec_quantity_jun',
     'elec_quantity_jul', 'elec_quantity_aug', 'elec_quantity_sep',
     'elec_quantity_oct', 'elec_quantity_nov', 'elec_quantity_dec',
     'mmbtuper_unit_jan', 'mmbtuper_unit_feb', 'mmbtuper_unit_mar',
     'mmbtuper_unit_apr', 'mmbtuper_unit_may', 'mmbtuper_unit_jun',
     'mmbtuper_unit_jul', 'mmbtuper_unit_aug', 'mmbtuper_unit_sep',
     'mmbtuper_unit_oct', 'mmbtuper_unit_nov', 'mmbtuper_unit_dec',
     'tot_mmbtujan', 'tot_mmbtufeb', 'tot_mmbtumar', 'tot_mmbtuapr',
     'tot_mmbtumay', 'tot_mmbtujun', 'tot_mmbtujul', 'tot_mmbtuaug',
     'tot_mmbtusep', 'tot_mmbtuoct', 'tot_mmbtunov', 'tot_mmbtudec',
     'elec_mmbtujan', 'elec_mmbtufeb', 'elec_mmbtumar', 'elec_mmbtuapr',
     'elec_mmbtumay', 'elec_mmbtujun', 'elec_mmbtujul', 'elec_mmbtuaug',
     'elec_mmbtusep', 'elec_mmbtuoct', 'elec_mmbtunov', 'elec_mmbtudec',
     'netgen_jan', 'netgen_feb', 'netgen_mar', 'netgen_apr', 'netgen_may',
     'netgen_jun', 'netgen_jul', 'netgen_aug', 'netgen_sep', 'netgen_oct',
     'netgen_nov', 'netgen_dec', 'total_fuel_consumption_quantity',
     'electric_fuel_consumption_quantity', 'total_fuel_consumption_mmbtu',
     'elec_fuel_consumption_mmbtu', 'net_generation_megawatthours', 'year'),
    (2012, 'plant_id', 'combined_heat_and_power_plant', 'nuclear_unit_id',
     'plant_name', 'operator_name', 'operator_id', 'plant_state',
     'census_region', 'nerc_region', 'reserved', 'naics_code',
     'eia_sector_number', 'sector_name', 'reported_prime_mover',
     'reported_fuel_type_code', 'aer_fuel_type_code', 'reserved', 'reserved',
     'physical_unit_label', 'quantity_january', 'quantity_february',
     'quantity_march', 'quantity_april', 'quantity_may', 'quantity_june',
     'quantity_july', 'quantity_august', 'quantity_september',
     'quantity_october', 'quantity_november', 'quantity_december',
     'elec_quantity_january', 'elec_quantity_february', 'elec_quantity_march',
     'elec_quantity_april', 'elec_quantity_may', 'elec_quantity_june',
     'elec_quantity_july', 'elec_quantity_august', 'elec_quantity_september',
     'elec_quantity_october', 'elec_quantity_november',
     'elec_quantity_december', 'mmbtuper_unit_january',
     'mmbtuper_unit_february', 'mmbtuper_unit_march', 'mmbtuper_unit_april',
     'mmbtuper_unit_may', 'mmbtuper_unit_june', 'mmbtuper_unit_july',
     'mmbtuper_unit_august', 'mmbtuper_unit_september',
     'mmbtuper_unit_october', 'mmbtuper_unit_november',
     'mmbtuper_unit_december', 'tot_mmbtu_january', 'tot_mmbtu_february',
     'tot_mmbtu_march', 'tot_mmbtu_april', 'tot_mmbtu_may', 'tot_mmbtu_june',
     'tot_mmbtu_july', 'tot_mmbtu_august', 'tot_mmbtu_september',
     'tot_mmbtu_october', 'tot_mmbtu_november', 'tot_mmbtu_december',
     'elec_mmbtu_january', 'elec_mmbtu_february', 'elec_mmbtu_march',
     'elec_mmbtu_april', 'elec_mmbtu_may', 'elec_mmbtu_june',
     'elec_mmbtu_july', 'elec_mmbtu_august', 'elec_mmbtu_september',
     'elec_mmbtu_october', 'elec_mmbtu_november', 'elec_mmbtu_december',
     'netgen_january', 'netgen_february', 'netgen_march', 'netgen_april',
     'netgen_may', 'netgen_june', 'netgen_july', 'netgen_august',
     'netgen_september', 'netgen_october', 'netgen_november',
     'netgen_december', 'total_fuel_consumption_quantity',
     'electric_fuel_consumption_quantity', 'total_fuel_consumption_mmbtu',
     'elec_fuel_consumption_mmbtu', 'net_generation_megawatthours', 'year'),
    (2013, 'plant_id', 'combined_heat_power_plant', 'nuclear_unit_id',
     'plant_name', 'operator_name', 'operator_id', 'state', 'census_region',
     'nerc_region', 'reserved', 'naics_code', 'eia_sector_number',
     'sector_name', 'reported_prime_mover', 'reported_fuel_type_code',
     'aer_fuel_type_code', 'reserved', 'reserved', 'physical_unit_label',
     'quantity_jan', 'quantity_feb', 'quantity_mar', 'quantity_apr',
     'quantity_may', 'quantity_jun', 'quantity_jul', 'quantity_aug',
     'quantity_sep', 'quantity_oct', 'quantity_nov', 'quantity_dec',
     'elec_quantity_jan', 'elec_quantity_feb', 'elec_quantity_mar',
     'elec_quantity_apr', 'elec_quantity_may', 'elec_quantity_jun',
     'elec_quantity_jul', 'elec_quantity_aug', 'elec_quantity_sep',
     'elec_quantity_oct', 'elec_quantity_nov', 'elec_quantity_dec',
     'mmbtuper_unit_jan', 'mmbtuper_unit_feb', 'mmbtuper_unit_mar',
     'mmbtuper_unit_apr', 'mmbtuper_unit_may', 'mmbtuper_unit_jun',
     'mmbtuper_unit_jul', 'mmbtuper_unit_aug', 'mmbtuper_unit_sep',
     'mmbtuper_unit_oct', 'mmbtuper_unit_nov', 'mmbtuper_unit_dec',
     'tot_mmbtujan', 'tot_mmbtufeb', 'tot_mmbtumar', 'tot_mmbtuapr',
     'tot_mmbtumay', 'tot_mmbtujun', 'tot_mmbtujul', 'tot_mmbtuaug',
     'tot_mmbtusep', 'tot_mmbtuoct', 'tot_mmbtunov', 'tot_mmbtudec',
     'elec_mmbtujan', 'elec_mmbtufeb', 'elec_mmbtumar', 'elec_mmbtuapr',
     'elec_mmbtumay', 'elec_mmbtujun', 'elec_mmbtujul', 'elec_mmbtuaug',
     'elec_mmbtusep', 'elec_mmbtuoct', 'elec_mmbtunov', 'elec_mmbtudec',
     'netgen_jan', 'netgen_feb', 'netgen_mar', 'netgen_apr', 'netgen_may',
     'netgen_jun', 'netgen_jul', 'netgen_aug', 'netgen_sep', 'netgen_oct',
     'netgen_nov', 'netgen_dec', 'total_fuel_consumption_quantity',
     'electric_fuel_consumption_quantity', 'total_fuel_consumption_mmbtu',
     'elec_fuel_consumption_mmbtu', 'net_generation_megawatthours', 'year'),
    (2014, 'plant_id', 'combined_heat_and_power_plant', 'nuclear_unit_id',
     'plant_name', 'operator_name', 'operator_id', 'plant_state',
     'census_region', 'nerc_region', 'reserved', 'naics_code',
     'eia_sector_number', 'sector_name', 'reported_prime_mover',
     'reported_fuel_type_code', 'aer_fuel_type_code', 'reserved', 'reserved',
     'physical_unit_label', 'quantity_january', 'quantity_february',
     'quantity_march', 'quantity_april', 'quantity_may', 'quantity_june',
     'quantity_july', 'quantity_august', 'quantity_september',
     'quantity_october', 'quantity_november', 'quantity_december',
     'elec_quantity_january', 'elec_quantity_february', 'elec_quantity_march',
     'elec_quantity_april', 'elec_quantity_may', 'elec_quantity_june',
     'elec_quantity_july', 'elec_quantity_august', 'elec_quantity_september',
     'elec_quantity_october', 'elec_quantity_november',
     'elec_quantity_december', 'mmbtuper_unit_january',
     'mmbtuper_unit_february', 'mmbtuper_unit_march', 'mmbtuper_unit_april',
     'mmbtuper_unit_may', 'mmbtuper_unit_june', 'mmbtuper_unit_july',
     'mmbtuper_unit_august', 'mmbtuper_unit_september',
     'mmbtuper_unit_october', 'mmbtuper_unit_november',
     'mmbtuper_unit_december', 'tot_mmbtu_january', 'tot_mmbtu_february',
     'tot_mmbtu_march', 'tot_mmbtu_april', 'tot_mmbtu_may', 'tot_mmbtu_june',
     'tot_mmbtu_july', 'tot_mmbtu_august', 'tot_mmbtu_september',
     'tot_mmbtu_october', 'tot_mmbtu_november', 'tot_mmbtu_december',
     'elec_mmbtu_january', 'elec_mmbtu_february', 'elec_mmbtu_march',
     'elec_mmbtu_april', 'elec_mmbtu_may', 'elec_mmbtu_june',
     'elec_mmbtu_july', 'elec_mmbtu_august', 'elec_mmbtu_september',
     'elec_mmbtu_october', 'elec_mmbtu_november', 'elec_mmbtu_december',
     'netgen_january', 'netgen_february', 'netgen_march', 'netgen_april',
     'netgen_may', 'netgen_june', 'netgen_july', 'netgen_august',
     'netgen_september', 'netgen_october', 'netgen_november',
     'netgen_december', 'total_fuel_consumption_quantity',
     'electric_fuel_consumption_quantity', 'total_fuel_consumption_mmbtu',
     'elec_fuel_consumption_mmbtu', 'net_generation_megawatthours', 'year'),
    (2015, 'plant_id', 'combined_heat_and_power_plant', 'nuclear_unit_id',
     'plant_name', 'operator_name', 'operator_id', 'plant_state',
     'census_region', 'nerc_region', 'reserved', 'naics_code',
     'eia_sector_number', 'sector_name', 'reported_prime_mover',
     'reported_fuel_type_code', 'aer_fuel_type_code', 'reserved', 'reserved',
     'physical_unit_label', 'quantity_january', 'quantity_february',
     'quantity_march', 'quantity_april', 'quantity_may', 'quantity_june',
     'quantity_july', 'quantity_august', 'quantity_september',
     'quantity_october', 'quantity_november', 'quantity_december',
     'elec_quantity_january', 'elec_quantity_february', 'elec_quantity_march',
     'elec_quantity_april', 'elec_quantity_may', 'elec_quantity_june',
     'elec_quantity_july', 'elec_quantity_august', 'elec_quantity_september',
     'elec_quantity_october', 'elec_quantity_november',
     'elec_quantity_december', 'mmbtuper_unit_january',
     'mmbtuper_unit_february', 'mmbtuper_unit_march', 'mmbtuper_unit_april',
     'mmbtuper_unit_may', 'mmbtuper_unit_june', 'mmbtuper_unit_july',
     'mmbtuper_unit_august', 'mmbtuper_unit_september',
     'mmbtuper_unit_october', 'mmbtuper_unit_november',
     'mmbtuper_unit_december', 'tot_mmbtu_january', 'tot_mmbtu_february',
     'tot_mmbtu_march', 'tot_mmbtu_april', 'tot_mmbtu_may', 'tot_mmbtu_june',
     'tot_mmbtu_july', 'tot_mmbtu_august', 'tot_mmbtu_september',
     'tot_mmbtu_october', 'tot_mmbtu_november', 'tot_mmbtu_december',
     'elec_mmbtu_january', 'elec_mmbtu_february', 'elec_mmbtu_march',
     'elec_mmbtu_april', 'elec_mmbtu_may', 'elec_mmbtu_june',
     'elec_mmbtu_july', 'elec_mmbtu_august', 'elec_mmbtu_september',
     'elec_mmbtu_october', 'elec_mmbtu_november', 'elec_mmbtu_december',
     'netgen_january', 'netgen_february', 'netgen_march', 'netgen_april',
     'netgen_may', 'netgen_june', 'netgen_july', 'netgen_august',
     'netgen_september', 'netgen_october', 'netgen_november',
     'netgen_december', 'total_fuel_consumption_quantity',
     'electric_fuel_consumption_quantity', 'total_fuel_consumption_mmbtu',
     'elec_fuel_consumption_mmbtu', 'net_generation_megawatthours', 'year'),
    (2016, 'plant_id', 'combined_heat_and_power_plant', 'nuclear_unit_id',
     'plant_name', 'operator_name', 'operator_id', 'plant_state',
     'census_region', 'nerc_region', 'reserved', 'naics_code',
     'eia_sector_number', 'sector_name', 'reported_prime_mover',
     'reported_fuel_type_code', 'aer_fuel_type_code', 'reserved', 'reserved',
     'physical_unit_label', 'quantity_january', 'quantity_february',
     'quantity_march', 'quantity_april', 'quantity_may', 'quantity_june',
     'quantity_july', 'quantity_august', 'quantity_september',
     'quantity_october', 'quantity_november', 'quantity_december',
     'elec_quantity_january', 'elec_quantity_february', 'elec_quantity_march',
     'elec_quantity_april', 'elec_quantity_may', 'elec_quantity_june',
     'elec_quantity_july', 'elec_quantity_august', 'elec_quantity_september',
     'elec_quantity_october', 'elec_quantity_november',
     'elec_quantity_december', 'mmbtuper_unit_january',
     'mmbtuper_unit_february', 'mmbtuper_unit_march', 'mmbtuper_unit_april',
     'mmbtuper_unit_may', 'mmbtuper_unit_june', 'mmbtuper_unit_july',
     'mmbtuper_unit_august', 'mmbtuper_unit_september',
     'mmbtuper_unit_october', 'mmbtuper_unit_november',
     'mmbtuper_unit_december', 'tot_mmbtu_january', 'tot_mmbtu_february',
     'tot_mmbtu_march', 'tot_mmbtu_april', 'tot_mmbtu_may', 'tot_mmbtu_june',
     'tot_mmbtu_july', 'tot_mmbtu_august', 'tot_mmbtu_september',
     'tot_mmbtu_october', 'tot_mmbtu_november', 'tot_mmbtu_december',
     'elec_mmbtu_january', 'elec_mmbtu_february', 'elec_mmbtu_march',
     'elec_mmbtu_april', 'elec_mmbtu_may', 'elec_mmbtu_june',
     'elec_mmbtu_july', 'elec_mmbtu_august', 'elec_mmbtu_september',
     'elec_mmbtu_october', 'elec_mmbtu_november', 'elec_mmbtu_december',
     'netgen_january', 'netgen_february', 'netgen_march', 'netgen_april',
     'netgen_may', 'netgen_june', 'netgen_july', 'netgen_august',
     'netgen_september', 'netgen_october', 'netgen_november',
     'netgen_december', 'total_fuel_consumption_quantity',
     'electric_fuel_consumption_quantity', 'total_fuel_consumption_mmbtu',
     'elec_fuel_consumption_mmbtu', 'net_generation_megawatthours', 'year')
],
    columns=['year_index', 'plant_id', 'combined_heat_and_power_plant',
             'nuclear_unit_id', 'plant_name', 'operator_name', 'operator_id',
             'plant_state', 'census_region', 'nerc_region', 'reserved',
             'naics_code', 'eia_sector_number', 'sector_name',
             'reported_prime_mover', 'reported_fuel_type_code',
             'aer_fuel_type_code', 'reserved_1', 'reserved_2',
             'physical_unit_label', 'quantity_january', 'quantity_february',
             'quantity_march', 'quantity_april', 'quantity_may',
             'quantity_june', 'quantity_july', 'quantity_august',
             'quantity_september', 'quantity_october', 'quantity_november',
             'quantity_december', 'elec_quantity_january',
             'elec_quantity_february', 'elec_quantity_march',
             'elec_quantity_april', 'elec_quantity_may', 'elec_quantity_june',
             'elec_quantity_july', 'elec_quantity_august',
             'elec_quantity_september', 'elec_quantity_october',
             'elec_quantity_november', 'elec_quantity_december',
             'mmbtuper_unit_january', 'mmbtuper_unit_february',
             'mmbtuper_unit_march', 'mmbtuper_unit_april', 'mmbtuper_unit_may',
             'mmbtuper_unit_june', 'mmbtuper_unit_july',
             'mmbtuper_unit_august', 'mmbtuper_unit_september',
             'mmbtuper_unit_october', 'mmbtuper_unit_november',
             'mmbtuper_unit_december', 'tot_mmbtu_january',
             'tot_mmbtu_february', 'tot_mmbtu_march', 'tot_mmbtu_april',
             'tot_mmbtu_may', 'tot_mmbtu_june', 'tot_mmbtu_july',
             'tot_mmbtu_august', 'tot_mmbtu_september', 'tot_mmbtu_october',
             'tot_mmbtu_november', 'tot_mmbtu_december', 'elec_mmbtu_january',
             'elec_mmbtu_february', 'elec_mmbtu_march', 'elec_mmbtu_april',
             'elec_mmbtu_may', 'elec_mmbtu_june', 'elec_mmbtu_july',
             'elec_mmbtu_august', 'elec_mmbtu_september', 'elec_mmbtu_october',
             'elec_mmbtu_november', 'elec_mmbtu_december', 'netgen_january',
             'netgen_february', 'netgen_march', 'netgen_april', 'netgen_may',
             'netgen_june', 'netgen_july', 'netgen_august', 'netgen_september',
             'netgen_october', 'netgen_november', 'netgen_december',
             'total_fuel_consumption_quantity',
             'electric_fuel_consumption_quantity',
             'total_fuel_consumption_mmbtu', 'elec_fuel_consumption_mmbtu',
             'net_generation_megawatthours', 'year'],
    index='year_index'
)

stocks_map_eia923 = DataFrame.from_records([
    (2009, 'none', 'coal_jan', 'coal_feb', 'coal_mar', 'coal_apr', 'coal_may',
     'coal_jun', 'coal_jul', 'coal_aug', 'coal_sep', 'coal_oct', 'coal_nov',
     'coal_dec', 'oil_jan', 'oil_feb', 'oil_mar', 'oil_apr', 'oil_may',
     'oil_jun', 'oil_jul', 'oil_aug', 'oil_sep', 'oil_oct', 'oil_nov',
     'oil_dec', 'petcoke_jan', 'petcoke_feb', 'petcoke_mar', 'petcoke_apr',
     'petcoke_may', 'petcoke_jun', 'petcoke_jul', 'petcoke_aug', 'petcoke_sep',
     'petcoke_oct', 'petcoke_nov', 'petcoke_dec'),
    (2010, 'none', 'coal_jan', 'coal_feb', 'coal_mar', 'coal_apr', 'coal_may',
     'coal_jun', 'coal_jul', 'coal_aug', 'coal_sep', 'coal_oct', 'coal_nov',
     'coal_dec', 'oil_jan', 'oil_feb', 'oil_mar', 'oil_apr', 'oil_may',
     'oil_jun', 'oil_jul', 'oil_aug', 'oil_sep', 'oil_oct', 'oil_nov',
     'oil_dec', 'petcoke_jan', 'petcoke_feb', 'petcoke_mar', 'petcoke_apr',
     'petcoke_may', 'petcoke_jun', 'petcoke_jul', 'petcoke_aug', 'petcoke_sep',
     'petcoke_oct', 'petcoke_nov', 'petcoke_dec'),
    (2011, 'region_name', 'coal_jan', 'coal_feb', 'coal_mar', 'coal_apr',
     'coal_may', 'coal_jun', 'coal_jul', 'coal_aug', 'coal_sep', 'coal_oct',
     'coal_nov', 'coal_dec', 'oil_jan', 'oil_feb', 'oil_mar', 'oil_apr',
     'oil_may', 'oil_jun', 'oil_jul', 'oil_aug', 'oil_sep', 'oil_oct',
     'oil_nov', 'oil_dec', 'petcoke_jan', 'petcoke_feb', 'petcoke_mar',
     'petcoke_apr', 'petcoke_may', 'petcoke_jun', 'petcoke_jul', 'petcoke_aug',
     'petcoke_sep', 'petcoke_oct', 'petcoke_nov', 'petcoke_dec'),
    (2012, 'census_division_and_state', 'coal_january', 'coal_february',
     'coal_march', 'coal_april', 'coal_may', 'coal_june', 'coal_july',
     'coal_august', 'coal_september', 'coal_october', 'coal_november',
     'coal_december', 'oil_january', 'oil_february', 'oil_march', 'oil_april',
     'oil_may', 'oil_june', 'oil_july', 'oil_august', 'oil_september',
     'oil_october', 'oil_november', 'oil_december', 'petcoke_january',
     'petcoke_february', 'petcoke_march', 'petcoke_april', 'petcoke_may',
     'petcoke_june', 'petcoke_july', 'petcoke_august', 'petcoke_september',
     'petcoke_october', 'petcoke_november', 'petcoke_december'),
    (2013, 'region_name', 'coal_jan', 'coal_feb', 'coal_mar', 'coal_apr',
     'coal_may', 'coal_jun', 'coal_jul', 'coal_aug', 'coal_sep', 'coal_oct',
     'coal_nov', 'coal_dec', 'oil_jan', 'oil_feb', 'oil_mar', 'oil_apr',
     'oil_may', 'oil_jun', 'oil_jul', 'oil_aug', 'oil_sep', 'oil_oct',
     'oil_nov', 'oil_dec', 'petcoke_jan', 'petcoke_feb', 'petcoke_mar',
     'petcoke_apr', 'petcoke_may', 'petcoke_jun', 'petcoke_jul', 'petcoke_aug',
     'petcoke_sep', 'petcoke_oct', 'petcoke_nov', 'petcoke_dec'),
    (2014, 'census_division_and_state', 'coal_january', 'coal_february',
     'coal_march', 'coal_april', 'coal_may', 'coal_june', 'coal_july',
     'coal_august', 'coal_september', 'coal_october', 'coal_november',
     'coal_december', 'oil_january', 'oil_february', 'oil_march', 'oil_april',
     'oil_may', 'oil_june', 'oil_july', 'oil_august', 'oil_september',
     'oil_october', 'oil_november', 'oil_december', 'petcoke_january',
     'petcoke_february', 'petcoke_march', 'petcoke_april', 'petcoke_may',
     'petcoke_june', 'petcoke_july', 'petcoke_august', 'petcoke_september',
     'petcoke_october', 'petcoke_november', 'petcoke_december'),
    (2015, 'census_division_and_state', 'coal_january', 'coal_february',
     'coal_march', 'coal_april', 'coal_may', 'coal_june', 'coal_july',
     'coal_august', 'coal_september', 'coal_october', 'coal_november',
     'coal_december', 'oil_january', 'oil_february', 'oil_march', 'oil_april',
     'oil_may', 'oil_june', 'oil_july', 'oil_august', 'oil_september',
     'oil_october', 'oil_november', 'oil_december', 'petcoke_january',
     'petcoke_february', 'petcoke_march', 'petcoke_april', 'petcoke_may',
     'petcoke_june', 'petcoke_july', 'petcoke_august', 'petcoke_september',
     'petcoke_october', 'petcoke_november', 'petcoke_december'),
    (2016, 'census_division_and_state', 'coal_january', 'coal_february',
     'coal_march', 'coal_april', 'coal_may', 'coal_june', 'coal_july',
     'coal_august', 'coal_september', 'coal_october', 'coal_november',
     'coal_december', 'oil_january', 'oil_february', 'oil_march', 'oil_april',
     'oil_may', 'oil_june', 'oil_july', 'oil_august', 'oil_september',
     'oil_october', 'oil_november', 'oil_december', 'petcoke_january',
     'petcoke_february', 'petcoke_march', 'petcoke_april', 'petcoke_may',
     'petcoke_june', 'petcoke_july', 'petcoke_august', 'petcoke_september',
     'petcoke_october', 'petcoke_november', 'petcoke_december')
],
    columns=['year_index', 'census_division_and_state', 'coal_january',
             'coal_february', 'coal_march', 'coal_april', 'coal_may',
             'coal_june', 'coal_july', 'coal_august', 'coal_september',
             'coal_october', 'coal_november', 'coal_december', 'oil_january',
             'oil_february', 'oil_march', 'oil_april', 'oil_may', 'oil_june',
             'oil_july', 'oil_august', 'oil_september', 'oil_october',
             'oil_november', 'oil_december', 'petcoke_january',
             'petcoke_february', 'petcoke_march', 'petcoke_april',
             'petcoke_may', 'petcoke_june', 'petcoke_july', 'petcoke_august',
             'petcoke_september', 'petcoke_october', 'petcoke_november',
             'petcoke_december'],
    index='year_index'
)

boiler_fuel_map_eia923 = DataFrame.from_records()

# patterns for matching columns to months:
month_dict_2015_eia923 = {1: '_january$',
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


# The set of FERC Form 1 tables that have the same composite primary keys: [
# respondent_id, report_year, report_prd, row_number, spplmnt_num ].
# TODO: THIS ONLY PERTAINS TO 2015 AND MAY NEED TO BE ADJUSTED BY YEAR...
ferc1_data_tables = \
    ['f1_acb_epda', 'f1_accumdepr_prvsn', 'f1_accumdfrrdtaxcr',
     'f1_adit_190_detail', 'f1_adit_190_notes',
     'f1_adit_amrt_prop', 'f1_adit_other', 'f1_adit_other_prop',
     'f1_allowances', 'f1_bal_sheet_cr', 'f1_capital_stock',
     'f1_cash_flow', 'f1_cmmn_utlty_p_e', 'f1_comp_balance_db',
     'f1_construction', 'f1_control_respdnt', 'f1_co_directors',
     'f1_cptl_stk_expns', 'f1_csscslc_pcsircs', 'f1_dacs_epda',
     'f1_dscnt_cptl_stk', 'f1_edcfu_epda', 'f1_elctrc_erg_acct',
     'f1_elctrc_oper_rev', 'f1_elc_oper_rev_nb',
     'f1_elc_op_mnt_expn', 'f1_electric', 'f1_envrnmntl_expns',
     'f1_envrnmntl_fclty', 'f1_fuel', 'f1_general_info',
     'f1_gnrt_plant', 'f1_important_chg', 'f1_incm_stmnt_2',
     'f1_income_stmnt', 'f1_miscgen_expnelc',
     'f1_misc_dfrrd_dr', 'f1_mthly_peak_otpt', 'f1_mtrl_spply',
     'f1_nbr_elc_deptemp', 'f1_nonutility_prop',
     'f1_note_fin_stmnt', 'f1_nuclear_fuel', 'f1_officers_co',
     'f1_othr_dfrrd_cr', 'f1_othr_pd_in_cptl',
     'f1_othr_reg_assets', 'f1_othr_reg_liab', 'f1_overhead',
     'f1_pccidica', 'f1_plant_in_srvce', 'f1_pumped_storage',
     'f1_purchased_pwr', 'f1_reconrpt_netinc',
     'f1_reg_comm_expn', 'f1_respdnt_control',
     'f1_retained_erng', 'f1_r_d_demo_actvty',
     'f1_sales_by_sched', 'f1_sale_for_resale',
     'f1_sbsdry_totals', 'f1_schedules_list',
     'f1_security_holder', 'f1_slry_wg_dstrbtn',
     'f1_substations', 'f1_taxacc_ppchrgyr', 'f1_unrcvrd_cost',
     'f1_utltyplnt_smmry', 'f1_work', 'f1_xmssn_adds',
     'f1_xmssn_elc_bothr', 'f1_xmssn_elc_fothr',
     'f1_xmssn_line', 'f1_xtraordnry_loss', 'f1_audit_log',
     'f1_privilege', 'f1_hydro', 'f1_footnote_tbl', 'f1_steam',
     'f1_leased', 'f1_sbsdry_detail', 'f1_plant',
     'f1_long_term_debt', 'f1_106_2009', 'f1_106a_2009',
     'f1_106b_2009', 'f1_208_elc_dep', 'f1_231_trn_stdycst',
     'f1_324_elc_expns', 'f1_325_elc_cust', 'f1_331_transiso',
     'f1_338_dep_depl', 'f1_397_isorto_stl', 'f1_398_ancl_ps',
     'f1_399_mth_peak', 'f1_400_sys_peak', 'f1_400a_iso_peak',
     'f1_429_trans_aff', 'f1_allowances_nox',
     'f1_cmpinc_hedge_a', 'f1_cmpinc_hedge', 'f1_freeze',
     'f1_rg_trn_srv_rev']

# Line numbers, and corresponding FERC account number
# from FERC Form 1 pages 204-207, Electric Plant in Service.
# Descriptions from: https://www.law.cornell.edu/cfr/text/18/part-101
ferc_electric_plant_accounts = DataFrame.from_records([
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

# Line numbers, and corresponding FERC account number
# from FERC Form 1 page 219, ACCUMULATED PROVISION FOR DEPRECIATION
# OF ELECTRIC UTILITY PLANT (Account 108).

ferc_accumulated_depreciation = DataFrame.from_records([

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
    (16, 'other_debit_or_credit items',\
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

# From Page 7 of EIA Form 923, Census Region the state is located in
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

# EIA 923: EIA Type of prime mover:
prime_movers_eia923 = {
    'BA': 'Energy Storage, Battery',
    'BT': 'Turbines Used in a Binary Cycle. \
        Including those used for geothermal applications',
    'CA': 'Combined-Cycle -- Steam Part',
    'CE': 'Energy Storage, Compressed Air',
    'CP': 'Energy Storage, Concentrated Solar Power',
    'CS': 'Combined-Cycle Single-Shaft Combustion \
        Turbine and Steam Turbine share of single',
    'CT': 'Combined-Cycle Combustion Turbine Part',
    'ES': 'Energy Storage, Other (Specify on Schedule 9, Comments)',
    'FC': 'Fuel Cell',
    'FW': 'Energy Storage, Flywheel',
    'GT': 'Combustion (Gas) Turbine. Including Jet Engine design',
    'HA': 'Hydrokinetic, Axial Flow Turbine',
    'HB': 'Hydrokinetic, Wave Buoy',
    'HK': 'Hydrokinetic, Other',
    'HY': 'Hydraulic Turbine. Including turbines associated \
        with delivery of water by pipeline.',
    'IC': 'Internal Combustion (diesel, piston, reciprocating) Engine',
    'PS': 'Energy Storage, Reversible Hydraulic Turbine (Pumped Storage)',
    'OT': 'Other',
    'ST': 'Steam Turbine. Including Nuclear, Geothermal, and \
        Solar Steam (does not include Combined Cycle).',
    'PV': 'Photovoltaic',
    'WT': 'Wind Turbine, Onshore',
    'WS': 'Wind Turbine, Offshore'
}

# EIA 923: The fuel code reported to EIA.Two or three letter alphanumeric:
fuel_type_eia923 = {
    'AB': 'Agricultural By-Products',
    'ANT': 'Anthracite Coal',
    'BFG': 'Blast Furnace Gas',
    'BIT': 'Bituminous Coal',
    'BLQ': 'Black Liquor',
    'CBL': 'Coal, Blended',
    'DFO': 'Distillate Fuel Oil. Including diesel, No. 1, No. 2, and No. 4 \
            fuel oils.',
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
    'OBG': 'Other Biomass Gas. Including digester gas, methane, and other \
            biomass gases.',
    'OBL': 'Other Biomass Liquids',
    'OBS': 'Other Biomass Solids',
    'OG': 'Other Gas',
    'OTH': 'Other Fuel',
    'PC': 'Petroleum Coke',
    'PG': 'Gaseous Propane',
    'PUR': 'Purchased Steam',
    'RC': 'Refined Coal',
    'RFO': 'Residual Fuel Oil. Including No. 5 & 6 fuel oils and \
         bunker C fuel oil.',
    'SC': 'Coal-based Synfuel. Including briquettes, pellets, or \
        extrusions, which are formed by binding materials or \
        processes that recycle materials.',
    'SGC': 'Coal-Derived Synthesis Gas',
    'SGP': 'Synthesis Gas from Petroleum Coke',
    'SLW': 'Sludge Waste',
    'SUB': 'Subbituminous Coal',
    'SUN': 'Solar',
    'TDF': 'Tire-derived Fuels',
    'WAT': 'Water at a Conventional Hydroelectric Turbine and \
         water used in Wave Buoy Hydrokinetic Technology, \
         current Hydrokinetic Technology, Tidal Hydrokinetic Technology, and \
         Pumping Energy for Reversible (Pumped Storage) Hydroelectric \
         Turbines.',
    'WC': 'Waste/Other Coal. Including anthracite culm, bituminous gob, \
        fine coal, lignite waste, waste coal.',
    'WDL': 'Wood Waste Liquids, excluding Black Liquor. Including red liquor, \
         sludge wood, spent sulfite liquor, and other wood-based liquids.',
    'WDS': 'Wood/Wood Waste Solids. Including paper pellets, \
         railroad ties, utility polies, wood chips, bark, and \
         other wood waste solids.',
    'WH': 'Waste Heat not directly attributed to a fuel source',
    'WND': 'Wind',
    'WO': 'Waste/Other Oil. Including crude oil, liquid butane, \
        liquid propane, naphtha, oil waste, re-refined moto oil, \
        sludge oil, tar oil, or other petroleum-based liquid wastes.'
}

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

# EIA 923: EIA The type of physical units fuel consumption is reported in.
# All consumption is reported in either short tons for solids,
# thousands of cubic feet for gases, and barrels for liquids:"""
fuel_units_eia923 = {
    'mcf': 'Thousands of cubic feet (for gases)',
    'short_tons': 'Short tons (for solids)',
    'barrels': 'Barrels (for liquids)'
}
# EIA 923: Designates the purchase type under which receipts occurred
# in the reporting month. One or two character alphanumeric:
contract_type_eia923 = {
    'C': 'Contract',
    'NC': 'New Contract',
    'S': 'Spot Purchase'
}
# EIA 923: The fuel code associated with the fuel receipt.
# Defined on Page 7 of EIA Form 923
# Two or three character alphanumeric:
energy_source_eia923 = {
    'ANT': 'Anthracite Coal',
    'BIT': 'Bituminous Coal',
    'DFO': 'Distillate Fuel Oil. Including diesel,\
           No. 1, No. 2, and No. 4 fuel oils.',
    'LIG': 'Lignite Coal',
    'NG': 'Natural Gas',
    'PC': 'Petroleum Coke',
    'RC': 'Refined Coal',
    'RFO': 'Residual Fuel Oil. Including \
           No. 5 & 6 fuel oils and bunker C fuel oil.',
    'SC': 'Coal-based Synfuel. Including briquettes, pellets, or \
          extrusions, which are formed by binding materials or \
          processes that recycle materials.',
    'SUB': 'Subbituminous Coal',
    'WC': 'Waste/Other Coal. Including anthracite culm, bituminous gob, fine\
          coal, lignite waste, waste coal.',
    'WO': 'Waste/Other Oil. Including crude oil, liquid butane, liquid propane,\
          naphtha, oil waste, re-refined moto oil, sludge oil, tar oil, or\
          other petroleum-based liquid wastes.'
}

# EIA 923 Fuel Group, from Page 7 EIA Form 923
# Groups the energy sources into fuel groups that are located in the Electric
# Power Monthly:  Coal, Natural Gas, Petroleum, Petroleum Coke.
fuel_group_eia923 = ['Coal', 'Natural Gas', 'Petroleum', 'Petroleum Coke']

# EIA 923: Type of Coal Mine as defined on Page 7 of EIA Form 923
coalmine_type_eia923 = {
    'p': 'Preparation Plant',
    'P': 'Preperation Plant',
    'S': 'Surface',
    'U': 'Underground',
    'U/S': 'Both an underground and surface mine with \
  most coal extracted from underground',
    'S/U': 'Both an underground and surface mine with \
  most coal extracted from surface'
}

# EIA 923: State abbreviation related to coal mine location.
# Country abbreviations are also listed under this category and are as follows:

coalmine_state_eia923 = {
    'AU': 'Australia',
    'CL': 'Columbia',
    'CN': 'Canada',
    'IS': 'Indonesia',
    'PL': 'Poland',
    'RS': 'Russia',
    'UK': 'United Kingdom',
    'VZ': 'Venezula',
    'OC': 'Other Country',
    'IM': 'Unknown'
}

# EIA 923: One character designates the reporting
# frequency for the plant. Alphanumeric:
respondent_frequency_eia923 = {
    'M': 'Monthly respondent',
    'A': 'Annual respondent'
}

# EIA 923: Mode for the longest / second longest distance.
transport_modes_eia923 = {
    'RR': 'Rail: Shipments of fuel moved to consumers by rail \
        (private or public/commercial). Included is coal hauled to or \
        away from a railroad siding by truck if the truck did not use public\
        roads.',
    'RV': 'River:  Shipments of fuel moved to consumers via river by barge.  \
        Not included are shipments to Great Lakes coal loading docks, \
        tidewater piers, or coastal ports.',
    'rv':  'River:  Shipments of fuel moved to consumers via river by barge.  \
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
    'TC': 'Tramway/Conveyor: Shipments of fuel moved to consumers \
        by tramway or conveyor.',
    'SP': 'Slurry Pipeline: Shipments of coal moved to consumers \
        by slurry pipeline.',
    'PL': 'Pipeline: Shipments of fuel moved to consumers by pipeline'
}

# EIA 923: Contract type for natural gas capacity service:
natural_gas_transport_eia923 = {
    '2': 'Unknown',
    '8': 'Unknown',
    'F': 'Firm',
    'I': 'Interruptible'
}
