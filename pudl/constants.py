# This file holds a bunch of constant values which are used throughout PUDL
# to populate static lists within the DB or for data cleaning purposes.

# These imports are necessary for the DBF to SQL type map.
from sqlalchemy import String, Date, Float, Integer
from sqlalchemy import Boolean, Text, Float, DateTime

######################################################################
# Constants used within the pudl.py module.
######################################################################
prime_movers = ['steam_turbine', 'gas_turbine', 'hydro', 'internal_combustion',
                'solar_pv', 'wind_turbine']

rto_iso = {
  'CAISO' :'California ISO',
  'ERCOT' :'Electric Reliability Council of Texas',
  'MISO'  :'Midcontinent ISO',
  'ISO-NE':'ISO New England',
  'NYISO' :'New York ISO',
  'PJM'   :'PJM Interconnection',
  'SPP'   :'Southwest Power Pool'
}

us_states = {
  'AK':'Alaska',
  'AL':'Alabama',
  'AR':'Arkansas',
  'AS':'American Samoa',
  'AZ':'Arizona',
  'CA':'California',
  'CO':'Colorado',
  'CT':'Connecticut',
  'DC':'District of Columbia',
  'DE':'Delaware',
  'FL':'Florida',
  'GA':'Georgia',
  'GU':'Guam',
  'HI':'Hawaii',
  'IA':'Iowa',
  'ID':'Idaho',
  'IL':'Illinois',
  'IN':'Indiana',
  'KS':'Kansas',
  'KY':'Kentucky',
  'LA':'Louisiana',
  'MA':'Massachusetts',
  'MD':'Maryland',
  'ME':'Maine',
  'MI':'Michigan',
  'MN':'Minnesota',
  'MO':'Missouri',
  'MP':'Northern Mariana Islands',
  'MS':'Mississippi',
  'MT':'Montana',
  'NA':'National',
  'NC':'North Carolina',
  'ND':'North Dakota',
  'NE':'Nebraska',
  'NH':'New Hampshire',
  'NJ':'New Jersey',
  'NM':'New Mexico',
  'NV':'Nevada',
  'NY':'New York',
  'OH':'Ohio',
  'OK':'Oklahoma',
  'OR':'Oregon',
  'PA':'Pennsylvania',
  'PR':'Puerto Rico',
  'RI':'Rhode Island',
  'SC':'South Carolina',
  'SD':'South Dakota',
  'TN':'Tennessee',
  'TX':'Texas',
  'UT':'Utah',
  'VA':'Virginia',
  'VI':'Virgin Islands',
  'VT':'Vermont',
  'WA':'Washington',
  'WI':'Wisconsin',
  'WV':'West Virginia',
  'WY':'Wyoming'
}

# Construct a dictionary mapping a canonical fuel name to a list of strings
# which are used to represent that fuel in the FERC Form 1 Reporting. Case is
# ignored, as all fuel strings can be converted to a lower case in the data
# set.
ferc1_coal_strings = ['coal', 'coal-subbit', 'lignite', 'coal(sb)',\
    'coal (sb)', 'coal-lignite', 'coke', 'coa', 'lignite/coal',\
    'coal - subbit', 'coal-subb', 'coal-sub', 'coal-lig', 'coal-sub bit',\
    'coals', 'ciak', 'petcoke']

ferc1_oil_strings  = ['oil', '#6 oil', '#2 oil', 'fuel oil', 'jet', 'no. 2 oil',\
                   'no.2 oil', 'no.6& used', 'used oil', 'oil-2', 'oil (#2)',\
                   'diesel oil', 'residual oil', '# 2 oil', 'resid. oil',\
                   'tall oil', 'oil/gas', 'no.6 oil', 'oil-fuel', 'oil-diesel',\
                   'oil / gas', 'oil bbls', 'oil bls', 'no. 6 oil',\
                   '#1 kerosene', 'diesel', 'no. 2 oils', 'blend oil',\
                   '#2oil diesel', '#2 oil-diesel', '# 2  oil', 'light oil',\
                   'heavy oil', 'gas.oil', '#2', '2', '6', 'bbl', 'no 2 oil',\
                   'no 6 oil', '#1 oil', '#6', 'oil-kero', 'oil bbl',\
                   'biofuel', 'no 2', 'kero', '#1 fuel oil', 'no. 2  oil',\
                   'blended oil', 'no 2. oil', '# 6 oil', 'nno. 2 oil',\
                   '#2 fuel', 'oill', 'oils', 'gas/oil', 'no.2 oil gas',\
                   '#2 fuel oil', 'oli', 'oil (#6)']

ferc1_gas_strings  = ['gas', 'methane', 'natural gas', 'blast gas', 'gas mcf',\
                   'propane', 'prop', 'natural  gas', 'nat.gas', 'nat gas',\
                   'nat. gas', 'natl gas', 'ga', 'gas`', 'syngas', 'ng', 'mcf',\
                   'blast gaa', 'nat  gas', 'gac', 'syngass', 'prop.','natural']

ferc1_nuke_strings = ['nuclear', 'grams of uran', 'grams of', 'grams of  ura',\
                   'grams', 'nucleur', 'nulear', 'nucl', 'nucleart']

ferc1_biomass_strings = ['switchgrass', 'wood waste', 'woodchips', 'biomass',\
                      'wood', 'wood chips']

ferc1_waste_strings = ['tires', 'tire', 'refuse']

ferc1_steam_strings = ['steam', 'purch steam', 'purch. steam']

# There are also a bunch of other weird and hard to categorize strings
# that I don't know what to do with... hopefully they constitute only a
# small fraction of the overall generation.

ferc1_fuel_strings = { 'coal'    : ferc1_coal_strings,
                       'gas'     : ferc1_gas_strings,
                       'oil'     : ferc1_oil_strings,
                       'nuke'    : ferc1_nuke_strings,
                       'biomass' : ferc1_biomass_strings,
                       'waste'   : ferc1_waste_strings,
                       'steam'   : ferc1_steam_strings
                     }

# Similarly, dictionary for cleaning up fuel unit strings
ferc1_ton_strings = ['toms','taons','tones','col-tons','toncoaleq','coal',\
                  'tons coal eq','coal-tons','ton','tons','tons coal',\
                  'coal-ton','tires-tons']

ferc1_mcf_strings = ['mcf',"mcf's",'mcfs','mcf.','gas mcf','"gas" mcf','gas-mcf',\
                  'mfc','mct',' mcf','msfs','mlf','mscf','mci','mcl','mcg',\
                  'm.cu.ft.']

ferc1_bbl_strings = ['barrel','bbls','bbl','barrels','bbrl','bbl.','bbls.',\
                  'oil 42 gal','oil-barrels','barrrels','bbl-42 gal',\
                  'oil-barrel','bb.','barrells','bar','bbld','oil- barrel',\
                  'barrels    .','bbl .','barels','barrell','berrels','bb',\
                  'bbl.s','oil-bbl','bls','bbl:','barrles','blb','propane-bbl']

ferc1_gal_strings = ['gallons','gal.','gals','gals.','gallon','gal']

ferc1_1kgal_strings = ['oil(1000 gal)','oil(1000)','oil (1000)','oil(1000']

ferc1_gramsU_strings = ['gram','grams','gm u','grams u235','grams u-235',\
                     'grams of uran','grams: u-235','grams:u-235',\
                     'grams:u235','grams u308','grams: u235','grams of']

ferc1_kgU_strings = ['kg of uranium','kg uranium','kilg. u-235','kg u-235',\
                  'kilograms-u23','kg','kilograms u-2','kilograms','kg of']

ferc1_mmbtu_strings = ['mmbtu','mmbtus',"mmbtu's",'nuclear-mmbtu','nuclear-mmbt']

ferc1_mwdth_strings = ['mwd therman','mw days-therm','mwd thrml','mwd thermal',\
                    'mwd/mtu','mw days','mwdth','mwd','mw day']

ferc1_mwhth_strings = ['mwh them','mwh threm','nwh therm','mwhth','mwh therm','mwh']

ferc1_fuel_unit_strings = { 'ton'   : ferc1_ton_strings,
                            'mcf'   : ferc1_mcf_strings,
                            'bbl'   : ferc1_bbl_strings,
                            'gal'   : ferc1_gal_strings,
                            '1kgal' : ferc1_1kgal_strings,
                            'gramsU': ferc1_gramsU_strings,
                            'kgU'   : ferc1_kgU_strings,
                            'mmbtu' : ferc1_mmbtu_strings,
                            'mwdth' : ferc1_mwdth_strings,
                            'mwhth' : ferc1_mwhth_strings
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
    'F1_52': 'f1_plant_in_srvce', # GET THIS ONE
    'F1_53': 'f1_pumped_storage', # GET THIS ONE
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
    'F1_398_ANCL_PS': 'f1_398_ancl_ps', # GET THIS ONE
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
    'F1_S0_FILING_LOG': 'f1_s0_filing_log', # GET THIS ONE
    'F1_SECURITY': 'f1_security'
}
# Invert the map above so we can go either way as needed
ferc1_tbl2dbf = { v: k for k, v in ferc1_dbf2tbl.items() }

# This dictionary maps the strings which are used to denote field types in the
# DBF objects to the corresponding generic SQLAlchemy Column types:
# These definitions come from a combination of the dbfread example program
# dbf2sqlite and this DBF file format documentation page:
# http://www.dbase.com/KnowledgeBase/int/db7_file_fmt.htm
# Un-mapped types left as 'XXX' which should obviously make an error...
dbf_typemap = {
    'C' : String,
    'D' : Date,
    'F' : Float,
    'I' : Integer,
    'L' : Boolean,
    'M' : Text, # 10 digit .DBT block number, stored as a string...
    'N' : Float,
    'T' : DateTime,
    'B' : 'XXX', # .DBT block number, binary string
    '@' : 'XXX', # Timestamp... Date = Julian Day, Time is in milliseconds?
    '+' : 'XXX', # Autoincrement (e.g. for IDs)
    'O' : 'XXX', # Double, 8 bytes
    'G' : 'XXX', # OLE 10 digit/byte number of a .DBT block, stored as string
    '0' : 'XXX' # #Integer? based on dbf2sqlite mapping
}

# We still don't understand the primary keys for these tables, and so they
# can't be inserted yet...
dbfs_bad_pk = ['F1_84','F1_S0_FILING_LOG']

# These are the FERC Form 1 DB tables that we're focusing on initially.
ferc1_default_tables = ['f1_respondent_id',
                        'f1_fuel',
                        'f1_steam',
                        'f1_gnrt_plant',
                        'f1_hydro',
                        'f1_pumped_storage',
                        'f1_plant_in_srvce',
                        'f1_purchased_pwr' ]

# This is the set of tables which have been successfully integrated into PUDL:
ferc1_pudl_tables = ['f1_fuel',]

# This is the full set of tables that currently ingestible by the ferc1 DB:
ferc1_working_tables = ['f1_respondent_id',
                        'f1_fuel',
                        'f1_steam',
                        'f1_gnrt_plant',
                        'f1_hydro',
                        'f1_pumped_storage',
                        'f1_plant_in_srvce',
                        'f1_purchased_pwr' ]

# The set of FERC Form 1 tables that have the same composite primary keys: [
# respondent_id, report_year, report_prd, row_number, spplmnt_num ].
# TODO: THIS ONLY PERTAINS TO 2015 AND MAY NEED TO BE ADJUSTED BY YEAR...
ferc1_data_tables = [ 'f1_acb_epda', 'f1_accumdepr_prvsn', 'f1_accumdfrrdtaxcr',
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
    'f1_xmssn_line', 'f1_xtraordnry_loss', 'f1_audit_log', 'f1_privilege',
    'f1_hydro', 'f1_footnote_tbl', 'f1_steam', 'f1_leased', 'f1_sbsdry_detail',
    'f1_plant', 'f1_long_term_debt', 'f1_106_2009', 'f1_106a_2009',
    'f1_106b_2009', 'f1_208_elc_dep', 'f1_231_trn_stdycst', 'f1_324_elc_expns',
    'f1_325_elc_cust', 'f1_331_transiso', 'f1_338_dep_depl',
    'f1_397_isorto_stl', 'f1_398_ancl_ps', 'f1_399_mth_peak', 'f1_400_sys_peak',
    'f1_400a_iso_peak', 'f1_429_trans_aff', 'f1_allowances_nox',
    'f1_cmpinc_hedge_a', 'f1_cmpinc_hedge', 'f1_freeze', 'f1_rg_trn_srv_rev' ]

# Line numbers, and corresponding FERC account number
# and FERC account numbers from FERC Form 1 page 203, Electric
# Plant in Service.
ferc1_plant_in_service = {
    2: { 'ferc_acct': '301', 'acct_desc': 'intangible_organization' },
    3: { 'ferc_acct': '302', 'acct_desc': 'intangible_franchises_and_consents'},
    4: { 'ferc_acct': '303', 'acct_desc': 'intangible_misc_intangible_plant'},
    8: { 'ferc_acct': '310', 'acct_desc': 'production_land_and_land_rights'},
    9: { 'ferc_acct': '311', 'acct_desc': 'production_structures_and_improvements'},
    10: { 'ferc_acct': '312', 'acct_desc': 'production_boiler_plant_equipment'},
    11: { 'ferc_acct': '313', 'acct_desc': 'production_engines_and_engine-driven_generators'},
    12: { 'ferc_acct': '314', 'acct_desc': 'production_turbogenerator_units'},
    13: { 'ferc_acct': '315', 'acct_desc': 'production_accessory_electric_equipment'},
    14: { 'ferc_acct': '316', 'acct_desc': 'production_misc_power_plant_equipment'},
    15: { 'ferc_acct': '317', 'acct_desc': 'production_asset_retirement_costs'},
    18: { 'ferc_acct': '320', 'acct_desc': 'nuclear_land_and_land_rights'},
    19: { 'ferc_acct': '321', 'acct_desc': 'nuclear_structures_and_improvements'},
    20: { 'ferc_acct': '322', 'acct_desc': 'nuclear_reactor_plant_equipment'},
    21: { 'ferc_acct': '323', 'acct_desc': 'nuclear_turbogenerator_units'},
    22: { 'ferc_acct': '324', 'acct_desc': 'nuclear_accessory_electric_equipment'},
    23: { 'ferc_acct': '325', 'acct_desc': 'nuclear_misc_power_plant_equipment'},
    24: { 'ferc_acct': '326', 'acct_desc': 'nuclear_asset_retirement_costs'},
    27: { 'ferc_acct': '330', 'acct_desc': 'hydraulic_land_and_land_rights'},
    28: { 'ferc_acct': '331', 'acct_desc': 'hydraulic_structures_and_improvements'},
    29: { 'ferc_acct': '332', 'acct_desc': 'hydraulic_reservoirs_dams_waterways'},
    30: { 'ferc_acct': '333', 'acct_desc': 'hydraulic_water_wheels_turbines_generators'},
    31: { 'ferc_acct': '334', 'acct_desc': 'hydraulic_accessory_electric_equipment'},
    32: { 'ferc_acct': '335', 'acct_desc': 'hydraulic_misc_power_plant_equipment'},
    33: { 'ferc_acct': '336', 'acct_desc': 'hydraulic_roads_railroads_bridges'},
    34: { 'ferc_acct': '337', 'acct_desc': 'hydraulic_asset_retirement_costs'},
    37: { 'ferc_acct': '340', 'acct_desc': 'other_land_and_land_rights'},
    38: { 'ferc_acct': '341', 'acct_desc': 'other_structures_and_improvements'},
    39: { 'ferc_acct': '342', 'acct_desc': 'other_fuel_holders_products_accessories'},
    40: { 'ferc_acct': '343', 'acct_desc': 'other_prime_movers'},
    41: { 'ferc_acct': '344', 'acct_desc': 'other_generators'},
    42: { 'ferc_acct': '345', 'acct_desc': 'other_accessory_electric_equipment'},
    43: { 'ferc_acct': '346', 'acct_desc': 'other_misc_power_plant_equipment'},
    44: { 'ferc_acct': '347', 'acct_desc': 'other_asset_retirement_costs'},
    48: { 'ferc_acct': '350', 'acct_desc': 'transmission_land_and_land_rights'},
    49: { 'ferc_acct': '352', 'acct_desc': 'transmission_structures_and_improvements'},
    50: { 'ferc_acct': '353', 'acct_desc': 'transmission_station_equipment'},
    51: { 'ferc_acct': '354', 'acct_desc': 'transmission_towers_and_fixtures'},
    52: { 'ferc_acct': '355', 'acct_desc': 'transmission_poles_and_fixtures'},
    53: { 'ferc_acct': '356', 'acct_desc': 'transmission_overhead_conductors'},
    54: { 'ferc_acct': '357', 'acct_desc': 'transmission_underground_conduit'},
    55: { 'ferc_acct': '358', 'acct_desc': 'transmission_underground_conductors'},
    56: { 'ferc_acct': '359', 'acct_desc': 'transmission_roads_and_trails'},
    57: { 'ferc_acct': '359.1', 'acct_desc': 'transmission_asset_retirement_costs'},
    60: { 'ferc_acct': '360', 'acct_desc': 'distribution_land_and_land_rights'},
    61: { 'ferc_acct': '361', 'acct_desc': 'distribution_structures_and_improvements'},
    62: { 'ferc_acct': '362', 'acct_desc': 'distribution_station_equipment'},
    63: { 'ferc_acct': '363', 'acct_desc': 'distribution_storage_battery_equipment'},
    64: { 'ferc_acct': '364', 'acct_desc': 'distribution_poles_towers_and_fixtures'},
    65: { 'ferc_acct': '365', 'acct_desc': 'distribution_overhead_conductors'},
    66: { 'ferc_acct': '366', 'acct_desc': 'distribution_underground_conduit'},
    67: { 'ferc_acct': '367', 'acct_desc': 'distribution_underground_conductors'},
    68: { 'ferc_acct': '368', 'acct_desc': 'distribution_line_transformers'},
    69: { 'ferc_acct': '369', 'acct_desc': 'distribution_services'},
    70: { 'ferc_acct': '370', 'acct_desc': 'distribution_meters'},
    71: { 'ferc_acct': '371', 'acct_desc': 'distribution_installations_customer_premises'},
    72: { 'ferc_acct': '372', 'acct_desc': 'distribution_leased_property_customer_premises'},
    73: { 'ferc_acct': '373', 'acct_desc': 'distribution_street_lighting_and_signals'},
    74: { 'ferc_acct': '374', 'acct_desc': 'distribution_asset_retirement_costs'},
    77: { 'ferc_acct': '380', 'acct_desc': 'regional_transmission_land_and_land_rights'},
    78: { 'ferc_acct': '381', 'acct_desc': 'regional_transmission_structures_and_improvements'},
    79: { 'ferc_acct': '382', 'acct_desc': 'regional_transmission_computer_hardware'},
    80: { 'ferc_acct': '383', 'acct_desc': 'regional_transmission_computer_software'},
    81: { 'ferc_acct': '384', 'acct_desc': 'regional_transmission_communication_equipment'},
    82: { 'ferc_acct': '385', 'acct_desc': 'regional_transmission_misc_transmission_and_market'},
    83: { 'ferc_acct': '386', 'acct_desc': 'regional_transmission_asset_retirement_costs'},
    86: { 'ferc_acct': '389', 'acct_desc': 'general_plant_land_and_land_rights'},
    87: { 'ferc_acct': '390', 'acct_desc': 'general_plant_structures_and_improvements'},
    88: { 'ferc_acct': '391', 'acct_desc': 'general_plant_office_furniture_and_equipment'},
    89: { 'ferc_acct': '392', 'acct_desc': 'general_plant_transportation_equipment'},
    90: { 'ferc_acct': '393', 'acct_desc': 'general_plant_stores_equipment'},
    91: { 'ferc_acct': '394', 'acct_desc': 'general_plant_tools_shop_garage_equipment'},
    92: { 'ferc_acct': '395', 'acct_desc': 'general_plant_laboratory_equipment'},
    93: { 'ferc_acct': '396', 'acct_desc': 'general_plant_power_operated_equipment'},
    94: { 'ferc_acct': '397', 'acct_desc': 'general_plant_communication_equipment'},
    95: { 'ferc_acct': '398', 'acct_desc': 'general_plant_misc_equipment'},
    97: { 'ferc_acct': '399', 'acct_desc': 'general_plant_other_tangible_property'},
    98: { 'ferc_acct': '399.1', 'acct_desc': 'general_plant_asset_retirement_costs'},
    101: { 'ferc_acct': '102', 'acct_desc': 'electric_plant_purchased'},
    #102: { 'ferc_acct': '102', 'acct_desc': 'electric_plant_sold'}, The FERC Account number is (Less)(102) - not sure how to handle, seems the value should be treated as a negative
    103: { 'ferc_acct': '103', 'acct_desc': 'experimental_plant_unclassified'}
}

# no totals in the above plant_in_service dictionary

census_region = {
  'NEW':'New England',
  'MAT':'Middle Atlantic',
  'SAT':'South Atlantic',
  'ESC':'East South Central',
  'WSC':'West South Central',
  'ENC':'East North Central',
  'WNC':'West North Central',
  'MTN':'Mountain',
  'PACC':'Pacific Contiguous (OR, WA, CA)',
  'PACN':'Pacific Non-Contiguous (AK, HI)',
}

nerc_region = {
  'NPCC':'Northeast Power Coordinating Council',
  'MRO':'Midwest Reliability Organization',
  'SERC':'SERC Reliability Corporation',
  'RFC':'Reliability First Corporation',
  'SPP':'Southwest Power Pool',
  'TRE':'Texas Regional Entity',
  'FRCC':'Florida Reliability Coordinating Council',
  'WECC':'Western Electricity Coordinating Council'
}

eia_sector = {
    #Traditional regulated electric utilities
    '1':'Electric Utility',

    #Independent power producers which are not cogenerators
    '2':'NAICS-22 Non-Cogen',

    #Independent power producers which are cogenerators, but whose
    # primary business purpose is the sale of electricity to the public
    '3':'NAICS-22 Cogen',

    # Commercial non-cogeneration facilities that produce electric power,
    # are connected to the gird, and can sell power to the public
    '4':'Commercial NAICS Non-Cogen',

    # Commercial cogeneration facilities that produce electric power, are
    # connected to the grid, and can sell power to the public
    '5':'Commercial NAICS Cogen',

    # Industrial non-cogeneration facilities that produce electric power, are
    # connected to the gird, and can sell power to the public
    '6':'Industrial NAICS Non-Cogen',

    # Industrial cogeneration facilities that produce electric power, are
    # connected to the gird, and can sell power to the public
    '7':'Industrial NAICS Cogen'
}

# EIA 923: EIA Type of prime mover:
eia_reported_prime_mover = {
  'BA':'Energy Storage, Battery',
  'BT':'Turbines Used in a Binary Cycle. \
        Including those used for geothermal applications',
  'CA':'Combined-Cycle -- Steam Part',
  'CE':'Energy Storage, Compressed Air',
  'CP':'Energy Storage, Concentrated Solar Power',
  'CS':'Combined-Cycle Single-Shaft Combustion \
        Turbine and Steam Turbine share of single',
  'CT':'Combined-Cycle Combustion Turbine Part',
  'ES':'Energy Storage, Other (Specify on Schedule 9, Comments)',
  'FC':'Fuel Cell',
  'FW':'Energy Storage, Flywheel',
  'GT':'Combustion (Gas) Turbine. Including Jet Engine design',
  'HA':'Hydrokinetic, Axial Flow Turbine',
  'HB':'Hydrokinetic, Wave Buoy',
  'HK':'Hydrokinetic, Other',
  'HY':'Hydraulic Turbine. Including turbines associated \
        with delivery of water by pipeline.',
  'IC':'Internal Combustion (diesel, piston, reciprocating) Engine',
  'PS':'Energy Storage, Reversible Hydraulic Turbine (Pumped Storage)',
  'OT':'Other',
  'ST':'Steam Turbine. Including Nuclear, Geothermal, and \
        Solar Steam (does not include Combined Cycle).',
  'PV':'Photovoltaic',
  'WT':'Wind Turbine, Onshore',
  'WS':'Wind Turbine, Offshore'
}

# EIA 923: The fuel code reported to EIA.Two or three letter alphanumeric:
eia_reported_fuel_type_code = {
  'AB':'Agricultural By-Products',
  'ANT':'Anthracite Coal',
  'BFG':'Blast Furnace Gas',
  'BIT':'Bituminous Coal',
  'BLQ':'Black Liquor',
  'DFO':'Distillate Fuel Oil. Including diesel, No. 1, No. 2, and No. 4 fuel oils.',
  'GEO':'Geothermal',
  'JF':'Jet Fuel',
  'KER':'Kerosene',
  'LFG':'Landfill Gas',
  'LIG':'Lignite Coal',
  'MSB':'Biogenic Municipal Solid Waste',
  'MSN':'Non-biogenic Municipal Solid Waste',
  'MWH':'Electricity used for energy storage',
  'NG':'Natural Gas',
  'NUC':'Nuclear. Including Uranium, Plutonium, and Thorium.',
  'OBG':'Other Biomass Gas. Including digester gas, methane, and other biomass gases.',
  'OBL':'Other Biomass Liquids',
  'OBS':'Other Biomass Solids',
  'OG':'Other Gas',
  'OTH':'Other Fuel',
  'PC':'Petroleum Coke',
  'PG':'Gaseous Propane',
  'PUR':'Purchased Steam',
  'RC':'Refined Coal',
  'RFO':'Residual Fuel Oil. Including No. 5 & 6 fuel oils and \
         bunker C fuel oil.',
  'SC':'Coal-based Synfuel. Including briquettes, pellets, or \
        extrusions, which are formed by binding materials or \
        processes that recycle materials.',
  'SGC':'Coal-Derived Synthesis Gas',
  'SGP':'Synthesis Gas from Petroleum Coke',
  'SLW':'Sludge Waste',
  'SUB':'Subbituminous Coal',
  'SUN':'Solar',
  'TDF':'Tire-derived Fuels',
  'WAT':'Water at a Conventional Hydroelectric Turbine and \
         water used in Wave Buoy Hydrokinetic Technology, \
         current Hydrokinetic Technology, Tidal Hydrokinetic Technology, and \
         Pumping Energy for Reversible (Pumped Storage) Hydroelectric Turbines.',
  'WC':'Waste/Other Coal. Including anthracite culm, bituminous gob, \
        fine coal, lignite waste, waste coal.',
  'WDL':'Wood Waste Liquids, excluding Black Liquor. Including red liquor, \
         sludge wood, spent sulfite liquor, and other wood-based liquids.',
  'WDS':'Wood/Wood Waste Solids. Including paper pellets, \
         railroad ties, utility polies, wood chips, bark, and \
         other wood waste solids.',
  'WH':'Waste Heat not directly attributed to a fuel source',
  'WND':'Wind',
  'WO':'Waste/Other Oil. Including crude oil, liquid butane, \
        liquid propane, naphtha, oil waste, re-refined moto oil, \
        sludge oil, tar oil, or other petroleum-based liquid wastes.'
}

# EIA 923: A partial aggregation of the reported fuel type codes into
# larger categories used by EIA in, for example,
# the Annual Energy Review (AER).Two or three letter alphanumeric.
# See the Fuel Code table (Table 5), below:
eia_aer_fuel_type_code = {
  'SUN':'Solar PV and thermal',
  'COL':'Coal',
  'DFO':'Distillate Petroleum',
  'GEO':'Geothermal',
  'HPS':'Hydroelectric Pumped Storage',
  'HYC':'Hydroelectric Conventional',
  'MLG':'Biogenic Municipal Solid Waste and Landfill Gas',
  'NG':'Natural Gas',
  'NUC':'Nuclear',
  'OOG':'Other Gases',
  'ORW':'Other Renewables',
  'OTH':'Other (including nonbiogenic MSW)',
  'PC':'Petroleum Coke',
  'RFO':'Residual Petroleum',
  'WND':'Wind',
  'WOC':'Waste Coal',
  'WOO':'Waste Oil',
  'WWW':'Wood and Wood Waste'
}

# EIA 923: EIA The type of physical units fuel consumption is reported in.
# All consumption is reported in either short tons for solids,
# thousands of cubic feet for gases, and barrels for liquids:"""
eia_physical_label_unit = {
  'mcf':'for gases',
  'short tons':'for solid',
  'barrels':'for liquids'
}
# EIA 923: Designates the purchase type under which receipts occurred
# in the reporting month. One or two character alphanumeric:
eia_contract_type = {
  'C':'Contract',
  'NC':'New Contract',
  'S':'Spot Purchase'
}
# EIA 923: The fuel code associated with the fuel receipt.
# Two or three character alphanumeric:
eia_energy_source = {
    'ANT':'Anthracite Coal',
    'BIT':'Bituminous Coal',
    'DFO':'Distillate Fuel Oil. Including diesel,\
           No. 1, No. 2, and No. 4 fuel oils.',
    'LIG':'Lignite Coal',
    'NG':'Natural Gas',
    'PC':'Petroleum Coke',
    'RC':'Refined Coal',
    'RFO':'Residual Fuel Oil. Including \
           No. 5 & 6 fuel oils and bunker C fuel oil.',
    'SC':'Coal-based Synfuel. Including briquettes, pellets, or \
          extrusions, which are formed by binding materials or \
          processes that recycle materials.',
    'SUB':'Subbituminous Coal',
    'WC':'Waste/Other Coal. Including anthracite culm, bituminous gob, fine\
          coal, lignite waste, waste coal.',
    'WO':'Waste/Other Oil. Including crude oil, liquid butane, liquid propane,\
          naphtha, oil waste, re-refined moto oil, sludge oil, tar oil, or\
          other petroleum-based liquid wastes.'
}

# EIA 923: Type of Coal Mine
eia_coalmine_type = {
  'P':'Preperation Plant',
  'S':'Surface',
  'U':'Underground',
  'U/S':'Both an underground and surface mine with \
  most coal extracted from underground',
  'S/U':'Both an underground and surface mine with \
  most coal extracted from surface'
}

# EIA 923: State abbreviation related to coal mine location.
# Country abbreviations are also listed under this category and are as follows:

eia_coalmine_state = {
  'AU':'Australia',
  'CL':'Columbia',
  'CN':'Canada',
  'IS':'Indonesia',
  'PL':'Poland',
  'RS':'Russia',
  'UK':'United Kingdom',
  'VZ':'Venezula',
  'OC':'Other Country'
}

#EIA 923: Non-Regulated Independent Power Plant or Nonutility Plant.
eia_regulated = {
  'REG':'Regulated Electric Utility',
  'UNR':'Non-Regulated Independent Power Plant or Nonutility Plant.'
}

#EIA 923: One character designates the reporting
#frequency for the plant. Alphanumeric:
eia_respondent_frequency = {
  'M':'Monthly respondent',
  'A':'Annual respondent'
}
#EIA 923: Mode for the longest / second longest distance.
eia_primary_secondary_mode_of_transporation = {
  'RR':'Rail: Shipments of fuel moved to consumers by rail \
        (private or public/commercial). Included is coal hauled to or \
        away from a railroad siding by truck if the truck did not use public\
        roads.',
  'RV':'River:  Shipments of fuel moved to consumers via river by barge.  \
        Not included are shipments to Great Lakes coal loading docks, \
        tidewater piers, or coastal ports.',
  'GL':'Great Lakes:  Shipments of coal moved to consumers via \
        the Great Lakes. These shipments are moved via the Great Lakes \
        coal loading docks, which are identified by name and location as follows:  \
        Conneaut Coal Storage & Transfer, Conneaut, Ohio;  \
        NS Coal Dock (Ashtabula Coal Dock), Ashtabula, Ohio;  \
        Sandusky Coal Pier, Sandusky, Ohio;  Toledo Docks, Toledo, Ohio; \
        KCBX Terminals Inc., Chicago, Illinois;  \
        Superior Midwest Energy Terminal, Superior, Wisconsin',
  'TP':'Tidewater Piers and Coastal Ports:  Shipments of coal moved to \
        Tidewater Piers and Coastal Ports for further shipments to consumers via \
        coastal water or ocean.  The Tidewater Piers and Coastal Ports are \
        identified by name and location as follows:  Dominion Terminal Associates, \
        Newport News, Virginia; McDuffie Coal Terminal, Mobile, Alabama;  \
        IC Railmarine Terminal, Convent, Louisiana;  \
        International Marine Terminals, Myrtle Grove, Louisiana;  \
        Cooper/T. Smith Stevedoring Co. Inc., Darrow, Louisiana;  \
        Seward Terminal Inc., Seward, Alaska;  Los Angeles Export Terminal, Inc., \
        Los Angeles, California;  Levin-Richmond Terminal Corp., \
        Richmond, California; Baltimore Terminal, Baltimore, Maryland;  \
        Norfolk Southern Lamberts Point P-6, Norfolk, Virginia;  \
        Chesapeake Bay Piers, Baltimore, Maryland;  Pier IX Terminal Company, \
        Newport News, Virginia;  Electro-Coal Transport Corp., Davant, Louisiana',
  'WT':'Water: Shipments of fuel moved to consumers by other waterways.',
  'TR':'Truck: Shipments of fuel moved to consumers by truck.  \
        Not included is fuel hauled to or away from a railroad siding by \
        truck on non-public roads.',
  'TC':'Tramway/Conveyor: Shipments of fuel moved to consumers \
        by tramway or conveyor.',
  'SP':'Slurry Pipeline: Shipments of coal moved to consumers \
        by slurry pipeline.',
  'PL':'Pipeline: Shipments of fuel moved to consumers by pipeline'
}

# EIA 923: Contract type for natural gas capacity service:
eia_natural_gas_transportation_service = {
  'F':'Firm',
  'I':'Interruptible'
}
