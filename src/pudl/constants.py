"""
A warehouse for constant values required to initilize the PUDL Database.

This constants module stores and organizes a bunch of constant values which are
used throughout PUDL to populate static lists within the data packages or for
data cleaning purposes.
"""

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
    'N': 'New Contract - see NC code. This abbreviation existed only in 2008 before being replaced by NC.',
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
    'OT': 'other_country',
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
         'iso_rto_code', 'latitude', 'longitude', 'service_area',
         'plant_name_eia', 'primary_purpose_naics_id',
         'sector_id', 'sector_name', 'state', 'street_address', 'zip_code'],
        # annual cols
        ['ash_impoundment', 'ash_impoundment_lined', 'ash_impoundment_status',
         'datum', 'energy_storage', 'ferc_cogen_docket_no', 'water_source',
         'ferc_exempt_wholesale_generator_docket_no',
         'ferc_small_power_producer_docket_no',
         'liquefied_natural_gas_storage',
         'natural_gas_local_distribution_company', 'natural_gas_storage',
         'natural_gas_pipeline_name_1', 'natural_gas_pipeline_name_2',
         'natural_gas_pipeline_name_3', 'nerc_region', 'net_metering',
         'pipeline_notes', 'regulatory_status_code',
         'transmission_distribution_owner_id',
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
         'ownership_code', 'owned_by_non_utility', 'deliver_power_transgrid',
         'summer_capacity_mw', 'winter_capacity_mw', 'summer_capacity_estimate',
         'winter_capacity_estimate', 'minimum_load_mw', 'distributed_generation',
         'technology_description', 'reactive_power_output_mvar',
         'energy_source_code_1', 'energy_source_code_2',
         'energy_source_code_3', 'energy_source_code_4',
         'energy_source_code_5', 'energy_source_code_6',
         'energy_source_1_transport_1', 'energy_source_1_transport_2',
         'energy_source_1_transport_3', 'energy_source_2_transport_1',
         'energy_source_2_transport_2', 'energy_source_2_transport_3',
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
         'winter_estimated_capability_mw', 'retirement_date',
         'utility_id_eia', 'data_source'],
        # need type fixing
        {}
    ],
    # utilities must come after plants. plant location needs to be
    # removed before the utility locations are compiled
    'utilities': [
        # base cols
        ['utility_id_eia'],
        # static cols
        ['utility_name_eia'],
        # annual cols
        ['street_address', 'city', 'state', 'zip_code', 'entity_type',
         'plants_reported_owner', 'plants_reported_operator',
         'plants_reported_asset_manager', 'plants_reported_other_relationship',
         'attention_line', 'address_2', 'zip_code_4',
         'contact_firstname', 'contact_lastname', 'contact_title',
         'contact_firstname_2', 'contact_lastname_2', 'contact_title_2',
         'phone_extension_1', 'phone_extension_2', 'phone_number_1',
         'phone_number_2'],
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

epacems_tables = ("hourly_emissions_epacems")
"""tuple: A tuple containing tables of EPA CEMS data to pull into PUDL.
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
    'eia860': tuple(range(2001, 2020)),
    'eia861': tuple(range(1990, 2020)),
    'eia923': tuple(range(2001, 2020)),
    'epacems': tuple(range(1995, 2021)),
    'epaipm': (None, ),
    'ferc1': tuple(range(1994, 2020)),
    'ferc714': (None, ),
}
"""
dict: A dictionary of data sources (keys) and tuples containing the years
    that we expect to be able to download for each data source (values).
"""

# The full set of years we currently expect to be able to ingest, per source:
working_partitions = {
    'eia860': {
        'years': tuple(range(2004, 2020))
    },
    'eia860m': {
        'year_month': '2020-11'
    },
    'eia861': {
        'years': tuple(range(2001, 2020))
    },
    'eia923': {
        'years': tuple(range(2001, 2020))
    },
    'epacems': {
        'years': tuple(range(1995, 2021)),
        'states': tuple(cems_states.keys())},
    'ferc1': {
        'years': tuple(range(1994, 2020))
    },
    'ferc714': {},
}
"""
dict: A dictionary of data sources (keys) and dictionaries (values) of names of
    partition type (sub-key) and paritions (sub-value) containing the paritions
    such as tuples of years for each data source that are able to be ingested
    into PUDL.
"""

pudl_tables = {
    'eia860': eia860_pudl_tables,
    'eia861': (
        "service_territory_eia861",
        "balancing_authority_eia861",
        "sales_eia861",
        "advanced_metering_infrastructure_eia861",
        "demand_response_eia861",
        "demand_side_management_eia861",
        "distributed_generation_eia861",
        "distribution_systems_eia861",
        "dynamic_pricing_eia861",
        "energy_efficiency_eia861",
        "green_pricing_eia861",
        "mergers_eia861",
        "net_metering_eia861",
        "non_net_metering_eia861",
        "operational_data_eia861",
        "reliability_eia861",
        "utility_data_eia861",
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
    'ferc714': [
        'electricity', 'electric', 'utility', 'planning area', 'form 714',
        'balancing authority', 'demand', 'system lambda', 'ferc',
        'federal energy regulatory commission', "hourly", "generation",
        "interchange", "forecast", "load", "adjacency", "plants",
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

REVENUE_CLASSES = [
    'retail_sales',
    'unbundled',
    'delivery_customers',
    'sales_for_resale',
    'credits_or_adjustments',
    'other',
    'transmission',
    'total',
]

RELIABILITY_STANDARDS = [
    'ieee_standard',
    'other_standard'
]

FUEL_CLASSES = [
    'gas',
    'oil',
    'other',
    'renewable',
    'water',
    'wind',
    'wood',
]

RTO_CLASSES = [
    'caiso',
    'ercot',
    'pjm',
    'nyiso',
    'spp',
    'miso',
    'isone',
    'other'
]

ESTIMATED_OR_ACTUAL = {'E': 'estimated', 'A': 'actual'}

TRANSIT_TYPE_DICT = {
    'CV': 'conveyer',
    'PL': 'pipeline',
    'RR': 'railroad',
    'TK': 'truck',
    'WA': 'water',
    'UN': 'unknown',
}

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
        'address_2': pd.StringDtype(),  # Added by AES for 860 utilities table
        'advanced_metering_infrastructure': pd.Int64Dtype(),  # Added by AES for AMI table
        # Added by AES for UD misc table
        'alternative_fuel_vehicle_2_activity': pd.BooleanDtype(),
        'alternative_fuel_vehicle_activity': pd.BooleanDtype(),
        'annual_indirect_program_cost': float,
        'annual_total_cost': float,
        'ash_content_pct': float,
        'ash_impoundment': pd.BooleanDtype(),
        'ash_impoundment_lined': pd.BooleanDtype(),
        # TODO: convert this field to more descriptive words
        'ash_impoundment_status': pd.StringDtype(),
        'associated_combined_heat_power': pd.BooleanDtype(),
        'attention_line': pd.StringDtype(),
        'automated_meter_reading': pd.Int64Dtype(),  # Added by AES for AMI table
        'backup_capacity_mw': float,  # Added by AES for NNM & DG misc table
        'balancing_authority_code_eia': pd.CategoricalDtype(),
        'balancing_authority_id_eia': pd.Int64Dtype(),
        'balancing_authority_name_eia': pd.StringDtype(),
        'bga_source': pd.StringDtype(),
        'boiler_id': pd.StringDtype(),
        'bunded_activity': pd.BooleanDtype(),
        'business_model': pd.CategoricalDtype(categories=[
            "retail", "energy_services"]),
        'buy_distribution_activity': pd.BooleanDtype(),
        'buying_transmission_activity': pd.BooleanDtype(),
        'bypass_heat_recovery': pd.BooleanDtype(),
        'caidi_w_major_event_days_minus_loss_of_service_minutes': float,
        'caidi_w_major_event_dats_minutes': float,
        'caidi_wo_major_event_days_minutes': float,
        'capacity_mw': float,
        'carbon_capture': pd.BooleanDtype(),
        'chlorine_content_ppm': float,
        'circuits_with_voltage_optimization': pd.Int64Dtype(),
        'city': pd.StringDtype(),
        'cofire_fuels': pd.BooleanDtype(),
        'consumed_by_facility_mwh': float,
        'consumed_by_respondent_without_charge_mwh': float,
        'contact_firstname': pd.StringDtype(),
        'contact_firstname_2': pd.StringDtype(),
        'contact_lastname': pd.StringDtype(),
        'contact_lastname_2': pd.StringDtype(),
        'contact_title': pd.StringDtype(),
        'contact_title_2': pd.StringDtype(),
        'contract_expiration_date': 'datetime64[ns]',
        'contract_type_code': pd.StringDtype(),
        'county': pd.StringDtype(),
        'county_id_fips': pd.StringDtype(),  # Must preserve leading zeroes
        'credits_or_adjustments': float,
        'critical_peak_pricing': pd.BooleanDtype(),
        'critical_peak_rebate': pd.BooleanDtype(),
        'current_planned_operating_date': 'datetime64[ns]',
        'customers': float,
        'customer_class': pd.CategoricalDtype(categories=CUSTOMER_CLASSES),
        'customer_incentives_cost': float,
        'customer_incentives_incremental_cost': float,
        'customer_incentives_incremental_life_cycle_cost': float,
        'customer_other_costs_incremental_life_cycle_cost': float,
        'daily_digital_access_customers': pd.Int64Dtype(),
        'data_observed': pd.BooleanDtype(),
        'datum': pd.StringDtype(),
        'deliver_power_transgrid': pd.BooleanDtype(),
        'delivery_customers': float,
        'direct_load_control_customers': pd.Int64Dtype(),
        'distributed_generation': pd.BooleanDtype(),
        'distributed_generation_owned_capacity_mw': float,
        'distribution_activity': pd.BooleanDtype(),
        'distribution_circuits': pd.Int64Dtype(),
        'duct_burners': pd.BooleanDtype(),
        'energy_displaced_mwh': float,
        'energy_efficiency_annual_cost': float,
        'energy_efficiency_annual_actual_peak_reduction_mw': float,
        'energy_efficiency_annual_effects_mwh': float,
        'energy_efficiency_annual_incentive_payment': float,
        'energy_efficiency_incremental_actual_peak_reduction_mw': float,
        'energy_efficiency_incremental_effects_mwh': float,
        'energy_savings_estimates_independently_verified': pd.BooleanDtype(),
        'energy_savings_independently_verified': pd.BooleanDtype(),
        'energy_savings_mwh': float,
        'energy_served_ami_mwh': float,
        'energy_source_1_transport_1': pd.CategoricalDtype(categories=TRANSIT_TYPE_DICT.values()),
        'energy_source_1_transport_2': pd.CategoricalDtype(categories=TRANSIT_TYPE_DICT.values()),
        'energy_source_1_transport_3': pd.CategoricalDtype(categories=TRANSIT_TYPE_DICT.values()),
        'energy_source_2_transport_1': pd.CategoricalDtype(categories=TRANSIT_TYPE_DICT.values()),
        'energy_source_2_transport_2': pd.CategoricalDtype(categories=TRANSIT_TYPE_DICT.values()),
        'energy_source_2_transport_3': pd.CategoricalDtype(categories=TRANSIT_TYPE_DICT.values()),
        'energy_source_code': pd.StringDtype(),
        'energy_source_code_1': pd.StringDtype(),
        'energy_source_code_2': pd.StringDtype(),
        'energy_source_code_3': pd.StringDtype(),
        'energy_source_code_4': pd.StringDtype(),
        'energy_source_code_5': pd.StringDtype(),
        'energy_source_code_6': pd.StringDtype(),
        'energy_storage': pd.BooleanDtype(),
        'entity_type': pd.CategoricalDtype(categories=ENTITY_TYPE_DICT.values()),
        'estimated_or_actual_capacity_data': pd.CategoricalDtype(categories=ESTIMATED_OR_ACTUAL.values()),
        'estimated_or_actual_fuel_data': pd.CategoricalDtype(categories=ESTIMATED_OR_ACTUAL.values()),
        'estimated_or_actual_tech_data': pd.CategoricalDtype(categories=ESTIMATED_OR_ACTUAL.values()),
        'exchange_energy_delivered_mwh': float,
        'exchange_energy_recieved_mwh': float,
        'ferc_cogen_docket_no': pd.StringDtype(),
        'ferc_cogen_status': pd.BooleanDtype(),
        'ferc_exempt_wholesale_generator': pd.BooleanDtype(),
        'ferc_exempt_wholesale_generator_docket_no': pd.StringDtype(),
        'ferc_small_power_producer': pd.BooleanDtype(),
        'ferc_small_power_producer_docket_no': pd.StringDtype(),
        'fluidized_bed_tech': pd.BooleanDtype(),
        'fraction_owned': float,
        'fuel_class': pd.StringDtype(),
        'fuel_consumed_for_electricity_mmbtu': float,
        'fuel_consumed_for_electricity_units': float,
        'fuel_consumed_mmbtu': float,
        'fuel_consumed_units': float,
        'fuel_cost_per_mmbtu': float,
        'fuel_group_code': pd.StringDtype(),
        'fuel_group_code_simple': pd.StringDtype(),
        'fuel_mmbtu_per_unit': float,
        'fuel_pct': float,
        'fuel_qty_units': float,
        # are fuel_type and fuel_type_code the same??
        # fuel_type includes 40 code-like things.. WAT, SUN, NUC, etc.
        'fuel_type': pd.StringDtype(),
        # from the boiler_fuel_eia923 table, there are 30 code-like things, like NG, BIT, LIG
        'fuel_type_code': pd.StringDtype(),
        'fuel_type_code_aer': pd.StringDtype(),
        'fuel_type_code_pudl': pd.StringDtype(),
        'furnished_without_charge_mwh': float,
        'generation_activity': pd.BooleanDtype(),
        # this is a mix of integer-like values (2 or 5) and strings like AUGSF
        'generator_id': pd.StringDtype(),
        'generators_number': float,
        'generators_num_less_1_mw': float,
        'green_pricing_revenue': float,
        'grid_voltage_2_kv': float,
        'grid_voltage_3_kv': float,
        'grid_voltage_kv': float,
        'heat_content_mmbtu_per_unit': float,
        'highest_distribution_voltage_kv': float,
        'home_area_network': pd.Int64Dtype(),
        'inactive_accounts_included': pd.BooleanDtype(),
        'incremental_energy_savings_mwh': float,
        'incremental_life_cycle_energy_savings_mwh': float,
        'incremental_life_cycle_peak_reduction_mwh': float,
        'incremental_peak_reduction_mw': float,
        'iso_rto_code': pd.StringDtype(),
        'latitude': float,
        'liquefied_natural_gas_storage': pd.BooleanDtype(),
        'load_management_annual_cost': float,
        'load_management_annual_actual_peak_reduction_mw': float,
        'load_management_annual_effects_mwh': float,
        'load_management_annual_incentive_payment': float,
        'load_management_annual_potential_peak_reduction_mw': float,
        'load_management_incremental_actual_peak_reduction_mw': float,
        'load_management_incremental_effects_mwh': float,
        'load_management_incremental_potential_peak_reduction_mw': float,
        'longitude': float,
        'major_program_changes': pd.BooleanDtype(),
        'mercury_content_ppm': float,
        'merge_address': pd.StringDtype(),
        'merge_city': pd.StringDtype(),
        'merge_company': pd.StringDtype(),
        'merge_date': 'datetime64[ns]',
        'merge_state': pd.StringDtype(),
        'mine_id_msha': pd.Int64Dtype(),
        'mine_id_pudl': pd.Int64Dtype(),
        'mine_name': pd.StringDtype(),
        'mine_type_code': pd.StringDtype(),
        'minimum_load_mw': float,
        'moisture_content_pct': float,
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
        'nerc_regions_of_operation': pd.CategoricalDtype(categories=RECOGNIZED_NERC_REGIONS),
        'net_generation_mwh': float,
        'net_metering': pd.BooleanDtype(),
        'net_power_exchanged_mwh': float,
        'net_wheeled_power_mwh': float,
        'new_parent': pd.StringDtype(),
        'non_amr_ami': pd.Int64Dtype(),
        'nuclear_unit_id': pd.Int64Dtype(),
        'operates_generating_plant': pd.BooleanDtype(),
        'operating_date': 'datetime64[ns]',
        'operating_switch': pd.StringDtype(),
        # TODO: double check this for early 860 years
        'operational_status': pd.StringDtype(),
        'operational_status_code': pd.StringDtype(),
        'original_planned_operating_date': 'datetime64[ns]',
        'other': float,
        'other_combustion_tech': pd.BooleanDtype(),
        'other_costs': float,
        'other_costs_incremental_cost': float,
        'other_modifications_date': 'datetime64[ns]',
        'other_planned_modifications': pd.BooleanDtype(),
        'outages_recorded_automatically': pd.BooleanDtype(),
        'owned_by_non_utility': pd.BooleanDtype(),
        'owner_city': pd.StringDtype(),
        'owner_name': pd.StringDtype(),
        'owner_state': pd.StringDtype(),
        'owner_street_address': pd.StringDtype(),
        'owner_utility_id_eia': pd.Int64Dtype(),
        'owner_zip_code': pd.StringDtype(),
        # we should transition these into readable codes, not a one letter thing
        'ownership_code': pd.StringDtype(),
        'phone_extension_1': pd.StringDtype(),
        'phone_extension_2': pd.StringDtype(),
        'phone_number_1': pd.StringDtype(),
        'phone_number_2': pd.StringDtype(),
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
        'plant_id_epa': pd.Int64Dtype(),
        'plant_id_pudl': pd.Int64Dtype(),
        'plant_name_eia': pd.StringDtype(),
        'plants_reported_asset_manager': pd.BooleanDtype(),
        'plants_reported_operator': pd.BooleanDtype(),
        'plants_reported_other_relationship': pd.BooleanDtype(),
        'plants_reported_owner': pd.BooleanDtype(),
        'point_source_unit_id_epa': pd.StringDtype(),
        'potential_peak_demand_savings_mw': float,
        'pulverized_coal_tech': pd.BooleanDtype(),
        'previously_canceled': pd.BooleanDtype(),
        'price_responsive_programes': pd.BooleanDtype(),
        'price_responsiveness_customers': pd.Int64Dtype(),
        'primary_transportation_mode_code': pd.StringDtype(),
        'primary_purpose_naics_id': pd.Int64Dtype(),
        'prime_mover_code': pd.StringDtype(),
        'pv_current_flow_type': pd.CategoricalDtype(categories=['AC', 'DC']),
        'reactive_power_output_mvar': float,
        'real_time_pricing_program': pd.BooleanDtype(),
        'rec_revenue': float,
        'rec_sales_mwh': float,
        'regulatory_status_code': pd.StringDtype(),
        'report_date': 'datetime64[ns]',
        'reported_as_another_company': pd.StringDtype(),
        'retail_marketing_activity': pd.BooleanDtype(),
        'retail_sales': float,
        'retail_sales_mwh': float,
        'retirement_date': 'datetime64[ns]',
        'revenue_class': pd.CategoricalDtype(categories=REVENUE_CLASSES),
        'rto_iso_lmp_node_id': pd.StringDtype(),
        'rto_iso_location_wholesale_reporting_id': pd.StringDtype(),
        'rtos_of_operation': pd.StringDtype(),
        'saidi_w_major_event_dats_minus_loss_of_service_minutes': float,
        'saidi_w_major_event_days_minutes': float,
        'saidi_wo_major_event_days_minutes': float,
        'saifi_w_major_event_days_customers': float,
        'saifi_w_major_event_days_minus_loss_of_service_customers': float,
        'saifi_wo_major_event_days_customers': float,
        'sales_for_resale': float,
        'sales_for_resale_mwh': float,
        'sales_mwh': float,
        'sales_revenue': float,
        'sales_to_ultimate_consumers_mwh': float,
        'secondary_transportation_mode_code': pd.StringDtype(),
        'sector_id': pd.Int64Dtype(),
        'sector_name': pd.StringDtype(),
        'service_area': pd.StringDtype(),
        'service_type': pd.CategoricalDtype(categories=[
            "bundled", "energy", "delivery",
        ]),
        'short_form': pd.BooleanDtype(),
        'sold_to_utility_mwh': float,
        'solid_fuel_gasification': pd.BooleanDtype(),
        'data_source': pd.StringDtype(),
        'standard': pd.CategoricalDtype(categories=RELIABILITY_STANDARDS),
        'startup_source_code_1': pd.StringDtype(),
        'startup_source_code_2': pd.StringDtype(),
        'startup_source_code_3': pd.StringDtype(),
        'startup_source_code_4': pd.StringDtype(),
        'state': pd.StringDtype(),
        'state_id_fips': pd.StringDtype(),  # Must preserve leading zeroes
        'street_address': pd.StringDtype(),
        'stoker_tech': pd.BooleanDtype(),
        'storage_capacity_mw': float,
        'storage_customers': pd.Int64Dtype(),
        'subcritical_tech': pd.BooleanDtype(),
        'sulfur_content_pct': float,
        'summer_capacity_mw': float,
        'summer_capacity_estimate': pd.BooleanDtype(),
        # TODO: check if there is any data pre-2016
        'summer_estimated_capability_mw': float,
        'summer_peak_demand_mw': float,
        'supercritical_tech': pd.BooleanDtype(),
        'supplier_name': pd.StringDtype(),
        'switch_oil_gas': pd.BooleanDtype(),
        'syncronized_transmission_grid': pd.BooleanDtype(),
        # Added by AES for NM & DG tech table (might want to consider merging with another fuel label)
        'tech_class': pd.CategoricalDtype(categories=TECH_CLASSES),
        'technology_description': pd.StringDtype(),
        'time_cold_shutdown_full_load_code': pd.StringDtype(),
        'time_of_use_pricing_program': pd.BooleanDtype(),
        'time_responsive_programs': pd.BooleanDtype(),
        'time_responsiveness_customers': pd.Int64Dtype(),
        'timezone': pd.StringDtype(),
        'topping_bottoming_code': pd.StringDtype(),
        'total': float,
        'total_capacity_less_1_mw': float,
        'total_meters': pd.Int64Dtype(),
        'total_disposition_mwh': float,
        'total_energy_losses_mwh': float,
        'total_sources_mwh': float,
        'transmission': float,
        'transmission_activity': pd.BooleanDtype(),
        'transmission_by_other_losses_mwh': float,
        'transmission_distribution_owner_id': pd.Int64Dtype(),
        'transmission_distribution_owner_name': pd.StringDtype(),
        'transmission_distribution_owner_state': pd.StringDtype(),
        'turbines_inverters_hydrokinetics': float,
        'turbines_num': pd.Int64Dtype(),  # TODO: check if any turbines show up pre-2016
        'ultrasupercritical_tech': pd.BooleanDtype(),
        'unbundled_revenues': float,
        'unit_id_eia': pd.StringDtype(),
        'unit_id_pudl': pd.Int64Dtype(),
        'uprate_derate_completed_date': 'datetime64[ns]',
        'uprate_derate_during_year': pd.BooleanDtype(),
        'utility_id_eia': pd.Int64Dtype(),
        'utility_id_pudl': pd.Int64Dtype(),
        'utility_name_eia': pd.StringDtype(),
        'utility_owned_capacity_mw': float,  # Added by AES for NNM table
        'variable_peak_pricing_program': pd.BooleanDtype(),  # Added by AES for DP table
        'virtual_capacity_mw': float,  # Added by AES for NM table
        'virtual_customers': pd.Int64Dtype(),  # Added by AES for NM table
        'water_heater': pd.Int64Dtype(),  # Added by AES for DR table
        'water_source': pd.StringDtype(),
        'weighted_average_life_years': float,
        'wheeled_power_delivered_mwh': float,
        'wheeled_power_recieved_mwh': float,
        'wholesale_marketing_activity': pd.BooleanDtype(),
        'wholesale_power_purchases_mwh': float,
        'winter_capacity_mw': float,
        'winter_capacity_estimate': pd.BooleanDtype(),
        'winter_estimated_capability_mw': float,
        'winter_peak_demand_mw': float,
        # 'with_med': float,
        # 'with_med_minus_los': float,
        # 'without_med': float,
        'zip_code': pd.StringDtype(),
        'zip_code_4': pd.StringDtype()
    },
    'depreciation': {
        'utility_id_ferc1': pd.Int64Dtype(),
        'utility_id_pudl': pd.Int64Dtype(),
        'plant_id_pudl': pd.Int64Dtype(),
        # 'plant_name': pd.StringDtype(),
        'note': pd.StringDtype(),
        'report_year': int,
        'report_date': 'datetime64[ns]',
        'common': pd.BooleanDtype(),
        'plant_balance': float,
        'book_reserve': float,
        'unaccrued_balance': float,
        'reserve_pct': float,
        # 'survivor_curve_type': pd.StringDtype(),
        'service_life_avg': float,
        'net_salvage_pct': float,
        'net_salvage_rate_type_pct': pd.BooleanDtype(),
        'net_removal': float,
        'net_removal_pct': float,
        'remaining_life_avg': float,
        # 'retirement_date': 'datetime64[ns]',
        'depreciation_annual_epxns': float,
        'depreciation_annual_pct': float,
        'depreciation_annual_rate_type_pct': pd.BooleanDtype(),
        # 'data_source': pd.StringDtype(),
    }
}
