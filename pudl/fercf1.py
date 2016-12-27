import numpy as np
import pandas as pd
import dbfread
import glob
import os.path

###########################################################################
# Variables and helper functions related to ingest & process of FERC Form 1
# data.
###########################################################################

# directory beneath which the FERC Form 1 data lives...
f1_datadir = '{}/data/ferc/form1'.format(os.path.dirname(os.path.dirname(__file__)))

# Dictionary for cleaning up fuel strings {{{
# Construct a dictionary mapping a canonical fuel name to a list of strings
# which are used to represent that fuel in the FERC Form 1 Reporting. Case is
# ignored, as all fuel strings can be converted to a lower case in the data
# set.
f1_coal_strings = ["coal","coal-subbit","lignite","coal(sb)","coal (sb)",\
                   "coal-lignite","coke","coa","lignite/coal",\
                   "coal - subbit","coal-subb","coal-sub","coal-lig",\
                   "coal-sub bit","coals","ciak","petcoke"]

f1_oil_strings  = ["oil","#6 oil","#2 oil","fuel oil","jet","no. 2 oil",\
                   "no.2 oil","no.6& used","used oil","oil-2","oil (#2)",\
                   "diesel oil","residual oil","# 2 oil","resid. oil",\
                   "tall oil","oil/gas","no.6 oil","oil-fuel","oil-diesel",\
                   "oil / gas","oil bbls","oil bls","no. 6 oil",\
                   "#1 kerosene","diesel","no. 2 oils","blend oil",\
                   "#2oil diesel","#2 oil-diesel","# 2  oil","light oil",\
                   "heavy oil","gas.oil","#2","2","6","bbl","no 2 oil",\
                   "no 6 oil","#1 oil","#6","oil-kero","oil bbl",\
                   "biofuel","no 2","kero","#1 fuel oil","no. 2  oil",\
                   "blended oil","no 2. oil","# 6 oil","nno. 2 oil",\
                   "#2 fuel","oill","oils","gas/oil","no.2 oil gas",\
                   "#2 fuel oil","oli","oil (#6)"]

f1_gas_strings  = ["gas","methane","natural gas","blast gas","gas mcf",\
                   "propane","prop","natural  gas","nat.gas","nat gas",\
                   "nat. gas","natl gas","ga","gas`","syngas","ng","mcf",\
                   "blast gaa","nat  gas","gac","syngass","prop."]

f1_nuke_strings = ["nuclear","grams of uran","grams of","grams of  ura",\
                   "grams","nucleur","nulear","nucl","nucleart"]

f1_biomass_strings = ["switchgrass","wood waste","woodchips","biomass",\
                      "wood","wood chips"]

f1_waste_strings = ["tires","tire","refuse"]

f1_steam_strings = ["steam","purch steam","purch. steam"]

# There are also a bunch of other weird and hard to categorize strings
# that I don't know what to do with... hopefully they constitute only a
# small fraction of the overall generation.

f1_fuel_strings = { 'coal'    : f1_coal_strings,
                    'gas'     : f1_gas_strings,
                    'oil'     : f1_oil_strings,
                    'nuke'    : f1_nuke_strings,
                    'biomass' : f1_biomass_strings,
                    'waste'   : f1_waste_strings,
                    'steam'   : f1_steam_strings
                  }
#}}}

# Dictionary for cleaning up fuel unit strings {{{
f1_ton_strings = ['toms','taons','tones','col-tons','toncoaleq','coal',\
                  'tons coal eq','coal-tons','ton','tons','tons coal',\
                  'coal-ton','tires-tons']

f1_mcf_strings = ['mcf',"mcf's",'mcfs','mcf.','gas mcf','"gas" mcf','gas-mcf',\
                  'mfc','mct',' mcf','msfs','mlf','mscf','mci','mcl','mcg',\
                  'm.cu.ft.']

f1_bbl_strings = ['barrel','bbls','bbl','barrels','bbrl','bbl.','bbls.',\
                  'oil 42 gal','oil-barrels','barrrels','bbl-42 gal',\
                  'oil-barrel','bb.','barrells','bar','bbld','oil- barrel',\
                  'barrels    .','bbl .','barels','barrell','berrels','bb',\
                  'bbl.s','oil-bbl','bls','bbl:','barrles','blb','propane-bbl']

f1_gal_strings = ['gallons','gal.','gals','gals.','gallon','gal']

f1_1kgal_strings = ['oil(1000 gal)','oil(1000)','oil (1000)','oil(1000']

f1_gramsU_strings = ['gram','grams','gm u','grams u235','grams u-235',\
                     'grams of uran','grams: u-235','grams:u-235',\
                     'grams:u235','grams u308','grams: u235','grams of']

f1_kgU_strings = ['kg of uranium','kg uranium','kilg. u-235','kg u-235',\
                  'kilograms-u23','kg','kilograms u-2','kilograms','kg of']

f1_mmbtu_strings = ['mmbtu','mmbtus',"mmbtu's",'nuclear-mmbtu','nuclear-mmbt']

f1_mwdth_strings = ['mwd therman','mw days-therm','mwd thrml','mwd thermal',\
                    'mwd/mtu','mw days','mwdth','mwd','mw day']

f1_mwhth_strings = ['mwh them','mwh threm','nwh therm','mwhth','mwh therm','mwh']

f1_fuel_unit_strings = { 'ton'   : f1_ton_strings,
                         'mcf'   : f1_mcf_strings,
                         'bbl'   : f1_bbl_strings,
                         'gal'   : f1_gal_strings,
                         '1kgal' : f1_1kgal_strings,
                         'gramsU': f1_gramsU_strings,
                         'kgU'   : f1_kgU_strings,
                         'mmbtu' : f1_mmbtu_strings,
                         'mwdth' : f1_mwdth_strings,
                         'mwhth' : f1_mwhth_strings
                       }
#}}}

# Dictionary mapping DBF files (w/o .DBF file extension) to DB table names
f1_dbf2tbl = { #{{{
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
f1_tbl2dbf = { v: k for k, v in f1_dbf2tbl.items() }
#}}}

# The set of FERC Form 1 tables that have the same composite primary keys: [
# respondent_id, report_year, report_prd, row_number, spplmnt_num ].
# TODO: THIS ONLY PERTAINS TO 2015 AND NEEDS TO BE ADJUSTED BY YEAR... {{{
f1_data_tables = [ 'f1_acb_epda', 'f1_accumdepr_prvsn', 'f1_accumdfrrdtaxcr',
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
                   'f1_income_stmnt', 'f1_miscgen_expnelc', 'f1_misc_dfrrd_dr',
                   'f1_mthly_peak_otpt', 'f1_mtrl_spply', 'f1_nbr_elc_deptemp',
                   'f1_nonutility_prop', 'f1_note_fin_stmnt',
                   'f1_nuclear_fuel', 'f1_officers_co', 'f1_othr_dfrrd_cr',
                   'f1_othr_pd_in_cptl', 'f1_othr_reg_assets',
                   'f1_othr_reg_liab', 'f1_overhead', 'f1_pccidica',
                   'f1_plant_in_srvce', 'f1_pumped_storage',
                   'f1_purchased_pwr', 'f1_reconrpt_netinc',
                   'f1_reg_comm_expn', 'f1_respdnt_control',
                   'f1_retained_erng', 'f1_r_d_demo_actvty',
                   'f1_sales_by_sched', 'f1_sale_for_resale',
                   'f1_sbsdry_totals', 'f1_schedules_list',
                   'f1_security_holder', 'f1_slry_wg_dstrbtn',
                   'f1_substations', 'f1_taxacc_ppchrgyr', 'f1_unrcvrd_cost',
                   'f1_utltyplnt_smmry', 'f1_work', 'f1_xmssn_adds',
                   'f1_xmssn_elc_bothr', 'f1_xmssn_elc_fothr', 'f1_xmssn_line',
                   'f1_xtraordnry_loss', 'f1_audit_log', 'f1_privilege',
                   'f1_hydro', 'f1_footnote_tbl', 'f1_steam', 'f1_leased',
                   'f1_sbsdry_detail', 'f1_plant', 'f1_long_term_debt',
                   'f1_106_2009', 'f1_106a_2009', 'f1_106b_2009',
                   'f1_208_elc_dep', 'f1_231_trn_stdycst', 'f1_324_elc_expns',
                   'f1_325_elc_cust', 'f1_331_transiso', 'f1_338_dep_depl',
                   'f1_397_isorto_stl', 'f1_398_ancl_ps', 'f1_399_mth_peak',
                   'f1_400_sys_peak', 'f1_400a_iso_peak', 'f1_429_trans_aff',
                   'f1_allowances_nox', 'f1_cmpinc_hedge_a', 'f1_cmpinc_hedge',
                   'f1_freeze', 'f1_rg_trn_srv_rev'
                 ] #}}}

def get_strings(filename, min=4):
    """Extract printable strings from a binary and return them as a generator.

    This is meant to emulate the Unix "strings" command, for the purposes of
    grabbing database table and column names from the F1_PUB.DBC file that is
    distributed with the FERC Form 1 data.
    """ #{{{
    import string
    with open(filename, errors="ignore") as f:
        result = ""
        for c in f.read():
            if c in string.printable:
                result += c
                continue
            if len(result) >= min:
                yield result
            result = ""
        if len(result) >= min:  # catch result at EOF
            yield result
#}}}

def f1_getTablesFields(dbc_filename, min=4):
    """Extract the names of all the tables and fields from FERC Form 1 DB

    This function reads all the strings in the given DBC database file for the
    and picks out the ones that appear to be database table names, and their
    subsequent table field names, for use in re-naming the truncated columns
    extracted from the corresponding DBF files (which are limited to having
    only 10 characters in their names.) Strings must have at least min
    printable characters.

    Returns a dictionary whose keys are the long table names extracted from
    the DBC file, and whose values are lists of pairs of values, the first
    of which is the full name of each field in the table with the same name
    as the key, and the second of which is the truncated (<=10 character)
    long name of that field as found in the DBF file.

    TODO: THIS SHOULD NOT REFER TO ANY PARTICULAR YEAR OF DATA
    """ #{{{
    import os.path
    import re

    # Extract all the strings longer than "min" from the DBC file
    dbc_strs = list(get_strings(dbc_filename, min=min))

    # Get rid of leading & trailing whitespace in the strings:
    dbc_strs = [ s.strip() for s in dbc_strs ]

    # Get rid of all the empty strings:
    dbc_strs = [ s for s in dbc_strs if s is not '' ]

    # Collapse all whitespace to a single space:
    dbc_strs = [ re.sub('\s+',' ',s) for s in dbc_strs ]

    # Pull out only strings that begin with Table or Field
    dbc_strs = [ s for s in dbc_strs if re.match('(^Table|^Field)',s) ]

    # Split each string by whitespace, and retain only the first two elements.
    # This eliminates some weird dangling junk characters
    dbc_strs = [ ' '.join(s.split()[:2]) for s in dbc_strs ]

    # Remove all of the leading Field keywords
    dbc_strs = [ re.sub('Field ','',s) for s in dbc_strs ]

    # Join all the strings together (separated by spaces) and then split the
    # big string on Table, so each string is now a table name followed by the
    # associated field names, separated by spaces
    dbc_list = ' '.join(dbc_strs).split('Table ')

    # strip leading & trailing whitespace from the lists, and get rid of empty
    # strings:
    dbc_list = [ s.strip() for s in dbc_list if s is not '' ]

    # Create a dictionary using the first element of these strings (the table
    # name) as the key, and the list of field names as the values, and return
    # it:
    tf_dict = {}
    for tbl in dbc_list:
        x = tbl.split()
        tf_dict[x[0]]=x[1:]

    tf_doubledict = {}
    for dbf in f1_dbf2tbl.keys():
        filename = '{}/2015/UPLOADERS/FORM1/working/{}.DBF'.format(f1_datadir,dbf)
        if os.path.isfile(filename):
            dbf_fields = dbfread.DBF(filename).field_names
            dbf_fields = [ f for f in dbf_fields if f != '_NullFlags' ]
            tf_doubledict[f1_dbf2tbl[dbf]]={ k:v for k,v in zip(dbf_fields,tf_dict[f1_dbf2tbl[dbf]]) }
            assert(len(tf_dict[f1_dbf2tbl[dbf]])==len(dbf_fields))

    # Insofar as we are able, make sure that the fields match each other
    for k in tf_doubledict.keys():
        for sn,ln in zip(tf_doubledict[k].keys(),tf_doubledict[k].values()):
            assert(ln[:8]==sn.lower()[:8])

    return(tf_doubledict)
#}}}

def f1_slurp():
    """Assuming an empty FERC Form 1 DB, create tables and insert data.

    This function uses dbfread and SQLAlchemy to migrate a set of FERC Form 1
    database tables from the provided DBF format into a postgres database.
    """ #{{{
    from sqlalchemy import create_engine, MetaData
    import datetime

    f1_engine = create_engine('postgresql://catalyst@localhost:5432/ferc_f1')
    f1_meta = MetaData()
    dbc_fn = '{}/2015/UPLOADERS/FORM1/working/F1_PUB.DBC'.format(f1_datadir)

    # These tables all have an unknown respondent_id = 454. For the moment, we
    # are working around this by inserting a dummy record for that utility...
    dbfs_454 = ['F1_31','F1_52','F1_54','F1_70','F1_71','F1_89','F1_398_ANCL_PS']

    # We still don't understand the primary keys for these tables, and so they
    # can't be inserted yet...
    dbfs_bad_pk = ['F1_84','F1_S0_FILING_LOG']

    # These are the DBF files that we're interested in and can insert now,
    # given the dummy Utility 454 entry
    dbfs = ['F1_1','F1_31','F1_33','F1_52','F1_53','F1_54','F1_70','F1_71',
            'F1_77','F1_79','F1_86','F1_89','F1_398_ANCL_PS']

    # This function (see below) uses metadata from the DBF files to define a
    # postgres database structure suitable for accepting the FERC Form 1 data
    f1_define_db(dbc_fn, dbfs, f1_meta, f1_engine)

    # Wipe the DB and start over... just to be sure we aren't munging stuff
    f1_meta.drop_all(f1_engine)

    # Create a new database, as defined in the f1_meta MetaData object:
    f1_meta.create_all(f1_engine)

    # Create a DB connection to use for the record insertions below:
    conn=f1_engine.connect()

    # This awkward dictionary of dictionaries lets us map from a DBF file
    # to a couple of lists -- one of the short field names from the DBF file,
    # and the other the full names that we want to have the SQL database...
    f1_tblmap = f1_getTablesFields(dbc_fn)

    for dbf in dbfs:
        dbf_filename = '{}/2015/UPLOADERS/FORM1/working/{}.DBF'.format(f1_datadir,dbf)
        dbf_table = dbfread.DBF(dbf_filename, load=True)

        # f1_dbf2tbl is a dictionary mapping DBF file names to SQL table names
        sql_table_name = f1_dbf2tbl[dbf]
        sql_table = f1_meta.tables[sql_table_name]

        # Build up a list of dictionaries to INSERT into the postgres database.
        # Each dictionary is one record. Within each dictionary the keys are
        # the field names, and the values are the values for that field.
        sql_records = []
        for dbf_rec in dbf_table.records:
            sql_rec = {}
            for dbf_field_name, sql_field_name in f1_tblmap[sql_table_name].items():
                sql_rec[sql_field_name] = dbf_rec[dbf_field_name]
            sql_records.append(sql_rec)

        # For some reason this respondent_id was missing from the master
        # table... but showing up in the data tables. Go figure
        if (sql_table_name == 'f1_respondent_id'):
            sql_records.append({
                'respondent_id' : 454,
                'respondent_name' : 'Entergy UNKNOWN SUBSIDIARY',
                'respondent_alias' : '',
                'status' : 'A',
                'form_type' : 0,
                'status_date' : datetime.date(1990,1,1),
                'sort_name' : '',
                'pswd_gen' : ''
            })

        # insert the new records!
        conn.execute(sql_table.insert(), sql_records)
    conn.close()
#}}}

def f1_define_db(dbc_fn, dbfs, f1_meta, db_engine):
    """Based on DBF files, create postgres tables to accept FERC Form 1 data.
    """ #{{{
    from sqlalchemy import create_engine
    from sqlalchemy import Table, Column, Integer, String, Float, DateTime
    from sqlalchemy import Boolean, Date, MetaData, Text, ForeignKeyConstraint
    from sqlalchemy import PrimaryKeyConstraint

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

    f1_tblmap = f1_getTablesFields(dbc_fn)

    for dbf in dbfs:
        # Create the DBF table object. XXX SHOULD NOT REFER TO 2015
        f1_dbf = dbfread.DBF('{}/2015/UPLOADERS/FORM1/working/{}.DBF'.format(f1_datadir,dbf))

        # And the corresponding SQLAlchemy Table object:
        table_name = f1_dbf2tbl[dbf]
        f1_sql = Table(table_name, f1_meta)

        # _NullFlags isn't a "real" data field... remove it.
        fields = [ f for f in f1_dbf.fields if f.name != '_NullFlags' ]

        for field in fields:
            col_name = f1_tblmap[f1_dbf2tbl[dbf]][field.name]
            col_type = dbf_typemap[field.type]

            # String/VarChar is the only type that really NEEDS a length
            if(col_type == String):
                col_type = col_type(length=field.length)

            f1_sql.append_column(Column(col_name, col_type))

        # Append primary key constraints to the table:

        if (table_name in f1_data_tables):
            # All the "real" data tables use the same 5 fields as a composite
            # primary key: [ respondent_id, report_year, report_prd,
            # row_number, spplmnt_num ]
            f1_sql.append_constraint(PrimaryKeyConstraint(
                'respondent_id',
                'report_year',
                'report_prd',
                'row_number',
                'spplmnt_num')
            )

            # They also all have respondent_id as their foreign key:
            f1_sql.append_constraint(ForeignKeyConstraint(
                columns=['respondent_id',],
                refcolumns=['f1_respondent_id.respondent_id'])
            )

        if (table_name == 'f1_respondent_id'):
            f1_sql.append_constraint(PrimaryKeyConstraint('respondent_id'))

        # Sadly the primary key definitions here don't seem to be right...
        if (table_name == 'f1_s0_filing_log'):
            f1_sql.append_constraint(PrimaryKeyConstraint(
                'respondent_id',
                'report_yr',
                'report_prd',
                'filing_num')
            )
            f1_sql.append_constraint(ForeignKeyConstraint(
                columns=['respondent_id',],
                refcolumns=['f1_respondent_id.respondent_id'])
            )

        # Sadly the primary key definitions here don't seem to be right...
        if (table_name == 'f1_row_lit_tbl'):
            f1_sql.append_constraint(PrimaryKeyConstraint(
                'sched_table_name',
                'report_year',
                'row_number')
            )

        # Other tables we have not yet attempted to deal with...

        #'f1_email'
        #  primary_key = respondent_id
        #  foreign_key = f1_respondent_id.respondent_id

        #'f1_ident_attsttn',
        #  primary_key = respondent_id
        #  primary_key = report_year
        #  primary_key = report_period
        #  foreign_key = f1_responded_id.respondent_id

        #'f1_footnote_data', #NOT USING NOW/NOT COMPLETE
        #  primary_key = fn_id
        #  primary_key = respondent_id
        #  foreign_key = f1_respondent_id.respondent_id
        #  foreign_key = f1_s0_filing_log.report_prd

        #'f1_pins',
        #  primary_key = f1_respondent_id.respondent_id
        #  foreign_key = f1_respondent_id.respondent_id

        #'f1_freeze',
        #'f1_security'
        #'f1_load_file_names'
        #'f1_unique_num_val',
        #'f1_sched_lit_tbl',
        #'f1_sys_error_log',
        #'f1_col_lit_tbl',    # GET THIS ONE
        #'f1_codes_val',
        #'f1_s0_checks',
#}}}

def f1_cleanstrings(field, stringmap, unmapped=None):
    """Clean up a field of string data in one of the Form 1 data frames.

    This function maps many different strings meant to represent the same value
    or category to a single value. In addition, white space is stripped and
    values are translated to lower case.  Optionally replace all unmapped
    values in the original field with a value (like NaN) to indicate data which
    is uncategorized or confusing.

    field is a pandas dataframe column (e.g. f1_fuel["FUEL"]

    stringmap is a dictionary whose keys are the strings we're mapping to, and
    whose values are the strings that get mapped.

    unmapped is the value which strings not found in the stringmap dictionary
    should be replaced by.

    The function returns a new pandas series/column that can be used to set the
    values of the original data.
    """ #{{{

    # Simplify the strings we're working with, to reduce the number of strings
    # we need to enumerate in the maps

    # Transform the strings to lower case
    field = field.apply(lambda x: x.lower())
    # remove leading & trailing whitespace
    field = field.apply(lambda x: x.strip())
    # remove duplicate internal whitespace
    field = field.replace('[\s+]', ' ', regex=True)

    for k in stringmap.keys():
        field = field.replace(stringmap[k],k)

    if unmapped is not None:
        badstrings = np.setdiff1d(field.unique(),list(stringmap.keys()))
        field = field.replace(badstrings,unmapped)

    return field
#}}} end f1_cleanstrings
