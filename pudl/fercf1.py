import numpy as np
import pandas as pd
import dbfread
import subprocess
import glob
import string
import re
import os.path

###########################################################################
# Variables and helper functions related to ingest & process of FERC Form 1
# data.
###########################################################################

# This is a list of all the years we have FERC Form 1 Data for:
f1_years = np.arange(1994,2016)

# directory beneath which the FERC Form 1 data lives...
f1_datadir = "data/ferc/form1"

# Pull in some metadata about the FERC Form 1 DB & its tables:
f1_db_notes    = pd.read_csv("{}/docs/f1_db_notes.csv".format(f1_datadir),header=0)
f1_fuel_notes  = pd.read_csv("{}/docs/f1_fuel_notes.csv".format(f1_datadir),header=0)
f1_steam_notes = pd.read_csv("{}/docs/f1_steam_notes.csv".format(f1_datadir),header=0)

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

def get_strings(filename, min=4):
    """Extract printable strings from a binary and return them as a generator.

    This is meant to emulate the Unix "strings" command, for the purposes of
    grabbing database table and column names from the F1_PUB.DBC file that is
    distributed with the FERC Form 1 data.
    """ #{{{
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

def f1_getTablesFields(year, min=4):
    """Extract the names of all the tables and fields from FERC Form 1 DB

    This function reads all the strings in the F1_PUB.DBC database file for the
    corresponding year, and picks out the ones that appear to be database table
    names, and their subsequent table field names, for use in re-naming the
    truncated columns extracted from the corresponding DBF files (which are
    limited to having only 10 characters in their names.) Strings must have at
    least min printable characters.
    """ #{{{

    # Find the right DBC file, based on the year we're looking at:
    filename = glob.glob('{}/{}/*/FORM1/working/F1_PUB.DBC'.format(f1_datadir,year))

    # Extract all the strings longer than "min" from the DBC file
    assert len(filename)==1
    dbc_strs = list(get_strings(filename[0], min=min))

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
    return(tf_dict)
#}}}

def f1_check_fieldnames(long_names, short_names):
    """Compares lists of long and short field names for consistency.

    DBF field names can only be 10 characters long. This function
    checks to see if the long names we've extracted from the DBC file
    are consistent with the short names from the DBF files by looking
    to see if the first nine characters of each are the same.
    """

    # Make sure we don't have this weird field...
    short_names = [ s for s in short_names if s.lower() != '_nullflags' ]
    # They better be the same length, or we have a mis-match.
    assert len(long_names) == len(short_names)
    return([ s.lower()[:9] for s in long_names  ] ==
           [ s.lower()[:9] for s in short_names ])

def f1_defgen(f1_dbf,year=2015):
    """A short hack to generate the code for defining SQLAlchemy table defs.
    """
    # Given a DBF file to convert:
    # We need to generate and assemble....
    #  - Name of the Table, e.g. f1_respondent_id
    #  - For each Column:
    #    - Field Name (e.g. respondent_id)
    #    - Field Data type (e.g. Varchar)
    #    - Field length  (e.g. 20)
    #    - Field decimal length (just in case)

    f1_tbls = f1_getTablesFields(year)

    f1_tablemap = {'F1_1':  'f1_respondent_id',
                   'F1_31': 'f1_fuel',
                   'F1_33': 'f1_gnrt_plant',
                   'F1_52': 'f1_plant_in_srvce',
                   'F1_53': 'f1_pumped_storage',
                   'F1_54': 'f1_purchased_pwr',
                   'F1_70': 'f1_work',
                   'F1_71': 'f1_xmssn_adds',
                   'F1_77': 'f1_sched_lit_tbl',
                   'F1_79': 'f1_col_lit_tbl',
                   'F1_84': 'f1_row_lit_tbl',
                   'F1_86': 'f1_hydro',
                   'F1_89': 'f1_steam',
                   'F1_398_ANCL_PS': 'f1_398_ancl_ps',
                   'F1_S0_FILING_LOG': 'f1_s0_filing_log'
                  }

    f1_typemap = {'B': 'XXX', # .DBT block number, binary string
                  'C': 'String',
                  'D': 'Date',
                  'N': 'Float',  # because it can be integer or float
                  'L': 'Boolean',
                  'M': 'XXX', # 10 digit .DBT block number, stored as a string...
                  '@': 'XXX', # Timestamp... Date = Julian Day, Time is in milliseconds?
                  'I': 'Integer',
                  '+': 'XXX', # Autoincrement (e.g. for IDs)
                  'F': 'Float',
                  'O': 'XXX', # Double, 8 bytes
                  'G': 'XXX', # OLE 10 digit/byte number of a .DBT block, stored as string
                  'T': 'DateTime', #DateTime, based on dbf2sqlite mapping
                  '0': 'XXX' # #Integer? based on dbf2sqlite mapping
                 }

    table_name = f1_tablemap[f1_dbf]
    dbf_file = glob.glob("{}/{}/*/FORM1/working/{}.DBF".format(f1_datadir,year,f1_dbf))
    dbf_fields = dbfread.DBF(dbf_file[0], load=True).fields
    
    print('Table(\'{}\', f1_meta,'.format(table_name))
    for (col_name,dbf_field) in zip(f1_tbls[table_name],dbf_fields[:-1]):
        len_str = ''
        key_str = ''
        if dbf_field.type == 'C':
            len_str = '({})'.format(dbf_field.length)
        if col_name == 'respondent_id':
            key_str = ', ForeignKey(\'f1_respondent_id.respondent_id\'), primary_key=True'
        if col_name == 'report_year':
            key_str = ', ForeignKey(\'f1_s0_filing_log.report_yr\'), primary_key=True'
        if col_name == 'report_prd':
            key_str = ', ForeignKey(\'f1_s0_filing_log.report_prd\'), primary_key=True'
        if col_name == 'spplmnt_num':
            key_str = ', primary_key=True'
        if col_name == 'row_number':
            key_str = ', primary_key=True'
        if col_name == 'sched_table_name':
            key_str = ', primary_key=True'
        if col_name == 'column_name':
            key_str = ', primary_key=True'
        if col_name == 'row_name':
            key_str = ', primary_key=True'

        print('        Column(\'{}\', {}{}{}),'.format(col_name,f1_typemap[dbf_field.type],len_str,key_str))
    print('    )') # end of the Table() definition

def f1_slurp():
    """A bespoke import of a subset of the FERC Form 1 database tables to Postgres.

    Programmatic creation of DB table structure requires using auxiliary DB
    migration tools beyond the scope of SQL Alchemy. For the time being, we are
    going to pull in just the initial tables we're interested in, and just for
    2015, and we're going to do it "by hand". {{{

    Initial FERC DBF files to be imported, and the corresponding tables:

        'F1_1':  'f1_respondent_id',
        'F1_31': 'f1_fuel',
        'F1_33': 'f1_gnrt_plant',
        'F1_52': 'f1_plant_in_srvce',
        'F1_53': 'f1_pumped_storage',
        'F1_54': 'f1_purchased_pwr',
        'F1_70': 'f1_work',
        'F1_71': 'f1_xmssn_adds',
        'F1_77': 'f1_sched_lit_tbl',
        'F1_79': 'f1_col_lit_tbl',
        'F1_84': 'f1_row_lit_tbl',
        'F1_86': 'f1_hydro',
        'F1_89': 'f1_steam',
        'F1_398_ANCL_PS': 'f1_398_ancl_ps',
        'F1_S0_FILING_LOG': 'f1_s0_filing_log',

    """
    from sqlalchemy import create_engine
    from sqlalchemy import Table, Column, Integer, String, Float, DateTime, Boolean, Date, MetaData, ForeignKey

    f1_engine = create_engine('postgresql://catalyst@localhost:5432/ferc_f1')

    f1_meta = MetaData()

    Table('f1_respondent_id', f1_meta, #{{{
        Column('respondent_id', Integer, primary_key=True),
        Column('respondent_name', String(70)),
        Column('respondent_alias', String(70)),
        Column('status', String(1)),
        Column('form_type', Integer),
        Column('status_date', Date),
        Column('sort_name', String(8)),
        Column('pswd_gen', String(15))
    ) #}}}

    Table('f1_s0_filing_log', f1_meta, #{{{
        Column('respondent_id', Integer, ForeignKey('f1_respondent_id.respondent_id'), primary_key=True),
        Column('report_yr', Integer),
        Column('report_prd', Integer),
        Column('filing_num', Integer),
        Column('poc_email', String(120)),
        Column('submitted', DateTime),
        Column('received', DateTime),
        Column('loaded', DateTime)
    ) #}}}

    Table('f1_sched_lit_tbl', f1_meta, #{{{
        Column('sched_table_name', String(18), primary_key=True),
        Column('report_year', Integer, ForeignKey('f1_s0_filing_log.report_yr'), primary_key=True),
        Column('sched_literal', String(70)),
        Column('sched_status', String(1)),
        Column('sched_chg_year', Integer),
        Column('turned_schedule', String(1)),
    ) #}}}

    Table('f1_col_lit_tbl', f1_meta, #{{{
        Column('sched_table_name', String(18), primary_key=True),
        Column('report_year', Integer, ForeignKey('f1_s0_filing_log.report_yr'), primary_key=True),
        Column('column_name', String(18), primary_key=True),
        Column('col_literal', String(70)),
        Column('col_status', String(1)),
        Column('col_chg_yr', Integer)
    ) #}}}

    Table('f1_row_lit_tbl', f1_meta, #{{{
        Column('sched_table_name', String(18), primary_key=True),
        Column('report_year', Integer, ForeignKey('f1_s0_filing_log.report_yr'), primary_key=True),
        Column('row_number', Integer, primary_key=True),
        Column('row_seq', Integer),
        Column('row_literal', String(70)),
        Column('row_status', String(1)),
        Column('row_chg_yr', Integer),
    ) #}}}

    Table('f1_plant_in_srvce', f1_meta, #{{{
        Column('respondent_id', Integer, ForeignKey('f1_respondent_id.respondent_id'), primary_key=True),
        Column('report_year', Integer, ForeignKey('f1_s0_filing_log.report_yr'), primary_key=True),
        Column('report_prd', Integer, ForeignKey('f1_s0_filing_log.report_prd'), primary_key=True),
        Column('spplmnt_num', Integer, primary_key=True),
        Column('row_number', Integer, primary_key=True),
        Column('row_seq', Integer),
        Column('row_prvlg', String(1)),
        Column('begin_yr_bal', Float),
        Column('addition', Float),
        Column('retirements', Float),
        Column('adjustments', Float),
        Column('transfers', Float),
        Column('yr_end_bal', Float),
        Column('begin_yr_bal_f', Integer),
        Column('addition_f', Integer),
        Column('retirements_f', Integer),
        Column('adjustments_f', Integer),
        Column('transfers_f', Integer),
        Column('yr_end_bal_f', Integer)
    ) #}}}

    Table('f1_purchased_pwr', f1_meta, #{{{
        Column('respondent_id', Integer, ForeignKey('f1_respondent_id.respondent_id'), primary_key=True),
        Column('report_year', Integer, ForeignKey('f1_s0_filing_log.report_yr'), primary_key=True),
        Column('report_prd', Integer, ForeignKey('f1_s0_filing_log.report_prd'), primary_key=True),
        Column('spplmnt_num', Integer, primary_key=True),
        Column('row_number', Integer, primary_key=True),
        Column('row_seq', Integer),
        Column('row_prvlg', String(1)),
        Column('athrty_co_name', String(38)),
        Column('sttstcl_clssfctn', String(2)),
        Column('rtsched_trffnbr', String(18)),
        Column('avgmth_bill_dmnd', String(18)),
        Column('avgmth_ncp_dmnd', String(18)),
        Column('avgmth_cp_dmnd', String(18)),
        Column('mwh_purchased', Float),
        Column('mwh_recv', Float),
        Column('mwh_delvd', Float),
        Column('dmnd_charges', Float),
        Column('erg_charges', Float),
        Column('othr_charges', Float),
        Column('settlement_tot', Float),
        Column('athrty_co_name_f', Integer),
        Column('sttstcl_clssfctn_f', Integer),
        Column('rtsched_trffnbr_f', Integer),
        Column('avgmth_bill_dmnd_f', Integer),
        Column('avgmth_ncp_dmnd_f', Integer),
        Column('avgmth_cp_dmnd_f', Integer),
        Column('mwh_purchased_f', Integer),
        Column('mwh_recv_f', Integer),
        Column('mwh_delvd_f', Integer),
        Column('dmnd_charges_f', Integer),
        Column('erg_charges_f', Integer),
        Column('othr_charges_f', Integer),
        Column('settlement_tot_f', Integer)
    ) #}}}

    Table('f1_xmssn_adds', f1_meta, #{{{
        Column('respondent_id', Integer, ForeignKey('f1_respondent_id.respondent_id'), primary_key=True),
        Column('report_year', Integer, ForeignKey('f1_s0_filing_log.report_yr'), primary_key=True),
        Column('report_prd', Integer, ForeignKey('f1_s0_filing_log.report_prd'), primary_key=True),
        Column('spplmnt_num', Integer, primary_key=True),
        Column('row_number', Integer, primary_key=True),
        Column('row_seq', Integer),
        Column('row_prvlg', String(1)),
        Column('designation_from', String(30)),
        Column('designation_to', String(30)),
        Column('line_length', Float),
        Column('structure_type', String(14)),
        Column('structure_miles', Float),
        Column('crct_present', Integer),
        Column('crct_ultimate', Float),
        Column('cndctr_size', String(10)),
        Column('cndctr_spec', String(7)),
        Column('cndctr_config', String(12)),
        Column('voltage', Float),
        Column('cost_land', Float),
        Column('cost_poles', Float),
        Column('cost_cndctr', Float),
        Column('asset_retire_cost', Float),
        Column('cost_total', Float),
        Column('designation_from_f', Integer),
        Column('designation_to_f', Integer),
        Column('line_length_f', Integer),
        Column('structure_type_f', Integer),
        Column('structure_miles_f', Integer),
        Column('crct_present_f', Integer),
        Column('crct_ultimate_f', Integer),
        Column('cndctr_size_f', Integer),
        Column('cndctr_spec_f', Integer),
        Column('cndctr_config_f', Integer),
        Column('voltage_f', Integer),
        Column('cost_land_f', Integer),
        Column('cost_poles_f', Integer),
        Column('cost_cndctr_f', Integer),
        Column('asset_retire_cost_f', Integer),
        Column('cost_total_f', Integer)
    ) #}}}

    Table('f1_fuel', f1_meta, #{{{
        Column('respondent_id', Integer, ForeignKey('f1_respondent_id.respondent_id'), primary_key=True),
        Column('report_year', Integer, ForeignKey('f1_s0_filing_log.report_yr'), primary_key=True),
        Column('report_prd', Integer, ForeignKey('f1_s0_filing_log.report_prd'), primary_key=True),
        Column('spplmnt_num', Integer, primary_key=True),
        Column('row_number', Integer, primary_key=True),
        Column('row_seq', Integer),
        Column('row_prvlg', String(1)),
        Column('plant_name', String(20)),
        Column('fuel', String(13)),
        Column('fuel_unit', String(13)),
        Column('fuel_quantity', Float),
        Column('fuel_avg_heat', Float),
        Column('fuel_cost_delvd', Float),
        Column('fuel_cost_burned', Float),
        Column('fuel_cost_btu', Float),
        Column('fuel_cost_kwh', Float),
        Column('fuel_generaton', Float),
        Column('fuel_f', Integer),
        Column('fuel_unit_f', Integer),
        Column('fuel_quantity_f', Integer),
        Column('fuel_avg_heat_f', Integer),
        Column('fuel_cost_delvd_f', Integer),
        Column('fuel_cost_burned_f', Integer),
        Column('fuel_cost_btu_f', Integer),
        Column('fuel_cost_kwh_f', Integer),
        Column('fuel_generaton_f', Integer)
    ) #}}}

    Table('f1_pumped_storage', f1_meta, #{{{
        Column('respondent_id', Integer, ForeignKey('f1_respondent_id.respondent_id'), primary_key=True),
        Column('report_year', Integer, ForeignKey('f1_s0_filing_log.report_yr'), primary_key=True),
        Column('report_prd', Integer, ForeignKey('f1_s0_filing_log.report_prd'), primary_key=True),
        Column('spplmnt_num', Integer, primary_key=True),
        Column('row_number', Integer, primary_key=True),
        Column('row_seq', Integer),
        Column('row_prvlg', String(1)),
        Column('project_no', Integer),
        Column('plant_name', String(20)),
        Column('plant_kind', String(20)),
        Column('yr_const', String(4)),
        Column('yr_installed', String(4)),
        Column('tot_capacity', Float),
        Column('peak_demand', Float),
        Column('plant_hours', Float),
        Column('plant_capability', Float),
        Column('avg_num_of_emp', Float),
        Column('net_generation', Float),
        Column('energy_used', Float),
        Column('net_load', Float),
        Column('cost_land', Float),
        Column('cost_structures', Float),
        Column('cost_facilties', Float),
        Column('cost_wheels', Float),
        Column('cost_electric', Float),
        Column('cost_misc_eqpmnt', Float),
        Column('cost_roads', Float),
        Column('asset_retire_cost', Float),
        Column('cost_of_plant', Float),
        Column('cost_per_kw', Float),
        Column('expns_operations', Float),
        Column('expns_water_pwr', Float),
        Column('expns_pump_strg', Float),
        Column('expns_electric', Float),
        Column('expns_misc_power', Float),
        Column('expns_rents', Float),
        Column('expns_engneering', Float),
        Column('expns_structures', Float),
        Column('expns_dams', Float),
        Column('expns_plant', Float),
        Column('expns_misc_plnt', Float),
        Column('expns_producton', Float),
        Column('pumping_expenses', Float),
        Column('tot_prdctn_exns', Float),
        Column('expns_kwh', Float),
        Column('project_no_f', Integer),
        Column('plant_name_f', Integer),
        Column('plant_kind_f', Integer),
        Column('yr_const_f', Integer),
        Column('yr_installed_f', Integer),
        Column('tot_capacity_f', Integer),
        Column('peak_demand_f', Integer),
        Column('plant_hours_f', Integer),
        Column('plant_capability_f', Integer),
        Column('avg_num_of_emp_f', Integer),
        Column('net_generation_f', Integer),
        Column('energy_used_f', Integer),
        Column('net_load_f', Integer),
        Column('cost_land_f', Integer),
        Column('cost_structures_f', Integer),
        Column('cost_facilties_f', Integer),
        Column('cost_wheels_f', Integer),
        Column('cost_electric_f', Integer),
        Column('cost_misc_eqpmnt_f', Integer),
        Column('cost_roads_f', Integer),
        Column('asset_retire_cost_f', Integer),
        Column('cost_of_plant_f', Integer),
        Column('cost_per_kw_f', Integer),
        Column('expns_operations_f', Integer),
        Column('expns_water_pwr_f', Integer),
        Column('expns_pump_strg_f', Integer),
        Column('expns_electric_f', Integer),
        Column('expns_misc_power_f', Integer),
        Column('expns_rents_f', Integer),
        Column('expns_engneering_f', Integer),
        Column('expns_structures_f', Integer),
        Column('expns_dams_f', Integer),
        Column('expns_plant_f', Integer),
        Column('expns_misc_plnt_f', Integer),
        Column('expns_producton_f', Integer),
        Column('pumping_expenses_f', Integer),
        Column('tot_prdctn_exns_f', Integer),
        Column('expns_kwh_f', Integer)
    ) #}}}

    Table('f1_work', f1_meta, #{{{
        Column('respondent_id', Integer, ForeignKey('f1_respondent_id.respondent_id'), primary_key=True),
        Column('report_year', Integer, ForeignKey('f1_s0_filing_log.report_yr'), primary_key=True),
        Column('report_prd', Integer, ForeignKey('f1_s0_filing_log.report_prd'), primary_key=True),
        Column('spplmnt_num', Integer, primary_key=True),
        Column('row_number', Integer, primary_key=True),
        Column('row_seq', Integer),
        Column('row_prvlg', String(1)),
        Column('description', String(91)),
        Column('work_in_progress', Float),
        Column('description_f', Integer),
        Column('work_in_progress_f', Integer)
    ) #}}}

    Table('f1_hydro', f1_meta, #{{{
        Column('respondent_id', Integer, ForeignKey('f1_respondent_id.respondent_id'), primary_key=True),
        Column('report_year', Integer, ForeignKey('f1_s0_filing_log.report_yr'), primary_key=True),
        Column('report_prd', Integer, ForeignKey('f1_s0_filing_log.report_prd'), primary_key=True),
        Column('spplmnt_num', Integer, primary_key=True),
        Column('row_number', Integer, primary_key=True),
        Column('row_seq', Integer),
        Column('row_prvlg', String(1)),
        Column('project_no', Integer),
        Column('plant_name', String(20)),
        Column('plant_kind', String(20)),
        Column('plant_const', String(20)),
        Column('yr_const', String(4)),
        Column('yr_installed', String(4)),
        Column('tot_capacity', Float),
        Column('peak_demand', Float),
        Column('plant_hours', Float),
        Column('favorable_cond', Float),
        Column('adverse_cond', Float),
        Column('avg_num_of_emp', Float),
        Column('net_generation', Float),
        Column('cost_of_land', Float),
        Column('cost_structure', Float),
        Column('cost_facilities', Float),
        Column('cost_equipment', Float),
        Column('cost_roads', Float),
        Column('cost_plant_total', Float),
        Column('cost_per_kw', Float),
        Column('expns_operations', Float),
        Column('expns_water_pwr', Float),
        Column('expns_hydraulic', Float),
        Column('expns_electric', Float),
        Column('expns_generation', Float),
        Column('expns_rents', Float),
        Column('expns_engnr', Float),
        Column('expns_structures', Float),
        Column('expns_dams', Float),
        Column('expns_plant', Float),
        Column('expns_misc_plant', Float),
        Column('expns_total', Float),
        Column('expns_kwh', Float),
        Column('project_no_f', Integer),
        Column('plant_name_f', Integer),
        Column('plant_kind_f', Integer),
        Column('plant_const_f', Integer),
        Column('yr_const_f', Integer),
        Column('yr_installed_f', Integer),
        Column('tot_capacity_f', Integer),
        Column('peak_demand_f', Integer),
        Column('plant_hours_f', Integer),
        Column('favorable_cond_f', Integer),
        Column('adverse_cond_f', Integer),
        Column('avg_num_of_emp_f', Integer),
        Column('net_generation_f', Integer),
        Column('cost_of_land_f', Integer),
        Column('cost_structure_f', Integer),
        Column('cost_facilities_f', Integer),
        Column('cost_equipment_f', Integer),
        Column('cost_roads_f', Integer),
        Column('cost_plant_total_f', Integer),
        Column('cost_per_kw_f', Integer),
        Column('expns_operations_f', Integer),
        Column('expns_water_pwr_f', Integer),
        Column('expns_hydraulic_f', Integer),
        Column('expns_electric_f', Integer),
        Column('expns_generation_f', Integer),
        Column('expns_rents_f', Integer),
        Column('expns_engnr_f', Integer),
        Column('expns_structures_f', Integer),
        Column('expns_dams_f', Integer),
        Column('expns_plant_f', Integer),
        Column('expns_misc_plant_f', Integer),
        Column('expns_total_f', Integer),
        Column('expns_kwh_f', Integer),
        Column('asset_retire_cost', Float),
        Column('asset_retire_cost_f', Integer)
    ) #}}}

    Table('f1_gnrt_plant', f1_meta, #{{{
        Column('respondent_id', Integer, ForeignKey('f1_respondent_id.respondent_id'), primary_key=True),
        Column('report_year', Integer, ForeignKey('f1_s0_filing_log.report_yr'), primary_key=True),
        Column('report_prd', Integer, ForeignKey('f1_s0_filing_log.report_prd'), primary_key=True),
        Column('spplmnt_num', Integer, primary_key=True),
        Column('row_number', Integer, primary_key=True),
        Column('row_seq', Integer),
        Column('row_prvlg', String(1)),
        Column('plant_name', String(48)),
        Column('yr_constructed', String(4)),
        Column('capacity_rating', Float),
        Column('net_demand', Float),
        Column('net_generation', Float),
        Column('plant_cost', Float),
        Column('plant_cost_mw', Float),
        Column('operation', Float),
        Column('expns_fuel', Float),
        Column('expns_maint', Float),
        Column('kind_of_fuel', String(20)),
        Column('fuel_cost', Float),
        Column('plant_name_f', Integer),
        Column('yr_constructed_f', Integer),
        Column('capacity_rating_f', Integer),
        Column('net_demand_f', Integer),
        Column('net_generation_f', Integer),
        Column('plant_cost_f', Integer),
        Column('plant_cost_mw_f', Integer),
        Column('operation_f', Integer),
        Column('expns_fuel_f', Integer),
        Column('expns_maint_f', Integer),
        Column('kind_of_fuel_f', Integer),
        Column('fuel_cost_f', Integer)
    ) #}}}

    Table('f1_398_ancl_ps', f1_meta, #{{{
        Column('respondent_id', Integer, ForeignKey('f1_respondent_id.respondent_id'), primary_key=True),
        Column('report_year', Integer, ForeignKey('f1_s0_filing_log.report_yr'), primary_key=True),
        Column('report_prd', Integer, ForeignKey('f1_s0_filing_log.report_prd'), primary_key=True),
        Column('spplmnt_num', Integer, primary_key=True),
        Column('row_number', Integer, primary_key=True),
        Column('row_seq', Integer),
        Column('purch_num_units', Float),
        Column('purch_unit', String(10)),
        Column('purch_dollars', Float),
        Column('sold_num_units', Float),
        Column('sold_unit', String(10)),
        Column('sold_dollars', Float),
        Column('row_prvlg', String(1)),
        Column('purch_num_units_f', Integer),
        Column('purch_unit_f', Integer),
        Column('purch_dollars_f', Integer),
        Column('sold_num_units_f', Integer),
        Column('sold_unit_f', Integer)
    ) #}}}

    Table('f1_steam', f1_meta, #{{{
        Column('respondent_id', Integer, ForeignKey('f1_respondent_id.respondent_id'), primary_key=True),
        Column('report_year', Integer, ForeignKey('f1_s0_filing_log.report_yr'), primary_key=True),
        Column('report_prd', Integer, ForeignKey('f1_s0_filing_log.report_prd'), primary_key=True),
        Column('spplmnt_num', Integer, primary_key=True),
        Column('row_number', Integer, primary_key=True),
        Column('row_seq', Integer),
        Column('row_prvlg', String(1)),
        Column('plant_name', String(20)),
        Column('plant_kind', String(20)),
        Column('type_const', String(20)),
        Column('yr_const', String(4)),
        Column('yr_installed', String(4)),
        Column('tot_capacity', Float),
        Column('peak_demand', Float),
        Column('plant_hours', Float),
        Column('plnt_capability', Float),
        Column('when_not_limited', Float),
        Column('when_limited', Float),
        Column('avg_num_of_emp', Float),
        Column('net_generation', Float),
        Column('cost_land', Float),
        Column('cost_structure', Float),
        Column('cost_equipment', Float),
        Column('cost_of_plant_to', Float),
        Column('cost_per_kw', Float),
        Column('expns_operations', Float),
        Column('expns_fuel', Float),
        Column('expns_coolants', Float),
        Column('expns_steam', Float),
        Column('expns_steam_othr', Float),
        Column('expns_transfer', Float),
        Column('expns_electric', Float),
        Column('expns_misc_power', Float),
        Column('expns_rents', Float),
        Column('expns_allowances', Float),
        Column('expns_engnr', Float),
        Column('expns_structures', Float),
        Column('expns_boiler', Float),
        Column('expns_plants', Float),
        Column('expns_misc_steam', Float),
        Column('tot_prdctn_expns', Float),
        Column('expns_kwh', Float),
        Column('plant_name_f', Integer),
        Column('plant_kind_f', Integer),
        Column('type_const_f', Integer),
        Column('yr_const_f', Integer),
        Column('yr_installed_f', Integer),
        Column('tot_capacity_f', Integer),
        Column('peak_demand_f', Integer),
        Column('plant_hours_f', Integer),
        Column('plnt_capability_f', Integer),
        Column('when_not_limited_f', Integer),
        Column('when_limited_f', Integer),
        Column('avg_num_of_emp_f', Integer),
        Column('net_generation_f', Integer),
        Column('cost_land_f', Integer),
        Column('cost_structure_f', Integer),
        Column('cost_equipment_f', Integer),
        Column('cost_of_plant_to_f', Integer),
        Column('cost_per_kw_f', Integer),
        Column('expns_operations_f', Integer),
        Column('expns_fuel_f', Integer),
        Column('expns_coolants_f', Integer),
        Column('expns_steam_f', Integer),
        Column('expns_steam_othr_f', Integer),
        Column('expns_transfer_f', Integer),
        Column('expns_electric_f', Integer),
        Column('expns_misc_power_f', Integer),
        Column('expns_rents_f', Integer),
        Column('expns_allowances_f', Integer),
        Column('expns_engnr_f', Integer),
        Column('expns_structures_f', Integer),
        Column('expns_boiler_f', Integer),
        Column('expns_plants_f', Integer),
        Column('expns_misc_steam_f', Integer),
        Column('tot_prdctn_expns_f', Integer),
        Column('expns_kwh_f', Integer),
        Column('asset_retire_cost', Float),
        Column('asset_retire_cost_f', Integer)
    ) #}}}

    # Make the Tables!
    f1_meta.create_all(f1_engine)
    #}}}

def f1_dbf2sql(dbf_tbl,yr,f1_db):
    """Imports a subset of the FERC Form 1 database tables into Postgres.

    This function uses the dbfread module to pull tables from the FERC
    Form 1 database into a Postgres database with the same basic
    structure and data types.

    """
    # Use the dbfread module to access a given FERC Form 1 database table,
    # and create a corresponding table in postgres.

    # Mapping of DBF filenames to corresponding logical tables.  We need to
    # preserve the table names because they are referenced inside some of the
    # tables, e.g. in f1_row_lit_tbl
    f1_tablemap = { #{{{
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
        'F1_77': 'f1_sched_lit_tbl',
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
    } #}}}

    # Make sure we got a valid DBF table...
    assert dbf_tbl in f1_tablemap.keys()
    # Construct the path to the DBF field:
    dbf_file = '{}/{}/*/FORM1/working/{}.DBF'.format(f1_datadir,yr,dbf_tbl)
    assert os.path.isfile(dbf_file)

    # name of the postgres table to create:
    pg_tbl_name = f1_tablemap[dbf_tbl]

    f1_table = dbfread.DBF(dbf_file, load=True)
    f1_tbl_name

    # Iterate over the list of DBF fields to generate an SQLAlchemy table
    # creation statement...

    # - Read the description of the fields.
    #   - name
    #   - type
    #   - length
    #   - decimal_count
    # - Determine the name for the Postgres table based on the information in
    #   f1_tablemap, 
    # - Based on the name of the table we're creating, get the list of table
    #   fields we expect to create from f1_getTablesFields
    # - Check to make sure that the names of the fields we're creating is
    #   at least consistent with the names we read from the DBF file. This is
    #   an ill specified mapping b/c it depends on the ordering of the fields
    #   in the DB, but that could be okay. We at least need to check for self
    #   consitency.
    # - 

#    dbf_file = dbf_path.split('/')[-1]
#    assert dbf_file in f1_tablemap.keys()

#    subprocess.run("pgdbf {path}".format(path=dbf_path))

    # grab the list of long field names from F1_PUB.DBF 

    # replace all of the short column names w/ the long names
#    for col in pg_table.columns:

def utilname2fercid(search_str, years=f1_years):
    """Takes a search string, which should be contained within a single utility
    name in the FERC Form 1 list of respondents, and returns a tuple containing
    the unique name and respondent ID that matched.  Allows multiple names to
    match, so long as there's only one responded ID mapped to all of them, to
    account for irregularities in reporting within the free form text field.
    e.g. with search_str="PacifiCo" the return value should be:
    ("PacifiCorp",134)

    If the string does not result in a single unique ID, consistent across all
    the years of data that we've got, then we need to throw an error.
    
    """ #{{{
    df = pd.DataFrame()

    for yr in years:
        f1_respondent_id_filename = glob.glob("{}/{}/*/FORM1/working/F1_1.DBF".format(f1_datadir,yr))
        numfiles = len(f1_respondent_id_filename)
        if numfiles!=1:
            print("ERROR: non-unique utility ID file for year {}".format(yr))
        assert(len(f1_respondent_id_filename)==1)
        f1_respondent_id_dbf = dbfread.DBF(f1_respondent_id_filename[0],load=True)
        new_df = pd.DataFrame(f1_respondent_id_dbf.records)
        new_df["YEAR"]=yr
        df = pd.concat((df,new_df))
    dfmatch = df[df.RESPONDEN2.str.contains(search_str)]
    util_names = dfmatch.RESPONDEN2.unique()
    util_ids   = dfmatch.RESPONDENT.unique()
    if(len(util_names) > 1):
        print("CAUTION: non-unique utility names found:")
        print(dfmatch[["YEAR","RESPONDENT","RESPONDEN2"]])
    if(len(util_names) == 0):
        print("ERROR: no matching utility name found")
    assert(len(util_ids)==1)
    return((util_names[0],util_ids[0]))
#}}} end utilname2fercid

def f1_table2df(dbf_file, util_ids=None, years=f1_years):
    """Take the name of a DBF file from the FERC Form 1 database, and pull all
    years worth of data for that table into a single pandas dataframe and
    return it for longitudinal analysis.

    dbf_file: Filename FERC Form 1 DBF file containing the data of interest.
    
    util_ids: a list of numbers corresponding to the FERC RESPONDENT field.  If
              no list of IDs is given, data for all utilities is returned.

    years: a list of years for which to pull the data.

    example: f1_table2df("F1_33",util_ids=(133,145),years=np.arange(2000,2016)
    """ #{{{

    df = pd.DataFrame()

    for yr in years:
        f1_file = glob.glob("{}/{}/*/FORM1/working/{}.DBF".format(f1_datadir,yr,dbf_file))
        numfiles = len(f1_file)
        if numfiles!=1:
            print("ERROR: non-unique utility ID file for year {}".format(yr))
        assert(len(f1_file)==1)
        f1_dbf = dbfread.DBF(f1_file[0],load=True)
        new_df = pd.DataFrame(f1_dbf.records)
        df = pd.concat((df,new_df))

    if util_ids is not None:
        df = df[df.RESPONDENT.isin(util_ids)]

    return(df)
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

def get_f1_fuel(years=f1_years, util_ids=None):
    """Pull FERC plant level fuel consumption data for a given set of utilities
    & years. Do some cleanup on the data, specific to the fuel data table.
    
    FERC Form 1 page 402, lines 36-44
    FERC DB file: F1_31.DBF
    """ #{{{

    f1_fuel = f1_table2df("F1_31", years=years, util_ids=util_ids)

    # Condense strings used to describe fuels and fuel units into a few canonical
    # values. May want to go to np.nan for unmapped string here eventually... but
    # need to figure out how to filter a DF for rows that don't have NaN in that
    # field first.
    f1_fuel['FUEL'] = f1_cleanstrings(f1_fuel['FUEL'],f1_fuel_strings, unmapped="")
    f1_fuel['FUEL_UNIT'] = f1_cleanstrings(f1_fuel['FUEL_UNIT'],f1_fuel_unit_strings)

    # Get rid of rows with no plant data in them:
    f1_fuel = f1_fuel[f1_fuel.PLANT_NAME!=""]

    # Get rid of rows with no fuel type listed:
    f1_fuel = f1_fuel[f1_fuel.FUEL!=""]

    return(f1_fuel)
#}}}

def get_f1_steam(years=f1_years, util_ids=None):
    """Pull generation data for a given set of utilities & years. Perform some
    data cleanup specific to this data table.
    
    FERC Form 1 page 402, lines 1-35
    FERC DB File: F1_89.DBF
    """ #{{{

    f1_steam = f1_table2df("F1_89",years=years, util_ids=util_ids)
    f1_steam = f1_steam[f1_steam.PLANT_NAME!=""]

    return(f1_steam)
#}}}
