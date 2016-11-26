import numpy as np
import pandas as pd
import dbfread
import dataset
import glob

###########################################################################
# Variables and helper functions related to ingest & process of FERC Form 1
# data.
###########################################################################

# This is a list of all the years we have FERC Form 1 Data for:
f1_years = np.arange(1994,2016)

# directory beneath which the FERC Form 1 data lives...
f1_dirname = "data/ferc/form1"

# Pull in some metadata about the FERC Form 1 DB & its tables:
f1_db_notes    = pd.read_csv("{}/docs/f1_db_notes.csv".format(f1_dirname),header=0)
f1_fuel_notes  = pd.read_csv("{}/docs/f1_fuel_notes.csv".format(f1_dirname),header=0)
f1_steam_notes = pd.read_csv("{}/docs/f1_steam_notes.csv".format(f1_dirname),header=0)

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

def f1_dbf2sql(dbf_files):
    """Converts FERC Form 1 data from DBF to SQL format.
    """
    # Mapping of DBF field types (characters) to SQL types.
    dbf2sql_typemap = {
        'F': 'FLOAT',
        'L': 'BOOLEAN',
        'I': 'INTEGER',
        'C': 'TEXT',
        'N': 'REAL',  # because it can be integer or float
        'M': 'TEXT',
        'D': 'DATE',
        'T': 'DATETIME',
        '0': 'INTEGER',
    }

    # Mapping of DBF filenames to corresponding logical tables.  We need to
    # preserve the table names becaue they are referenced inside some of the
    # tables, e.g. in f1_row_lit_tbl
    f1_tablemap = { #{{{
        'F1_1.DBF': 'f1_respondent_id',
        'F1_10.DBF': 'f1_allowances',
        'F1_106_2009.DBF': 'f1_106_2009',
        'F1_106A_2009.DBF': 'f1_106a_2009',
        'F1_106B_2009.DBF': 'f1_106b_2009',
        'F1_11.DBF': 'f1_bal_sheet_cr',
        'F1_12.DBF': 'f1_capital_stock',
        'F1_13.DBF': 'f1_cash_flow',
        'F1_14.DBF': 'f1_cmmn_utlty_p_e',
        'F1_15.DBF': 'f1_comp_balance_db',
        'F1_16.DBF': 'f1_construction',
        'F1_17.DBF': 'f1_control_respdnt',
        'F1_18.DBF': 'f1_co_directors',
        'F1_19.DBF': 'f1_cptl_stk_expns',
        'F1_2.DBF': 'f1_acb_epda',
        'F1_20.DBF': 'f1_csscslc_pcsircs',
        'F1_208_ELC_DEP.DBF': 'f1_208_elc_dep',
        'F1_21.DBF': 'f1_dacs_epda',
        'F1_22.DBF': 'f1_dscnt_cptl_stk',
        'F1_23.DBF': 'f1_edcfu_epda',
        'F1_231_TRN_STDYCST.DBF': 'f1_231_trn_stdycst',
        'F1_24.DBF': 'f1_elctrc_erg_acct',
        'F1_25.DBF': 'f1_elctrc_oper_rev',
        'F1_26.DBF': 'f1_elc_oper_rev_nb',
        'F1_27.DBF': 'f1_elc_op_mnt_expn',
        'F1_28.DBF': 'f1_electric',
        'F1_29.DBF': 'f1_envrnmntl_expns',
        'F1_3.DBF': 'f1_accumdepr_prvsn',
        'F1_30.DBF': 'f1_envrnmntl_fclty',
        'F1_31.DBF': 'f1_fuel',
        'F1_32.DBF': 'f1_general_info',
        'F1_324_ELC_EXPNS.DBF': 'f1_324_elc_expns',
        'F1_325_ELC_CUST.DBF': 'f1_325_elc_cust',
        'F1_33.DBF': 'f1_gnrt_plant',
        'F1_331_TRANSISO.DBF': 'f1_331_transiso',
        'F1_338_DEP_DEPL.DBF': 'f1_338_dep_depl',
        'F1_34.DBF': 'f1_important_chg',
        'F1_35.DBF': 'f1_incm_stmnt_2',
        'F1_36.DBF': 'f1_income_stmnt',
        'F1_37.DBF': 'f1_miscgen_expnelc',
        'F1_38.DBF': 'f1_misc_dfrrd_dr',
        'F1_39.DBF': 'f1_mthly_peak_otpt',
        'F1_397_ISORTO_STL.DBF': 'f1_397_isorto_stl',
        'F1_398_ANCL_PS.DBF': 'f1_398_ancl_ps',
        'F1_399_MTH_PEAK.DBF': 'f1_399_mth_peak',
        'F1_4.DBF': 'f1_accumdfrrdtaxcr',
        'F1_40.DBF': 'f1_mtrl_spply',
        'F1_400_SYS_PEAK.DBF': 'f1_400_sys_peak',
        'F1_400A_ISO_PEAK.DBF': 'f1_400a_iso_peak',
        'F1_41.DBF': 'f1_nbr_elc_deptemp',
        'F1_42.DBF': 'f1_nonutility_prop',
        'F1_429_TRANS_AFF.DBF': 'f1_429_trans_aff',
        'F1_43.DBF': 'f1_note_fin_stmnt',
        'F1_44.DBF': 'f1_nuclear_fuel',
        'F1_45.DBF': 'f1_officers_co',
        'F1_46.DBF': 'f1_othr_dfrrd_cr',
        'F1_47.DBF': 'f1_othr_pd_in_cptl',
        'F1_48.DBF': 'f1_othr_reg_assets',
        'F1_49.DBF': 'f1_othr_reg_liab',
        'F1_5.DBF': 'f1_adit_190_detail',
        'F1_50.DBF': 'f1_overhead',
        'F1_51.DBF': 'f1_pccidica',
        'F1_52.DBF': 'f1_plant_in_srvce',
        'F1_53.DBF': 'f1_pumped_storage',
        'F1_54.DBF': 'f1_purchased_pwr',
        'F1_55.DBF': 'f1_reconrpt_netinc',
        'F1_56.DBF': 'f1_reg_comm_expn',
        'F1_57.DBF': 'f1_respdnt_control',
        'F1_58.DBF': 'f1_retained_erng',
        'F1_59.DBF': 'f1_r_d_demo_actvty',
        'F1_6.DBF': 'f1_adit_190_notes',
        'F1_60.DBF': 'f1_sales_by_sched',
        'F1_61.DBF': 'f1_sale_for_resale',
        'F1_62.DBF': 'f1_sbsdry_totals',
        'F1_63.DBF': 'f1_schedules_list',
        'F1_64.DBF': 'f1_security_holder',
        'F1_65.DBF': 'f1_slry_wg_dstrbtn',
        'F1_66.DBF': 'f1_substations',
        'F1_67.DBF': 'f1_taxacc_ppchrgyr',
        'F1_68.DBF': 'f1_unrcvrd_cost',
        'F1_69.DBF': 'f1_utltyplnt_smmry',
        'F1_7.DBF': 'f1_adit_amrt_prop',
        'F1_70.DBF': 'f1_work',
        'F1_71.DBF': 'f1_xmssn_adds',
        'F1_72.DBF': 'f1_xmssn_elc_bothr',
        'F1_73.DBF': 'f1_xmssn_elc_fothr',
        'F1_74.DBF': 'f1_xmssn_line',
        'F1_75.DBF': 'f1_xtraordnry_loss',
        'F1_76.DBF': 'f1_codes_val',
        'F1_77.DBF': 'f1_sched_lit_tbl',
        'F1_78.DBF': 'f1_audit_log',
        'F1_79.DBF': 'f1_col_lit_tbl',
        'F1_8.DBF': 'f1_adit_other',
        'F1_80.DBF': 'f1_load_file_names',
        'F1_81.DBF': 'f1_privilege',
        'F1_82.DBF': 'f1_sys_error_log',
        'F1_83.DBF': 'f1_unique_num_val',
        'F1_84.DBF': 'f1_row_lit_tbl',
        'F1_85.DBF': 'f1_footnote_data',
        'F1_86.DBF': 'f1_hydro',
        'F1_87.DBF': 'f1_footnote_tbl',
        'F1_88.DBF': 'f1_ident_attsttn',
        'F1_89.DBF': 'f1_steam',
        'F1_9.DBF': 'f1_adit_other_prop',
        'F1_90.DBF': 'f1_leased',
        'F1_91.DBF': 'f1_sbsdry_detail',
        'F1_92.DBF': 'f1_plant',
        'F1_93.DBF': 'f1_long_term_debt',
        'F1_ALLOWANCES_NOX.DBF': 'f1_allowances_nox',
        'F1_CMPINC_HEDGE_A.DBF': 'f1_cmpinc_hedge_a',
        'F1_CMPINC_HEDGE.DBF': 'f1_cmpinc_hedge',
        'F1_EMAIL.DBF': 'f1_email',
        'F1_FREEZE.DBF': 'f1_freeze',
        'F1_PINS.DBF': 'f1_pins',
        'F1_RG_TRN_SRV_REV.DBF': 'f1_rg_trn_srv_rev',
        'F1_S0_CHECKS.DBF': 'f1_s0_checks',
        'F1_S0_FILING_LOG.DBF': 'f1_s0_filing_log',
        'F1_SECURITY.DBF': 'f1_security'
    } #}}}

    for tf in dbf_files:
        print_table(DBF(tf, lowernames=True))

def print_table(table):
    field_types = {}
    for f in table.fields:
        field_types[f.name] = dbf2sql_typemap.get(f.type, 'TEXT')


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
        f1_respondent_id_filename = glob.glob("{}/{}/*/FORM1/working/F1_1.DBF".format(f1_dirname,yr))
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
        f1_file = glob.glob("{}/{}/*/FORM1/working/{}.DBF".format(f1_dirname,yr,dbf_file))
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

def get_f1_fuel(years=f1_years, util_ids=None): #{{{
    """Pull FERC plant level fuel consumption data for a given set of utilities
    & years. Do some cleanup on the data, specific to the fuel data table.
    
    FERC Form 1 page 402, lines 36-44
    FERC DB file: F1_31.DBF
    """

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

def get_f1_steam(years=f1_years, util_ids=None): #{{{
    """Pull generation data for a given set of utilities & years. Perform some
    data cleanup specific to this data table.
    
    FERC Form 1 page 402, lines 1-35
    FERC DB File: F1_89.DBF
    """

    f1_steam = f1_table2df("F1_89",years=years, util_ids=util_ids)
    f1_steam = f1_steam[f1_steam.PLANT_NAME!=""]

    return(f1_steam)
#}}}

def get_f1_plant_in_svc(years=f1_years, util_ids=None): #{{{
    """Pull electric plant in service data for a given set of utilties and
    years, and do some basic cleanup on the data.
    
    FERC Form 1 Pages: 202-207
    FERC DB File: F1_52.DBF

    Plant in service includes the beginning & end of year balances for each of
    nearly 100 FERC accounts, as well as the annual additions, retirements,
    transfers and adjustments applied to each of those accounts. The data is
    aggregated across the entire utility that is reporting, and not broken out
    by individual plants.

    There are several lines (identified by row_number) within the reporting
    that are totals of other groups of lines, organized by type of assets.

    ROW_NUMBER : Category
    5  : "Intangible Plant (sum of rows 2, 3, & 4)"
    16 : "Total Steam Production Plant (sum of rows 8-15)"
    25 : "Total Nuclear Production Plant (sum of rows 18-24)"
    35 : "Total Hydraulic Production Plant (sum of rows 27-34)"
    45 : "Total Other Production Plant (sum of rows 37-44)"
    46 : "Total Production Plant (sum of rows 16, 25, 35, 45)"
    58 : "Total Transmission Plant (sum of rows 48-57)"
    75 : "Total Distribution Plant (sum of rows 60-74)"
    84 : "Total Transmission and Market Operation Plant (sum of rows 77-83)"
    96 : "Subtotal (sum of rows 86-95)"
    99 : "Total General Plant (sum of rows 96, 97, & 98)"
    100: "Total Plant"
    104: "Total Electric Plant in Service (sum of rows 100-103)"
    """

    f1_plant_in_svc = f1_table2df("F1_52", years=years, util_ids=util_ids)

    # Key is row number, value is a list: [row_literal,
    pis_desc =

    # Categorize the different rows in the sheet
    intangible_plant_rows = np.arange(2,5)
    intangible_plant_tot = 5

    steam_production_plant_rows = np.arange(8,16)
    steam_production_plant_tot  = 16

    nuclear_production_plant_rows = np.arange(18,25)
    nuclear_production_plant_tot  = 25

    hydraulic_production_plant_rows = np.arange(27,35)
    other_production_plant_rows = np.arange(37,45)

    # Array of all the row numbers that are primary production plant data...
    production_plant_rows = np.concatenate([steam_production_plant_rows,
                                            nuclear_production_plant_rows,
                                            hydraulic_production_plant_rows,
                                            other_production_plant_rows])

    return(f1_plant_in_svc)
#}}}

def get_f1_cwip(years=f1_years, util_ids=None): #{{{
    """Retrieve construction work in progress data from FERC Form 1 database.

    FERC Form 1 Page: 216
    FERC DB File: F1_70.DBF
    """
    f1_cwip = f1_table2df("F1_70", years=years, util_ids=util_ids)
    return(f1_cwip)
    #}}}

def get_f1_purch_pwr(years=f1_years, util_ids=None): #{{{
    """Retrieve purchased power data from FERC Form 1 database.

    FERC Form 1 Pages: 326-327
    FERC DB File: F1_54.DBF
    """
    f1_purch_pwr = f1_table2df("F1_54", years=years, util_ids=util_ids)
    return(f1_purch_pwr)
    #}}}

def get_f1_small_plant(years=f1_years, util_ids=None): #{{{
    """Retrieve small generating plant data from the FERC Form 1 database.

    FERC Form 1 Pages: 410-411
    FERC DB File: F1_33.DBF
    """
    f1_small_plant = f1_table2df("F1_33", years=years, util_ids=util_ids)
    return(f1_small_plant)
    #}}}

def get_f1_ancil_svcs(years=f1_years, util_ids=None): #{{{
    """Retrieve ancillary service purchase data from FERC Form 1 database.
    
    FERC Form 1 Pages: 398
    FERC DB File: F1_398_ANCL_PS.DBF
    """
    f1_ancil_svcs = f1_table2df("F1_398_ANCL_PS", years=years, util_ids=util_ids)
    return(f1_ancil_svcs)
    #}}}

def get_f1_trans_add(years=f1_years, util_ids=None): #{{{
    """Retrieve transmission line addition data from the FERC Form 1 database.

    FERC Form 1 Pages: 424-425
    FERC DB File: F1_71.DBF
    """
    f1_trans_add = f1_table2df("F1_71", years=years, util_ids=util_ids)
    return(f1_trans_add)
    #}}}

def get_f1_pump_stor(years=f1_years, util_ids=None): #{{{
    """ Retrieve pumped storage data from the FERC Form 1 database.

    FERC Form 1 Pages: 408-409
    FERC DB File: F1_53.DBF
    """
    f1_pump_stor = f1_table2df("F1_53", years=years, util_ids=util_ids)
    return(f1_pump_stor)
    #}}}

def get_f1_hydro(years=f1_years, util_ids=None): #{{{
    """Retrieve hydroelectric facility data from the FERC Form 1 database.

    FERC Form 1 Pages: 406-407
    FERC DB File: F1_86
    """
    f1_hydro = f1_table2df("F1_86", years=years, util_ids=util_ids)
    return(f1_hydro)
    #}}}

# Dummy get f1_table function....
#def get_f1_(years=f1_years, util_ids=None): #{{{
#    """Retrieve XXX data from the FERC Form 1 database.
#
#    FERC Form 1 Pages:
#    FERC DB File:
#    """
#    f1_ = f1_table2df("", years=years, util_ids=util_ids)
#    #}}}
