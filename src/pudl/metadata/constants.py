"""Metadata and operational constants."""
import datetime
from typing import Callable, Dict, List, Type

import pandas as pd
import pyarrow as pa
import sqlalchemy as sa

FIELD_DTYPES_PANDAS: Dict[str, str] = {
    "string": "string",
    "number": "float64",
    "integer": "Int64",
    "boolean": "boolean",
    "date": "datetime64[ns]",
    "datetime": "datetime64[ns]",
    "year": "datetime64[ns]",
}
"""
Pandas data type by PUDL field type (Data Package `field.type`).
"""

FIELD_DTYPES_PYARROW: Dict[str, pa.lib.DataType] = {
    "boolean": pa.bool_(),
    "date": pa.date32(),
    # We'll probably need to make the TZ dynamic rather than hard coded...
    "datetime": pa.timestamp("s", tz="UTC"),
    "integer": pa.int32(),
    "number": pa.float32(),
    "string": pa.string(),
    "year": pa.int32(),
}

FIELD_DTYPES_SQL: Dict[str, sa.sql.visitors.VisitableType] = {
    "boolean": sa.Boolean,
    "date": sa.Date,
    "datetime": sa.DateTime,
    "integer": sa.Integer,
    "number": sa.Float,
    "string": sa.Text,
    "year": sa.Integer,
}
"""
SQLAlchemy column types by PUDL field type (Data Package `field.type`).
"""

CONSTRAINT_DTYPES: Dict[str, Type] = {
    'string': str,
    'integer': int,
    'year': int,
    'number': float,
    'boolean': bool,
    'date': datetime.date,
    'datetime': datetime.datetime
}
"""
Python types for field constraints by PUDL field type (Data Package `field.type`).
"""

LICENSES: Dict[str, Dict[str, str]] = {
    "cc-by-4.0": {
        "name": "CC-BY-4.0",
        "title": "Creative Commons Attribution 4.0",
        "path": "https://creativecommons.org/licenses/by/4.0",
    },
    "us-govt": {
        "name": "other-pd",
        "title": "U.S. Government Works",
        "path": "https://www.usa.gov/government-works",
    },
}
"""
License attributes.
"""

PERIODS: Dict[str, Callable[[pd.Series], pd.Series]] = {
    "year": lambda x: x.astype("datetime64[Y]"),
    "quarter": lambda x: x.apply(
        pd.tseries.offsets.QuarterBegin(startingMonth=1).rollback
    ),
    "month": lambda x: x.astype("datetime64[M]"),
    "date": lambda x: x.astype("datetime64[D]"),
}
"""
Functions converting datetimes to period start times, by time period.
"""

CONTRIBUTORS: Dict[str, Dict[str, str]] = {
    "catalyst-cooperative": {
        "title": "Catalyst Cooperative",
        "email": "pudl@catalyst.coop",
        "path": "https://catalyst.coop",
        "role": "publisher",
        "organization": "Catalyst Cooperative",
    },
    "zane-selvans": {
        "title": "Zane Selvans",
        "email": "zane.selvans@catalyst.coop",
        "path": "https://amateurearthling.org",
        "role": "wrangler",
        "organization": "Catalyst Cooperative",
        "orcid": "0000-0002-9961-7208"
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
        "path": "https://karldw.org",
        "role": "contributor",
        "organization": "UC Berkeley",
    },
    "greg-schivley": {
        "title": "Greg Schivley",
        "path": "https://gschivley.github.io",
        "role": "contributor",
        "organization": "Carbon Impact Consulting",
    },
    "austen-sharpe": {
        "title": "Austen Sharpe",
        "email": "austen.sharpe@catalyst.coop",
        "role": "contributor",
        "organization": "Catalyst Cooperative",
    },
    "katherine-lamb": {
        "title": "Katherine Lamb",
        "email": "katherine.lamb@catalyst.coop",
        "role": "contributor",
        "organization": "Catalyst Cooperative",
    },
    "bennett-norman": {
        "title": "Bennett Norman",
        "email": "bennett.norman@catalyst.coop",
        "role": "contributor",
        "organization": "Catalyst Cooperative",
    },
    "trenton-bush": {
        "title": "Trenton Bush",
        "email": "trenton.bush@catalyst.coop",
        "role": "contributor",
        "organization": "Catalyst Cooperative",
    },
    "ethan-welty": {
        "title": "Ethan Welty",
        "email": "ethan.welty@gmail.com",
        "role": "contributor",
        "organization": "Catalyst Cooperative",
    },
}
"""
PUDL Contributors for attribution.
"""

KEYWORDS: Dict[str, List[str]] = {
    "electricity": [
        "electricity",
        "electric",
        "generation",
        "energy",
        "utility",
        "transmission",
        "distribution",
        "kWh",
        "MWh",
        "kW",
        "MW",
        "kilowatt hours",
        "kilowatts",
        "megawatts",
        "megawatt hours",
        "power",
    ],
    "fuels": [
        "fuel",
        "coal",
        "bituminous",
        "lignite",
        "nagural gas",
        "solar",
        "wind",
        "hydro",
        "nuclear"
        "subbituminous",
        "heat content",
        "mmbtu",
        "fuel cost",
        "fuel price",
    ],
    "plants": [
        "plant",
        "boilers",
        "generators",
        "steam",
        "turbine",
        "combined_cycle",
        "retirement",
        "planned",
        "proposed",
        "combustion",
        "prime mover",
        "capacity",
        "heat rate",
    ],
    "finance": [
        "finance",
        "debt",
        "accounting",
        "capital",
        "cost",
        "contract",
        "price",
        "receipts",
        "ownership",
        "depreciation",
        "plant in service",
        "capex",
        "opex",
        "operating expenses",
        "capital expenses",
    ],
    "environment": [
        "emissions",
        "pollution",
        "ash",
        "sulfur",
        "mercury",
        "chlorine",
        "sox",
        "so2",
        "nox",
        "ghg",
        "co2",
        "carbon dioxide",
        "particulate",
        "pm2.5",
    ],
    "eia": [
        "eia",
        "energy information administration",
    ],
    "ferc": [
        "ferc",
        "federal energy regulatory commission",
    ],
    "epa": [
        "epa",
        "environmental protection agency",
    ],
    "us_govt": [
        "united states",
        "us",
        "usa",
        "government",
        "federal",
    ]
}

DBF_TABLES_FILENAMES = {
    'f1_respondent_id': 'F1_1.DBF',
    'f1_acb_epda': 'F1_2.DBF',
    'f1_accumdepr_prvsn': 'F1_3.DBF',
    'f1_accumdfrrdtaxcr': 'F1_4.DBF',
    'f1_adit_190_detail': 'F1_5.DBF',
    'f1_adit_190_notes': 'F1_6.DBF',
    'f1_adit_amrt_prop': 'F1_7.DBF',
    'f1_adit_other': 'F1_8.DBF',
    'f1_adit_other_prop': 'F1_9.DBF',
    'f1_allowances': 'F1_10.DBF',
    'f1_bal_sheet_cr': 'F1_11.DBF',
    'f1_capital_stock': 'F1_12.DBF',
    'f1_cash_flow': 'F1_13.DBF',
    'f1_cmmn_utlty_p_e': 'F1_14.DBF',
    'f1_comp_balance_db': 'F1_15.DBF',
    'f1_construction': 'F1_16.DBF',
    'f1_control_respdnt': 'F1_17.DBF',
    'f1_co_directors': 'F1_18.DBF',
    'f1_cptl_stk_expns': 'F1_19.DBF',
    'f1_csscslc_pcsircs': 'F1_20.DBF',
    'f1_dacs_epda': 'F1_21.DBF',
    'f1_dscnt_cptl_stk': 'F1_22.DBF',
    'f1_edcfu_epda': 'F1_23.DBF',
    'f1_elctrc_erg_acct': 'F1_24.DBF',
    'f1_elctrc_oper_rev': 'F1_25.DBF',
    'f1_elc_oper_rev_nb': 'F1_26.DBF',
    'f1_elc_op_mnt_expn': 'F1_27.DBF',
    'f1_electric': 'F1_28.DBF',
    'f1_envrnmntl_expns': 'F1_29.DBF',
    'f1_envrnmntl_fclty': 'F1_30.DBF',
    'f1_fuel': 'F1_31.DBF',
    'f1_general_info': 'F1_32.DBF',
    'f1_gnrt_plant': 'F1_33.DBF',
    'f1_important_chg': 'F1_34.DBF',
    'f1_incm_stmnt_2': 'F1_35.DBF',
    'f1_income_stmnt': 'F1_36.DBF',
    'f1_miscgen_expnelc': 'F1_37.DBF',
    'f1_misc_dfrrd_dr': 'F1_38.DBF',
    'f1_mthly_peak_otpt': 'F1_39.DBF',
    'f1_mtrl_spply': 'F1_40.DBF',
    'f1_nbr_elc_deptemp': 'F1_41.DBF',
    'f1_nonutility_prop': 'F1_42.DBF',
    'f1_note_fin_stmnt': 'F1_43.DBF',
    'f1_nuclear_fuel': 'F1_44.DBF',
    'f1_officers_co': 'F1_45.DBF',
    'f1_othr_dfrrd_cr': 'F1_46.DBF',
    'f1_othr_pd_in_cptl': 'F1_47.DBF',
    'f1_othr_reg_assets': 'F1_48.DBF',
    'f1_othr_reg_liab': 'F1_49.DBF',
    'f1_overhead': 'F1_50.DBF',
    'f1_pccidica': 'F1_51.DBF',
    'f1_plant_in_srvce': 'F1_52.DBF',
    'f1_pumped_storage': 'F1_53.DBF',
    'f1_purchased_pwr': 'F1_54.DBF',
    'f1_reconrpt_netinc': 'F1_55.DBF',
    'f1_reg_comm_expn': 'F1_56.DBF',
    'f1_respdnt_control': 'F1_57.DBF',
    'f1_retained_erng': 'F1_58.DBF',
    'f1_r_d_demo_actvty': 'F1_59.DBF',
    'f1_sales_by_sched': 'F1_60.DBF',
    'f1_sale_for_resale': 'F1_61.DBF',
    'f1_sbsdry_totals': 'F1_62.DBF',
    'f1_schedules_list': 'F1_63.DBF',
    'f1_security_holder': 'F1_64.DBF',
    'f1_slry_wg_dstrbtn': 'F1_65.DBF',
    'f1_substations': 'F1_66.DBF',
    'f1_taxacc_ppchrgyr': 'F1_67.DBF',
    'f1_unrcvrd_cost': 'F1_68.DBF',
    'f1_utltyplnt_smmry': 'F1_69.DBF',
    'f1_work': 'F1_70.DBF',
    'f1_xmssn_adds': 'F1_71.DBF',
    'f1_xmssn_elc_bothr': 'F1_72.DBF',
    'f1_xmssn_elc_fothr': 'F1_73.DBF',
    'f1_xmssn_line': 'F1_74.DBF',
    'f1_xtraordnry_loss': 'F1_75.DBF',
    'f1_codes_val': 'F1_76.DBF',
    'f1_sched_lit_tbl': 'F1_77.DBF',
    'f1_audit_log': 'F1_78.DBF',
    'f1_col_lit_tbl': 'F1_79.DBF',
    'f1_load_file_names': 'F1_80.DBF',
    'f1_privilege': 'F1_81.DBF',
    'f1_sys_error_log': 'F1_82.DBF',
    'f1_unique_num_val': 'F1_83.DBF',
    'f1_row_lit_tbl': 'F1_84.DBF',
    'f1_footnote_data': 'F1_85.DBF',
    'f1_hydro': 'F1_86.DBF',
    'f1_footnote_tbl': 'F1_87.DBF',
    'f1_ident_attsttn': 'F1_88.DBF',
    'f1_steam': 'F1_89.DBF',
    'f1_leased': 'F1_90.DBF',
    'f1_sbsdry_detail': 'F1_91.DBF',
    'f1_plant': 'F1_92.DBF',
    'f1_long_term_debt': 'F1_93.DBF',
    'f1_106_2009': 'F1_106_2009.DBF',
    'f1_106a_2009': 'F1_106A_2009.DBF',
    'f1_106b_2009': 'F1_106B_2009.DBF',
    'f1_208_elc_dep': 'F1_208_ELC_DEP.DBF',
    'f1_231_trn_stdycst': 'F1_231_TRN_STDYCST.DBF',
    'f1_324_elc_expns': 'F1_324_ELC_EXPNS.DBF',
    'f1_325_elc_cust': 'F1_325_ELC_CUST.DBF',
    'f1_331_transiso': 'F1_331_TRANSISO.DBF',
    'f1_338_dep_depl': 'F1_338_DEP_DEPL.DBF',
    'f1_397_isorto_stl': 'F1_397_ISORTO_STL.DBF',
    'f1_398_ancl_ps': 'F1_398_ANCL_PS.DBF',
    'f1_399_mth_peak': 'F1_399_MTH_PEAK.DBF',
    'f1_400_sys_peak': 'F1_400_SYS_PEAK.DBF',
    'f1_400a_iso_peak': 'F1_400A_ISO_PEAK.DBF',
    'f1_429_trans_aff': 'F1_429_TRANS_AFF.DBF',
    'f1_allowances_nox': 'F1_ALLOWANCES_NOX.DBF',
    'f1_cmpinc_hedge_a': 'F1_CMPINC_HEDGE_A.DBF',
    'f1_cmpinc_hedge': 'F1_CMPINC_HEDGE.DBF',
    'f1_email': 'F1_EMAIL.DBF',
    'f1_rg_trn_srv_rev': 'F1_RG_TRN_SRV_REV.DBF',
    'f1_s0_checks': 'F1_S0_CHECKS.DBF',
    'f1_s0_filing_log': 'F1_S0_FILING_LOG.DBF',
    'f1_security': 'F1_SECURITY.DBF'
}
