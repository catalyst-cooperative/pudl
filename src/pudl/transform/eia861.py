"""Module to perform data cleaning functions on EIA861 data tables.

All transformations include:
- Replace . values with NA.
"""
import pandas as pd
from dagster import AssetIn, AssetOut, Output, asset, multi_asset

import pudl
from pudl.helpers import (
    add_fips_ids,
    clean_eia_counties,
    convert_cols_dtypes,
    convert_to_date,
    fix_eia_na,
)
from pudl.metadata.classes import Package
from pudl.metadata.enums import (
    CUSTOMER_CLASSES,
    FUEL_CLASSES,
    NERC_REGIONS,
    RELIABILITY_STANDARDS,
    REVENUE_CLASSES,
    RTO_CLASSES,
    TECH_CLASSES,
)
from pudl.metadata.fields import apply_pudl_dtypes
from pudl.metadata.labels import ESTIMATED_OR_ACTUAL

logger = pudl.logging_helpers.get_logger(__name__)


BA_ID_NAME_FIXES: pd.DataFrame = (
    pd.DataFrame(
        [
            # report_date, util_id, ba_id, ba_name
            ("2001-01-01", 40577, 99999, "Multiple Control Areas"),
            ("2002-01-01", 40577, 99999, "Multiple Control Areas"),
            ("2002-01-01", 2759, 13781, "Xcel Energy"),
            ("2002-01-01", 1004, 40604, "Heartland Consumer Power Dist."),
            ("2002-01-01", 5659, 20847, "Wisconsin Electric Power"),
            ("2002-01-01", 5588, 9417, "Interstate Power & Light"),
            ("2002-01-01", 6112, 9417, "INTERSTATE POWER & LIGHT"),
            ("2002-01-01", 6138, 13781, "Xcel Energy"),
            ("2002-01-01", 6276, pd.NA, "Vectren Energy Delivery"),
            ("2002-01-01", 6501, 9417, "Interstate Power and Light"),
            ("2002-01-01", 6579, 4716, "Dairyland Power Coop"),
            ("2002-01-01", 6848, pd.NA, pd.NA),
            ("2002-01-01", 7140, 18195, "Southern Co Services Inc"),
            ("2002-01-01", 7257, 22500, "Westar Energy"),
            ("2002-01-01", 7444, 14232, "Minnkota Power Cooperative"),
            ("2002-01-01", 8490, 22500, "Westar"),
            ("2002-01-01", 8632, 12825, "NorthWestern Energy"),
            ("2002-01-01", 8770, 22500, "Westar Energy"),
            ("2002-01-01", 8796, 13434, "ISO New England"),
            ("2002-01-01", 9699, pd.NA, "Tri-State G&T"),
            ("2002-01-01", 10040, 13781, "Xcel Energy"),
            ("2002-01-01", 10171, 56669, "Midwest Indep System Operator"),
            # This might also be MISO's BA ID... need to investigate more.
            # ("2002-01-01", 10171, 12524, "Midwest Indep System Operator"),
            ("2002-01-01", 11053, 9417, "INTERSTATE POWER & LIGHT"),
            ("2002-01-01", 11148, 2775, "California ISO"),
            ("2002-01-01", 11522, 1, "Maritimes-Canada"),
            ("2002-01-01", 11731, 13781, "XCEL Energy"),
            ("2002-01-01", 11788, 9417, "Interstate Power & Light"),
            ("2002-01-01", 12301, 14232, "Minnkota Power Cooperative"),
            ("2002-01-01", 12698, 20391, "Aquila Networks - MPS"),
            ("2002-01-01", 12706, 18195, "Southern Co Services Inc"),
            ("2002-01-01", 3258, 9417, "Interstate Power & Light"),
            ("2002-01-01", 3273, 15473, "Public Regulatory Commission"),
            ("2002-01-01", 3722, 9417, "Interstate Power and Light"),
            ("2002-01-01", 1417, 12825, "NorthWestern Energy"),
            ("2002-01-01", 1683, 12825, "Northwestern Energy"),
            ("2002-01-01", 1890, 5416, "Duke Energy Corporation"),
            ("2002-01-01", 4319, 20447, "Okla. Municipal Pwr. Authority"),
            ("2002-01-01", 18446, 9417, "Interstate Power and Light"),
            ("2002-01-01", 19108, pd.NA, "NC Rural Electrification Auth."),
            ("2002-01-01", 19545, 28503, "Western Area Power Admin"),
            ("2002-01-01", 12803, 18195, "Southern Illinois Power"),
            ("2002-01-01", 13382, 8283, "Harrison County Rural Electric"),
            ("2002-01-01", 13423, 829, "Town of New Carlisle"),
            ("2002-01-01", 13815, 13781, "Xcel Energy"),
            ("2002-01-01", 14649, 18195, "GSOC (Georgia System Operation"),
            ("2002-01-01", 15672, 924, "Associated Electric Coop Inc"),
            ("2002-01-01", 16023, 9417, "Interstate Power and Light"),
            ("2002-01-01", 16463, pd.NA, "Central Louisiana Electric Co."),
            ("2002-01-01", 16922, 22500, "Westar Energy"),
            ("2002-01-01", 16992, 9417, "Interstate Power and Light"),
            ("2002-01-01", 17643, 924, "Associated Electric Coop Inc"),
            ("2002-01-01", 17706, 9417, "Interstate Power & Light"),
            ("2002-01-01", 20811, 19876, "Dominion NC Power"),
            ("2002-01-01", 3227, 15466, "Xcel Energy"),
            ("2002-01-01", 20227, 14063, "OG&E"),
            ("2002-01-01", 17787, 13337, "Mun. Energy Agcy of Nebraska"),
            ("2002-01-01", 19264, 17718, "Excel Energy"),
            ("2002-01-01", 11701, 19578, "We Energies"),
            ("2002-01-01", 28802, 14725, "PJM Interconnection"),
            ("2002-01-01", 20546, 1692, "Big Rivers Electric Corp."),
            ("2002-01-01", 6223, 1, "Maritimes-Canada"),
            ("2002-01-01", 14405, 19876, "VA Power"),
            ("2002-01-01", 14405, 14725, "PJM"),
            ("2002-01-01", 12698, 20391, "Aquila Networks - L&P"),
            ("2002-01-01", 16267, 12698, "Aquila"),
            ("2002-01-01", 15871, 5723, "ERC of Texas"),
            ("2002-01-01", 6753, 28503, "Regional Office"),
            ("2002-01-01", 5571, 14328, "Pacific Gas and Electric Co."),
            ("2002-01-01", 367, pd.NA, "Western Area Power Admin"),
            ("2002-01-01", 3247, 13501, "NYISO"),
            ("2002-01-01", 11014, 5723, "Ercot"),
            ("2002-01-01", 20845, 12427, "Michigan Power Pool 12427"),
            ("2002-01-01", 17267, pd.NA, "Watertown, SD"),
            ("2002-01-01", 12811, pd.NA, "First Energy Corp."),
            ("2002-01-01", 17368, 13501, "NYISO"),
            ("2002-01-01", 5877, 13501, "NYISO"),
            ("2002-01-01", 3240, pd.NA, "Pacific NW Generating Cooperat"),
            ("2002-01-01", 3037, pd.NA, "Trans Electric"),
            ("2002-01-01", 12199, 28503, "WAPA-Rocky Mountain"),
            ("2002-01-01", 8936, 14378, "Pacificorp"),
            ("2002-01-01", 40604, pd.NA, "Watertown, SD Office"),
            ("2002-01-01", 19108, pd.NA, "USDA- Rural Utility Service"),
            ("2002-01-01", 8199, 20391, "Aquila"),
            ("2002-01-01", 12698, 20391, "Aquila Networks - WPC"),
            ("2002-01-01", 12698, 20391, "Aquila Networks - WPK"),
            ("2002-01-01", 20387, 14725, "PJM West"),
            ("2002-01-01", 588, 20447, "Western Farmers Elec Coop Inc"),
            ("2002-01-01", 17561, 5723, "ERCOT ISO"),
            ("2002-01-01", 17320, 13781, "Xcel Energy"),
            ("2002-01-01", 13676, 17716, "Southwestern Power Admin."),
            ("2002-01-01", 5703, 13501, "NTISO"),
            ("2002-01-01", 113, 13501, "NYISO"),
            ("2002-01-01", 4486, pd.NA, "REMC of Western Indiana"),
            ("2002-01-01", 1039, 13501, "NYISO"),
            ("2002-01-01", 5609, pd.NA, "NMISA"),
            ("2002-01-01", 3989, pd.NA, "WAPA"),
            ("2002-01-01", 13539, 13501, "NY Independent System Operator"),
            ("2002-01-01", 15263, 14725, "PJM West"),
            ("2002-01-01", 12796, 14725, "PJM West"),
            ("2002-01-01", 3539, 13434, "ISO New England"),
            ("2002-01-01", 3575, 13434, "ISO New England"),
            ("2002-01-01", 3559, 13434, "ISO New England"),
            ("2002-01-01", 18193, pd.NA, pd.NA),
            ("2002-01-01", 838, 3413, "Chelan PUD"),
            ("2002-01-01", 1049, 1738, "Bonneville"),
            ("2002-01-01", 9248, 14725, "PJM"),
            ("2002-01-01", 15026, 803, "APS Control Area"),
            ("2002-01-01", 798, 16572, "Salt River Project"),
            ("2002-01-01", 5603, 13501, "ISO - NEW YORK"),
            ("2002-01-01", 12260, 19876, "Dominion Virginia Power"),
            ("2002-01-01", 14788, 17716, "Southwest Power Administration"),
            ("2002-01-01", 12909, 22500, "Westar Energy"),
            ("2002-01-01", 5605, 9417, "Interstate Power and Light"),
            ("2002-01-01", 10908, 9417, "Interstate Power and Light"),
            ("2003-01-01", 3258, 9417, "Interstate Power & Light"),
            ("2003-01-01", 6501, 9417, "Interstate Power & Light"),
            ("2003-01-01", 10650, 9417, "Interstate Power & Light"),
            ("2003-01-01", 16992, 9417, "Interstate Power & Light"),
            ("2003-01-01", 3722, 9417, "Interstate Power & Light"),
            ("2003-01-01", 11788, 9417, "Interstate Power & Light"),
            ("2003-01-01", 5588, 9417, "Interstate Power & Light"),
            ("2003-01-01", 11053, 9417, "Interstate Power & Light"),
            ("2003-01-01", 16023, 9417, "Interstate Power & Light"),
            ("2003-01-01", 17706, 9417, "Interstate Power & Light"),
            ("2003-01-01", 18446, 9417, "Interstate Power & Light"),
            ("2004-01-01", 5309, 18195, "Southern Company Services Inc"),
            ("2004-01-01", 192, 192, "Ryant T. Rose"),
            ("2004-01-01", 6501, 9417, "Interstate Power & Light"),
            ("2004-01-01", 16992, 9417, "Interstate Power & Light"),
            ("2004-01-01", 8192, 14725, "PJM-West"),
            ("2004-01-01", 192, 192, "Phillip K. Peter, Sr."),
            ("2004-01-01", 192, 192, "Nelson Kinegak"),
            ("2004-01-01", 1004, 40604, "Heartland Consumer Power Dist."),
            ("2004-01-01", 3258, 9417, "Interstate Power & Light"),
            ("2004-01-01", 3722, 9417, "Interstate Power & Light"),
            ("2004-01-01", 19879, pd.NA, "Kevin Smalls St Croix Districe"),
            ("2004-01-01", 11788, 9417, "Interstate Power & Light"),
            ("2004-01-01", 4191, 13434, "NEISO"),
            ("2004-01-01", 10650, 9417, "Interstate Power & Light"),
            ("2004-01-01", 11053, 9417, "Interstate Power & Light"),
            ("2004-01-01", 18446, 9417, "Interstate Power & Light"),
            ("2004-01-01", 27000, pd.NA, "Multiple Operators"),
            ("2004-01-01", 19879, pd.NA, "Corey Hodge - St Thomass/St Jo"),
            ("2004-01-01", 13382, 8283, "Harrison County Rural Electric"),
            ("2004-01-01", 10784, pd.NA, "Hawkeye Tri-county REC"),
            ("2004-01-01", 16922, pd.NA, "The Brown Atchison Electric Co"),
            ("2004-01-01", 15026, 803, "APS Control Area"),
            ("2005-01-01", 192, 192, "Ryant T. Rose"),
            ("2005-01-01", 192, 192, "Phillip K. Peter, Sr."),
            ("2005-01-01", 192, 182, "Nelson Kinegak"),
            ("2005-01-01", 3258, 9417, "Interstate Power & Light"),
            ("2005-01-01", 1004, 40604, "Heartland Consumer Power Dist."),
            ("2005-01-01", 5309, 18195, "Southern Company Services Inc"),
            ("2005-01-01", 6501, 9417, "Interstate Power & Light"),
            ("2005-01-01", 10623, 6455, "Florida Power Corp"),
            ("2005-01-01", 10650, 9417, "Interstate Power & Light"),
            ("2005-01-01", 13382, 8283, "Harrison County Rural Electric"),
            ("2005-01-01", 16922, pd.NA, "The Brown Atchison Electric Co"),
            ("2005-01-01", 3722, 9417, "Interstate Power & Light"),
            ("2005-01-01", 4191, 13434, "NEISO"),
            ("2005-01-01", 11788, 9417, "Interstate Power & Light"),
            ("2005-01-01", 8192, 14725, "PJM-West"),
            ("2005-01-01", 11053, 9417, "Interstate Power & Light"),
            ("2005-01-01", 13815, 13781, "Northern States Power Co"),
            ("2005-01-01", 15026, 803, "APS Control Area"),
            ("2005-01-01", 18446, 9417, "Interstate Power & Light"),
            ("2005-01-01", 19879, pd.NA, "Kevin Smalls St Croix Districe"),
            ("2005-01-01", 19879, pd.NA, "Corey Hodge - St Thomass/St Jo"),
            ("2005-01-01", 27000, pd.NA, "Multiple Operators"),
            ("2005-01-01", 10610, 13501, "ISO New York"),
            ("2006-01-01", 10610, 13501, "ISO New York"),
            ("2008-01-01", 10610, 13501, "ISO New York"),
            ("2009-01-01", 10610, 13501, "ISO New York"),
            ("2010-01-01", 6389, 3755, "Cleveland Electric Illum Co"),
            ("2010-01-01", 6389, 13998, "Ohio Edison Co"),
            ("2010-01-01", 6389, 18997, "Toledo Edison Co"),
            ("2010-01-01", 6949, 10000, "Kansas City Power & Light Co"),
            ("2010-01-01", 14127, 14127, "Omaha Public Power District"),
            ("2010-01-01", 11196, 13434, "ISO New England"),
            ("2010-01-01", 97, 56669, "Midwest Independent System Operator"),
            ("2010-01-01", 3258, 56669, "Midwest Independent System Operator"),
            ("2010-01-01", 3405, 56669, "Midwest Independent System Operator"),
            ("2010-01-01", 3755, 56669, "Midwest Independent System Operator"),
            ("2010-01-01", 7292, 56669, "Midwest Independent System Operator"),
            ("2010-01-01", 8847, 56669, "Midwest Independent System Operator"),
            ("2010-01-01", 11701, 56669, "Midwest Independent System Operator"),
            ("2010-01-01", 13032, 56669, "Midwest Independent System Operator"),
            ("2010-01-01", 13998, 56669, "Midwest Independent System Operator"),
            ("2010-01-01", 14716, 56669, "Midwest Independent System Operator"),
            ("2010-01-01", 17141, 56669, "Midwest Independent System Operator"),
            ("2010-01-01", 18997, 56669, "Midwest Independent System Operator"),
            ("2010-01-01", 21249, 56669, "Midwest Independent System Operator"),
            ("2010-01-01", 40582, 56669, "Midwest Independent System Operator"),
            ("2010-01-01", 54862, 56669, "Midwest Independent System Operator"),
            ("2010-01-01", 56162, 56669, "Midwest Independent System Operator"),
            ("2010-01-01", 56496, 56669, "Midwest Independent System Operator"),
            ("2010-01-01", 10610, 13501, "ISO New York"),
            ("2011-01-01", 1968, 56669, "Midwest Independent System Operator"),
            ("2011-01-01", 20806, 56669, "Midwest Independent System Operator"),
            ("2011-01-01", 29296, 56669, "Midwest Independent System Operator"),
            ("2012-01-01", 1968, 56669, "Midwest Independent System Operator"),
            ("2012-01-01", 20806, 56669, "Midwest Independent System Operator"),
            ("2012-01-01", 29296, 56669, "Midwest Independent System Operator"),
        ],
        columns=[
            "report_date",  # We have this
            "utility_id_eia",  # We have this
            "balancing_authority_id_eia",  # We need to set this
            "balancing_authority_name_eia",  # We have this
        ],
    )
    .assign(report_date=lambda x: pd.to_datetime(x.report_date))
    .pipe(apply_pudl_dtypes, group="eia")
    .dropna(subset=["report_date", "balancing_authority_name_eia", "utility_id_eia"])
    .set_index(["report_date", "balancing_authority_name_eia", "utility_id_eia"])
)

EIA_FIPS_COUNTY_FIXES: pd.DataFrame = pd.DataFrame(
    [
        ("AK", "Akiachak", "Bethel Census Area"),
        ("AK", "Aleutian Islands", "Aleutians East"),
        ("AK", "Aleutians Ea", "Aleutians East"),
        ("AK", "Aleutians East Boro", "Aleutians East Borough"),
        ("AK", "Aleutians We", "Aleutians West Census Area"),
        ("AK", "Aleutians West (unorganized)", "Aleutians West Census Area"),
        ("AK", "Angoon", "Hoonah-Angoon"),
        ("AK", "Borough, Kodiak Island", "Kodiak Island Borough"),
        ("AK", "Chilkat Vall", "Hoonah-Angoon Census Area"),
        ("AK", "Chilkat Valley", "Hoonah-Angoon Census Area"),
        ("AK", "Chuathbaluk", "Bethel Census Area"),
        ("AK", "Copper Basin", "Valdez-Cordova Census Area"),
        ("AK", "Cordova", "Valdez-Cordova"),
        ("AK", "Crooked Creek", "Bethel Census Area"),
        ("AK", "Delta - No County", "Southeast Fairbanks Census Area"),
        ("AK", "Elfin Cove", "Hoonah-Angoon Census Area"),
        ("AK", "FRBS North Star", "Fairbanks North Star Borough"),
        ("AK", "Galena", "Yukon-Koyukuk Census Area"),
        ("AK", "Hoonah", "Hoonah-Angoon"),
        ("AK", "Kake", "Prince of Wales-Hyder Census Area"),
        ("AK", "Kasaan", "Prince of Wales-Hyder Census Area"),
        ("AK", "Kenai Penins", "Kenai Peninsula"),
        ("AK", "Ketchikan Ga", "Ketchikan Gateway Borough"),
        ("AK", "Ketchikan Gateway Bo", "Ketchikan Gateway Borough"),
        ("AK", "Klukwan", "Hoonah-Angoon Census Area"),
        ("AK", "Kodiak Isla", "Kodiak Island Borough"),
        ("AK", "Kodiak Islan", "Kodiak Island"),
        ("AK", "Kodiak Island Boroug", "Kodiak Island Borough"),
        ("AK", "Kuskokwim Bay", "Bethel Census Area"),
        ("AK", "Kwigillingok", "Bethel Census Area"),
        ("AK", "LARSEN BAY", "Kodiak Island Borough"),
        ("AK", "Lake & Peninsula Bor", "Lake and Peninsula Borough"),
        ("AK", "Lake & Peninsula Borough", "Lake and Peninsula"),
        ("AK", "Lake and Pen", "Lake and Peninsula"),
        ("AK", "Larsen Bay", "Kodiak Island Borough"),
        ("AK", "Matanuska Su", "Matanuska-Susitna Borough"),
        ("AK", "Matanuska Susitna", "Matanuska-Susitna"),
        ("AK", "Matanuska Susitna Borough", "Matanuska-Susitna"),
        ("AK", "NW Arctic Borough", "Northwest Arctic"),
        ("AK", "Nenana", "Yukon-Koyukuk Census Area"),
        ("AK", "Nenana - No County", "Yukon-Koyukuk Census Area"),
        ("AK", "Nenena - No County", "Yukon-Koyukuk Census Area"),
        ("AK", "Northwest Ar", "Northwest Arctic Borough"),
        ("AK", "Northwest Arctic Bor", "Northwest Arctic Borough"),
        ("AK", "Notin", "Hoonah-Angoon Census Area"),
        ("AK", "Prince Wales", "Prince of Wales-Hyder"),
        ("AK", "Prince of Wa", "Prince of Wales-Hyder Census Area"),
        ("AK", "Prince of Wale", "Prince of Wales-Hyder"),
        ("AK", "Prince of Wales", "Prince of Wales-Hyder Census Area"),
        ("AK", "Prince of Wales Ketchikan", "Prince of Wales-Hyder"),
        ("AK", "Prince of Wales-Outer Ketchika", "Prince of Wales-Hyder Census Area"),
        ("AK", "Red Devil", "Bethel Census Area"),
        ("AK", "Skagway Hoonah Angoon", "Hoonah-Angoon"),
        ("AK", "Skagway Yaku", "Skagway"),
        ("AK", "Skagway-Hoonah-Angoon", "Hoonah-Angoon Census Area"),
        ("AK", "Skagway-Yakutat", "Skagway"),
        ("AK", "Sleetmute", "Bethel Census Area"),
        ("AK", "Southeast Fa", "Southeast Fairbanks Census Area"),
        ("AK", "Stony River", "Bethel Census Area"),
        ("AK", "Tanana", "Yukon-Koyukuk Census Area"),
        ("AK", "Tenakee Springs", "Hoonah-Angoon Census Area"),
        ("AK", "Valdez Cordo", "Valdez-Cordova"),
        ("AK", "Valdez Cordova", "Valdez-Cordova"),
        ("AK", "Wrangell Pet", "Wrangell"),
        ("AK", "Wrangell Petersburg", "Wrangell"),
        ("AK", "Wrangell-Petersburg", "Petersburg Census Area"),
        ("AK", "Yukon Koyuku", "Yukon-Koyukuk"),
        ("AK", "Yukon Koyukuk", "Yukon-Koyukuk"),
        ("AK", "Yukon-Koyuku", "Yukon-Koyukuk"),
        ("AL", "De Kalb", "DeKalb"),
        ("AR", "Hot Springs", "Hot Spring County"),
        ("AR", "Saint Franci", "St. Francis"),
        ("AZ", "Maricopa (see footnote)", "Maricopa"),
        ("AZ", "Nogales", "Santa Cruz"),
        ("CA", "San Bernadino", "San Bernardino"),
        ("CA", "San Bernardi", "San Bernardino"),
        ("CA", "San Francisc", "San Francisco County"),
        ("CA", "San Luis Obi", "San Luis Obispo County"),
        ("CA", "Santa Barbar", "Santa Barbara County"),
        ("CT", "Shelton", "Fairfield"),
        ("DC", "District of", "District of Columbia"),
        ("FL", "Dade", "Miami-Dade"),
        ("FL", "De Soto", "DeSoto"),
        ("FL", "Miami Dade", "Miami-Dade"),
        ("FL", "St. Loucie", "St. Lucie"),
        ("FL", "St. Lucic", "St. Lucie"),
        ("GA", "Chattahooche", "Chattahoochee"),
        ("GA", "De Kalb", "DeKalb"),
        ("GA", "Glasscock", "Glascock County"),
        ("IA", "Hardin -remove", "Hardin County"),
        ("IA", "Harris", "Harrison"),
        ("IA", "Humbolt", "Humboldt"),
        ("IA", "Kossuh", "Kossuth"),
        ("IA", "Louisa-remove", "Louisa County"),
        ("IA", "Lousia", "Louisa"),
        ("IA", "O Brien", "O'Brien"),
        ("IA", "Pottawattami", "Pottawattamie"),
        ("IA", "Poweshick", "Poweshiek"),
        ("IA", "Union-remove", "Union County"),
        ("ID", "Marshall", "Custer County"),
        ("IL", "Burke", "Christian"),
        ("IL", "Carol", "DuPage County"),
        ("IL", "De Kalb", "DeKalb County"),
        ("IL", "DeWitt", "De Witt"),
        ("IL", "Dewitt", "De Witt"),
        ("IL", "Du Page", "DuPage"),
        ("IL", "Green", "Greene"),
        ("IL", "JoDavies", "Jo Daviess"),
        ("IL", "La Salle", "LaSalle"),
        ("IL", "McCoupin", "Macoupin"),
        ("IN", "De Kalb", "DeKalb County"),
        ("IN", "De Kalb County", "DeKalb County"),
        ("IN", "La Porte", "LaPorte"),
        ("IN", "Putman", "Putnam"),
        ("IN", "Pyke", "Pike"),
        ("IN", "Sulliva", "Sullivan"),
        ("KS", "Leaveworth", "Leavenworth"),
        ("KY", "Hawkins", "Hopkins County"),
        ("KY", "LAURE", "Larue County"),
        ("KY", "Spenser", "Spencer"),
        ("KY", "Sullivan", "Union County"),
        ("KY", "WOLE", "Wolfe County"),
        ("LA", "Burke", "Iberia"),
        ("LA", "DeSoto", "De Soto"),
        ("LA", "East Baton R", "East Baton Rouge Parish"),
        ("LA", "East Felicia", "East Feliciana Parish"),
        ("LA", "Jefferson Da", "Jefferson Davis"),
        ("LA", "Morehouse Pa", "Morehouse Parish"),
        ("LA", "Pointe Coupe", "Pointe Coupee"),
        ("LA", "Saint Helina", "St. Helena Parish"),
        ("LA", "Saint Tamman", "St. Tammany Parish"),
        ("LA", "West Baton R", "West Baton Rouge"),
        ("LA", "West Feleciana", "West Feliciana"),
        ("LA", "West Felicia", "West Feliciana Parish"),
        ("MA", "North Essex", "Essex"),
        ("MD", "Baltimore Ci", "Baltimore City"),
        ("MD", "Balto. City", "Baltimore City"),
        ("MD", "Prince Georg", "Prince George's County"),
        ("MD", "Worchester", "Worcester"),
        ("MI", "Antim", "Antrim"),
        ("MI", "Graitiot", "Gratiot County"),
        ("MI", "Grand Traver", "Grand Traverse"),
        ("MI", "Missauke", "Missaukee County"),
        ("MN", "Fairbault", "Faribault"),
        ("MN", "La Qui Parle", "Lac qui Parle County"),
        ("MN", "Lac Qui Parl", "Lac Qui Parle"),
        ("MN", "Lake of The", "Lake of the Woods"),
        ("MN", "Olmstead", "Olmsted County"),
        ("MN", "Ottertail", "Otter Tail"),
        ("MN", "Yellow Medic", "Yellow Medicine"),
        ("MO", "Cape Girarde", "Cape Girardeau"),
        ("MO", "De Kalb", "DeKalb"),
        ("MO", "Paris", "Monroe County"),
        ("MO", "Saint Charle", "St. Charles County"),
        ("MO", "Saint Franco", "St. Francois County"),
        ("MO", "Sainte Genev", "Ste. Genevieve County"),
        ("MS", "Clark", "Clarke"),
        ("MS", "Clark", "Clarke"),
        ("MS", "De Soto", "DeSoto"),
        ("MS", "Homoshitto", "Amite"),
        ("MS", "Jefferson Da", "Jefferson Davis"),
        ("MT", "Anaconda-Dee", "Deer Lodge"),
        ("MT", "Butte-Silver", "Silver Bow"),
        ("MT", "Golden Valle", "Golden Valley"),
        ("MT", "Lewis and Cl", "Lewis and Clark"),
        ("NC", "Cherokee (NP&L)", "Cherokee County"),
        ("NC", "Clay (NP&L)", "Clay County"),
        ("NC", "Gilford", "Guilford"),
        ("NC", "Graham (NP&L)", "Graham County"),
        ("NC", "Hartford", "Hertford"),
        ("NC", "Jackson (NP&L)", "Jackson County"),
        ("NC", "Macon (NP&L)", "Macon County"),
        ("NC", "North Hampton", "Northampton"),
        ("NC", "Stanley", "Stanly County"),
        ("NC", "Swain (NP&L)", "Swain County"),
        ("ND", "Golden Valle", "Golden Valley County"),
        ("ND", "La Moure", "LaMoure"),
        ("ND", "Remsey", "Ramsey County"),
        ("NH", "Hillsboro", "Hillsborough County"),
        ("NH", "New Hampshire", "Coos"),
        ("NH", "Plaquemines", "Coos"),
        ("NV", "Carson City city", "Carson City"),
        ("NY", "Saint Lawren", "St. Lawrence County"),
        ("NY", "Westcherster", "Westchester"),
        ("OH", "Cochocton", "Coshocton County"),
        ("OH", "Columbian", "Columbiana County"),
        ("OH", "Tuscarawa", "Tuscarawas County"),
        ("OK", "Cimmaron", "Cimarron"),
        ("OK", "MuCurtain", "McCurtain County"),
        ("OR", "Unioin", "Union"),
        ("PA", "Northumberla", "Northumberland"),
        ("PR", "Aquadilla", "Aguadilla"),
        ("PR", "Sabana Grand", "Sabana Grande"),
        ("PR", "San Sebastia", "San Sebastian"),
        ("PR", "Trujillo Alt", "Trujillo Alto"),
        ("RI", "Portsmouth", "Newport"),
        ("SD", "Pierce", "Hughes County"),
        ("SD", "Valley Springs", "Minnehaha County"),
        ("TX", "Collingswort", "Collingsworth"),
        ("TX", "De Witt", "DeWitt"),
        ("TX", "Hayes", "Hays"),
        ("TX", "San Augustin", "San Augustine"),
        ("VA", "Albermarle", "Albemarle County"),
        ("VA", "Alexandria C", "Alexandria City"),
        ("VA", "Charlottesvi", "Charlottesville City"),
        ("VA", "Chesapeake C", "Chesapeake City"),
        ("VA", "City of Manassas", "Manassas City"),
        ("VA", "City of Suff", "Suffolk City"),
        ("VA", "City of Suffolk", "Suffolk city"),
        ("VA", "Clifton Forg", "Alleghany"),
        ("VA", "Clifton Forge", "Alleghany"),
        ("VA", "Colonial Hei", "Colonial Heights City"),
        ("VA", "Covington Ci", "Covington City"),
        ("VA", "Fredericksbu", "Fredericksburg City"),
        ("VA", "Hopewell Cit", "Hopewell City"),
        ("VA", "Isle of Wigh", "Isle of Wight"),
        ("VA", "King and Que", "King and Queen"),
        ("VA", "Lexington Ci", "Lexington City"),
        ("VA", "Manasas Park", "Manassas Park city"),
        ("VA", "Manassas Cit", "Manassas City"),
        ("VA", "Manassas Par", "Manassas Park City"),
        ("VA", "Northumberla", "Northumberland"),
        ("VA", "Petersburg C", "Petersburg City"),
        ("VA", "Poquoson Cit", "Poquoson City"),
        ("VA", "Portsmouth C", "Portsmouth City"),
        ("VA", "Prince Edwar", "Prince Edward"),
        ("VA", "Prince Georg", "Prince George"),
        ("VA", "Prince Willi", "Prince William"),
        ("VA", "Richmond Cit", "Richmond City"),
        ("VA", "Staunton Cit", "Staunton City"),
        ("VA", "Virginia", "Virginia Beach city"),
        ("VA", "Virginia Bea", "Virginia Beach City"),
        ("VA", "Waynesboro C", "Waynesboro City"),
        ("VA", "Winchester C", "Winchester City"),
        ("WA", "Wahkiakurn", "Wahkiakum"),
        ("WV", "Greenbriar", "Greenbrier County"),
    ],
    columns=["state", "eia_county", "fips_county"],
)

BA_NAME_FIXES: pd.DataFrame = pd.DataFrame(
    [
        ("Omaha Public Power District", 14127, "OPPD"),
        ("Kansas City Power & Light Co", 10000, "KCPL"),
        ("Toledo Edison Co", 18997, pd.NA),
        ("Ohio Edison Co", 13998, pd.NA),
        ("Cleveland Electric Illum Co", 3755, pd.NA),
    ],
    columns=[
        "balancing_authority_name_eia",
        "balancing_authority_id_eia",
        "balancing_authority_code_eia",
    ],
)

NERC_SPELLCHECK: dict[str, str] = {
    "GUSTAVUSAK": "ASCC",
    "AK": "ASCC",
    "HI": "HICC",
    "ERCTO": "ERCOT",
    "RFO": "RFC",
    "RF": "RFC",
    "REC": "RFC",
    "SSP": "SPP",
    "VACAR": "SERC",  # VACAR is a subregion of SERC
    "GATEWAY": "SERC",  # GATEWAY is a subregion of SERC
    "TERR": "GU",
    25470: "MRO",
    "TX": "TRE",
    "NY": "NPCC",
    "NEW": "NPCC",
    "YORK": "NPCC",
}


###############################################################################
# EIA Form 861 Transform Helper functions
###############################################################################
def _pre_process(df: pd.DataFrame) -> pd.DataFrame:
    """Pre-processing applied to all EIA-861 dataframes.

    * Standardize common NA values found in EIA spreadsheets.
    * Drop the ``early_release`` column, which only contains non-null values when the
      data is an early release, and we extract this information from the filenames, as
      it's uniform across the whole dataset.
    * Convert report_year column to report_date.
    """
    return (
        fix_eia_na(df)
        .drop(columns=["early_release"], errors="ignore")
        .pipe(convert_to_date)
    )


def _post_process(df: pd.DataFrame) -> pd.DataFrame:
    """Post-processing applied to all EIA-861 dataframes."""
    return convert_cols_dtypes(df, data_source="eia")


def _filter_class_cols(df, class_list):
    regex = f"^({'_|'.join(class_list)}).*$"
    return df.filter(regex=regex)


def _filter_non_class_cols(df, class_list):
    regex = f"^(?!({'_|'.join(class_list)})).*$"
    return df.filter(regex=regex)


def add_backfilled_ba_code_column(df, by_cols: list[str]) -> pd.DataFrame:
    """Make a backfilled Balancing Authority Code column based on codes in later years.

    Args:
        df: table with columns: ``balancing_authority_code_eia``, ``report_date`` and
            all ``by_cols``
        by_cols: list of columns to use as ``by`` argument in :meth:`pd.groupby`


    Returns:
        pandas.DataFrame: An altered version of ``df`` with an additional column
        ``balancing_authority_code_eia_bfilled``
    """
    start_len = len(df)
    start_nas = len(df.loc[df.balancing_authority_code_eia.isnull()])
    logger.info(
        f"Started with {start_nas} missing BA Codes out of {start_len} "
        f"records ({start_nas/start_len:.2%})"
    )
    ba_ids = (
        df[by_cols + ["balancing_authority_code_eia", "report_date"]]
        .drop_duplicates()
        .sort_values(by_cols + ["report_date"])
    )
    ba_ids["balancing_authority_code_eia_bfilled"] = ba_ids.groupby(by_cols)[
        "balancing_authority_code_eia"
    ].fillna(method="bfill")
    ba_eia861_filled = df.merge(ba_ids, how="left")

    end_len = len(ba_eia861_filled)
    if start_len != end_len:
        raise AssertionError(
            f"Number of rows in the dataframe changed {start_len}!={end_len}!"
        )
    end_nas = len(
        ba_eia861_filled.loc[
            ba_eia861_filled.balancing_authority_code_eia_bfilled.isnull()
        ]
    )
    logger.info(
        f"Ended with {end_nas} missing BA Codes out of {end_len} "
        f"records ({end_nas/end_len:.2%})"
    )
    return ba_eia861_filled


def backfill_ba_codes_by_ba_id(df: pd.DataFrame) -> pd.DataFrame:
    """Fill in missing BA Codes by backfilling based on BA ID.

    Note:
        The BA Code to ID mapping can change from year to year. If a Balancing Authority
        is bought by another entity, the code may change, but the old EIA BA ID will be
        retained.

    Args:
        df: The transformed EIA 861 Balancing Authority dataframe
            (balancing_authority_eia861).

    Returns:
        pandas.DataFrame: The balancing_authority_eia861 dataframe, but with many fewer
        NA values in the balancing_authority_code_eia column.
    """
    ba_eia861_filled = (
        add_backfilled_ba_code_column(df, by_cols=["balancing_authority_id_eia"])
        .assign(
            balancing_authority_code_eia=lambda x: x.balancing_authority_code_eia_bfilled
        )
        .drop(columns="balancing_authority_code_eia_bfilled")
    )
    return ba_eia861_filled


def _tidy_class_dfs(df, df_name, idx_cols, class_list, class_type, keep_totals=False):
    # Clean up values just enough to use primary key columns as a multi-index:
    logger.debug(f"Cleaning {df_name} table index columns so we can tidy data.")
    if "balancing_authority_code_eia" in idx_cols:
        df = df.assign(
            balancing_authority_code_eia=(
                lambda x: x.balancing_authority_code_eia.fillna("UNK")
            )
        )
    raw_df = (
        df.dropna(subset=["utility_id_eia"])
        .astype({"utility_id_eia": "Int64"})
        .set_index(idx_cols)
    )
    # Split the table into index, data, and "denormalized" columns for processing:
    # Separate customer classes and reported data into a hierarchical index
    logger.debug(f"Stacking EIA861 {df_name} data columns by {class_type}.")
    data_cols = _filter_class_cols(raw_df, class_list)

    # Create a regex identifier that splits the column headers based on the strings
    # deliniated in the class_list not just an underscore. This enables prefixes with
    # underscores such as fuel_cell as opposed to single-word prefixes followed by
    # underscores. Final string looks like: '(?<=customer_test)_|(?<=unbundled)_'
    # This ensures that the underscore AFTER the desired string (that can also include
    # underscores) is where the column headers are split, not just the first underscore.
    class_list_regex = "|".join(["(?<=" + col + ")_" for col in class_list])

    data_cols.columns = data_cols.columns.str.split(
        rf"{class_list_regex}", n=1, expand=True
    ).set_names([class_type, None])
    # Now stack the customer classes into their own categorical column,
    data_cols = data_cols.stack(level=0, dropna=False).reset_index()
    denorm_cols = _filter_non_class_cols(raw_df, class_list).reset_index()

    # Merge the index, data, and denormalized columns back together
    tidy_df = pd.merge(denorm_cols, data_cols, on=idx_cols)

    # Compare reported totals with sum of component columns
    if "total" in class_list:
        _compare_totals(data_cols, idx_cols, class_type, df_name)
    if keep_totals is False:
        tidy_df = tidy_df.query(f"{class_type}!='total'")

    return tidy_df, idx_cols + [class_type]


def _drop_dupes(df, df_name, subset):
    tidy_nrows = len(df)
    deduped_df = df.drop_duplicates(subset=subset)
    deduped_nrows = len(df)
    logger.info(
        f"Dropped {tidy_nrows-deduped_nrows} duplicate records from EIA 861 "
        f"{df_name} table, out of a total of {tidy_nrows} records "
        f"({(tidy_nrows-deduped_nrows)/tidy_nrows:.4%} of all records). "
    )
    return deduped_df


def _check_for_dupes(df: pd.DataFrame, df_name: str, subset: list[str]) -> pd.DataFrame:
    dupes = df.duplicated(subset=subset, keep=False)
    if dupes.any():
        raise AssertionError(
            f"Found {len(df[dupes])} duplicate rows in the {df_name} table, "
            f"when zero were expected!"
        )
    return df


def _compare_totals(data_cols, idx_cols, class_type, df_name):
    """Compare reported totals with sum of component columns.

    Args:
        data_cols (pd.DataFrame): A DataFrame containing only the columns with
            normalized information.
        idx_cols (list): A list of the primary keys for the given denormalized
            DataFrame.
        class_type (str): The name (either 'customer_class' or 'tech_class') of
            the column for which you'd like to compare totals to components.
        df_name (str): The name of the dataframe.
    """
    # Convert column dtypes so that numeric cols can be adequately summed
    data_cols = convert_cols_dtypes(data_cols, data_source="eia")
    # Drop data cols that are non numeric (preserve primary keys)
    logger.debug(f"{idx_cols}, {class_type}")
    data_cols = (
        data_cols.set_index(idx_cols + [class_type])
        .select_dtypes("number")
        .reset_index()
    )
    logger.debug(f"{data_cols.columns.tolist()}")
    # Create list of data columns to be summed
    # (may include non-numeric that will be excluded)
    data_col_list = set(data_cols.columns.tolist()) - set(idx_cols + [class_type])
    logger.debug(f"{data_col_list}")
    # Distinguish reported totals from segments
    data_totals_df = data_cols.loc[data_cols[class_type] == "total"]
    data_no_tots_df = data_cols.loc[data_cols[class_type] != "total"]
    # Calculate sum of segments for comparison against reported total
    data_sums_df = data_no_tots_df.groupby(idx_cols + [class_type], observed=True).sum()
    sum_total_df = pd.merge(
        data_totals_df,
        data_sums_df,
        on=idx_cols,
        how="outer",
        suffixes=("_total", "_sum"),
    )
    # Check each data column's calculated sum against the reported total
    for col in data_col_list:
        col_df = sum_total_df.loc[sum_total_df[col + "_total"].notnull()]
        if len(col_df) > 0:
            col_df = col_df.assign(
                compare_totals=lambda x: (x[col + "_total"] == x[col + "_sum"])
            )
            bad_math = (col_df["compare_totals"]).sum() / len(col_df)
            logger.debug(
                f"{df_name}: for column {col}, {bad_math:.0%} "
                "of non-null reported totals = the sum of parts."
            )
        else:
            logger.debug(f"{df_name}: for column {col} all total values are NaN")


def clean_nerc(df: pd.DataFrame, idx_cols: list[str]) -> pd.DataFrame:
    """Clean NERC region entries and make new rows for multiple nercs.

    This function examines reported NERC regions and makes sure the output column of the
    same name has reliable, singular NERC region acronyms. To do so, this function
    identifies entries where there are two or more NERC regions specified in a single
    cell (such as SPP & ERCOT) and makes new, duplicate rows for each NERC region. It
    also converts non-recognized reported nerc regions to 'UNK'.

    Args:
        df: A DataFrame with the column 'nerc_region' to be cleaned.
        idx_cols: A list of the primary keys and `nerc_region`.

    Returns:
        A DataFrame with correct and clean nerc regions.
    """
    idx_no_nerc = idx_cols.copy()
    if "nerc_region" in idx_no_nerc:
        idx_no_nerc.remove("nerc_region")

    # Split raw df into primary keys plus nerc region and other value cols
    nerc_df = df[idx_cols].copy()
    other_df = df.drop(columns="nerc_region").set_index(idx_no_nerc)

    # Make all values upper-case
    # Replace all NA values with UNK
    # Make nerc values into lists to see how many separate values are stuffed into one row (ex: 'SPP & ERCOT' --> ['SPP', 'ERCOT'])
    nerc_df = nerc_df.assign(
        nerc_region=(
            lambda x: (x.nerc_region.str.upper().fillna("UNK").str.findall(r"[A-Z]+"))
        )
    )

    # Record a list of the reported nerc regions not included in the recognized regions list (these eventually become UNK)
    nerc_col = nerc_df["nerc_region"].tolist()
    nerc_list = list({item for sublist in nerc_col for item in sublist})
    non_nerc_list = [
        nerc_entity
        for nerc_entity in nerc_list
        if nerc_entity not in NERC_REGIONS + list(NERC_SPELLCHECK.keys())
    ]
    if non_nerc_list:
        logger.info(
            "The following reported NERC regions are not currently recognized and "
            f"become UNK values: {non_nerc_list}"
        )

    # Function to turn instances of 'SPP_UNK' or 'SPP_SPP' into 'SPP'
    def _remove_nerc_duplicates(entity_list):
        if len(entity_list) > 1:
            if "UNK" in entity_list:
                entity_list.remove("UNK")
            if all(x == entity_list[0] for x in entity_list):
                entity_list = [entity_list[0]]
        return entity_list

    # Go through the nerc regions, spellcheck errors, delete those that aren't
    # recognized, and piece them back together (with _ separator if more than one
    # recognized)
    nerc_df["nerc_region"] = (
        nerc_df["nerc_region"]
        .apply(
            lambda x: (
                [
                    i if i not in NERC_SPELLCHECK.keys() else NERC_SPELLCHECK[i]
                    for i in x
                ]
            )
        )
        .apply(lambda x: sorted(i if i in NERC_REGIONS else "UNK" for i in x))
        .apply(lambda x: _remove_nerc_duplicates(x))
        .str.join("_")
    )

    # Merge all data back together
    full_df = pd.merge(nerc_df, other_df, on=idx_no_nerc)

    return full_df


def _compare_nerc_physical_w_nerc_operational(df: pd.DataFrame) -> pd.DataFrame:
    """Show df rows where physical NERC region does not match operational region.

    In the Utility Data table, there is the 'nerc_region' index column, otherwise
    interpreted as nerc region in which the utility is physically located and the
    'nerc_regions_of_operation' column that depicts the nerc regions the utility
    operates in. In most cases, these two columns are the same, however there are
    certain instances where this is not true. There are also instances where a
    utility operates in multiple nerc regions in which case one row will match and
    another row will not. The output of this function in a table that shows only the
    utilities where the physical nerc region does not match the operational region
    ever, meaning there is no additional row for the same utility during the same
    year where there is a match between the cols.

    Args:
        df: utility_data_nerc_eia861 table from :func:`clean_utility_data_eia861`

    Returns:
        A DataFrame with rows for utilities where NO listed operating nerc region
        matches the "physical location" nerc region column that's a part of the index.
    """
    # Set NA states to UNK
    df["state"] = df["state"].fillna("UNK")

    # Create column indicating whether the nerc region matches the nerc region of
    # operation (TRUE)
    df["nerc_match"] = df["nerc_region"] == df["nerc_regions_of_operation"]

    # Group by utility, state, and report date to see which groups have at least one
    # TRUE value
    grouped_nerc_match_bools = (
        df.groupby(["utility_id_eia", "state", "report_date"])[["nerc_match"]]
        .any()
        .reset_index()
        .rename(columns={"nerc_match": "nerc_group_match"})
    )

    # Merge back with original df to show cases where there are multiple non-matching
    # nerc values per utility id, year, and state.
    expanded_nerc_match_bools = pd.merge(
        df,
        grouped_nerc_match_bools,
        on=["utility_id_eia", "state", "report_date"],
        how="left",
    )

    # Keep only rows where there are no matches for the whole group.
    expanded_nerc_match_bools_false = expanded_nerc_match_bools[
        ~expanded_nerc_match_bools["nerc_group_match"]
    ]

    return expanded_nerc_match_bools_false


def _pct_to_mw(df, pct_col):
    """Turn pct col into mw capacity using total capacity col."""
    mw_value = df["total_capacity_mw"] * df[pct_col] / 100
    return mw_value


def _make_yn_bool(df_object):
    """Turn Y/N reporting into True or False boolean statements for df or series."""
    return df_object.replace(
        {
            "Y": True,
            "y": True,
            "N": False,
            "n": False,
        }
    )


def _thousand_to_one(df_object):
    """Turn reporting in thousands of dollars to regular dollars for df or series."""
    return df_object * 1000


def _harvest_associations(dfs: list[pd.DataFrame], cols: list[str]) -> pd.DataFrame:
    """Compile all unique, non-null combinations of values ``cols`` within ``dfs``.

    Find all unique, non-null combinations of the columns ``cols`` in the dataframes
    ``dfs`` within records that are selected by ``query``. All of ``cols`` must be
    present in each of the ``dfs``.

    Args:
        dfs: DataFrames to search for unique combinations of values.
        cols: Columns within which to find unique, non-null combinations of values.

    Raises:
        ValueError: if no associations for cols are found in dfs.

    Returns:
        A dataframe containing all the unique, non-null combinations of values found in
        ``cols``.
    """
    assn = pd.DataFrame()
    for df in dfs:
        if set(df.columns).issuperset(set(cols)):
            assn = pd.concat([assn, df[cols]])
    assn = assn.dropna().drop_duplicates()
    if assn.empty:
        raise ValueError(
            f"These dataframes contain no associations for the columns: {cols}"
        )
    return assn


###############################################################################
# EIA Form 861 Table Transform Functions
###############################################################################
@asset(io_manager_key="pudl_sqlite_io_manager")
def service_territory_eia861(
    raw_service_territory_eia861: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the EIA 861 utility service territory table.

    Transformations include:

    * Homogenize spelling of county names.
    * Add field for state/county FIPS code.

    Args:
        raw_service_territory_eia861: Raw EIA-861 utility service territory dataframe.

    Returns:
        The cleaned utility service territory dataframe.
    """
    df = _pre_process(raw_service_territory_eia861)
    # A little WV county sandwiched between OH & PA, got miscategorized a few times:
    df.loc[(df.state == "OH") & (df.county == "Brooke"), "state"] = "WV"
    df = (
        # There are a few NA values in the county column which get interpreted
        # as floats, which messes up the parsing of counties by addfips.
        df.astype({"county": "string"})
        # Ensure that we have the canonical US Census county names:
        .pipe(clean_eia_counties, fixes=EIA_FIPS_COUNTY_FIXES)
        # Add FIPS IDs based on county & state names:
        .pipe(add_fips_ids)
        .assign(short_form=lambda x: _make_yn_bool(x.short_form))
        .pipe(_post_process)
    )
    # The Virgin Islands and Guam aren't covered by addfips but they have FIPS:
    st_croix = (df.state == "VI") & (df.county.isin(["St. Croix", "Saint Croix"]))
    df.loc[st_croix, "county_id_fips"] = "78010"
    st_john = (df.state == "VI") & (df.county.isin(["St. John", "Saint John"]))
    df.loc[st_john, "county_id_fips"] = "78020"
    st_thomas = (df.state == "VI") & (df.county.isin(["St. Thomas", "Saint Thomas"]))
    df.loc[st_thomas, "county_id_fips"] = "78030"
    df.loc[df.state == "GU", "county_id_fips"] = "66010"

    pk = (
        Package.from_resource_ids()
        .get_resource("service_territory_eia861")
        .schema.primary_key
    )
    # We've fixed all we can fix! ~99.84% FIPS coverage.
    df = (
        df.dropna(subset=["county_id_fips"])
        .sort_values(pk + ["county"])
        .drop_duplicates(pk)  # Several hundred duplicates w/ different county names.
    )
    return df


@asset
def clean_balancing_authority_eia861(
    raw_balancing_authority_eia861: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the EIA 861 Balancing Authority table.

    Transformations include:

    * Fill in balancing authrority IDs based on date, utility ID, and BA Name.
    * Backfill balancing authority codes based on BA ID.
    * Fix BA code and ID typos.
    """
    # No data tidying required
    # All columns are already type compatible.
    # Value transformations:
    # * Backfill BA codes on a per BA ID basis
    # * Fix data entry errors
    df = (
        _pre_process(raw_balancing_authority_eia861)
        .pipe(apply_pudl_dtypes, "eia")
        .set_index(["report_date", "balancing_authority_name_eia", "utility_id_eia"])
    )
    # Fill in BA IDs based on date, utility ID, and BA Name:
    # using merge and then reverse backfilling balancing_authority_id_eia means
    # that this will work even if not all years are requested
    df = (  # this replaces the previous process to address #828
        df.merge(
            BA_ID_NAME_FIXES,
            on=["report_date", "balancing_authority_name_eia", "utility_id_eia"],
            how="left",
            validate="m:1",
            suffixes=(None, "_"),
        )
        .assign(
            balancing_authority_id_eia=lambda x: x.balancing_authority_id_eia_.fillna(
                x.balancing_authority_id_eia
            )
        )
        .drop(columns=["balancing_authority_id_eia_"])
    )

    # Backfill BA Codes based on BA IDs:
    df = df.reset_index().pipe(backfill_ba_codes_by_ba_id)
    # Typo: NEVP, BA ID is 13407, but in 2014-2015 in UT, entered as 13047
    df.loc[
        (df.balancing_authority_code_eia == "NEVP")
        & (df.balancing_authority_id_eia == 13047),
        "balancing_authority_id_eia",
    ] = 13407
    # Typo: Turlock Irrigation District is TIDC, not TID.
    df.loc[
        (df.balancing_authority_code_eia == "TID")
        & (df.balancing_authority_id_eia == 19281),
        "balancing_authority_code_eia",
    ] = "TIDC"

    return _post_process(df)


@asset(io_manager_key="pudl_sqlite_io_manager")
def sales_eia861(raw_sales_eia861: pd.DataFrame) -> pd.DataFrame:
    """Transform the EIA 861 Sales table.

    Transformations include:

    * Remove rows with utility ids 88888 and 99999.
    * Tidy data by customer class.
    * Drop primary key duplicates.
    * Convert 1000s of dollars into dollars.
    * Convert data_observed field I/O into boolean.
    * Map full spelling onto code values.
    """
    idx_cols = [
        "utility_id_eia",
        "state",
        "report_date",
        "balancing_authority_code_eia",
    ]

    # Pre-tidy clean specific to sales table
    raw_sales = _pre_process(raw_sales_eia861).query(
        "utility_id_eia not in (88888, 99999)"
    )

    ###########################################################################
    # Tidy Data:
    ###########################################################################
    logger.info("Tidying the EIA 861 Sales table.")
    tidy_sales, idx_cols = _tidy_class_dfs(
        raw_sales,
        df_name="Sales",
        idx_cols=idx_cols,
        class_list=CUSTOMER_CLASSES,
        class_type="customer_class",
    )

    # remove duplicates on the primary key columns + customer_class -- there
    # are lots of records that have reporting errors in the form of duplicates.
    # Many of them include different values and are therefore impossible to tell
    # which are "correct". The following function drops all but the first of
    # these duplicate entries.
    deduped_sales = _drop_dupes(df=tidy_sales, df_name="Sales", subset=idx_cols)

    ###########################################################################
    # Transform Values:
    # * Turn 1000s of dollars back into dollars
    # * Re-code data_observed to boolean:
    #   * O="observed" => True
    #   * I="imputed" => False
    # * Change the form code (A, B, C, D) into the business model that it
    #   corresponds to (retail vs. energy_services), which in combination with
    #   the service_type column (energy, delivery, bundled) will now serve as
    #   part of the primary key for the table.
    ###########################################################################
    logger.info("Performing value transformations on EIA 861 Sales table.")
    transformed_sales = deduped_sales.assign(
        sales_revenue=lambda x: _thousand_to_one(x.sales_revenue),
        data_observed=lambda x: x.data_observed.replace(
            {
                "O": True,
                "I": False,
            }
        ),
        business_model=lambda x: x.business_model.replace(
            {
                "A": "retail",
                "B": "retail",
                "C": "retail",
                "D": "energy_services",
            }
        ),
        service_type=lambda x: x.service_type.str.lower().replace(
            {"bundle": "bundled"}
        ),
        short_form=lambda x: _make_yn_bool(x.short_form),
    )

    return _post_process(transformed_sales)


@asset(io_manager_key="pudl_sqlite_io_manager")
def advanced_metering_infrastructure_eia861(
    raw_advanced_metering_infrastructure_eia861: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the EIA 861 Advanced Metering Infrastructure table.

    Transformations include:

    * Tidy data by customer class.
    * Drop total_meters columns (it's calculable with other fields).
    """
    idx_cols = [
        "utility_id_eia",
        "state",
        "balancing_authority_code_eia",
        "report_date",
    ]
    ###########################################################################
    # Tidy Data:
    ###########################################################################
    logger.info("Tidying the EIA 861 Advanced Metering Infrastructure table.")
    tidy_ami, idx_cols = _tidy_class_dfs(
        _pre_process(raw_advanced_metering_infrastructure_eia861),
        df_name="Advanced Metering Infrastructure",
        idx_cols=idx_cols,
        class_list=CUSTOMER_CLASSES,
        class_type="customer_class",
    )

    df = (
        tidy_ami.assign(short_form=lambda x: _make_yn_bool(x.short_form))
        # No duplicates to speak of but take measures to check just in case
        .pipe(
            _check_for_dupes,
            df_name="Advanced Metering Infrastructure",
            subset=idx_cols,
        )
        .drop(columns=["total_meters"])
        .pipe(_post_process)
    )

    return df


@multi_asset(
    outs={
        "demand_response_eia861": AssetOut(io_manager_key="pudl_sqlite_io_manager"),
        "demand_response_water_heater_eia861": AssetOut(
            io_manager_key="pudl_sqlite_io_manager"
        ),
    },
)
def demand_response_eia861(raw_demand_response_eia861: pd.DataFrame):
    """Transform the EIA 861 Demand Response table.

    Transformations include:

    * Fill in NA balancing authority codes with UNK (because it's part of the primary
      key).
    * Tidy subset of the data by customer class.
    * Drop duplicate rows based on primary keys.
    * Convert 1000s of dollars into dollars.
    """
    idx_cols = [
        "utility_id_eia",
        "state",
        "balancing_authority_code_eia",
        "report_date",
    ]

    raw_dr = _pre_process(raw_demand_response_eia861)
    # fill na BA values with 'UNK'
    raw_dr["balancing_authority_code_eia"] = raw_dr[
        "balancing_authority_code_eia"
    ].fillna("UNK")
    raw_dr["short_form"] = _make_yn_bool(raw_dr.short_form)

    # Split data into tidy-able and not
    raw_dr_water_heater = raw_dr[idx_cols + ["water_heater", "data_maturity"]].copy()
    dr_water = _drop_dupes(
        df=raw_dr_water_heater, df_name="Demand Response Water Heater", subset=idx_cols
    )
    raw_dr = raw_dr.drop(columns=["water_heater"])

    ###########################################################################
    # Tidy Data:
    ###########################################################################

    logger.info("Tidying the EIA 861 Demand Response table.")
    tidy_dr, idx_cols = _tidy_class_dfs(
        raw_dr,
        df_name="Demand Response",
        idx_cols=idx_cols,
        class_list=CUSTOMER_CLASSES,
        class_type="customer_class",
    )

    # shouldn't be duplicates but there are some strange values from IN.
    # these values have Nan BA values and should be deleted earlier.
    # thinking this might have to do with DR table weirdness between 2012 and 2013
    # will come back to this after working on the DSM table. Dropping dupes for now.
    deduped_dr = _drop_dupes(df=tidy_dr, df_name="Demand Response", subset=idx_cols)

    ###########################################################################
    # Transform Values:
    # * Turn 1000s of dollars back into dollars
    ###########################################################################
    logger.info("Performing value transformations on EIA 861 Demand Response table.")
    transformed_dr = deduped_dr.assign(
        customer_incentives_cost=lambda x: (
            _thousand_to_one(x.customer_incentives_cost)
        ),
        other_costs=lambda x: (_thousand_to_one(x.other_costs)),
    )

    return (
        Output(
            output_name="demand_response_eia861",
            value=_post_process(transformed_dr),
        ),
        Output(
            output_name="demand_response_water_heater_eia861",
            value=_post_process(dr_water),
        ),
    )


@multi_asset(
    outs={
        "demand_side_management_sales_eia861": AssetOut(
            io_manager_key="pudl_sqlite_io_manager"
        ),
        "demand_side_management_ee_dr_eia861": AssetOut(
            io_manager_key="pudl_sqlite_io_manager"
        ),
        "demand_side_management_misc_eia861": AssetOut(
            io_manager_key="pudl_sqlite_io_manager"
        ),
    },
)
def demand_side_management_eia861(
    raw_demand_side_management_eia861: pd.DataFrame,
):
    """Transform the EIA 861 Demand Side Management table.

    In 2013, the EIA changed the contents of the 861 form so that information pertaining
    to demand side management was no longer housed in a single table, but rather two
    seperate ones pertaining to energy efficiency and demand response. While the pre and
    post 2013 tables contain similar information, one column in the pre-2013 demand side
    management table may not have an obvious column equivalent in the post-2013 energy
    efficiency or demand response data. We've addressed this by keeping the demand side
    management and energy efficiency and demand response tables seperate. Use the DSM
    table for pre 2013 data and the EE / DR tables for post 2013 data. Despite the
    uncertainty of comparing across these years, the data are similar and we hope to
    provide a cohesive dataset in the future with all years and comprable columns
    combined.

    Transformations include:

    * Clean up NERC codes and ensure one per row.
    * Remove demand_side_management and data_observed columns (they are all the same).
    * Tidy subset of the data by customer class.
    * Convert Y/N columns to booleans.
    * Convert 1000s of dollars into dollars.
    """
    idx_cols = [
        "utility_id_eia",
        "state",
        "nerc_region",
        "report_date",
    ]

    sales_cols = [
        "sales_for_resale_mwh",
        "sales_to_ultimate_consumers_mwh",
        "data_maturity",
    ]

    bool_cols = [
        "energy_savings_estimates_independently_verified",
        "energy_savings_independently_verified",
        "major_program_changes",
        "price_responsive_programs",
        "short_form",
        "time_responsive_programs",
    ]

    cost_cols = [
        "annual_indirect_program_cost",
        "annual_total_cost",
        "energy_efficiency_annual_cost",
        "energy_efficiency_annual_incentive_payment",
        "load_management_annual_cost",
        "load_management_annual_incentive_payment",
    ]

    ###########################################################################
    # Transform Data Round 1 (must be done to avoid issues with nerc_region col in
    # _tidy_class_dfs())
    # * Clean NERC region col
    # * Drop data_status and demand_side_management cols (they don't contain anything)
    ###########################################################################
    transformed_dsm1 = (
        clean_nerc(_pre_process(raw_demand_side_management_eia861), idx_cols)
        .drop(columns=["demand_side_management", "data_status"])
        .query("utility_id_eia not in [88888]")
    )

    # Separate dsm data into sales vs. other table (the latter of which can be tidied)
    dsm_sales = transformed_dsm1[idx_cols + sales_cols].copy()
    dsm_ee_dr = transformed_dsm1.drop(
        columns=[x for x in sales_cols if x != "data_maturity"]
    )

    ###########################################################################
    # Tidy Data:
    ###########################################################################

    tidy_dsm, dsm_idx_cols = pudl.transform.eia861._tidy_class_dfs(
        dsm_ee_dr,
        df_name="Demand Side Management",
        idx_cols=idx_cols,
        class_list=CUSTOMER_CLASSES,
        class_type="customer_class",
        keep_totals=True,
    )

    ###########################################################################
    # Transform Data Round 2
    # * Make booleans (Y=True, N=False)
    # * Turn 1000s of dollars back into dollars
    ###########################################################################

    # Split tidy dsm data into transformable chunks
    tidy_dsm_bool = tidy_dsm[dsm_idx_cols + bool_cols].copy().set_index(dsm_idx_cols)
    tidy_dsm_cost = tidy_dsm[dsm_idx_cols + cost_cols].copy().set_index(dsm_idx_cols)
    tidy_dsm_ee_dr = tidy_dsm.drop(columns=bool_cols + cost_cols)

    # Calculate transformations for each chunk
    transformed_dsm2_bool = (
        _make_yn_bool(tidy_dsm_bool)
        .reset_index()
        .assign(short_form=lambda x: x.short_form.fillna(False))
    )
    transformed_dsm2_cost = _thousand_to_one(tidy_dsm_cost).reset_index()

    # Merge transformed chunks back together
    transformed_dsm2 = pd.merge(
        transformed_dsm2_bool, transformed_dsm2_cost, on=dsm_idx_cols, how="outer"
    )
    transformed_dsm2 = pd.merge(
        transformed_dsm2, tidy_dsm_ee_dr, on=dsm_idx_cols, how="outer"
    )

    # Split into final tables
    ee_cols = [col for col in transformed_dsm2 if "energy_efficiency" in col]
    dr_cols = [col for col in transformed_dsm2 if "load_management" in col]
    program_cols = ["price_responsiveness_customers", "time_responsiveness_customers"]
    total_cost_cols = ["annual_indirect_program_cost", "annual_total_cost"]

    dsm_ee_dr = transformed_dsm2[
        dsm_idx_cols
        + ee_cols
        + dr_cols
        + program_cols
        + total_cost_cols
        + ["data_maturity"]
    ].copy()
    dsm_misc = transformed_dsm2.drop(
        columns=ee_cols + dr_cols + program_cols + total_cost_cols + ["customer_class"]
    )
    dsm_misc = _drop_dupes(
        df=dsm_misc,
        df_name="Demand Side Management Misc.",
        subset=["utility_id_eia", "state", "nerc_region", "report_date"],
    )

    return (
        Output(
            output_name="demand_side_management_sales_eia861",
            value=_post_process(dsm_sales),
        ),
        Output(
            output_name="demand_side_management_ee_dr_eia861",
            value=_post_process(dsm_ee_dr),
        ),
        Output(
            output_name="demand_side_management_misc_eia861",
            value=_post_process(dsm_misc),
        ),
    )


@multi_asset(
    outs={
        "distributed_generation_tech_eia861": AssetOut(
            io_manager_key="pudl_sqlite_io_manager"
        ),
        "distributed_generation_fuel_eia861": AssetOut(
            io_manager_key="pudl_sqlite_io_manager"
        ),
        "distributed_generation_misc_eia861": AssetOut(
            io_manager_key="pudl_sqlite_io_manager"
        ),
    },
)
def distributed_generation_eia861(
    raw_distributed_generation_eia861: pd.DataFrame,
):
    """Transform the EIA 861 Distributed Generation table.

    Transformations include:

    * Map full spelling onto code values.
    * Convert pre-2010 percent values in mw values.
    * Remove total columns calculable with other fields.
    * Tidy subset of the data by tech class.
    * Tidy subset of the data by fuel class.
    """
    idx_cols = [
        "utility_id_eia",
        "state",
        "report_date",
    ]

    misc_cols = [
        "backup_capacity_mw",
        "backup_capacity_pct",
        "distributed_generation_owned_capacity_mw",
        "distributed_generation_owned_capacity_pct",
        "estimated_or_actual_capacity_data",
        "generators_number",
        "generators_num_less_1_mw",
        "total_capacity_mw",
        "total_capacity_less_1_mw",
        "utility_name_eia",
        "data_maturity",
    ]

    tech_cols = [
        "all_storage_capacity_mw",
        "combustion_turbine_capacity_mw",
        "combustion_turbine_capacity_pct",
        "estimated_or_actual_tech_data",
        "hydro_capacity_mw",
        "hydro_capacity_pct",
        "internal_combustion_capacity_mw",
        "internal_combustion_capacity_pct",
        "other_capacity_mw",
        "other_capacity_pct",
        "pv_capacity_mw",
        "steam_capacity_mw",
        "steam_capacity_pct",
        "total_capacity_mw",
        "wind_capacity_mw",
        "wind_capacity_pct",
        "data_maturity",
    ]

    fuel_cols = [
        "oil_fuel_pct",
        "estimated_or_actual_fuel_data",
        "gas_fuel_pct",
        "other_fuel_pct",
        "renewable_fuel_pct",
        "water_fuel_pct",
        "wind_fuel_pct",
        "wood_fuel_pct",
        "data_maturity",
    ]

    # Pre-tidy transform: set estimated or actual A/E values to 'Acutal'/'Estimated'
    raw_dg = _pre_process(raw_distributed_generation_eia861).assign(
        estimated_or_actual_capacity_data=lambda x: (
            x.estimated_or_actual_capacity_data.map(ESTIMATED_OR_ACTUAL)
        ),
        estimated_or_actual_fuel_data=lambda x: (
            x.estimated_or_actual_fuel_data.map(ESTIMATED_OR_ACTUAL)
        ),
        estimated_or_actual_tech_data=lambda x: (
            x.estimated_or_actual_tech_data.map(ESTIMATED_OR_ACTUAL)
        ),
    )

    # Split into three tables: Capacity/tech-related, fuel-related, and misc.
    raw_dg_tech = raw_dg[idx_cols + tech_cols].copy()
    raw_dg_fuel = raw_dg[idx_cols + fuel_cols].copy()
    raw_dg_misc = raw_dg[idx_cols + misc_cols].copy()

    ###########################################################################
    # Transform Values:
    # * Turn pct values into mw values
    # * Remove old pct cols and totals cols
    # Explanation: Pre 2010 reporting asks for components as a percent of total capacity
    # whereas after 2010, the forms ask for the component portion as a mw value. In
    # order to coalesce similar data, we've used total values to turn percent values
    # from pre 2010 into mw values like those post-2010.
    ###########################################################################

    # Separate datasets into years with only pct values (pre-2010) and years with only mw values (post-2010)
    dg_tech_early = raw_dg_tech[raw_dg_tech["report_date"] < "2010-01-01"]
    dg_tech_late = raw_dg_tech[raw_dg_tech["report_date"] >= "2010-01-01"]
    dg_misc_early = raw_dg_misc[raw_dg_misc["report_date"] < "2010-01-01"]
    dg_misc_late = raw_dg_misc[raw_dg_misc["report_date"] >= "2010-01-01"]

    logger.info("Converting pct to MW for distributed generation misc table")
    dg_misc_early = dg_misc_early.assign(
        distributed_generation_owned_capacity_mw=lambda x: _pct_to_mw(
            x, "distributed_generation_owned_capacity_pct"
        ),
        backup_capacity_mw=lambda x: _pct_to_mw(x, "backup_capacity_pct"),
    )
    dg_misc = pd.concat([dg_misc_early, dg_misc_late])
    dg_misc = dg_misc.drop(
        columns=[
            "distributed_generation_owned_capacity_pct",
            "backup_capacity_pct",
            "total_capacity_mw",
        ],
    )

    logger.info("Converting pct into MW for distributed generation tech table")
    dg_tech_early = dg_tech_early.assign(
        combustion_turbine_capacity_mw=lambda x: (
            _pct_to_mw(x, "combustion_turbine_capacity_pct")
        ),
        hydro_capacity_mw=lambda x: _pct_to_mw(x, "hydro_capacity_pct"),
        internal_combustion_capacity_mw=lambda x: (
            _pct_to_mw(x, "internal_combustion_capacity_pct")
        ),
        other_capacity_mw=lambda x: _pct_to_mw(x, "other_capacity_pct"),
        steam_capacity_mw=lambda x: _pct_to_mw(x, "steam_capacity_pct"),
        wind_capacity_mw=lambda x: _pct_to_mw(x, "wind_capacity_pct"),
    )
    dg_tech = pd.concat([dg_tech_early, dg_tech_late])
    dg_tech = dg_tech.drop(
        columns=[
            "combustion_turbine_capacity_pct",
            "hydro_capacity_pct",
            "internal_combustion_capacity_pct",
            "other_capacity_pct",
            "steam_capacity_pct",
            "wind_capacity_pct",
            "total_capacity_mw",
        ],
    )

    ###########################################################################
    # Tidy Data
    ###########################################################################
    logger.info("Tidying Distributed Generation Tech Table")
    tidy_dg_tech, tech_idx_cols = _tidy_class_dfs(
        df=dg_tech,
        df_name="Distributed Generation Tech Component Capacity",
        idx_cols=idx_cols,
        class_list=TECH_CLASSES,
        class_type="tech_class",
    )

    logger.info("Tidying Distributed Generation Fuel Table")
    tidy_dg_fuel, fuel_idx_cols = _tidy_class_dfs(
        df=raw_dg_fuel,
        df_name="Distributed Generation Fuel Percent",
        idx_cols=idx_cols,
        class_list=FUEL_CLASSES,
        class_type="fuel_class",
    )

    return (
        Output(
            output_name="distributed_generation_tech_eia861",
            value=_post_process(tidy_dg_tech),
        ),
        Output(
            output_name="distributed_generation_fuel_eia861",
            value=_post_process(tidy_dg_fuel),
        ),
        Output(
            output_name="distributed_generation_misc_eia861",
            value=_post_process(dg_misc),
        ),
    )


@asset(io_manager_key="pudl_sqlite_io_manager")
def distribution_systems_eia861(
    raw_distribution_systems_eia861: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the EIA 861 Distribution Systems table.

    * No additional transformations.
    """
    df = (
        _pre_process(raw_distribution_systems_eia861)
        .assign(short_form=lambda x: _make_yn_bool(x.short_form))
        # No duplicates to speak of but take measures to check just in case
        .pipe(
            _check_for_dupes,
            df_name="Distribution Systems",
            subset=["utility_id_eia", "state", "report_date"],
        )
        .pipe(_post_process)
    )
    return df


@asset(io_manager_key="pudl_sqlite_io_manager")
def dynamic_pricing_eia861(raw_dynamic_pricing_eia861: pd.DataFrame) -> pd.DataFrame:
    """Transform the EIA 861 Dynamic Pricing table.

    Transformations include:

    * Tidy subset of the data by customer class.
    * Convert Y/N columns to booleans.
    """
    idx_cols = [
        "utility_id_eia",
        "state",
        "balancing_authority_code_eia",
        "report_date",
    ]

    class_attributes = [
        "critical_peak_pricing",
        "critical_peak_rebate",
        "real_time_pricing",
        "time_of_use_pricing",
        "variable_peak_pricing",
    ]

    raw_dp = _pre_process(
        raw_dynamic_pricing_eia861.query("utility_id_eia not in [88888]").assign(
            short_form=lambda x: _make_yn_bool(x.short_form)
        )
    )

    ###########################################################################
    # Tidy Data:
    ###########################################################################

    logger.info("Tidying the EIA 861 Dynamic Pricing table.")
    tidy_dp, idx_cols = _tidy_class_dfs(
        raw_dp,
        df_name="Dynamic Pricing",
        idx_cols=idx_cols,
        class_list=CUSTOMER_CLASSES,
        class_type="customer_class",
    )

    # No duplicates to speak of but take measures to check just in case
    _check_for_dupes(tidy_dp, "Dynamic Pricing", idx_cols)

    ###########################################################################
    # Transform Values:
    # * Make Y/N's into booleans and X values into pd.NA
    ###########################################################################

    logger.info("Performing value transformations on EIA 861 Dynamic Pricing table.")
    for col in class_attributes:
        tidy_dp[col] = (
            tidy_dp[col]
            .replace({"Y": True, "N": False})
            .apply(lambda x: x if x in [True, False] else pd.NA)
        )

    return _post_process(tidy_dp)


@asset(io_manager_key="pudl_sqlite_io_manager")
def energy_efficiency_eia861(
    raw_energy_efficiency_eia861: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the EIA 861 Energy Efficiency table.

    Transformations include:

    * Tidy subset of the data by customer class.
    * Drop website column (almost no valid information).
    * Convert 1000s of dollars into dollars.
    """
    idx_cols = [
        "utility_id_eia",
        "state",
        "balancing_authority_code_eia",
        "report_date",
    ]

    raw_ee = (
        _pre_process(raw_energy_efficiency_eia861).assign(
            short_form=lambda x: _make_yn_bool(x.short_form)
        )
        # No duplicates to speak of but take measures to check just in case
        .pipe(_check_for_dupes, df_name="Energy Efficiency", subset=idx_cols)
    )

    ###########################################################################
    # Tidy Data:
    ###########################################################################
    logger.info("Tidying the EIA 861 Energy Efficiency table.")

    # wide-to-tall by customer class (must be done before wide-to-tall by fuel class)
    tidy_ee, _ = _tidy_class_dfs(
        raw_ee,
        df_name="Energy Efficiency",
        idx_cols=idx_cols,
        class_list=CUSTOMER_CLASSES,
        class_type="customer_class",
        keep_totals=True,
    )

    ###########################################################################
    # Transform Values:
    # * Turn 1000s of dollars back into dollars
    # * Get rid of website column
    ###########################################################################

    logger.info("Transforming the EIA 861 Energy Efficiency table.")

    transformed_ee = tidy_ee.assign(
        customer_incentives_incremental_cost=lambda x: (
            _thousand_to_one(x.customer_incentives_incremental_cost)
        ),
        customer_incentives_incremental_life_cycle_cost=lambda x: (
            _thousand_to_one(x.customer_incentives_incremental_life_cycle_cost)
        ),
        customer_other_costs_incremental_life_cycle_cost=lambda x: (
            _thousand_to_one(x.customer_other_costs_incremental_life_cycle_cost)
        ),
        other_costs_incremental_cost=lambda x: (
            _thousand_to_one(x.other_costs_incremental_cost)
        ),
    ).drop(columns=["website"])

    return _post_process(transformed_ee)


@asset(io_manager_key="pudl_sqlite_io_manager")
def green_pricing_eia861(
    raw_green_pricing_eia861: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the EIA 861 Green Pricing table.

    Transformations include:

    * Tidy subset of the data by customer class.
    * Convert 1000s of dollars into dollars.
    """
    idx_cols = [
        "utility_id_eia",
        "state",
        "report_date",
    ]

    ###########################################################################
    # Tidy Data:
    ###########################################################################
    logger.info("Tidying the EIA 861 Green Pricing table.")
    tidy_gp, idx_cols = _tidy_class_dfs(
        _pre_process(raw_green_pricing_eia861),
        df_name="Green Pricing",
        idx_cols=idx_cols,
        class_list=CUSTOMER_CLASSES,
        class_type="customer_class",
    )

    _check_for_dupes(tidy_gp, "Green Pricing", idx_cols)

    ###########################################################################
    # Transform Values:
    # * Turn 1000s of dollars back into dollars
    ###########################################################################
    logger.info("Performing value transformations on EIA 861 Green Pricing table.")
    transformed_gp = tidy_gp.assign(
        green_pricing_revenue=lambda x: (_thousand_to_one(x.green_pricing_revenue)),
        rec_revenue=lambda x: (_thousand_to_one(x.rec_revenue)),
    )

    return _post_process(transformed_gp)


@asset(io_manager_key="pudl_sqlite_io_manager")
def mergers_eia861(raw_mergers_eia861: pd.DataFrame) -> pd.DataFrame:
    """Transform the EIA 861 Mergers table."""
    # No duplicates to speak of but take measures to check just in case
    df = (
        _pre_process(raw_mergers_eia861)
        .pipe(
            _check_for_dupes,
            df_name="Mergers",
            subset=["utility_id_eia", "state", "report_date"],
        )
        .pipe(_post_process)
    )
    return df


@multi_asset(
    outs={
        "net_metering_customer_fuel_class_eia861": AssetOut(
            io_manager_key="pudl_sqlite_io_manager"
        ),
        "net_metering_misc_eia861": AssetOut(io_manager_key="pudl_sqlite_io_manager"),
    },
)
def net_metering_eia861(raw_net_metering_eia861: pd.DataFrame):
    """Transform the EIA 861 Net Metering table.

    Transformations include:

    * Remove rows with utility ids 99999.
    * Tidy subset of the data by customer class.
    * Tidy subset of the data by tech class.
    """
    idx_cols = [
        "utility_id_eia",
        "state",
        "balancing_authority_code_eia",
        "report_date",
    ]

    misc_cols = ["pv_current_flow_type"]

    # Pre-tidy clean specific to net_metering table
    raw_nm = (
        _pre_process(raw_net_metering_eia861)
        .query("utility_id_eia not in [99999]")
        .assign(short_form=lambda x: _make_yn_bool(x.short_form))
    )

    # Separate customer class data from misc data (in this case just one col: current flow)
    # Could easily add this to tech_class if desired.
    raw_nm_customer_fuel_class = raw_nm.drop(columns=misc_cols).copy()
    raw_nm_misc = raw_nm[idx_cols + misc_cols + ["data_maturity"]].copy()

    # Check for duplicates before idx cols get changed
    _ = _check_for_dupes(raw_nm_misc, "Net Metering Current Flow Type PV", idx_cols)

    ###########################################################################
    # Tidy Data:
    ###########################################################################

    logger.info("Tidying the EIA 861 Net Metering table.")
    # wide-to-tall by customer class (must be done before wide-to-tall by fuel class)
    tidy_nm_customer_class, idx_cols = _tidy_class_dfs(
        raw_nm_customer_fuel_class,
        df_name="Net Metering",
        idx_cols=idx_cols,
        class_list=CUSTOMER_CLASSES,
        class_type="customer_class",
    )

    # wide-to-tall by fuel class
    tidy_nm_customer_fuel_class, idx_cols = _tidy_class_dfs(
        tidy_nm_customer_class,
        df_name="Net Metering",
        idx_cols=idx_cols,
        class_list=TECH_CLASSES,
        class_type="tech_class",
        keep_totals=True,
    )

    # No duplicates to speak of but take measures to check just in case
    _ = _check_for_dupes(
        tidy_nm_customer_fuel_class,
        df_name="Net Metering Customer & Fuel Class",
        subset=idx_cols,
    )

    return (
        Output(
            output_name="net_metering_customer_fuel_class_eia861",
            value=_post_process(tidy_nm_customer_fuel_class),
        ),
        Output(
            output_name="net_metering_misc_eia861",
            value=_post_process(raw_nm_misc),
        ),
    )


@multi_asset(
    outs={
        "non_net_metering_customer_fuel_class_eia861": AssetOut(
            io_manager_key="pudl_sqlite_io_manager"
        ),
        "non_net_metering_misc_eia861": AssetOut(
            io_manager_key="pudl_sqlite_io_manager"
        ),
    },
)
def non_net_metering_eia861(raw_non_net_metering_eia861: pd.DataFrame):
    """Transform the EIA 861 Non-Net Metering table.

    Transformations include:

    * Remove rows with utility ids 99999.
    * Drop duplicate rows.
    * Tidy subset of the data by customer class.
    * Tidy subset of the data by tech class.
    """
    idx_cols = [
        "utility_id_eia",
        "state",
        "balancing_authority_code_eia",
        "report_date",
    ]

    misc_cols = [
        "backup_capacity_mw",
        "generators_number",
        "pv_current_flow_type",
        "utility_owned_capacity_mw",
    ]

    # Pre-tidy clean specific to non_net_metering table
    raw_nnm = _pre_process(raw_non_net_metering_eia861).query(
        "utility_id_eia not in '99999'"
    )

    # there are ~80 fully duplicate records in the 2018 table. We need to
    # remove those duplicates
    og_len = len(raw_nnm)
    raw_nnm = raw_nnm.drop_duplicates(keep="first")
    diff_len = og_len - len(raw_nnm)
    if diff_len > 100:
        raise ValueError(
            f"""Too many duplicate dropped records in raw non-net metering
    table: {diff_len}"""
        )

    # Separate customer class data from misc data
    raw_nnm_customer_fuel_class = raw_nnm.drop(columns=misc_cols).copy()
    raw_nnm_misc = (raw_nnm[idx_cols + misc_cols + ["data_maturity"]]).copy()

    # Check for duplicates before idx cols get changed
    _ = _check_for_dupes(
        raw_nnm_misc, df_name="Non Net Metering Misc.", subset=idx_cols
    )

    ###########################################################################
    # Tidy Data:
    ###########################################################################

    logger.info("Tidying the EIA 861 Non Net Metering table.")

    # wide-to-tall by customer class (must be done before wide-to-tall by fuel class)
    tidy_nnm_customer_class, idx_cols = _tidy_class_dfs(
        raw_nnm_customer_fuel_class,
        df_name="Non Net Metering",
        idx_cols=idx_cols,
        class_list=CUSTOMER_CLASSES,
        class_type="customer_class",
        keep_totals=True,
    )

    # wide-to-tall by fuel class
    tidy_nnm_customer_fuel_class, idx_cols = _tidy_class_dfs(
        tidy_nnm_customer_class,
        df_name="Non Net Metering",
        idx_cols=idx_cols,
        class_list=TECH_CLASSES,
        class_type="tech_class",
        keep_totals=True,
    )

    # No duplicates to speak of (deleted 2018 duplicates above) but take measures to
    # check just in case
    _ = _check_for_dupes(
        tidy_nnm_customer_fuel_class,
        df_name="Non Net Metering Customer & Fuel Class",
        subset=idx_cols,
    )

    # Delete total_capacity_mw col for redundancy (must delete x not y)
    tidy_nnm_customer_fuel_class = tidy_nnm_customer_fuel_class.drop(
        columns="capacity_mw_x"
    ).rename(columns={"capacity_mw_y": "capacity_mw"})

    return (
        Output(
            output_name="non_net_metering_customer_fuel_class_eia861",
            value=_post_process(tidy_nnm_customer_fuel_class),
        ),
        Output(
            output_name="non_net_metering_misc_eia861",
            value=_post_process(raw_nnm_misc),
        ),
    )


@multi_asset(
    outs={
        "operational_data_revenue_eia861": AssetOut(
            io_manager_key="pudl_sqlite_io_manager"
        ),
        "operational_data_misc_eia861": AssetOut(
            io_manager_key="pudl_sqlite_io_manager"
        ),
    },
)
def operational_data_eia861(raw_operational_data_eia861: pd.DataFrame):
    """Transform the EIA 861 Operational Data table.

    Transformations include:

    * Remove rows with utility ids 88888.
    * Remove rows with NA utility id.
    * Clean up NERC codes and ensure one per row.
    * Convert data_observed field I/O into boolean.
    * Tidy subset of the data by revenue class.
    * Convert 1000s of dollars into dollars.
    """
    idx_cols = [
        "utility_id_eia",
        "state",
        "nerc_region",
        "report_date",
    ]

    # Pre-tidy clean specific to operational data table
    raw_od = _pre_process(raw_operational_data_eia861)
    raw_od = raw_od[
        (raw_od["utility_id_eia"] != 88888) & (raw_od["utility_id_eia"].notnull())
    ]

    ###########################################################################
    # Transform Data Round 1:
    # * Clean up reported NERC regions:
    #    * Fix puncuation/case
    #    * Replace na with 'UNK'
    #    * Make sure NERC regions are a verified NERC region
    #    * Add underscore between double entires (SPP_ERCOT)
    # * Re-code data_observed to boolean:
    #   * O="observed" => True
    #   * I="imputed" => False
    ###########################################################################

    transformed_od = clean_nerc(raw_od, idx_cols).assign(
        data_observed=lambda x: x.data_observed.replace({"O": True, "I": False}),
        short_form=lambda x: _make_yn_bool(x.short_form),
    )

    # Split data into 2 tables:
    #  * Revenue (wide-to-tall)
    #  * Misc. (other)
    revenue_cols = [col for col in transformed_od if "revenue" in col]
    transformed_od_misc = transformed_od.drop(columns=revenue_cols)
    transformed_od_rev = transformed_od[
        idx_cols + revenue_cols + ["data_maturity"]
    ].copy()

    # Wide-to-tall revenue columns
    tidy_od_rev, idx_cols = _tidy_class_dfs(
        transformed_od_rev,
        df_name="Operational Data Revenue",
        idx_cols=idx_cols,
        class_list=REVENUE_CLASSES,
        class_type="revenue_class",
    )

    ###########################################################################
    # Transform Data Round 2:
    # * Turn 1000s of dollars back into dollars
    ###########################################################################

    # Transform revenue 1000s into dollars
    transformed_od_rev = tidy_od_rev.assign(
        revenue=lambda x: (_thousand_to_one(x.revenue))
    )

    return (
        Output(
            output_name="operational_data_revenue_eia861",
            value=_post_process(transformed_od_rev),
        ),
        Output(
            output_name="operational_data_misc_eia861",
            value=_post_process(transformed_od_misc),
        ),
    )


@asset(io_manager_key="pudl_sqlite_io_manager")
def reliability_eia861(raw_reliability_eia861: pd.DataFrame) -> pd.DataFrame:
    """Transform the EIA 861 Reliability table.

    Transformations include:

    * Tidy subset of the data by reliability standard.
    * Convert Y/N columns to booleans.
    * Map full spelling onto code values.
    * Drop duplicate rows.
    """
    idx_cols = ["utility_id_eia", "state", "report_date"]

    ###########################################################################
    # Tidy Data:
    ###########################################################################

    logger.info("Tidying the EIA 861 Reliability table.")

    # wide-to-tall by standards
    tidy_r, idx_cols = _tidy_class_dfs(
        df=_pre_process(raw_reliability_eia861),
        df_name="Reliability",
        idx_cols=idx_cols,
        class_list=RELIABILITY_STANDARDS,
        class_type="standard",
        keep_totals=False,
    )

    ###########################################################################
    # Transform Data:
    # * Re-code outages_recorded_automatically and inactive_accounts_included to
    # boolean:
    #   * Y/y="Yes" => True
    #   * N/n="No" => False
    # * Expand momentary_interruption_definition:
    #   * 'L' => 'Less than one minute'
    #   * 'F' => 'Less than or equal to five minutes'
    #   * 'O' => 'Other'
    ###########################################################################

    transformed_r = (
        tidy_r.assign(
            outages_recorded_automatically=lambda x: (
                _make_yn_bool(x.outages_recorded_automatically)
            ),
            inactive_accounts_included=lambda x: (
                _make_yn_bool(x.inactive_accounts_included)
            ),
            short_form=lambda x: _make_yn_bool(x.short_form),
        )
        # Drop duplicate entries for utilities 13027, 3408 and 9697
        .pipe(_drop_dupes, df_name="Reliability", subset=idx_cols).pipe(_post_process)
    )

    return transformed_r


@multi_asset(
    outs={
        "utility_data_nerc_eia861": AssetOut(io_manager_key="pudl_sqlite_io_manager"),
        "utility_data_rto_eia861": AssetOut(io_manager_key="pudl_sqlite_io_manager"),
        "utility_data_misc_eia861": AssetOut(io_manager_key="pudl_sqlite_io_manager"),
    },
)
def utility_data_eia861(raw_utility_data_eia861: pd.DataFrame):
    """Transform the EIA 861 Utility Data table.

    Transformations include:

    * Remove rows with utility ids 88888.
    * Clean up NERC codes and ensure one per row.
    * Tidy subset of the data by NERC region.
    * Tidy subset of the data by RTO.
    * Convert Y/N columns to booleans.
    """
    idx_cols = ["utility_id_eia", "state", "report_date", "nerc_region"]

    # Pre-tidy clean specific to operational data table
    raw_ud = _pre_process(raw_utility_data_eia861).query(
        "utility_id_eia not in [88888]"
    )

    ##############################################################################
    # Transform Data Round 1 (must be done to avoid issues with nerc_region col in
    # _tidy_class_dfs())
    # * Clean NERC region col
    ##############################################################################

    transformed_ud = clean_nerc(raw_ud, idx_cols).assign(
        short_form=lambda x: _make_yn_bool(x.short_form)
    )

    # Establish columns that are nerc regions vs. rtos
    nerc_cols = [col for col in raw_ud if "nerc_region_operation" in col] + [
        "data_maturity"
    ]
    logger.info(f"{nerc_cols=}")
    rto_cols = [col for col in raw_ud if "rto_operation" in col] + ["data_maturity"]
    logger.info(f"{rto_cols=}")
    misc_cols = [
        col
        for col in raw_ud
        if "nerc_region_operation" not in col and "rto_operation" not in col
    ]
    logger.info(f"{misc_cols=}")
    # Make separate tables for nerc vs. rto vs. misc data
    raw_ud_nerc = transformed_ud[idx_cols + nerc_cols].copy()
    raw_ud_rto = transformed_ud[idx_cols + rto_cols].copy()
    raw_ud_misc = transformed_ud[misc_cols].copy()

    ###########################################################################
    # Tidy Data:
    ###########################################################################

    logger.info("Tidying the EIA 861 Utility Data tables.")

    tidy_ud_nerc, _ = _tidy_class_dfs(
        df=raw_ud_nerc,
        df_name="Utility Data NERC Regions",
        idx_cols=idx_cols,
        class_list=[x.lower() for x in NERC_REGIONS],
        class_type="nerc_regions_of_operation",
    )

    tidy_ud_rto, _ = _tidy_class_dfs(
        df=raw_ud_rto,
        df_name="Utility Data RTOs",
        idx_cols=idx_cols,
        class_list=RTO_CLASSES,
        class_type="rtos_of_operation",
    )

    ###########################################################################
    # Transform Data Round 2:
    # * Re-code operating_in_XX to boolean:
    #   * Y = "Yes" => True
    #   * N = "No" => False
    #   * Blank => False
    # * Make nerc_regions uppercase
    ###########################################################################

    # Transform NERC region table
    transformed_ud_nerc = tidy_ud_nerc.assign(
        nerc_region_operation=lambda x: (
            _make_yn_bool(x.nerc_region_operation.fillna(False))
        ),
        nerc_regions_of_operation=lambda x: (x.nerc_regions_of_operation.str.upper()),
    )

    # Only keep true values and drop bool col
    transformed_ud_nerc = transformed_ud_nerc[
        transformed_ud_nerc.nerc_region_operation
    ].drop(columns=["nerc_region_operation"])

    # Transform RTO table
    transformed_ud_rto = tidy_ud_rto.assign(
        rto_operation=lambda x: (
            x.rto_operation.fillna(False).replace({"N": False, "Y": True})
        ),
    )

    # Only keep true values and drop bool col
    transformed_ud_rto = transformed_ud_rto[transformed_ud_rto.rto_operation].drop(
        columns=["rto_operation"]
    )

    # Transform MISC table by first separating bool cols from non bool cols
    # and then making them into boolean values.
    transformed_ud_misc_bool = (
        raw_ud_misc.drop(columns=["entity_type", "utility_name_eia", "data_maturity"])
        .set_index(idx_cols)
        .fillna(False)
        .replace({"N": False, "Y": True})
    )

    # Merge misc. bool cols back together with misc. non bool cols
    transformed_ud_misc = pd.merge(
        raw_ud_misc[idx_cols + ["entity_type", "utility_name_eia", "data_maturity"]],
        transformed_ud_misc_bool,
        on=idx_cols,
        how="outer",
    )

    return (
        Output(
            output_name="utility_data_nerc_eia861",
            value=_post_process(transformed_ud_nerc),
        ),
        Output(
            output_name="utility_data_rto_eia861",
            value=_post_process(transformed_ud_rto),
        ),
        Output(
            output_name="utility_data_misc_eia861",
            value=_post_process(transformed_ud_misc),
        ),
    )


##############################################################################
# Secondary Transformations
##############################################################################
@asset(
    ins={
        "advanced_metering_infrastructure_eia861": AssetIn(),
        "demand_response_eia861": AssetIn(),
        "demand_response_water_heater_eia861": AssetIn(),
        "demand_side_management_ee_dr_eia861": AssetIn(),
        "demand_side_management_misc_eia861": AssetIn(),
        "demand_side_management_sales_eia861": AssetIn(),
        "distributed_generation_fuel_eia861": AssetIn(),
        "distributed_generation_misc_eia861": AssetIn(),
        "distributed_generation_tech_eia861": AssetIn(),
        "distribution_systems_eia861": AssetIn(),
        "dynamic_pricing_eia861": AssetIn(),
        "energy_efficiency_eia861": AssetIn(),
        "green_pricing_eia861": AssetIn(),
        "mergers_eia861": AssetIn(),
        "net_metering_customer_fuel_class_eia861": AssetIn(),
        "net_metering_misc_eia861": AssetIn(),
        "non_net_metering_customer_fuel_class_eia861": AssetIn(),
        "non_net_metering_misc_eia861": AssetIn(),
        "operational_data_misc_eia861": AssetIn(),
        "operational_data_revenue_eia861": AssetIn(),
        "reliability_eia861": AssetIn(),
        "sales_eia861": AssetIn(),
        "utility_data_misc_eia861": AssetIn(),
        "utility_data_nerc_eia861": AssetIn(),
        "utility_data_rto_eia861": AssetIn(),
    },
    io_manager_key="pudl_sqlite_io_manager",
)
def utility_assn_eia861(**data_dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Harvest a Utility-Date-State Association Table."""
    logger.info("Building an EIA 861 Util-State-Date association table.")
    df = _harvest_associations(
        dfs=list(data_dfs.values()), cols=["report_date", "utility_id_eia", "state"]
    ).pipe(_post_process)
    return df


@asset(
    ins={
        "advanced_metering_infrastructure_eia861": AssetIn(),
        "clean_balancing_authority_eia861": AssetIn(),
        "demand_response_eia861": AssetIn(),
        "demand_response_water_heater_eia861": AssetIn(),
        "demand_side_management_ee_dr_eia861": AssetIn(),
        "demand_side_management_misc_eia861": AssetIn(),
        "demand_side_management_sales_eia861": AssetIn(),
        "distributed_generation_fuel_eia861": AssetIn(),
        "distributed_generation_misc_eia861": AssetIn(),
        "distributed_generation_tech_eia861": AssetIn(),
        "distribution_systems_eia861": AssetIn(),
        "dynamic_pricing_eia861": AssetIn(),
        "energy_efficiency_eia861": AssetIn(),
        "green_pricing_eia861": AssetIn(),
        "mergers_eia861": AssetIn(),
        "net_metering_customer_fuel_class_eia861": AssetIn(),
        "net_metering_misc_eia861": AssetIn(),
        "non_net_metering_customer_fuel_class_eia861": AssetIn(),
        "non_net_metering_misc_eia861": AssetIn(),
        "operational_data_misc_eia861": AssetIn(),
        "operational_data_revenue_eia861": AssetIn(),
        "reliability_eia861": AssetIn(),
        "sales_eia861": AssetIn(),
        "utility_data_misc_eia861": AssetIn(),
        "utility_data_nerc_eia861": AssetIn(),
        "utility_data_rto_eia861": AssetIn(),
    },
    io_manager_key="pudl_sqlite_io_manager",
)
def balancing_authority_assn_eia861(**dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Compile a balancing authority, utility, state association table.

    For the years up through 2012, the only BA-Util information that's available comes
    from the balancing_authority_eia861 table, and it does not include any state-level
    information. However, there is utility-state association information in the
    sales_eia861 and other data tables.

    For the years from 2013 onward, there's explicit BA-Util-State information in the
    data tables (e.g. :ref:`sales_eia861`). These observed associations can be compiled
    to give us a picture of which BA-Util-State associations exist. However, we need to
    merge in the balancing authority IDs since the data tables only contain the
    balancing authority codes.

    Args:
        dfs: A dictionary of transformed EIA 861 dataframes. This must include any
            dataframes from which we want to compile BA-Util-State associations, which
            means this function has to be called after all the basic transform functions
            that depend on only a single raw table.

    Returns:
        An association table describing the annual linkages between balancing
        authorities, states, and utilities. Becomes
        :ref:`balancing_authority_assn_eia861`.
    """
    # The dataframes from which to compile BA-Util-State associations
    ba_eia861 = dfs.pop("clean_balancing_authority_eia861")
    data_dfs = list(dfs.values())

    logger.info("Building an EIA 861 BA-Util-State association table.")

    # Helpful shorthand query strings....
    early_years = "report_date<='2012-12-31'"
    late_years = "report_date>='2013-01-01'"
    early_dfs = [df.query(early_years) for df in data_dfs]
    late_dfs = [df.query(late_years) for df in data_dfs]

    # The old BA table lists utilities directly, but has no state information.
    early_date_ba_util = _harvest_associations(
        dfs=[
            ba_eia861.query(early_years),
        ],
        cols=["report_date", "balancing_authority_id_eia", "utility_id_eia"],
    )
    # State-utility associations are brought in from observations in data_dfs
    early_date_util_state = _harvest_associations(
        dfs=early_dfs,
        cols=["report_date", "utility_id_eia", "state"],
    )
    early_date_ba_util_state = early_date_ba_util.merge(
        early_date_util_state, how="outer"
    ).drop_duplicates()

    # New BA table has no utility information, but has BA Codes...
    late_ba_code_id = _harvest_associations(
        dfs=[
            ba_eia861.query(late_years),
        ],
        cols=[
            "report_date",
            "balancing_authority_code_eia",
            "balancing_authority_id_eia",
        ],
    )
    # BA Code allows us to bring in utility+state data from data_dfs:
    late_date_ba_code_util_state = _harvest_associations(
        dfs=late_dfs,
        cols=["report_date", "balancing_authority_code_eia", "utility_id_eia", "state"],
    )
    # We merge on ba_code then drop it, b/c only BA ID exists in all years consistently:
    late_date_ba_util_state = (
        late_date_ba_code_util_state.merge(late_ba_code_id, how="outer")
        .drop(columns="balancing_authority_code_eia")
        .drop_duplicates()
    )

    ba_assn_eia861 = (
        pd.concat([early_date_ba_util_state, late_date_ba_util_state])
        # If there's no BA ID, the record is not useful:
        .dropna(subset=["balancing_authority_id_eia"]).pipe(
            apply_pudl_dtypes, group="eia"
        )
    )
    ba_assn_eia861 = ba_assn_eia861[
        # Without both Utility ID and State, there's no association information:
        (ba_assn_eia861.state.notna() | ba_assn_eia861.utility_id_eia.notna())
        # 99999 & 88888 are special placeholder IDs.
        & (~ba_assn_eia861.balancing_authority_id_eia.isin([99999, 88888]))
        & (~ba_assn_eia861.utility_id_eia.isin([88888, 99999]))
    ]
    ba_assn_eia861["state"] = ba_assn_eia861.state.fillna("UNK")
    return ba_assn_eia861


@asset(io_manager_key="pudl_sqlite_io_manager")
def balancing_authority_eia861(
    clean_balancing_authority_eia861: pd.DataFrame,
) -> pd.DataFrame:
    """Finish the normalization of the balancing_authority_eia861 table.

    The :ref:`balancing_authority_assn_eia861` table depends on information that is only
    available in the UN-normalized form of the :ref:`balancing_authority_eia861` table,
    and also on having access to a bunch of transformed data tables, so it can compile
    the observed combinations of report dates, balancing authorities, states, and
    utilities.  This means that we have to hold off on the final normalization of the
    :ref:`balancing_authority_eia861` table until the rest of the transform process is
    over.

    Args:
        clean_balancing_authority_eia861: A cleaned up version of the originally
            reported balancing authority table.

    Returns:
        The final, normalized version of the :ref:`balancing_authority_eia861` table,
        linking together balancing authorities and utility IDs by year, but without
        information about what states they were operating in (which is captured in
        :ref:`balancing_authority_assn_eia861`).
    """
    logger.info("Completing normalization of balancing_authority_eia861.")
    ba_eia861_normed = clean_balancing_authority_eia861.loc[
        :,
        [
            "report_date",
            "balancing_authority_id_eia",
            "balancing_authority_code_eia",
            "balancing_authority_name_eia",
        ],
    ].drop_duplicates(subset=["report_date", "balancing_authority_id_eia"])

    # Make sure that there aren't any more BA IDs we can recover from later years:
    ba_ids_missing_codes = (
        ba_eia861_normed.loc[
            ba_eia861_normed.balancing_authority_code_eia.isnull(),
            "balancing_authority_id_eia",
        ]
        .drop_duplicates()
        .dropna()
    )
    fillable_ba_codes = ba_eia861_normed[
        (ba_eia861_normed.balancing_authority_id_eia.isin(ba_ids_missing_codes))
        & (ba_eia861_normed.balancing_authority_code_eia.notnull())
    ]
    if len(fillable_ba_codes) != 0:
        raise ValueError(
            f"Found {len(fillable_ba_codes)} unfilled but fillable BA Codes!"
        )
    len_before = len(ba_eia861_normed)
    ba_eia861_normed = ba_eia861_normed.dropna(
        subset=["report_date", "balancing_authority_id_eia"]
    )
    len_after = len(ba_eia861_normed)
    delta = len_before - len_after
    if delta > 3:
        logger.warning(
            "Unexpectedly large number of rows with NULL PK values found in "
            f"balancing_authority_eia861. Expected 3, found {delta} (out of "
            f"{len_before} total)."
        )

    return _post_process(ba_eia861_normed)
