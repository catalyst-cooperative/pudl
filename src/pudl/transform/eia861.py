"""
Module to perform data cleaning functions on EIA861 data tables.

Inputs to the transform functions are a dictionary of dataframes, each of which
represents a concatenation of records with common column names from across some set of
years of reported data. The names of those columns are determined by the xlsx_maps
metadata associated with EIA 861 in PUDL's package_metadata.

This raw data is transformed in 3 main steps:

1. Structural transformations that re-shape / tidy the data and turn it into rows that
   represent a single observation, and columns that represent a single variable. These
   transformations should not require knowledge of or access to the contents of the
   data, which may or may not yet be usable at this point, depending on the true data
   type and how much cleaning has to happen. One exception to this that may come up is
   the need to clean up columns that are part of the primary composite key, since you
   can't usefully index on NA values. Alternatively this might mean removing rows that
   have invalid key values.

2. Data type compatibility: whatever massaging of the data is required to ensure that it
   can be cast to the appropriate data type, including identifying NA values and
   assigning them to an appropriate type-specific NA value. At the end of this you can
   assign all the columns their (preferably nullable) types. Note that because some of
   the columns that exist at this point may not end up in the final database table, you
   may need to set them individually, rather than using the systemwide dictionary of
   column data types.

3. Value based data cleaning: At this point every column should have a known, homogenous
   type, allowing it to be reliably manipulated as a Series, so we can move on to
   cleaning up the values themselves. This includes re-coding freeform string fields to
   impose a controlled vocabulary, converting column units (e.g. kWh to MWh) and
   renaming the columns appropriately, as well as correcting clear data entry errors.

At the end of the main coordinating transform() function, every column that remains in
each of the transformed dataframes should correspond to a column that will exist in the
database and be associated with the EIA datasets, which means it is also part of the EIA
column namespace. part of the EIA namespace. It's important that you make sure these
column names match the naming conventions that are being used, and if any of the columns
exist in other tables, that they have exactly the same name and datatype.

If you find that you need to rename a column for it to conform to those requirements, in
many cases that should happen in the xlsx_map metadata, so that column renamings can be
kept to a minimum and only used for real semantic transformations of a column (like a
unit conversion).

At the end of this step it should also be easy to categorize every column in every
dataframe as to whether it is a "data" column (containing data unique this the table it
is found in) or whether it is part of the primary key for the table (the minimal set of
columns whose values are required to uniquely specify a record), and/or whether it is a
"denormalized" column whose home table is really elsewhere in the database. Note that
denormalized columns may also be part of the primary key. This information is important
for the next step that happens after the intra-table transformations, in which the
collection of EIA tables is normalized as a whole.

"""
import logging

import pandas as pd

import pudl
from pudl import constants as pc

logger = logging.getLogger(__name__)

BA_ID_NAME_FIXES = (
    pd.DataFrame([
        # report_date, util_id, ba_id, ba_name
        ('2001-01-01', 40577, 99999, 'Multiple Control Areas'),

        ('2002-01-01', 40577, 99999, 'Multiple Control Areas'),
        ('2002-01-01', 2759, 13781, 'Xcel Energy'),
        ('2002-01-01', 1004, 40604, 'Heartland Consumer Power Dist.'),
        ('2002-01-01', 5659, 20847, 'Wisconsin Electric Power'),
        ('2002-01-01', 5588, 9417, 'Interstate Power & Light'),
        ('2002-01-01', 6112, 9417, 'INTERSTATE POWER & LIGHT'),
        ('2002-01-01', 6138, 13781, 'Xcel Energy'),
        ('2002-01-01', 6276, pd.NA, 'Vectren Energy Delivery'),
        ('2002-01-01', 6501, 9417, 'Interstate Power and Light'),
        ('2002-01-01', 6579, 4716, 'Dairyland Power Coop'),
        ('2002-01-01', 6848, pd.NA, pd.NA),
        ('2002-01-01', 7140, 18195, 'Southern Co Services Inc'),
        ('2002-01-01', 7257, 22500, 'Westar Energy'),
        ('2002-01-01', 7444, 14232, 'Minnkota Power Cooperative'),
        ('2002-01-01', 8490, 22500, 'Westar'),
        ('2002-01-01', 8632, 12825, 'NorthWestern Energy'),
        ('2002-01-01', 8770, 22500, 'Westar Energy'),
        ('2002-01-01', 8796, 13434, 'ISO New England'),
        ('2002-01-01', 9699, pd.NA, 'Tri-State G&T'),
        ('2002-01-01', 10040, 13781, 'Xcel Energy'),
        ('2002-01-01', 10171, 56669, 'Midwest Indep System Operator'),
        ('2002-01-01', 11053, 9417, 'INTERSTATE POWER & LIGHT'),
        ('2002-01-01', 11148, 2775, 'California ISO'),
        ('2002-01-01', 11522, 1, 'Maritimes-Canada'),
        ('2002-01-01', 11731, 13781, 'XCEL Energy'),
        ('2002-01-01', 11788, 9417, 'Interstate Power & Light'),
        ('2002-01-01', 12301, 14232, 'Minnkota Power Cooperative'),
        ('2002-01-01', 12698, 20391, 'Aquila Networks - MPS'),
        ('2002-01-01', 12706, 18195, 'Southern Co Services Inc'),
        ('2002-01-01', 3258, 9417, 'Interstate Power & Light'),
        ('2002-01-01', 3273, 15473, 'Public Regulatory Commission'),
        ('2002-01-01', 3722, 9417, 'Interstate Power and Light'),
        ('2002-01-01', 1417, 12825, 'NorthWestern Energy'),
        ('2002-01-01', 1683, 12825, 'Northwestern Energy'),
        ('2002-01-01', 1890, 5416, 'Duke Energy Corporation'),
        ('2002-01-01', 4319, 20447, 'Okla. Municipal Pwr. Authority'),
        ('2002-01-01', 18446, 9417, 'Interstate Power and Light'),
        ('2002-01-01', 19108, pd.NA, 'NC Rural Electrification Auth.'),
        ('2002-01-01', 19545, 28503, 'Western Area Power Admin'),
        ('2002-01-01', 12803, 18195, 'Southern Illinois Power'),
        ('2002-01-01', 13382, 8283, 'Harrison County Rural Electric'),
        ('2002-01-01', 13423, 829, 'Town of New Carlisle'),
        ('2002-01-01', 13815, 13781, 'Xcel Energy'),
        ('2002-01-01', 14649, 18195, 'GSOC (Georgia System Operation'),
        ('2002-01-01', 15672, 924, 'Associated Electric Coop Inc'),
        ('2002-01-01', 16023, 9417, 'Interstate Power and Light'),
        ('2002-01-01', 16463, pd.NA, 'Central Louisiana Electric Co.'),
        ('2002-01-01', 16922, 22500, 'Westar Energy'),
        ('2002-01-01', 16992, 9417, 'Interstate Power and Light'),
        ('2002-01-01', 17643, 924, 'Associated Electric Coop Inc'),
        ('2002-01-01', 17706, 9417, 'Interstate Power & Light'),
        ('2002-01-01', 20811, 19876, 'Dominion NC Power'),
        ('2002-01-01', 3227, 15466, 'Xcel Energy'),
        ('2002-01-01', 20227, 14063, 'OG&E'),
        ('2002-01-01', 17787, 13337, 'Mun. Energy Agcy of Nebraska'),
        ('2002-01-01', 19264, 17718, 'Excel Energy'),
        ('2002-01-01', 11701, 19578, 'We Energies'),
        ('2002-01-01', 28802, 14725, 'PJM Interconnection'),
        ('2002-01-01', 20546, 1692, 'Big Rivers Electric Corp.'),
        ('2002-01-01', 6223, 1, 'Maritimes-Canada'),
        ('2002-01-01', 14405, 19876, 'VA Power'),
        ('2002-01-01', 14405, 14725, 'PJM'),
        ('2002-01-01', 12698, 20391, 'Aquila Networks - L&P'),
        ('2002-01-01', 16267, 12698, 'Aquila'),
        ('2002-01-01', 15871, 5723, 'ERC of Texas'),
        ('2002-01-01', 6753, 28503, 'Regional Office'),
        ('2002-01-01', 5571, 14328, 'Pacific Gas and Electric Co.'),
        ('2002-01-01', 367, pd.NA, 'Western Area Power Admin'),
        ('2002-01-01', 3247, 13501, 'NYISO'),
        ('2002-01-01', 11014, 5723, 'Ercot'),
        ('2002-01-01', 20845, 12427, 'Michigan Power Pool 12427'),
        ('2002-01-01', 17267, pd.NA, 'Watertown, SD'),
        ('2002-01-01', 12811, pd.NA, 'First Energy Corp.'),
        ('2002-01-01', 17368, 13501, 'NYISO'),
        ('2002-01-01', 5877, 13501, 'NYISO'),
        ('2002-01-01', 3240, pd.NA, 'Pacific NW Generating Cooperat'),
        ('2002-01-01', 3037, pd.NA, 'Trans Electric'),
        ('2002-01-01', 12199, 28503, 'WAPA-Rocky Mountain'),
        ('2002-01-01', 8936, 14378, 'Pacificorp'),
        ('2002-01-01', 40604, pd.NA, 'Watertown, SD Office'),
        ('2002-01-01', 19108, pd.NA, 'USDA- Rural Utility Service'),
        ('2002-01-01', 8199, 20391, 'Aquila'),
        ('2002-01-01', 12698, 20391, 'Aquila Networks - WPC'),
        ('2002-01-01', 12698, 20391, 'Aquila Networks - WPK'),
        ('2002-01-01', 20387, 14725, 'PJM West'),
        ('2002-01-01', 588, 20447, 'Western Farmers Elec Coop Inc'),
        ('2002-01-01', 17561, 5723, 'ERCOT ISO'),
        ('2002-01-01', 17320, 13781, 'Xcel Energy'),
        ('2002-01-01', 13676, 17716, 'Southwestern Power Admin.'),
        ('2002-01-01', 5703, 13501, 'NTISO'),
        ('2002-01-01', 113, 13501, 'NYISO'),
        ('2002-01-01', 4486, pd.NA, 'REMC of Western Indiana'),
        ('2002-01-01', 1039, 13501, 'NYISO'),
        ('2002-01-01', 5609, pd.NA, 'NMISA'),
        ('2002-01-01', 3989, pd.NA, 'WAPA'),
        ('2002-01-01', 13539, 13501, 'NY Independent System Operator'),
        ('2002-01-01', 15263, 14725, 'PJM West'),
        ('2002-01-01', 12796, 14725, 'PJM West'),
        ('2002-01-01', 3539, 13434, 'ISO New England'),
        ('2002-01-01', 3575, 13434, 'ISO New England'),
        ('2002-01-01', 3559, 13434, 'ISO New England'),
        ('2002-01-01', 18193, pd.NA, pd.NA),
        ('2002-01-01', 838, 3413, 'Chelan PUD'),
        ('2002-01-01', 1049, 1738, 'Bonneville'),
        ('2002-01-01', 9248, 14725, 'PJM'),
        ('2002-01-01', 15026, 803, 'APS Control Area'),
        ('2002-01-01', 798, 16572, 'Salt River Project'),
        ('2002-01-01', 5603, 13501, 'ISO - NEW YORK'),
        ('2002-01-01', 12260, 19876, 'Dominion Virginia Power'),
        ('2002-01-01', 14788, 17716, 'Southwest Power Administration'),
        ('2002-01-01', 12909, 22500, 'Westar Energy'),
        ('2002-01-01', 5605, 9417, 'Interstate Power and Light'),
        ('2002-01-01', 10908, 9417, 'Interstate Power and Light'),

        ('2003-01-01', 3258, 9417, 'Interstate Power & Light'),
        ('2003-01-01', 6501, 9417, 'Interstate Power & Light'),
        ('2003-01-01', 10650, 9417, 'Interstate Power & Light'),
        ('2003-01-01', 16992, 9417, 'Interstate Power & Light'),
        ('2003-01-01', 3722, 9417, 'Interstate Power & Light'),
        ('2003-01-01', 11788, 9417, 'Interstate Power & Light'),
        ('2003-01-01', 5588, 9417, 'Interstate Power & Light'),
        ('2003-01-01', 11053, 9417, 'Interstate Power & Light'),
        ('2003-01-01', 16023, 9417, 'Interstate Power & Light'),
        ('2003-01-01', 17706, 9417, 'Interstate Power & Light'),
        ('2003-01-01', 18446, 9417, 'Interstate Power & Light'),
        ('2004-01-01', 5309, 18195, 'Southern Company Services Inc'),
        ('2004-01-01', 192, 192, 'Ryant T. Rose'),
        ('2004-01-01', 6501, 9417, 'Interstate Power & Light'),
        ('2004-01-01', 16992, 9417, 'Interstate Power & Light'),
        ('2004-01-01', 8192, 14725, 'PJM-West'),
        ('2004-01-01', 192, 192, 'Phillip K. Peter, Sr.'),
        ('2004-01-01', 192, 192, 'Nelson Kinegak'),
        ('2004-01-01', 1004, 40604, 'Heartland Consumer Power Dist.'),
        ('2004-01-01', 3258, 9417, 'Interstate Power & Light'),
        ('2004-01-01', 3722, 9417, 'Interstate Power & Light'),
        ('2004-01-01', 19879, pd.NA, 'Kevin Smalls St Croix Districe'),
        ('2004-01-01', 11788, 9417, 'Interstate Power & Light'),
        ('2004-01-01', 4191, 13434, 'NEISO'),
        ('2004-01-01', 10650, 9417, 'Interstate Power & Light'),
        ('2004-01-01', 11053, 9417, 'Interstate Power & Light'),
        ('2004-01-01', 18446, 9417, 'Interstate Power & Light'),
        ('2004-01-01', 27000, pd.NA, 'Multiple Operators'),
        ('2004-01-01', 19879, pd.NA, 'Corey Hodge - St Thomass/St Jo'),
        ('2004-01-01', 13382, 8283, 'Harrison County Rural Electric'),
        ('2004-01-01', 10784, pd.NA, 'Hawkeye Tri-county REC'),
        ('2004-01-01', 16922, pd.NA, 'The Brown Atchison Electric Co'),
        ('2004-01-01', 15026, 803, 'APS Control Area'),
        ('2005-01-01', 192, 192, 'Ryant T. Rose'),
        ('2005-01-01', 192, 192, 'Phillip K. Peter, Sr.'),
        ('2005-01-01', 192, 182, 'Nelson Kinegak'),
        ('2005-01-01', 3258, 9417, 'Interstate Power & Light'),
        ('2005-01-01', 1004, 40604, 'Heartland Consumer Power Dist.'),
        ('2005-01-01', 5309, 18195, 'Southern Company Services Inc'),
        ('2005-01-01', 6501, 9417, 'Interstate Power & Light'),
        ('2005-01-01', 10623, 6455, 'Florida Power Corp'),
        ('2005-01-01', 10650, 9417, 'Interstate Power & Light'),
        ('2005-01-01', 13382, 8283, 'Harrison County Rural Electric'),
        ('2005-01-01', 16922, pd.NA, 'The Brown Atchison Electric Co'),
        ('2005-01-01', 3722, 9417, 'Interstate Power & Light'),
        ('2005-01-01', 4191, 13434, 'NEISO'),
        ('2005-01-01', 11788, 9417, 'Interstate Power & Light'),
        ('2005-01-01', 8192, 14725, 'PJM-West'),
        ('2005-01-01', 11053, 9417, 'Interstate Power & Light'),
        ('2005-01-01', 13815, 13781, 'Northern States Power Co'),
        ('2005-01-01', 15026, 803, 'APS Control Area'),
        ('2005-01-01', 18446, 9417, 'Interstate Power & Light'),
        ('2005-01-01', 19879, pd.NA, 'Kevin Smalls St Croix Districe'),
        ('2005-01-01', 19879, pd.NA, 'Corey Hodge - St Thomass/St Jo'),
        ('2005-01-01', 27000, pd.NA, 'Multiple Operators'),
        ('2005-01-01', 10610, 13501, 'ISO New York'),

        ('2006-01-01', 10610, 13501, 'ISO New York'),

        ('2008-01-01', 10610, 13501, 'ISO New York'),

        ('2009-01-01', 10610, 13501, 'ISO New York'),

        ('2010-01-01', 6389, 3755, 'Cleveland Electric Illum Co'),
        ('2010-01-01', 6389, 13998, 'Ohio Edison Co'),
        ('2010-01-01', 6389, 18997, 'Toledo Edison Co'),
        ('2010-01-01', 6949, 10000, 'Kansas City Power & Light Co'),
        ('2010-01-01', 14127, 14127, 'Omaha Public Power District'),
        ('2010-01-01', 11196, 13434, 'ISO New England'),
        ('2010-01-01', 97, 56669, 'Midwest Independent System Operator'),
        ('2010-01-01', 3258, 56669, 'Midwest Independent System Operator'),
        ('2010-01-01', 3405, 56669, 'Midwest Independent System Operator'),
        ('2010-01-01', 3755, 56669, 'Midwest Independent System Operator'),
        ('2010-01-01', 7292, 56669, 'Midwest Independent System Operator'),
        ('2010-01-01', 8847, 56669, 'Midwest Independent System Operator'),
        ('2010-01-01', 11701, 56669, 'Midwest Independent System Operator'),
        ('2010-01-01', 13032, 56669, 'Midwest Independent System Operator'),
        ('2010-01-01', 13998, 56669, 'Midwest Independent System Operator'),
        ('2010-01-01', 14716, 56669, 'Midwest Independent System Operator'),
        ('2010-01-01', 17141, 56669, 'Midwest Independent System Operator'),
        ('2010-01-01', 18997, 56669, 'Midwest Independent System Operator'),
        ('2010-01-01', 21249, 56669, 'Midwest Independent System Operator'),
        ('2010-01-01', 40582, 56669, 'Midwest Independent System Operator'),
        ('2010-01-01', 54862, 56669, 'Midwest Independent System Operator'),
        ('2010-01-01', 56162, 56669, 'Midwest Independent System Operator'),
        ('2010-01-01', 56496, 56669, 'Midwest Independent System Operator'),
        ('2010-01-01', 10610, 13501, 'ISO New York'),

        ('2011-01-01', 1968, 56669, 'Midwest Independent System Operator'),
        ('2011-01-01', 20806, 56669, 'Midwest Independent System Operator'),
        ('2011-01-01', 29296, 56669, 'Midwest Independent System Operator'),

        ('2012-01-01', 1968, 56669, 'Midwest Independent System Operator'),
        ('2012-01-01', 20806, 56669, 'Midwest Independent System Operator'),
        ('2012-01-01', 29296, 56669, 'Midwest Independent System Operator'),

    ], columns=[
        "report_date",  # We have this
        "utility_id_eia",  # We have this
        "balancing_authority_id_eia",  # We need to set this
        "balancing_authority_name_eia",  # We have this
    ])
    .assign(report_date=lambda x: pd.to_datetime(x.report_date))
    .astype({
        "utility_id_eia": pd.Int64Dtype(),
        "balancing_authority_id_eia": pd.Int64Dtype(),
        "balancing_authority_name_eia": pd.StringDtype(),
    })
    .dropna(subset=["report_date", "balancing_authority_name_eia", "utility_id_eia"])
    .set_index(["report_date", "balancing_authority_name_eia", "utility_id_eia"])
)

EIA_FIPS_COUNTY_FIXES = pd.DataFrame([
    ("AK", "Aleutians Ea", "Aleutians East"),
    ("AK", "Aleutian Islands", "Aleutians East"),
    ("AK", "Aleutians East Boro", "Aleutians East Borough"),
    ("AK", "Prince of Wales Ketchikan", "Prince of Wales-Hyder"),
    ("AK", "Prince Wales", "Prince of Wales-Hyder"),
    ("AK", "Ketchikan Gateway Bo", "Ketchikan Gateway Borough"),
    ("AK", "Prince of Wale", "Prince of Wales-Hyder"),
    ("AK", "Wrangell Petersburg", "Wrangell"),
    ("AK", "Wrangell Pet", "Wrangell"),
    ("AK", "Borough, Kodiak Island", "Kodiak Island Borough"),
    ("AK", "Matanuska Susitna Borough", "Matanuska-Susitna"),
    ("AK", "Matanuska Susitna", "Matanuska-Susitna"),
    ("AK", "Skagway-Yakutat", "Skagway"),
    ("AK", "Skagway Yaku", "Skagway"),
    ("AK", "Skagway Hoonah Angoon", "Hoonah-Angoon"),
    ("AK", "Angoon", "Hoonah-Angoon"),
    ("AK", "Hoonah", "Hoonah-Angoon"),
    ("AK", "Yukon Koyukuk", "Yukon-Koyukuk"),
    ("AK", "Yukon Koyuku", "Yukon-Koyukuk"),
    ("AK", "Yukon-Koyuku", "Yukon-Koyukuk"),
    ("AK", "Valdez Cordova", "Valdez-Cordova"),
    ("AK", "Cordova", "Valdez-Cordova"),
    ("AK", "Valdez Cordo", "Valdez-Cordova"),
    ("AK", "Lake and Pen", "Lake and Peninsula"),
    ("AK", "Lake & Peninsula Borough", "Lake and Peninsula"),
    ("AK", "Kodiak Islan", "Kodiak Island"),
    ("AK", "Kenai Penins", "Kenai Peninsula"),
    ("AK", "NW Arctic Borough", "Northwest Arctic"),
    ("AL", "De Kalb", "DeKalb"),
    ("AR", "Saint Franci", "St. Francis"),
    ("CA", "San Bernadino", "San Bernardino"),
    ("CA", "San Bernardi", "San Bernardino"),
    ("CT", "Shelton", "Fairfield"),
    ("FL", "De Soto", "DeSoto"),
    ("FL", "Miami Dade", "Miami-Dade"),
    ("FL", "Dade", "Miami-Dade"),
    ("FL", "St. Lucic", "St. Lucie"),
    ("FL", "St. Loucie", "St. Lucie"),
    ("GA", "De Kalb", "DeKalb"),
    ("GA", "Chattahooche", "Chattahoochee"),
    ("IA", "Pottawattami", "Pottawattamie"),
    ("IA", "Kossuh", "Kossuth"),
    ("IA", "Lousia", "Louisa"),
    ("IA", "Poweshick", "Poweshiek"),
    ("IA", "Humbolt", "Humboldt"),
    ("IA", "Harris", "Harrison"),
    ("IA", "O Brien", "O'Brien"),
    ("IL", "JoDavies", "Jo Daviess"),
    ("IL", "La Salle", "LaSalle"),
    ("IL", "Green", "Greene"),
    ("IL", "DeWitt", "De Witt"),
    ("IL", "Dewitt", "De Witt"),
    ("IL", "Du Page", "DuPage"),
    ("IL", "Burke", "Christian"),
    ("IL", "McCoupin", "Macoupin"),
    ("IN", "De Kalb County", "DeKalb County"),
    ("IN", "De Kalb", "DeKalb County"),
    ("IN", "La Porte", "LaPorte"),
    ("IN", "Putman", "Putnam"),
    ("IN", "Pyke", "Pike"),
    ("IN", "Sulliva", "Sullivan"),
    ("KS", "Leaveworth", "Leavenworth"),
    ("KY", "Spenser", "Spencer"),
    ("LA", "Jefferson Da", "Jefferson Davis"),
    ("LA", "Pointe Coupe", "Pointe Coupee"),
    ("LA", "West Baton R", "West Baton Rouge"),
    ("LA", "DeSoto", "De Soto"),
    ("LA", "Burke", "Iberia"),
    ("LA", "West Feleciana", "West Feliciana"),
    ("MA", "North Essex", "Essex"),
    ("MI", "Grand Traver", "Grand Traverse"),
    ("MI", "Antim", "Antrim"),
    ("MD", "Balto. City", "Baltimore City"),
    ("MD", "Prince Georg", "Prince George's County"),
    ("MD", "Worchester", "Worcester"),
    ("MN", "Fairbault", "Faribault"),
    ("MN", "Lac Qui Parl", "Lac Qui Parle"),
    ("MN", "Lake of The", "Lake of the Woods"),
    ("MN", "Ottertail", "Otter Tail"),
    ("MN", "Yellow Medic", "Yellow Medicine"),
    ("MO", "De Kalb", "DeKalb"),
    ("MO", "Cape Girarde", "Cape Girardeau"),
    ("MS", "Clark", "Clarke"),
    ("MS", "Clark", "Clarke"),
    ("MS", "De Soto", "DeSoto"),
    ("MS", "Jefferson Da", "Jefferson Davis"),
    ("MS", "Homoshitto", "Amite"),
    ("MT", "Anaconda-Dee", "Deer Lodge"),
    ("MT", "Butte-Silver", "Silver Bow"),
    ("MT", "Golden Valle", "Golden Valley"),
    ("MT", "Lewis and Cl", "Lewis and Clark"),
    ("NC", "Hartford", "Hertford"),
    ("NC", "Gilford", "Guilford"),
    ("NC", "North Hampton", "Northampton"),
    ("ND", "La Moure", "LaMoure"),
    ("NH", "Plaquemines", "Coos"),
    ("NH", "New Hampshire", "Coos"),
    ("OK", "Cimmaron", "Cimarron"),
    ("NY", "Westcherster", "Westchester"),
    ("OR", "Unioin", "Union"),
    ("PA", "Northumberla", "Northumberland"),
    ("PR", "Aquadilla", "Aguadilla"),
    ("PR", "Sabana Grand", "Sabana Grande"),
    ("PR", "San Sebastia", "San Sebastian"),
    ("PR", "Trujillo Alt", "Trujillo Alto"),
    ("RI", "Portsmouth", "Newport"),
    ("TX", "Collingswort", "Collingsworth"),
    ("TX", "De Witt", "DeWitt"),
    ("TX", "Hayes", "Hays"),
    ("TX", "San Augustin", "San Augustine"),
    ("VA", "Alexandria C", "Alexandria City"),
    ("VA", "City of Suff", "Suffolk City"),
    ("VA", "City of Manassas", "Manassas City"),
    ("VA", "Charlottesvi", "Charlottesville City"),
    ("VA", "Chesapeake C", "Chesapeake City"),
    ("VA", "Clifton Forg", "Alleghany"),
    ("VA", "Colonial Hei", "Colonial Heights City"),
    ("VA", "Covington Ci", "Covington City"),
    ("VA", "Fredericksbu", "Fredericksburg City"),
    ("VA", "Hopewell Cit", "Hopewell City"),
    ("VA", "Isle of Wigh", "Isle of Wight"),
    ("VA", "King and Que", "King and Queen"),
    ("VA", "Lexington Ci", "Lexington City"),
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
    ("VA", "Virginia Bea", "Virginia Beach City"),
    ("VA", "Waynesboro C", "Waynesboro City"),
    ("VA", "Winchester C", "Winchester City"),
    ("WA", "Wahkiakurn", "Wahkiakum"),
], columns=["state", "eia_county", "fips_county"])

BA_NAME_FIXES = pd.DataFrame([
    ("Omaha Public Power District", 14127, "OPPD"),
    ("Kansas City Power & Light Co", 10000, "KCPL"),
    ("Toledo Edison Co", 18997, pd.NA),
    ("Ohio Edison Co", 13998, pd.NA),
    ("Cleveland Electric Illum Co", 3755, pd.NA),
], columns=["balancing_authority_name_eia",
            "balancing_authority_id_eia",
            "balancing_authority_code_eia",
            ]
)

NERC_SPELLCHECK = {
    'GUSTAVUSAK': 'ASCC',
    'AK': 'ASCC',
    'HI': 'HICC',
    'ERCTO': 'ERCOT',
    'RFO': 'RFC',
    'RF': 'RFC',
    'SSP': 'SPP',
    'VACAR': 'SERC',  # VACAR is a subregion of SERC
    'GATEWAY': 'SERC',  # GATEWAY is a subregion of SERC
    'TERR': 'GU',
    25470: 'MRO',
    'TX': 'TRE',
    'NY': 'NPCC',
    'NEW': 'NPCC',
    'YORK': 'NPCC',
}

NERC_CLASS = [
    'tre',
    'frcc',
    'mro',
    'npcc',
    'rfc',
    'serc',
    'spp',
    'wecc',
]

RTO_CLASS = [
    'caiso',
    'ercot',
    'pjm',
    'nyiso',
    'spp',
    'miso',
    'isone',
    'other'
]

###############################################################################
# EIA Form 861 Transform Helper functions
###############################################################################


def _filter_class_cols(df, class_list):
    regex = f"^({'_|'.join(class_list)}).*$"
    return df.filter(regex=regex)


def _filter_non_class_cols(df, class_list):
    regex = f"^(?!({'_|'.join(class_list)})).*$"
    return df.filter(regex=regex)


def _ba_code_backfill(df):
    """
    Backfill Balancing Authority Codes based on codes in later years.

    Note:
        The BA Code to ID mapping can change from year to year. If a Balancing Authority
        is bought by another entity, the code may change, but the old EIA BA ID will be
        retained.

    Args:
        ba_eia861 (pandas.DataFrame): The transformed EIA 861 Balancing
            Authority dataframe (balancing_authority_eia861).

    Returns:
        pandas.DataFrame: The balancing_authority_eia861 dataframe, but with
        many fewer NA values in the balancing_authority_code_eia column.

    """
    start_len = len(df)
    start_nas = len(df.loc[df.balancing_authority_code_eia.isnull()])
    logger.info(
        f"Started with {start_nas} missing BA Codes out of {start_len} "
        f"records ({start_nas/start_len:.2%})")
    ba_ids = (
        df[["balancing_authority_id_eia", "balancing_authority_code_eia", "report_date"]]
        .drop_duplicates()
        .sort_values(["balancing_authority_id_eia", "report_date"])
    )
    ba_ids["ba_code_filled"] = (
        ba_ids.groupby("balancing_authority_id_eia")[
            "balancing_authority_code_eia"].fillna(method="bfill")
    )
    ba_eia861_filled = df.merge(ba_ids, how="left")
    ba_eia861_filled = (
        ba_eia861_filled.assign(
            balancing_authority_code_eia=lambda x: x.ba_code_filled)
        .drop("ba_code_filled", axis="columns")
    )
    end_len = len(ba_eia861_filled)
    if start_len != end_len:
        raise AssertionError(
            f"Number of rows in the dataframe changed {start_len}!={end_len}!"
        )
    end_nas = len(
        ba_eia861_filled.loc[ba_eia861_filled.balancing_authority_code_eia.isnull()])
    logger.info(
        f"Ended with {end_nas} missing BA Codes out of {end_len} "
        f"records ({end_nas/end_len:.2%})")
    return ba_eia861_filled


def _tidy_class_dfs(df, df_name, idx_cols, class_list, class_type, keep_totals=False):
    # Clean up values just enough to use primary key columns as a multi-index:
    logger.debug(
        f"Cleaning {df_name} table index columns so we can tidy data.")
    if 'balancing_authority_code_eia' in idx_cols:
        df = (
            df.assign(
                balancing_authority_code_eia=lambda x: x.balancing_authority_code_eia.fillna("UNK"))
        )
    raw_df = (
        df.dropna(subset=["utility_id_eia"])
        .astype({"utility_id_eia": pd.Int64Dtype()})
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
    # This ensures that the underscore AFTER the desired string (that can also include underscores)
    # is where the column headers are split, not just the first underscore.
    class_list_regex = '|'.join(['(?<=' + col + ')_' for col in class_list])

    data_cols.columns = (
        data_cols.columns.str.split(fr"{class_list_regex}", n=1, expand=True)
        .set_names([class_type, None])
    )
    # Now stack the customer classes into their own categorical column,
    data_cols = (
        data_cols.stack(level=0, dropna=False)
        .reset_index()
    )
    denorm_cols = _filter_non_class_cols(raw_df, class_list).reset_index()

    # Merge the index, data, and denormalized columns back together
    tidy_df = pd.merge(denorm_cols, data_cols, on=idx_cols)

    # Compare reported totals with sum of component columns
    if 'total' in class_list:
        _compare_totals(data_cols, idx_cols, class_type, df_name)
    if keep_totals is False:
        tidy_df = tidy_df.query(f"{class_type}!='total'")

    return tidy_df, idx_cols + [class_type]


def _drop_dupes(df, subset):
    tidy_nrows = len(df)
    deduped_df = df.drop_duplicates(
        subset=subset, keep=False)
    deduped_nrows = len(df)
    logger.info(
        f"Dropped {tidy_nrows-deduped_nrows} duplicate records from EIA 861 "
        f"Demand Response table, out of a total of {tidy_nrows} records "
        f"({(tidy_nrows-deduped_nrows)/tidy_nrows:.4%} of all records). "
    )
    return deduped_df


def _check_for_dupes(df, df_name, subset):
    dupes = (
        df.duplicated(
            subset=subset, keep=False)
    )
    if dupes.any():
        raise AssertionError(
            f"Found {len(df[dupes])} duplicate rows in the {df_name} table, "
            f"when zero were expected!"
        )


def _early_transform(df):
    """Fix EIA na values and convert year column to date."""
    df = pudl.helpers.fix_eia_na(df)
    df = pudl.helpers.convert_to_date(df)
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
    data_cols = pudl.helpers.convert_cols_dtypes(data_cols, 'eia')
    # Drop data cols that are non numeric (preserve primary keys)
    logger.debug(f'{idx_cols}, {class_type}')
    data_cols = (
        data_cols.set_index(idx_cols + [class_type])
        .select_dtypes('number')
        .reset_index()
    )
    logger.debug(f'{data_cols.columns.tolist()}')
    # Create list of data columns to be summed
    # (may include non-numeric that will be excluded)
    data_col_list = set(data_cols.columns.tolist()) - \
        set(idx_cols + [class_type])
    logger.debug(f'{data_col_list}')
    # Distinguish reported totals from segments
    data_totals_df = data_cols.loc[data_cols[class_type] == 'total']
    data_no_tots_df = data_cols.loc[data_cols[class_type] != 'total']
    # Calculate sum of segments for comparison against reported total
    data_sums_df = data_no_tots_df.groupby(
        idx_cols + [class_type], observed=True).sum()
    sum_total_df = pd.merge(data_totals_df, data_sums_df, on=idx_cols,
                            how='outer', suffixes=('_total', '_sum'))
    # Check each data column's calculated sum against the reported total
    for col in data_col_list:
        col_df = (sum_total_df.loc[sum_total_df[col + '_total'].notnull()])
        if len(col_df) > 0:
            col_df = (
                col_df.assign(
                    compare_totals=lambda x: (x[col + '_total'] == x[col + '_sum']))
            )
            bad_math = (col_df['compare_totals']).sum() / len(col_df)
            logger.debug(
                f"{df_name}: for column {col}, {bad_math:.0%} "
                "of non-null reported totals = the sum of parts."
            )
        else:
            logger.debug(
                f'{df_name}: for column {col} all total values are NaN')


def _clean_nerc(df, idx_cols):
    """Clean NERC region entries and make new rows for multiple nercs.

    This function examines reported NERC regions and makes sure the output column of
    the same name has reliable, singular NERC region acronyms. To do so, this function
    identifies entries where there are two or more NERC regions specified in a single cell
    (such as SPP & ERCOT) and makes new, duplicate rows for each NERC region. It also
    converts non-recognized reported nerc regions to 'UNK'.

    Args:
        df (pandas.DataFrame): A DataFrame with the column 'nerc_region' to be
            cleaned.
        idx_cols (list): A list of the primary keys.

    Returns:
        pandas.DataFrame: A DataFrame with correct and clean nerc regions.

    """
    idx_no_nerc = idx_cols.copy()
    if 'nerc_region' in idx_no_nerc:
        idx_no_nerc.remove('nerc_region')

    # Split raw df into primary keys plus nerc region and other value cols
    nerc_df = df[idx_cols].copy()
    other_df = df.drop('nerc_region', axis=1).set_index(idx_no_nerc)

    # Make all values upper-case
    # Replace all NA values with UNK
    # Make nerc values into lists to see how many separate values are stuffed into one row (ex: 'SPP & ERCOT' --> ['SPP', 'ERCOT'])
    nerc_df = (
        nerc_df.assign(
            nerc_region=(lambda x: (
                x.nerc_region
                .str.upper()
                .fillna('UNK')
                .str.findall(r'[A-Z]+')))
        )
    )

    # Record a list of the reported nerc regions not included in the recognized regions list (these eventually become UNK)
    nerc_col = nerc_df['nerc_region'].tolist()
    nerc_list = list(set([item for sublist in nerc_col for item in sublist]))
    non_nerc_list = [
        nerc_entity for nerc_entity in nerc_list if nerc_entity not in pc.RECOGNIZED_NERC_REGIONS + list(NERC_SPELLCHECK.keys())]
    print(
        f'The following reported NERC regions are not currently recognized and become UNK values: {non_nerc_list}')

    # Function to turn instances of 'SPP_UNK' or 'SPP_SPP' into 'SPP'
    def _remove_nerc_duplicates(entity_list):
        if len(entity_list) > 1:
            if 'UNK' in entity_list:
                entity_list.remove('UNK')
            if all(x == entity_list[0] for x in entity_list):
                entity_list = [entity_list[0]]
        return entity_list

    # Go through the nerc regions, spellcheck errors, delete those that aren't recognized, and piece them back together
    # (with _ separator if more than one recognized)
    nerc_df['nerc_region'] = (
        nerc_df['nerc_region']
        .apply(lambda x: [i if i not in NERC_SPELLCHECK.keys() else NERC_SPELLCHECK[i] for i in x])
        .apply(lambda x: sorted([i if i in pc.RECOGNIZED_NERC_REGIONS else 'UNK' for i in x]))
        .apply(lambda x: _remove_nerc_duplicates(x))
        .str.join('_')
    )

    # Merge all data back together
    full_df = pd.merge(nerc_df, other_df, on=idx_no_nerc)

    return full_df


def _compare_nerc_physical_w_nerc_operational(df):
    """Show df rows where physical nerc region does not match operational region.

    In the Utility Data table, there is the 'nerc_region' index column, otherwise
    interpreted as nerc region in which the utility is physically located and the
    'nerc_regions_of_operation' column that depicts the nerc regions the utility
    operates in. In most cases, these two columns are the same, however there are certain
    instances where this is not true. There are also instances where a utility operates in
    multiple nerc regions in which case one row will match and another row will not.
    The output of this function in a table that shows only the utilities where the physical
    nerc region does not match the operational region ever, meaning there is no additional
    row for the same utlity during the same year where there is a match between the cols.

    Args:
        df (pandas.DataFrame): The utility_data_nerc_eia861 table output from the utility_data()
            function.
    Returns:
        pandas.DataFrame: A DataFrame with rows for utilities where NO listed operating nerc
            region matches the "physical location" nerc region column that's a part of the index.

    """
    # Set NA states to UNK
    df['state'] = df['state'].fillna('UNK')

    # Create column indicating whether the nerc region matches the nerc region of operation (TRUE)
    df['nerc_match'] = df['nerc_region'] == df['nerc_regions_of_operation']

    # Group by utility, state, and report date to see which groups have at least one TRUE value
    grouped_nerc_match_bools = (
        df.groupby(['utility_id_eia', 'state', 'report_date'])
        [['nerc_match']].any()
        .reset_index()
        .rename(columns={'nerc_match': 'nerc_group_match'})
    )

    # Merge back with original df to show cases where there are multiple non-matching nerc values
    # per utility id, year, and state.
    expanded_nerc_match_bools = (
        pd.merge(df,
                 grouped_nerc_match_bools,
                 on=['utility_id_eia', 'state', 'report_date'],
                 how='left')
    )

    # Keep only rows where there are no matches for the whole group.
    # Delete row with individual match boolean vs. group match boolean
    expanded_nerc_match_bools_false = (
        expanded_nerc_match_bools[~expanded_nerc_match_bools['nerc_group_match']]
        .drop(columns='nerc_match', axis=1)
    )

    return expanded_nerc_match_bools_false


###############################################################################
# EIA Form 861 Table Transform Functions
###############################################################################


def service_territory(tfr_dfs):
    """Transform the EIA 861 utility service territory table.

    Args:
        tfr_dfs (dict): A dictionary of DataFrame objects in
            which pages from EIA861 form (keys) correspond to normalized
            DataFrames of values from that page (values)

    Returns:
        dict: a dictionary of pandas.DataFrame objects in
        which pages from EIA861 form (keys) correspond to normalized
        DataFrames of values from that page (values)

    """
    # No data tidying required
    # There are a few NA values in the county column which get interpreted
    # as floats, which messes up the parsing of counties by addfips.
    type_compatible_df = tfr_dfs["service_territory_eia861"].astype({
                                                                    "county": str})
    # Transform values:
    # * Add state and county fips IDs
    transformed_df = (
        # Ensure that we have the canonical US Census county names:
        pudl.helpers.clean_eia_counties(
            type_compatible_df,
            fixes=EIA_FIPS_COUNTY_FIXES)
        # Add FIPS IDs based on county & state names:
        .pipe(pudl.helpers.add_fips_ids)
    )
    tfr_dfs["service_territory_eia861"] = transformed_df
    return tfr_dfs


def balancing_authority(tfr_dfs):
    """
    Transform the EIA 861 Balancing Authority table.

    Args:
        tfr_dfs (dict): A dictionary of transformed EIA 861 DataFrames, keyed by table
            name. It will be mutated by this function.

    Returns:
        dict: A dictionary of transformed EIA 861 dataframes, keyed by table name.

    """
    # No data tidying required
    # All columns are already type compatible.
    # Value transformations:
    # * Backfill BA codes on a per BA ID basis
    # * Fix data entry errors
    df = (
        tfr_dfs["balancing_authority_eia861"]
        .pipe(pudl.helpers.convert_cols_dtypes, "eia", "balancing_authority_eia861")
        .set_index(["report_date", "balancing_authority_name_eia", "utility_id_eia"])
    )

    # Fill in BA IDs based on date, utility ID, and BA Name:
    df.loc[BA_ID_NAME_FIXES.index,
           "balancing_authority_id_eia"] = BA_ID_NAME_FIXES.balancing_authority_id_eia

    # Backfill BA Codes based on BA IDs:
    df = df.reset_index().pipe(_ba_code_backfill)
    # Typo: NEVP, BA ID is 13407, but in 2014-2015 in UT, entered as 13047
    df.loc[
        (df.balancing_authority_code_eia == "NEVP") &
        (df.balancing_authority_id_eia == 13047),
        "balancing_authority_id_eia"
    ] = 13407
    # Typo: Turlock Irrigation District is TIDC, not TID.
    df.loc[
        (df.balancing_authority_code_eia == "TID") &
        (df.balancing_authority_id_eia == 19281),
        "balancing_authority_code_eia"
    ] = "TIDC"

    tfr_dfs["balancing_authority_eia861"] = df
    return tfr_dfs


def balancing_authority_assn(tfr_dfs):
    """
    Compile a balancing authority, utility, state association table.

    For the years up through 2012, the only BA-Util information that's
    available comes from the balancing_authority_eia861 table, and it
    does not include any state-level information. However, there is
    utility-state association information in the sales_eia861 and
    other data tables.

    For the years from 2013 onward, there's explicit BA-Util-State
    information in the data tables (e.g. sales_eia861). These observed
    associations can be compiled to give us a picture of which
    BA-Util-State associations exist. However, we need to merge in
    the balancing authority IDs since the data tables only contain
    the balancing authority codes.

    Args:
        tfr_dfs (dict): A dictionary of transformed EIA 861 dataframes.
            This must include any dataframes from which we want to
            compile BA-Util-State associations, which means this
            function has to be called after all the basic transform
            functions that depend on only a single raw table.

    Returns:
        dict: a dictionary of transformed dataframes. This function
        both compiles the association table, and finishes the
        normalization of the balancing authority table. It may be that
        once the harvesting process incorporates the EIA 861, some or
        all of this functionality should be pulled into the phase-2
        transform functions.

    """
    # These aren't really "data" tables, and should not be searched for associations:
    non_data_dfs = [
        "balancing_authority_eia861",
        "service_territory_eia861",
    ]
    # The dataframes from which to compile BA-Util-State associations
    data_dfs = [tfr_dfs[table] for table in tfr_dfs if table not in non_data_dfs]

    logger.info("Building an EIA 861 BA-Util-State-Date association table.")

    # Helpful shorthand query strings....
    early_years = "report_date<='2012-12-31'"
    late_years = "report_date>='2013-01-01'"
    early_dfs = [df.query(early_years) for df in data_dfs]
    late_dfs = [df.query(late_years) for df in data_dfs]

    # The old BA table lists utilities directly, but has no state information.
    early_date_ba_util = _harvest_associations(
        dfs=[tfr_dfs["balancing_authority_eia861"].query(early_years), ],
        cols=["report_date",
              "balancing_authority_id_eia",
              "utility_id_eia"],
    )
    # State-utility associations are brought in from observations in data_dfs
    early_date_util_state = _harvest_associations(
        dfs=early_dfs,
        cols=["report_date",
              "utility_id_eia",
              "state"],
    )
    early_date_ba_util_state = (
        early_date_ba_util
        .merge(early_date_util_state, how="outer")
        .drop_duplicates()
    )

    # New BA table has no utility information, but has BA Codes...
    late_ba_code_id = _harvest_associations(
        dfs=[tfr_dfs["balancing_authority_eia861"].query(late_years), ],
        cols=["report_date",
              "balancing_authority_code_eia",
              "balancing_authority_id_eia"],
    )
    # BA Code allows us to bring in utility+state data from data_dfs:
    late_date_ba_code_util_state = _harvest_associations(
        dfs=late_dfs,
        cols=["report_date",
              "balancing_authority_code_eia",
              "utility_id_eia",
              "state"],
    )
    # We merge on ba_code then drop it, b/c only BA ID exists in all years consistently:
    late_date_ba_util_state = (
        late_date_ba_code_util_state
        .merge(late_ba_code_id, how="outer")
        .drop("balancing_authority_code_eia", axis="columns")
        .drop_duplicates()
    )

    tfr_dfs["balancing_authority_assn_eia861"] = (
        pd.concat([early_date_ba_util_state, late_date_ba_util_state])
        .dropna(subset=["balancing_authority_id_eia", ])
        .astype({"utility_id_eia": pd.Int64Dtype()})
    )
    return tfr_dfs


def utility_assn(tfr_dfs):
    """Harvest a Utility-Date-State Association Table."""
    # These aren't really "data" tables, and should not be searched for associations
    non_data_dfs = [
        "balancing_authority_eia861",
        "service_territory_eia861",
    ]
    # The dataframes from which to compile BA-Util-State associations
    data_dfs = [tfr_dfs[table] for table in tfr_dfs if table not in non_data_dfs]

    logger.info("Building an EIA 861 Util-State-Date association table.")
    tfr_dfs["utility_assn_eia861"] = _harvest_associations(
        data_dfs, ["report_date", "utility_id_eia", "state"])
    return tfr_dfs


def _harvest_associations(dfs, cols):
    """
    Compile all unique, non-null combinations of values ``cols`` within ``dfs``.

    Args:
        dfs (iterable of pandas.DataFrame): The DataFrames in which to search for
            unique associations.
        cols (iterable of str): Labels of columns for which to find unique, non-null
            combinations of values.

    Raises:
        ValueError: if no associations for cols are found in dfs.

    Returns:
        pandas.DataFrame: A dataframe containing all the unique, non-null combinations
        of values found in ``cols``.

    """
    assn = pd.DataFrame()
    for df in dfs:
        if set(df.columns).issuperset(set(cols)):
            assn = assn.append(df[cols])
    assn = assn.dropna().drop_duplicates()
    if assn.empty:
        raise ValueError(
            "These dataframes contain no associations for the columns: "
            f"{cols}"
        )
    return assn


def normalize_balancing_authority(tfr_dfs):
    """
    Finish the normalization of the balancing_authority_eia861 table.

    The balancing_authority_assn_eia861 table depends on information that is only
    available in the UN-normalized form of the balancing_authority_eia861 table, so
    and also on having access to a bunch of transformed data tables, so it can compile
    the observed combinations of report dates, balancing authorities, states, and
    utilities. This means that we have to hold off on the final normalization of the
    balancing_authority_eia861 table until the rest of the transform process is over.

    """
    logger.info("Completing normalization of balancing_authority_eia861.")
    ba_eia861_normed = (
        tfr_dfs["balancing_authority_eia861"]
        .loc[:, [
            "report_date",
            "balancing_authority_id_eia",
            "balancing_authority_code_eia",
            "balancing_authority_name_eia",
        ]]
        .drop_duplicates(subset=["report_date", "balancing_authority_id_eia"])
    )

    # Make sure that there aren't any more BA IDs we can recover from later years:
    ba_ids_missing_codes = (
        ba_eia861_normed.loc[
            ba_eia861_normed.balancing_authority_code_eia.isnull(),
            "balancing_authority_id_eia"]
        .drop_duplicates()
        .dropna()
    )
    fillable_ba_codes = ba_eia861_normed[
        (ba_eia861_normed.balancing_authority_id_eia.isin(ba_ids_missing_codes)) &
        (ba_eia861_normed.balancing_authority_code_eia.notnull())
    ]
    if len(fillable_ba_codes) != 0:
        raise ValueError(
            f"Found {len(fillable_ba_codes)} unfilled but fillable BA Codes!"
        )

    tfr_dfs["balancing_authority_eia861"] = ba_eia861_normed
    return tfr_dfs


def sales(tfr_dfs):
    """Transform the EIA 861 Sales table."""
    idx_cols = [
        "utility_id_eia",
        "state",
        "report_date",
        "business_model",
        "service_type",
        "balancing_authority_code_eia"
    ]

    # Pre-tidy clean specific to sales table
    raw_sales = (
        tfr_dfs["sales_eia861"].copy()
        .query("utility_id_eia not in (88888, 99999)")
    )

    ###########################################################################
    # Tidy Data:
    ###########################################################################

    logger.info("Tidying the EIA 861 Sales table.")
    tidy_sales, idx_cols = _tidy_class_dfs(
        raw_sales,
        df_name='Sales',
        idx_cols=idx_cols,
        class_list=pc.CUSTOMER_CLASSES,
        class_type='customer_class',
    )

    # remove duplicates on the primary key columns + customer_class -- there
    # are a handful of records, all from 2010-2012, that have reporting errors
    # that produce dupes, which do not have a clear meaning. The utility_id_eia
    # values involved are: [8153, 13830, 17164, 56431, 56434, 56466, 56778,
    # 56976, 56990, 57081, 57411, 57476, 57484, 58300]
    deduped_sales = _drop_dupes(tidy_sales, idx_cols)

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
    transformed_sales = (
        deduped_sales.assign(
            sales_revenue=lambda x: x.sales_revenue * 1000.0,
            data_observed=lambda x: x.data_observed.replace({
                "O": True,
                "I": False,
            }),
            business_model=lambda x: x.business_model.replace({
                "A": "retail",
                "B": "retail",
                "C": "retail",
                "D": "energy_services",
            }),
            service_type=lambda x: x.service_type.str.lower(),
        )
    )

    tfr_dfs["sales_eia861"] = transformed_sales
    return tfr_dfs


def advanced_metering_infrastructure(tfr_dfs):
    """
    Transform the EIA 861 Advanced Metering Infrastructure table.

    Args:
        tfr_dfs (dict): A dictionary of transformed EIA 861 DataFrames, keyed by table
            name. It will be mutated by this function.

    Returns:
        dict: A dictionary of transformed EIA 861 dataframes, keyed by table name.

    """
    idx_cols = [
        "utility_id_eia",
        "state",
        "balancing_authority_code_eia",
        "report_date",
    ]

    raw_ami = tfr_dfs["advanced_metering_infrastructure_eia861"].copy()

    ###########################################################################
    # Tidy Data:
    ###########################################################################

    logger.info("Tidying the EIA 861 Advanced Metering Infrastructure table.")
    tidy_ami, idx_cols = _tidy_class_dfs(
        raw_ami,
        df_name='Advanced Metering Infrastructure',
        idx_cols=idx_cols,
        class_list=pc.CUSTOMER_CLASSES,
        class_type='customer_class',
    )

    # No duplicates to speak of but take measures to check just in case
    _check_for_dupes(tidy_ami, 'Advanced Metering Infrastructure', idx_cols)

    tfr_dfs["advanced_metering_infrastructure_eia861"] = tidy_ami
    return tfr_dfs


def demand_response(tfr_dfs):
    """
    Transform the EIA 861 Demand Response table.

    Args:
        extract step. tfr_dfs (dict): A dictionary of transformed EIA 861
        DataFrames, keyed by table name. It will be mutated by this function.

    Returns:
        dict: A dictionary of transformed EIA 861 dataframes, keyed by table
            name.
    """
    idx_cols = [
        "utility_id_eia",
        "state",
        "balancing_authority_code_eia",
        "report_date",
    ]

    raw_dr = tfr_dfs["demand_response_eia861"].copy()
    # fill na BA values with 'UNK'
    # raw_dr['balancing_authority_code_eia'] = (
    #     raw_dr['balancing_authority_code_eia'].fillna('UNK')
    # )

    ###########################################################################
    # Tidy Data:
    ###########################################################################

    logger.info("Tidying the EIA 861 Demand Response table.")
    tidy_dr, idx_cols = _tidy_class_dfs(
        raw_dr,
        df_name='Demand Response',
        idx_cols=idx_cols,
        class_list=pc.CUSTOMER_CLASSES,
        class_type='customer_class',
    )

    # shouldn't be duplicates but there are some strange values from IN.
    # these values have Nan BA values and should be deleted earlier.
    # thinking this might have to do with DR table weirdness between 2012 and 2013
    # will come back to this after working on the DSM table. Dropping dupes for now.
    deduped_dr = _drop_dupes(tidy_dr, idx_cols)

    ###########################################################################
    # Transform Values:
    # * Turn 1000s of dollars back into dollars
    ###########################################################################
    logger.info(
        "Performing value transformations on EIA 861 Demand Response table.")
    transformed_dr = (
        deduped_dr.assign(
            customer_incentives_cost=lambda x: x.customer_incentives_cost * 1000.0,
            other_costs=lambda x: x.other_costs * 1000.0
        )
    )

    tfr_dfs["demand_response_eia861"] = transformed_dr
    return tfr_dfs


def demand_side_management(tfr_dfs):
    """
    Transform the EIA 861 Demand Side Management table.

    Args:
        tfr_dfs (dict): A dictionary of transformed EIA 861 DataFrames, keyed by table
            name. It will be mutated by this function.

    Returns:
        dict: A dictionary of transformed EIA 861 dataframes, keyed by table name.

    """
    return tfr_dfs


def distributed_generation(tfr_dfs):
    """
    Transform the EIA 861 Distributed Generation table.

    Args:
        tfr_dfs (dict): A dictionary of transformed EIA 861 DataFrames, keyed by table
            name. It will be mutated by this function.

    Returns:
        dict: A dictionary of transformed EIA 861 dataframes, keyed by table name.

    """
    return tfr_dfs


def distribution_systems(tfr_dfs):
    """
    Transform the EIA 861 Distribution Systems table.

    Args:
        tfr_dfs (dict): A dictionary of transformed EIA 861 DataFrames, keyed by table
            name. It will be mutated by this function.

    Returns:
        dict: A dictionary of transformed EIA 861 dataframes, keyed by table name.

    """
    # No data tidying or transformation required

    raw_ds = (
        tfr_dfs['distribution_systems_eia861'].copy()
    )

    # No duplicates to speak of but take measures to check just in case
    _check_for_dupes(raw_ds, 'Distribution Systems', [
                     "utility_id_eia", "state", "report_date"])

    tfr_dfs["distribution_systems_eia861"] = raw_ds
    return tfr_dfs


def dynamic_pricing(tfr_dfs):
    """
    Transform the EIA 861 Dynamic Pricing table.

    Args:
        tfr_dfs (dict): A dictionary of transformed EIA 861 DataFrames, keyed by table
            name. It will be mutated by this function.

    Returns:
        dict: A dictionary of transformed EIA 861 dataframes, keyed by table name.

    """
    idx_cols = [
        "utility_id_eia",
        "state",
        "balancing_authority_code_eia",
        "report_date",
    ]

    class_attributes = [
        'critical_peak_pricing',
        'critical_peak_rebate',
        'real_time_pricing_program',
        'time_of_use_pricing_program',
        'variable_peak_pricing_program'
    ]

    raw_dp = tfr_dfs["dynamic_pricing_eia861"].copy()

    ###########################################################################
    # Tidy Data:
    ###########################################################################

    logger.info("Tidying the EIA 861 Dynamic Pricing table.")
    tidy_dp, idx_cols = _tidy_class_dfs(
        raw_dp,
        df_name='Dynamic Pricing',
        idx_cols=idx_cols,
        class_list=pc.CUSTOMER_CLASSES,
        class_type='customer_class',
    )

    # No duplicates to speak of but take measures to check just in case
    _check_for_dupes(tidy_dp, 'Dynamic Pricing', idx_cols)

    ###########################################################################
    # Transform Values:
    # * Make Y/N's into booleans and X values into pd.NA
    ###########################################################################

    logger.info(
        "Performing value transformations on EIA 861 Dynamic Pricing table.")
    for col in class_attributes:
        tidy_dp[col] = (
            tidy_dp[col].replace({'Y': True, 'N': False})
            .apply(lambda x: x if x in [True, False] else pd.NA)
        )

    tfr_dfs["dynamic_pricing_eia861"] = tidy_dp
    return tfr_dfs


def green_pricing(tfr_dfs):
    """
    Transform the EIA 861 Green Pricing table.

    Args:
        tfr_dfs (dict): A dictionary of transformed EIA 861 DataFrames, keyed by table
            name. It will be mutated by this function.

    Returns:
        dict: A dictionary of transformed EIA 861 dataframes, keyed by table name.

    """
    idx_cols = [
        "utility_id_eia",
        "state",
        "report_date",
    ]

    raw_gp = tfr_dfs["green_pricing_eia861"].copy()

    ###########################################################################
    # Tidy Data:
    ###########################################################################

    logger.info("Tidying the EIA 861 Green Pricing table.")
    tidy_gp, idx_cols = _tidy_class_dfs(
        raw_gp,
        df_name='Green Pricing',
        idx_cols=idx_cols,
        class_list=pc.CUSTOMER_CLASSES,
        class_type='customer_class',
    )

    _check_for_dupes(tidy_gp, 'Green Pricing', idx_cols)

    ###########################################################################
    # Transform Values:
    # * Turn 1000s of dollars back into dollars
    ###########################################################################
    logger.info(
        "Performing value transformations on EIA 861 Green Pricing table.")
    transformed_gp = (
        tidy_gp.assign(
            green_pricing_revenue=lambda x: x.green_pricing_revenue * 1000.0,
            rec_revenue=lambda x: x.rec_revenue * 1000.0
        )
    )

    tfr_dfs["green_pricing_eia861"] = transformed_gp

    return tfr_dfs


def mergers(tfr_dfs):
    """
    Transform the EIA 861 Mergers table.

    Args:
        tfr_dfs (dict): A dictionary of transformed EIA 861 DataFrames, keyed by table
            name. It will be mutated by this function.

    Returns:
        dict: A dictionary of transformed EIA 861 dataframes, keyed by table name.

    """
    raw_mergers = tfr_dfs["mergers_eia861"].copy()

    # No data tidying required

    ###########################################################################
    # Transform Values:
    # * Turn ownership column from single-letter code to full ownership category.
    # * Retain preceeding zeros in zip codes
    ###########################################################################

    transformed_mergers = (
        raw_mergers.assign(
            entity_type=lambda x: x.entity_type.map(pc.ENTITY_TYPE_DICT),
            merge_zip_5=lambda x: pudl.helpers.zero_pad_zips(x.merge_zip_5, 5),
            merge_zip_4=lambda x: pudl.helpers.zero_pad_zips(x.merge_zip_4, 4)
        )
    )

    # No duplicates to speak of but take measures to check just in case
    _check_for_dupes(transformed_mergers, 'Mergers', [
                     "utility_id_eia", "state", "report_date"])

    tfr_dfs["mergers_eia861"] = transformed_mergers
    return tfr_dfs


def net_metering(tfr_dfs):
    """
    Transform the EIA 861 Net Metering table.

    Args:
        tfr_dfs (dict): A dictionary of transformed EIA 861 DataFrames, keyed by table
            name. It will be mutated by this function.

    Returns:
        dict: A dictionary of transformed EIA 861 dataframes, keyed by table name.

    """
    idx_cols = [
        'utility_id_eia',
        'state',
        'balancing_authority_code_eia',
        'report_date',
    ]

    misc_cols = [
        'pv_current_flow_type'
    ]

    # Pre-tidy clean specific to net_metering table
    raw_nm = (
        tfr_dfs["net_metering_eia861"].copy()
        .query("utility_id_eia not in '99999'")
    )

    # Separate customer class data from misc data (in this case just one col: current flow)
    # Could easily add this to tech_class if desired.
    raw_nm_customer_fuel_class = (
        raw_nm.drop(misc_cols, axis=1).copy())
    raw_nm_misc = raw_nm[idx_cols + misc_cols].copy()

    # Check for duplicates before idx cols get changed
    _check_for_dupes(
        raw_nm_misc, 'Net Metering Current Flow Type PV', idx_cols)

    ###########################################################################
    # Tidy Data:
    ###########################################################################

    logger.info("Tidying the EIA 861 Net Metering table.")
    # Normalize by customer class (must be done before normalizing by fuel class)
    tidy_nm_customer_class, idx_cols = _tidy_class_dfs(
        raw_nm_customer_fuel_class,
        df_name='Net Metering',
        idx_cols=idx_cols,
        class_list=pc.CUSTOMER_CLASSES,
        class_type='customer_class',
    )

    # Normalize by fuel class
    tidy_nm_customer_fuel_class, idx_cols = _tidy_class_dfs(
        tidy_nm_customer_class,
        df_name='Net Metering',
        idx_cols=idx_cols,
        class_list=pc.TECH_CLASSES,
        class_type='tech_class',
        keep_totals=True,
    )

    # No duplicates to speak of but take measures to check just in case
    _check_for_dupes(
        tidy_nm_customer_fuel_class, 'Net Metering Customer & Fuel Class', idx_cols)

    # No transformation needed

    # Drop original net_metering_eia861 table from tfr_dfs
    del tfr_dfs['net_metering_eia861']

    tfr_dfs["net_metering_customer_fuel_class_eia861"] = tidy_nm_customer_fuel_class
    tfr_dfs["net_metering_misc_eia861"] = raw_nm_misc

    return tfr_dfs


def non_net_metering(tfr_dfs):
    """
    Transform the EIA 861 Non-Net Metering table.

    Args:
        tfr_dfs (dict): A dictionary of transformed EIA 861 DataFrames, keyed by table
            name. It will be mutated by this function.

    Returns:
        dict: A dictionary of transformed EIA 861 dataframes, keyed by table name.

    """
    idx_cols = [
        'utility_id_eia',
        'state',
        'balancing_authority_code_eia',
        'report_date',
    ]

    misc_cols = [
        'backup_capacity_mw',
        'generators_number',
        'pv_current_flow_type',
        'utility_owned_capacity_mw'
    ]

    # Pre-tidy clean specific to non_net_metering table
    raw_nnm = (
        tfr_dfs["non_net_metering_eia861"].copy()
        .query("utility_id_eia not in '99999'")
    )

    # there are ~80 fully duplicate records in the 2018 table. We need to
    # remove those duplicates
    og_len = len(raw_nnm)
    raw_nnm = raw_nnm.drop_duplicates(keep='first')
    diff_len = og_len - len(raw_nnm)
    if diff_len > 100:
        raise ValueError(
            f"""Too many duplicate dropped records in raw non-net metering
    table: {diff_len}""")

    # Separate customer class data from misc data
    raw_nnm_customer_fuel_class = raw_nnm.drop(misc_cols, axis=1).copy()
    raw_nnm_misc = (raw_nnm[idx_cols + misc_cols]).copy()

    # Check for duplicates before idx cols get changed
    _check_for_dupes(
        raw_nnm_misc, 'Non Net Metering Misc.', idx_cols)

    ###########################################################################
    # Tidy Data:
    ###########################################################################

    logger.info("Tidying the EIA 861 Non Net Metering table.")

    # Normalize by customer class (must be done before normalizing by fuel class)
    tidy_nnm_customer_class, idx_cols = _tidy_class_dfs(
        raw_nnm_customer_fuel_class,
        df_name='Non Net Metering',
        idx_cols=idx_cols,
        class_list=pc.CUSTOMER_CLASSES,
        class_type='customer_class',
        keep_totals=True
    )

    # Normalize by fuel class
    tidy_nnm_customer_fuel_class, idx_cols = _tidy_class_dfs(
        tidy_nnm_customer_class,
        df_name='Non Net Metering',
        idx_cols=idx_cols,
        class_list=pc.TECH_CLASSES,
        class_type='tech_class',
        keep_totals=True
    )

    # No duplicates to speak of (deleted 2018 duplicates above) but take measures to check just in case
    _check_for_dupes(
        tidy_nnm_customer_fuel_class, 'Non Net Metering Customer & Fuel Class', idx_cols)

    # Delete total_capacity_mw col for redundancy (it doesn't matter which one)
    tidy_nnm_customer_fuel_class = (
        tidy_nnm_customer_fuel_class.drop(columns='capacity_mw_y')
        .rename(columns={'capacity_mw_x': 'capacity_mw'})
    )

    # No transformation needed

    # Drop original net_metering_eia861 table from tfr_dfs
    del tfr_dfs['non_net_metering_eia861']

    tfr_dfs["non_net_metering_customer_fuel_class_eia861"] = tidy_nnm_customer_fuel_class
    tfr_dfs["non_net_metering_misc_eia861"] = raw_nnm_misc

    return tfr_dfs


def operational_data(tfr_dfs):
    """
    Transform the EIA 861 Operational Data table.

    Args:
        tfr_dfs (dict): A dictionary of transformed EIA 861 DataFrames, keyed by table
            name. It will be mutated by this function.

    Returns:
        dict: A dictionary of transformed EIA 861 dataframes, keyed by table name.

    """
    idx_cols = [
        'utility_id_eia',
        'state',
        'nerc_region',
        'report_date',
    ]

    # Pre-tidy clean specific to operational data table
    raw_od = tfr_dfs["operational_data_eia861"].copy()
    raw_od = (
        raw_od[(raw_od['utility_id_eia'].notnull()) &
               (raw_od['utility_id_eia'] != 88888)]
    )

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

    transformed_od = (
        _clean_nerc(raw_od, idx_cols)
        .assign(
            data_observed=lambda x: x.data_observed.replace({
                "O": True,
                "I": False}))
    )

    # Split data into 2 tables:
    #  * Revenue (wide-to-tall)
    #  * Misc. (other)
    revenue_cols = [col for col in transformed_od if 'revenue' in col]
    transformed_od_misc = (transformed_od.drop(revenue_cols, axis=1))
    transformed_od_rev = (transformed_od[idx_cols + revenue_cols].copy())

    # Wide-to-tall revenue columns
    tidy_od_rev, idx_cols = (
        _tidy_class_dfs(
            transformed_od_rev,
            df_name='Operational Data Revenue',
            idx_cols=idx_cols,
            class_list=pc.REVENUE_CLASSES,
            class_type='revenue_class'
        )
    )

    ###########################################################################
    # Transform Data Round 2:
    # * Turn 1000s of dollars back into dollars
    ###########################################################################

    # Transform revenue 1000s into dollars
    transformed_od_rev = (
        tidy_od_rev.assign(revenue=lambda x: x.revenue * 1000)
    )

    # Drop original operational_data_eia861 table from tfr_dfs
    del tfr_dfs['operational_data_eia861']

    tfr_dfs["operational_data_revenue_eia861"] = transformed_od_rev
    tfr_dfs["operational_data_misc_eia861"] = transformed_od_misc

    return tfr_dfs


def reliability(tfr_dfs):
    """
    Transform the EIA 861 Reliability table.

    Args:
        tfr_dfs (dict): A dictionary of transformed EIA 861 DataFrames, keyed by table
            name. It will be mutated by this function.

    Returns:
        dict: A dictionary of transformed EIA 861 dataframes, keyed by table name.

    """
    idx_cols = [
        'utility_id_eia',
        'state',
        'report_date'
    ]

    # Pre-tidy clean specific to operational data table
    raw_r = tfr_dfs["reliability_eia861"].copy()

    ###########################################################################
    # Tidy Data:
    ###########################################################################

    logger.info("Tidying the EIA 861 Reliability table.")

    # Normalize by standards
    tidy_r, idx_cols = _tidy_class_dfs(
        df=raw_r,
        df_name='Reliability',
        idx_cols=idx_cols,
        class_list=pc.RELIABILITY_STANDARDS,
        class_type='reliability_standard',
        keep_totals=False,
    )

    ###########################################################################
    # Transform Data:
    # * Re-code outages_recorded_automatically and inactive_accounts_included to boolean:
    #   * Y/y="Yes" => True
    #   * N/n="No" => False
    ###########################################################################

    transformed_r = (
        tidy_r.assign(
            outages_recorded_automatically=lambda x: (
                x.outages_recorded_automatically.str.upper().replace({
                    'Y': True,
                    'N': False})),
            inactive_accounts_included=lambda x: (
                x.inactive_accounts_included.replace({
                    'Y': True,
                    'N': False})),
            momentary_interruption_definition=lambda x: (
                x.momentary_interruption_definition.map(
                    pc.MOMENTARY_INTERRUPTION_DEF))
        )
    )

    tfr_dfs["reliability_eia861"] = transformed_r

    return tfr_dfs


def utility_data(tfr_dfs):
    """
    Transform the EIA 861 Utility Data table.

    Args:
        tfr_dfs (dict): A dictionary of transformed EIA 861 DataFrames, keyed by table
            name. It will be mutated by this function.

    Returns:
        dict: A dictionary of transformed EIA 861 dataframes, keyed by table name.

    """
    idx_cols = [
        'utility_id_eia',
        'state',
        'report_date',
        'nerc_region'
    ]

    # Pre-tidy clean specific to operational data table
    raw_ud = (
        tfr_dfs["utility_data_eia861"].copy()
        .query("utility_id_eia not in [88888]")

    )

    ###########################################################################
    # Transform Data Round 1:
    # * Clean NERC region col
    ###########################################################################

    transformed_ud = _clean_nerc(raw_ud, idx_cols)

    # Establish columns that are nerc regions vs. rtos
    nerc_cols = [col for col in raw_ud if 'nerc_region_operation' in col]
    rto_cols = [col for col in raw_ud if 'rto_operation' in col]

    # Make separate tables for nerc vs. rto vs. misc data
    raw_ud_nerc = transformed_ud[idx_cols + nerc_cols].copy()
    raw_ud_rto = transformed_ud[idx_cols + rto_cols].copy()
    raw_ud_misc = transformed_ud.drop(nerc_cols + rto_cols, axis=1).copy()

    ###########################################################################
    # Tidy Data:
    ###########################################################################

    logger.info("Tidying the EIA 861 Utility Data tables.")

    tidy_ud_nerc, nerc_idx_cols = _tidy_class_dfs(
        df=raw_ud_nerc,
        df_name='Utility Data NERC Regions',
        idx_cols=idx_cols,
        class_list=NERC_CLASS,
        class_type='nerc_regions_of_operation',
    )

    tidy_ud_rto, rto_idx_cols = _tidy_class_dfs(
        df=raw_ud_rto,
        df_name='Utility Data RTOs',
        idx_cols=idx_cols,
        class_list=RTO_CLASS,
        class_type='rtos_of_operation'
    )

    ###########################################################################
    # Transform Data Round 2:
    # * Re-code operating_in_XX to boolean:
    #   * Y = "Yes" => True
    #   * N = "No" => False
    #   * Blank => False
    ###########################################################################

    # Transform NERC region table
    transformed_ud_nerc = (
        tidy_ud_nerc.assign(
            nerc_region_operation=lambda x: (
                x.nerc_region_operation
                .fillna(False)
                .replace({"N": False, "Y": True})),
            nerc_regions_of_operation=lambda x: (
                x.nerc_regions_of_operation.str.upper()
            )
        )
    )

    # Only keep true values and drop bool col
    transformed_ud_nerc = (
        transformed_ud_nerc[transformed_ud_nerc.nerc_region_operation]
        .drop(['nerc_region_operation'], axis=1)
    )

    # Transform RTO table
    transformed_ud_rto = (
        tidy_ud_rto.assign(
            rto_operation=lambda x: (
                x.rto_operation
                .fillna(False)
                .replace({"N": False, "Y": True})),
            rtos_of_operation=lambda x: (
                x.rtos_of_operation.str.upper()
            )
        )
    )

    # Only keep true values and drop bool col
    transformed_ud_rto = (
        transformed_ud_rto[transformed_ud_rto.rto_operation]
        .drop(['rto_operation'], axis=1)
    )

    # Transform MISC table by first separating bool cols from non bool cols
    # and then making them into boolean values.
    transformed_ud_misc_bool = (
        raw_ud_misc
        .drop(['entity_type', 'utility_name_eia'], axis=1)
        .set_index(idx_cols)
        .fillna(False)
        .replace({"N": False, "Y": True})
    )

    # Merge misc. bool cols back together with misc. non bool cols
    transformed_ud_misc = (
        pd.merge(
            raw_ud_misc[idx_cols + ['entity_type', 'utility_name_eia']],
            transformed_ud_misc_bool,
            on=idx_cols,
            how='outer'
        )
    )

    # Drop original operational_data_eia861 table from tfr_dfs
    del tfr_dfs['utility_data_eia861']

    tfr_dfs["utility_data_nerc_eia861"] = transformed_ud_nerc
    tfr_dfs["utility_data_rto_eia861"] = transformed_ud_rto
    tfr_dfs["utility_data_misc_eia861"] = transformed_ud_misc

    return tfr_dfs


##############################################################################
# Coordinating Transform Function
##############################################################################

def transform(raw_dfs, eia861_tables=pc.pudl_tables["eia861"]):
    """
    Transform EIA 861 DataFrames.

    Args:
        raw_dfs (dict): a dictionary of tab names (keys) and DataFrames
            (values). This can be generated by pudl.
        eia861_tables (tuple): A tuple containing the names of the EIA 861
            tables that can be pulled into PUDL

    Returns:
        dict: A dictionary of DataFrame objects in which pages from EIA 861 form
        (keys) corresponds to a normalized DataFrame of values from that page
        (values)

    """
    # these are the tables that we have transform functions for...
    tfr_funcs = {
        "balancing_authority_eia861": balancing_authority,
        "service_territory_eia861": service_territory,
        "sales_eia861": sales,
        "advanced_metering_infrastructure_eia861": advanced_metering_infrastructure,
        "demand_response_eia861": demand_response,
        "distribution_systems_eia861": distribution_systems,
        "dynamic_pricing_eia861": dynamic_pricing,
        "green_pricing_eia861": green_pricing,
        "mergers_eia861": mergers,
        "net_metering_eia861": net_metering,
        "non_net_metering_eia861": non_net_metering,
        "operational_data_eia861": operational_data,
        "reliability_eia861": reliability,
        # "demand_side_management_eia861": demand_side_management,
        # "distributed_generation_eia861": distributed_generation,
        "utility_data_eia861": utility_data,
    }

    # Dictionary for transformed dataframes and pre-transformed dataframes.
    # Pre-transformed dataframes may be split into two or more output dataframes.
    tfr_dfs = {}

    if not raw_dfs:
        logger.info(
            "No raw EIA 861 dataframes found. Not transforming EIA 861.")
        return tfr_dfs
    # for each of the tables, run the respective transform funtction
    for table in eia861_tables:
        if table not in tfr_funcs.keys():
            raise ValueError(f"Unrecognized EIA 861 table: {table}")
        logger.info(f"Transforming raw EIA 861 DataFrames for {table} "
                    f"concatenated across all years.")
        tfr_dfs[table] = _early_transform(raw_dfs[table])
        tfr_dfs = tfr_funcs[table](tfr_dfs)

    # This is more like harvesting stuff, and should probably be relocated:
    tfr_dfs = balancing_authority_assn(tfr_dfs)
    tfr_dfs = utility_assn(tfr_dfs)
    tfr_dfs = normalize_balancing_authority(tfr_dfs)
    tfr_dfs = pudl.helpers.convert_dfs_dict_dtypes(tfr_dfs, 'eia')
    return tfr_dfs
