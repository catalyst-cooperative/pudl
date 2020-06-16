"""
Module to perform data cleaning functions on EIA861 data tables.

Inputs to the transform functions are a dictionary of dataframes, each of which
represents a concatenation of records with common column names from across some
set of years of reported data. This raw data is transformed in 3 main steps:

1. Structural transformations that re-shape / tidy the data and turn it into
   rows that represent a single observation, and columns that represent a
   single variable. These transformations do not require knowledge of the
   contents of the data, which may or may not yet be usable, depending on the
   true data type and how much cleaning has to happen. May also involve
   removing duplicate / derived values.

2. Data type compatibility: whatever massaging of the data is required to
   ensure that it can be cast to the appropriate data type, including
   identifying NA values and assigning them to a type-specific NA value.

3. Value based data cleaning: Re-coding freeform strings, calculating derived
   values, correction of data entry errors, etc.

After those transformation, additional processsing that involves relationships
between tables and between data sources takes place -- id / value harvesting
for static entity table generation, the generation of intra and inter-dataset
glue tables, potentially the calculation of more complex derived values.

"""

import logging

import pandas as pd

import pudl
import pudl.constants as pc

logger = logging.getLogger(__name__)

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


def service_territory(raw_dfs, tfr_dfs):
    """Transform the EIA 861 utility service territory table.

    Args:
        raw_dfs (dict): Each entry in this dictionary of DataFrame objects
            corresponds to a page from the EIA861 form, as reported in the
            Excel spreadsheets they distribute.
        tfr_dfs (dict): A dictionary of DataFrame objects in
            which pages from EIA861 form (keys) correspond to normalized
            DataFrames of values from that page (values)

    Returns:
        dict: a dictionary of pandas.DataFrame objects in
        which pages from EIA861 form (keys) correspond to normalized
        DataFrames of values from that page (values)

    """
    df = (
        # Ensure that we have the canonical US Census county names:
        pudl.helpers.clean_eia_counties(
            raw_dfs["service_territory_eia861"],
            fixes=EIA_FIPS_COUNTY_FIXES)
        # Add FIPS IDs based on county & state names:
        .pipe(pudl.helpers.add_fips_ids)
        # Set the final data types for the table:
        .pipe(pudl.helpers.convert_cols_dtypes,
              "eia", "service_territory_eia861")
    )
    tfr_dfs["service_territory_eia861"] = df
    return tfr_dfs


def balancing_authority(raw_dfs, tfr_dfs):
    """
    Transform the EIA 861 Balancing Authority table.

    Args:
        raw_dfs (dict): Each entry in this dictionary of DataFrame objects
            corresponds to a page from the EIA861 form, as reported in the
            Excel spreadsheets they distribute.
        tfr_dfs (dict): A dictionary of DataFrame objects in
            which pages from EIA861 form (keys) correspond to normalized
            DataFrames of values from that page (values)

    Returns:
        dict: a dictionary of pandas.DataFrame objects in
        which pages from EIA861 form (keys) correspond to normalized
        DataFrames of values from that page (values)

    """
    df = (
        raw_dfs["balancing_authority_eia861"]
        .pipe(pudl.helpers.convert_cols_dtypes,
              "eia", "balancing_authority_eia861")
    )
    tfr_dfs["balancing_authority_eia861"] = df
    return tfr_dfs


def transform(raw_dfs, tables=pc.pudl_tables["eia861"]):
    """
    Transforms EIA 860 DataFrames.

    Args:
        raw_dfs (dict): a dictionary of tab names (keys) and DataFrames
            (values). This can be generated by pudl.
        tables (tuple): A tuple containing the names of the EIA 861
            tables that can be pulled into PUDL

    Returns:
        dict: A dictionary of DataFrame objects in which pages from EIA861 form
        (keys) corresponds to a normalized DataFrame of values from that page
        (values)

    """
    # these are the tables that we have transform functions for...
    eia861_transform_functions = {
        'service_territory_eia861': service_territory,
    }
    tfr_dfs = {}

    if not raw_dfs:
        logger.info("No raw EIA 861 dataframes found. "
                    "Not transforming EIA 861.")
        return tfr_dfs
    # for each of the tables, run the respective transform funtction
    for table in eia861_transform_functions:
        if table in tables:
            logger.info(f"Transforming raw EIA 861 DataFrames for {table} "
                        f"concatenated across all years.")
            eia861_transform_functions[table](raw_dfs, tfr_dfs)

    return tfr_dfs
