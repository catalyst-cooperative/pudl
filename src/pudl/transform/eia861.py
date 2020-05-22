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


###############################################################################
# EIA Form 861 Transform Helper functions
###############################################################################
def _filter_customer_cols(df, customer_classes):
    regex = f"^({'_|'.join(customer_classes)}).*$"
    return df.filter(regex=regex)


def _filter_non_customer_cols(df, customer_classes):
    regex = f"^(?!({'_|'.join(customer_classes)})).*$"
    return df.filter(regex=regex)


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


def sales(raw_dfs, tfr_dfs):
    """Transform the EIA 861 Sales table."""
    idx_cols = [
        "utility_id_eia",
        "state",
        "report_year",
        "business_model",
        "service_type",
        "ba_code"
    ]

    customer_classes = [
        "commercial",
        "industrial",
        "other",
        "residential",
        "total",
        "transportation"
    ]

    ###########################################################################
    # Tidy Data:
    ###########################################################################
    logger.info("Tidying the EIA 861 Sales table.")
    # Clean up values just enough to use primary key columns as a multi-index:
    logger.debug("Cleaning up EIA861 Sales index columns so we can tidy data.")
    raw_sales = (
        raw_dfs["sales_eia861"].copy()
        .assign(ba_code=lambda x: x.ba_code.fillna("UNK"))
        .dropna(subset=["utility_id_eia"])
        .query("utility_id_eia not in (88888, 99999)")
        .astype({"utility_id_eia": pd.Int64Dtype()})
        .set_index(idx_cols)
    )
    # Split the table into index, data, and "denormalized" columns for processing:
    # Separate customer classes and reported data into a hierarchical index
    logger.debug("Stacking EIA861 Sales data columns by customer class.")
    data_cols = _filter_customer_cols(raw_sales, customer_classes)
    data_cols.columns = (
        data_cols.columns.str.split("_", n=1, expand=True)
        .set_names(["customer_class", None])
    )
    # Now stack the customer classes into their own categorical column,
    data_cols = (
        data_cols.stack(level=0, dropna=False)
        .reset_index()
    )

    denorm_cols = _filter_non_customer_cols(
        raw_sales, customer_classes).reset_index()

    # Merge the index, data, and denormalized columns back together
    tidy_sales = pd.merge(denorm_cols, data_cols, on=idx_cols)

    # Remove the now redundant "Total" records -- they can be reconstructed
    # from the other customer classes.
    tidy_sales = tidy_sales.query("customer_class!='total'")
    tidy_nrows = len(tidy_sales)
    # remove duplicates on the primary key columns + customer_class -- there
    # are a handful of records, all from 2010-2012, that have reporting errors
    # that produce dupes, which do not have a clear meaning. The utility_id_eia
    # values involved are: [8153, 13830, 17164, 56431, 56434, 56466, 56778,
    # 56976, 56990, 57081, 57411, 57476, 57484, 58300]
    tidy_sales = tidy_sales.drop_duplicates(
        subset=idx_cols + ["customer_class"], keep=False)
    deduped_nrows = len(tidy_sales)
    logger.info(
        f"Dropped {tidy_nrows-deduped_nrows} duplicate records from EIA 861 "
        f"sales table, out of a total of {tidy_nrows} records "
        f"({(tidy_nrows-deduped_nrows)/tidy_nrows:.4%} of all records). "
    )

    ###########################################################################
    # Set Datatypes:
    # Need to ensure type compatibility before we can do the value based
    # transformations below.
    ###########################################################################
    logger.info("Ensuring raw columns are type compatible.")
    type_compat_sales = pudl.helpers.fix_eia_na(tidy_sales)

    ###########################################################################
    # Transform Values:
    # * Turn 1000s of dollars back into dollars
    # * Replace report_year (int) with report_date (datetime64[ns])
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
        type_compat_sales.assign(
            revenues=lambda x: x.revenues * 1000.0,
            report_date=lambda x: pd.to_datetime({
                "year": x.report_year,
                "month": 1,
                "day": 1,
            }),
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
        .drop(["report_year"], axis="columns")
    )

    # REMOVE: when EIA 861 has been integrated with ETL -- this step
    # should be happening after all of the tables are transformed.
    transformed_sales = pudl.helpers.convert_cols_dtypes(
        transformed_sales, "eia", "sales_eia861")

    tfr_dfs["sales_eia861"] = transformed_sales

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
        "service_territory_eia861": service_territory,
        "balancing_authority_eia861": balancing_authority,
        "sales_eia861": sales,
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
