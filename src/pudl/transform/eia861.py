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


###############################################################################
# EIA Form 861 Transform Helper functions
###############################################################################
def _filter_customer_cols(df, customer_classes):
    regex = f"^({'_|'.join(customer_classes)}).*$"
    return df.filter(regex=regex)


def _filter_non_customer_cols(df, customer_classes):
    regex = f"^(?!({'_|'.join(customer_classes)})).*$"
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
        f"Started with {start_nas} missing BA Codes out of {start_len} records ({start_nas/start_len:.2%})")
    ba_ids = (
        df[["balancing_authority_id_eia", "balancing_authority_code_eia", "report_date"]]
        .drop_duplicates()
        .sort_values(["balancing_authority_id_eia", "report_date"])
    )
    ba_ids["ba_code_filled"] = (
        ba_ids.groupby("balancing_authority_id_eia")[
            "balancing_authority_code_eia"]
        .apply(lambda x: x.bfill())
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
        f"Ended with {end_nas} missing BA Codes out of {end_len} records ({end_nas/end_len:.2%})")
    return ba_eia861_filled


###############################################################################
# EIA Form 861 Table Transform Functions
###############################################################################
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
    # No data tidying required
    # All columns are already type compatible
    # Transform values:
    # * Add state and county fips IDs
    # * Convert report_year into report_date
    df = (
        # Ensure that we have the canonical US Census county names:
        pudl.helpers.clean_eia_counties(
            raw_dfs["service_territory_eia861"],
            fixes=EIA_FIPS_COUNTY_FIXES)
        # Add FIPS IDs based on county & state names:
        .pipe(pudl.helpers.add_fips_ids)
        # Convert report year to report date
        .pipe(pudl.helpers.convert_to_date)
    )
    tfr_dfs["service_territory_eia861"] = df
    return tfr_dfs


def balancing_authority(raw_dfs, tfr_dfs):
    """
    Transform the EIA 861 Balancing Authority table.

    Args:
        raw_dfs (dict): A dictionary of raw EIA 861 dataframes, from the extract step.
        tfr_dfs (dict): A dictionary of transformed EIA 861 DataFrames, keyed by table
            name. It will be mutated by this function.

    Returns:
        dict: A dictionary of transformed EIA 861 dataframes, keyed by table name.

    """
    # No data tidying required
    # All columns are already type compatible.
    # Value transformations:
    # * Backfill BA codes on a per BA ID basis
    # * convert report_year into report_date
    # * Fix data entry errors
    df = (
        raw_dfs["balancing_authority_eia861"]
        .pipe(pudl.helpers.convert_to_date)
        .pipe(_ba_code_backfill)
    )
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


def sales(raw_dfs, tfr_dfs):
    """Transform the EIA 861 Sales table."""
    idx_cols = [
        "utility_id_eia",
        "state",
        "report_year",
        "business_model",
        "service_type",
        "balancing_authority_code_eia"
    ]

    ###########################################################################
    # Tidy Data:
    ###########################################################################
    logger.info("Tidying the EIA 861 Sales table.")
    # Clean up values just enough to use primary key columns as a multi-index:
    logger.debug("Cleaning up EIA861 Sales index columns so we can tidy data.")
    raw_sales = (
        raw_dfs["sales_eia861"].copy()
        .assign(balancing_authority_code_eia=lambda x: x.balancing_authority_code_eia.fillna("UNK"))
        .dropna(subset=["utility_id_eia"])
        .query("utility_id_eia not in (88888, 99999)")
        .astype({"utility_id_eia": pd.Int64Dtype()})
        .set_index(idx_cols)
    )
    # Split the table into index, data, and "denormalized" columns for processing:
    # Separate customer classes and reported data into a hierarchical index
    logger.debug("Stacking EIA861 Sales data columns by customer class.")
    data_cols = _filter_customer_cols(raw_sales, pc.CUSTOMER_CLASSES)
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
        raw_sales, pc.CUSTOMER_CLASSES).reset_index()

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
        .pipe(pudl.helpers.convert_to_date)
    )

    # REMOVE: when EIA 861 has been integrated with ETL -- this step
    # should be happening after all of the tables are transformed.
    # transformed_sales = pudl.helpers.convert_cols_dtypes(
    #    transformed_sales, "eia", "sales_eia861")

    tfr_dfs["sales_eia861"] = transformed_sales

    return tfr_dfs


def advanced_metering_infrastructure(raw_dfs, tfr_dfs):
    """
    Transform the EIA 861 Advanced Metering Infrastructure table.

    Args:
        raw_dfs (dict): A dictionary of raw EIA 861 dataframes, from the extract step.
        tfr_dfs (dict): A dictionary of transformed EIA 861 DataFrames, keyed by table
            name. It will be mutated by this function.

    Returns:
        dict: A dictionary of transformed EIA 861 dataframes, keyed by table name.

    """
    return tfr_dfs


def demand_response(raw_dfs, tfr_dfs):
    """
    Transform the EIA 861 Demand Response table.

    Args:
        raw_dfs (dict): A dictionary of raw EIA 861 dataframes, from the
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
        "report_year",
    ]

    ###########################################################################
    # Tidy Data:
    ###########################################################################
    logger.info("Tidying the EIA 861 Demand Response table.")
    # Clean up values just enough to use primary key columns as a multi-index:
    logger.debug("Cleaning up EIA861 Demand Response index columns so we can tidy data.")
    raw_dr = (
        raw_dfs["demand_response_eia861"].copy()
        .assign(balancing_authority_code_eia=lambda x: x.balancing_authority_code_eia.fillna("UNK"))
        .dropna(subset=["utility_id_eia"])
        .query("utility_id_eia not in (88888, 99999)")
        .astype({"utility_id_eia": pd.Int64Dtype()})
        .set_index(idx_cols)
    )
    # Split the table into index, data, and "denormalized" columns for processing:
    # Separate customer classes and reported data into a hierarchical index
    logger.debug("Stacking EIA861 Demand Response data columns by customer class.")
    data_cols = _filter_customer_cols(raw_dr, pc.CUSTOMER_CLASSES)
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
        raw_dr, pc.CUSTOMER_CLASSES).reset_index()

    # Merge the index, data, and denormalized columns back together
    tidy_dr = pd.merge(denorm_cols, data_cols, on=idx_cols)

    # Remove the now redundant "Total" records -- they can be reconstructed
    # from the other customer classes. Note that the utility records sometimes
    # employ rounding so that their reported total values are 1 off from the
    # calculated values. For the vast majority of these values "1" comprises
    # less than 1% of their reported total, making it a menial difference.
    # value columns tend to have 1 or 2 exceptions wherein the percent is
    # between 10 and 33.333.
    tidy_dr = tidy_dr.query("customer_class!='total'")
    tidy_nrows = len(tidy_dr)
    # remove duplicates on the primary key columns (I don't think there are
    # any here, this is just in case.)
    tidy_dr = tidy_dr.drop_duplicates(
        subset=idx_cols + ["customer_class"], keep=False)
    deduped_nrows = len(tidy_dr)
    logger.info(
        f"Dropped {tidy_nrows-deduped_nrows} duplicate records from EIA 861 "
        f"demand response table, out of a total of {tidy_nrows} records "
        f"({(tidy_nrows-deduped_nrows)/tidy_nrows:.4%} of all records). "
    )

    ###########################################################################
    # Set Datatypes:
    # Need to ensure type compatibility before we can do the value based
    # transformations below.
    ###########################################################################
    logger.info("Ensuring raw columns are type compatible.")
    type_compat_dr = pudl.helpers.fix_eia_na(tidy_dr)
    type_compat_dr = pudl.helpers.convert_cols_dtypes(type_compat_dr, 'eia')

    ###########################################################################
    # Transform Values:
    # * Turn 1000s of dollars back into dollars
    # * Replace report_year (int) with report_date (datetime64[ns])
    ###########################################################################
    logger.info("Performing value transformations on EIA 861 Sales table.")
    transformed_dr = (
        type_compat_dr.assign(
            customer_incentives_cost=lambda x: x.customer_incentives_cost * 1000.0,
            other_costs=lambda x: x.other_costs * 1000.0
        )
        .pipe(pudl.helpers.convert_to_date)
    )

    # REMOVE: when EIA 861 has been integrated with ETL -- this step
    # should be happening after all of the tables are transformed.
    # transformed_sales = pudl.helpers.convert_cols_dtypes(
    #    transformed_sales, "eia", "sales_eia861")

    tfr_dfs["demand_response_eia861"] = transformed_dr

    return tfr_dfs


def demand_side_management(raw_dfs, tfr_dfs):
    """
    Transform the EIA 861 Demand Side Management table.

    Args:
        raw_dfs (dict): A dictionary of raw EIA 861 dataframes, from the extract step.
        tfr_dfs (dict): A dictionary of transformed EIA 861 DataFrames, keyed by table
            name. It will be mutated by this function.

    Returns:
        dict: A dictionary of transformed EIA 861 dataframes, keyed by table name.

    """
    return tfr_dfs


def distributed_generation(raw_dfs, tfr_dfs):
    """
    Transform the EIA 861 Distributed Generation table.

    Args:
        raw_dfs (dict): A dictionary of raw EIA 861 dataframes, from the extract step.
        tfr_dfs (dict): A dictionary of transformed EIA 861 DataFrames, keyed by table
            name. It will be mutated by this function.

    Returns:
        dict: A dictionary of transformed EIA 861 dataframes, keyed by table name.

    """
    return tfr_dfs


def distribution_systems(raw_dfs, tfr_dfs):
    """
    Transform the EIA 861 Distribution Systems table.

    Args:
        raw_dfs (dict): A dictionary of raw EIA 861 dataframes, from the extract step.
        tfr_dfs (dict): A dictionary of transformed EIA 861 DataFrames, keyed by table
            name. It will be mutated by this function.

    Returns:
        dict: A dictionary of transformed EIA 861 dataframes, keyed by table name.

    """
    return tfr_dfs


def dynamic_pricing(raw_dfs, tfr_dfs):
    """
    Transform the EIA 861 Dynamic Pricing table.

    Args:
        raw_dfs (dict): A dictionary of raw EIA 861 dataframes, from the extract step.
        tfr_dfs (dict): A dictionary of transformed EIA 861 DataFrames, keyed by table
            name. It will be mutated by this function.

    Returns:
        dict: A dictionary of transformed EIA 861 dataframes, keyed by table name.

    """
    return tfr_dfs


def green_pricing(raw_dfs, tfr_dfs):
    """
    Transform the EIA 861 Green Pricing table.

    Args:
        raw_dfs (dict): A dictionary of raw EIA 861 dataframes, from the extract step.
        tfr_dfs (dict): A dictionary of transformed EIA 861 DataFrames, keyed by table
            name. It will be mutated by this function.

    Returns:
        dict: A dictionary of transformed EIA 861 dataframes, keyed by table name.

    """
    return tfr_dfs


def mergers(raw_dfs, tfr_dfs):
    """
    Transform the EIA 861 Mergers table.

    Args:
        raw_dfs (dict): A dictionary of raw EIA 861 dataframes, from the extract step.
        tfr_dfs (dict): A dictionary of transformed EIA 861 DataFrames, keyed by table
            name. It will be mutated by this function.

    Returns:
        dict: A dictionary of transformed EIA 861 dataframes, keyed by table name.

    """
    return tfr_dfs


def net_metering(raw_dfs, tfr_dfs):
    """
    Transform the EIA 861 Net Metering table.

    Args:
        raw_dfs (dict): A dictionary of raw EIA 861 dataframes, from the extract step.
        tfr_dfs (dict): A dictionary of transformed EIA 861 DataFrames, keyed by table
            name. It will be mutated by this function.

    Returns:
        dict: A dictionary of transformed EIA 861 dataframes, keyed by table name.

    """
    return tfr_dfs


def non_net_metering(raw_dfs, tfr_dfs):
    """
    Transform the EIA 861 Non-Net Metering table.

    Args:
        raw_dfs (dict): A dictionary of raw EIA 861 dataframes, from the extract step.
        tfr_dfs (dict): A dictionary of transformed EIA 861 DataFrames, keyed by table
            name. It will be mutated by this function.

    Returns:
        dict: A dictionary of transformed EIA 861 dataframes, keyed by table name.

    """
    return tfr_dfs


def operational_data(raw_dfs, tfr_dfs):
    """
    Transform the EIA 861 Operational Data table.

    Args:
        raw_dfs (dict): A dictionary of raw EIA 861 dataframes, from the extract step.
        tfr_dfs (dict): A dictionary of transformed EIA 861 DataFrames, keyed by table
            name. It will be mutated by this function.

    Returns:
        dict: A dictionary of transformed EIA 861 dataframes, keyed by table name.

    """
    return tfr_dfs


def reliability(raw_dfs, tfr_dfs):
    """
    Transform the EIA 861 Reliability table.

    Args:
        raw_dfs (dict): A dictionary of raw EIA 861 dataframes, from the extract step.
        tfr_dfs (dict): A dictionary of transformed EIA 861 DataFrames, keyed by table
            name. It will be mutated by this function.

    Returns:
        dict: A dictionary of transformed EIA 861 dataframes, keyed by table name.

    """
    return tfr_dfs


def utility_data(raw_dfs, tfr_dfs):
    """
    Transform the EIA 861 Utility Data table.

    Args:
        raw_dfs (dict): A dictionary of raw EIA 861 dataframes, from the extract step.
        tfr_dfs (dict): A dictionary of transformed EIA 861 DataFrames, keyed by table
            name. It will be mutated by this function.

    Returns:
        dict: A dictionary of transformed EIA 861 dataframes, keyed by table name.

    """
    return tfr_dfs


##############################################################################
# Coordinating Transform Function
##############################################################################

def transform(raw_dfs, eia861_tables=pc.pudl_tables["eia861"]):
    """
    Transforms EIA 861 DataFrames.

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
    eia861_transform_functions = {
        "service_territory_eia861": service_territory,
        "balancing_authority_eia861": balancing_authority,
        "sales_eia861": sales,
        "demand_response_eia861": demand_response,
    }
    tfr_dfs = {}

    if not raw_dfs:
        logger.info("No raw EIA 861 dataframes found. "
                    "Not transforming EIA 861.")
        return tfr_dfs
    # for each of the tables, run the respective transform funtction
    for table in eia861_transform_functions:
        if table in eia861_tables:
            logger.info(f"Transforming raw EIA 861 DataFrames for {table} "
                        f"concatenated across all years.")
            eia861_transform_functions[table](raw_dfs, tfr_dfs)

    return tfr_dfs
