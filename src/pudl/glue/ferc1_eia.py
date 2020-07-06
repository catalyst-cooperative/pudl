"""
Extract and transform glue tables between FERC Form 1 and EIA 860/923.

FERC1 and EIA report on many of the same plants and utilities, but have no
embedded connection. We have combed through the FERC and EIA plants and
utilities to generate id's which can connect these datasets. The resulting
fields in the PUDL tables are `plant_id_pudl` and `utility_id_pudl`,
respectively. This was done by hand in a spreadsheet which is in the
`package_data/glue` directory. When mapping plants, we considered a plant a
co-located collection of electricity generation equipment. If a coal plant was
converted to a natural gas unit, our aim was to consider this the same plant.
This module simply reads in the mapping spreadsheet and converts it to a
dictionary of dataframes.

Because these mappings were done by hand and for every one of FERC Form 1's
thousands of reported plants, we know there are probably some incorrect or
incomplete mappings. If you see a `plant_id_pudl` or `utility_id_pudl` mapping
that you think is incorrect, please open an issue on our Github!

Note that the PUDL IDs may change over time. They are not guaranteed to be
stable. If you need to find a particular plant or utility reliably, you should
use its plant_id_eia, utility_id_eia, or utility_id_ferc1.

Another note about these id's: these id's map our definition of plants, which
is not the most granular level of plant unit. The generators are typically the
smaller, more interesting unit. FERC does not typically report in units
(although it sometimes does), but it does often break up gas units from coal
units. EIA reports on the generator and boiler level. When trying to use these
PUDL id's, consider the granularity that you desire and the potential
implications of using a co-located set of plant infrastructure as an id.

"""
import importlib
import logging

import pandas as pd
import sqlalchemy as sa

import pudl
from pudl import constants as pc

logger = logging.getLogger(__name__)


def get_plant_map():
    """Read in the manual FERC to EIA plant mapping data."""
    map_eia_ferc_file = importlib.resources.open_binary(
        'pudl.package_data.glue', 'mapping_eia923_ferc1.xlsx')

    return pd.read_excel(
        map_eia_ferc_file, 'plants_output',
        na_values='', keep_default_na=False,
        converters={'plant_id_pudl': int,
                    'plant_name_pudl': str,
                    'utility_id_ferc1': int,
                    'utility_name_ferc1': str,
                    'plant_name_ferc1': str,
                    'plant_id_eia': int,
                    'plant_name_eia': str,
                    'utility_name_eia': str,
                    'utility_id_eia': int})


def get_utility_map():
    """Read in the manual FERC to EIA utility mapping data."""
    map_eia_ferc_file = importlib.resources.open_binary(
        'pudl.package_data.glue', 'mapping_eia923_ferc1.xlsx')

    return pd.read_excel(map_eia_ferc_file, 'utilities_output',
                         na_values='', keep_default_na=False,
                         converters={'utility_id_pudl': int,
                                     'utility_name_pudl': str,
                                     'utility_id_ferc1': int,
                                     'utility_name_ferc1': str,
                                     'utility_id_eia': int,
                                     'utility_name_eia': str})


def get_db_plants_ferc1(pudl_settings, years):
    """
    Pull a dataframe of all plants in the FERC Form 1 DB for the given years.

    This function looks in the f1_steam, f1_gnrt_plant, f1_hydro and
    f1_pumped_storage tables, and generates a dataframe containing every unique
    combination of respondent_id (utility_id_ferc1) and plant_name is finds.
    Also included is the capacity of the plant in MW (as reported in the
    raw FERC Form 1 DB), the respondent_name (utility_name_ferc1) and a column
    indicating which of the plant tables the record came from.  Plant and
    utility names are translated to lowercase, with leading and trailing
    whitespace stripped and repeating internal whitespace compacted to a single
    space.

    This function is primarily meant for use generating inputs into the manual
    mapping of FERC to EIA plants with PUDL IDs.

    Args:
        pudl_settings (dict): Dictionary containing various paths and database
            URLs used by PUDL.
        years (iterable): Years for which plants should be compiled.

    Returns:
        pandas.DataFrame: A dataframe containing columns
        utility_id_ferc1, utility_name_ferc1, plant_name, capacity_mw, and
        plant_table. Each row is a unique combination of utility_id_ferc1 and
        plant_name.

    """
    # Need to be able to use years outside the "valid" range if we're trying
    # to get new plant ID info...
    for yr in years:
        if yr not in pc.data_years['ferc1']:
            raise ValueError(
                f"Input year {yr} is not available in the FERC data.")

    # Grab the FERC 1 DB metadata so we can query against the DB w/ SQLAlchemy:
    ferc1_engine = sa.create_engine(pudl_settings["ferc1_db"])
    ferc1_meta = sa.MetaData(bind=ferc1_engine)
    ferc1_meta.reflect()
    ferc1_tables = ferc1_meta.tables

    # This table contains the utility names and IDs:
    respondent_table = ferc1_tables['f1_respondent_id']
    # These are all the tables we're gathering "plants" from:
    plant_tables = ['f1_steam', 'f1_gnrt_plant',
                    'f1_hydro', 'f1_pumped_storage']
    # FERC doesn't use the sme column names for the same values across all of
    # Their tables... but all of these are cpacity in MW.
    capacity_cols = {'f1_steam': 'tot_capacity',
                     'f1_gnrt_plant': 'capacity_rating',
                     'f1_hydro': 'tot_capacity',
                     'f1_pumped_storage': 'tot_capacity'}

    # Generate a list of all combinations of utility ID, utility name, and
    # plant name that currently exist inside the raw FERC Form 1 Database, by
    # iterating over the tables that contain "plants" and grabbing those
    # columns (along with their capacity, since that's useful for matching
    # purposes)
    all_plants = pd.DataFrame()
    for tbl in plant_tables:
        plant_select = sa.sql.select([
            ferc1_tables[tbl].c.respondent_id,
            ferc1_tables[tbl].c.plant_name,
            ferc1_tables[tbl].columns[capacity_cols[tbl]],
            respondent_table.c.respondent_name
        ]).distinct().where(
            sa.and_(
                ferc1_tables[tbl].c.respondent_id == respondent_table.c.respondent_id,
                ferc1_tables[tbl].c.plant_name != '',
                ferc1_tables[tbl].c.report_year.in_(years)
            )
        )
        # Add all the plants from the current table to our bigger list:
        all_plants = all_plants.append(
            pd.read_sql(plant_select, ferc1_engine).
            rename(columns={"respondent_id": "utility_id_ferc1",
                            "respondent_name": "utility_name_ferc1",
                            "plant_name": "plant_name_ferc1",
                            capacity_cols[tbl]: "capacity_mw"}).
            pipe(pudl.helpers.strip_lower, columns=["plant_name_ferc1",
                                                    "utility_name_ferc1"]).
            assign(plant_table=tbl).
            loc[:, ["utility_id_ferc1",
                    "utility_name_ferc1",
                    "plant_name_ferc1",
                    "capacity_mw",
                    "plant_table"]]
        )

    # We don't want dupes, and sorting makes the whole thing more readable:
    all_plants = (
        all_plants.drop_duplicates(["utility_id_ferc1", "plant_name_ferc1"]).
        sort_values(["utility_id_ferc1", "plant_name_ferc1"])
    )
    return all_plants


def get_mapped_plants_ferc1():
    """
    Generate a dataframe containing all previously mapped FERC 1 plants.

    Many plants are reported in FERC Form 1 with different versions of the same
    name in different years. Because FERC provides no unique ID for plants,
    these names must be used as part of their identifier. We manually curate a
    list of all the versions of plant names which map to the same actual plant.
    In order to identify new plants each year, we have to compare the new plant
    names and respondent IDs against this raw mapping, not the contents of the
    PUDL data, since within PUDL we use one canonical name for the plant. This
    function pulls that list of various plant names and their corresponding
    utilities (both name and ID) for use in identifying which plants have yet
    to be mapped when we are integrating new data.

    Args:
        None

    Returns:
        pandas.DataFrame A DataFrame with three columns: plant_name,
        utility_id_ferc1, and utility_name_ferc1. Each row represents a unique
        combination of utility_id_ferc1 and plant_name.

    """
    # If we're only trying to get the NEW plants, then we need to see which
    # ones we have already integrated into the PUDL database. However, because
    # FERC doesn't use the same plant names from year to year, we have to rely
    # on the full mapping of FERC plant names to PUDL IDs, which only exists
    # in the ID mapping spreadhseet (the FERC Plant names in the PUDL DB are
    # canonincal names we've chosen to represent all the varied plant names
    # that exist in the raw FERC DB.
    ferc1_mapped_plants = (
        pudl.glue.ferc1_eia.get_plant_map().
        loc[:, ["utility_id_ferc1", "utility_name_ferc1", "plant_name_ferc1"]].
        dropna(subset=["utility_id_ferc1"]).
        pipe(pudl.helpers.strip_lower,
             columns=["utility_id_ferc1",
                      "utility_name_ferc1",
                      "plant_name_ferc1"]).
        astype({"utility_id_ferc1": int}).
        drop_duplicates(["utility_id_ferc1", "plant_name_ferc1"]).
        sort_values(["utility_id_ferc1", "plant_name_ferc1"])
    )
    return ferc1_mapped_plants


def get_mapped_utils_ferc1():
    """
    Read in the list of manually mapped utilities for FERC Form 1.

    Unless a new utility has appeared in the database, this should be identical
    to the full list of utilities available in the FERC Form 1 database.

    Args:
        None

    Returns:
        pandas.DataFrame

    """
    ferc1_mapped_utils = (
        pudl.glue.ferc1_eia.get_utility_map().
        loc[:, ["utility_id_ferc1", "utility_name_ferc1"]].
        dropna(subset=["utility_id_ferc1"]).
        pipe(pudl.helpers.strip_lower,
             columns=["utility_id_ferc1", "utility_name_ferc1"]).
        drop_duplicates("utility_id_ferc1").
        astype({"utility_id_ferc1": int}).
        sort_values(["utility_id_ferc1"])
    )
    return ferc1_mapped_utils


def get_unmapped_plants_ferc1(pudl_settings, years):
    """
    Generate a DataFrame of all unmapped FERC plants in the given years.

    Pulls all plants from the FERC Form 1 DB for the given years, and compares
    that list against the already mapped plants. Any plants found in the
    database but not in the list of mapped plants are returned.

    Args:
        pudl_settings (dict): Dictionary containing various paths and database
            URLs used by PUDL.
        years (iterable): Years for which plants should be compiled from the
            raw FERC Form 1 DB.

    Returns:
        pandas.DataFrame: A dataframe containing five columns:
        utility_id_ferc1, utility_name_ferc1, plant_name, capacity_mw, and
        plant_table. Each row is a unique combination of utility_id_ferc1 and
        plant_name, which appears in the FERC Form 1 DB, but not in the list of
        manually mapped plants.

    """
    db_plants = (
        get_db_plants_ferc1(pudl_settings, years).
        set_index(["utility_id_ferc1", "plant_name_ferc1"])
    )
    mapped_plants = (
        get_mapped_plants_ferc1().
        set_index(["utility_id_ferc1", "plant_name_ferc1"])
    )
    new_plants_index = db_plants.index.difference(mapped_plants.index)
    unmapped_plants = db_plants.loc[new_plants_index].reset_index()
    return unmapped_plants


def get_unmapped_utils_ferc1(pudl_settings, years):
    """
    Generate a list of as-of-yet unmapped utilities from the FERC Form 1 DB.

    Find any utilities which exist in the FERC Form 1 database for the years
    requested, but which do not show up in the mapped plants.  Note that there
    are many more utilities in FERC Form 1 that simply have no plants
    associated with them that will not show up here.

    Args:
        pudl_settings (dict): Dictionary containing various paths and database
            URLs used by PUDL.
        years (iterable): Years for which plants should be compiled from the
            raw FERC Form 1 DB.
    Returns:
        pandas.DataFrame

    """
    # Note: we only map the utlities that have plants associated with them.
    # Grab the list of all utilities listed in the mapped plants:
    mapped_utilities = get_mapped_utils_ferc1().set_index("utility_id_ferc1")
    # Generate a list of all utilities which have unmapped plants:
    # (Since any unmapped utility *must* have unmapped plants)
    utils_with_unmapped_plants = (
        get_unmapped_plants_ferc1(pudl_settings, years).
        loc[:, ["utility_id_ferc1", "utility_name_ferc1"]].
        drop_duplicates("utility_id_ferc1").
        set_index("utility_id_ferc1")
    )
    # Find the indices of all utilities with unmapped plants that do not appear
    # in the list of mapped utilities at all:
    new_utilities_index = (
        utils_with_unmapped_plants.index.
        difference(mapped_utilities.index)
    )
    # Use that index to select only the previously unmapped utilities:
    unmapped_utilities = (
        utils_with_unmapped_plants.
        loc[new_utilities_index].
        reset_index()
    )
    return unmapped_utilities


def get_db_plants_eia(pudl_engine):
    """
    Get a list of all EIA plants appearing in the PUDL DB.

    This list of plants is used to determine which plants need to be added to
    the FERC 1 / EIA plant mappings, where we assign PUDL Plant IDs. Unless a
    new year's worth of data has been added to the PUDL DB, but the plants
    have not yet been mapped, all plants in the PUDL DB should also appear in
    the plant mappings. It only makes sense to run this with a connection to a
    PUDL DB that has all the EIA data in it.

    Args:
        pudl_engine (sqlalchemy.engine.Engine): A database connection
            engine for connecting to a PUDL SQLite database.

    Returns:
        pandas.DataFrame: A DataFrame with plant_id_eia, plant_name_eia, and
        state columns, for addition to the FERC 1 / EIA plant mappings.

    """
    db_plants_eia = (
        pd.read_sql("plants_entity_eia", pudl_engine).
        loc[:, ["plant_id_eia", "plant_name_eia", "state"]].
        pipe(pudl.helpers.strip_lower, columns=["plant_name_eia"]).
        astype({"plant_id_eia": int}).
        drop_duplicates("plant_id_eia").
        sort_values("plant_id_eia")
    )
    return db_plants_eia


def get_mapped_plants_eia():
    """
    Get a list of all EIA plants that have been assigned PUDL Plant IDs.

    Read in the list of already mapped EIA plants from the FERC 1 / EIA plant
    and utility mapping spreadsheet kept in the package_data.

    Args:
        None

    Returns:
        pandas.DataFrame: A DataFrame listing the plant_id_eia and
        plant_name_eia values for every EIA plant which has already been
        assigned a PUDL Plant ID.

    """
    mapped_plants_eia = (
        pudl.glue.ferc1_eia.get_plant_map().
        loc[:, ["plant_id_eia", "plant_name_eia"]].
        dropna(subset=["plant_id_eia"]).
        pipe(pudl.helpers.strip_lower, columns=["plant_name_eia"]).
        astype({"plant_id_eia": int}).
        drop_duplicates("plant_id_eia").
        sort_values("plant_id_eia")
    )
    return mapped_plants_eia


def get_unmapped_plants_eia(pudl_engine):
    """Identify any as-of-yet unmapped EIA Plants."""
    plants_utils_eia = (
        pd.read_sql("""SELECT DISTINCT plant_id_eia, utility_id_eia
                       FROM plants_eia860;""", pudl_engine).
        dropna().
        astype({"plant_id_eia": int,
                "utility_id_eia": int}).
        drop_duplicates().
        # Need to get the name of the utility, to merge with the ID
        merge(get_db_utils_eia(pudl_engine).reset_index(),
              on="utility_id_eia")
    )
    plant_capacity_mw = (
        pd.read_sql("SELECT * FROM generators_eia860;", pudl_engine).
        groupby(["plant_id_eia"])[["capacity_mw"]].agg(sum).
        reset_index()
    )
    db_plants_eia = get_db_plants_eia(pudl_engine).set_index("plant_id_eia")
    mapped_plants_eia = get_mapped_plants_eia().set_index("plant_id_eia")
    unmapped_plants_idx = (
        db_plants_eia.index.
        difference(mapped_plants_eia.index)
    )
    unmapped_plants_eia = (
        db_plants_eia.loc[unmapped_plants_idx].
        merge(plants_utils_eia, how="left", on="plant_id_eia").
        merge(plant_capacity_mw, how="left", on="plant_id_eia").
        loc[:, ["plant_id_eia", "plant_name_eia",
                "utility_id_eia", "utility_name_eia",
                "state", "capacity_mw"]].
        astype({"utility_id_eia": "Int32"})  # Woo! Nullable Integers FTW!
    )

    return unmapped_plants_eia


def get_lost_plants_eia(pudl_engine):
    """Identify any EIA plants which were mapped, but then lost from the DB."""
    mapped_plants_eia = get_mapped_plants_eia().set_index("plant_id_eia")
    db_plants_eia = get_db_plants_eia(pudl_engine).set_index("plant_id_eia")
    lost_plants_idx = mapped_plants_eia.index.difference(db_plants_eia.index)
    lost_plants_eia = mapped_plants_eia.loc[lost_plants_idx]
    return lost_plants_eia


def get_db_utils_eia(pudl_engine):
    """Get a list of all EIA Utilities appearing in the PUDL DB."""
    db_utils_eia = (
        pd.read_sql("utilities_entity_eia", pudl_engine).
        loc[:, ["utility_id_eia", "utility_name_eia"]].
        pipe(pudl.helpers.strip_lower, columns=["utility_name_eia"]).
        astype({"utility_id_eia": int}).
        drop_duplicates("utility_id_eia").
        sort_values("utility_id_eia").
        set_index("utility_id_eia")
    )
    return db_utils_eia


def get_mapped_utils_eia():
    """Get a list of all the EIA Utilities that have PUDL IDs."""
    mapped_utils_eia = (
        pudl.glue.ferc1_eia.get_utility_map().
        loc[:, ["utility_id_eia", "utility_name_eia"]].
        dropna(subset=["utility_id_eia"]).
        pipe(pudl.helpers.strip_lower, columns=["utility_name_eia"]).
        astype({"utility_id_eia": int}).
        drop_duplicates(["utility_id_eia"]).
        sort_values(["utility_id_eia"]).
        set_index("utility_id_eia")
    )
    return mapped_utils_eia


def get_unmapped_utils_eia(pudl_engine):
    """Get a list of all the EIA Utilities in the PUDL DB without PUDL IDs."""
    db_utils_eia = get_db_utils_eia(pudl_engine)
    mapped_utils_eia = get_mapped_utils_eia()
    unmapped_utils_idx = db_utils_eia.index.difference(mapped_utils_eia.index)
    unmapped_utils_eia = db_utils_eia.loc[unmapped_utils_idx]
    return unmapped_utils_eia


def get_unmapped_utils_with_plants_eia(pudl_engine):
    """Get all EIA Utilities that lack PUDL IDs but have plants/ownership."""
    pudl_out = pudl.output.pudltabl.PudlTabl(pudl_engine)

    utils_idx = ["utility_id_eia", "report_date"]
    plants_idx = ["plant_id_eia", "report_date"]
    own_idx = ["plant_id_eia", "generator_id",
               "owner_utility_id_eia", "report_date"]

    utils_eia860 = (
        pudl_out.utils_eia860()
        .dropna(subset=utils_idx)
        .set_index(utils_idx)
    )
    plants_eia860 = (
        pudl_out.plants_eia860()
        .dropna(subset=plants_idx)
        .set_index(plants_idx)
    )
    own_eia860 = (
        pudl_out.own_eia860()
        .dropna(subset=own_idx)
        .set_index(own_idx)
    )

    own_miss_utils = set(
        own_eia860[own_eia860.utility_id_pudl.isnull()]
        .utility_id_eia.unique()
    )
    plants_miss_utils = set(
        plants_eia860[plants_eia860.utility_id_pudl.isnull()]
        .utility_id_eia.unique()
    )

    utils_eia860 = utils_eia860.reset_index()
    miss_utils = utils_eia860[
        (utils_eia860.utility_id_pudl.isna()) &
        (
            (utils_eia860.plants_reported_owner == "True") |
            (utils_eia860.plants_reported_asset_manager == "True") |
            (utils_eia860.plants_reported_operator == "True") |
            (utils_eia860.plants_reported_other_relationship == "True") |
            (utils_eia860.utility_id_eia.isin(own_miss_utils)) |
            (utils_eia860.utility_id_eia.isin(plants_miss_utils))
        )
    ]

    miss_utils = (
        miss_utils.drop_duplicates("utility_id_eia")
        .set_index("utility_id_eia")
        .loc[:, ["utility_name_eia"]]
    )
    return miss_utils


def get_lost_utils_eia(pudl_engine):
    """Get a list of all mapped EIA Utilites not found in the PUDL DB."""
    db_utils_eia = get_db_utils_eia(pudl_engine)
    mapped_utils_eia = get_mapped_utils_eia()
    lost_utils_idx = mapped_utils_eia.index.difference(db_utils_eia.index)
    lost_utils_eia = mapped_utils_eia.loc[lost_utils_idx]
    return lost_utils_eia


def glue(ferc1=False, eia=False):
    """Generates a dictionary of dataframes for glue tables between FERC1, EIA.

    That data is primarily stored in the plant_output and
    utility_output tabs of package_data/glue/mapping_eia923_ferc1.xlsx in the
    repository. There are a total of seven relations described in this data:

        - utilities: Unique id and name for each utility for use across the
          PUDL DB.
        - plants: Unique id and name for each plant for use across the PUDL DB.
        - utilities_eia: EIA operator ids and names attached to a PUDL
          utility id.
        - plants_eia: EIA plant ids and names attached to a PUDL plant id.
        - utilities_ferc: FERC respondent ids & names attached to a PUDL
          utility id.
        - plants_ferc: A combination of FERC plant names and respondent ids,
          associated with a PUDL plant ID. This is necessary because FERC does
          not provide plant ids, so the unique plant identifier is a
          combination of the respondent id and plant name.
        - utility_plant_assn: An association table which describes which plants
          have relationships with what utilities. If a record exists in this
          table then combination of PUDL utility id & PUDL plant id does have
          an association of some kind. The nature of that association is
          somewhat fluid, and more scrutiny will likely be required for use in
          analysis.

    Presently, the 'glue' tables are a very basic piece of infrastructure for
    the PUDL DB, because they contain the primary key fields for utilities and
    plants in FERC1.

    Args:
        ferc1 (bool): Are we ingesting FERC Form 1 data?
        eia (bool): Are we ingesting EIA data?

    Returns:
        dict: a dictionary of glue table DataFrames

    """
    # ferc glue tables are structurally entity tables w/ foreign key
    # relationships to ferc datatables, so we need some of the eia/ferc 'glue'
    # even when only ferc is ingested into the database.
    if not ferc1 and not eia:
        return

    # We need to standardize plant names -- same capitalization and no leading
    # or trailing white space... since this field is being used as a key in
    # many cases. This also needs to be done any time plant_name is pulled in
    # from other tables.
    plant_map = (
        get_plant_map().
        pipe(pudl.helpers.strip_lower, ['plant_name_ferc1'])
    )

    plants_pudl = (
        plant_map.
        loc[:, ['plant_id_pudl', 'plant_name_pudl']].
        drop_duplicates('plant_id_pudl')
    )
    plants_eia = (
        plant_map.
        loc[:, ['plant_id_eia', 'plant_name_eia', 'plant_id_pudl']].
        drop_duplicates("plant_id_eia").
        dropna(subset=["plant_id_eia"])
    )
    plants_ferc1 = (
        plant_map.
        loc[:, ['plant_name_ferc1', 'utility_id_ferc1', 'plant_id_pudl']].
        drop_duplicates(['plant_name_ferc1', 'utility_id_ferc1']).
        dropna(subset=["utility_id_ferc1", "plant_name_ferc1"])
    )

    utility_map = get_utility_map()
    utilities_pudl = (
        utility_map.loc[:, ['utility_id_pudl', 'utility_name_pudl']].
        drop_duplicates('utility_id_pudl')
    )
    utilities_eia = (
        utility_map.
        loc[:, ['utility_id_eia', 'utility_name_eia', 'utility_id_pudl']].
        drop_duplicates('utility_id_eia').
        dropna(subset=['utility_id_eia'])
    )
    utilities_ferc1 = (
        utility_map.
        loc[:, ['utility_id_ferc1', 'utility_name_ferc1', 'utility_id_pudl']].
        drop_duplicates('utility_id_ferc1').
        dropna(subset=['utility_id_ferc1'])
    )

    # Now we need to create a table that indicates which plants are associated
    # with every utility.

    # These dataframes map our plant_id to FERC respondents and EIA
    # operators -- the equivalents of our "utilities"
    plants_utilities_ferc1 = (
        plant_map.
        loc[:, ['plant_id_pudl', 'utility_id_ferc1']].
        dropna(subset=['utility_id_ferc1'])
    )
    plants_utilities_eia = (
        plant_map.
        loc[:, ['plant_id_pudl', 'utility_id_eia']].
        dropna(subset=['utility_id_eia'])
    )

    # Here we treat the dataframes like database tables, and join on the
    # FERC respondent_id and EIA operator_id, respectively.
    # Now we can concatenate the two dataframes, and get rid of all the columns
    # except for plant_id and utility_id (which determine the  utility to plant
    # association), and get rid of any duplicates or lingering NaN values...
    utility_plant_assn = (
        pd.concat(
            [pd.merge(utilities_eia,
                      plants_utilities_eia,
                      on='utility_id_eia'),
             pd.merge(utilities_ferc1,
                      plants_utilities_ferc1,
                      on='utility_id_ferc1')],
            sort=True
        )
    )

    utility_plant_assn = (
        utility_plant_assn.
        loc[:, ['plant_id_pudl', 'utility_id_pudl']].
        dropna().
        drop_duplicates()
    )

    # At this point there should be at most one row in each of these data
    # frames with NaN values after we drop_duplicates in each. This is because
    # there will be some plants and utilities that only exist in FERC, or only
    # exist in EIA, and while they will have PUDL IDs, they may not have
    # FERC/EIA info (and it'll get pulled in as NaN)

    for df, df_n in zip(
        [plants_eia, plants_ferc1, utilities_eia, utilities_ferc1],
        ['plants_eia', 'plants_ferc1', 'utilities_eia', 'utilities_ferc1']
    ):
        if df[pd.isnull(df).any(axis=1)].shape[0] > 1:
            raise AssertionError(f"FERC to EIA glue breaking in {df_n}")
        df = df.dropna()

    # Before we start inserting records into the database, let's do some basic
    # sanity checks to ensure that it's (at least kind of) clean.
    # INSERT SANITY HERE

    # Any FERC respondent_id that appears in plants_ferc1 must also exist in
    # utilities_ferc1:
    # INSERT MORE SANITY HERE

    glue_dfs = {
        "plants_pudl": plants_pudl,
        "utilities_pudl": utilities_pudl,
        "plants_ferc1": plants_ferc1,
        "utilities_ferc1": utilities_ferc1,
        "plants_eia": plants_eia,
        "utilities_eia": utilities_eia,
        "utility_plant_assn": utility_plant_assn,
    }

    # if we're not ingesting eia, exclude eia only tables
    if not eia:
        del glue_dfs['utilities_eia']
        del glue_dfs['plants_eia']
    # if we're not ingesting ferc, exclude ferc1 only tables
    if not ferc1:
        del glue_dfs['utilities_ferc1']
        del glue_dfs['plants_ferc1']

    return glue_dfs
