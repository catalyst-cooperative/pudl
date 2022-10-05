#!/usr/bin/env python
"""Identify all unmapped plant and utility IDs in FERC 1 and EIA data.

This script identifies plants and utilities which exist in the FERC 1 and EIA data that
do not yet appear in our manually compiled mapping between the two datasets.

For the FERC 1 plants/utilities it compares the IDs in the spreadsheet to the utility
IDs and plant names that show up in the raw ferc1.sqlite DB.

For the EIA plants/utilities it comapres the IDs in the spreadsheet to the utility and
plant IDs that appear in the PUDL DB.

Before running this script:
===========================
* Load all available years of EIA 860/923 data into your PUDL DB. This includes the new
  year of data to be integrated, so you'll have to do the file, tab, and column mapping
  first, and may need to do a bit of data wrangling to get the transform steps to work.
  You'll probably want to load this data into the PUDL DB without any FERC Form 1 data.
  The script you'll use is ``pudl_etl`` and you'll need to edit the full ETL settings
  file to suit your purpses.
* Load all available years of FERC Form 1 data into your FERC 1 DB. Again, this includes
  the new year of data to be integrated. You'll need to use ``ferc1_to_sqlite`` to do
  this, and you'll want to edit the ``ferc1_to_sqlite`` specific settings in the full
  ETL settings file to include the new year of data, and to use it as the reference
  year for creating the database schema.

If this script sees that all available years of data are not loaded in your databases,
it will raise an AssertionError.

If there is an unexpectedly large number of "lost" EIA plants or utilities (which appear
in the mapping spreadsheet but not the database) it will raise an AssertionError. This
usually indicates that not all of the available data is loaded into the DB.

Script Outputs:
===============
unmapped_utils_ferc1.csv:
    Respondent IDs and respondent names of utilities which appear in the FERC Form 1 DB,
    but which do not appear in the PUDL ID mapping spreadsheet. We should attempt to
    find the corresponding EIA utilities for all of them.

unmapped_plants_ferc1.csv:
    Plant names, respondent names, and respondent IDs associated with plants that appear
    in the FERC Form 1 DB, but which do not appear in the PUDL ID Mapping spreadsheet.
    We should try and find corresponding EIA plants for all of them. This output
    includes large steam plants, as well as small, hydro, and pumped storage plants,
    with the table of origin indicated.

unmapped_utils_eia.csv:
    EIA Utility IDs and names of utilities which appear in the PUDL DB, but which do not
    appear in the PUDL ID mapping spreadsheet. We should only attempt to link EIA
    utilities to their FERC 1 counterparts if they are associated with plants that
    reported data in the EIA 923. These utilities have "True" in the "link_to_ferc1"
    column. All other EIA utility IDs should get added to the mapping spreadsheet with
    an automatically assigned PUDL ID.

unmapped_plants_eia.csv:
    EIA Plant IDs and Plant Names of plants which appear in the PUDL DB, but which do
    not appear in the PUDL ID mapping spreadsheet. The Utility ID and Name for the
    primary plant operator, as well as the aggregate plant capacity and the state the
    plant is located in are also proved to aid in PUDL ID mapping. We don't attempt to
    link all plants to their FERC 1 counterparts, only those with a capacity over some
    minimum threshold. These plants are indicated with "True" in the "link_to_ferc1"
    column. All other EIA PLant IDs should get added to the mapping spreadsheet with an
    automatically assigned PUDL ID.
"""
import argparse
import logging
import sys
from collections.abc import Iterable
from pathlib import Path

import coloredlogs
import pandas as pd
import sqlalchemy as sa

import pudl
from pudl.glue.ferc1_eia import (
    get_plant_map,
    get_raw_plants_ferc1,
    get_utility_map_pudl,
)
from pudl.metadata.classes import DataSource

logger = logging.getLogger(__name__)

PLANTS_FERC1_PATH = Path(__file__).parent / "unmapped_plants_ferc1.csv"
UTILS_FERC1_PATH = Path(__file__).parent / "unmapped_utils_ferc1.csv"
PLANTS_EIA_PATH = Path(__file__).parent / "unmapped_plants_eia.csv"
UTILS_EIA_PATH = Path(__file__).parent / "unmapped_utils_eia.csv"
MIN_PLANT_CAPACITY_MW: float = 5.0
MAX_LOST_PLANTS_EIA: int = 50
MAX_LOST_UTILS_EIA: int = 10

PUDL_SETTINGS: dict[str, str] = pudl.workspace.setup.get_defaults()

# Identify only those utilities assocaited with plants that reported data
# at some point in the EIA 923 -- these are the ones we might need to link
# to the FERC Form 1 utilities:
DATA_TABLES_EIA923: list[str] = [
    "boiler_fuel_eia923",
    "fuel_receipts_costs_eia923",
    "generation_eia923",
    "generation_fuel_eia923",
    "generation_fuel_nuclear_eia923",
]


def get_mapped_utils_eia() -> pd.DataFrame:
    """Get a list of all the EIA Utilities that have PUDL IDs."""
    mapped_utils_eia = (
        get_utility_map_pudl()
        .loc[:, ["utility_id_eia", "utility_name_eia"]]
        .dropna(subset=["utility_id_eia"])
        .pipe(pudl.helpers.simplify_strings, columns=["utility_name_eia"])
        .astype({"utility_id_eia": int})
        .drop_duplicates("utility_id_eia")
        .set_index("utility_id_eia")
        .sort_index()
    )
    return mapped_utils_eia


def get_mapped_plants_eia():
    """Get a list of all EIA plants that have been assigned PUDL Plant IDs.

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
        pudl.glue.ferc1_eia.get_plant_map()
        .loc[:, ["plant_id_eia", "plant_name_eia"]]
        .dropna(subset=["plant_id_eia"])
        .pipe(pudl.helpers.simplify_strings, columns=["plant_name_eia"])
        .astype({"plant_id_eia": int})
        .drop_duplicates("plant_id_eia")
        .sort_values("plant_id_eia")
    )
    return mapped_plants_eia


def get_mapped_plants_ferc1() -> pd.DataFrame:
    """Generate a dataframe containing all previously mapped FERC 1 plants.

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
        A DataFrame with three columns: ``plant_name``, ``utility_id_ferc1``, and
        ``utility_name_ferc1``. Each row represents a unique combination of
        ``utility_id_ferc1`` and ``plant_name``.
    """
    # If we're only trying to get the NEW plants, then we need to see which
    # ones we have already integrated into the PUDL database. However, because
    # FERC doesn't use the same plant names from year to year, we have to rely
    # on the full mapping of FERC plant names to PUDL IDs, which only exists
    # in the ID mapping spreadhseet (the FERC Plant names in the PUDL DB are
    # canonincal names we've chosen to represent all the varied plant names
    # that exist in the raw FERC DB.
    ferc1_mapped_plants = (
        get_plant_map()
        .loc[:, ["utility_id_ferc1", "utility_name_ferc1", "plant_name_ferc1"]]
        .dropna(subset=["utility_id_ferc1"])
        .pipe(
            pudl.helpers.simplify_strings,
            columns=["utility_id_ferc1", "utility_name_ferc1", "plant_name_ferc1"],
        )
        .astype({"utility_id_ferc1": int})
        .drop_duplicates(["utility_id_ferc1", "plant_name_ferc1"])
        .sort_values(["utility_id_ferc1", "plant_name_ferc1"])
    )
    return ferc1_mapped_plants


def get_db_utils_eia(pudl_engine):
    """Get a list of all EIA Utilities appearing in the PUDL DB."""
    db_utils_eia = (
        pd.read_sql("utilities_entity_eia", pudl_engine)
        .loc[:, ["utility_id_eia", "utility_name_eia"]]
        .pipe(pudl.helpers.simplify_strings, columns=["utility_name_eia"])
        .astype({"utility_id_eia": int})
        .drop_duplicates("utility_id_eia")
        .set_index("utility_id_eia")
        .sort_index()
    )
    return db_utils_eia


def get_db_plants_eia(pudl_engine):
    """Get a list of all EIA plants appearing in the PUDL DB.

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
        pd.read_sql("plants_entity_eia", pudl_engine)
        .loc[:, ["plant_id_eia", "plant_name_eia", "state"]]
        .pipe(pudl.helpers.simplify_strings, columns=["plant_name_eia"])
        .astype({"plant_id_eia": int})
        .drop_duplicates("plant_id_eia")
        .sort_values("plant_id_eia")
    )
    return db_plants_eia


##########
# Unmapped
##########


def get_unmapped_plants_ferc1(
    pudl_settings: dict[str, str],
    years: Iterable[int],
) -> pd.DataFrame:
    """Generate a DataFrame of all unmapped FERC plants in the given years.

    Pulls all plants from the FERC Form 1 DB for the given years, and compares that list
    against the already mapped plants. Any plants found in the database but not in the
    list of mapped plants are returned.

    Note: Currently being used in ``find_unmapped_plants_utils``. Maybe delete.

    Args:
        pudl_settings: Dictionary containing various paths and database URLs used by
            PUDL.
        years: Years for which plants should be compiled from the raw FERC Form 1 DB.

    Returns:
        A dataframe containing five columns: utility_id_ferc1, utility_name_ferc1,
        plant_name, capacity_mw, and plant_table. Each row is a unique combination of
        utility_id_ferc1 and plant_name, which appears in the FERC Form 1 DB, but not in
        the list of manually mapped plants.
    """
    db_plants = get_raw_plants_ferc1(pudl_settings, years).set_index(
        ["utility_id_ferc1", "plant_name_ferc1"]
    )
    mapped_plants = get_mapped_plants_ferc1().set_index(
        ["utility_id_ferc1", "plant_name_ferc1"]
    )
    new_plants_index = db_plants.index.difference(mapped_plants.index)
    unmapped_plants = db_plants.loc[new_plants_index].reset_index()
    return unmapped_plants


def get_unmapped_plants_eia(pudl_engine):
    """Identify any as-of-yet unmapped EIA Plants."""
    plants_utils_eia = (
        pd.read_sql(
            "SELECT DISTINCT plant_id_eia, utility_id_eia FROM plants_eia860;",
            pudl_engine,
        )
        .dropna()
        .astype(
            {
                "plant_id_eia": int,
                "utility_id_eia": int,
            }
        )
        .drop_duplicates()
        # Need to get the name of the utility, to merge with the ID
        .merge(
            get_db_utils_eia(pudl_engine).reset_index(),
            on="utility_id_eia",
        )
    )
    plant_capacity_mw = (
        pd.read_sql("SELECT * FROM generators_eia860;", pudl_engine)
        .groupby(["plant_id_eia"])[["capacity_mw"]]
        .agg(sum)
        .reset_index()
    )
    db_plants_eia = get_db_plants_eia(pudl_engine).set_index("plant_id_eia")
    mapped_plants_eia = get_mapped_plants_eia().set_index("plant_id_eia")
    unmapped_plants_idx = db_plants_eia.index.difference(mapped_plants_eia.index)
    unmapped_plants_eia = (
        db_plants_eia.loc[unmapped_plants_idx]
        .merge(plants_utils_eia, how="left", on="plant_id_eia")
        .merge(plant_capacity_mw, how="left", on="plant_id_eia")
        .loc[
            :,
            [
                "plant_id_eia",
                "plant_name_eia",
                "utility_id_eia",
                "utility_name_eia",
                "state",
                "capacity_mw",
            ],
        ]
        .astype({"utility_id_eia": "Int32"})  # Woo! Nullable Integers FTW!
        .set_index("plant_id_eia")
    )
    return unmapped_plants_eia


def get_utility_most_recent_capacity(pudl_engine) -> pd.DataFrame:
    """Calculate total generation capacity by utility in most recent reported year."""
    gen_caps = pd.read_sql(
        "SELECT utility_id_eia, capacity_mw, report_date FROM generators_eia860",
        con=pudl_engine,
        parse_dates=["report_date"],
    )
    gen_caps["utility_id_eia"] = gen_caps["utility_id_eia"].astype("Int64")

    most_recent_gens_idx = (
        gen_caps.groupby("utility_id_eia")["report_date"].transform(max)
        == gen_caps["report_date"]
    )
    most_recent_gens = gen_caps.loc[most_recent_gens_idx]
    utility_caps = most_recent_gens.groupby("utility_id_eia").sum()
    return utility_caps


def get_unmapped_utils_eia(
    pudl_engine: sa.engine.Engine,
    data_tables_eia923: list[str] = DATA_TABLES_EIA923,
) -> pd.DataFrame:
    """Get a list of all the EIA Utilities in the PUDL DB without PUDL IDs.

    Identify any EIA Utility that appears in the data but does not have a
    utility_id_pudl associated with it in our ID mapping spreadsheet. Label some of
    those utilities for potential linkage to FERC 1 utilities, but only if they have
    plants which report data somewhere in the EIA-923 data tables. For those utilites
    that do have plants reporting in EIA-923, sum up the total capacity of all of their
    plants and include that in the output dataframe so that we can effectively
    prioritize mapping them.
    """
    db_utils_eia = get_db_utils_eia(pudl_engine)
    mapped_utils_eia = get_mapped_utils_eia()
    unmapped_utils_idx = db_utils_eia.index.difference(mapped_utils_eia.index)
    unmapped_utils_eia = db_utils_eia.loc[unmapped_utils_idx]

    # Get the most recent total capacity for the unmapped utils.
    unmapped_utils_eia = unmapped_utils_eia.merge(
        get_utility_most_recent_capacity(pudl_engine),
        on="utility_id_eia",
        how="left",
        validate="1:1",
    )

    plant_ids = pd.Series(dtype="Int64")
    for table in data_tables_eia923:
        query = f"SELECT DISTINCT plant_id_eia FROM {table}"  # nosec
        new_ids = pd.read_sql(query, pudl_engine)
        plant_ids = pd.concat([plant_ids, new_ids["plant_id_eia"]])
    plant_ids_in_eia923 = sorted(set(plant_ids))

    utils_with_plants = (
        pd.read_sql(
            "SELECT utility_id_eia, plant_id_eia FROM plants_eia860", pudl_engine
        )
        .astype("Int64")
        .drop_duplicates()
        .dropna()
    )
    utils_with_data_in_eia923 = utils_with_plants.loc[
        utils_with_plants.plant_id_eia.isin(plant_ids_in_eia923), "utility_id_eia"
    ].to_frame()

    # Most unmapped utilities have no EIA 923 data and so don't need to be linked:
    unmapped_utils_eia["link_to_ferc1"] = False
    # Any utility ID that's both unmapped and has EIA 923 data should get linked:
    idx_to_link = unmapped_utils_eia.index.intersection(
        utils_with_data_in_eia923.utility_id_eia
    )
    unmapped_utils_eia.loc[idx_to_link, "link_to_ferc1"] = True

    unmapped_utils_eia = unmapped_utils_eia.sort_values(
        by="capacity_mw", ascending=False
    )

    return unmapped_utils_eia


def get_lost_plants_eia(pudl_engine):
    """Identify any EIA plants which were mapped, but then lost from the DB."""
    mapped_plants_eia = get_mapped_plants_eia().set_index("plant_id_eia")
    db_plants_eia = get_db_plants_eia(pudl_engine).set_index("plant_id_eia")
    lost_plants_idx = mapped_plants_eia.index.difference(db_plants_eia.index)
    lost_plants_eia = mapped_plants_eia.loc[lost_plants_idx]
    return lost_plants_eia


def get_unmapped_utils_with_plants_eia(pudl_engine):
    """Get all EIA Utilities that lack PUDL IDs but have plants/ownership."""
    pudl_out = pudl.output.pudltabl.PudlTabl(pudl_engine)

    utils_idx = ["utility_id_eia", "report_date"]
    plants_idx = ["plant_id_eia", "report_date"]
    own_idx = ["plant_id_eia", "generator_id", "owner_utility_id_eia", "report_date"]

    utils_eia860 = pudl_out.utils_eia860().dropna(subset=utils_idx).set_index(utils_idx)
    plants_eia860 = (
        pudl_out.plants_eia860().dropna(subset=plants_idx).set_index(plants_idx)
    )
    own_eia860 = pudl_out.own_eia860().dropna(subset=own_idx).set_index(own_idx)

    own_miss_utils = set(
        own_eia860[own_eia860.utility_id_pudl.isnull()].utility_id_eia.unique()
    )
    plants_miss_utils = set(
        plants_eia860[plants_eia860.utility_id_pudl.isnull()].utility_id_eia.unique()
    )

    utils_eia860 = utils_eia860.reset_index()
    miss_utils = utils_eia860[
        (utils_eia860.utility_id_pudl.isna())
        & (
            (utils_eia860.plants_reported_owner == "True")
            | (utils_eia860.plants_reported_asset_manager == "True")
            | (utils_eia860.plants_reported_operator == "True")
            | (utils_eia860.plants_reported_other_relationship == "True")
            | (utils_eia860.utility_id_eia.isin(own_miss_utils))
            | (utils_eia860.utility_id_eia.isin(plants_miss_utils))
        )
    ]

    miss_utils = (
        miss_utils.drop_duplicates("utility_id_eia")
        .set_index("utility_id_eia")
        .loc[:, ["utility_name_eia"]]
    )

    # Get the most recent total capacity for the unmapped utils.
    utils_recent_capacity = get_utility_most_recent_capacity(pudl_engine)
    miss_utils = miss_utils.merge(
        utils_recent_capacity, on="utility_id_eia", how="left", validate="1:1"
    )
    miss_utils = miss_utils.sort_values(by="capacity_mw", ascending=False)
    return miss_utils


def get_lost_utils_eia(pudl_engine):
    """Get a list of all mapped EIA Utilites not found in the PUDL DB."""
    db_utils_eia = get_db_utils_eia(pudl_engine)
    mapped_utils_eia = get_mapped_utils_eia()
    lost_utils_idx = mapped_utils_eia.index.difference(db_utils_eia.index)
    lost_utils_eia = mapped_utils_eia.loc[lost_utils_idx]
    return lost_utils_eia


# CLI


def parse_command_line(argv: str) -> argparse.Namespace:
    """Parse command line arguments. See the -h option.

    Args:
        argv: Command line arguments, including caller filename.

    Returns:
        Command line arguments and their parsed values.
    """
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    arguments = parser.parse_args(argv[1:])
    return arguments


def main():
    """Identify unmapped plant and utility IDs in FERC 1 and EIA data."""
    pudl_logger = logging.getLogger("pudl")
    log_format = "%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s"
    coloredlogs.install(fmt=log_format, level="INFO", logger=pudl_logger)

    _ = parse_command_line(sys.argv)

    # Create DB Connections
    ferc1_engine = sa.create_engine(PUDL_SETTINGS["ferc1_db"])
    pudl_engine = sa.create_engine(PUDL_SETTINGS["pudl_db"])

    # Get and check FERC1 Years:
    ferc1_years = pd.read_sql(
        "SELECT DISTINCT report_year FROM f1_steam ORDER BY report_year ASC",
        ferc1_engine,
    )
    ferc1_years = list(ferc1_years.report_year)
    pudl_logger.info(f"Examining FERC 1 data for {min(ferc1_years)}-{max(ferc1_years)}")
    all_ferc1_years = DataSource.from_id("ferc1").working_partitions["years"]
    missing_ferc1_years = set(all_ferc1_years).difference(ferc1_years)
    if missing_ferc1_years:
        raise AssertionError(
            "All available years of FERC 1 data have not been loaded into the "
            f"FERC 1 DB. {missing_ferc1_years} could have been included but were "
            "not. You need to run ferc1_to_sqlite with all available years."
        )

    # Get and check EIA Years:
    eia_years = pd.read_sql(
        "SELECT DISTINCT report_date FROM plants_eia860 ORDER BY report_date ASC",
        pudl_engine,
        parse_dates=["report_date"],
    )
    eia_years = list(eia_years.report_date.dt.year)
    pudl_logger.info(f"Examining EIA data for {min(eia_years)}-{max(eia_years)}")
    all_eia_years = DataSource.from_id("eia860").working_partitions["years"]
    missing_eia_years = set(all_eia_years).difference(eia_years)
    if missing_eia_years:
        raise AssertionError(
            "All available years of EIA data have not been loaded into the PUDL DB. "
            f"{missing_eia_years} could have been included but were not. "
            "You need to run pudl_etl with all available years of EIA data."
        )

    # Check for lost EIA plants
    lost_plants_eia = get_lost_plants_eia(pudl_engine)
    if len(lost_plants_eia) > MAX_LOST_PLANTS_EIA:
        raise AssertionError(
            f"Found {len(lost_plants_eia)} mapped EIA plants that no longer "
            "appear in the database. This is more than the expected max of "
            f"{MAX_LOST_PLANTS_EIA}! "
            "Are all years of EIA data actually loaded into the PUDL DB?"
        )

    # Check for lost EIA utilities
    lost_utils_eia = get_lost_utils_eia(pudl_engine)
    if len(lost_utils_eia) > MAX_LOST_UTILS_EIA:
        raise AssertionError(
            f"Found {len(lost_utils_eia)} mapped EIA utilities that no longer "
            "appear in the database. This is more than the expected max of "
            f"{MAX_LOST_UTILS_EIA}! "
            "Are all years of EIA data actually loaded into the PUDL DB?"
        )

    # ==========================
    # Unmapped FERC 1 Plants
    # ==========================
    unmapped_plants_ferc1 = get_unmapped_plants_ferc1(PUDL_SETTINGS, years=ferc1_years)
    pudl_logger.info(
        f"Found {len(unmapped_plants_ferc1)} unmapped FERC 1 plants in "
        f"{min(ferc1_years)}-{max(ferc1_years)}."
    )
    pudl_logger.info(f"Writing unmapped FERC 1 plants out to {PLANTS_FERC1_PATH}")
    unmapped_plants_ferc1.to_csv(PLANTS_FERC1_PATH, index=False)

    # ==========================
    # Unmapped FERC 1 Utilities:
    # ==========================
    # No longer applicable!!! Remove/edit for new ferc1 util tables
    # unmapped_utils_ferc1 = get_unmapped_utils_ferc1_dbf(ferc1_engine)
    # pudl_logger.info(
    #     f"Found {len(unmapped_utils_ferc1)} unmapped FERC 1 utilities in "
    #     f"{min(ferc1_years)}-{max(ferc1_years)}."
    # )
    # pudl_logger.info(f"Writing unmapped FERC 1 utilities out to {UTILS_FERC1_PATH}")
    # unmapped_utils_ferc1.to_csv(UTILS_FERC1_PATH, index=False)

    # ==========================
    # Unmapped EIA Plants
    # ==========================
    unmapped_plants_eia = get_unmapped_plants_eia(pudl_engine).sort_values(
        "capacity_mw", ascending=False
    )
    unmapped_plants_eia["link_to_ferc1"] = (
        unmapped_plants_eia.capacity_mw >= MIN_PLANT_CAPACITY_MW
    )
    pudl_logger.info(
        f"Found {len(unmapped_plants_eia)} unmapped EIA plants in "
        f"{min(eia_years)}-{max(eia_years)}."
    )
    pudl_logger.info(
        f"Found {unmapped_plants_eia.link_to_ferc1.sum()} unmapped plants "
        f"with capacity_mw >= {MIN_PLANT_CAPACITY_MW} to link to FERC Form 1."
    )
    pudl_logger.info(f"Writing unmapped EIA plants out to {PLANTS_EIA_PATH}")
    unmapped_plants_eia.to_csv(PLANTS_EIA_PATH)

    # ==========================
    # Unmapped EIA Utilities
    # ==========================
    unmapped_utils_eia = get_unmapped_utils_eia(pudl_engine)
    pudl_logger.info(
        f"Found a total of {len(unmapped_utils_eia)} unmapped EIA utilities."
    )
    pudl_logger.info(
        f"Found {unmapped_utils_eia.link_to_ferc1.sum()} unmapped utilities "
        f"with data reported in EIA 923 to link to FERC Form 1."
    )
    pudl_logger.info(f"Writing unmapped EIA utilities out to {UTILS_EIA_PATH}")
    unmapped_utils_eia.to_csv(UTILS_EIA_PATH)


if __name__ == "__main__":
    main()
