#!/usr/bin/env python
"""
Identify all unmapped plant and utility IDs in FERC 1 and EIA data.

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
from pathlib import Path
from typing import Dict

import coloredlogs
import pandas as pd
import sqlalchemy as sa

import pudl
from pudl.glue.ferc1_eia import (get_lost_plants_eia, get_lost_utils_eia,
                                 get_unmapped_plants_eia,
                                 get_unmapped_plants_ferc1,
                                 get_unmapped_utils_eia,
                                 get_unmapped_utils_ferc1)
from pudl.metadata.classes import DataSource

logger = logging.getLogger(__name__)

PLANTS_FERC1_PATH = Path(__file__).parent / "unmapped_plants_ferc1.csv"
UTILS_FERC1_PATH = Path(__file__).parent / "unmapped_utils_ferc1.csv"
PLANTS_EIA_PATH = Path(__file__).parent / "unmapped_plants_eia.csv"
UTILS_EIA_PATH = Path(__file__).parent / "unmapped_utils_eia.csv"
MIN_PLANT_CAPACITY_MW: float = 5.0
MAX_LOST_PLANTS_EIA: int = 50
MAX_LOST_UTILS_EIA: int = 10

PUDL_SETTINGS: Dict[str, str] = pudl.workspace.setup.get_defaults()


def parse_command_line(argv: str) -> argparse.Namespace:
    """
    Parse command line arguments. See the -h option.

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
    log_format = '%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s'
    coloredlogs.install(fmt=log_format, level='INFO', logger=pudl_logger)

    _ = parse_command_line(sys.argv)

    # Create DB Connections
    ferc1_engine = sa.create_engine(PUDL_SETTINGS["ferc1_db"])
    pudl_engine = sa.create_engine(PUDL_SETTINGS["pudl_db"])

    # Get and check FERC1 Years:
    ferc1_years = pd.read_sql(
        "SELECT DISTINCT report_year FROM f1_steam ORDER BY report_year ASC",
        ferc1_engine
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
        parse_dates=["report_date"]
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
    unmapped_utils_ferc1 = get_unmapped_utils_ferc1(ferc1_engine)
    pudl_logger.info(
        f"Found {len(unmapped_utils_ferc1)} unmapped FERC 1 utilities in "
        f"{min(ferc1_years)}-{max(ferc1_years)}."
    )
    pudl_logger.info(f"Writing unmapped FERC 1 utilities out to {UTILS_FERC1_PATH}")
    unmapped_utils_ferc1.to_csv(UTILS_FERC1_PATH, index=False)

    # ==========================
    # Unmapped EIA Plants
    # ==========================
    unmapped_plants_eia = (
        get_unmapped_plants_eia(pudl_engine)
        .sort_values("capacity_mw", ascending=False)
    )
    unmapped_plants_eia["link_to_ferc1"] = unmapped_plants_eia.capacity_mw >= MIN_PLANT_CAPACITY_MW
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
