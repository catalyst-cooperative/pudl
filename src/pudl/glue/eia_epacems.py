"""
Extract, clean, and normalize the EPA-EIA crosswalk.

This module defines functions that read the raw EPA-EIA crosswalk file, clean
up the column names, and separate it into three distinctive normalize tables
for integration in the database. There are many gaps in the mapping of EIA
plant and generator ids to EPA plant and unit ids, so, for the time being these
tables are sparse.

The EPA, in conjunction with the EIA, plans to relase an crosswalk with fewer
gaps at the beginning of 2021. Until then, this module reads and cleans the
currently available crosswalk.

The raw crosswalk file was obtained from Greg Schivley. His methods for filling
in some of the gaps are not included in this version of the module.
https://github.com/grgmiller/EPA-EIA-Unit-Crosswalk
"""
import importlib
import logging

import pandas as pd

import pudl
from pudl.metadata.fields import apply_pudl_dtypes

logger = logging.getLogger(__name__)


def grab_n_clean_epa_orignal():
    """
    Retrieve and clean column names for the original EPA-EIA crosswalk file.

    Returns:
        pandas.DataFrame: a version of the EPA-EIA crosswalk containing only
            relevant columns. Columns names are clear and programatically
            accessible.
    """
    logger.info("grabbing original crosswalk")
    eia_epacems_crosswalk_csv = (
        importlib.resources.open_text(
            'pudl.package_data.glue',
            'epa_eia_crosswalk_from_epa.csv')
    )
    eia_epacems_crosswalk = (
        pd.read_csv(eia_epacems_crosswalk_csv)
        .pipe(pudl.helpers.simplify_columns)
        .rename(columns={
            'oris_code': 'plant_id_epa',
            'eia_oris': 'plant_id_eia',
            'unit_id': 'unit_id_epa',
            'facility_name': 'plant_name_eia'})
        .filter([
            'plant_name_eia',
            'plant_id_eia',
            'plant_id_epa',
            'unit_id_epa',
            'generator_id',
            'boiler_id',
        ])
        .pipe(apply_pudl_dtypes, 'eia')
    )
    return eia_epacems_crosswalk


def split_tables(df):
    """
    Split the cleaned EIA-EPA crosswalk table into three normalized tables.

    Args:
        pandas.DataFrame: a DataFrame of relevant, readible columns from the
            EIA-EPA crosswalk. Output of grab_n_clean_epa_original().
    Returns:
        dict: a dictionary of three normalized DataFrames comprised of the data
        in the original crosswalk file. EPA plant id to EPA unit id; EPA plant
        id to EIA plant id; and EIA plant id to EIA generator id to EPA unit
        id. Includes no nan values.
    """
    logger.info("splitting crosswalk into three normalized tables")
    epa_df = (
        df.filter(['plant_id_epa', 'unit_id_epa']).copy()
        .drop_duplicates()
        .dropna()
    )
    plants_eia_epa = (
        df.filter(['plant_id_eia', 'plant_id_epa']).copy()
        .drop_duplicates()
        .dropna()
    )
    gen_unit_df = (
        df.filter(['plant_id_eia', 'generator_id', 'unit_id_epa']).copy()
        .drop_duplicates()
        .dropna()
    )

    return {
        'plant_unit_epa': epa_df,
        'assn_plant_id_eia_epa': plants_eia_epa,
        'assn_gen_eia_unit_epa': gen_unit_df}


def grab_clean_split():
    """
    Clean raw crosswalk data, drop nans, and return split tables.

    Returns:
        dict: a dictionary of three normalized DataFrames comprised of the data
        in the original crosswalk file. EPA plant id to EPA unit id; EPA plant
        id to EIA plant id; and EIA plant id to EIA generator id to EPA unit
        id.
    """
    crosswalk = (
        grab_n_clean_epa_orignal()
        .reset_index()
        .dropna()
    )

    return split_tables(crosswalk)
