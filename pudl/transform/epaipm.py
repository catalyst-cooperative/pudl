"""Module to perform data cleaning functions on EPA IPM data tables."""

import logging
import pandas as pd
import pudl.constants as pc
from pudl.helpers import simplify_columns

logger = logging.getLogger(__name__)


def load_curves(epaipm_dfs, epaipm_transformed_dfs):
    """
    Pull and transform the load curve table from wide to tidy format.

    Args:
        epaipm_dfs (dictionary of pandas.DataFrame): Each entry in this
            dictionary of DataFrame objects corresponds to a table from
            EPA's IPM, as reported in the Excel spreadsheets they distribute.
        epa_ipm_transformed_dfs (dictionary of DataFrames)

    Returns: transformed dataframe.

    """
    lc = epaipm_dfs['load_curves_ipm'].copy()
    lc = simplify_columns(lc)
    # Melt the load curves
    melt_lc = lc.melt(
        id_vars=['region', 'month', 'day'],
        var_name='hour',
        value_name='load_mw'
    )

    melt_lc['hour'] = (
        melt_lc['hour'].str.replace('hour_', '').astype(int)
    )
    # IPM hour designations are 1-24. Convert to 0-23 to match datetime.
    melt_lc['hour'] -= 1

    # Group to easily create 8760 time_index
    grouped = melt_lc.groupby('region')

    df_list = []
    for _, df in grouped:
        df = df.sort_values(['month', 'day', 'hour'])
        df = df.reset_index(drop=True)
        df['time_index'] = df.index + 1
        df_list.append(df)

    tidy_load_curves = pd.concat(df_list)
    tidy_load_curves = tidy_load_curves.rename(
        columns=pc.epaipm_rename_dict['load_curves_ipm']
    )

    epaipm_transformed_dfs['load_curves_ipm'] = tidy_load_curves

    return epaipm_transformed_dfs


def transmission_single(epaipm_dfs, epaipm_transformed_dfs):
    """
    Pull and transform the transmission constraints between individual regions
    table, renaming columns.

    Args:
        epaipm_dfs (dictionary of pandas.DataFrame): Each entry in this
            dictionary of DataFrame objects corresponds to a table from
            EPA's IPM, as reported in the Excel spreadsheets they distribute.
        epa_ipm_transformed_dfs (dictionary of DataFrames)

    Returns: transformed dataframe.

    """
    trans_df = epaipm_dfs['transmission_single_ipm'].copy()
    trans_df = trans_df.reset_index()
    trans_df = trans_df.rename(
        columns=pc.epaipm_rename_dict['transmission_single_ipm']
    )
    epaipm_transformed_dfs['transmission_single_ipm'] = trans_df

    return epaipm_transformed_dfs


def transmission_joint(epaipm_dfs, epaipm_transformed_dfs):

    trans_df = epaipm_dfs['transmission_joint_ipm'].copy()
    epaipm_transformed_dfs['transmission_joint_ipm'] = trans_df

    return epaipm_transformed_dfs


def plant_region_map(epaipm_dfs, epaipm_transformed_dfs):
    """
    Pull and transform the map of plant ids to IPM regions for both active and
    retiring plants.

    Args:
        epaipm_dfs (dictionary of pandas.DataFrame): Each entry in this
            dictionary of DataFrame objects corresponds to a table from
            EPA's IPM, as reported in the Excel spreadsheets they distribute.
        epa_ipm_transformed_dfs (dictionary of DataFrames)

    Returns: transformed dataframe.

    """
    trans_df = pd.concat(
        [
            epaipm_dfs['plant_region_map_ipm_active'],
            epaipm_dfs['plant_region_map_ipm_retired']
        ]
    )
    trans_df = trans_df.drop_duplicates()
    trans_df = trans_df.reset_index(drop=True)
    trans_df = trans_df.rename(
        columns=pc.epaipm_rename_dict['plant_region_map_ipm']
    )

    # Plants that are in IPM but appear to be retired or not listed in EIA files
    # missing_plants = [
    #     7939, 56892, 57717, 59089, 59397, 59398, 59399, 83001,
    #     83002, 83003, 83004, 83005, 83006, 83007,
    # ]
    # trans_df = trans_df.loc[~trans_df['plant_id_eia'].isin(missing_plants), :]

    epaipm_transformed_dfs['plant_region_map_ipm'] = trans_df

    return epaipm_transformed_dfs


def transform(epaipm_raw_dfs, epaipm_tables=pc.epaipm_pudl_tables):
    """Transform EPA IPM dfs."""
    epaipm_transform_functions = {
        'transmission_single_ipm': transmission_single,
        'transmission_joint_ipm': transmission_joint,
        'load_curves_ipm': load_curves,
        'plant_region_map_ipm': plant_region_map,
    }
    epaipm_transformed_dfs = {}

    if not epaipm_raw_dfs:
        logger.info("No raw EPA IPM dataframes found. "
                    "Not transforming EPA IPM.")
        return epaipm_transformed_dfs

    for table in epaipm_transform_functions:
        if table in epaipm_tables:
            logger.info(f"Transforming raw EPA IPM DataFrames for {table}")
            epaipm_transform_functions[table](epaipm_raw_dfs,
                                              epaipm_transformed_dfs)

    return epaipm_transformed_dfs
