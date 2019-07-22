"""Module to perform data cleaning functions on EIA860 data tables."""

import logging

import numpy as np
import pandas as pd

import pudl
import pudl.constants as pc

logger = logging.getLogger(__name__)


def ownership(eia860_dfs, eia860_transformed_dfs):
    """
    Pulls and transforms the ownership table.

    Args:
        eia860_dfs (dict): Each entry in this dictionary of DataFrame objects
            corresponds to a page from the EIA860 form, as reported in the
            Excel spreadsheets they distribute
        eia860_transformed_dfs (dict): A dictionary of DataFrame objects in
            which pages from EIA860 form (keys) correspond to normalized
            DataFrames of values from that page (values)

    Returns:
        dict: eia860_transformed_dfs, a dictionary of DataFrame objects in
        which pages from EIA860 form (keys) correspond to normalized
        DataFrames of values from that page (values)

    Todo:
        Convert assert statement to AssertionError

    """
    o_df = eia860_dfs['ownership'].copy()

    # Replace '.' and ' ' with NaN in order to read in integer values
    o_df = pudl.helpers.fix_eia_na(o_df)

    o_df = pudl.helpers.convert_to_date(o_df)

    # The fix we're making here is only known to be valid for 2011 -- if we
    # get older data... then we need to to revisit the cleaning function and
    # make sure it also applies to those earlier years.
    assert min(o_df.report_date.dt.year) >= 2011

    # Prior to 2012, ownership was reported as a percentage, rather than
    # as a proportion, so we need to divide those values by 100.
    o_df.loc[o_df.report_date.dt.year == 2011, 'fraction_owned'] = \
        o_df.loc[o_df.report_date.dt.year == 2011, 'fraction_owned'] / 100

    o_df['owner_utility_id_eia'] = o_df['owner_utility_id_eia'].astype(int)
    o_df['utility_id_eia'] = o_df['utility_id_eia'].astype(int)
    o_df['plant_id_eia'] = o_df['plant_id_eia'].astype(int)

    eia860_transformed_dfs['ownership_eia860'] = o_df

    return eia860_transformed_dfs


def generators(eia860_dfs, eia860_transformed_dfs):
    """
    Pulls and transforms the generators table.

    There are three tabs that the generator records come from (proposed,
    existing, and retired). We pull each tab into one dataframe and include
    an 'operational_status' to indicate which tab the record came from.

    Args:
        eia860_dfs (dict): Each entry in this
            dictionary of DataFrame objects corresponds to a page from the
            EIA860 form, as reported in the Excel spreadsheets they distribute.
        eia860_transformed_dfs (dict): A dictionary of DataFrame objects in
            which pages from EIA860 form (keys) correspond to a normalized
            DataFrame of values from that page (values)

    Returns:
        dict: eia860_transformed_dfs, a dictionary of DataFrame objects in
        which pages from EIA860 form (keys) correspond to normalized
        DataFrames of values from that page (values)

    """
    # Groupby objects were creating chained assignment warning that is N/A
    pd.options.mode.chained_assignment = None

    # There are three sets of generator data reported in the EIA860 table,
    # planned, existing, and retired generators. We're going to concatenate
    # them all together into a single big table, with a column that indicates
    # which one of these tables the data came from, since they all have almost
    # exactly the same structure
    gp_df = eia860_dfs['generator_proposed'].copy()
    ge_df = eia860_dfs['generator_existing'].copy()
    gr_df = eia860_dfs['generator_retired'].copy()
    gp_df['operational_status'] = 'proposed'
    ge_df['operational_status'] = 'existing'
    gr_df['operational_status'] = 'retired'

    gens_df = pd.concat([ge_df, gp_df, gr_df], sort=True)

    # Get rid of any unidentifiable records:
    gens_df.dropna(subset=['generator_id', 'plant_id_eia'], inplace=True)

    # Replace empty strings, whitespace, and '.' fields with real NA values
    gens_df = pudl.helpers.fix_eia_na(gens_df)

    # A subset of the columns have zero values, where NA is appropriate:
    columns_to_fix = [
        'planned_retirement_month',
        'planned_retirement_year',
        'planned_uprate_month',
        'planned_uprate_year',
        'other_modifications_month',
        'other_modifications_year',
        'planned_derate_month',
        'planned_derate_year',
        'planned_repower_month',
        'planned_repower_year',
        'planned_net_summer_capacity_derate_mw',
        'planned_net_summer_capacity_uprate_mw',
        'planned_net_winter_capacity_derate_mw',
        'planned_net_winter_capacity_uprate_mw',
        'planned_new_capacity_mw',
        'nameplate_power_factor',
        'minimum_load_mw',
        'winter_capacity_mw',
        'summer_capacity_mw'
    ]

    for column in columns_to_fix:
        gens_df[column] = gens_df[column].replace(
            to_replace=[" ", 0], value=np.nan)

    # A subset of the columns have "X" values, where other columns_to_fix
    # have "N" values. Replacing these values with "N" will make for uniform
    # values that can be converted to Boolean True and False pairs.

    gens_df.duct_burners = \
        gens_df.duct_burners.replace(to_replace='X', value='N')
    gens_df.heat_bypass_recovery = \
        gens_df.heat_bypass_recovery.replace(to_replace='X', value='N')
    gens_df.syncronized_transmission_grid = \
        gens_df.heat_bypass_recovery.replace(to_replace='X', value='N')

    # A subset of the columns have "U" values, presumably for "Unknown," which
    # must be set to None in order to convert the columns to datatype Boolean.

    gens_df.multiple_fuels = \
        gens_df.multiple_fuels.replace(to_replace='U', value=None)
    gens_df.switch_oil_gas = \
        gens_df.switch_oil_gas.replace(to_replace='U', value=None)

    boolean_columns_to_fix = [
        'duct_burners',
        'multiple_fuels',
        'deliver_power_transgrid',
        'syncronized_transmission_grid',
        'solid_fuel_gasification',
        'pulverized_coal_tech',
        'fluidized_bed_tech',
        'subcritical_tech',
        'supercritical_tech',
        'ultrasupercritical_tech',
        'carbon_capture',
        'stoker_tech',
        'other_combustion_tech',
        'cofire_fuels',
        'switch_oil_gas',
        'heat_bypass_recovery',
        'associated_combined_heat_power',
        'planned_modifications',
        'other_planned_modifications',
        'uprate_derate_during_year',
        'previously_canceled'
    ]

    for column in boolean_columns_to_fix:
        gens_df[column] = gens_df[column].fillna('False')
        gens_df[column] = gens_df[column].replace(
            to_replace=["Y", "N"], value=[True, False])

    gens_df = (
        gens_df.
        pipe(pudl.helpers.month_year_to_date).
        assign(fuel_type_code_pudl=lambda x: pudl.helpers.cleanstrings_series(
            x['energy_source_code_1'], pc.fuel_type_eia860_simple_map)).
        pipe(pudl.helpers.strip_lower,
             columns=['rto_iso_lmp_node_id',
                      'rto_iso_location_wholesale_reporting_id']).
        astype({
            'plant_id_eia': int,
            'generator_id': str,
            'utility_id_eia': int
        }).
        pipe(pudl.helpers.convert_to_date)
    )

    eia860_transformed_dfs['generators_eia860'] = gens_df

    return eia860_transformed_dfs


def plants(eia860_dfs, eia860_transformed_dfs):
    """
    Pulls and transforms the plants table.

    Much of the static plant information is reported repeatedly, and scattered
    across several different pages of EIA 923. The data frame which this
    function uses is assembled from those many different pages, and passed in
    via the same dictionary of dataframes that all the other ingest functions
    use for uniformity.

    Args:
        eia860_dfs (dict): Each entry in this
            dictionary of DataFrame objects corresponds to a page from the
            EIA860 form, as reported in the Excel spreadsheets they distribute.
        eia860_transformed_dfs (dict): A dictionary of DataFrame objects in
            which pages from EIA860 form (keys) correspond to normalized
            DataFrames of values from that page (values)

    Returns:
        dict: eia860_transformed_dfs, a dictionary of DataFrame objects in
        which pages from EIA860 form (keys) correspond to normalized
        DataFrames of values from that page (values)

    """
    # Populating the 'plants_eia860' table
    p_df = eia860_dfs['plant'].copy()

    # Replace empty strings, whitespace, and '.' fields with real NA values
    p_df = pudl.helpers.fix_eia_na(p_df)

    # Cast values in zip_code to strings to avoid type errors
    p_df['zip_code'] = p_df['zip_code'].astype(str)

    # A subset of the columns have "X" values, where other columns_to_fix
    # have "N" values. Replacing these values with "N" will make for uniform
    # values that can be converted to Boolean True and False pairs.

    p_df.ash_impoundment_lined = p_df.ash_impoundment_lined.replace(
        to_replace='X', value='N')
    p_df.natural_gas_storage = p_df.natural_gas_storage.replace(
        to_replace='X', value='N')
    p_df.liquefied_natural_gas_storage = \
        p_df.liquefied_natural_gas_storage.replace(to_replace='X', value='N')

    boolean_columns_to_fix = [
        'ferc_cogen_status',
        'ferc_small_power_producer',
        'ferc_exempt_wholesale_generator',
        'ash_impoundment',
        'ash_impoundment_lined',
        'energy_storage',
        'natural_gas_storage',
        'liquefied_natural_gas_storage'
    ]

    for column in boolean_columns_to_fix:
        p_df[column] = p_df[column].fillna('False')
        p_df[column] = p_df[column].replace(
            to_replace=["Y", "N"], value=[True, False])

    # Ensure plant & operator IDs are integers.
    p_df['plant_id_eia'] = p_df['plant_id_eia'].astype(int)
    p_df['utility_id_eia'] = p_df['utility_id_eia'].astype(int)
    p_df['primary_purpose_naics_id'] = p_df['primary_purpose_naics_id'].astype(
        int)

    p_df = pudl.helpers.convert_to_date(p_df)

    eia860_transformed_dfs['plants_eia860'] = p_df

    return eia860_transformed_dfs


def boiler_generator_assn(eia860_dfs, eia860_transformed_dfs):
    """
    Pulls and transforms the boilder generator association table.

    Args:
        eia860_dfs (dict): Each entry in this dictionary of DataFrame objects
            corresponds to a page from the EIA860 form, as reported in the
            Excel spreadsheets they distribute.
        eia860_transformed_dfs (dict): A dictionary of DataFrame objects in
            which pages from EIA860 form (keys) correspond to normalized
            DataFrames of values from that page (values)

    Returns:
        dict: eia860_transformed_dfs, a dictionary of DataFrame objects in
        which pages from EIA860 form (keys) correspond to normalized
        DataFrames of values from that page (values)

    """
    # Populating the 'generators_eia860' table
    b_g_df = eia860_dfs['boiler_generator_assn'].copy()

    b_g_cols = ['report_year',
                'utility_id_eia',
                'plant_id_eia',
                'boiler_id',
                'generator_id']

    b_g_df = b_g_df[b_g_cols]

    # There are some bad (non-data) lines in some of the boiler generator
    # data files (notes from EIA) which are messing up the import. Need to
    # identify and drop them early on.
    b_g_df['utility_id_eia'] = b_g_df['utility_id_eia'].astype(str)
    b_g_df = b_g_df[b_g_df.utility_id_eia.str.isnumeric()]

    b_g_df['plant_id_eia'] = b_g_df['plant_id_eia'].astype(int)

    # We need to cast the generator_id column as type str because sometimes
    # it is heterogeneous int/str which make drop_duplicates fail.
    b_g_df['generator_id'] = b_g_df['generator_id'].astype(str)
    b_g_df['boiler_id'] = b_g_df['boiler_id'].astype(str)

    # This drop_duplicates isn't removing all duplicates
    b_g_df = b_g_df.drop_duplicates().dropna()

    b_g_df = pudl.helpers.convert_to_date(b_g_df)

    eia860_transformed_dfs['boiler_generator_assn_eia860'] = b_g_df

    return eia860_transformed_dfs


def utilities(eia860_dfs, eia860_transformed_dfs):
    """
    Pulls and transforms the utilities table.

    Args:
        eia860_dfs (dict): Each entry in this
            dictionary of DataFrame objects corresponds to a page from the
            EIA860 form, as reported in the Excel spreadsheets they distribute.
        eia860_transformed_dfs (dict): A dictionary of DataFrame objects in
            which pages from EIA860 form (keys) correspond to normalized
            DataFrames of values from that page (values)

    Returns:
        dict: eia860_transformed_dfs, a dictionary of DataFrame objects in
        which pages from EIA860 form (keys) correspond to normalized
        DataFrames of values from that page (values)

    """
    # Populating the 'utilities_eia860' table
    u_df = eia860_dfs['utility'].copy()

    # Replace empty strings, whitespace, and '.' fields with real NA values
    u_df = pudl.helpers.fix_eia_na(u_df)
    u_df['state'] = u_df.state.str.upper()
    u_df['state'] = u_df.state.replace({
        'QB': 'QC',  # wrong abbreviation for Quebec
        'Y': 'NY',  # Typo
    })

    boolean_columns_to_fix = [
        'plants_reported_owner',
        'plants_reported_operator',
        'plants_reported_asset_manager',
        'plants_reported_other_relationship'
    ]

    for column in boolean_columns_to_fix:
        u_df[column] = u_df[column].fillna('False')
        u_df[column] = u_df[column].replace(
            to_replace=["Y", "N"], value=[True, False])

    u_df = pudl.helpers.convert_to_date(u_df)

    u_df['utility_id_eia'] = u_df['utility_id_eia'].astype(int)

    eia860_transformed_dfs['utilities_eia860'] = u_df

    return eia860_transformed_dfs


def transform(eia860_raw_dfs, eia860_tables=pc.eia860_pudl_tables):
    """
    Transforms EIA 860 DataFrames.

    Args:
        eia860_raw_dfs (dict): a dictionary of tab names (keys) and DataFrames
            (values). This can be generated by pudl.
        eia860_tables (tuple): A tuple containing the names of the EIA 860
            tables that can be pulled into PUDL

    Returns:
        dict: A dictionary of DataFrame objects in
        which pages from EIA860 form (keys) corresponds to a normalized
        DataFrame of values from that page (values)

    """
    # these are the tables that we have transform functions for...
    eia860_transform_functions = {
        'ownership_eia860': ownership,
        'generators_eia860': generators,
        'plants_eia860': plants,
        'boiler_generator_assn_eia860': boiler_generator_assn,
        'utilities_eia860': utilities}
    eia860_transformed_dfs = {}

    if not eia860_raw_dfs:
        logger.info("No raw EIA 860 dataframes found. "
                    "Not transforming EIA 860.")
        return eia860_transformed_dfs
    # for each of the tables, run the respective transform funtction
    for table in eia860_transform_functions:
        if table in eia860_tables:
            logger.info(f"Transforming raw EIA 860 DataFrames for {table} "
                        f"concatenated across all years.")
            eia860_transform_functions[table](eia860_raw_dfs,
                                              eia860_transformed_dfs)

    return eia860_transformed_dfs
