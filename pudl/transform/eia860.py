"""Module to perform data cleaning functions on EIA860 data tables."""

import pandas as pd
import numpy as np
from pudl import constants as pc
import pudl.transform.pudl


def clean_ownership_eia860(eia860_dfs):
    """
    Pull and transform the ownership table

    Args: eia860_dfs (dictionary of pandas.DataFrame): Each entry in this
        dictionary of DataFrame objects corresponds to a page from the
        EIA860 form, as reported in the Excel spreadsheets they distribute.
    Returns: transformed dataframe.
    """
    o_df = eia860_dfs['ownership'].copy()

    # Replace '.' and ' ' with NaN in order to read in integer values
    o_df.replace(to_replace='^\.$', value=np.nan, regex=True, inplace=True)
    o_df.replace(to_replace='^\s$', value=np.nan, regex=True, inplace=True)
    o_df.replace(to_replace='^$', value=np.nan, regex=True, inplace=True)

    o_df = pudl.transform.pudl.convert_to_date(o_df)

    # The fix we're making here is only known to be valid for 2011 -- if we
    # get older data... then we need to to revisit the cleaning function and
    # make sure it also applies to those earlier years.
    assert min(o_df.report_date.dt.year) >= 2011

    # Prior to 2012, ownership was reported as a percentage, rather than
    # as a proportion, so we need to divide those values by 100.
    o_df.loc[o_df.report_date.dt.year == 2011, 'fraction_owned'] = \
        o_df.loc[o_df.report_date.dt.year == 2011, 'fraction_owned'] / 100

    # TODO: this function should feed this altered dataframe back into eia860,
    # which should then feed into a yet to be created standardized 'load' step

    return(o_df)


def clean_generators_eia860(gens_df):
    """Clean up the combined EIA860 generators data frame."""
    # Get rid of any unidentifiable records:
    gens_df.dropna(subset=['generator_id', 'plant_id_eia'], inplace=True)

    # Replace empty strings, whitespace, and '.' fields with real NA values
    gens_df.replace(to_replace='^\.$', value=np.nan, regex=True, inplace=True)
    gens_df.replace(to_replace='^\s$', value=np.nan, regex=True, inplace=True)
    gens_df.replace(to_replace='^$', value=np.nan, regex=True, inplace=True)

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
        'planned_new_nameplate_capacity_mw',
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
        gens_df[column] = gens_df[column].replace(
            to_replace=["Y", "N"], value=[True, False])
        gens_df[column] = gens_df[column].fillna('False')

    gens_df = pudl.transform.pudl.month_year_to_date(gens_df)

    gens_df['fuel_type_pudl'] = \
        pudl.transform.pudl.cleanstrings(gens_df['energy_source_1'],
                                         pc.fuel_type_eia860_simple_map)

    return(gens_df)


def clean_plants_eia860(eia860_dfs):
    """
    Pull and transform the EIA 860 plants table

    Args: eia860_dfs (dictionary of pandas.DataFrame): Each entry in this
        dictionary of DataFrame objects corresponds to a page from the
        EIA860 form, as reported in the Excel spreadsheets they distribute.
    Returns: transformed dataframe.
    """
    # Pull the plants data frame
    p_df = eia860_dfs['plant'].copy()

    # # Replace '.' and ' ' with NaN in order to read in integer values
    # p_df.replace(to_replace='.', value=np.nan, regex=True, inplace=True)
    # p_df.replace(to_replace=' ', value=np.nan, regex=True,  inplace=True)

    # Replace empty strings, whitespace, and '.' fields with real NA values
    p_df.replace(to_replace='^\.$', value=np.nan, regex=True, inplace=True)
    p_df.replace(to_replace='^\s$', value=np.nan, regex=True, inplace=True)
    p_df.replace(to_replace='^$', value=np.nan, regex=True, inplace=True)

    # Cast integer values in sector to floats to avoid type errors
    p_df['sector'] = p_df['sector'].astype(float)

    # Cast various types in transmission_distribution_owner_id to str
    p_df['transmission_distribution_owner_id'] = \
        p_df['transmission_distribution_owner_id'].astype(str)

    # Cast values in zip_code to floats to avoid type errors
    p_df['zip_code'] = p_df['zip_code'].astype(str)

    # A subset of the columns have "X" values, where other columns_to_fix
    # have "N" values. Replacing these values with "N" will make for uniform
    # values that can be converted to Boolean True and False pairs.

    p_df.ash_impoundment_lined = \
        p_df.ash_impoundment_lined.replace(to_replace='X', value='N')
    p_df.natural_gas_storage = \
        p_df.natural_gas_storage.replace(to_replace='X', value='N')
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
        p_df[column] = p_df[column].replace(
            to_replace=["Y", "N"], value=[True, False])
        p_df[column] = p_df[column].fillna('False')

    p_df = pudl.transform.pudl.convert_to_date(p_df)

    # The fix we're making here is only known to be valid for 2011 -- if we
    # get older data... then we need to to revisit the cleaning function and
    # make sure it also applies to those earlier years.
    assert min(p_df.report_date.dt.year) >= 2011

    return(p_df)
