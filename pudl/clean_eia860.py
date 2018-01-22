"""Module to perform data cleaning functions on EIA860 data tables."""

import pandas as pd
import numpy as np
from pudl import clean_pudl
from pudl import constants as pc


def clean_ownership_eia860(own_df):
    """Standardize the per-generator ownership fractions."""
    # The fix we're making here is only known to be valid for 2011 -- if we
    # get older data... then we need to to revisit the cleaning function and
    # make sure it also applies to those earlier years.
    assert min(own_df.report_date.dt.year) >= 2011
    # Prior to 2012, ownership was reported as a percentage, rather than
    # as a proportion, so we need to divide those values by 100.
    own_df.loc[own_df.report_date.dt.year == 2011, 'fraction_owned'] = \
        own_df.loc[own_df.report_date.dt.year == 2011, 'fraction_owned'] / 100

    return(own_df)


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

    gens_df = clean_pudl.month_year_to_date(gens_df)

    gens_df['fuel_type_pudl'] = \
        clean_pudl.cleanstrings(gens_df['energy_source_1'],
                                pc.fuel_type_eia860_simple_map)

    return(gens_df)
