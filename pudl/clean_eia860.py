"""Module to perform data cleaning functions on EIA860 data tables."""

import pandas as pd
import numpy as np
from pudl import clean_pudl
from pudl import constants as pc


def clean_generators_eia860(gens_df):
    """Clean up the combined EIA860 generators data frame."""
    # Get rid of any unidentifiable records:
    gens_df.dropna(subset=['generator_id', 'plant_id'], inplace=True)

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
        'planned_net_summer_capacity_derate',
        'planned_net_summer_capacity_uprate',
        'planned_net_winter_capacity_derate',
        'planned_net_winter_capacity_uprate',
        'planned_new_nameplate_capacity_mw',
        'nameplate_power_factor',
        'minimum_load_mw',
        'winter_capacity_mw',
        'summer_capacity_mw'
    ]

    for column in columns_to_fix:
        gens_df[column] = gens_df[column].replace(
            to_replace=[" ", 0], value=np.nan)

    gens_df = clean_pudl.month_year_to_date(gens_df)

    gens_df['fuel_type_pudl'] = \
        clean_pudl.cleanstrings(gens_df['energy_source_1'],
                                pc.fuel_type_eia860_simple_map)

    return(gens_df)
