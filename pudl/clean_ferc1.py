"""
Routines for transforming FERC Form 1 data before loading into the PUDL DB.

This module provides a variety of functions that are used in cleaning up the
FERC Form 1 data prior to loading into our database. This includes adopting
standardized units and column names, standardizing the formatting of some
string values, and correcting data entry errors which we can infer based on
the existing data. It may also include removing bad data, or replacing it
with the appropriate NA values.
"""

import pandas as pd
import numpy as np
from pudl import clean_pudl
import pudl.constants as pc

##############################################################################
# HELPER FUNCTIONS ###########################################################
##############################################################################


def multiplicative_error_correction(tofix, mask, minval, maxval, mults):
    """
    Correct data entry errors resulting in data being multiplied by a factor.

    In many cases we know that a particular column in the database should have
    a value in a particular rage (e.g. the heat content of a ton of coal is a
    well defined physical quantity -- it can be 15 mmBTU/ton or 22 mmBTU/ton,
    but it can't be 1 mmBTU/ton or 100 mmBTU/ton). Sometimes these fields
    are reported in the wrong units (e.g. kWh of electricity generated rather
    than MWh) resulting in several distributions that have a similar shape
    showing up at different ranges of value within the data.  This function
    takes a one dimensional data series, a description of a valid range for
    the values, and a list of factors by which we expect to see some of the
    data multiplied due to unit errors.  Data found in these "ghost"
    distributions are multiplied by the appropriate factor to bring them into
    the expected range.

    Data values which are not found in one of the acceptable multiplicative
    ranges are set to NA.

    Args:
        tofix (pandas.Series): A 1-dimensional data series containing the
            values to be fixed.
        mask (pandas.Series): A 1-dimensional masking array of True/False
            values, which will be used to select a subset of the tofix
            series onto which we will apply the multiplicative fixes.
        min (float): the minimum realistic value for the data series.
        max (float): the maximum realistic value for the data series.
        mults (list of floats): values by which "real" data may have been
            multiplied due to common data entry errors. These values both
            show us where to look in the full data series to find recoverable
            data, and also tell us by what factor those values need to be
            multiplied to bring them back into the reasonable range.
    Returns:
        fixed (pandas.Series): a data series of the same length as the
            input, but with the transformed values.
    """
    # Grab the subset of the input series we are going to work on:
    records_to_fix = tofix[mask]
    # Drop those records from our output series
    fixed = tofix.drop(records_to_fix.index)
    # Iterate over the multipliers, applying fixes to outlying populations
    for mult in mults:
        records_to_fix = records_to_fix.apply(lambda x: x * mult
                                              if x > minval / mult
                                              and x < maxval / mult
                                              else x)
    # Set any record that wasn't inside one of our identified populations to
    # NA -- we are saying that these are true outliers, which can't be part
    # of the population of values we are examining.
    records_to_fix = records_to_fix.apply(lambda x: np.nan
                                          if x < minval
                                          or x > maxval
                                          else x)
    # Add our fixed records back to the complete data series and return it
    fixed = fixed.append(records_to_fix)
    return(fixed)


##############################################################################
# DATABSE TABLE SPECIFIC PROCEDURES ##########################################
##############################################################################
def clean_fuel_ferc1(fuel_ferc1_df):
    """Transform FERC Form 1 fuel data for loading into PUDL Database."""
    # Make sure that we can't alter the dataframe that got passed in.
    fuel_ferc1_df = fuel_ferc1_df.copy()

    #########################################################################
    # PRUNE IRRELEVANT COLUMNS ##############################################
    #########################################################################
    fuel_ferc1_df.drop(['spplmnt_num', 'row_number', 'row_seq', 'row_prvlg',
                        'report_prd'], axis=1, inplace=True)

    #########################################################################
    # STANDARDIZE NAMES AND CODES ###########################################
    #########################################################################
    # Standardize plant_name capitalization and remove leading/trailing white
    # space -- necesary b/c plant_name is part of many foreign keys.
    fuel_ferc1_df['plant_name'] = fuel_ferc1_df['plant_name'].str.strip()
    fuel_ferc1_df['plant_name'] = fuel_ferc1_df['plant_name'].str.title()

    # Take the messy free-form fuel & fuel_unit fields, and do our best to
    # map them to some canonical categories... this is necessarily imperfect:
    fuel_ferc1_df.fuel = \
        clean_pudl.cleanstrings(fuel_ferc1_df.fuel,
                                pc.ferc1_fuel_strings,
                                unmapped=np.nan)
    fuel_ferc1_df.fuel_unit = \
        clean_pudl.cleanstrings(fuel_ferc1_df.fuel_unit,
                                pc.ferc1_fuel_unit_strings,
                                unmapped=np.nan)

    #########################################################################
    # PERFORM UNIT CONVERSIONS ##############################################
    #########################################################################

    # Replace fuel cost per kWh with fuel cost per MWh
    fuel_ferc1_df['fuel_cost_per_mwh'] = 1000 * fuel_ferc1_df['fuel_cost_kwh']
    fuel_ferc1_df.drop('fuel_cost_kwh', axis=1, inplace=True)

    # Replace BTU/kWh with millions of BTU/MWh
    fuel_ferc1_df['fuel_mmbtu_per_mwh'] = (1e3 / 1e6) * \
        fuel_ferc1_df['fuel_generaton']
    fuel_ferc1_df.drop('fuel_generaton', axis=1, inplace=True)

    # Convert from BTU/unit of fuel to 1e6 BTU/unit.
    fuel_ferc1_df['fuel_avg_mmbtu_per_unit'] = \
        fuel_ferc1_df['fuel_avg_heat'] / 1e6
    fuel_ferc1_df.drop('fuel_avg_heat', axis=1, inplace=True)

    #########################################################################
    # RENAME COLUMNS TO MATCH PUDL DB #######################################
    #########################################################################
    fuel_ferc1_df.rename(columns={
        # FERC 1 DB Name      PUDL DB Name
        'fuel_quantity': 'fuel_qty_burned',
        'fuel_cost_burned': 'fuel_cost_per_unit_burned',
        'fuel_cost_delvd': 'fuel_cost_per_unit_delivered',
        'fuel_cost_btu': 'fuel_cost_per_mmbtu'},
        inplace=True)

    #########################################################################
    # CORRECT DATA ENTRY ERRORS #############################################
    #########################################################################
    coal_mask = fuel_ferc1_df['fuel'] == 'coal'
    gas_mask = fuel_ferc1_df['fuel'] == 'gas'

    corrections = [
        # mult = 2000: reported in units of lbs instead of short tons
        # mult = 1e6:  reported BTUs instead of mmBTUs
        ['fuel_avg_mmbtu_per_unit', coal_mask, 9.0, 30.0, (2e3, 1e6)],

        # mult = 1e-2: reported cents/mmBTU instead of USD/mmBTU
        ['fuel_cost_per_mmbtu', coal_mask, 0.5, 6.0, (1e-2, )],

        # mult = 1e3: reported fuel quantity in cubic feet, not mcf
        # mult = 1e6: reported fuel quantity in BTU, not mmBTU
        ['fuel_avg_mmbtu_per_unit', gas_mask, 0.8, 1.2, (1e3, 1e6)],

        # mult = 1e-2: reported in cents/mmBTU instead of USD/mmBTU
        # Is $165/mmBTU really a reasonable upper bound? Seems huge.
        ['fuel_cost_per_mmbtu', gas_mask, 1.5, 165, (1e-2, )]
    ]

    for (coltofix, mask, minval, maxval, mults) in corrections:
        fuel_ferc1_df[coltofix] = \
            multiplicative_error_correction(fuel_ferc1_df[coltofix],
                                            mask, minval, maxval, mults)

    #########################################################################
    # REMOVE BAD DATA #######################################################
    #########################################################################
    # Drop any records that are missing data. This is a blunt instrument, to
    # be sure. In some cases we lose data here, because some utilities have
    # (for example) a "Total" line w/ only fuel_mmbtu_per_kwh on it. Grr.
    fuel_ferc1_df.dropna(inplace=True)

    return(fuel_ferc1_df)
