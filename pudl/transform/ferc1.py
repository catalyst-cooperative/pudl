"""
Routines for transforming FERC Form 1 data before loading into the PUDL DB.

This module provides a variety of functions that are used in cleaning up the
FERC Form 1 data prior to loading into our database. This includes adopting
standardized units and column names, standardizing the formatting of some
string values, and correcting data entry errors which we can infer based on
the existing data. It may also include removing bad data, or replacing it
with the appropriate NA values.
"""

import os.path
import scipy
import pandas as pd
import numpy as np

# These modules are required for the FERC Form 1 Plant ID & Time Series
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import LabelBinarizer
from sklearn.preprocessing import normalize
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import scale
from sklearn.base import BaseEstimator, ClassifierMixin

import pudl.constants as pc
import pudl.models.ferc1
import pudl.extract.ferc1
import pudl.transform.pudl
import pudl.settings

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
    return fixed


##############################################################################
# DATABSE TABLE SPECIFIC PROCEDURES ##########################################
##############################################################################
def fuel(ferc1_raw_dfs, ferc1_transformed_dfs):
    """
    Transform FERC Form 1 fuel data for loading into PUDL Database.

    This process includes converting some columns to be in terms of our
    preferred units, like MWh and mmbtu instead of kWh and btu. Plant names are
    also standardized (stripped & Title Case). Fuel and fuel unit strings are
    also standardized using our cleanstrings() function and string cleaning
    dictionaries found in pudl.constants.

    Args:
        ferc1_raw_dfs (dictionary of pandas.DataFrame): Each entry in this
            dictionary of DataFrame objects corresponds to a page from the
            EIA860 form, as reported in the Excel spreadsheets they distribute.
        ferc1_transformed_dfs (dictionary of DataFrames)

    Returns: transformed dataframe.
    """
    # grab table from dictionary of dfs
    fuel_ferc1_df = ferc1_raw_dfs['fuel_ferc1']
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
        pudl.transform.pudl.cleanstrings(fuel_ferc1_df.fuel,
                                         pc.ferc1_fuel_strings,
                                         unmapped=np.nan)

    fuel_ferc1_df.rename(columns={'fuel': 'fuel_type_pudl'}, inplace=True)

    fuel_ferc1_df.fuel_unit = \
        pudl.transform.pudl.cleanstrings(fuel_ferc1_df.fuel_unit,
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
    coal_mask = fuel_ferc1_df['fuel_type_pudl'] == 'coal'
    gas_mask = fuel_ferc1_df['fuel_type_pudl'] == 'gas'
    oil_mask = fuel_ferc1_df['fuel_type_pudl'] == 'oil'

    corrections = [
        # mult = 2000: reported in units of lbs instead of short tons
        # mult = 1e6:  reported BTUs instead of mmBTUs
        # minval and maxval of 10 and 29 mmBTUs are the range of values
        # specified by EIA 923 instructions at:
        # https://www.eia.gov/survey/form/eia_923/instructions.pdf
        ['fuel_avg_mmbtu_per_unit', coal_mask, 10.0, 29.0, (2e3, 1e6)],

        # mult = 1e-2: reported cents/mmBTU instead of USD/mmBTU
        # minval and maxval of .5 and 7.5 dollars per mmBTUs are the
        # end points of the primary distribution of EIA 923 fuel receipts
        # and cost per mmBTU data weighted by quantity delivered
        ['fuel_cost_per_mmbtu', coal_mask, 0.5, 7.5, (1e-2, )],

        # mult = 1e3: reported fuel quantity in cubic feet, not mcf
        # mult = 1e6: reported fuel quantity in BTU, not mmBTU
        # minval and maxval of .8 and 1.2 mmBTUs are the range of values
        # specified by EIA 923 instructions
        ['fuel_avg_mmbtu_per_unit', gas_mask, 0.8, 1.2, (1e3, 1e6)],

        # mult = 1e-2: reported in cents/mmBTU instead of USD/mmBTU
        # minval and maxval of 1 and 35 dollars per mmBTUs are the
        # end points of the primary distribution of EIA 923 fuel receipts
        # and cost per mmBTU data weighted by quantity delivered
        ['fuel_cost_per_mmbtu', gas_mask, 1, 35, (1e-2, )],

        # mult = 42: reported fuel quantity in gallons, not barrels
        # mult = 1e6: reported fuel quantity in BTU, not mmBTU
        # minval and maxval of 3 and 6.9 mmBTUs are the range of values
        # specified by EIA 923 instructions
        ['fuel_avg_mmbtu_per_unit', oil_mask, 3, 6.9, (42, )],

        # mult = 1e-2: reported in cents/mmBTU instead of USD/mmBTU
        # minval and maxval of 5 and 33 dollars per mmBTUs are the
        # end points of the primary distribution of EIA 923 fuel receipts
        # and cost per mmBTU data weighted by quantity delivered
        ['fuel_cost_per_mmbtu', oil_mask, 5, 33, (1e-2, )]
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

    ferc1_transformed_dfs['fuel_ferc1'] = fuel_ferc1_df

    return ferc1_transformed_dfs


def plants_steam(ferc1_raw_dfs, ferc1_transformed_dfs):
    """
    Transform FERC Form 1 plant_steam data for loading into PUDL Database.

    This includes converting to our preferred units of MWh and MW, as well as
    standardizing the strings describing the kind of plant and construction.

    Args:
        ferc1_raw_dfs (dictionary of pandas.DataFrame): Each entry in this
            dictionary of DataFrame objects corresponds to a page from the
            EIA860 form, as reported in the Excel spreadsheets they distribute.
        ferc1_transformed_dfs (dictionary of DataFrames)

    Returns: transformed dataframe.
    """
    # grab table from dictionary of dfs
    ferc1_steam_df = ferc1_raw_dfs['plants_steam_ferc1']

    # Discard DataFrame columns that we aren't pulling into PUDL:
    ferc1_steam_df.drop(['spplmnt_num', 'row_number', 'row_seq', 'row_prvlg',
                         'report_prd'], axis=1, inplace=True)

    # Standardize plant_name capitalization and remove leading/trailing white
    # space -- necesary b/c plant_name is part of many foreign keys.
    ferc1_steam_df['plant_name'] = ferc1_steam_df['plant_name'].str.strip()
    ferc1_steam_df['plant_name'] = ferc1_steam_df['plant_name'].str.title()

    # Take the messy free-form construction_type and plant_kind fields, and do
    # our best to map them to some canonical categories...
    # this is necessarily imperfect:

    ferc1_steam_df.type_const = \
        pudl.transform.pudl.cleanstrings(ferc1_steam_df.type_const,
                                         pc.ferc1_construction_type_strings,
                                         unmapped=np.nan)
    ferc1_steam_df.plant_kind = \
        pudl.transform.pudl.cleanstrings(ferc1_steam_df.plant_kind,
                                         pc.ferc1_plant_kind_strings,
                                         unmapped=np.nan)

    # Force the construction and installation years to be numeric values, and
    # set them to NA if they can't be converted. (table has some junk values)
    ferc1_steam_df['yr_const'] = pd.to_numeric(
        ferc1_steam_df['yr_const'],
        errors='coerce')
    ferc1_steam_df['yr_installed'] = pd.to_numeric(
        ferc1_steam_df['yr_installed'],
        errors='coerce')

    # Converting everything to per MW and MWh units...
    ferc1_steam_df['cost_per_mw'] = 1000 * ferc1_steam_df['cost_per_kw']
    ferc1_steam_df.drop('cost_per_kw', axis=1, inplace=True)
    ferc1_steam_df['net_generation_mwh'] = \
        ferc1_steam_df['net_generation'] / 1000
    ferc1_steam_df.drop('net_generation', axis=1, inplace=True)
    ferc1_steam_df['expns_per_mwh'] = 1000 * ferc1_steam_df['expns_kwh']
    ferc1_steam_df.drop('expns_kwh', axis=1, inplace=True)

    ferc1_steam_df.rename(columns={
        # FERC 1 DB Name      PUDL DB Name
        'yr_const': 'year_constructed',
        'type_const': 'construction_type',
        'asset_retire_cost': 'asset_retirement_cost',
        'yr_installed': 'year_installed',
        'tot_capacity': 'total_capacity_mw',
        'peak_demand': 'peak_demand_mw',
        'plnt_capability': 'plant_capability_mw',
        'when_limited': 'water_limited_mw',
        'when_not_limited': 'not_water_limited_mw',
        'avg_num_of_emp': 'avg_num_employees',
        'net_generation': 'net_generation_mwh',
        'cost_of_plant_to': 'total_cost_of_plant',
        'expns_steam_othr': 'expns_steam_other',
        'expns_engnr': 'expns_engineering',
        'tot_prdctn_expns': 'expns_production_total'},
        inplace=True)

    ferc1_transformed_dfs['plants_steam_ferc1'] = ferc1_steam_df

    return ferc1_transformed_dfs


def plants_small(ferc1_raw_dfs, ferc1_transformed_dfs):
    """
    Transform FERC Form 1 plant_small data for loading into PUDL Database.

    This FERC Form 1 table contains information about a large number of small
    plants, including many small hydroelectric and other renewable generation
    facilities. Unfortunately the data is not well standardized, and so the
    plants have been categorized manually, with the results of that
    categorization stored in an Excel spreadsheet. This function reads in the
    plant type data from the spreadsheet and merges it with the rest of the
    information from the FERC DB based on record number, FERC respondent ID,
    and report year. When possible the FERC license number for small hydro
    plants is also manually extracted from the data.

    This categorization will need to be renewed with each additional year of
    FERC data we pull in. As of v0.1 the small plants have been categorized
    for 2004-2015.

    Args:
        ferc1_raw_dfs (dictionary of pandas.DataFrame): Each entry in this
            dictionary of DataFrame objects corresponds to a page from the
            EIA860 form, as reported in the Excel spreadsheets they distribute.
        ferc1_transformed_dfs (dictionary of DataFrames)

    Returns: transformed dataframe.
    """
    # grab table from dictionary of dfs
    ferc1_small_df = ferc1_raw_dfs['plants_small_ferc1']
    # Standardize plant_name_raw capitalization and remove leading/trailing
    # white space -- necesary b/c plant_name_raw is part of many foreign keys.
    ferc1_small_df['plant_name'] = \
        ferc1_small_df['plant_name'].str.strip()
    ferc1_small_df['plant_name'] = \
        ferc1_small_df['plant_name'].str.title()

    ferc1_small_df['kind_of_fuel'] = ferc1_small_df['kind_of_fuel'].str.strip()
    ferc1_small_df['kind_of_fuel'] = ferc1_small_df['kind_of_fuel'].str.title()

    # Force the construction and installation years to be numeric values, and
    # set them to NA if they can't be converted. (table has some junk values)
    ferc1_small_df['yr_constructed'] = pd.to_numeric(
        ferc1_small_df['yr_constructed'],
        errors='coerce')
    # Convert from cents per mmbtu to dollars per mmbtu to be consistent
    # with the f1_fuel table data. Also, let's use a clearer name.
    ferc1_small_df['fuel_cost_per_mmbtu'] = ferc1_small_df['fuel_cost'] / 100.0
    ferc1_small_df.drop('fuel_cost', axis=1, inplace=True)

    # Create a single "record number" for the individual lines in the FERC
    # Form 1 that report different small plants, so that we can more easily
    # tell whether they are adjacent to each other in the reporting.
    ferc1_small_df['record_number'] = 46 * ferc1_small_df['spplmnt_num'] + \
        ferc1_small_df['row_number']

    # Unforunately the plant types were not able to be parsed automatically
    # in this table. It's been done manually for 2004-2015, and the results
    # get merged in in the following section.
    small_types_file = os.path.join(pudl.settings.PUDL_DIR,
                                    'results',
                                    'ferc1_small_plants',
                                    'small_plants_2004-2016.xlsx')
    small_types_df = pd.read_excel(small_types_file)

    # Only rows with plant_type set will give us novel information.
    small_types_df.dropna(subset=['plant_type', ], inplace=True)
    # We only need this small subset of the columns to extract the plant type.
    small_types_df = small_types_df[['report_year', 'respondent_id',
                                     'record_number', 'plant_name_clean',
                                     'plant_type', 'ferc_license']]

    # Munge the two dataframes together, keeping everything from the
    # frame we pulled out of the FERC1 DB, and supplementing it with the
    # plant_name, plant_type, and ferc_license fields from our hand
    # made file.
    ferc1_small_df = pd.merge(ferc1_small_df,
                              small_types_df,
                              how='left',
                              on=['report_year',
                                  'respondent_id',
                                  'record_number'])

    # We don't need to pull these columns into PUDL, so drop them:
    ferc1_small_df.drop(['row_seq', 'row_prvlg', 'report_prd',
                         'row_number', 'spplmnt_num', 'record_number'],
                        axis=1, inplace=True)

    # Standardize plant_name capitalization and remove leading/trailing white
    # space, so that plant_name matches formatting of plant_name_raw
    ferc1_small_df['plant_name_clean'] = \
        ferc1_small_df['plant_name_clean'].str.strip()
    ferc1_small_df['plant_name_clean'] = \
        ferc1_small_df['plant_name_clean'].str.title()

    # in order to create one complete column of plant names, we have to use the
    # cleaned plant names when available and the orignial plant names when the
    # cleaned version is not available, but the strings first need cleaning
    ferc1_small_df['plant_name_clean'] = \
        ferc1_small_df['plant_name_clean'].fillna(value="")
    ferc1_small_df['plant_name_clean'] = ferc1_small_df.apply(
        lambda row: row['plant_name']
        if (row['plant_name_clean'] == "")
        else row['plant_name_clean'],
        axis=1)

    # now we don't need the uncleaned version anymore
    # ferc1_small_df.drop(['plant_name'], axis=1, inplace=True)

    ferc1_small_df.rename(columns={
        # FERC 1 DB Name      PUDL DB Name
        'yr_constructed': 'year_constructed',
        'capacity_rating': 'total_capacity_mw',
        'net_demand': 'peak_demand_mw',
        'net_generation': 'net_generation_mwh',
        'plant_cost': 'total_cost_of_plant',
        'plant_cost_mw': 'cost_of_plant_per_mw',
        'operation': 'cost_of_operation',
        'expns_maint': 'expns_maintenance',
        'fuel_cost': 'fuel_cost_per_mmbtu'},
        # 'plant_name': 'plant_name_raw',
        # 'plant_name_clean': 'plant_name'},
        inplace=True)

    ferc1_transformed_dfs['plants_small_ferc1'] = ferc1_small_df

    return ferc1_transformed_dfs


def plants_hydro(ferc1_raw_dfs, ferc1_transformed_dfs):
    """
    Transform FERC Form 1 plant_hydro data for loading into PUDL Database.

    Standardizes plant names (stripping whitespace and Using Title Case).  Also
    converts into our preferred units of MW and MWh.

    Args:
        ferc1_raw_dfs (dictionary of pandas.DataFrame): Each entry in this
            dictionary of DataFrame objects corresponds to a page from the
            EIA860 form, as reported in the Excel spreadsheets they distribute.
        ferc1_transformed_dfs (dictionary of DataFrames)

    Returns: transformed dataframe.
    """
    # grab table from dictionary of dfs
    ferc1_hydro_df = ferc1_raw_dfs['plants_hydro_ferc1']

    ferc1_hydro_df.drop(['spplmnt_num', 'row_number', 'row_seq', 'row_prvlg',
                         'report_prd'], axis=1, inplace=True)

    # Standardize plant_name capitalization and remove leading/trailing white
    # space -- necesary b/c plant_name is part of many foreign keys.
    ferc1_hydro_df['plant_name'] = ferc1_hydro_df['plant_name'].str.strip()
    ferc1_hydro_df['plant_name'] = ferc1_hydro_df['plant_name'].str.title()

    # Converting kWh to MWh
    ferc1_hydro_df['net_generation_mwh'] = \
        ferc1_hydro_df['net_generation'] / 1000.0
    ferc1_hydro_df.drop('net_generation', axis=1, inplace=True)
    # Converting cost per kW installed to cost per MW installed:
    ferc1_hydro_df['cost_per_mw'] = ferc1_hydro_df['cost_per_kw'] * 1000.0
    ferc1_hydro_df.drop('cost_per_kw', axis=1, inplace=True)
    # Converting kWh to MWh
    ferc1_hydro_df['expns_per_mwh'] = ferc1_hydro_df['expns_kwh'] * 1000.0
    ferc1_hydro_df.drop('expns_kwh', axis=1, inplace=True)

    ferc1_hydro_df['yr_const'] = pd.to_numeric(
        ferc1_hydro_df['yr_const'],
        errors='coerce')
    ferc1_hydro_df['yr_installed'] = pd.to_numeric(
        ferc1_hydro_df['yr_installed'],
        errors='coerce')
    ferc1_hydro_df.dropna(inplace=True)
    ferc1_hydro_df.rename(columns={
        # FERC1 DB          PUDL DB
        'project_no': 'project_number',
        'yr_const': 'year_constructed',
        'plant_const': 'plant_construction',
        'yr_installed': 'year_installed',
        'tot_capacity': 'total_capacity_mw',
        'peak_demand': 'peak_demand_mw',
        'plant_hours': 'plant_hours_connected_while_generating',
        'favorable_cond': 'net_capacity_favorable_conditions_mw',
        'adverse_cond': 'net_capacity_adverse_conditions_mw',
        'avg_num_of_emp': 'avg_num_employees',
        'cost_of_land': 'cost_land',
        'expns_engnr': 'expns_engineering',
        'expns_total': 'expns_production_total',
        'asset_retire_cost': 'asset_retirement_cost'
    }, inplace=True)

    ferc1_transformed_dfs['plants_hydro_ferc1'] = ferc1_hydro_df

    return ferc1_transformed_dfs


def plants_pumped_storage(ferc1_raw_dfs, ferc1_transformed_dfs):
    """
    Transform FERC Form 1 pumped storage data for loading into PUDL Database.

    Standardizes plant names (stripping whitespace and Using Title Case).  Also
    converts into our preferred units of MW and MWh.

    Args:
        ferc1_raw_dfs (dictionary of pandas.DataFrame): Each entry in this
            dictionary of DataFrame objects corresponds to a page from the
            EIA860 form, as reported in the Excel spreadsheets they distribute.
        ferc1_transformed_dfs (dictionary of DataFrames)

    Returns: transformed dataframe.
    """
    # grab table from dictionary of dfs
    ferc1_pumped_storage_df = ferc1_raw_dfs['plants_pumped_storage_ferc1']

    ferc1_pumped_storage_df.drop(['spplmnt_num', 'row_number', 'row_seq',
                                  'row_prvlg', 'report_prd'],
                                 axis=1, inplace=True)

    # Standardize plant_name capitalization and remove leading/trailing white
    # space -- necesary b/c plant_name is part of many foreign keys.
    ferc1_pumped_storage_df['plant_name'] = \
        ferc1_pumped_storage_df['plant_name'].str.strip()
    ferc1_pumped_storage_df['plant_name'] = \
        ferc1_pumped_storage_df['plant_name'].str.title()

    # Converting kWh to MWh
    ferc1_pumped_storage_df['net_generation_mwh'] = \
        ferc1_pumped_storage_df['net_generation'] / 1000.0
    ferc1_pumped_storage_df.drop('net_generation', axis=1, inplace=True)

    ferc1_pumped_storage_df['energy_used_for_pumping_mwh'] = \
        ferc1_pumped_storage_df['energy_used'] / 1000.0
    ferc1_pumped_storage_df.drop('energy_used', axis=1, inplace=True)

    ferc1_pumped_storage_df['net_load_mwh'] = \
        ferc1_pumped_storage_df['net_load'] / 1000.0
    ferc1_pumped_storage_df.drop('net_load', axis=1, inplace=True)

    # Converting cost per kW installed to cost per MW installed:
    ferc1_pumped_storage_df['cost_per_mw'] = \
        ferc1_pumped_storage_df['cost_per_kw'] * 1000.0
    ferc1_pumped_storage_df.drop('cost_per_kw', axis=1, inplace=True)

    ferc1_pumped_storage_df['expns_per_mwh'] = \
        ferc1_pumped_storage_df['expns_kwh'] * 1000.0
    ferc1_pumped_storage_df.drop('expns_kwh', axis=1, inplace=True)

    ferc1_pumped_storage_df['yr_const'] = pd.to_numeric(
        ferc1_pumped_storage_df['yr_const'],
        errors='coerce')
    ferc1_pumped_storage_df['yr_installed'] = pd.to_numeric(
        ferc1_pumped_storage_df['yr_installed'],
        errors='coerce')

    ferc1_pumped_storage_df.dropna(inplace=True)

    ferc1_pumped_storage_df.rename(columns={
        # FERC1 DB          PUDL DB
        'tot_capacity': 'total_capacity_mw',
        'project_no': 'project_number',
        'peak_demand': 'peak_demand_mw',
        'yr_const': 'year_constructed',
        'yr_installed': 'year_installed',
        'plant_hours': 'plant_hours_connected_while_generating',
        'plant_capability': 'plant_capability_mw',
        'avg_num_of_emp': 'avg_num_employees',
        'cost_wheels': 'cost_wheels_turbines_generators',
        'cost_electric': 'cost_equipment_electric',
        'cost_misc_eqpmnt': 'cost_equipment_misc',
        'cost_of_plant': 'cost_plant_total',
        'expns_water_pwr': 'expns_water_for_pwr',
        'expns_pump_strg': 'expns_pump_storage',
        'expns_misc_power': 'expns_generation_misc',
        'expns_misc_plnt': 'expns_misc_plant',
        'expns_producton': 'expns_production_before_pumping',
        'tot_prdctn_exns': 'expns_production_total'},
        inplace=True)

    ferc1_transformed_dfs['plants_pumped_storage_ferc1'] = ferc1_pumped_storage_df

    return ferc1_transformed_dfs


def plant_in_service(ferc1_raw_dfs, ferc1_transformed_dfs):
    """
    Transform FERC Form 1 plant_in_service data for loading into PUDL Database.

    This information is organized by FERC account, with each line of the FERC
    Form 1 having a different FERC account id (most are numeric and correspond
    to FERC's Uniform Electric System of Accounts). As of PUDL v0.1, this data
    is only valid from 2007 onward, as the line numbers for several accounts
    are different in earlier years.

    Args:
        ferc1_raw_dfs (dictionary of pandas.DataFrame): Each entry in this
            dictionary of DataFrame objects corresponds to a page from the
            EIA860 form, as reported in the Excel spreadsheets they distribute.
        ferc1_transformed_dfs (dictionary of DataFrames)

    Returns: transformed dataframe.
    """
    # grab table from dictionary of dfs
    ferc1_pis_df = ferc1_raw_dfs['plant_in_service_ferc1']
    # Discard DataFrame columns that we aren't pulling into PUDL. For the
    # Plant In Service table, we need to hold on to the row_number because it
    # corresponds to a FERC account number.
    ferc1_pis_df.drop(['spplmnt_num', 'row_seq', 'row_prvlg', 'report_prd'],
                      axis=1, inplace=True)

    # Now we need to add a column to the DataFrame that has the FERC account
    # IDs corresponding to the row_number that's already in there...
    ferc_accts_df = pc.ferc_electric_plant_accounts.drop(
        ['ferc_account_description'], axis=1)
    ferc_accts_df.dropna(inplace=True)
    ferc_accts_df['row_number'] = ferc_accts_df['row_number'].astype(int)

    ferc1_pis_df = pd.merge(ferc1_pis_df, ferc_accts_df,
                            how='left', on='row_number')
    ferc1_pis_df.drop('row_number', axis=1, inplace=True)

    ferc1_pis_df.rename(columns={
        # FERC 1 DB Name  PUDL DB Name
        'begin_yr_bal': 'beginning_year_balance',
        'addition': 'additions',
        'yr_end_bal': 'year_end_balance'},
        inplace=True)

    ferc1_transformed_dfs['plant_in_service_ferc1'] = ferc1_pis_df

    return ferc1_transformed_dfs


def purchased_power(ferc1_raw_dfs, ferc1_transformed_dfs):
    """
    Transform FERC Form 1 pumped storage data for loading into PUDL Database.

    This table has data about inter-untility power purchases into the PUDL DB.
    This includes how much electricty was purchased, how much it cost, and who
    it was purchased from. Unfortunately the field describing which other
    utility the power was being bought from is poorly standardized, making it
    difficult to correlate with other data. It will need to be categorized by
    hand or with some fuzzy matching eventually.

    Args:
        ferc1_raw_dfs (dictionary of pandas.DataFrame): Each entry in this
            dictionary of DataFrame objects corresponds to a page from the
            EIA860 form, as reported in the Excel spreadsheets they distribute.
        ferc1_transformed_dfs (dictionary of DataFrames)

    Returns: transformed dataframe.
    """
    # grab table from dictionary of dfs
    ferc1_purchased_pwr_df = ferc1_raw_dfs['purchased_power_ferc1']

    ferc1_purchased_pwr_df.drop(['spplmnt_num', 'row_number', 'row_seq',
                                 'row_prvlg', 'report_prd'],
                                axis=1, inplace=True)
    ferc1_purchased_pwr_df.replace(to_replace='', value=np.nan, inplace=True)
    ferc1_purchased_pwr_df.dropna(subset=['sttstcl_clssfctn',
                                          'rtsched_trffnbr'], inplace=True)

    ferc1_purchased_pwr_df.rename(columns={
        # FERC 1 DB Name  PUDL DB Name
        'athrty_co_name': 'authority_company_name',
        'sttstcl_clssfctn': 'statistical_classification',
        'rtsched_trffnbr': 'rate_schedule_tariff_number',
        'avgmth_bill_dmnd': 'average_billing_demand',
        'avgmth_ncp_dmnd': 'average_monthly_ncp_demand',
        'avgmth_cp_dmnd': 'average_monthly_cp_demand',
        'mwh_recv': 'mwh_received',
        'mwh_delvd': 'mwh_delivered',
        'dmnd_charges': 'demand_charges',
        'erg_charges': 'energy_charges',
        'othr_charges': 'other_charges',
        'settlement_tot': 'settlement_total'},
        inplace=True)

    ferc1_transformed_dfs['purchased_power_ferc1'] = ferc1_purchased_pwr_df

    return ferc1_transformed_dfs


def accumulated_depreciation(ferc1_raw_dfs, ferc1_transformed_dfs):
    """
    Transform FERC Form 1 depreciation data for loading into PUDL Database.

    This information is organized by FERC account, with each line of the FERC
    Form 1 having a different descriptive identifier like 'balance_end_of_year'
    or 'transmission'.

    Args:
        ferc1_raw_dfs (dictionary of pandas.DataFrame): Each entry in this
            dictionary of DataFrame objects corresponds to a page from the
            EIA860 form, as reported in the Excel spreadsheets they distribute.
        ferc1_transformed_dfs (dictionary of DataFrames)

    Returns: transformed dataframe.
    """
    # grab table from dictionary of dfs
    ferc1_apd_df = ferc1_raw_dfs['accumulated_depreciation_ferc1']

    # Discard DataFrame columns that we aren't pulling into PUDL. For
    ferc1_apd_df.drop(['spplmnt_num', 'row_seq',
                       'row_prvlg', 'item', 'report_prd'],
                      axis=1, inplace=True)

    ferc1_acct_apd = pc.ferc_accumulated_depreciation.drop(
        ['ferc_account_description'], axis=1)
    ferc1_acct_apd.dropna(inplace=True)
    ferc1_acct_apd['row_number'] = ferc1_acct_apd['row_number'].astype(int)

    ferc1_accumdepr_prvsn_df = pd.merge(ferc1_apd_df, ferc1_acct_apd,
                                        how='left', on='row_number')
    ferc1_accumdepr_prvsn_df.drop('row_number', axis=1, inplace=True)

    ferc1_accumdepr_prvsn_df.rename(columns={
        # FERC1 DB   PUDL DB
        'total_cde': 'total'},
        inplace=True)

    ferc1_transformed_dfs['accumulated_depreciation_ferc1'] = \
        ferc1_accumdepr_prvsn_df

    return ferc1_transformed_dfs


###############################################################################
# Identifying FERC Plants
###############################################################################
# Sadly FERC doesn't provide any kind of real IDs for the plants that report to
# them -- all we have is their names (a freeform string) and the data that is
# reported alongside them. This is often enough information to be able to
# recognize which records ought to be associated with each other year to year
# to create a continuous time series. However, we want to do that
# programmatically, which means using some clustering / categorization tools
# from scikit-learn

# These sets of strings allow us to simplify the plant_kind field. They came
# from Uday & Laura at CPI. We may change/replace them, and they'll need to go
# into the constants module eventually. But for now they're here for coziness.
diesel_strings = [
    'DIESEL',
    'Diesel Engine',
    'Diesel Turbine',
]

geothermal_strings = [
    'Steam - Geothermal',
]
natural_gas_strings = [
    'Combined Cycle',
    'Combustion Turbine',
    'GT',
    'GAS TURBINE',
    'Comb. Turbine',
    'Gas Turbine #1',
    'Combine Cycle Oper',
    'Combustion',
    'Combined',
    'Gas Turbine/Steam',
    'Gas Turbine Peaker',
    'Gas Turbine - Note 1',
    'Resp Share Gas Note3',
    'Gas Turbines',
    'Simple Cycle',
    'Gas / Steam',
    'GasTurbine',
    'Combine Cycle',
    'CTG/Steam-Gas',
    'GTG/Gas',
    'CTG/Steam -Gas',
    'Steam/Gas Turbine',
    'CombustionTurbine',
    'Gas Turbine-Simple',
    'STEAM & GAS TURBINE',
    'Gas & Steam Turbine',
    'Gas',
    'Gas Turbine (2)',
    'COMBUSTION AND GAS',
    'Com Turbine Peaking',
    'Gas Turbine Peaking',
    'Comb Turb Peaking',
    'JET ENGINE',
    'Comb. Cyc',
    'Com. Cyc',
    'Com. Cycle',
    'GAS TURB-COMBINED CY',
    'Gas Turb',
    'Combined Cycle - 40%',
    'IGCC/Gas Turbine',
    'CC',
    'Combined Cycle Oper',
    'Simple Cycle Turbine',
    'Steam and CC',
    'Com Cycle Gas Turb',
    'I.C.E/  Gas Turbine',
    'Combined Cycle CTG',
    'GAS-TURBINE',
    'Gas Expander Turbine',
    'Gas Turbine (Leased)',
    'Gas Turbine # 1',
    'Gas Turbine (Note 1)',
    'COMBUSTINE TURBINE',
    'Gas Turb, Int. Comb.',
    'Combined Turbine',
    'Comb Turb Peak Units',
    'Combustion Tubine',
    'Comb. Cycle',
    'COMB.TURB.PEAK.UNITS',
    'Steam  and  CC',
    'I.C.E. /Gas Turbine',
    'Conbustion Turbine',
    'Gas Turbine/Int Comb',
    'Steam & CC',
    'GAS TURB. & HEAT REC',
    'Gas Turb/Comb. Cyc',
    'Comb. Turine',
]
nuclear_strings = [
    'Nuclear',
    'Nuclear (3)',
]
other_strings = [
    'IC',
    'Internal Combustion',
    'Int Combust - Note 1',
    'Resp. Share - Note 2',
    'Int. Combust - Note1',
    'Resp. Share - Note 4',
    'Resp Share - Note 5',
    'Resp. Share - Note 7',
    'Internal Comb Recip',
    'Reciprocating Engine',
    'Internal Comb',
    'Resp. Share - Note 8',
    'Resp. Share - Note 9',
    'Resp Share - Note 11',
    'Resp. Share - Note 6',
    'INT.COMBUSTINE',
    'Steam (Incl I.C.)',
    'Other',
    'Int Combust (Note 1)',
    'Resp. Share (Note 2)',
    'Int. Combust (Note1)',
    'Resp. Share (Note 8)',
    'Resp. Share (Note 9)',
    'Resp Share (Note 11)',
    'Resp. Share (Note 4)',
    'Resp. Share (Note 6)',
    'Plant retired- 2013',
    'Retired - 2013',
]
steam_strings = [
    'Steam',
    'Steam Units 1, 2, 3',
    'Resp Share St Note 3',
    'Steam Turbine',
    'Steam-Internal Comb',
    'IGCC',
    'Steam- 72%',
    'Steam (1)',
    'Steam (1)',
    'Steam Units 1,2,3',
    'Steam/Fossil',
    'Steams',
    'Steam - 72%',
    'Steam - 100%',
    'Stream',
    'Steam Units 4, 5',
    'Steam - 64%',
    'Common',
    'Steam (A)',
    'Coal',
    'Steam;Retired - 2013',
    'Steam Units 4 & 6',
]
wind_strings = [
    'Wind',
    'Wind Turbine',
    'Wind - Turbine',
    'Wind Energy',
]
solar_strings = [
    'Solar Photovoltaic',
    'Solar Thermal',
    'SOLAR PROJECT',
    'Solar',
    'Photovoltaic',
]
cpi_plant_kind_map = {
    'natural_gas': natural_gas_strings,
    'diesel': diesel_strings,
    'geothermal': geothermal_strings,
    'nuclear': nuclear_strings,
    'steam': steam_strings,
    'wind': wind_strings,
    'solar': solar_strings,
    'other': other_strings,
}


def prepare_plants(ferc1_steam):
    """Prepare raw FERC Form 1 plant records for identification."""

    ferc1_steam = ferc1_steam.drop(
        ['row_seq', 'row_prvlg', 'report_prd'], axis=1)
    ferc1_steam['plant_kind_cpi'] = \
        pudl.transform.pudl.cleanstrings(ferc1_steam.plant_kind,
                                         cpi_plant_kind_map,
                                         unmapped='')
    # Create a unique inter-year FERC table record ID:
    ferc1_steam['record_id'] = \
        ferc1_steam.report_year.astype(str) + \
        ferc1_steam.respondent_id.astype(str) + \
        ferc1_steam.spplmnt_num.astype(str) + \
        ferc1_steam.row_number.astype(str)
    # ferc1_steam['record_id'] = ferc1_steam.record_id.astype(int)

    # If there's no generation, no fuel expenses, and no total expenses...
    # probably the record is not relevant.
    mask_one = \
        ((ferc1_steam.net_generation == 0) |
         (ferc1_steam.net_generation.isnull())) & \
        ((ferc1_steam.expns_fuel == 0) | (ferc1_steam.expns_fuel.isnull())) & \
        ((ferc1_steam.tot_prdctn_expns == 0) |
         (ferc2_steam.tot_prdctn_expns.isnull()))
    ferc1_steam = ferc1_steam[~mask_one].reset_index()
    # Simplify the plant names for matching purposes.
    # May also want to remove non-alphanumeric characters
    ferc1_steam['plant_name'] = \
        ferc1_steam.plant_name.str.strip().str.lower().str.replace('\s+', ' ')

    # These are the columns which we're using to infer inter-year time series,
    # based on the experience of doing it by hand.
    matching_cols = [
        'record_id',
        'report_year',
        'spplmnt_num',
        'row_number',
        'respondent_id',
        'plant_name',
        'plant_kind_cpi',
        'yr_const',
        'tot_capacity'
    ]

    # This was purely for readability in the notebook:
    # ferc1_tomatch = ferc1_steam[matching_cols]
    # ferc1_steam_new = ferc1_steam.drop(matching_cols, axis=1)
    # ferc1_steam_new = pd.merge(
    #     ferc1_tomatch, ferc1_steam_new, left_index=True, right_index=True)


def vectorize_plants(plants,
                     ngram_range=(2, 5),
                     sup_num_wt=1.0,
                     row_num_wt=1.0,
                     plant_name_wt=1.0,
                     yr_const_wt=1.0,
                     respondent_wt=1.0,
                     plant_kind_wt=1.0,
                     capacity_wt=1.0):
    """
    Vectorize and weight FERC Form 1 large plant records for clustering.

    Several vectorization methods are used for different features:
      - TF-IDF with character based n-grams for plant names
      - MinMaxScaler with mean of zero and range (-1,1) for plant capacity
      - Categorical binary weights for:
        - respondent_id
        - construction year
        - plant type
        - row number
        - supplement number

    The vectorized plant features are normalized according to the weights that
    were passed in before the feature matrix is returned.
    """

    plant_name_vectorizer = TfidfVectorizer(
        analyzer='char', ngram_range=ngram_range)
    plant_name_vectors = plant_name_vectorizer.fit_transform(plants.plant_name)

    scaler = MinMaxScaler()
    capacity_vectors = scaler.fit_transform(
        plants.tot_capacity.values.reshape(-1, 1))

    lb_yr_const = LabelBinarizer()
    yr_const_vectors = scipy.sparse.csr_matrix(
        lb_yr_const.fit_transform(plants.yr_const))

    lb_sup_num = LabelBinarizer()
    sup_num_vectors = scipy.sparse.csr_matrix(
        lb_sup_num.fit_transform(plants.spplmnt_num))

    lb_row_num = LabelBinarizer()
    row_num_vectors = scipy.sparse.csr_matrix(
        lb_row_num.fit_transform(plants.row_number))

    lb_respondent = LabelBinarizer()
    respondent_vectors = scipy.sparse.csr_matrix(
        lb_respondent.fit_transform(plants.respondent_id))

    lb_plantkind = LabelBinarizer()
    plant_kind_vectors = scipy.sparse.csr_matrix(
        lb_plantkind.fit_transform(plants.plant_kind_cpi))

    plant_vectors = normalize(scipy.sparse.hstack([
        plant_name_vectors * plant_name_wt,
        yr_const_vectors * yr_const_wt,
        respondent_vectors * respondent_wt,
        plant_kind_vectors * plant_kind_wt,
        capacity_vectors * capacity_wt,
        sup_num_vectors * sup_num_wt,
        row_num_vectors * row_num_wt
    ]))
    return plant_vectors


def best_by_year(plants_df, sim_df, min_sim=0.8):
    """Find the best match for each plant record in each other year."""
    out_df = plants_df.copy()

    # only keep similarity matrix entries above our minimum threshold:
    sim_df = sim_df[sim_df >= min_sim]

    # Add a column for each of the years, in which we will store indices of the
    # records which best match the record in question:
    years = plants_df.report_year.unique()
    for yr in years:
        newcol = yr
        out_df[newcol] = -1

    # seed_yr is the year we are matching *from* -- we do the entire matching
    # process from each year, since it may not be symmetric:
    for seed_yr in years:
        seed_idx = plants_df.index[plants_df.report_year == seed_yr]
        # match_yr is all the other years, in which we are finding the best match
        for match_yr in years:
            bestof_yr = match_yr
            match_idx = plants_df.index[plants_df.report_year == match_yr]
            # For each record specified by seed_idx, obtain the index of
            # the record within match_idx that that is the most similar.
            best_idx = sim_df.iloc[seed_idx, match_idx].idxmax(axis=1)
            out_df[bestof_yr].iloc[seed_idx] = best_idx

        #out_df = pd.merge(out_df, sim_df.iloc[yr_idx, y_idx].idxmax(axis=1).to_frame(), left_index=True, right_index=True)
    return(out_df)


class FERCPlantClassifier(BaseEstimator, ClassifierMixin):
    """A classifier for identifying FERC plants in FERC Form 1 data."""

    def __init__(self,
                 ngram_min=2,
                 ngram_max=5,
                 sup_num_wt=1.0,
                 row_num_wt=1.0,
                 plant_name_wt=1.0,
                 yr_const_wt=1.0,
                 respondent_wt=1.0,
                 plant_kind_wt=1.0,
                 capacity_wt=1.0):
        """
        Called when initializing the classifier.
        """
        self.ngram_min = ngram_min
        self.ngram_max = ngram_max
        self.sup_num_wt = sup_num_wt
        self.row_num_wt = row_num_wt
        self.plant_name_wt = plant_name_wt
        self.yr_const_wt = yr_const_wt
        self.respondent_wt = respondent_wt
        self.plant_kind_wt = plant_kind_wt
        self.capacity_wt = capacity_wt

    def fit(self, X, y=None):
        """
        This should fit classifier. All the "work" should be done here.

        Note: assert is not a good choice here and you should rather
        use try/except blog with exceptions. This is just for short syntax.
        """

        assert (type(self.ngram_min) == int), "ngram_min must be integer"
        assert (type(self.ngram_max) == int), "ngram_max must be integer"

        self.treshold_ = (sum(X) / len(X)) + self.intValue  # mean + intValue

        return self

    def _meaning(self, x):
        # returns True/False according to fitted classifier
        # notice underscore on the beginning
        return(True if x >= self.treshold_ else False)

    def predict(self, X, y=None):
        try:
            getattr(self, "treshold_")
        except AttributeError:
            raise RuntimeError(
                "You must train classifer before predicting data!")

        return([self._meaning(x) for x in X])

    def score(self, X, y=None):
        # counts number of values bigger than mean
        return(sum(self.predict(X)))
