import logging
import os.path
import datetime
import time
import pandas as pd
import shutil

import pudl

import pudl.constants as pc
from pudl.settings import SETTINGS

logger = logging.getLogger(__name__)


###############################################################################
# EIA EXPORT FUNCTIONS
###############################################################################

def _input_validate_eia(inputs):
    # extract all of the inputs for the EIA ETL function
    # empty dictionary to compile inputs
    eia_input_dict = {}
    # when nothing is set in the settings file, the years will default as none
    try:
        eia_input_dict['eia860_years'] = inputs['eia860_years']
    except KeyError:
        eia_input_dict['eia860_years'] = []

    # the tables will default to all of the tables if nothing is given
    try:
        eia_input_dict['eia860_tables'] = inputs['eia860_tables']
    except KeyError:
        eia_input_dict['eia860_tables'] = pc.pudl_tables['eia860']

    try:
        eia_input_dict['eia923_years'] = inputs['eia923_years']
    except KeyError:
        eia_input_dict['eia923_years'] = []

    try:
        eia_input_dict['eia923_tables'] = inputs['eia923_tables']
    except KeyError:
        eia_input_dict['eia923_tables'] = pc.pudl_tables['eia923']

    # Validate the inputs
    if eia_input_dict['eia860_tables']:
        for table in eia_input_dict['eia860_tables']:
            if table not in pc.eia860_pudl_tables:
                raise AssertionError(
                    f"Unrecognized EIA 860 table: {table}"
                )

    if eia_input_dict['eia923_tables']:
        for table in eia_input_dict['eia923_tables']:
            if table not in pc.eia923_pudl_tables:
                raise AssertionError(
                    f"Unrecogized EIA 923 table: {table}"
                )

    for year in eia_input_dict['eia860_years']:
        if year not in pc.working_years['eia860']:
            raise AssertionError(f"Unrecognized EIA 860 year: {year}")

    for year in eia_input_dict['eia923_years']:
        if year not in pc.working_years['eia923']:
            raise AssertionError(f"Unrecognized EIA 923 year: {year}")

    return eia_input_dict


def _load_static_tables_eia(pkg_dir):
    """
    Populate static EIA tables with constants for use as foreign keys.

    There are many values specified within the data that are essentially
    constant, but which we need to store for data validation purposes, for use
    as foreign keys.  E.g. the list of valid EIA fuel type codes, or the
    possible state and country codes indicating a coal delivery's location of
    origin. For now these values are primarily stored in a large collection of
    lists, dictionaries, and dataframes which are specified in the
    pudl.constants module.  This function uses those data structures to
    populate a bunch of small infrastructural tables within packages that
    include EIA tables.
    """
    # create dfs for tables with static data from constants.
    fuel_type_eia923 = \
        pd.DataFrame({'abbr': list(pc.fuel_type_eia923.keys()),
                      'fuel_type': list(pc.fuel_type_eia923.values())})

    prime_movers_eia923 = \
        pd.DataFrame({'abbr': list(pc.prime_movers_eia923.keys()),
                      'prime_mover': list(pc.prime_movers_eia923.values())})

    fuel_type_aer_eia923 = \
        pd.DataFrame({'abbr': list(pc.fuel_type_aer_eia923.keys()),
                      'fuel_type': list(pc.fuel_type_aer_eia923.values())})

    energy_source_eia923 = \
        pd.DataFrame({'abbr': list(pc.energy_source_eia923.keys()),
                      'source': list(pc.energy_source_eia923.values())})

    transport_modes_eia923 = \
        pd.DataFrame({'abbr': list(pc.transport_modes_eia923.keys()),
                      'mode': list(pc.transport_modes_eia923.values())})

    # compile the dfs in a dictionary, prep for dict_dump
    static_dfs = {'fuel_type_eia923': fuel_type_eia923,
                  'prime_movers_eia923': prime_movers_eia923,
                  'fuel_type_aer_eia923': fuel_type_aer_eia923,
                  'energy_source_eia923': energy_source_eia923,
                  'transport_modes_eia923': transport_modes_eia923}

    # run the dictionary of prepped static tables through dict_dump to make
    # CSVs
    pudl.load.dict_dump(static_dfs,
                        "Static EIA Tables",
                        need_fix_inting=pc.need_fix_inting,
                        pkg_dir=pkg_dir)
    return list(static_dfs.keys())


def _ETL_eia_pkg(inputs, pkg_dir):
    eia_inputs = _input_validate_eia(inputs)
    eia923_tables = eia_inputs['eia923_tables']
    eia923_years = eia_inputs['eia923_years']
    eia860_tables = eia_inputs['eia860_tables']
    eia860_years = eia_inputs['eia860_years']

    if (not eia923_tables or not eia923_years) and \
            (not eia860_tables or not eia860_years):
        logger.info('Not loading EIA.')
        return []

    # generate CSVs for the static EIA tables, return the list of tables
    static_tables = _load_static_tables_eia(pkg_dir)

    # Extract EIA forms 923, 860
    eia923_raw_dfs = pudl.extract.eia923.extract(eia923_years=eia923_years)
    eia860_raw_dfs = pudl.extract.eia860.extract(eia860_years=eia860_years)
    # Transform EIA forms 923, 860
    eia923_transformed_dfs = \
        pudl.transform.eia923.transform(eia923_raw_dfs,
                                        eia923_tables=eia923_tables)
    eia860_transformed_dfs = \
        pudl.transform.eia860.transform(eia860_raw_dfs,
                                        eia860_tables=eia860_tables)
    # create an eia transformed dfs dictionary
    eia_transformed_dfs = eia860_transformed_dfs.copy()
    eia_transformed_dfs.update(eia923_transformed_dfs.copy())

    entities_dfs, eia_transformed_dfs = \
        pudl.transform.eia.transform(eia_transformed_dfs,
                                     eia923_years=eia923_years,
                                     eia860_years=eia860_years)

    # Compile transformed dfs for loading...
    transformed_dfs = {"Entities": entities_dfs, "EIA": eia_transformed_dfs}
    # Load step
    for data_source, transformed_df in transformed_dfs.items():
        pudl.load.dict_dump(transformed_df,
                            data_source,
                            need_fix_inting=pc.need_fix_inting,
                            pkg_dir=pkg_dir)

    return list(eia_transformed_dfs.keys()) + list(entities_dfs.keys()) + static_tables

###############################################################################
# FERC1 EXPORT FUNCTIONS
###############################################################################


def _input_validate_ferc1(inputs):
    ferc1_dict = {}
    # pull out the inputs from the dictionary passed into this function
    try:
        ferc1_dict['ferc1_years'] = inputs['ferc1_years']
    except KeyError:
        ferc1_dict['ferc1_years'] = [None]
    # the tables will default to all of the tables if nothing is given
    try:
        ferc1_dict['ferc1_tables'] = inputs['ferc1_tables']
    except KeyError:
        ferc1_dict['ferc1_tables'] = pc.pudl_tables['ferc1']
    # if nothing is passed in, assume that we're not testing
    try:
        ferc1_dict['ferc1_testing'] = inputs['ferc1_testing']
    except KeyError:
        ferc1_dict['ferc1_testing'] = False

    try:
        ferc1_dict['debug'] = inputs['debug']
    except KeyError:
        ferc1_dict['debug'] = False

    if (not ferc1_dict['debug']) and (ferc1_dict['ferc1_tables']):
        for table in ferc1_dict['ferc1_tables']:
            if table not in pc.ferc1_pudl_tables:
                raise AssertionError(
                    f"Unrecognized FERC table: {table}."
                )

    return (ferc1_dict)


def _load_static_tables_ferc(pkg_dir):
    """
    Populate static PUDL tables with constants for use as foreign keys.

    There are many values specified within the data that are essentially
    constant, but which we need to store for data validation purposes, for use
    as foreign keys.  E.g. the list of valid EIA fuel type codes, or the
    possible state and country codes indicating a coal delivery's location of
    origin. For now these values are primarily stored in a large collection of
    lists, dictionaries, and dataframes which are specified in the
    pudl.constants module.  This function uses those data structures to
    populate a bunch of small infrastructural tables within the PUDL DB.
    """
    # create dfs for tables with static data from constants.
    ferc_accounts = pc.ferc_electric_plant_accounts.drop('row_number', axis=1).\
        replace({'ferc_account_description': r'\s+'}, ' ', regex=True).\
        rename(columns={'ferc_account_description': 'description'})

    ferc_depreciation_lines = pc.ferc_accumulated_depreciation.drop('row_number', axis=1).\
        rename(columns={'ferc_account_description': 'description'})

    # compile the dfs in a dictionary, prep for dict_dump
    static_dfs = {'ferc_accounts': ferc_accounts,
                  'ferc_depreciation_lines': ferc_depreciation_lines
                  }

    # run the dictionary of prepped static tables through dict_dump to make
    # CSVs
    pudl.load.dict_dump(static_dfs,
                        "Static FERC Tables",
                        need_fix_inting=pc.need_fix_inting,
                        pkg_dir=pkg_dir)

    return list(static_dfs.keys())


def _ETL_ferc1_pkg(inputs, pkg_dir):
    ferc1_inputs = _input_validate_ferc1(inputs)

    ferc1_years = ferc1_inputs['ferc1_years']
    ferc1_tables = ferc1_inputs['ferc1_tables']

    if not ferc1_years or not ferc1_tables:
        logger.info('Not loading FERC1')
        return []

    static_tables = _load_static_tables_ferc(pkg_dir)
    # Extract FERC form 1
    ferc1_raw_dfs = pudl.extract.ferc1.extract(ferc1_tables=ferc1_tables,
                                               ferc1_years=ferc1_years,
                                               testing=ferc1_inputs['ferc1_testing'])
    # Transform FERC form 1
    ferc1_transformed_dfs = pudl.transform.ferc1.transform(
        ferc1_raw_dfs, ferc1_tables=ferc1_tables)
    # Load FERC form 1
    pudl.load.dict_dump(ferc1_transformed_dfs,
                        "FERC 1",
                        need_fix_inting=pc.need_fix_inting,
                        pkg_dir=pkg_dir)
    return list(ferc1_transformed_dfs.keys()) + static_tables

###############################################################################
# EPA CEMPS EXPORT FUNCTIONS
###############################################################################


def _input_validate_epacems(inputs):
    fake_dict = {}
    print("no CEMS for now...")
    return fake_dict


def _ETL_cems_pkg(inputs, pkg_dir):
    print("you've made it this far ()!), but still no cems")
    return []

###############################################################################
# GLUE EXPORT FUNCTIONS
###############################################################################


def _ETL_glue(eia923_years, eia860_years, ferc1_years, out_dir):
    """
    Grab the glue tables and generates CSVs.

    Right now, this function only generates the glue between EIA and FERC

    """
    # grab the glue tables for ferc1 & eia
    glue_dfs = pudl.glue.ferc1_eia.glue(eia923_years,
                                        eia860_years,
                                        ferc1_years)

    pudl.load.dict_dump(glue_dfs,
                        "Glue",
                        need_fix_inting=pc.need_fix_inting,
                        out_dir=out_dir)
    return list(glue_dfs.keys())


###############################################################################
# Coordinating functions
###############################################################################

def _prep_directories(pkg_dir):
    """
    Prep dictionaries for CSVs.
    """
    # delete package directories if they exist
    if os.path.exists(pkg_dir):
        shutil.rmtree(pkg_dir)

    # create the main package directory
    os.mkdir(pkg_dir)
    # also create the data directory for the CSVs to live in
    os.mkdir(os.path.join(pkg_dir, 'data'))


def _input_validate(settings_init):
    """
    Extract and validate the inputs from a settings file

    Args:
        settings_init (iterable) : a list of inputs for
            datapackages typically imported from settings like:
            pudl.settings.settings_init(settings_file='settings_init_pudl_package.yml')
            with different file name depending on your setting yml file.
    Returns:
        validated_settings (iterable) : validated list of inputs
    """
    input_validation_functions = {'eia': _input_validate_eia,
                                  'ferc1': _input_validate_ferc1,
                                  'epacems': _input_validate_epacems
                                  }
    # where we are going to compile the new validated settings
    validated_settings = []

    for pkg in settings_init:
        validated_pkg_settings = {}
        validated_pkg_settings.update({'name': pkg['name'],
                                       'title': pkg['title'],
                                       'description': pkg['description']
                                       })
        dataset_dicts = []
        for settings_dataset_dict in pkg['datasets']:
            for dataset in settings_dataset_dict:
                validacted_dataset_dict = {
                    dataset: input_validation_functions[dataset](settings_dataset_dict[dataset])}
                dataset_dicts.extend([validacted_dataset_dict])
        validated_pkg_settings['datasets'] = dataset_dicts
        validated_settings.extend([validated_pkg_settings])
    return validated_settings


def ETL_pkg(pkg_settings, out_dir=SETTINGS['out_dir']):
    """
    Extract, transform and load CSVs.

    Args:
        pkg_settings (dict) : a dictionary of inputs for a
            datapackage
    Returns:
        tables_dict (dict) : dictionary with datapackpackages (keys) and
            lists of tables (values)
    """
    ETL_functions = {'eia': _ETL_eia_pkg,
                     'ferc1': _ETL_ferc1_pkg,
                     'epacems': _ETL_cems_pkg,
                     'glue': _ETL_glue,
                     }

    # a dictionary to compile the list of tables being loaded for each package
    print(pkg_settings['name'])
    # define the package directory
    pkg_dir = os.path.join(out_dir, pkg_settings['name'])
    # prepping the directories where the pkges will live
    _prep_directories(pkg_dir)
    # compile a list of tables in each dataset
    tables = []
    for dataset_dict in pkg_settings['datasets']:
        for dataset in dataset_dict:
            tbls = ETL_functions[dataset](dataset_dict[dataset], pkg_dir)
            tables.extend(tbls)
    # Add an assertion that tables =
    # os.listdir(os.path.join(pkg_dir,"data"))
    return tables
