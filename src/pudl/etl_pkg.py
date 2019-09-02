"""Module coordinating the PUDL ETL pipeline, generating data packages."""

import logging
import os.path
import shutil
import time

import pandas as pd

import pudl
import pudl.constants as pc

logger = logging.getLogger(__name__)


###############################################################################
# EIA EXPORT FUNCTIONS
###############################################################################

def _validate_input_eia(inputs):  # noqa: C901
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

    # if we are only extracting 860, we also need to pull in the
    # boiler_fuel_eia923 table. this is for harvessting and also for the boiler
    # generator association
    if not eia_input_dict['eia923_years'] and eia_input_dict['eia860_years']:
        eia_input_dict['eia923_years'] = eia_input_dict['eia860_years']
        eia_input_dict['eia923_tables'] = [
            'boiler_fuel_eia923', 'generation_eia923']

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


def _etl_eia_pkg(inputs, data_dir, pkg_dir):
    eia_inputs = _validate_input_eia(inputs)
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
    eia923_raw_dfs = pudl.extract.eia923.extract(eia923_years=eia923_years,
                                                 data_dir=data_dir)
    eia860_raw_dfs = pudl.extract.eia860.extract(eia860_years=eia860_years,
                                                 data_dir=data_dir)
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


def _validate_input_ferc1(inputs):
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


def _etl_ferc1_pkg(inputs, pudl_settings, pkg_dir):
    ferc1_inputs = _validate_input_ferc1(inputs)

    ferc1_years = ferc1_inputs['ferc1_years']
    ferc1_tables = ferc1_inputs['ferc1_tables']

    if not ferc1_years or not ferc1_tables:
        logger.info('Not loading FERC1')
        return []

    static_tables = _load_static_tables_ferc(pkg_dir)
    # Extract FERC form 1
    ferc1_raw_dfs = pudl.extract.ferc1.extract(
        ferc1_tables=ferc1_tables,
        ferc1_years=ferc1_years,
        pudl_settings=pudl_settings)
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


def _validate_input_epacems(inputs):
    epacems_dict = {}
    # pull out the inputs from the dictionary passed into this function
    try:
        epacems_dict['epacems_years'] = inputs['epacems_years']
    except KeyError:
        epacems_dict['epacems_years'] = [None]
    # the states will default to all of the states if nothing is given
    try:
        epacems_dict['epacems_states'] = inputs['epacems_states']
    except KeyError:
        epacems_dict['epacems_states'] = []
    # if states are All, then we grab all of the states from constants
    if epacems_dict['epacems_states']:
        if epacems_dict['epacems_states'][0].lower() == 'all':
            epacems_dict['epacems_states'] = list(pc.cems_states.keys())

    return epacems_dict


def _etl_epacems_pkg(inputs, data_dir, pkg_dir):
    epacems_dict = _validate_input_epacems(inputs)
    epacems_years = epacems_dict['epacems_years']
    epacems_states = epacems_dict['epacems_states']
    # If we're not doing CEMS, just stop here to avoid printing messages like
    # "Reading EPA CEMS data...", which could be confusing.
    if not epacems_states or not epacems_years:
        logger.info('Not ingesting EPA CEMS.')
        return []

    # NOTE: This a generator for raw dataframes
    epacems_raw_dfs = pudl.extract.epacems.extract(
        epacems_years=epacems_years, states=epacems_states, data_dir=data_dir)
    # NOTE: This is a generator for transformed dataframes
    epacems_transformed_dfs = pudl.transform.epacems.transform_pkg(
        epacems_raw_dfs=epacems_raw_dfs, pkg_dir=pkg_dir)
    logger.info("Loading tables from EPA CEMS into PUDL:")
    if logger.isEnabledFor(logging.INFO):
        start_time = time.monotonic()
    with pudl.load.BulkCopyPkg(
            table_name="hourly_emissions_epacems",
            pkg_dir=pkg_dir) as loader:

        for transformed_df_dict in epacems_transformed_dfs:
            # There's currently only one dataframe in this dict at a time,
            # but that could be changed if useful.
            # The keys to the dict are a tuple (year, month, state)
            for transformed_df in transformed_df_dict.values():
                loader.add(transformed_df)
    if logger.isEnabledFor(logging.INFO):
        time_message = "    Loading    EPA CEMS took {}".format(
            time.strftime("%H:%M:%S",
                          time.gmtime(time.monotonic() - start_time)))
        logger.info(time_message)
        start_time = time.monotonic()
    # pudl.models.epacems.finalize(pudl_engine)
    # if logger.isEnabledFor(logging.INFO):
    #    time_message = "    Finalizing EPA CEMS took {}".format(
    #        time.strftime("%H:%M:%S", time.gmtime(
    #            time.monotonic() - start_time))
    #    )
    #    logger.info(time_message)
    return ['hourly_emissions_epacems']


###############################################################################
# GLUE EXPORT FUNCTIONS
###############################################################################


def _validate_input_glue(inputs):
    glue_dict = {}
    # pull out the inputs from the dictionary passed into this function
    try:
        glue_dict['ferc1'] = inputs['ferc1']
    except KeyError:
        glue_dict['ferc1'] = False
    try:
        glue_dict['eia'] = inputs['eia']
    except KeyError:
        glue_dict['eia'] = False
    return(glue_dict)


def _etl_glue(inputs, pkg_dir):
    """
    Grab the glue tables and generate CSVs.

    Right now, this function only generates the glue between EIA and FERC

    """
    glue_dict = _validate_input_glue(inputs)

    ferc1 = glue_dict['ferc1']
    eia = glue_dict['eia']
    if not eia and not ferc1:
        return []
        # grab the glue tables for ferc1 & eia
    glue_dfs = pudl.glue.ferc1_eia.glue(
        ferc1=glue_dict['ferc1'],
        eia=glue_dict['eia']
    )

    pudl.load.dict_dump(glue_dfs,
                        "Glue",
                        need_fix_inting=pc.need_fix_inting,
                        pkg_dir=pkg_dir)
    return list(glue_dfs.keys())


###############################################################################
# Coordinating functions
###############################################################################

def _prep_directories(pkg_dir):
    """Prepare data package directories to receive CSVs."""
    # delete package directories if they exist
    if os.path.exists(pkg_dir):
        shutil.rmtree(pkg_dir)

    # create the main package directory
    os.mkdir(pkg_dir)
    # also create the data directory for the CSVs to live in
    os.mkdir(os.path.join(pkg_dir, 'data'))


def validate_input(settings_init):
    """
    Read and validate the inputs from a settings file.

    Args:
        settings_init (iterable): a list of data package parameters,
            with each element of the list being a dictionary specifying
            the data to be packaged.

    Returns:
        iterable: validated list of inputs

    """
    input_validation_functions = {
        'eia': _validate_input_eia,
        'ferc1': _validate_input_ferc1,
        'epacems': _validate_input_epacems,
        'glue': _validate_input_glue,
    }
    # where we are going to compile the new validated settings
    validated_settings = []

    for pkg in settings_init:
        validated_pkg_settings = {}
        validated_pkg_settings.update({
            'name': pkg['name'],
            'title': pkg['title'],
            'description': pkg['description']
        })
        dataset_dicts = []
        for settings_dataset_dict in pkg['datasets']:
            for dataset in settings_dataset_dict:
                validacted_dataset_dict = {
                    dataset: input_validation_functions[dataset](
                        settings_dataset_dict[dataset]
                    )
                }
                dataset_dicts.extend([validacted_dataset_dict])
        validated_pkg_settings['datasets'] = dataset_dicts
        validated_settings.extend([validated_pkg_settings])
    return validated_settings


def etl_pkg(pkg_settings, pudl_settings):
    """Extracts, transforms and loads CSVs.

    Args:
        pkg_settings (dict) : a dictionary of inputs for a datapackage.
        pudl_settings (dict) : a dictionary filled with settings that mostly
            describe paths to various resources and outputs.

    Returns:
        dict: dictionary with datapackpackages (keys) and
        lists of tables (values)

    """
    # a dictionary to compile the list of tables being loaded for each package
    # define the package directory
    pkg_dir = os.path.join(pudl_settings['datapackage_dir'],
                           pkg_settings['name'])
    # prepping the directories where the pkges will live
    _prep_directories(pkg_dir)
    # compile a list of tables in each dataset
    tables = []
    for dataset_dict in pkg_settings['datasets']:
        for dataset in dataset_dict:
            if dataset == 'eia':
                tbls = _etl_eia_pkg(
                    dataset_dict['eia'],
                    data_dir=pudl_settings['data_dir'],
                    pkg_dir=pkg_dir
                )
            elif dataset == 'ferc1':
                tbls = _etl_ferc1_pkg(
                    dataset_dict['ferc1'],
                    pudl_settings=pudl_settings,
                    pkg_dir=pkg_dir
                )
            elif dataset == 'epacems':
                tbls = _etl_epacems_pkg(
                    dataset_dict['epacems'],
                    data_dir=pudl_settings['data_dir'],
                    pkg_dir=pkg_dir
                )
            elif dataset == 'glue':
                tbls = _etl_glue(
                    dataset_dict['glue'],
                    pkg_dir=pkg_dir
                )
            else:
                raise AssertionError(
                    f'Invalid dataset {dataset} found in input.'
                )
            tables.extend(tbls)
    # Add an assertion that tables =
    # os.listdir(os.path.join(pkg_dir,"data"))
    return tables
