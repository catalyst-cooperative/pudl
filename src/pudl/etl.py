"""
Module coordinating the PUDL ETL pipeline, generating data packages.

The PUDL project integrates several different public data sets into well
normalized data packages allowing easier access and interaction between all each
dataset. This module coordinates the extract/transfrom/load process of data from:

 - US Energy Information Agency (EIA):
   - Form 860 (eia860)
   - Form 923 (eia923)
 - US Federal Energy Regulatory Commission (FERC):
   - Form 1 (ferc1)
 - US Environmental Protection Agency (EPA):
   - Continuous Emissions Monitory System (epacems)
   - Integrated Planning Model (epaipm)

"""

import logging
import os.path
import shutil
import time
import uuid

import pandas as pd

import pudl
import pudl.constants as pc

logger = logging.getLogger(__name__)


def _validate_params_partition(etl_params_og, tables):
    # if there is a `partition` in the package settings..
    partition_dict = {}
    try:
        partition_dict = etl_params_og['partition']
        # it should be a dictionary with tables (keys) and partitions (values)
        # so for each table, grab the list of the corresponding partition.
        for table in tables:
            if partition_dict[table] not in etl_params_og.keys():
                raise AssertionError('Partion not recognized')
    except KeyError:
        partition_dict['partition'] = None
    return(partition_dict)


###############################################################################
# EIA EXPORT FUNCTIONS
###############################################################################


def _validate_params_eia(etl_params):  # noqa: C901
    # extract all of the etl_params for the EIA ETL function
    # empty dictionary to compile etl_params
    eia_input_dict = {}
    # when nothing is set in the settings file, the years will default as none
    try:
        eia_input_dict['eia860_years'] = etl_params['eia860_years']
    except KeyError:
        eia_input_dict['eia860_years'] = []

    # the tables will default to all of the tables if nothing is given
    try:
        eia_input_dict['eia860_tables'] = etl_params['eia860_tables']
    except KeyError:
        eia_input_dict['eia860_tables'] = pc.pudl_tables['eia860']

    try:
        eia_input_dict['eia923_years'] = etl_params['eia923_years']
    except KeyError:
        eia_input_dict['eia923_years'] = []

    try:
        eia_input_dict['eia923_tables'] = etl_params['eia923_tables']
    except KeyError:
        eia_input_dict['eia923_tables'] = pc.pudl_tables['eia923']

    # if we are only extracting 860, we also need to pull in the
    # boiler_fuel_eia923 table. this is for harvessting and also for the boiler
    # generator association
    if not eia_input_dict['eia923_years'] and eia_input_dict['eia860_years']:
        eia_input_dict['eia923_years'] = eia_input_dict['eia860_years']
        eia_input_dict['eia923_tables'] = [
            'boiler_fuel_eia923', 'generation_eia923']

    # if someone is trying to generate 923 without 860... well that won't work
    # so we're forcing the same 860 years.
    if not eia_input_dict['eia860_years'] and eia_input_dict['eia923_years']:
        eia_input_dict['eia860_years'] = eia_input_dict['eia923_years']

    # Validate the etl_params
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
    if not eia_input_dict['eia923_years'] and not eia_input_dict['eia860_years']:
        return None
    else:
        return eia_input_dict


def _load_static_tables_eia(pkg_dir):
    """Populate static EIA tables with constants for use as foreign keys.

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
    fuel_type_eia923 = pd.DataFrame({'abbr': list(pc.fuel_type_eia923.keys()),
                                     'fuel_type': list(pc.fuel_type_eia923.values())})

    prime_movers_eia923 = pd.DataFrame({'abbr': list(pc.prime_movers_eia923.keys()),
                                        'prime_mover': list(pc.prime_movers_eia923.values())})

    fuel_type_aer_eia923 = pd.DataFrame({'abbr': list(pc.fuel_type_aer_eia923.keys()),
                                         'fuel_type': list(pc.fuel_type_aer_eia923.values())})

    energy_source_eia923 = pd.DataFrame({'abbr': list(pc.energy_source_eia923.keys()),
                                         'source': list(pc.energy_source_eia923.values())})

    transport_modes_eia923 = pd.DataFrame({'abbr': list(pc.transport_modes_eia923.keys()),
                                           'mode': list(pc.transport_modes_eia923.values())})

    # compile the dfs in a dictionary, prep for dict_dump
    static_dfs = {'fuel_type_eia923': fuel_type_eia923,
                  'prime_movers_eia923': prime_movers_eia923,
                  'fuel_type_aer_eia923': fuel_type_aer_eia923,
                  'energy_source_eia923': energy_source_eia923,
                  'transport_modes_eia923': transport_modes_eia923}

    # run the dictionary of prepped static tables through dict_dump to make
    # CSVs
    pudl.load.csv.dict_dump(static_dfs,
                            "Static EIA Tables",
                            need_fix_inting=pc.need_fix_inting,
                            pkg_dir=pkg_dir)
    return list(static_dfs.keys())


def _etl_eia_pkg(etl_params, data_dir, pkg_dir):
    eia_inputs = _validate_params_eia(etl_params)
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
        pudl.load.csv.dict_dump(transformed_df,
                                data_source,
                                need_fix_inting=pc.need_fix_inting,
                                pkg_dir=pkg_dir)

    return list(eia_transformed_dfs.keys()) + list(entities_dfs.keys()) + static_tables

###############################################################################
# FERC1 EXPORT FUNCTIONS
###############################################################################


def _validate_params_ferc1(etl_params):  # noqa: C901
    ferc1_dict = {}
    # pull out the etl_params from the dictionary passed into this function
    try:
        ferc1_dict['ferc1_years'] = etl_params['ferc1_years']
    except KeyError:
        ferc1_dict['ferc1_years'] = [None]
    # the tables will default to all of the tables if nothing is given
    try:
        ferc1_dict['ferc1_tables'] = etl_params['ferc1_tables']
    except KeyError:
        ferc1_dict['ferc1_tables'] = pc.pudl_tables['ferc1']

    try:
        ferc1_dict['debug'] = etl_params['debug']
    except KeyError:
        ferc1_dict['debug'] = False

    # try:
    #    ferc1_dict['partition'] = etl_params['partition']
    # except KeyError:
    #    ferc1_dict['partition'] = None

    if (not ferc1_dict['debug']) and (ferc1_dict['ferc1_tables']):
        for table in ferc1_dict['ferc1_tables']:
            if table not in pc.ferc1_pudl_tables:
                raise AssertionError(
                    f"Unrecognized FERC table: {table}."
                )
    if not ferc1_dict['ferc1_years']:
        return {}
    else:
        return ferc1_dict


def _load_static_tables_ferc(pkg_dir):
    """Populate static PUDL tables with constants for use as foreign keys.

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
    pudl.load.csv.dict_dump(static_dfs,
                            "Static FERC Tables",
                            need_fix_inting=pc.need_fix_inting,
                            pkg_dir=pkg_dir)

    return list(static_dfs.keys())


def _etl_ferc1_pkg(etl_params, pudl_settings, pkg_dir):
    ferc1_inputs = _validate_params_ferc1(etl_params)

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
    pudl.load.csv.dict_dump(ferc1_transformed_dfs,
                            "FERC 1",
                            need_fix_inting=pc.need_fix_inting,
                            pkg_dir=pkg_dir)
    return list(ferc1_transformed_dfs.keys()) + static_tables

###############################################################################
# EPA CEMPS EXPORT FUNCTIONS
###############################################################################


def _validate_params_epacems(etl_params):
    epacems_dict = {}
    # pull out the etl_params from the dictionary passed into this function
    try:
        epacems_dict['epacems_years'] = etl_params['epacems_years']
    except KeyError:
        epacems_dict['epacems_years'] = []
    # the states will default to all of the states if nothing is given
    try:
        epacems_dict['epacems_states'] = etl_params['epacems_states']
    except KeyError:
        epacems_dict['epacems_states'] = []
    # if states are All, then we grab all of the states from constants
    if epacems_dict['epacems_states']:
        if epacems_dict['epacems_states'][0].lower() == 'all':
            epacems_dict['epacems_states'] = list(pc.cems_states.keys())

    epacems_dict['partition'] = _validate_params_partition(
        etl_params, [pc.epacems_tables])
    if not epacems_dict['partition']:
        raise AssertionError('No partition found for EPA CEMS. '
                             'EPA CEMS requires either states or years as a partion'
                             )

    if not epacems_dict['epacems_years'] or not epacems_dict['epacems_states']:
        return None
    else:
        return epacems_dict


def _etl_epacems_part(part, epacems_years, epacems_states, data_dir, pkg_dir):
    # NOTE: This a generator for raw dataframes
    epacems_raw_dfs = pudl.extract.epacems.extract(
        epacems_years=[part], states=epacems_states, data_dir=data_dir)
    # NOTE: This is a generator for transformed dataframes
    epacems_transformed_dfs = pudl.transform.epacems.transform(
        epacems_raw_dfs=epacems_raw_dfs, pkg_dir=pkg_dir)
    logger.info("Loading tables from EPA CEMS into PUDL:")
    if logger.isEnabledFor(logging.INFO):
        start_time = time.monotonic()
    table_name = f"hourly_emissions_epacems_{part}"
    with pudl.load.csv.BulkCopy(
            table_name=table_name,
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
    return(table_name)


def _etl_epacems_pkg(etl_params, data_dir, pkg_dir):
    epacems_dict = _validate_params_epacems(etl_params)
    epacems_years = epacems_dict['epacems_years']
    epacems_states = epacems_dict['epacems_states']
    epacems_partition = epacems_dict['partition']
    # If we're not doing CEMS, just stop here to avoid printing messages like
    # "Reading EPA CEMS data...", which could be confusing.
    if not epacems_states or not epacems_years:
        logger.info('Not ingesting EPA CEMS.')
        return []
    epacems_tables = []
    for part in epacems_dict[epacems_partition[pc.epacems_tables]]:
        if part in epacems_years:
            epacems_years = [part]
        if part in epacems_states:
            epacems_states = [part]

        epacems_tables.append(_etl_epacems_part(part, epacems_years,
                                                epacems_states,
                                                data_dir,
                                                pkg_dir))
    return epacems_tables
    # return ['hourly_emissions_epacems']

###############################################################################
# EPA IPM ETL FUNCTIONS
###############################################################################


def _validate_params_epaipm(etl_params):
    """Validate the etl parameters for EPA IPM.

    Args:
        etl_params (iterable): dictionary of inputs

    Returns:
        iterable: validated dictionary of inputs
    """
    epaipm_dict = {}
    # pull out the etl_params from the dictionary passed into this function
    try:
        epaipm_dict['epaipm_tables'] = etl_params['epaipm_tables']
    except KeyError:
        epaipm_dict['epaipm_tables'] = [None]
    return(epaipm_dict)


def _load_static_tables_epaipm(pkg_dir):
    """Populate static PUDL tables with constants for use as foreign keys.

    For IPM, there is only one list of regional id's stored in constants that
    we want to load as a tabular resource because many of the other tabular
    resources in IPM rely on the regional_id_epaipm as a foreign key.

    Args:
        pkg_dir (path-like): The location of the directory for this package.
            The data package directory will be a subdirectory in the
            `datapackage_dir` directory, with the name of the package as the
            name of the subdirectory.
    Returns:
        iterable: list of tables
    """
    # compile the dfs in a dictionary, prep for dict_dump
    static_dfs = {'regions_entity_epaipm':
                  pd.DataFrame(pc.epaipm_region_names, columns=['region_id_epaipm'])}

    # run the dictionary of prepped static tables through dict_dump to make
    # CSVs
    pudl.load.csv.dict_dump(static_dfs,
                            "Static IPM Tables",
                            need_fix_inting=pc.need_fix_inting,
                            pkg_dir=pkg_dir)

    return list(static_dfs.keys())


def _etl_epaipm(etl_params, data_dir, pkg_dir):
    """Extracts, transforms and loads CSVs for EPA IPM.

    Args:
        etl_params (iterable): dictionary of parameters for etl
        data_dir (path-like): The location of the directory for the data store.
        pkg_dir (path-like): The location of the directory for this package.
            The data package directory will be a subdirectory in the
            `datapackage_dir` directory, with the name of the package as the
            name of the subdirectory.
    Returns:
        iterable: list of tables
    """
    epaipm_dict = _validate_params_epaipm(etl_params)
    epaipm_tables = epaipm_dict['epaipm_tables']
    static_tables = _load_static_tables_epaipm(pkg_dir)

    # Extract IPM tables
    epaipm_raw_dfs = pudl.extract.epaipm.extract(
        epaipm_tables, data_dir=data_dir)

    epaipm_transformed_dfs = pudl.transform.epaipm.transform(
        epaipm_raw_dfs, epaipm_tables
    )

    pudl.load.csv.dict_dump(
        epaipm_transformed_dfs,
        "EPA IPM",
        need_fix_inting=pc.need_fix_inting,
        pkg_dir=pkg_dir
    )

    return list(epaipm_transformed_dfs.keys()) + static_tables


###############################################################################
# GLUE EXPORT FUNCTIONS
###############################################################################


def _validate_params_glue(etl_params):
    glue_dict = {}
    # pull out the etl_params from the dictionary passed into this function
    try:
        glue_dict['ferc1'] = etl_params['ferc1']
    except KeyError:
        glue_dict['ferc1'] = False
    try:
        glue_dict['eia'] = etl_params['eia']
    except KeyError:
        glue_dict['eia'] = False
    if not glue_dict['ferc1'] and not glue_dict['eia']:
        return {}
    else:
        return(glue_dict)


def _etl_glue(etl_params, pkg_dir):
    """Grab the glue tables and generate CSVs.

    Right now, this function only generates the glue between EIA and FERC.
    """
    glue_dict = _validate_params_glue(etl_params)
    ferc1 = glue_dict['ferc1']
    eia = glue_dict['eia']
    if not eia and not ferc1:
        return ('ahhhh this is not werking')  # [False]
        # grab the glue tables for ferc1 & eia
    glue_dfs = pudl.glue.ferc1_eia.glue(
        ferc1=glue_dict['ferc1'],
        eia=glue_dict['eia']
    )

    pudl.load.csv.dict_dump(glue_dfs,
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


def _insert_glue_settings(dataset_dicts):
    """Add glue settings into data package settings if this is a glue-y dataset.

    Args:
        dataset_dicts (iterable): list of dictionaries with datasets (keys) and
            dictionaries of etl paramaters (values)
    Returns:
        iterable: updated dataset_dicts. a dictionary with the glue dataset will
            be added if the datasets are glue-y datasets
    """
    # if there are valid datasets in the settings, we need to check if any
    # of these
    if dataset_dicts:
        glue_param = {'ferc1': False,
                      'eia': False}
        datasets_w_glue = ['ferc1', 'eia']
        for dataset_input in dataset_dicts:
            for dataset in dataset_input:
                if dataset in datasets_w_glue:
                    if dataset == 'ferc1':
                        glue_param['ferc1'] = True
                    if dataset == 'eia':
                        glue_param['eia'] = True
        validated_glue_params = _validate_params_glue(glue_param)
        if validated_glue_params:
            dataset_dicts.extend([{'glue': validated_glue_params}])
    return dataset_dicts


def _add_missing_parameters(flattened_params_dict):
    """Add the standard etl parameters if they are missing."""
    standard_params = ['ferc1_years',
                       'eia923_years',
                       'eia860_years',
                       'epacems_years',
                       'epacems_states']
    for param in standard_params:
        try:
            flattened_params_dict[param]
        except KeyError:
            flattened_params_dict[param] = []
    return flattened_params_dict


def get_flattened_etl_parameters(pkg_bundle_settings):  # noqa: C901
    """
    Compile flattened etl parameters.

    Args:
        pkg_bundle_settings (iterable): a list of data package parameters,
            with each element of the list being a dictionary specifying
            the data to be packaged.
    Returns:
        dict: dictionary of etl parameters (i.e. ferc1_years, eia923_years)
    """
    flattened_parameters = []
    for pkg in pkg_bundle_settings:
        for settings_dataset_dict in pkg['datasets']:
            for dataset in settings_dataset_dict:
                if settings_dataset_dict[dataset]:
                    flattened_parameters.append(settings_dataset_dict[dataset])
    flattened_params_dict = {}
    for dataset in flattened_parameters:
        for param in dataset:
            try:
                flattened_params_dict[param]
                logger.debug(f'{param} is already present present')
                if flattened_params_dict[param] is True or False:
                    if flattened_params_dict[param] or dataset[param] is True:
                        flattened_params_dict[param] = True
                    else:
                        flattened_params_dict[param] = False
                elif isinstance(flattened_params_dict[param], list):
                    flattened_params_dict[param] = set(
                        flattened_params_dict[param] + dataset[param])
            except KeyError:
                flattened_params_dict[param] = dataset[param]
    flattened_params_dict = _add_missing_parameters(flattened_params_dict)
    return flattened_params_dict


def validate_params(pkg_bundle_settings, pudl_settings):
    """
    Read and validate the etl inputs from a settings file.

    Args:
        pkg_bundle_settings (iterable): a list of data package parameters,
            with each element of the list being a dictionary specifying
            the data to be packaged.

    Returns:
        iterable: validated list of inputs
    """
    logger.info('reading and validating etl settings')
    param_validation_functions = {
        'eia': _validate_params_eia,
        'ferc1': _validate_params_ferc1,
        'epacems': _validate_params_epacems,
        'glue': _validate_params_glue,
        'epaipm': _validate_params_epaipm
    }
    # where we are going to compile the new validated settings
    validated_settings = []
    # for each of the packages, rebuild the settings
    for pkg in pkg_bundle_settings:
        validated_pkg_settings = {}
        validated_pkg_settings.update({
            'name': pkg['name'],
            'title': pkg['title'],
            'description': pkg['description']
        })
        dataset_dicts = []
        for settings_dataset_dict in pkg['datasets']:
            for dataset in settings_dataset_dict:
                etl_params = param_validation_functions[dataset](
                    settings_dataset_dict[dataset])
                validated_dataset_dict = {dataset: etl_params}
                if etl_params:
                    dataset_dicts.extend([validated_dataset_dict])
        dataset_dicts = _insert_glue_settings(dataset_dicts)
        if dataset_dicts:
            validated_pkg_settings['datasets'] = dataset_dicts
            validated_settings.extend([validated_pkg_settings])
    return(validated_settings)


def etl_pkg(pkg_settings, pudl_settings, pkg_bundle_dir):
    """Extracts, transforms and loads CSVs.

    This is the coordinating function for generating all of the CSV's for a data
    package. For each of the datasets enumerated in the pkg_settings, this
    function runs the dataset specific ETL function. Along the way, we are
    accumulating which tables have been loaded. This is useful for generating
    the metadata associated with the package.

    Args:
        pkg_settings (dict) : a dictionary of etl_params for a datapackage.
        pudl_settings (dict) : a dictionary filled with settings that mostly
            describe paths to various resources and outputs.
        uuid_pkgs (uuid)

    Returns:
        dict: dictionary with datapackpackages (keys) and
        lists of tables (values)

    """
    # a dictionary to compile the list of tables being loaded for each package
    # define the package directory
    pkg_dir = os.path.join(pkg_bundle_dir,
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
            elif dataset == 'epaipm':
                tbls = _etl_epaipm(
                    dataset_dict['epaipm'],
                    data_dir=pudl_settings['data_dir'],
                    pkg_dir=pkg_dir
                )
            else:
                raise AssertionError(
                    f'Invalid dataset {dataset} found in input.'
                )
            if tbls:
                tables.extend(tbls)
    return tables


def generate_data_packages(pkg_bundle_settings,
                           pudl_settings,
                           pkg_bundle_name,
                           debug=False,
                           clobber=False):
    """
    Coordinate the generation of data packages.

    For each bundle of packages laid out in the package_settings, this function
    generates data packages. First, the settings are validated (which runs
    through each of the settings listed in the package_settings). Then for
    each of the packages, run through the etl (extract, transform, load)
    functions, which generates CSVs. Then the metadata for the packages is
    generated by pulling from the metadata (which is a json file containing
    the schema for all of the possible pudl tables).

    Args:
        pkg_bundle_settings (iterable) : a list of dictionaries. Each item in
            the list corresponds to a data package. Each data package's
            dictionary contains the arguements for its ETL function.
        pudl_settings (dict) : a dictionary filled with settings that mostly
            describe paths to various resources and outputs.
        pkg_bundle_name (string): name of directory you want the bundle of
            data packages to live.
        debug (bool): If True, return a dictionary with package names (keys)
            and a list with the data package metadata and report (values).
        clobber (bool):

    Returns:
        tuple: A tuple containing generated metadata for the packages laid out
        in the package_settings.

    """
    # validate the settings from the settings file.
    validated_bundle_settings = validate_params(
        pkg_bundle_settings, pudl_settings)
    uuid_pkgs = str(uuid.uuid4())

    pkg_bundle_dir = pudl.load.metadata.prep_pkg_bundle_directory(
        pudl_settings, pkg_bundle_name, clobber=clobber)

    metas = {}
    for pkg_settings in validated_bundle_settings:
        pkg_dir = os.path.join(pkg_bundle_dir, pkg_settings['name'])
        # run the ETL functions for this pkg and return the list of tables
        # dumped to CSV
        pkg_tables = etl_pkg(pkg_settings, pudl_settings, pkg_bundle_dir)
        # assure that the list of tables from ETL match up with the CVSs and
        # dependent tables
        # pudl.load.metadata.test_file_consistency(
        #    pkg_tables,
        #    pkg_settings,
        #    pkg_dir=pkg_dir)

        if pkg_tables:
            # generate the metadata for the package and validate
            # TODO: we'll probably want to remove this double return... but having
            # the report and the metadata while debugging is very useful.
            report = pudl.load.metadata.generate_metadata(
                pkg_settings,
                pkg_tables,
                pkg_dir,
                uuid_pkgs=uuid_pkgs)
            metas[pkg_settings['name']] = report
        else:
            logger.info(f"Not generating metadata for {pkg_settings['name']}")
    if debug:
        return (metas)
