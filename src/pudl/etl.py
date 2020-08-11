"""
Run the PUDL ETL Pipeline.

The PUDL project integrates several different public data sets into well
normalized data packages allowing easier access and interaction between all
each dataset. This module coordinates the extract/transfrom/load process for
data from:

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
import time
import uuid
from pathlib import Path

import pandas as pd

import pudl
from pudl import constants as pc

logger = logging.getLogger(__name__)


def _validate_params_partition(etl_params_og, tables):
    # if there is a `partition` in the package settings..
    partition_dict = {}
    try:
        partition_dict = etl_params_og['partition']
        # it should be a dictionary with tables (keys) and partitions (values)
        # so for each table, grab the list of the corresponding partition.
        for table in tables:
            try:
                for part in partition_dict[table]:
                    if part not in etl_params_og.keys():
                        raise AssertionError('Partion not recognized')
            except KeyError:
                pass
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
    # boiler_fuel_eia923 table. this is for harvesting and also for the boiler
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
            if table not in pc.pudl_tables["eia860"]:
                raise AssertionError(
                    f"Unrecognized EIA 860 table: {table}"
                )

    if eia_input_dict['eia923_tables']:
        for table in eia_input_dict['eia923_tables']:
            if table not in pc.pudl_tables["eia923"]:
                raise AssertionError(
                    f"Unrecogized EIA 923 table: {table}"
                )

    for year in eia_input_dict['eia860_years']:
        if year not in pc.working_years['eia860']:
            raise AssertionError(f"Unrecognized EIA 860 year: {year}")

    for year in eia_input_dict['eia923_years']:
        if year not in pc.working_years['eia923']:
            raise AssertionError(f"Unrecognized EIA 923 year: {year}")
    if (not eia_input_dict['eia923_years']
            and not eia_input_dict['eia860_years']):
        return None
    else:
        return eia_input_dict


def _load_static_tables_eia(datapkg_dir):
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
    fuel_type_eia923 = pd.DataFrame(
        {'abbr': list(pc.fuel_type_eia923.keys()),
         'fuel_type': list(pc.fuel_type_eia923.values())})

    prime_movers_eia923 = pd.DataFrame(
        {'abbr': list(pc.prime_movers_eia923.keys()),
         'prime_mover': list(pc.prime_movers_eia923.values())})

    fuel_type_aer_eia923 = pd.DataFrame(
        {'abbr': list(pc.fuel_type_aer_eia923.keys()),
         'fuel_type': list(pc.fuel_type_aer_eia923.values())})

    energy_source_eia923 = pd.DataFrame(
        {'abbr': list(pc.energy_source_eia923.keys()),
         'source': list(pc.energy_source_eia923.values())})

    transport_modes_eia923 = pd.DataFrame(
        {'abbr': list(pc.transport_modes_eia923.keys()),
         'mode': list(pc.transport_modes_eia923.values())})

    # compile the dfs in a dictionary, prep for dict_dump
    static_dfs = {'fuel_type_eia923': fuel_type_eia923,
                  'prime_movers_eia923': prime_movers_eia923,
                  'fuel_type_aer_eia923': fuel_type_aer_eia923,
                  'energy_source_eia923': energy_source_eia923,
                  'transport_modes_eia923': transport_modes_eia923}

    # run dictionaries of prepped static tables through dict_dump to make CSVs
    pudl.load.csv.dict_dump(static_dfs,
                            "Static EIA Tables",
                            datapkg_dir=datapkg_dir)
    return list(static_dfs.keys())


def _etl_eia(etl_params, datapkg_dir, pudl_settings):
    """Extract, transform and load CSVs for the EIA datasets.

    Args:
        etl_params (dict): ETL parameters required by this data source.
        datapkg_dir (path-like): The location of the directory for this
            package, wihch will contain a datapackage.json file and a data
            directory in which the CSV file are stored.
        pudl_settings (dict) : a dictionary filled with settings that mostly
            describe paths to various resources and outputs.

    Returns:
        list: Names of PUDL DB tables output by the ETL for this data source.

    """
    eia_inputs = _validate_params_eia(etl_params)
    eia860_tables = eia_inputs["eia860_tables"]
    eia860_years = eia_inputs["eia860_years"]
    eia923_tables = eia_inputs["eia923_tables"]
    eia923_years = eia_inputs["eia923_years"]

    if (
        (not eia923_tables or not eia923_years)
        and (not eia860_tables or not eia860_years)
    ):
        logger.info('Not loading EIA.')
        return []

    # generate CSVs for the static EIA tables, return the list of tables
    static_tables = _load_static_tables_eia(datapkg_dir)

    sandbox = pudl_settings.get("sandbox", False)
    ds = pudl.workspace.datastore.Datastore(
        Path(pudl_settings["pudl_in"]),
        sandbox=sandbox)
    # Extract EIA forms 923, 860
    eia923_raw_dfs = pudl.extract.eia923.Extractor(ds).extract(eia923_years)
    eia860_raw_dfs = pudl.extract.eia860.Extractor(ds).extract(eia860_years)

    # Transform EIA forms 923, 860
    eia860_transformed_dfs = pudl.transform.eia860.transform(
        eia860_raw_dfs, eia860_tables=eia860_tables)
    eia923_transformed_dfs = pudl.transform.eia923.transform(
        eia923_raw_dfs, eia923_tables=eia923_tables)
    # create an eia transformed dfs dictionary
    eia_transformed_dfs = eia860_transformed_dfs.copy()
    eia_transformed_dfs.update(eia923_transformed_dfs.copy())
    # convert types..
    eia_transformed_dfs = pudl.helpers.convert_dfs_dict_dtypes(
        eia_transformed_dfs, 'eia')

    entities_dfs, eia_transformed_dfs = pudl.transform.eia.transform(
        eia_transformed_dfs,
        eia860_years=eia860_years,
        eia923_years=eia923_years,
    )
    # convert types..
    entities_dfs = pudl.helpers.convert_dfs_dict_dtypes(entities_dfs, 'eia')

    # Compile transformed dfs for loading...
    transformed_dfs = {"Entities": entities_dfs, "EIA": eia_transformed_dfs}
    # Load step
    for data_source, transformed_df in transformed_dfs.items():
        pudl.load.csv.dict_dump(transformed_df,
                                data_source,
                                datapkg_dir=datapkg_dir)

    return (
        list(eia_transformed_dfs.keys())
        + list(entities_dfs.keys())
        + static_tables)


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

    if (not ferc1_dict['debug']) and (ferc1_dict['ferc1_tables']):
        for table in ferc1_dict['ferc1_tables']:
            if table not in pc.pudl_tables["ferc1"]:
                raise AssertionError(
                    f"Unrecognized FERC table: {table}."
                )
    if not ferc1_dict['ferc1_years']:
        return {}
    else:
        return ferc1_dict


def _load_static_tables_ferc1(datapkg_dir):
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
    ferc_accounts = (
        pc.ferc_electric_plant_accounts
        .drop('row_number', axis=1)
        .replace({'ferc_account_description': r'\s+'}, ' ', regex=True)
        .rename(columns={'ferc_account_description': 'description'})
    )

    ferc_depreciation_lines = (
        pc.ferc_accumulated_depreciation
        .drop('row_number', axis=1)
        .rename(columns={'ferc_account_description': 'description'})
    )

    # compile the dfs in a dictionary, prep for dict_dump
    static_dfs = {
        'ferc_accounts': ferc_accounts,
        'ferc_depreciation_lines': ferc_depreciation_lines
    }

    # run dictionary of prepped static tables through dict_dump to make CSVs
    pudl.load.csv.dict_dump(static_dfs,
                            "Static FERC Tables",
                            datapkg_dir=datapkg_dir)

    return list(static_dfs.keys())


def _etl_ferc1(etl_params, datapkg_dir, pudl_settings):
    """Extract, transform and load CSVs for FERC Form 1.

    Args:
        etl_params (dict): ETL parameters required by this data source.
        datapkg_dir (path-like): The location of the directory for this
            package, wihch will contain a datapackage.json file and a data
            directory in which the CSV file are stored.
        pudl_settings (dict) : a dictionary filled with settings that mostly
            describe paths to various resources and outputs.

    Returns:
        list: Names of PUDL DB tables output by the ETL for this data source.

    """
    ferc1_inputs = _validate_params_ferc1(etl_params)
    ferc1_years = ferc1_inputs['ferc1_years']
    ferc1_tables = ferc1_inputs['ferc1_tables']

    if not ferc1_years or not ferc1_tables:
        logger.info('Not loading FERC1')
        return []

    static_tables = _load_static_tables_ferc1(datapkg_dir)
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
                            datapkg_dir=datapkg_dir)
    return list(ferc1_transformed_dfs.keys()) + static_tables


###############################################################################
# EPA CEMS EXPORT FUNCTIONS
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

    # CEMS is ALWAYS going to be partitioned by year and state. This means we
    # are functinoally removing the option to not partition or partition
    # another way. Nonetheless, we are adding it in here because we still need
    # to know what the partitioning is like for the metadata generation
    # (it treats partitioned tables differently).
    epacems_dict['partition'] = {'hourly_emissions_epacems':
                                 ['epacems_years', 'epacems_states']}
    # this is maybe unnecessary because we are hardcoding the partitions, but
    # we are still going to validate that the partitioning is
    epacems_dict['partition'] = _validate_params_partition(
        epacems_dict, [pc.epacems_tables])
    if not epacems_dict['partition']:
        raise AssertionError(
            'No partition found for EPA CEMS.'
            'EPA CEMS requires either states or years as a partion'
        )

    if not epacems_dict['epacems_years'] or not epacems_dict['epacems_states']:
        return None
    else:
        return epacems_dict


def _etl_epacems(etl_params, datapkg_dir, pudl_settings):
    """Extract, transform and load CSVs for EPA CEMS.

    Args:
        etl_params (dict): ETL parameters required by this data source.
        datapkg_dir (path-like): The location of the directory for this
            package, wihch will contain a datapackage.json file and a data
            directory in which the CSV file are stored.
        pudl_settings (dict) : a dictionary filled with settings that mostly
            describe paths to various resources and outputs.

    Returns:
        list: Names of PUDL DB tables output by the ETL for this data source.

    """
    epacems_dict = pudl.etl._validate_params_epacems(etl_params)
    epacems_years = epacems_dict['epacems_years']
    epacems_states = epacems_dict['epacems_states']
    # If we're not doing CEMS, just stop here to avoid printing messages like
    # "Reading EPA CEMS data...", which could be confusing.
    if not epacems_states or not epacems_years:
        logger.info('Not ingesting EPA CEMS.')

    # NOTE: This a generator for raw dataframes
    sandbox = pudl_settings.get("sandbox", False)
    ds = pudl.extract.epacems.EpaCemsDatastore(
        Path(pudl_settings["pudl_in"]),
        sandbox=sandbox)
    epacems_raw_dfs = pudl.extract.epacems.extract(
        epacems_years, epacems_states, ds)

    # NOTE: This is a generator for transformed dataframes
    epacems_transformed_dfs = pudl.transform.epacems.transform(
        epacems_raw_dfs=epacems_raw_dfs,
        datapkg_dir=datapkg_dir)

    logger.info("Loading tables from EPA CEMS into PUDL:")
    if logger.isEnabledFor(logging.INFO):
        start_time = time.monotonic()
    epacems_tables = []
    # run the cems generator dfs through the load step
    for transformed_df_dict in epacems_transformed_dfs:
        pudl.load.csv.dict_dump(transformed_df_dict,
                                "EPA CEMS",
                                datapkg_dir=datapkg_dir)
        epacems_tables.append(list(transformed_df_dict.keys())[0])
    if logger.isEnabledFor(logging.INFO):
        time_message = "    Loading    EPA CEMS took {}".format(
            time.strftime("%H:%M:%S",
                          time.gmtime(time.monotonic() - start_time)))
        logger.info(time_message)
        start_time = time.monotonic()

    return epacems_tables


###############################################################################
# EPA IPM ETL FUNCTIONS
###############################################################################
def _validate_params_epaipm(etl_params):
    """
    Validate the etl parameters for EPA IPM.

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
        epaipm_dict['epaipm_tables'] = []
    if not epaipm_dict['epaipm_tables']:
        return {}
    return epaipm_dict


def _load_static_tables_epaipm(datapkg_dir):
    """
    Populate static PUDL tables with constants for use as foreign keys.

    For IPM, there is only one list of regional id's stored in constants that
    we want to load as a tabular resource because many of the other tabular
    resources in IPM rely on the regional_id_epaipm as a foreign key.

    Args:
        datapkg_dir (path-like): The location of the directory for this
            package. The data package directory will be a subdirectory in the
            `datapkg_dir` directory, with the name of the package as the
            name of the subdirectory.

    Returns:
        list: names of tables which were loaded.

    """
    # compile the dfs in a dictionary, prep for dict_dump
    static_dfs = {'regions_entity_epaipm':
                  pd.DataFrame(
                      pc.epaipm_region_names, columns=['region_id_epaipm'])}

    # run the dictionary of prepped static tables through dict_dump to make
    # CSVs
    pudl.load.csv.dict_dump(static_dfs,
                            "Static IPM Tables",
                            datapkg_dir=datapkg_dir)

    return list(static_dfs.keys())


def _etl_epaipm(etl_params, datapkg_dir, pudl_settings):
    """Extract, transform and load CSVs for EPA IPM.

    Args:
        etl_params (dict): ETL parameters required by this data source.
        datapkg_dir (path-like): The location of the directory for this
            package, wihch will contain a datapackage.json file and a data
            directory in which the CSV file are stored.
        pudl_settings (dict) : a dictionary filled with settings that mostly
            describe paths to various resources and outputs.

    Returns:
        list: Names of PUDL DB tables output by the ETL for this data source.

    """
    epaipm_dict = _validate_params_epaipm(etl_params)
    epaipm_tables = epaipm_dict['epaipm_tables']
    if not epaipm_tables:
        logger.info('Not ingesting EPA IPM.')
        return []
    static_tables = _load_static_tables_epaipm(datapkg_dir)

    # Extract IPM tables
    sandbox = pudl_settings.get("sandbox", False)
    ds = pudl.extract.epaipm.EpaIpmDatastore(
        Path(pudl_settings["pudl_in"]), sandbox=sandbox)
    epaipm_raw_dfs = pudl.extract.epaipm.extract(epaipm_tables, ds)

    epaipm_transformed_dfs = pudl.transform.epaipm.transform(
        epaipm_raw_dfs, epaipm_tables
    )

    pudl.load.csv.dict_dump(epaipm_transformed_dfs,
                            "EPA IPM",
                            datapkg_dir=datapkg_dir)

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


def _etl_glue(etl_params, datapkg_dir, pudl_settings):
    """Extract, transform and load CSVs for the Glue tables.

    Currently this only generates glue connecting FERC Form 1 and EIA

    Args:
        etl_params (dict): ETL parameters required by this data source.
        datapkg_dir (path-like): The location of the directory for this
            package, wihch will contain a datapackage.json file and a data
            directory in which the CSV file are stored.
        pudl_settings (dict) : a dictionary filled with settings that mostly
            describe paths to various resources and outputs.

    Returns:
        list: Names of PUDL DB tables output by the ETL for this data source.

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

    pudl.load.csv.dict_dump(
        glue_dfs, "Glue", datapkg_dir=datapkg_dir)
    return list(glue_dfs.keys())


###############################################################################
# Coordinating functions
###############################################################################
def _insert_glue_settings(dataset_dicts):
    """Add glue settings into data package settings if this is a glue-y dataset.

    Args:
        dataset_dicts (iterable): A list of dictionaries with dataset codes
            as the keys (e.g. eia, ferc1), and dictionaries of etl paramaters
            as the values.

    Returns:
        list: An updated version of dataset_dicts which includes any glue data
        required to stick together the datasets being loaded.

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
            _ = flattened_params_dict[param]
        except KeyError:
            flattened_params_dict[param] = []
    return flattened_params_dict


def get_flattened_etl_parameters(datapkg_bundle_settings):  # noqa: C901
    """
    Compile flattened etl parameters.

    The datapkg_bundle_settings is a list of dictionaries with the specific etl
    parameters for each dataset nested inside the dictionary. This function
    extracts the years, states, tables, etc. from the list datapackage settings
    and compiles them into one dictionary.


    Args:
        datapkg_bundle_settings (iterable): a list of data package parameters,
            with each element of the list being a dictionary specifying
            the data to be packaged.

    Returns:
        dict: dictionary of etl parameters with etl parameter names (keys)
        (i.e. ferc1_years, eia923_years) and etl parameters (values) (i.e. a
        list of years for ferc1_years)

    """
    flattened_parameters = []
    for datapkg in datapkg_bundle_settings:
        for settings_dataset_dict in datapkg['datasets']:
            for dataset in settings_dataset_dict:
                if settings_dataset_dict[dataset]:
                    flattened_parameters.append(settings_dataset_dict[dataset])
    flattened_params_dict = {}
    for dataset in flattened_parameters:
        for param in dataset:
            try:
                _ = flattened_params_dict[param]
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


def validate_params(datapkg_bundle_settings, pudl_settings):
    """
    Enforce validity of ETL parameters found in datapackage bundle settings.

    For each enumerated data package in the datapkg_bundle_settings, this
    function checks to ensure the input parameters for each of the datasets
    are consistent with the known input options. Most of those options are
    enumerated in pudl.constants. For each dataset, the years, states, tables,
    etc. are checked to ensure that they are valid and present. If parameters
    are not valid, assertions will be raised.

    There is some options that have default options or are hard coded during
    validation. Tables will typically be defaulted to all of the tables if
    they aren't set. CEMS is always going to be partitioned by year and state.
    This means we have functinoally removed the option to not partition or
    partition another way.

    Args:
        datapkg_bundle_settings (iterable): a list of data package parameters,
            with each element of the list being a dictionary specifying
            the data to be packaged.
        pudl_settings (dict): a dictionary describing paths to various
            resources and outputs.

    Returns:
        iterable: validated list of data package parameters, with each element
            of the list being a dictionary specitying the data to be packaged.

    """
    logger.info('reading and validating etl settings')
    param_validation_functions = {
        'eia': _validate_params_eia,
        'ferc1': _validate_params_ferc1,
        'epacems': _validate_params_epacems,
        'glue': _validate_params_glue,
        'epaipm': _validate_params_epaipm,
    }
    # where we are going to compile the new validated settings
    validated_settings = []
    # for each of the packages, rebuild the settings
    for datapkg_settings in datapkg_bundle_settings:
        validated_datapkg_settings = {}
        # Required fields:
        validated_datapkg_settings.update({
            'name': datapkg_settings['name'],
            'title': datapkg_settings['title'],
            'description': datapkg_settings['description'],
        })
        # Optional fields...
        for field in ["version", "datapkg_bundle_doi"]:
            try:
                validated_datapkg_settings[field] = datapkg_settings[field]
            except KeyError:
                pass

        dataset_dicts = []
        for settings_dataset_dict in datapkg_settings['datasets']:
            for dataset in settings_dataset_dict:
                etl_params = param_validation_functions[dataset](
                    settings_dataset_dict[dataset])
                validated_dataset_dict = {dataset: etl_params}
                if etl_params:
                    dataset_dicts.extend([validated_dataset_dict])
        dataset_dicts = _insert_glue_settings(dataset_dicts)
        if dataset_dicts:
            validated_datapkg_settings['datasets'] = dataset_dicts
            validated_settings.extend([validated_datapkg_settings])
    return validated_settings


def etl(datapkg_settings, output_dir, pudl_settings):
    """
    Run ETL process for data package specified by datapkg_settings dictionary.

    This is the coordinating function for generating all of the CSV's for a
    data package. For each of the datasets enumerated in the datapkg_settings,
    this function runs the dataset specific ETL function. Along the way, we are
    accumulating which tables have been loaded. This is useful for generating
    the metadata associated with the package.

    Args:
        datapkg_settings (dict): Validated ETL parameters for a single
            datapackage, originally read in from the PUDL ETL input file.
        output_dir (path-like): The individual datapackage directory, which
            will contain the datapackage.json file and the data directory.
        pudl_settings (dict): a dictionary describing paths to various
            resources and outputs.

    Returns:
        list: The names of the tables included in the output datapackage.

    """
    # compile a list of tables in each dataset
    processed_tables = []
    etl_funcs = {
        "eia": _etl_eia,
        "ferc1": _etl_ferc1,
        "epacems": _etl_epacems,
        "glue": _etl_glue,
        "epaipm": _etl_epaipm,
    }
    for dataset_dict in datapkg_settings['datasets']:
        for dataset in dataset_dict:
            new_tables = etl_funcs[dataset](
                dataset_dict[dataset], output_dir, pudl_settings)
            if new_tables:
                processed_tables.extend(new_tables)
    return processed_tables


def generate_datapkg_bundle(datapkg_bundle_settings,
                            pudl_settings,
                            datapkg_bundle_name,
                            datapkg_bundle_doi=None,
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
        datapkg_bundle_settings (iterable): a list of dictionaries. Each item
            in the list corresponds to a data package. Each data package's
            dictionary contains the arguements for its ETL function.
        pudl_settings (dict): a dictionary filled with settings that mostly
            describe paths to various resources and outputs.
        datapkg_bundle_name (str): name of directory you want the bundle of
            data packages to live.
        clobber (bool): If True and there is already a directory with data
            packages with the datapkg_bundle_name, the existing data packages
            will be deleted and new data packages will be generated in their
            place.

    Returns:
        dict: A dictionary with datapackage names as the keys, and Python
        dictionaries representing tabular datapackage resource descriptors as
        the values, one per datapackage that was generated as part of the
        bundle.

    """
    # validate the settings from the settings file.
    validated_bundle_settings = validate_params(
        datapkg_bundle_settings, pudl_settings)

    # Generate a random UUID to identify this ETL run / data package bundle
    datapkg_bundle_uuid = str(uuid.uuid4())
    datapkg_bundle_dir = Path(pudl_settings["datapkg_dir"], datapkg_bundle_name)

    # Create, or delete and re-create the top level datapackage bundle directory:
    _ = pudl.helpers.prep_dir(datapkg_bundle_dir, clobber=clobber)

    metas = {}
    for datapkg_settings in validated_bundle_settings:
        output_dir = Path(
            pudl_settings["datapkg_dir"],  # PUDL datapackage output dir
            datapkg_bundle_name,           # Name of the datapackage bundle
            datapkg_settings["name"])      # Name of the datapackage

        # Create the datapackge directory, and its data subdir:
        (output_dir / "data").mkdir(parents=True)
        # run the ETL functions for this pkg and return the list of tables
        # output to CSVs:
        datapkg_resources = etl(datapkg_settings, output_dir, pudl_settings)

        if datapkg_resources:
            descriptor = pudl.load.metadata.generate_metadata(
                datapkg_settings,
                datapkg_resources,
                output_dir,
                datapkg_bundle_uuid=datapkg_bundle_uuid,
                datapkg_bundle_doi=datapkg_bundle_doi)
            metas[datapkg_settings["name"]] = descriptor
        else:
            logger.info(
                f"Not generating metadata for {datapkg_settings['name']}")

    return metas
