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
import uuid
from pathlib import Path

import pandas as pd
import prefect
from prefect import task, unmapped
# from prefect.engine.executors import DaskExecutor
from prefect.engine.results import LocalResult

import pudl
from pudl import constants as pc

logger = logging.getLogger(__name__)


@task
def _extract_table_names(df_map, extra_df_map):
    logging.info(f'_extract_table_names: {df_map}, {extra_df_map}')
    table_names = set()
    # Sometimes this can get list of dicts when result of mapped task is passed in
    if type(df_map) == list:
        for segment in df_map:
            table_names.update(segment)
    elif type(df_map) == dict:
        table_names.update(df_map)
    else:
        raise AssertionError(
            f'Unexpected df_map type {type(df_map)}, expected list of dict.')
    if extra_df_map:
        table_names.update(extra_df_map)
    return sorted(table_names)


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


def _all_params_present(params, required_params):
    """Returns true iff all params in required_params are defined and nonempty."""
    for p in required_params:
        if not params.get(p):
            return False
    return True

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


@task(result=LocalResult(), target="{task_name}")  # noqa: FS003
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
    return {'fuel_type_eia923': fuel_type_eia923,
            'prime_movers_eia923': prime_movers_eia923,
            'fuel_type_aer_eia923': fuel_type_aer_eia923,
            'energy_source_eia923': energy_source_eia923,
            'transport_modes_eia923': transport_modes_eia923}


@task(result=LocalResult(), target="{task_name}")  # noqa: FS003
def _extract_eia860(params, pudl_settings):
    sandbox = pudl_settings.get("sandbox", False)
    ds = pudl.workspace.datastore.Datastore(
        Path(pudl_settings["pudl_in"]),
        sandbox=sandbox)
    dfs = pudl.extract.eia860.Extractor(ds).extract(params['eia860_years'])
    return dfs


@task(result=LocalResult(), target="{task_name}")  # noqa: FS003
def _extract_eia923(params, pudl_settings):
    sandbox = pudl_settings.get("sandbox", False)
    ds = pudl.workspace.datastore.Datastore(
        Path(pudl_settings["pudl_in"]),
        sandbox=sandbox)
    return pudl.extract.eia923.Extractor(ds).extract(params['eia923_years'])


@task(result=LocalResult(), target="{task_name}")  # noqa: FS003
def _transform_eia860(params, dfs):
    return pudl.transform.eia860.transform(dfs, params['eia860_tables'])


@task(result=LocalResult(), target="{task_name}")  # noqa: FS003
def _transform_eia923(params, dfs):
    return pudl.transform.eia923.transform(dfs, params['eia923_tables'])


@task(result=LocalResult(), target="{task_name}")  # noqa: FS003
def _transform_eia(params, eia860_dfs, eia923_dfs, datapkg_dir):
    dfs = eia860_dfs.copy()
    dfs.update(eia923_dfs.copy())
    dfs = pudl.helpers.convert_dfs_dict_dtypes(dfs, 'eia')
    entities_dfs, eia_transformed_dfs = pudl.transform.eia.transform(
        dfs,
        eia860_years=params['eia860_years'],
        eia923_years=params['eia923_years'])
    overlap = set(entities_dfs).intersection(set(eia_transformed_dfs))
    if overlap:
        raise AssertionError(
            f"eia and entity dataframes overlap: {sorted(overlap)}")
    eia_transformed_dfs.update(entities_dfs)
    return eia_transformed_dfs


def _etl_eia(etl_params, datapkg_dir, pudl_settings, flow, task_tags):
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
    params = _validate_params_eia(etl_params)
    if not (_all_params_present(params, ['eia923_tables', 'eia923_years']) or
            _all_params_present(params, ['eia860_tables', 'eia860_years'])):
        return None
    with flow:

        with task_tags:
            static_tables = _load_static_tables_eia(datapkg_dir)
            eia860_raw_dfs = _extract_eia860(params, pudl_settings)
            eia860_out_dfs = _transform_eia860(params, eia860_raw_dfs)

            eia923_raw_dfs = _extract_eia923(params, pudl_settings)
            eia923_out_dfs = _transform_eia923(params, eia923_raw_dfs)
            dfs = _transform_eia(params, eia860_out_dfs, eia923_out_dfs, datapkg_dir)
            pudl.load.csv.write_datapackages(dfs, datapkg_dir)
            return _extract_table_names(dfs, static_tables)


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


@task(result=LocalResult(), target="{task_name}")  # noqa: FS003
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
    return {
        'ferc_accounts': ferc_accounts,
        'ferc_depreciation_lines': ferc_depreciation_lines
    }


@task(result=LocalResult(), target="{task_name}")  # noqa: FS003
def _extract_ferc1(params, pudl_settings):
    return pudl.extract.ferc1.extract(
        ferc1_tables=params['ferc1_tables'],
        ferc1_years=params['ferc1_years'],
        pudl_settings=pudl_settings)


@task(result=LocalResult(), target="{task_name}")  # noqa: FS003
def _transform_ferc1(params, dfs):
    return pudl.transform.ferc1.transform(
        dfs, ferc1_tables=params['ferc1_tables'])


def _etl_ferc1(etl_params, datapkg_dir, pudl_settings, flow, task_tags):
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
    params = _validate_params_ferc1(etl_params)
    if not _all_params_present(params, ['ferc1_years', 'ferc1_tables']):
        return None
    with flow:
        with task_tags:
            static_tables = _load_static_tables_ferc1(datapkg_dir)
            dfs = _transform_ferc1(params, _extract_ferc1(params, pudl_settings))
            pudl.load.csv.write_datapackages(dfs, datapkg_dir)
            return _extract_table_names(dfs, static_tables)


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


@task(result=LocalResult(), target="{task_name}")  # noqa: FS003
def _extract_epacems(params, pudl_settings):
    sandbox = pudl_settings.get("sandbox", False)
    ds = pudl.extract.epacems.EpaCemsDatastore(
        Path(pudl_settings["pudl_in"]),
        sandbox=sandbox)
    return pudl.extract.epacems.extract(
        params['epacems_years'], params['epacems_states'], ds)


def _etl_epacems(etl_params, datapkg_dir, pudl_settings, flow, task_tags):
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
    params = _validate_params_epacems(etl_params)

    sandbox = pudl_settings.get("sandbox", False)
    ds = pudl.extract.epacems.EpaCemsDatastore(
        Path(pudl_settings["pudl_in"]),
        sandbox=sandbox)

    if not _all_params_present(params, ['epacems_states', 'epacems_years']):
        return None
    with flow:
        with task_tags:
            # TODO(rousik): This is the task that depends on _eia_transform !!!
            plant_utc_offset = pudl.transform.epacems.load_plant_utc_offset(datapkg_dir)

            raw_dfs = pudl.extract.epacems.extract_fragment.map(
                year=params['epacems_years'],
                state=params['epacems_states'],
                ds=prefect.unmapped(ds))

            dfs = pudl.transform.epacems.transform_fragment.map(
                raw_dfs,
                plant_utc_offset=prefect.unmapped(plant_utc_offset))
            pudl.load.csv.write_datapackages.map(
                df_map=dfs, datapkg_dir=unmapped(datapkg_dir))
            return _extract_table_names(dfs, None)

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


@task(result=LocalResult(), target="{task_name}")  # noqa: FS003
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
    return {'regions_entity_epaipm':
            pd.DataFrame(
                pc.epaipm_region_names, columns=['region_id_epaipm'])}


@task(result=LocalResult(), target="{task_name}")  # noqa: FS003
def _extract_epaipm(params, pudl_settings):
    sandbox = pudl_settings.get("sandbox", False)
    ds = pudl.extract.epaipm.EpaIpmDatastore(
        Path(pudl_settings["pudl_in"]), sandbox=sandbox)
    return pudl.extract.epaipm.extract(params['epaipm_tables'], ds)


@task(result=LocalResult(), target="{task_name}")  # noqa: FS003
def _transform_epaipm(params, dfs):
    return pudl.transform.epaipm.transform(dfs, params['epaipm_tables'])


def _etl_epaipm(etl_params, datapkg_dir, pudl_settings, flow, task_tags):
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
    params = _validate_params_epaipm(etl_params)
    if not _all_params_present(params, ['epaipm_tables']):
        return None
    with flow:
        with task_tags:
            static_tables = _load_static_tables_epaipm(datapkg_dir)
            dfs = _extract_epaipm(params, pudl_settings)
            dfs_names = _transform_epaipm(params, dfs)
            return [static_tables, dfs_names]


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


@task(result=LocalResult(), target="{task_name}")  # noqa: FS003
def _transform_glue(params):
    return pudl.glue.ferc1_eia.glue(ferc1=params['ferc1'], eia=params['eia'])


def _etl_glue(etl_params, datapkg_dir, pudl_settings, flow, task_tags):
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
    params = _validate_params_glue(etl_params)
    if not _all_params_present(params, ['ferc1', 'eia']):
        return None
    with flow:
        with task_tags:
            dfs = _transform_glue(params)
            pudl.load.csv.write_datapackages(dfs, datapkg_dir)
            return _extract_table_names(dfs)


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


def _create_synthetic_dependencies(flow, src_group, dst_group):
    src_tasks = flow.get_tasks(**src_group)
    dst_tasks = flow.get_tasks(**dst_group)
    logger.info(
        f'Linking {len(src_tasks)} to {len(dst_tasks)} [{src_group}] to [{dst_group}]')
    for i in src_tasks:
        for j in dst_tasks:
            flow.add_edge(i, j)


def etl(datapkg_settings, output_dir, pudl_settings, flow=None, bundle_name=None):
    """
    Run ETL process for data package specified by datapkg_settings dictionary.

    This is the coordinating function for generating all of the CSV's for a
    data package. For each of the datasets enumerated in the datapkg_settings,
    this function runs the dataset specific ETL function. Along the way, we are
    accumulating which tables have been loaded. This is useful for generating
    the metadata associated with the package.

    Args)
        datapkg_settings (dict): Validated ETL parameters for a single
            datapackage, originally read in from the PUDL ETL input file.
        output_dir (path-like): The individual datapackage directory, which
            will contain the datapackage.json file and the data directory.
        pudl_settings (dict): a dictionary describing paths to various
            resources and outputs.

    Returns:
        list: List of the task results that hold name of the tables included in the output
        datapackage.

    """
    # compile a list of tables in each dataset
    task_results = []
    etl_funcs = {
        'ferc1': _etl_ferc1,
        'eia': _etl_eia,
        'epacems': _etl_epacems,
        'epaipm': _etl_epaipm,
        'glue': _etl_glue,
    }
    bundle_name_tag = f'bundle/{bundle_name}/{datapkg_settings["name"]}'

    for dataset_dict in datapkg_settings['datasets']:
        for dataset in dataset_dict:
            task_tags = prefect.tags(
                f'dataset/{dataset}', bundle_name_tag)
            df_names = etl_funcs[dataset](
                dataset_dict[dataset], output_dir, pudl_settings,
                flow, task_tags)
            if df_names:
                task_results.append(df_names)
    # Synthetic dependencies between _extract_table_names and write_datapackages
    _create_synthetic_dependencies(
        flow,
        dict(name='write_datapackages', tags=[bundle_name_tag, 'dataset/eia']),
        dict(name='load_plant_utc_offset', tags=[bundle_name_tag]))
    _create_synthetic_dependencies(
        flow,
        dict(name='write_datapackages', tags=[bundle_name_tag]),
        dict(name='_extract_table_names', tags=[bundle_name_tag]))

    # TODO(rousik): this is list of lists
    return prefect.tasks.core.collections.List().bind(*task_results, flow=flow)


class MetadataBundleMaker(prefect.Task):
    """Thin prefect.Task wrapper around pudl.load.metadata.generate_metadata."""

    def __init__(self, datapkg_bundle_settings, output_dir, uuid, doi, *args, **kwargs):  # @
        """Initialize MetadataBundleMaker."""
        # TODO(rousik): the only variable thing here is datapkg_bundle_settings and
        # table_names. This could be turned into task factory that will have simpler
        # API.
        # But perhaps there is an even easier way to refactor the underlying code.
        super().__init__(*args, **kwargs)
        self.datapkg_bundle_settings = datapkg_bundle_settings
        self.output_dir = output_dir
        self.uuid = uuid
        self.doi = doi

    def run(self, table_names):
        """Generates metadata for the datapackage bundle."""
        unique_tables = set()
        for ts in table_names:
            unique_tables.update(ts)

        return pudl.load.metadata.generate_metadata(
            self.datapkg_bundle_settings,
            sorted(unique_tables),
            self.output_dir,
            datapkg_bundle_uuid=self.uuid,
            datapkg_bundle_doi=self.doi)


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
    # Generate a random UUID to identify this ETL run / data package bundle
    # Create, or delete and re-create the top level datapackage bundle directory:

    # datapkg_bundle_uuid = str(uuid.uuid4())
    # datapkg_bundle_dir = Path(pudl_settings["datapkg_dir"], datapkg_bundle_name)
    # _ = pudl.helpers.prep_dir(datapkg_bundle_dir, clobber=clobber)

    # metas = {}
    flow = prefect.Flow("PUDL ETL")
    datapkg_bundle_uuid = str(uuid.uuid4())
    datapkg_bundle_dir = Path(pudl_settings["datapkg_dir"], datapkg_bundle_name)
    # Create, or delete and re-create the top level datapackage bundle directory:
    pudl.helpers.prep_dir(datapkg_bundle_dir, clobber=clobber)

    # validate the settings from the settings file.
    validated_bundle_settings = validate_params(
        datapkg_bundle_settings, pudl_settings)
    # this is a list and should be processed by another task
    for datapkg_settings in validated_bundle_settings:
        output_dir = Path(
            pudl_settings["datapkg_dir"],  # PUDL datapackage output dir
            datapkg_bundle_name,           # Name of the datapackage bundle
            datapkg_settings["name"])      # Name of the datapackage

        # Create the datapackage directory, and its data subdir:
        (output_dir / "data").mkdir(parents=True)

        datapkg_resources = etl(datapkg_settings, output_dir, pudl_settings, flow=flow,
                                bundle_name=datapkg_bundle_name)
        bundle_maker = MetadataBundleMaker(
            datapkg_settings,
            output_dir,
            uuid=datapkg_bundle_uuid, doi=datapkg_bundle_doi,
            name=f'Create metadata for {datapkg_bundle_name}/{datapkg_settings["name"]}')
        bundle_maker.bind(table_names=datapkg_resources, flow=flow)

        # _generate_metadata(
        #        datapkg_settings,
        #        datapkg_resources,
        #        output_dir,
        #        datapkg_bundle_uuid=datapkg_bundle_uuid,
        #        datapkg_bundle_doi=datapkg_bundle_doi)
        # TODO(rousik): figure out how to run _generate_metadata

    # TODO(rousik): print out the flow structure
    flow.visualize()
    # state = flow.run(executor=DaskExecutor(adapt_kwargs={'minimum':2, 'maximum': 10}))
    state = flow.run()
    flow.visualize(flow_state=state)
    # TODO(rousik): figure out if we need to actually return the metas or just toss
    # it away. Perhaps this is needed for testing.
    return {}
    # return {name: state.result[task_result] for name, task_result in meta.items()}
#     return metas
