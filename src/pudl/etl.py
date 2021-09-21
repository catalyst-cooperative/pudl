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

"""
import logging
import time
from pathlib import Path
from typing import Dict

import pandas as pd
import sqlalchemy as sa

import pudl
from pudl import constants as pc
from pudl.constants import PUDL_TABLES
from pudl.metadata.labels import (ENERGY_SOURCES_EIA,
                                  FUEL_TRANSPORTATION_MODES_EIA,
                                  FUEL_TYPES_AER_EIA, PRIME_MOVERS_EIA)
from pudl.workspace.datastore import Datastore

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
    return partition_dict


def check_for_bad_years(try_years, dataset):
    """Check for bad data years."""
    bad_years = [
        y for y in try_years
        if y not in pc.working_partitions[dataset]['years']]
    if bad_years:
        raise AssertionError(f"Unrecognized {dataset} years: {bad_years}")


def check_for_bad_tables(try_tables, dataset):
    """Check for bad data tables."""
    bad_tables = [t for t in try_tables if t not in PUDL_TABLES[dataset]]
    if bad_tables:
        raise AssertionError(f"Unrecognized {dataset} table: {bad_tables}")

###############################################################################
# EIA EXPORT FUNCTIONS
###############################################################################


def _validate_params_eia(etl_params):
    # extract all of the etl_params for the EIA ETL function
    # empty dictionary to compile etl_params
    eia_input_dict = {}
    # when nothing is set in the settings file, the years will default as none
    eia_input_dict['eia860_years'] = etl_params.get('eia860_years', [])
    # Ensure there are no duplicate years:
    eia_input_dict["eia860_years"] = sorted(set(eia_input_dict["eia860_years"]))

    # the tables will default to all of the tables if nothing is given
    eia_input_dict['eia860_tables'] = etl_params.get(
        'eia860_tables', PUDL_TABLES['eia860']
    )
    # Ensure no duplicate tables:
    eia_input_dict['eia860_tables'] = list(set(eia_input_dict['eia860_tables']))

    # if eia860_ytd updates flag isn't included, the default is to not load ytd
    eia_input_dict['eia860_ytd'] = etl_params.get('eia860_ytd', False)

    eia_input_dict['eia923_years'] = etl_params.get('eia923_years', [])
    # Ensure no duplicate years:
    eia_input_dict['eia923_years'] = sorted(set(eia_input_dict['eia923_years']))

    eia_input_dict['eia923_tables'] = etl_params.get(
        'eia923_tables', PUDL_TABLES['eia923']
    )
    # Ensure no duplicate tables:
    eia_input_dict['eia923_tables'] = list(set(eia_input_dict['eia923_tables']))

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

    eia860m_year = pd.to_datetime(
        pc.working_partitions['eia860m']['year_month']).year
    if (eia_input_dict['eia860_ytd']
            and (eia860m_year in eia_input_dict['eia860_years'])):
        raise AssertionError(
            "Attempting to integrate an eia860m year "
            f"({eia860m_year}) that is within the eia860 years: "
            f"{eia_input_dict['eia860_years']}. Consider switching eia860_ytd "
            "parameter to False."
        )
    check_for_bad_tables(
        try_tables=eia_input_dict['eia923_tables'], dataset='eia923')
    check_for_bad_tables(
        try_tables=eia_input_dict['eia860_tables'], dataset='eia860')
    check_for_bad_years(
        try_years=eia_input_dict['eia860_years'], dataset='eia860')
    check_for_bad_years(
        try_years=eia_input_dict['eia923_years'], dataset='eia923')

    return eia_input_dict


def _read_static_tables_eia() -> Dict[str, pd.DataFrame]:
    """Build dataframes of static EIA tables for use as foreign key constraints.

    There are many values specified within the data that are essentially
    constant, but which we need to store for data validation purposes, for use
    as foreign keys.  E.g. the list of valid EIA fuel type codes, or the
    possible state and country codes indicating a coal delivery's location of
    origin. For now these values are primarily stored in a large collection of
    lists, dictionaries, and dataframes which are specified in the
    :mod:`pudl.constants` module.

    """
    return {
        'energy_sources_eia': pd.DataFrame(
            columns=["abbr", "energy_source"],
            data=ENERGY_SOURCES_EIA.items(),
        ),
        'fuel_types_aer_eia': pd.DataFrame(
            columns=["abbr", "fuel_type"],
            data=FUEL_TYPES_AER_EIA.items(),
        ),
        'prime_movers_eia': pd.DataFrame(
            columns=["abbr", "prime_mover"],
            data=PRIME_MOVERS_EIA.items(),
        ),
        'fuel_transportation_modes_eia': pd.DataFrame(
            columns=["abbr", "fuel_transportation_mode"],
            data=FUEL_TRANSPORTATION_MODES_EIA.items(),
        ),
    }


def _etl_eia(etl_params, ds_kwargs):
    """Extract, transform and load CSVs for the EIA datasets.

    Args:
        etl_params (dict): ETL parameters required by this data source.
        ds_kwargs: (dict): Keyword arguments for instantiating a PUDL datastore,
            so that the ETL can access the raw input data.

    Returns:
        list: Names of PUDL DB tables output by the ETL for this data source.

    """
    eia_inputs = _validate_params_eia(etl_params)
    eia860_tables = eia_inputs["eia860_tables"]
    eia860_years = eia_inputs["eia860_years"]
    eia860_ytd = eia_inputs["eia860_ytd"]
    eia923_tables = eia_inputs["eia923_tables"]
    eia923_years = eia_inputs["eia923_years"]

    if (
        (not eia923_tables or not eia923_years)
        and (not eia860_tables or not eia860_years)
    ):
        logger.info('Not loading EIA.')
        return []

    # generate dataframes for the static EIA tables
    out_dfs = _read_static_tables_eia()

    ds = Datastore(**ds_kwargs)
    # Extract EIA forms 923, 860
    eia923_raw_dfs = pudl.extract.eia923.Extractor(ds).extract(
        year=eia923_years)
    eia860_raw_dfs = pudl.extract.eia860.Extractor(ds).extract(
        year=eia860_years)
    # if we are trying to add the EIA 860M YTD data, then extract it and append
    if eia860_ytd:
        eia860m_raw_dfs = pudl.extract.eia860m.Extractor(ds).extract(
            year_month=pc.working_partitions['eia860m']['year_month'])
        eia860_raw_dfs = pudl.extract.eia860m.append_eia860m(
            eia860_raw_dfs=eia860_raw_dfs, eia860m_raw_dfs=eia860m_raw_dfs)

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
        eia860_ytd=eia860_ytd,
    )
    # convert types..
    entities_dfs = pudl.helpers.convert_dfs_dict_dtypes(entities_dfs, 'eia')

    out_dfs.update(entities_dfs)
    out_dfs.update(eia_transformed_dfs)
    return out_dfs


###############################################################################
# FERC1 EXPORT FUNCTIONS
###############################################################################
def _validate_params_ferc1(etl_params):
    ferc1_dict = {}
    # pull out the etl_params from the dictionary passed into this function
    ferc1_dict['ferc1_years'] = etl_params.get('ferc1_years', [])
    # Ensure no there are no duplicate years by converting to set and back:
    ferc1_dict["ferc1_years"] = sorted(set(ferc1_dict["ferc1_years"]))

    # the tables will default to all of the tables if nothing is given
    ferc1_dict['ferc1_tables'] = etl_params.get(
        'ferc1_tables', PUDL_TABLES['ferc1']
    )
    # Ensure no duplicate tables:
    ferc1_dict["ferc1_tables"] = list(set(ferc1_dict["ferc1_tables"]))

    ferc1_dict['debug'] = etl_params.get('debug', False)

    if not ferc1_dict['debug']:
        check_for_bad_tables(
            try_tables=ferc1_dict['ferc1_tables'], dataset='ferc1')

    if not ferc1_dict['ferc1_years']:
        ferc1_dict = {}

    return ferc1_dict


def _read_static_tables_ferc1() -> Dict[str, pd.DataFrame]:
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
    return {
        'ferc_accounts': pc.ferc_electric_plant_accounts[[
            "ferc_account_id",
            "ferc_account_description",
        ]],
        'ferc_depreciation_lines': pc.ferc_accumulated_depreciation[[
            "line_id",
            "ferc_account_description",
        ]],
    }


def _etl_ferc1(etl_params, pudl_settings) -> Dict[str, pd.DataFrame]:
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

    # Compile static FERC 1 dataframes
    out_dfs = _read_static_tables_ferc1()

    # Extract FERC form 1
    ferc1_raw_dfs = pudl.extract.ferc1.extract(
        ferc1_tables=ferc1_tables,
        ferc1_years=ferc1_years,
        pudl_settings=pudl_settings)
    # Transform FERC form 1
    ferc1_transformed_dfs = pudl.transform.ferc1.transform(
        ferc1_raw_dfs, ferc1_tables=ferc1_tables)

    out_dfs.update(ferc1_transformed_dfs)
    return out_dfs


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
            epacems_dict['epacems_states'] = pc.working_partitions['epacems']['states']

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
        epacems_dict, [PUDL_TABLES["epacems"]])
    if not epacems_dict['partition']:
        raise AssertionError(
            'No partition found for EPA CEMS.'
            'EPA CEMS requires either states or years as a partion'
        )

    if not epacems_dict['epacems_years'] or not epacems_dict['epacems_states']:
        return None
    else:
        return epacems_dict


def etl_epacems(etl_params, pudl_settings, ds_kwargs) -> None:
    """Extract, transform and load CSVs for EPA CEMS.

    Args:
        etl_params (dict): ETL parameters required by this data source.
        pudl_settings (dict) : a dictionary filled with settings that mostly
            describe paths to various resources and outputs.
        ds_kwargs: (dict): Keyword arguments for instantiating a PUDL datastore,
            so that the ETL can access the raw input data.

    Returns:
        None: Unlike the other ETL functions, the EPACEMS writes its output to
            Parquet as it goes, since the dataset is too large to hold in memory.
            So it doesn't return a dictionary of dataframes.

    """
    epacems_dict = _validate_params_epacems(etl_params)
    # Deduplicate the years and states just in case. This happens outside of
    # the settings validation because this ETL function is also called directly
    # in the epacems_to_parquet() script.
    epacems_years = sorted(set(epacems_dict['epacems_years']))
    epacems_states = sorted(set(epacems_dict['epacems_states']))

    # If we're not doing CEMS, just stop here to avoid printing messages like
    # "Reading EPA CEMS data...", which could be confusing.
    if not epacems_states or not epacems_years:
        logger.info('Not ingesting EPA CEMS.')

    pudl_engine = sa.create_engine(pudl_settings["pudl_db"])

    # Verify that we have a PUDL DB with plant attributes:
    inspector = sa.inspect(pudl_engine)
    if "plants_eia860" not in inspector.get_table_names():
        raise RuntimeError(
            "No plants_eia860 available in the PUDL DB! Have you run the ETL? "
            f"Trying to access PUDL DB: {pudl_engine}"
        )

    eia_plant_years = pd.read_sql(
        """
        SELECT DISTINCT strftime('%Y', report_date)
        AS year
        FROM plants_eia860
        ORDER BY year ASC
        """, pudl_engine).year.astype(int)
    missing_years = list(set(epacems_years) - set(eia_plant_years))
    if missing_years:
        logger.info(
            f"EPA CEMS years with no EIA plant data: {missing_years} "
            "Some timezones may be estimated based on plant state."
        )

    # NOTE: This is a generator for raw dataframes
    epacems_raw_dfs = pudl.extract.epacems.extract(
        epacems_years, epacems_states, Datastore(**ds_kwargs))

    # NOTE: This is a generator for transformed dataframes
    epacems_transformed_dfs = pudl.transform.epacems.transform(
        epacems_raw_dfs=epacems_raw_dfs,
        pudl_engine=pudl_engine,
    )

    logger.info("Processing EPA CEMS data and writing it to Apache Parquet.")
    if logger.isEnabledFor(logging.INFO):
        start_time = time.monotonic()

    # run the cems generator dfs through the load step
    for df in epacems_transformed_dfs:
        pudl.load.parquet.epacems_to_parquet(
            df,
            root_path=Path(pudl_settings["parquet_dir"]) / "epacems",
        )

    if logger.isEnabledFor(logging.INFO):
        delta_t = time.strftime("%H:%M:%S", time.gmtime(
            time.monotonic() - start_time))
        time_message = f"Processing EPA CEMS took {delta_t}"
        logger.info(time_message)
        start_time = time.monotonic()


###############################################################################
# GLUE EXPORT FUNCTIONS
###############################################################################
def _validate_params_glue(etl_params):
    glue_dict = {}
    # pull out the etl_params from the dictionary passed into this function
    glue_dict['ferc1'] = etl_params.get('ferc1', False)
    glue_dict['eia'] = etl_params.get('eia', False)

    if glue_dict['ferc1'] or glue_dict['eia']:
        return glue_dict
    else:
        return {}


def _etl_glue(etl_params) -> Dict[str, pd.DataFrame]:
    """Extract, transform and load CSVs for the Glue tables.

    Args:
        etl_params (dict): ETL parameters required by this data source.

    Returns:
        dict: A dictionary of :class:`pandas.Dataframe` whose keys are the names
        of the corresponding database table.

    """
    glue_dict = _validate_params_glue(etl_params)
    if not glue_dict:
        raise ValueError(
            "Neither EIA nor FERC 1 data is beiing processed. Nothing to glue."
        )
    # grab the glue tables for ferc1 & eia
    glue_dfs = pudl.glue.ferc1_eia.glue(
        ferc1=glue_dict['ferc1'],
        eia=glue_dict['eia'],
    )

    # Add the EPA to EIA crosswalk, but only if the eia data is being processed.
    # Otherwise the foreign key references will have nothing to point at:
    if glue_dict["eia"]:
        glue_dfs.update(pudl.glue.eia_epacems.grab_clean_split())

    return glue_dfs


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


def validate_params(datapkg_bundle_settings):
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


def etl(  # noqa: C901
    etl_settings_bundle,
    pudl_settings,
    clobber: bool = False,
    use_local_cache: bool = True,
    gcs_cache_path: str = None,
    check_foreign_keys: bool = True,
    check_types: bool = True,
    check_values: bool = True,
):
    """
    Run the PUDL Extract, Transform, and Load data pipeline.

    First we validate the settings, and then process data destined for loading
    into SQLite, which includes The FERC Form 1 and the EIA Forms 860 and 923.
    Once those data have been output to SQLite we mvoe on to processing the
    long tables, which will be loaded into Apache Parquet files. Some of this
    processing depends on data that's already been loaded into the SQLite DB.

    Args:
        etl_settings_bundle (iterable): a list of dictionaries. Each item
            in the list corresponds to a data package. Each data package's
            dictionary contains the arguements for its ETL function.
        pudl_settings (dict): a dictionary filled with settings that mostly
            describe paths to various resources and outputs.
        clobber (bool): If True and there is already a pudl.sqlite database
            it will be deleted and a new one will be created.
        use_local_cache (bool): controls whether datastore should be using local
            file cache.
        gcs_cache_path (str): controls whether datastore should be using Google
            Cloud Storage based cache.

    Returns:
        None

    """
    pudl_db_path = Path(pudl_settings["sqlite_dir"]) / "pudl.sqlite"
    if pudl_db_path.exists() and not clobber:
        raise SystemExit(
            "The PUDL DB already exists, and we don't want to clobber it.\n"
            f"Move {pudl_db_path} aside or set clobber=True and try again."
        )

    # Configure how we want to obtain raw input data:
    ds_kwargs = dict(
        gcs_cache_path=gcs_cache_path,
        sandbox=pudl_settings.get("sandbox", False)
    )
    if use_local_cache:
        ds_kwargs["local_cache_path"] = Path(pudl_settings["pudl_in"]) / "data"

    validated_etl_settings = validate_params(etl_settings_bundle)
    # This is an artefact of outputting multiple data packages in the past.
    # Now the settings file should only describe a single ETL run, but we
    # haven't revised the ETL settings validation code yet, so we just grab
    # the first specification:
    num_settings = len(validated_etl_settings)
    if num_settings != 1:
        raise RuntimeError(
            "The PUDL ETL expects one and only one set of datasets to process "
            f"in each settings file, but here we found {num_settings}."
        )
    etl_settings = validated_etl_settings[0]

    # Check for existing EPA CEMS outputs if we're going to process CEMS, and
    # do it before running the SQLite part of the ETL so we don't do a bunch of
    # work only to discover that we can't finish.
    for dataset in etl_settings["datasets"]:
        if dataset.get("epacems", False):
            epacems_pq_path = Path(pudl_settings["parquet_dir"]) / "epacems"
            _ = pudl.helpers.prep_dir(epacems_pq_path, clobber=clobber)

    sqlite_dfs = {}
    # This could be cleaner if we simplified the settings file format:
    for dataset in etl_settings["datasets"]:
        if dataset.get("ferc1", False):
            sqlite_dfs.update(_etl_ferc1(dataset["ferc1"], pudl_settings))
        elif dataset.get("eia", False):
            sqlite_dfs.update(_etl_eia(dataset["eia"], ds_kwargs))
        elif dataset.get("glue", False):
            sqlite_dfs.update(_etl_glue(dataset["glue"]))

    # Load the ferc1 + eia data directly into the SQLite DB:
    pudl_engine = sa.create_engine(pudl_settings["pudl_db"])
    pudl.load.sqlite.dfs_to_sqlite(
        sqlite_dfs,
        engine=pudl_engine,
        check_foreign_keys=check_foreign_keys,
        check_types=check_types,
        check_values=check_values,
    )

    # Parquet Outputs:
    for dataset in etl_settings["datasets"]:
        if dataset.get("epacems", False):
            etl_epacems(dataset["epacems"], pudl_settings, ds_kwargs)
