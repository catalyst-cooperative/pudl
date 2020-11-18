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
from datetime import timedelta
from pathlib import Path

import pandas as pd
import prefect
from prefect import task, unmapped
from prefect.engine import cache_validators
from prefect.engine.cache_validators import all_inputs
from prefect.engine.executors import DaskExecutor
from prefect.engine.results import LocalResult

import pudl
from pudl import constants as pc

logger = logging.getLogger(__name__)


@task
def merge_dataframe_maps(apply_dtypes=None, **kwargs):
    """Aggregates series of dicts into single dict.

    Each named argument can be either dict or list of dicts.

    Returns:
        aggregated dict encompassing key,value mapping from all inputs.
    """
    final_map = {}
    for _, input_map in kwargs.items():
        if type(input_map) == dict:
            final_map.update(input_map)
        elif type(input_map) == list:
            for df_map in input_map:
                final_map.update(df_map)
        else:
            raise AssertionError(
                f'Invalid input type for merge_dataframe_map: {type(input_map)}')
    if apply_dtypes:
        final_map = pudl.helpers.convert_dfs_dict_dtypes(final_map, apply_dtypes)
    return final_map


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


class DatasetPipeline:
    """This object encapsulates the logic for processing pudl dataset.

    When instantiated, it will extract the relevant dataset parameters,
    determine if any tasks need to be run and attaches relevant prefect
    tasks to the provided flow.

    When implementing subclasses of this, you should:
    - set DATASET class attribute
    - implement validate_params() method
    - implement build() method
    """

    DATASET = None

    def __init__(self, pudl_settings, dataset_list, flow, datapkg_name=None, etl_settings=None,
                 clobber=False):
        """Initialize Pipeline object and construct prefect tasks.

        Args:
            pudl_settings (dict): overall configuration (paths and such)
            dataset_list (list): list of named datasets associated with this bundle
            flow (prefect.Flow): attach prefect tasks to this flow
            datapkg_name (str): fully qualified name of the datapackage/bundle
            etl_settings (dict): the complete ETL configuration
            clobber (bool): if True, then existing outputs will be clobbered
        """
        if not self.DATASET:
            raise NotImplementedError(
                f'{self.__cls__.__name__}: missing DATASET attribute')
        self.flow = flow
        self.pipeline_params = None
        self.pudl_settings = pudl_settings
        self.output_dataframes = None
        self.pipeline_params = self._get_dataset_params(dataset_list)
        self.datapkg_name = datapkg_name
        self.etl_settings = etl_settings
        self.clobber = clobber
        if self.pipeline_params:
            self.pipeline_params = self.validate_params(self.pipeline_params)
            self.output_dataframes = self.build(self.pipeline_params)
        else:
            logger.info(
                f'{self.DATASET}: pipeline not running due to missing parameters.')

    def _get_dataset_params(self, dataset_list):
        """Return params that match self.DATASET.

        Args:
          dataset_list: list of dataset parameters to search for the matching params
          structure
        """
        matching_ds = []
        for ds in dataset_list:
            if self.DATASET in ds:
                matching_ds.append(ds[self.DATASET])
        if not matching_ds:
            return None
        if len(matching_ds) > 1:
            raise AssertionError(
                f'Non-unique settings found for dataset {self.DATASET}')
        return matching_ds[0]

    @staticmethod
    def validate_params(datapkg_settings):
        """Validates pipeline parameters/settings.

        Returns:
          (dict) normalized datapkg parameters
        """
        raise NotImplementedError('Please implement pipeline validate_settings method')

    def build(self, etl_params):
        """Add pipeline tasks to the flow.

        Args:
          etl_params: parameters for this pipeline (returned by self.validate_params)
        """
        raise NotImplementedError(
            f'{self.__name__}: Please implement pipeline build method.')

    def get_outputs(self):
        """Returns prefect.Result which contains the output of the pipeline.

        It is expected that the result is a dataframe map which maps table names
        to pandas.DataFrame objects.
        """
        return self.output_dataframes

    def get_table(self, table_name):
        """Returns DataFrame for given table that is emitted by the pipeline.

        This is intended to be used to link pipeline outputs to other prefect tasks.
        It does not actually return the pandas.DataFrame directly but rather creates
        prefect task that scans through the results of this pipeline run and extracts
        the table of interest.

        Args:
          table_name: Name of the table to be returned.

        Returns:
          (prefect.Task) task which returns pandas.DataFrame for the requested table.
        """
        # TODO(rousik): once we break down tasks to table-level granularity, this
        # extraction task/functionality may no longer be needed.
        with self.flow:
            return _extract_table(self.get_outputs(), table_name)

    @classmethod
    def get_pipeline_for_dataset(cls, dataset):
        """Returns subclass of DatasetPipeline associated with the given dataset."""
        for subclass in cls.__subclasses__():
            if subclass.DATASET == dataset:
                return subclass
        return None


###############################################################################
# EIA EXPORT FUNCTIONS
###############################################################################


@task(result=LocalResult(), target="{task_name}")  # noqa: FS003
def _load_static_tables_eia():
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


def pudl_task_target_name(**kwargs):
    """Constructs the path where ETL task result should be stored."""
    target_task = kwargs["task_full_name"]
    logger.debug(
        f'pudl_task_target_name for {target_task} has these kwargs: {sorted(kwargs)}')
    output_path = ''
    parsed_tags = dict(tag.split(':', maxsplit=1) for tag in kwargs["task_tags"])
    if 'datapkg' in parsed_tags:
        output_path += parsed_tags["datapkg"] + "/"
    output_path += kwargs["task_name"]
    return output_path

    # raise NotImplementedError('Booh, this actually does not work.')


@task(result=LocalResult(),
      target=pudl_task_target_name,
      cache_for=timedelta(days=1), cache_validator=all_inputs)
def _extract_eia860(params, pudl_settings):
    sandbox = pudl_settings.get("sandbox", False)
    ds = pudl.workspace.datastore.Datastore(
        Path(pudl_settings["pudl_in"]),
        sandbox=sandbox)
    dfs = pudl.extract.eia860.Extractor(ds).extract(params['eia860_years'])
    return dfs


@task(result=LocalResult(),
      target=pudl_task_target_name,
      cache_for=timedelta(days=1), cache_validator=all_inputs)
def _extract_eia923(params, pudl_settings):
    sandbox = pudl_settings.get("sandbox", False)
    ds = pudl.workspace.datastore.Datastore(
        Path(pudl_settings["pudl_in"]),
        sandbox=sandbox)
    return pudl.extract.eia923.Extractor(ds).extract(params['eia923_years'])


@task(result=LocalResult(),
      target=pudl_task_target_name,
      cache_for=timedelta(days=1),
      cache_validator=cache_validators.partial_inputs_only(['params']))
def _transform_eia860(params, dfs):
    return pudl.transform.eia860.transform(dfs, params['eia860_tables'])


@task(result=LocalResult(),
      target=pudl_task_target_name,
      cache_for=timedelta(days=1),
      cache_validator=cache_validators.partial_inputs_only(['params']))
def _transform_eia923(params, dfs):
    return pudl.transform.eia923.transform(dfs, params['eia923_tables'])


@task(result=LocalResult(),
      target=pudl_task_target_name,
      cache_for=timedelta(days=1),
      cache_validator=cache_validators.partial_inputs_only(['params']))
def _transform_eia(params, dfs):
    return pudl.transform.eia.transform(
        dfs,
        eia860_years=params['eia860_years'],
        eia923_years=params['eia923_years'])
    # TODO(rousik): the above method could be replaced with @task annotation on eia.transform


class EiaPipeline(DatasetPipeline):
    """Runs eia923, eia860 and eia (entity extraction) tasks."""

    DATASET = 'eia'

    @staticmethod  # noqa: C901
    def validate_params(etl_params):
        """Validate and normalize eia parameters."""
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

    def build(self, params):
        """Extract, transform and load CSVs for the EIA datasets."""
        if not (_all_params_present(params, ['eia923_tables', 'eia923_years']) or
                _all_params_present(params, ['eia860_tables', 'eia860_years'])):
            return None
        with self.flow:
            # @with prefect.context(datapkg_name=self.datapkg_name):
            with prefect.tags(f'datapkg:{self.datapkg_name}'):
                static_tables = _load_static_tables_eia()
                eia860_raw_dfs = _extract_eia860(params, self.pudl_settings)
                eia860_out_dfs = _transform_eia860(params, eia860_raw_dfs)

                eia923_raw_dfs = _extract_eia923(params, self.pudl_settings)
                eia923_out_dfs = _transform_eia923(params, eia923_raw_dfs)
                dfs = merge_dataframe_maps(eia860=eia860_out_dfs, eia923=eia923_out_dfs)
                out_dfs = _transform_eia(params, dfs)
                return merge_dataframe_maps(static_tables=static_tables, eia=out_dfs)


###############################################################################
# FERC1 EXPORT FUNCTIONS
###############################################################################
@task(result=LocalResult(), target="{task_name}")  # noqa: FS003
def _load_static_tables_ferc1():
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


@task(result=LocalResult(), cache_for=timedelta(days=1), cache_validator=all_inputs)
def _extract_ferc1(params, pudl_settings):
    return pudl.extract.ferc1.extract(
        ferc1_tables=params['ferc1_tables'],
        ferc1_years=params['ferc1_years'],
        pudl_settings=pudl_settings)


@task(result=LocalResult(), cache_for=timedelta(days=1), cache_validator=all_inputs)
def _transform_ferc1(params, dfs):
    return pudl.transform.ferc1.transform(
        dfs, ferc1_tables=params['ferc1_tables'])


@task(result=LocalResult(), cache_for=timedelta(days=1), cache_validator=all_inputs)
def _extract_table(df_map, table_name):
    if table_name not in df_map:
        raise KeyError(f'Table {table_name} not found in {sorted(df_map)}')
    return df_map.get(table_name)


class Ferc1Pipeline(DatasetPipeline):
    """Runs ferc1 tasks."""

    DATASET = 'ferc1'

    @staticmethod  # noqa: C901
    def validate_params(etl_params):
        """Validate and normalize ferc1 parameters."""
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

    def build(self, params):
        """Add ferc1 tasks to the flow."""
        if not _all_params_present(params, ['ferc1_years', 'ferc1_tables']):
            return None
        with self.flow:
            # ferc1_to_sqlite task should only happen once.
            # Only add this task to the flow if it is not already present.
            if not self.flow.get_tasks(name='ferc1_to_sqlite'):
                pudl.extract.ferc1.ferc1_to_sqlite(
                    self.etl_settings, self.pudl_settings, clobber=True)
                # TODO(rousik): wire the clobber argument to commandline flag,
                # --create-ferc1-sqlite=always|once|never
            raw_dfs = _extract_ferc1(params, self.pudl_settings,
                                     upstream_tasks=self.flow.get_tasks(name='ferc1_to_sqlite'))
            dfs = _transform_ferc1(params, raw_dfs)
            return merge_dataframe_maps(
                static_tables=_load_static_tables_ferc1(),
                dfs=dfs)


class EpaCemsPipeline(DatasetPipeline):
    """Runs epacems tasks."""

    DATASET = 'epacems'

    def __init__(self, *args, eia_pipeline=None, **kwargs):
        """Initializes epacems pipeline, hooks it to the existing eia pipeline.

        epacems depends on the plants_entity_eia table that is generated by the
        EiaPipeline. If epacems is run, it will pull this table from the existing
        eia pipeline.

        Args:
          eia_pipeline: instance of EiaPipeline that holds the plants_entity_eia
            table.
        """
        self.eia_pipeline = eia_pipeline
        super().__init__(*args, **kwargs)

    @staticmethod
    def validate_params(etl_params):
        """Validate and normalize epacems parameters."""
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

    def build(self, params):
        """Add epacems tasks to the flow."""
        if not _all_params_present(params, ['epacems_states', 'epacems_years']):
            return None

        sandbox = self.pudl_settings.get("sandbox", False)
        ds = pudl.extract.epacems.EpaCemsDatastore(
            Path(self.pudl_settings["pudl_in"]),
            sandbox=sandbox)
        with self.flow:
            plants = pudl.transform.epacems.load_plant_utc_offset(
                _extract_table(self.eia_pipeline.get_outputs(), 'plants_entity_eia'))
            raw_dfs = pudl.extract.epacems.extract_fragment.map(
                year=params["epacems_years"],
                state=params["epacems_states"],
                ds=unmapped(ds))
            tf_dfs = pudl.transform.epacems.transform_fragment.map(
                raw_dfs, plant_utc_offset=unmapped(plants))
            return merge_dataframe_maps(epacems_dfs=tf_dfs)


##############################################################################
# EPA IPM ETL FUNCTIONS
###############################################################################
@task(result=LocalResult(), target="{task_name}")  # noqa: FS003
def _load_static_tables_epaipm():
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


@task(result=LocalResult(), cache_for=timedelta(days=1), cache_validator=all_inputs)
def _extract_epaipm(params, pudl_settings):
    sandbox = pudl_settings.get("sandbox", False)
    ds = pudl.extract.epaipm.EpaIpmDatastore(
        Path(pudl_settings["pudl_in"]), sandbox=sandbox)
    return pudl.extract.epaipm.extract(params['epaipm_tables'], ds)


@task(result=LocalResult(), cache_for=timedelta(days=1), cache_validator=all_inputs)
def _transform_epaipm(params, dfs):
    return pudl.transform.epaipm.transform(dfs, params['epaipm_tables'])


class EpaIpmPipeline(DatasetPipeline):
    """Runs epaipm tasks."""

    DATASET = 'epaipm'

    @staticmethod
    def validate_params(etl_params):
        """Validate and normalize epaipm parameters."""
        epaipm_dict = {}
        # pull out the etl_params from the dictionary passed into this function
        try:
            epaipm_dict['epaipm_tables'] = etl_params['epaipm_tables']
        except KeyError:
            epaipm_dict['epaipm_tables'] = []
        if not epaipm_dict['epaipm_tables']:
            return {}
        return epaipm_dict

    def build(self, params):
        """Add epaipm tasks to the flow."""
        if not _all_params_present(params, ['epaipm_tables']):
            return None
        with self.flow:
            # TODO(rousik): annotate epaipm extract/transform methods with @task decorators
            return merge_dataframe_maps(
                static_tables=_load_static_tables_epaipm(),
                dfs=_transform_epaipm(params, _extract_epaipm(params, self.pudl_settings)))


###############################################################################
# GLUE EXPORT FUNCTIONS
###############################################################################
@task(result=LocalResult(),
      target=pudl_task_target_name,
      cache_for=timedelta(days=1))
def _transform_glue(params):
    # TODO(rousik): replace this thin wrapper with @task annotation on ferc1_eia.glue()
    return pudl.glue.ferc1_eia.glue(ferc1=params['ferc1'], eia=params['eia'])


class GluePipeline(DatasetPipeline):
    """Runs glue tasks combining eia/ferc1 results."""

    DATASET = 'glue'

    @staticmethod
    def validate_params(etl_params):
        """Validate and normalize glue parameters."""
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
            return glue_dict

    def build(self, params):
        """Add glue tasks to the flow."""
        if not params.get('ferc1') and not params.get('eia'):
            return None
        with self.flow:
            return _transform_glue(params)


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
        validated_glue_params = GluePipeline.validate_params(glue_param)
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
                pipeline_cls = DatasetPipeline.get_pipeline_for_dataset(dataset)
                if not pipeline_cls:
                    raise AssertionError(
                        f'DatasetPipeline class for dataset {dataset} not found.')
                etl_params = pipeline_cls.validate_params(
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


def etl(datapkg_settings, pudl_settings, flow=None, bundle_name=None,
        datapkg_builder=None, etl_settings=None, clobber=False):
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
        etl_settings (dict): the complete configuration for the ETL run.
        clobber (bool): if True then existing results will be overwritten.

    Returns:
        list: List of the task results that hold name of the tables included in the output
        datapackage.

    """
    output_dir = datapkg_builder.get_datapkg_output_dir(datapkg_settings)
    pipelines = {}
    dataset_list = datapkg_settings['datasets']  # list of {dataset: params_dict}
    # For debugging purposes, print the dataset names
    dataset_names = set()
    for ds in dataset_list:
        dataset_names.update(ds.keys())
    logger.warning(
        f'Running etl with the following configurations: {sorted(dataset_names)}')

    # datapkg_name = datapkg_builder.get_datapkg_name(datapkg_settings)

    for pl_class in [Ferc1Pipeline, EiaPipeline, EpaIpmPipeline, GluePipeline]:
        pipelines[pl_class.DATASET] = pl_class(
            pudl_settings, dataset_list, flow,
            datapkg_name=datapkg_builder.get_datapkg_name(datapkg_settings),
            etl_settings=etl_settings, clobber=clobber)
    # EpaCems pipeline is special because it needs to read the output of eia
    # pipeline
    pipelines['epacems'] = EpaCemsPipeline(
        pudl_settings,
        dataset_list,
        flow,
        eia_pipeline=pipelines['eia'])
    with flow:
        table_names = pudl.load.csv.write_datapackages.map(
            [pl.get_outputs() for pl in pipelines.values() if pl.get_outputs()],
            datapkg_dir=unmapped(output_dir))
        datapkg_builder(table_names, datapkg_settings)
        # TODO(rousik): how can we set name on this task?


class DatapackageBuilder(prefect.Task):
    """Builds and validates datapackages.

    Writes metadata for each data package and validates results.
    """

    def __init__(self, bundle_name, pudl_settings, doi, *args, **kwargs):
        """Constructs the datapackage builder.

        Args:
            bundle_name: top-level name of the bundle that is being
              built by this instance.
            pudl_settings: configuration object for this ETL run.
            doi: doi associated with the datapackages that are to be built.
        """
        self.uuid = str(uuid.uuid4())
        self.doi = doi
        self.pudl_settings = pudl_settings
        self.bundle_dir = Path(pudl_settings["datapkg_dir"], bundle_name)
        self.bundle_name = bundle_name
        logger.warning(f'DatapackageBuilder uuid is {self.uuid}')
        super().__init__(*args, **kwargs)

    def prepare_output_directories(self, clobber=False):
        """Create (or wipe and re-create) output directory for the bundle."""
        logger.info(f'Prep dir {self.bundle_dir}')
        pudl.helpers.prep_dir(self.bundle_dir, clobber=clobber)

    def make_datapkg_dir(self, datapkg_settings):
        """Create directory to hold the datapackage csv files."""
        (self.get_datapkg_output_dir(datapkg_settings) / "data").mkdir(parents=True)

    def get_datapkg_output_dir(self, datapkg_settings):
        """Returns path where datapkg files should be stored."""
        return Path(
            self.pudl_settings["datapkg_dir"],
            self.bundle_name,
            datapkg_settings["name"])

    def get_datapkg_name(self, datapkg_settings):
        """Returns fully qualified datapkg name in the form of bundle_name/datapkg."""
        return f'{self.bundle_name}/{datapkg_settings["name"]}'

    def run(self, list_of_df_maps, datapkg_settings):
        """Write metadata and validate contents."""
        unique_tables = set()
        for df_map in list_of_df_maps:
            unique_tables.update(df_map)
        datapkg_full_name = self.get_datapkg_name(datapkg_settings)
        tables = sorted(unique_tables)
        logger.info(f'Building metadata for {datapkg_full_name} with tables: {tables}')
        return pudl.load.metadata.generate_metadata(
            datapkg_settings,
            tables,
            self.get_datapkg_output_dir(datapkg_settings),
            datapkg_bundle_uuid=self.uuid,
            datapkg_bundle_doi=self.doi)


def generate_datapkg_bundle(etl_settings,
                            pudl_settings,
                            datapkg_bundle_name,
                            datapkg_bundle_doi=None,
                            clobber=False,
                            use_dask_executor=False):
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
        etl_settings (dict): ETL configuration object.
        pudl_settings (dict): a dictionary filled with settings that mostly
            describe paths to various resources and outputs.
        datapkg_bundle_name (str): name of directory you want the bundle of
            data packages to live.
        clobber (bool): If True and there is already a directory with data
            packages with the datapkg_bundle_name, the existing data packages
            will be deleted and new data packages will be generated in their
            place.
        use_dask_executor (bool): If True, launch local Dask cluster to run the
            ETL tasks on.

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

    datapkg_bundle_settings = etl_settings['datapkg_bundle_settings']
    flow = prefect.Flow("PUDL ETL")
    datapkg_builder = DatapackageBuilder(
        datapkg_bundle_name, pudl_settings, doi=datapkg_bundle_doi)
    # Create, or delete and re-create the top level datapackage bundle directory:
    datapkg_builder.prepare_output_directories(clobber=clobber)

    # validate the settings from the settings file.
    validated_bundle_settings = validate_params(
        datapkg_bundle_settings, pudl_settings)
    # this is a list and should be processed by another task
    for datapkg_settings in validated_bundle_settings:
        datapkg_builder.make_datapkg_dir(datapkg_settings)
        etl(datapkg_settings, pudl_settings, flow=flow,
            bundle_name=datapkg_bundle_name, datapkg_builder=datapkg_builder,
            etl_settings=etl_settings, clobber=clobber)

    # TODO(rousik): print out the flow structure
    flow.visualize()
    if use_dask_executor:
        state = flow.run(executor=DaskExecutor(
            adapt_kwargs={'minimum': 2, 'maximum': 10}))
    else:
        state = flow.run()
    flow.visualize(flow_state=state)
    # TODO(rousik): determine what kind of return value should happen here. For now lets just
    # not return anything.
    return {}
