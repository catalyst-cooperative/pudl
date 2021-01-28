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
import argparse
import itertools
import logging
import os
import shutil
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import fsspec
import pandas as pd
import prefect
import pyarrow
from prefect import task, unmapped
from prefect.engine.results import GCSResult, LocalResult
from prefect.executors import DaskExecutor
from prefect.executors.local import LocalExecutor
from prefect.tasks.shell import ShellTask
from pyarrow import parquet

import pudl
from pudl import constants as pc
from pudl import dfc
from pudl.convert import datapkg_to_sqlite, epacems_to_parquet
from pudl.dfc import DataFrameCollection
from pudl.extract.epacems import EpaCemsPartition
from pudl.extract.ferc1 import SqliteOverwriteMode
from pudl.load import csv

logger = logging.getLogger(__name__)


def command_line_flags() -> argparse.ArgumentParser:
    """Returns argparse.ArgumentParser containing flags relevant to the ETL component."""
    parser = argparse.ArgumentParser(
        description="ETL configuration flags", add_help=False)
    parser.add_argument(
        '-c',
        '--clobber',
        action='store_true',
        help="""Clobber existing datapackages if they exist. If clobber is not
        included but the datapackage bundle directory already exists the _build
        will fail. Either the datapkg_bundle_name in the settings_file needs to
        be unique or you need to include --clobber""",
        default=False)
    parser.add_argument(
        "--use-dask-executor",
        action="store_true",
        default=False,
        help='If enabled, use local DaskExecutor to run the flow.')
    parser.add_argument(
        "--dask-executor-address",
        default=None,
        help='If specified, use pre-existing DaskExecutor at this address.')
    parser.add_argument(
        "--upload-to-gcs",
        type=str,
        default=os.environ.get('PUDL_UPLOAD_TO_GCS'),
        help="""gs://bucket/path where the ETL results should be uploaded to.

        If not set, the default value will be read from PUDL_UPLOAD_TO_GCS
        environment variable.

        Files will be stored under ${pudl_upload_to_gcs}/${run_id}.

        If neither command-line flag nor env variable are set, the results
        will be stored to local disk only and will not be uploaded to GCS.""")
    parser.add_argument(
        "--overwrite-ferc1-db",
        type=lambda mode: SqliteOverwriteMode[mode],
        default=SqliteOverwriteMode.ALWAYS,
        choices=list(SqliteOverwriteMode))
    parser.add_argument(
        "--show-flow-graph",
        action="store_true",
        default=False,
        help="Controls whether flow dependency graphs should be displayed.")
    parser.add_argument(
        "--gcs-cache-path",
        type=str,
        default=os.environ.get('PUDL_GCS_CACHE_PATH'),
        help="""Specifies path in the form of gs://${bucket-name}[/optional-path-prefix].

        If set, datastore will use this storage bucket as a caching layer and will retrieve
        resources from there before contacting Zenodo.

        This caching layer will be set to read-only mode. If you need to modify its
        contents you should use pudl_datastore to populate it.

        If not specified, the default will be loaded from environment variable
        PUDL_GCS_CACHE_PATH. If that one is not set, Google Cloud Storage caching will
        not be used.""")
    # TODO(rousik): the above should be marked as "datastore" cache.
    parser.add_argument(
        "--bypass-local-cache",
        action="store_true",
        default=False,
        help="If enabled, the local file cache for datastore will not be used.")
    # TODO(rousik): the above should also be marked as "datastore" cache
    parser.add_argument(
        "--gcs-bucket-for-prefect-cache",
        type=str,
        default=None,
        help="""If specified, use given GCS bucket to cache prefect task results.""")
    parser.add_argument(
        "--task-result-cache-path",
        type=str,
        default=None,
        help="""Specifies where temporary DataFrames shoudl be written. This could be
        either absolute path, file://local-path or gs://path-to-cloud-storage. DataFrames
        will be serialized in the form of parquet files in this location as they're passed
        between prefect tasks.""")
    parser.add_argument(
        "--pipeline-cache-path",
        type=str,
        default=None,
        help="""Controls where the pipeline should be storing its cache. This should be
        used for both the prefect task results as well as for the DataFrameCollections.""")
    parser.add_argument(
        "--keep-cache",
        action="store_true",
        help="""Do not remove local pipeline cache even if the pipeline succeeds. This can
        be used for development/debugging purposes.
        """)
    parser.add_argument(
        "--validate",
        default=False,
        action="store_true",
        help="""If enabled, run validation via tox from the current directory.""")

    # TODO(rousik): the above should replace --task-result-cache-path and
    # --gcs-bucket-for-prefect-cache. This should also be made such that both
    # local paths as well as gs://paths are supported.

    return parser


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
    return all(params.get(p) for p in required_params)


def check_for_bad_years(try_years, dataset):
    """Check for bad data years."""
    bad_years = [
        y for y in try_years
        if y not in pc.working_partitions[dataset]['years']]
    if bad_years:
        raise AssertionError(f"Unrecognized {dataset} years: {bad_years}")


def check_for_bad_tables(try_tables, dataset):
    """Check for bad data tables."""
    bad_tables = [t for t in try_tables if t not in pc.pudl_tables[dataset]]
    if bad_tables:
        raise AssertionError(f"Unrecognized {dataset} table: {bad_tables}")


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
                 clobber=False, datapkg_dir=None):
        """Initialize Pipeline object and construct prefect tasks.

        Args:
            pudl_settings (dict): overall configuration (paths and such)
            dataset_list (list): list of named datasets associated with this bundle
            flow (prefect.Flow): attach prefect tasks to this flow
            datapkg_name (str): fully qualified name of the datapackage/bundle
            etl_settings (dict): the complete ETL configuration
            clobber (bool): if True, then existing outputs will be clobbered
            datapkg_dir (str): specifies where the output datapkg csv files should be
              written to.
        """
        if not self.DATASET:
            raise NotImplementedError(
                f'{self.__cls__.__name__}: missing DATASET attribute')
        self.flow = flow
        self.pipeline_params = None
        self.pudl_settings = pudl_settings
        self.output_dfc = None
        self.pipeline_params = self._get_dataset_params(dataset_list)
        self.datapkg_name = datapkg_name
        self.etl_settings = etl_settings
        self.clobber = clobber
        self.datapkg_dir = datapkg_dir
        if self.pipeline_params:
            self.pipeline_params = self.validate_params(self.pipeline_params)
            self.output_dfc = self.build(self.pipeline_params)

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

    def outputs(self):
        """Returns prefect.Result containing DataFrameCollection."""
        return self.output_dfc

    def is_executed(self):
        """Returns true if the pipeline is executed."""
        return bool(self.pipeline_params)

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


@task(target="eia.static-tables")
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
    return DataFrameCollection(
        fuel_type_eia923=pd.DataFrame(
            {'abbr': list(pc.fuel_type_eia923.keys()),
             'fuel_type': list(pc.fuel_type_eia923.values())}
        ),
        prime_movers_eia923=pd.DataFrame(
            {'abbr': list(pc.prime_movers_eia923.keys()),
             'prime_mover': list(pc.prime_movers_eia923.values())}
        ),
        fuel_type_aer_eia923=pd.DataFrame(
            {'abbr': list(pc.fuel_type_aer_eia923.keys()),
             'fuel_type': list(pc.fuel_type_aer_eia923.values())}
        ),
        energy_source_eia923=pd.DataFrame(
            {'abbr': list(pc.energy_source_eia923.keys()),
             'source': list(pc.energy_source_eia923.values())}
        ),
        transport_modes_eia923=pd.DataFrame(
            {'abbr': list(pc.transport_modes_eia923.keys()),
             'mode': list(pc.transport_modes_eia923.values())}
        )
    )


@task
def merge_eia860m(eia860: DataFrameCollection, eia860m: DataFrameCollection):
    """Combines overlapping eia860 and eia860m data frames."""
    eia860m_dfs = eia860m.to_dict()
    result = DataFrameCollection()

    for table_name, table_id in eia860.references():
        if table_name not in eia860m_dfs:
            result.add_reference(table_name, table_id)
        else:
            logger.info(
                f'Extending eia860 table {table_name} with montly data from eia860m.')
            df = eia860.get(table_name)
            df = df.append(eia860m_dfs[table_name], ignore_index=True, sort=True)
            result.store(table_name, df)
    # To be really safe, check that there are no eia860m tables that are not present in eia860.
    known_eia860_tables = set(eia860.get_table_names())
    for table_name in list(eia860m_dfs):
        if table_name not in known_eia860_tables:
            logger.error(f'eia860m table {table_name} is not present in eia860')
    return result


class EiaPipeline(DatasetPipeline):
    """Runs eia923, eia860 and eia (entity extraction) tasks."""

    DATASET = 'eia'

    @staticmethod  # noqa: C901
    def validate_params(etl_params):
        """Validate and normalize eia parameters."""
        # extract all of the etl_params for the EIA ETL function
        # empty dictionary to compile etl_params
        eia_input_dict = {
            'eia860_years': etl_params.get('eia860_years', []),
            'eia860_tables': etl_params.get('eia860_tables', pc.pudl_tables['eia860']),

            'eia860_ytd': etl_params.get('eia860_ytd', False),

            'eia923_years': etl_params.get('eia923_years', []),
            'eia923_tables': etl_params.get('eia923_tables', pc.pudl_tables['eia923']),
        }

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

    def build(self, params):
        """Extract, transform and load CSVs for the EIA datasets.

        Returns:
            prefect.Result object that contains emitted table names.
        """
        if not (_all_params_present(params, ['eia923_tables', 'eia923_years']) or
                _all_params_present(params, ['eia860_tables', 'eia860_years'])):
            return None

        # TODO(rousik): task names are nice for human readable results but in the end, they
        # are probably not worth worrying about.
        eia860_extract = pudl.extract.eia860.Extractor(
            name='eia860.extract',
            target=f'{self.datapkg_name}/eia860.extract')
        eia860m_extract = pudl.extract.eia860m.Extractor(
            name='eia860m.extract',
            target=f'{self.datapkg_name}/eia860m.extract')
        eia923_extract = pudl.extract.eia923.Extractor(
            name='eia923.extract',
            target=f'{self.datapkg_name}/eia923.extract')
        with self.flow:
            eia860_df = eia860_extract(year=params['eia860_years'])
            if params['eia860_ytd']:
                m_df = eia860m_extract(
                    year_month=pc.working_partitions['eia860m']['year_month'])
                eia860_df = merge_eia860m(
                    eia860_df, m_df,
                    task_args=dict(target=f'{self.datapkg_name}/eia860m.merge'))
            eia860_df = pudl.transform.eia860.transform_eia860(
                eia860_df,
                params['eia860_tables'],
                task_args=dict(target=f'{self.datapkg_name}/eia860.transform'))

            eia923_df = pudl.transform.eia923.transform_eia923(
                eia923_extract(year=params['eia923_years']),
                params['eia923_tables'],
                task_args=dict(target=f'{self.datapkg_name}/eia923.transform'))

            output_tables = pudl.transform.eia.transform_eia(
                dfc.merge_list(
                    [eia860_df, eia923_df, pudl.glue.eia_epacems.grab_clean_split()]),
                eia860_years=params['eia860_years'],
                eia923_years=params['eia923_years'],
                eia860_ytd=params['eia860_ytd'])
            return dfc.merge(
                _load_static_tables_eia(),
                output_tables,
                task_args=dict(target=f'{self.datapkg_name}/eia.final_tables'))

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
        # Loads csv form
        # TODO(rousik): once we break down tasks to table-level granularity, this
        # extraction task/functionality may no longer be needed.
        return _extract_table.bind(self.output_dfc, table_name, flow=self.flow)

###############################################################################
# FERC1 EXPORT FUNCTIONS
###############################################################################


@task(target="ferc1.static-tables")
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
    df = DataFrameCollection()

    df.store(
        'ferc_accounts',
        pc.ferc_electric_plant_accounts
        .drop('row_number', axis=1)
        .replace({'ferc_account_description': r'\s+'}, ' ', regex=True)
        .rename(columns={'ferc_account_description': 'description'})
    )

    df.store(
        'ferc_depreciation_lines',
        pc.ferc_accumulated_depreciation
        .drop('row_number', axis=1)
        .rename(columns={'ferc_account_description': 'description'})
    )
    return df


@task(target="ferc1.extract")
def _extract_ferc1(params, pudl_settings):
    return DataFrameCollection(
        **pudl.extract.ferc1.extract(
            ferc1_tables=params['ferc1_tables'],
            ferc1_years=params['ferc1_years'],
            pudl_settings=pudl_settings))


@task(target="ferc1.transform")
def _transform_ferc1(params, dfs):
    return DataFrameCollection(
        **pudl.transform.ferc1.transform(dfs, ferc1_tables=params['ferc1_tables']))


@task(target="extract-table.{table_name}")  # noqa: FS003
def _extract_table(dfc: DataFrameCollection, table_name: str) -> pd.DataFrame:
    return dfc.get(table_name)


class Ferc1Pipeline(DatasetPipeline):
    """Runs ferc1 tasks."""

    DATASET = 'ferc1'

    def __init__(self, *args, overwrite_ferc1_db=SqliteOverwriteMode.ALWAYS, **kwargs):
        """Initializes ferc1 pipeline, optionally creates ferc1 sqlite database."""
        self.overwrite_ferc1_db = overwrite_ferc1_db
        super().__init__(*args, **kwargs)

    @staticmethod  # noqa: C901
    def validate_params(etl_params):
        """Validate and normalize ferc1 parameters."""
        ferc1_dict = {
            'ferc1_years': etl_params.get('ferc1_years', [None]),
            'ferc1_tables': etl_params.get('ferc1_tables', pc.pudl_tables['ferc1']),
            'debug': etl_params.get('debug', False),
        }
        if not ferc1_dict['debug']:
            check_for_bad_tables(try_tables=ferc1_dict['ferc1_tables'], dataset='ferc1')

        if not ferc1_dict['ferc1_years']:
            # TODO(rousik): this really does not make much sense? We should be skipping the pipeline
            # when ferc1_years assumes false value so why do we need to clear the parameters dict
            # here???
            # Perhaps this is due to the [None] hack above that may span all years? Who knows.
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
                    self.etl_settings,
                    self.pudl_settings,
                    overwrite=self.overwrite_ferc1_db)
            raw_dfs = _extract_ferc1(params, self.pudl_settings,
                                     upstream_tasks=self.flow.get_tasks(name='ferc1_to_sqlite'))
            dfs = _transform_ferc1(params, raw_dfs)
            return dfc.merge(_load_static_tables_ferc1(), dfs)


def write_epacems_parquet_files(df: pd.DataFrame, table_name: str, partition: EpaCemsPartition):
    """Writes epacems dataframes to parquet files."""
    schema = epacems_to_parquet.create_cems_schema()
    df = pudl.helpers.convert_cols_dtypes(df, "epacems")
    df = csv.reindex_table(df, table_name)
    # TODO(rousik): this is a dirty hack, year column should simply be part of the
    # dataframe all along, however reindex_table complains about it.
    df["year"] = int(partition.year)
    df.year = df.year.astype(int)

    table = pyarrow.Table.from_pandas(
        df, preserve_index=False, schema=schema).cast(schema)
    # FIXME(rousik): cast(schema) applies schema for the second time.
    # This looks unnecessary but prevents crash when calling write_to_dataset with
    # pyarrow~=2.0.0 that fails on missing metadata.
    # This code has already been fixed in the new pyarrow codebase but this may not have
    # yet been released and we have not updated our dependency versions yet.
    # Here's the offending line in pyarrow that throws an exception:
    # https://github.com/apache/arrow/blob/478286658055bb91737394c2065b92a7e92fb0c1/python/pyarrow/pandas_compat.py#L1184
    #
    # This should be fixed in the newer pyarrow releases and could be removed
    # once we update our dependency.
    output_path = os.path.join(prefect.context.pudl_settings["parquet_dir"], "epacems")
    parquet.write_to_dataset(
        table,
        root_path=output_path,
        partition_cols=['year', 'state'],
        compression='snappy')


@task(target="epacems-{partition}", task_run_name="epacems-{partition}")  # noqa: FS003
def epacems_process_partition(
        partition: EpaCemsPartition,
        plant_utc_offset: pd.DataFrame) -> DataFrameCollection:
    """Runs extract and transform phases for a given epacems partition."""
    logger.info(f'Processing epacems partition {partition}')
    table_name = f'hourly_emissions_epacems_{partition.year}_{partition.state.lower()}'
    df = pudl.extract.epacems.extract_epacems(partition)
    df = pudl.transform.epacems.transform_epacems(df, plant_utc_offset)
    write_epacems_parquet_files(df, table_name, partition)
    return DataFrameCollection()  # return empty DFC because everything is on disk


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
        epacems_dict = {
            "epacems_years": etl_params.get("epacems_years", []),
            "epacems_states": etl_params.get("epacems_states", []),
        }
        if epacems_dict["epacems_states"] and epacems_dict["epacems_states"][0].lower() == "all":
            epacems_dict["epacems_states"] = sorted(pc.cems_states.keys())

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
        with self.flow:
            plants = pudl.transform.epacems.load_plant_utc_offset(
                self.eia_pipeline.get_table('plants_entity_eia'))

            partitions = [
                EpaCemsPartition(year=y, state=s)
                for y, s in itertools.product(params["epacems_years"], params["epacems_states"])]
            logger.info(f'epacems pipeline has {len(partitions)} partitions.')

            epacems_dfc = epacems_process_partition.map(
                partitions,
                plant_utc_offset=unmapped(plants))
            df = dfc.merge_list(epacems_dfc)
            if prefect.context.pudl_upload_to_gcs:
                p = os.path.join(
                    prefect.context.pudl_settings["parquet_dir"], "epacems")
                upload_datapackages_to_gcs(
                    prefect.context.pudl_upload_to_gcs, [p],
                    upstream_tasks=[df])
            log_dfc_tables(df, logprefix='epacems-pipeline-tables')
            return df

##############################################################################
# EPA IPM ETL FUNCTIONS
###############################################################################


@task
def _load_static_tables_epaipm() -> DataFrameCollection:
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
    return DataFrameCollection(
        regions_entity_epaipm=pd.DataFrame(
            pc.epaipm_region_names, columns=['region_id_epaipm']))


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
            tables = params["epaipm_tables"]
            dfc = pudl.extract.epaipm.extract(tables)
            dfc = pudl.transform.epaipm.transform(dfc, tables)
            return dfc.merge(
                _load_static_tables_epaipm(),
                dfc)


###############################################################################
# GLUE EXPORT FUNCTIONS
###############################################################################
class GluePipeline(DatasetPipeline):
    """Runs glue tasks combining eia/ferc1 results."""

    # TODO(rousik): this is a lot of boilerplate for very little use. Perhaps refactor.
    DATASET = 'glue'

    @staticmethod
    def validate_params(etl_params):
        """
        Validates and normalizes glue parameters.

        This effectively creates dict with ferc1 and eia entries that determine
        whether eia and ferc1 components of the glue process should be retrieved.
        """
        # Create dict that indicates whether ferc1, eia records are in the etl_params
        glue_params = {p: bool(etl_params.get(p, False)) for p in ['ferc1', 'eia']}
        if any(glue_params.values()):
            return glue_params
        return {}

    def build(self, params):
        """Add glue tasks to the flow."""
        with self.flow:
            # This expects two named attributes ferc1, eia that have bool values
            return pudl.glue.ferc1_eia.glue(**params)

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


@task
def log_dfc_tables(dfc: DataFrameCollection, logprefix: str = 'log_dfc_tables'):
    """Debug log tables contained in dfc."""
    tnames = dfc.get_table_names()
    logger.info(f'{logprefix}: {len(tnames)} tables: {tnames}')


def etl(datapkg_settings, pudl_settings, flow=None, bundle_name=None,
        datapkg_builder=None, etl_settings=None, clobber=False,
        overwrite_ferc1_db=SqliteOverwriteMode.ALWAYS):
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
        etl_settings (dict): the complete configuration for the ETL run.
        clobber (bool): if True then existing results will be overwritten.
        overwrite_ferc1_db (SqliteOverwriteMode): controls how ferc1 db should
            be treated.

    Returns:
        Prefect result for the DatapackageBuilder final task (that contains path to where
        the datapackage is stored)
    """
    datapkg_dir = datapkg_builder.get_datapkg_output_dir(datapkg_settings)
    pipelines = {}
    dataset_list = datapkg_settings['datasets']  # list of {dataset: params_dict}
    # For debugging purposes, print the dataset names
    dataset_names = set()
    for ds in dataset_list:
        dataset_names.update(ds.keys())
    logger.warning(
        f'Running etl with the following configurations: {sorted(dataset_names)}')

    datapkg_name = datapkg_builder.get_datapkg_name(datapkg_settings)
    extra_params = {
        'ferc1': {'overwrite_ferc1_db': overwrite_ferc1_db},
    }
    # TODO(rousik): we need to have a good way of configuring the datastore caching options
    # from commandline arguments here. Perhaps passing cmdline args by Datastore constructor
    # may do the trick?

    for pl_class in [Ferc1Pipeline, EiaPipeline, EpaIpmPipeline, GluePipeline]:
        pipelines[pl_class.DATASET] = pl_class(
            pudl_settings, dataset_list, flow,
            datapkg_name=datapkg_builder.get_datapkg_name(datapkg_settings),
            etl_settings=etl_settings,
            datapkg_dir=datapkg_dir,
            **extra_params.get(pl_class.DATASET, {}))
    # EpaCems pipeline is special because it needs to read the output of eia
    # pipeline

    pipelines['epacems'] = EpaCemsPipeline(
        pudl_settings,
        dataset_list,
        flow,
        datapkg_dir=datapkg_dir,
        eia_pipeline=pipelines['eia'])
    with flow:
        outputs = []
        for dataset, pl in pipelines.items():
            if pl.is_executed():
                logger.info(f"{datapkg_name} contains dataset {dataset}")
                outputs.append(pl.outputs())

        tables = dfc.merge_list(outputs)
        # Note that epacems is not generating any tables. It will directly write its
        # outputs to parquet files and upload them to gcs if necessary.
        resource_descriptors = pudl.load.metadata.write_csv_and_build_resource_descriptor.map(
            dfc.fanout(tables),
            datapkg_dir=unmapped(datapkg_dir),
            datapkg_settings=unmapped(datapkg_settings))

        return datapkg_builder(
            tables,
            prefect.flatten(resource_descriptors),
            datapkg_settings,
            task_args={'task_run_name': f'DatapackageBuilder-{datapkg_name}'})


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
            gcs_bucket (str): if specified, upload the archived datapackagers to this
              Google Cloud Storage bucket.
        """
        self.uuid = str(uuid.uuid4())
        self.timestamp = datetime.now().strftime('%F-%H%M%S')
        self.doi = doi
        self.pudl_settings = pudl_settings
        self.bundle_dir = Path(pudl_settings["datapkg_dir"], bundle_name)
        self.bundle_name = bundle_name
        logger.warning(f'DatapackageBuilder uuid is {self.uuid}')
        logger.info(
            f'Datapackage bundle_name {bundle_name} with settings: {pudl_settings}')

        super().__init__(*args, **kwargs)

    def noslash_doi(self):
        """Returns doi where slashes are replaced with."""
        return self.doi.replace("/", "-")

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

    def run(
            self,
            tables: DataFrameCollection,
            resource_descriptors: List[Dict],
            datapkg_settings: Dict) -> str:
        """
        Generates datapackage.json and validates contents of associated resources.

        Returns:
          top-level path that contains datapackage.json and resource files under data/
        """
        datapkg_full_name = self.get_datapkg_name(datapkg_settings)
        logger.info(
            f"Building metadata for {datapkg_full_name}, tables: {tables.get_table_names()}")
        pudl.load.metadata.generate_metadata(
            datapkg_settings,
            tables,
            resource_descriptors,
            self.get_datapkg_output_dir(datapkg_settings),
            datapkg_bundle_uuid=self.uuid,
            datapkg_bundle_doi=self.doi)
        return self.get_datapkg_output_dir(datapkg_settings)


@task(checkpoint=False)
def upload_datapackages_to_gcs(gcs_root_path: str, local_directories: List[str]) -> None:
    """
    Upload datapackage files to GCS.

    local_directories will be recursively scanned for files and these will be uploaded
    to GCS under gcs_root_path/${pudl_run_id}. File structure will be retained using
    paths relative to pudl_settings["pudl_out"].

    Args:
        gcs_root_path (str): a path prefix in the form of gs://bucket/path_prefix
        local_directories (List[str]): list of directories that contain datapackage.json
            files and the associated resources.
    """
    gcs_base_path = os.path.join(gcs_root_path, prefect.context.pudl_run_id)
    local_base_path = prefect.context.pudl_settings["pudl_out"]

    def upload_file(local_file: Path):
        rel_path = local_file.relative_to(local_base_path)
        target_path = os.path.join(gcs_base_path, rel_path)
        with local_file.open(mode="rb") as in_file:
            with fsspec.open(target_path, mode="wb") as out_file:
                out_file.write(in_file.read())

    for d in local_directories:
        pd = Path(d)
        if pd.is_file():
            upload_file(Path(d))
        elif pd.is_dir():
            for local_file in pd.rglob("*"):
                if not local_file.is_file():
                    continue
                upload_file(local_file)
        else:
            logger.warning(f'{pd} is neither file nor directory.')


def log_task_failures(flow_state: prefect.engine.state.State) -> None:
    """Log messages for directly failed tasks."""
    if not flow_state.is_failed():
        return
    for task_instance, task_state in flow_state.result.items():
        if not isinstance(task_state, prefect.engine.state.Failed):
            continue
        if isinstance(task_state, prefect.engine.state.TriggerFailed):
            continue
        logger.error(f'ETL task {task_instance.name} failed: {task_state.message}')


def cleanup_pipeline_cache(state, commandline_args):
    """
    Runs the pipeline cache cleanup logic, possibly removing the local cache.

    Currently, the cache is destroyed if caching is enabled, if it is done
    locally (not on GCS) and if the flow completed succesfully.
    """
    # TODO(rousik): add --keep-cache=ALWAYS|NEVER|ONFAIL commandline flag to control this
    if commandline_args.keep_cache:
        logger.warning('--keep-cache prevents cleanup of local cache.')
        return
    if state.is_successful() and commandline_args.pipeline_cache_path:
        cache_root = commandline_args.pipeline_cache_path
        if not cache_root.startswith("gs://"):
            logger.warning(f'Deleting pipeline cache directory under {cache_root}')
            # TODO(rousik): in order to prevent catastrophic results due to misconfiguration
            # we should refuse to delete cache_root unless the directory has the expected
            # run_id form of YYYY-MM-DD-HHMM-uuid4
            shutil.rmtree(cache_root)


def configure_prefect_context(etl_settings, pudl_settings, commandline_args):
    """
    Sets all pudl ETL relevant variables within prefect context.

    The variables that are set and their meaning:
      * pudl_etl_settings: the settings.yml file that configures the operation of the
        pipeline.
    """
    prefect.context.pudl_etl_settings = etl_settings
    prefect.context.pudl_datapkg_bundle_settings = etl_settings['datapkg_bundle_settings']
    prefect.context.pudl_settings = pudl_settings
    prefect.context.pudl_datapkg_bundle_name = etl_settings['datapkg_bundle_name']
    prefect.context.pudl_commandline_args = commandline_args
    prefect.context.pudl_upload_to_gcs = commandline_args.upload_to_gcs

    local_cache_path = None
    if not commandline_args.bypass_local_cache:
        local_cache_path = Path(pudl_settings["pudl_in"]) / "data"

    # Allow for construction of datastore by setting the params to context
    # This will allow us to construct datastore when needed
    # by calling Datastore.from_prefect_context()
    prefect.context.datastore_config = dict(
        sandbox=pudl_settings.get("sandbox", False),
        local_cache_path=local_cache_path,
        gcs_cache_path=commandline_args.gcs_cache_path,
        gcs_cache_readonly=True)

    pipeline_cache_path = commandline_args.pipeline_cache_path
    if not pipeline_cache_path:
        pipeline_cache_path = os.path.join(pudl_settings["pudl_out"], "cache")
    prefect.context.pudl_pipeline_cache_path = pipeline_cache_path
    prefect.context.data_frame_storage_path = os.path.join(
        pipeline_cache_path, "dataframes")


def generate_datapkg_bundle(etl_settings: dict,
                            pudl_settings: dict,
                            datapkg_bundle_doi: str = None,
                            commandline_args: argparse.Namespace = None):
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
        commandline_args (argparse.Namespace): provides parsed commandline
            flags that control the operation of the ETL pipeline.

    Returns:
        dict: A dictionary with datapackage names as the keys, and Python
        dictionaries representing tabular datapackage resource descriptors as
        the values, one per datapackage that was generated as part of the
        bundle.

    """
    # TODO(rousik): args.clobber should be moved to prefect.context, also other
    # configuration parameters such as overwrite-ferc1-db.
    datapkg_bundle_name = etl_settings['datapkg_bundle_name']
    datapkg_bundle_settings = etl_settings['datapkg_bundle_settings']
    configure_prefect_context(etl_settings, pudl_settings, commandline_args)

    flow_kwargs = {}
    # TODO(rousik): simplify this by allowing --pipeline-cache-path to be either local
    # or point to gcs storage. Pick GCSResult or LocalResult accordingly.
    # TODO(rousik): simplify this by allowing --pipeline-cache-path to be either local
    # or point to gcs storage. Pick GCSResult or LocalResult accordingly.
    if commandline_args.gcs_bucket_for_prefect_cache:
        flow_kwargs["result"] = GCSResult(
            bucket=commandline_args.gcs_bucket_for_prefect_cache)
    else:
        result_cache = os.path.join(prefect.context.pudl_pipeline_cache_path)
        if not result_cache.startswith("gs://"):
            Path(result_cache).mkdir(exist_ok=True, parents=True)
        flow_kwargs["result"] = LocalResult(dir=result_cache)

    flow = prefect.Flow("PUDL ETL", **flow_kwargs)

    # TODO(rousik): DatapackageBuilder.uuid should be restored upon --rerun $uuid,
    # perhaps by deriving the uuid from the top uuid in a deterministic fashion (e.g.
    # combining top-level uuid with the datapackage name)
    datapkg_builder = DatapackageBuilder(
        datapkg_bundle_name, pudl_settings, doi=datapkg_bundle_doi)
    # Create, or delete and re-create the top level datapackage bundle directory:
    datapkg_builder.prepare_output_directories(clobber=commandline_args.clobber)

    # validate the settings from the settings file.
    validated_bundle_settings = validate_params(
        datapkg_bundle_settings, pudl_settings)

    datapkg_paths = []
    # this is a list and should be processed by another task
    for datapkg_settings in validated_bundle_settings:
        datapkg_builder.make_datapkg_dir(datapkg_settings)
        result = etl(datapkg_settings,
                     pudl_settings,
                     flow=flow,
                     bundle_name=datapkg_bundle_name,
                     datapkg_builder=datapkg_builder,
                     etl_settings=etl_settings,
                     clobber=commandline_args.clobber,
                     overwrite_ferc1_db=commandline_args.overwrite_ferc1_db)
        # TODO(rousik): we should simply pass commandline_args to etl function
        datapkg_paths.append(result)

    validation_task = ShellTask(stream_output=True)
    # TODO(rousik): perhaps we could fail early if tox.ini is not found in the current
    # directory.
    with flow:
        pudl_db = datapkg_to_sqlite.populate_pudl_database(
            datapkg_paths, clobber=commandline_args.clobber)

        # TODO(rousik): we could also upload pudl db to gcs as an artifact
        if commandline_args.validate:
            validation_task(command="tox -ve validate", upstream_tasks=[pudl_db])
        if commandline_args.upload_to_gcs:
            upload_datapackages_to_gcs(
                commandline_args.upload_to_gcs,
                datapkg_paths)
            upload_datapackages_to_gcs(
                commandline_args.upload_to_gcs,
                [Path(pudl_settings["sqlite_dir"], "pudl.sqlite")],
                task_args={'name': 'upload_pudl_db_to_gcs'},
                upstream_tasks=[pudl_db])

    if commandline_args.show_flow_graph:
        flow.visualize()

    prefect_executor = LocalExecutor()
    if commandline_args.dask_executor_address or commandline_args.use_dask_executor:
        prefect_executor = DaskExecutor(address=commandline_args.dask_executor_address)
    state = flow.run(executor=prefect_executor)

    log_task_failures(state)
    cleanup_pipeline_cache(state, commandline_args)

    # TODO(rousik): summarize flow errors (directly failed tasks and their execeptions)
    if commandline_args.show_flow_graph:
        flow.visualize(flow_state=state)

    # TODO(rousik): if the flow failed, summarize the failed tasks and throw an exception here.
    # It is unclear whether we want to generate partial results or wipe them.
    return {}
