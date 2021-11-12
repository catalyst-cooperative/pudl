"""
Run the PUDL ETL Pipeline.

The PUDL project integrates several different public datasets into a well
normalized relational database allowing easier access and interaction between all
datasets. This module coordinates the extract/transfrom/load process for
data from:

 - US Energy Information Agency (EIA):
   - Form 860 (eia860)
   - Form 923 (eia923)
 - US Federal Energy Regulatory Commission (FERC):
   - Form 1 (ferc1)
 - US Environmental Protection Agency (EPA):
   - Continuous Emissions Monitory System (epacems)

"""
import argparse
import logging
import os
import shutil
import time
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import prefect
import sqlalchemy as sa
from prefect.executors import DaskExecutor
from prefect.executors.dask import LocalDaskExecutor
from prefect.executors.local import LocalExecutor

import pudl
from pudl import constants as pc
from pudl import dfc
from pudl.extract.ferc1 import SqliteOverwriteMode
from pudl.fsspec_result import FSSpecResult
from pudl.metadata import RESOURCE_METADATA
from pudl.metadata.codes import (CONTRACT_TYPES_EIA, ENERGY_SOURCES_EIA,
                                 FUEL_TRANSPORTATION_MODES_EIA,
                                 FUEL_TYPES_AER_EIA, PRIME_MOVERS_EIA,
                                 SECTOR_CONSOLIDATED_EIA)
from pudl.metadata.dfs import FERC_ACCOUNTS, FERC_DEPRECIATION_LINES
from pudl.settings import (EiaSettings, EpaCemsSettings, EtlSettings,
                           Ferc1Settings, GlueSettings)
from pudl.workflow.eia import EiaPipeline
from pudl.workflow.epacems import EpaCemsPipeline
from pudl.workflow.ferc1 import Ferc1Pipeline
from pudl.workflow.glue import GluePipeline
from pudl.workspace.datastore import Datastore

logger = logging.getLogger(__name__)

PUDL_META = pudl.metadata.classes.Package.from_resource_ids(RESOURCE_METADATA)


def command_line_flags() -> argparse.ArgumentParser:
    """Returns argparse.ArgumentParser containing flags relevant to the ETL component."""
    parser = argparse.ArgumentParser(
        description="ETL configuration flags", add_help=False)
    parser.add_argument(
        "--use-local-dask-executor",
        action="store_true",
        default=False,
        help='If enabled, use LocalDaskExecutor to run the flow.')
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
        "--upload-to",
        type=str,
        default=os.environ.get('PUDL_UPLOAD_TO'),
        help="""A location (local or remote) where the results of the ETL run
        should be uploaded to. This path will be interpreted by fsspec so
        anything supported by that module is a valid destination.
        This should work with GCS and S3 remote destinations.
        Default value for this will be loaded from PUDL_UPLOAD_TO environment
        variable.
        Files will be stored under {upload_to}/{run_id} to avoid conflicts.
        """)
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
        "--zenodo-cache-path",
        type=str,
        default=os.environ.get('PUDL_ZENODO_CACHE_PATH'),
        help="""Specifies fsspec-like path where zenodo datapackages are cached.
        This can be local as well as remote location (e.g. GCS or S3).
        If specified, datastore will use this as a read-only cache and will retrieve
        files from this location before contacting Zenodo.
        This is set to read-only mode and will not be modified during ETL run. If you
        need to update or set it up, you can use pudl_datastore CLI to do so.
        Default value for this flag is loaded from PUDL_ZENODO_CACHE_PATH environment
        variable.""")
    # TODO(rousik): the above should be marked as "datastore" cache.
    parser.add_argument(
        "--bypass-local-cache",
        action="store_true",
        default=False,
        help="If enabled, the local file cache for datastore will not be used.")
    # TODO(rousik): the above should also be marked as "datastore" cache
    parser.add_argument(
        "--pipeline-cache-path",
        type=str,
        default=os.environ.get('PUDL_PIPELINE_CACHE_PATH'),
        help="""Controls where the pipeline should be storing its cache. This should be
        used for both the prefect task results as well as for the DataFrameCollections.""")
    parser.add_argument(
        "--keep-cache",
        action="store_true",
        help="""Do not remove local pipeline cache even if the pipeline succeeds. This can
        be used for development/debugging purposes.
        """)
    parser.add_argument(
        "--gcs-requester-pays",
        type=str,
        help="If specified, use this project name to charge the GCS operations to.")

    return parser


###############################################################################
# EIA EXPORT FUNCTIONS
###############################################################################

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
        'energy_sources_eia': ENERGY_SOURCES_EIA["df"],
        'fuel_types_aer_eia': FUEL_TYPES_AER_EIA["df"],
        'prime_movers_eia': PRIME_MOVERS_EIA["df"],
        'sector_consolidated_eia': SECTOR_CONSOLIDATED_EIA["df"],
        'fuel_transportation_modes_eia': FUEL_TRANSPORTATION_MODES_EIA["df"],
        'contract_types_eia': CONTRACT_TYPES_EIA["df"]
    }


def _etl_eia(
    etl_settings: EiaSettings,
    ds_kwargs: Dict[str, Any]
) -> Dict[str, pd.DataFrame]:
    """Extract, transform and load CSVs for the EIA datasets.

    Args:
        etl_settings: Validated ETL parameters required by this data source.
        ds_kwargs: Keyword arguments for instantiating a PUDL datastore,
            so that the ETL can access the raw input data.

    Returns:
        A dictionary of EIA dataframes ready for loading into the PUDL DB.

    """
    eia860_tables = etl_settings.eia860.tables
    eia860_years = etl_settings.eia860.years
    eia860m = etl_settings.eia860.eia860m
    eia923_tables = etl_settings.eia923.tables
    eia923_years = etl_settings.eia923.years

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
    if eia860m:
        eia860m_raw_dfs = pudl.extract.eia860m.Extractor(ds).extract(
            year_month=pc.WORKING_PARTITIONS['eia860m']['year_month'])
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
        eia860m=eia860m,
    )
    # convert types..
    entities_dfs = pudl.helpers.convert_dfs_dict_dtypes(entities_dfs, 'eia')
    for table in entities_dfs:
        entities_dfs[table] = PUDL_META.get_resource(table).encode(entities_dfs[table])

    out_dfs.update(entities_dfs)
    out_dfs.update(eia_transformed_dfs)
    return out_dfs


###############################################################################
# FERC1 EXPORT FUNCTIONS
###############################################################################


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
        'ferc_accounts': FERC_ACCOUNTS[[
            "ferc_account_id",
            "ferc_account_description",
        ]],
        'ferc_depreciation_lines': FERC_DEPRECIATION_LINES[[
            "line_id",
            "ferc_account_description",
        ]],
    }


def _etl_ferc1(
    etl_settings: Ferc1Settings,
    pudl_settings: Dict[str, Any],
) -> Dict[str, pd.DataFrame]:
    """Extract, transform and load CSVs for FERC Form 1.

    Args:
        etl_settings: Validated ETL parameters required by this data source.
        pudl_settings: a dictionary filled with settings that mostly
            describe paths to various resources and outputs.

    Returns:
        Dataframes containing PUDL database tables pertaining to the FERC Form 1
        data, keyed by table name.

    """
    ferc1_years = etl_settings.years
    ferc1_tables = etl_settings.tables

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


def etl_epacems(
    etl_settings: EpaCemsSettings,
    pudl_settings: Dict[str, Any],
    ds_kwargs: Dict[str, Any],
) -> None:
    """Extract, transform and load CSVs for EPA CEMS.

    Args:
        etl_settings: Validated ETL parameters required by this data source.
        pudl_settings: a dictionary filled with settings that mostly describe paths to
            various resources and outputs.
        ds_kwargs: Keyword arguments for instantiating a PUDL datastore, so that the ETL
            can access the raw input data.

    Returns:
        Unlike the other ETL functions, the EPACEMS writes its output to Parquet as it
        goes, since the dataset is too large to hold in memory.  So it doesn't return a
        dictionary of dataframes.

    """
    epacems_years = etl_settings.years
    epacems_states = etl_settings.states

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

def _etl_glue(etl_settings: GlueSettings) -> Dict[str, pd.DataFrame]:
    """Extract, transform and load CSVs for the Glue tables.

    Args:
        etl_settings (GlueSettings): Validated ETL parameters required by this data source.

    Returns:
        dict: A dictionary of :class:`pandas.Dataframe` whose keys are the names
        of the corresponding database table.

    """
    # grab the glue tables for ferc1 & eia
    glue_dfs = pudl.glue.ferc1_eia.glue(
        ferc1=etl_settings.ferc1,
        eia=etl_settings.eia,
    )

    # Add the EPA to EIA crosswalk, but only if the eia data is being processed.
    # Otherwise the foreign key references will have nothing to point at:
    if etl_settings.eia:
        glue_dfs.update(pudl.glue.eia_epacems.grab_clean_split())

    return glue_dfs


###############################################################################
# Coordinating functions
###############################################################################

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
    """Runs the pipeline cache cleanup logic, possibly removing the local cache.

    Currently, the cache is destroyed if caching is enabled, if it is done
    locally (not on GCS) and if the flow completed succesfully.
    """
    # TODO(rousik): add --keep-cache=ALWAYS|NEVER|ONFAIL commandline flag to control this
    # TODO(bendnorman): When is this cache actually used? rerun_id?
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
    """Sets all pudl ETL relevant variables within prefect context.

    The variables that are set and their meaning:
      * etl_settings: the settings.yml file that configures the operation of the
        pipeline.
    """
    prefect.context.etl_settings = etl_settings
    prefect.context.pudl_settings = pudl_settings
    prefect.context.pudl_commandline_args = commandline_args
    prefect.context.pudl_upload_to = commandline_args.upload_to
    pudl.workspace.datastore.Datastore.configure_prefect_context(commandline_args)

    pipeline_cache_path = commandline_args.pipeline_cache_path
    if not pipeline_cache_path:
        pipeline_cache_path = os.path.join(pudl_settings["pudl_out"], "cache")
    prefect.context.pudl_pipeline_cache_path = pipeline_cache_path
    prefect.context.data_frame_storage_path = os.path.join(
        pipeline_cache_path, "dataframes")


def etl(  # noqa: C901
    etl_settings: EtlSettings,
    pudl_settings: Dict,
    commandline_args: argparse.Namespace = None
):
    """
    Run the PUDL Extract, Transform, and Load data pipeline.

    First we validate the settings, and then process data destined for loading
    into SQLite, which includes The FERC Form 1 and the EIA Forms 860 and 923.
    Once those data have been output to SQLite we mvoe on to processing the
    long tables, which will be loaded into Apache Parquet files. Some of this
    processing depends on data that's already been loaded into the SQLite DB.

    Args:
        etl_settings: settings that describe datasets to be loaded.
        pudl_settings: a dictionary filled with settings that mostly
            describe paths to various resources and outputs.

    Returns:
        None

    """
    pudl_db_path = Path(pudl_settings["sqlite_dir"]) / "pudl.sqlite"
    if pudl_db_path.exists() and not commandline_args.clobber:
        raise SystemExit(
            "The PUDL DB already exists, and we don't want to clobber it.\n"
            f"Move {pudl_db_path} aside or set clobber=True and try again."
        )

    # TODO(bendnorman): what are we using ds_kwargs for? epacems?
    # Configure how we want to obtain raw input data:
    ds_kwargs = dict(
        gcs_cache_path=commandline_args.gcs_cache_path,
        sandbox=pudl_settings.get("sandbox", False)
    )
    if not commandline_args.bypass_local_cache:
        ds_kwargs["local_cache_path"] = Path(pudl_settings["pudl_in"]) / "data"

    # TODO (bendnorman): The naming convention here is getting a little wacky.
    validated_etl_settings = etl_settings.datasets

    # Check for existing EPA CEMS outputs if we're going to process CEMS, and
    # do it before running the SQLite part of the ETL so we don't do a bunch of
    # work only to discover that we can't finish.
    datasets = validated_etl_settings.get_datasets()
    if validated_etl_settings.epacems:
        epacems_pq_path = Path(pudl_settings["parquet_dir"]) / "epacems"
        _ = pudl.helpers.prep_dir(epacems_pq_path, clobber=commandline_args.clobber)

    # Setup pipeline cache
    # TODO(bendnorman): what do we want to live in here? How does it affect testing?
    configure_prefect_context(etl_settings, pudl_settings, commandline_args)

    # TODO(bendnorman): How does this differ from pudl_pipeline_cache_path?
    result_cache = os.path.join(prefect.context.pudl_pipeline_cache_path, "prefect")
    flow = prefect.Flow("PUDL ETL", result=FSSpecResult(root_dir=result_cache))

    logger.warning(
        f'Running etl with the following configurations: {sorted(datasets.keys())}')
    prefect.context.dataset_names = datasets.keys()

    # TODO(rousik): we need to have a good way of configuring the datastore caching options
    # from commandline arguments here. Perhaps passing cmdline args by Datastore constructor
    # may do the trick?

    pipelines = {}

    # TODO(bendnorman): There's got to be a better way here.
    if validated_etl_settings.ferc1:
        pipelines[Ferc1Pipeline.DATASET] = Ferc1Pipeline(
            flow, pudl_settings, validated_etl_settings.ferc1)
    if validated_etl_settings.eia:
        pipelines[EiaPipeline.DATASET] = EiaPipeline(
            flow, pudl_settings, validated_etl_settings.eia, etl_settings.name)
    if validated_etl_settings.glue:
        pipelines[GluePipeline.DATASET] = GluePipeline(
            flow, pudl_settings, validated_etl_settings.glue)

    if pipelines:
        with flow:
            outputs = []
            for dataset, pl in pipelines.items():
                # TODO(bendnorman) is_excuted() working properly?:
                if pl.is_executed():
                    outputs.append(pl.outputs())

            # `tables` is a DataFrame collections containing every dataframe from the pipelines.
            tables = dfc.merge_list(outputs)

            # # Load the ferc1 + eia data directly into the SQLite DB:
            pudl_engine = sa.create_engine(pudl_settings["pudl_db"])
            pudl.load.sqlite.dfs_to_sqlite(
                tables,
                engine=pudl_engine,
                check_foreign_keys=not commandline_args.ignore_foreign_key_constraints,
                check_types=not commandline_args.ignore_type_constraints,
                check_values=not commandline_args.ignore_value_constraints,
            )

    # Add CEMS pipeline to the flow
    # TODO(bendnorman): can this live inside the flow statement above? ^^
    if validated_etl_settings.epacems:
        _ = EpaCemsPipeline(
            flow,
            pudl_settings,
            validated_etl_settings.epacems)

    if commandline_args.show_flow_graph:
        flow.visualize()

    # Set the prefect executor.
    prefect_executor = LocalExecutor()
    if commandline_args.use_local_dask_executor:
        prefect_executor = LocalDaskExecutor()
    elif commandline_args.dask_executor_address or commandline_args.use_dask_executor:
        prefect_executor = DaskExecutor(address=commandline_args.dask_executor_address)
    logger.info(f"Using {type(prefect_executor)} Prefect executor.")
    flow.executor = prefect_executor

    state = flow.run()

    log_task_failures(state)
    cleanup_pipeline_cache(state, commandline_args)

    # TODO(rousik): summarize flow errors (directly failed tasks and their execeptions)
    if commandline_args.show_flow_graph:
        flow.visualize(flow_state=state)

    # TODO(rousik): if the flow failed, summarize the failed tasks and throw an exception here.
    # It is unclear whether we want to generate partial results or wipe them.
    return {}
