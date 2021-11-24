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
from pathlib import Path
from typing import Dict

import prefect
import sqlalchemy as sa
from prefect.executors import DaskExecutor
from prefect.executors.dask import LocalDaskExecutor
from prefect.executors.local import LocalExecutor

import pudl
from pudl import dfc
from pudl.fsspec_result import FSSpecResult
from pudl.metadata import RESOURCE_METADATA
from pudl.settings import EtlSettings
from pudl.workflow.dataset_pipeline import DatasetPipeline
from pudl.workflow.epacems import EpaCemsPipeline

logger = logging.getLogger(__name__)

PUDL_META = pudl.metadata.classes.Package.from_resource_ids(RESOURCE_METADATA)


def command_line_flags() -> argparse.ArgumentParser:
    """Returns argparse.ArgumentParser containing flags relevant to the ETL component."""
    parser = argparse.ArgumentParser(
        description="ETL configuration flags", add_help=False)
    parser.add_argument(
        "--executor",
        choices=["LOCAL", "LOCAL-DASK", "DASK"],
        default="LOCAL",
        help="Which Prefect executor to run the ETL on."
    ),
    parser.add_argument(
        "--dask-executor-address",
        default=None,
        help='If specified, use pre-existing DaskExecutor at this address.')
    # TODO(bendnorman): Should upload-to be supported right now?
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
        action="store_true",
        default=False,
        help="Control whether to rerun the ferc1 database.")
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
        choices=["ALWAYS", "NEVER", "ONFAIL"],
        default="ONFAIL",
        help="""Do not remove local pipeline cache even if the pipeline succeeds. This can
        be used for development/debugging purposes. Defaults to ONFAIL.
        """)
    parser.add_argument(
        "--gcs-requester-pays",
        type=str,
        help="If specified, use this project name to charge the GCS operations to.")

    return parser


###############################################################################
# Coordinating functions
###############################################################################

def set_downstream_checkpoints(flow, task):
    """Set downstream checkpoints to false if task checkpoint is false."""
    downstream_tasks = flow.downstream_tasks(task)
    if not downstream_tasks:
        return
    if task.checkpoint is False:
        for d_task in downstream_tasks:
            d_task.checkpoint = False

    for d_task in downstream_tasks:
        set_downstream_checkpoints(flow, d_task)


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
    onfail_success = (commandline_args.keep_cache == 'ONFAIL' and state.is_successful())
    cache_root = commandline_args.pipeline_cache_path

    if (commandline_args.keep_cache == 'ALWAYS') or not onfail_success or commandline_args.rerun:
        logger.warning(f'Keep pipeline cache director under {cache_root}')
        return
    if (commandline_args.keep_cache == 'NEVER') or onfail_success:
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
    prefect.context.pudl_upload_to = commandline_args.upload_to
    pudl.workspace.datastore.Datastore.configure_prefect_context(commandline_args)

    prefect.context.overwrite_ferc1_db = commandline_args.overwrite_ferc1_db

    prefect.context.datasets = etl_settings.datasets.get_datasets()

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

    # Setup pipeline cache
    configure_prefect_context(etl_settings, pudl_settings, commandline_args)

    # Check for existing EPA CEMS outputs if we're going to process CEMS, and
    # do it before running the SQLite part of the ETL so we don't do a bunch of
    # work only to discover that we can't finish.
    datasets = prefect.context.get("datasets")
    if datasets.get("epacems"):
        epacems_pq_path = Path(pudl_settings["parquet_dir"]) / "epacems"
        _ = pudl.helpers.prep_dir(epacems_pq_path, clobber=commandline_args.clobber)

    result_cache = os.path.join(prefect.context.get(
        "pudl_pipeline_cache_path"), "prefect")
    flow = prefect.Flow("PUDL ETL", result=FSSpecResult(root_dir=result_cache))

    logger.warning(
        f'Running etl with the following configurations: {sorted(datasets.keys())}')

    # TODO(rousik): we need to have a good way of configuring the datastore caching options
    # from commandline arguments here. Perhaps passing cmdline args by Datastore constructor
    # may do the trick?

    pipelines = {}

    for dataset, settings in datasets.items():
        # Add cems to flow after eia is added.
        if settings and dataset != "epacems":
            pipeline = DatasetPipeline.get_pipeline_for_dataset(dataset)
            pipelines[dataset] = pipeline(flow, settings)

    with flow:
        outputs = []
        for dataset, pl in pipelines.items():
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
    if datasets.get("epacems"):
        _ = EpaCemsPipeline(
            flow,
            datasets.get("epacems"))

    if commandline_args.show_flow_graph:
        flow.visualize()

    # Set the prefect executor.
    if commandline_args.executor == "LOCAL":
        prefect_executor = LocalExecutor()
    if commandline_args.executor == "LOCAL-DASK":
        prefect_executor = LocalDaskExecutor()
    if commandline_args.executor == "DASK" or commandline_args.dask_executor_address:
        prefect_executor = DaskExecutor(address=commandline_args.dask_executor_address)

    logger.info(f"Using {commandline_args.executor.title()} Prefect executor.")
    flow.executor = prefect_executor

    # Turn off downstream checkpoints if keeping cached version
    if commandline_args.keep_cache == "ALWAYS":
        root_tasks = flow.root_tasks()
        for root_task in root_tasks:
            set_downstream_checkpoints(flow, root_task)

    state = flow.run()

    log_task_failures(state)
    cleanup_pipeline_cache(state, commandline_args)

    if commandline_args.show_flow_graph:
        flow.visualize(flow_state=state)

    # TODO(rousik): if the flow failed, summarize the failed tasks and throw an exception here.
    # It is unclear whether we want to generate partial results or wipe them.
    return {}
