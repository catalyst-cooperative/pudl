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
import logging
import os
import shutil
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import fsspec
import prefect
from prefect import task, unmapped
from prefect.executors import DaskExecutor
from prefect.executors.local import LocalExecutor
from prefect.tasks.shell import ShellTask

import pudl
from pudl import dfc
from pudl.convert import datapkg_to_sqlite
from pudl.dfc import DataFrameCollection
from pudl.extract.ferc1 import SqliteOverwriteMode
from pudl.fsspec_result import FSSpecResult
from pudl.workflow.dataset_pipeline import DatasetPipeline
from pudl.workflow.eia import EiaPipeline
from pudl.workflow.epacems import EpaCemsPipeline
from pudl.workflow.epaipm import EpaIpmPipeline
from pudl.workflow.ferc1 import Ferc1Pipeline
from pudl.workflow.glue import GluePipeline

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
        "--validate",
        default=False,
        action="store_true",
        help="""If enabled, run validation via tox from the current directory.""")
    return parser


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
def upload_files(remote_root_path: str, local_directories: List[str]) -> None:
    """
    Upload local files and directories to remote_root_path.

    Local directories will be recursively scanned for files and these will be uploaded
    to the remote storage under {remote_root_path}/{run_id}. The local directory
    structure relative to pudl_out will be maintained on the remote end, e.g.
    pudl_out/some/path/file.txt will be uploaded to
    remote_root_path/run_id/some/path/file.txt.

    Args:
        remote_root_path (str): a path prefix that can be interpreted by fsspec.
        local_directories (List[str]): list of files or directories that should
            be uploaded.
    """
    remote_base_path = os.path.join(remote_root_path, prefect.context.pudl_run_id)
    local_base_path = prefect.context.pudl_settings["pudl_out"]

    def upload_file(local_file: Path):
        rel_path = local_file.relative_to(local_base_path)
        target_path = os.path.join(remote_base_path, rel_path)
        with local_file.open(mode="rb") as in_file:
            with fsspec.open(target_path, mode="wb") as out_file:
                out_file.write(in_file.read())

    for d in local_directories:
        pathd = Path(d)
        if pathd.is_file():
            upload_file(Path(d))
        elif pathd.is_dir():
            for local_file in pathd.rglob("*"):
                if not local_file.is_file():
                    continue
                upload_file(local_file)
        else:
            logger.warning(f'{pathd} is neither file nor directory.')


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
    prefect.context.pudl_upload_to = commandline_args.upload_to
    pudl.workspace.datastore.Datastore.configure_prefect_context(commandline_args)

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

    result_cache = os.path.join(prefect.context.pudl_pipeline_cache_path, "prefect")
    flow = prefect.Flow("PUDL ETL", result=FSSpecResult(root_dir=result_cache))

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
        if commandline_args.upload_to:
            upload_files(commandline_args.upload_to, datapkg_paths)
            upload_files(
                commandline_args.upload_to,
                [os.path.join(pudl_settings["sqlite_dir"], "pudl.sqlite")],
                task_args={'name': 'upload_pudl_db'},
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
