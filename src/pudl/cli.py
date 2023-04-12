"""A command line interface (CLI) to the main PUDL ETL functionality.

This script cordinates the PUDL ETL process, based on parameters provided via a YAML
settings file.

If the settings for a dataset has empty parameters (meaning there are no years or tables
included), no outputs will be generated. See :doc:`/dev/run_the_etl` for details.

The output SQLite and Parquet files will be stored in ``PUDL_OUTPUT``.  To
setup your default ``PUDL_INPUT`` and ``PUDL_OUTPUT`` directories see
``pudl_setup --help``.
"""

import argparse
import sys
from collections.abc import Callable

from dagster import (
    DagsterInstance,
    Definitions,
    JobDefinition,
    build_reconstructable_job,
    define_asset_job,
    execute_job,
)

import pudl
from pudl.settings import EtlSettings

logger = pudl.logging_helpers.get_logger(__name__)


def parse_command_line(argv):
    """Parse script command line arguments. See the -h option.

    Args:
        argv (list): command line arguments including caller file name.

    Returns:
        dict: A dictionary mapping command line arguments to their values.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        dest="settings_file", type=str, default="", help="path to ETL settings file."
    )
    parser.add_argument(
        "--sandbox",
        action="store_true",
        default=False,
        help="Use the Zenodo sandbox rather than production",
    )
    parser.add_argument(
        "--logfile",
        default=None,
        help="If specified, write logs to this file.",
    )
    parser.add_argument(
        "--gcs-cache-path",
        type=str,
        help="Load datastore resources from Google Cloud Storage. Should be gs://bucket[/path_prefix]",
    )
    parser.add_argument(
        "--loglevel",
        help="Set logging level (DEBUG, INFO, WARNING, ERROR, or CRITICAL).",
        default="INFO",
    )
    arguments = parser.parse_args(argv[1:])
    return arguments


def pudl_etl_job_factory(
    logfile: str | None = None, loglevel: str = "INFO", process_epacems: bool = True
) -> Callable[[], JobDefinition]:
    """Factory for parameterizing a reconstructable pudl_etl job.

    Args:
        loglevel: The log level for the job's execution.
        logfile: Path to a log file for the job's execution.
        process_epacems: Include EPA CEMS assets in the job execution.

    Returns:
        The job definition to be executed.
    """

    def get_pudl_etl_job():
        """Create an pudl_etl_job wrapped by to be wrapped by reconstructable."""
        pudl.logging_helpers.configure_root_logger(logfile=logfile, loglevel=loglevel)
        jobs = [define_asset_job("etl_job")]
        if not process_epacems:
            jobs = [
                define_asset_job(
                    "etl_job",
                    selection=pudl.etl.create_non_cems_selection(
                        pudl.etl.default_assets
                    ),
                )
            ]
        return Definitions(
            assets=pudl.etl.default_assets,
            resources=pudl.etl.default_resources,
            jobs=jobs,
        ).get_job_def("etl_job")

    return get_pudl_etl_job


def main():
    """Parse command line and initialize PUDL DB."""
    args = parse_command_line(sys.argv)

    # Display logged output from the PUDL package:
    pudl.logging_helpers.configure_root_logger(
        logfile=args.logfile, loglevel=args.loglevel
    )

    etl_settings = EtlSettings.from_yaml(args.settings_file)

    # Set PUDL_INPUT/PUDL_OUTPUT env vars from .pudl.yml if not set already!
    pudl.workspace.setup.get_defaults()

    dataset_settings_config = etl_settings.datasets.dict()
    process_epacems = True
    if etl_settings.datasets.epacems is None:
        process_epacems = False
        # Dagster config expects values for the epacems settings even though
        # the CEMS assets will not be executed. Fill in the config dictionary
        # with default cems values. Replace this workaround once dagster pydantic
        # config classes are available.
        dataset_settings_config["epacems"] = pudl.settings.EpaCemsSettings().dict()

    pudl_etl_reconstructable_job = build_reconstructable_job(
        "pudl.cli",
        "pudl_etl_job_factory",
        reconstructable_kwargs={
            "loglevel": args.loglevel,
            "logfile": args.logfile,
            "process_epacems": process_epacems,
        },
    )
    result = execute_job(
        pudl_etl_reconstructable_job,
        instance=DagsterInstance.get(),
        run_config={
            "resources": {
                "dataset_settings": {"config": dataset_settings_config},
                "datastore": {
                    "config": {
                        "sandbox": args.sandbox,
                        "gcs_cache_path": args.gcs_cache_path
                        if args.gcs_cache_path
                        else "",
                    },
                },
            },
        },
    )

    # Workaround to reliably getting full stack trace
    if not result.success:
        for event in result.all_events:
            if event.event_type_value == "STEP_FAILURE":
                raise Exception(event.event_specific_data.error)


if __name__ == "__main__":
    sys.exit(main())
