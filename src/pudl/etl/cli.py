"""A command line interface (CLI) to the main PUDL ETL functionality."""

import pathlib
import sys
from collections.abc import Callable

import click
import fsspec
from dagster import (
    DagsterInstance,
    JobDefinition,
    build_reconstructable_job,
    execute_job,
)

import pudl
from pudl.etl import defs
from pudl.helpers import get_dagster_execution_config
from pudl.settings import EpaCemsSettings, EtlSettings
from pudl.workspace.setup import PudlPaths

logger = pudl.logging_helpers.get_logger(__name__)


def pudl_etl_job_factory(
    logfile: str | None = None,
    loglevel: str = "INFO",
    process_epacems: bool = True,
    base_job: str = "etl_full",
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
        cems_suffix = "" if process_epacems else "_no_cems"
        job_name = f"{base_job}{cems_suffix}"
        return defs.get_job_def(job_name)

    return get_pudl_etl_job


@click.command(
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.argument(
    "etl_settings_yml",
    type=click.Path(
        exists=True,
        dir_okay=False,
        resolve_path=True,
        path_type=pathlib.Path,
    ),
)
@click.option(
    "--dagster-workers",
    default=0,
    type=int,
    help="Max number of processes Dagster can launch. Defaults to the number of CPUs.",
)
@click.option(
    "--gcs-cache-path",
    type=str,
    default="",
    help=(
        "Load cached inputs from Google Cloud Storage if possible. This is usually "
        "much faster and more reliable than downloading from Zenodo directly. The "
        "path should be a URL of the form gs://bucket[/path_prefix]. Internally we use "
        "gs://internal-zenodo-cache.catalyst.coop. A public cache is available at "
        "gs://zenodo-cache.catalyst.coop but requires GCS authentication and a billing "
        "project to pay data egress costs."
    ),
)
@click.option(
    "--logfile",
    help="If specified, write logs to this file.",
    type=click.Path(
        exists=False,
        resolve_path=True,
        path_type=pathlib.Path,
    ),
)
@click.option(
    "--loglevel",
    default="INFO",
    type=click.Choice(
        ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False
    ),
)
def pudl_etl(
    etl_settings_yml: pathlib.Path,
    dagster_workers: int,
    gcs_cache_path: str,
    logfile: pathlib.Path,
    loglevel: str,
):
    """Use Dagster to run the PUDL ETL, as specified by the file ETL_SETTINGS_YML."""
    # Display logged output from the PUDL package:
    pudl.logging_helpers.configure_root_logger(logfile=logfile, loglevel=loglevel)
    etl_settings = EtlSettings.from_yaml(etl_settings_yml)

    dataset_settings_config = etl_settings.datasets.model_dump()
    process_epacems = True
    if etl_settings.datasets.epacems is None or etl_settings.datasets.epacems.disabled:
        process_epacems = False
        # Dagster config expects values for the epacems settings even though
        # the CEMS assets will not be executed. Fill in the config dictionary
        # with default cems values. Replace this workaround once dagster pydantic
        # config classes are available.
        dataset_settings_config["epacems"] = EpaCemsSettings().model_dump()

    pudl_etl_reconstructable_job = build_reconstructable_job(
        "pudl.etl.cli",
        "pudl_etl_job_factory",
        reconstructable_kwargs={
            "loglevel": loglevel,
            "logfile": logfile,
            "process_epacems": process_epacems,
        },
    )
    run_config = {
        "resources": {
            "dataset_settings": {"config": dataset_settings_config},
            "datastore": {
                "config": {
                    "gcs_cache_path": gcs_cache_path,
                },
            },
        },
    }

    # Limit the number of concurrent workers when launch assets that use a lot of memory.
    tag_concurrency_limits = [
        {
            "key": "memory-use",
            "value": "high",
            "limit": 4,
        },
    ]

    run_config.update(
        get_dagster_execution_config(
            num_workers=dagster_workers,
            tag_concurrency_limits=tag_concurrency_limits,
        )
    )

    result = execute_job(
        pudl_etl_reconstructable_job,
        instance=DagsterInstance.get(),
        run_config=run_config,
    )

    # Workaround to reliably getting full stack trace
    if not result.success:
        for event in result.all_events:
            if event.event_type_value == "STEP_FAILURE":
                raise Exception(event.event_specific_data.error)
    else:
        logger.info("ETL job completed successfully, publishing outputs.")
        for output_path in etl_settings.publish_destinations:
            logger.info(f"Publishing outputs to {output_path}")
            fs, _, _ = fsspec.get_fs_token_paths(output_path)
            fs.put(
                PudlPaths().output_dir,
                output_path,
                recursive=True,
            )


if __name__ == "__main__":
    sys.exit(pudl_etl())
