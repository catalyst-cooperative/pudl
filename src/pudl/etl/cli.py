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
from pudl.settings import EtlSettings
from pudl.workspace.setup import PudlPaths

logger = pudl.logging_helpers.get_logger(__name__)


def pudl_etl_job_factory(
    logfile: str | None = None,
    loglevel: str = "INFO",
    base_job: str = "etl_full",
) -> Callable[[], JobDefinition]:
    """Factory for parameterizing a reconstructable pudl_etl job.

    Args:
        loglevel: The log level for the job's execution.
        logfile: Path to a log file for the job's execution.
        base_job: Name of the Dagster ETL job to execute.

    Returns:
        The job definition to be executed.
    """

    def get_pudl_etl_job():
        """Create an pudl_etl_job wrapped by to be wrapped by reconstructable."""
        pudl.logging_helpers.configure_root_logger(logfile=logfile, loglevel=loglevel)
        return defs.get_job_def(base_job)

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
    "--cloud-cache-path",
    type=str,
    default="s3://pudl.catalyst.coop/zenodo",
    help=(
        "Load cached inputs from cloud object storage (S3 or GCS). This is typically "
        "much faster and more reliable than downloading from Zenodo directly. By "
        "default we read from the cache in PUDL's free, public AWS Open Data Registry "
        "bucket."
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
    cloud_cache_path: str,
    logfile: pathlib.Path | None,
    loglevel: str,
):
    """Use Dagster to run the PUDL ETL, as specified by the file ETL_SETTINGS_YML."""
    # Display logged output from the PUDL package:
    logfile_str = str(logfile) if logfile is not None else None
    pudl.logging_helpers.configure_root_logger(logfile=logfile_str, loglevel=loglevel)
    etl_settings = EtlSettings.from_yaml(str(etl_settings_yml))
    if etl_settings.datasets is None:
        raise click.BadParameter(
            "No datasets were configured in the ETL settings file.",
            param_hint="etl_settings_yml",
        )

    if etl_settings.datasets.epacems is None or etl_settings.datasets.epacems.disabled:
        raise click.BadParameter(
            "EPA CEMS is now always included in the ETL. "
            "Set datasets.epacems with disabled: false in your ETL settings file.",
            param_hint="etl_settings_yml",
        )

    dataset_settings_config = etl_settings.datasets.model_dump()

    pudl_etl_reconstructable_job = build_reconstructable_job(
        "pudl.etl.cli",
        "pudl_etl_job_factory",
        reconstructable_kwargs={
            "loglevel": loglevel,
            "logfile": logfile_str,
        },
    )
    run_config = {
        "resources": {
            "dataset_settings": {"config": dataset_settings_config},
            "datastore": {
                "config": {
                    "cloud_cache_path": cloud_cache_path,
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
                event_error = getattr(event.event_specific_data, "error", None)
                if event_error is not None:
                    raise Exception(event_error)
                raise Exception("ETL failed but no step error details were available.")
    else:
        logger.info("ETL job completed successfully, publishing outputs.")
        for output_path in etl_settings.publish_destinations:
            logger.info(f"Publishing outputs to {output_path}")
            fs, _, _ = fsspec.get_fs_token_paths(output_path)
            fs.put(
                PudlPaths().output_dir,  # type: ignore[call-arg]
                output_path,
                recursive=True,
            )


if __name__ == "__main__":
    sys.exit(pudl_etl())
