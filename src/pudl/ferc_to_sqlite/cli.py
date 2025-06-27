"""A script using Dagster to convert FERC data from DBF and XBRL to SQLite databases."""

import pathlib
import sys
import time
from collections.abc import Callable

import click
from dagster import (
    DagsterInstance,
    JobDefinition,
    build_reconstructable_job,
    execute_job,
)

import pudl
from pudl import ferc_to_sqlite
from pudl.helpers import get_dagster_execution_config
from pudl.settings import EtlSettings

# Create a logger to output any messages we might have...
logger = pudl.logging_helpers.get_logger(__name__)


def ferc_to_sqlite_job_factory(
    logfile: str | None = None,
    loglevel: str = "INFO",
    dataset_only: str | None = None,
) -> Callable[[], JobDefinition]:
    """Factory for parameterizing a reconstructable ferc_to_sqlite job.

    Args:
        logfile: Path to a log file for the job's execution.
        loglevel: The log level for the job's execution.

    Returns:
        The job definition to be executed.
    """

    def get_ferc_to_sqlite_job():
        """Module level func for creating a job to be wrapped by reconstructable."""
        ferc_to_sqlite_graph = ferc_to_sqlite.ferc_to_sqlite
        op_selection = None
        if dataset_only is not None:
            logger.warning(f"Running ferc_to_sqlite restricted to {dataset_only}")
            op_selection = [dataset_only]
        return ferc_to_sqlite_graph.to_job(
            resource_defs=ferc_to_sqlite.default_resources_defs,
            name="ferc_to_sqlite_job",
            op_selection=op_selection,
        )

    return get_ferc_to_sqlite_job


@click.command(
    name="ferc_to_sqlite",
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
    "-b",
    "--batch-size",
    type=int,
    default=50,
    help="Number of XBRL instances to be processed at a time.",
)
@click.option(
    "-w",
    "--workers",
    type=int,
    default=None,
    help=(
        "Number of worker processes to use when parsing XBRL filings. "
        "Defaults to using the number of CPUs."
    ),
)
@click.option(
    "--dagster-workers",
    type=int,
    default=0,
    help=(
        "Set the max number of processes that dagster can launch. "
        "If set to 1, in-process serial executor will be used. If set to 0, "
        "dagster will saturate available CPUs (this is the default)."
    ),
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
    type=click.Path(
        exists=False,
        resolve_path=True,
        path_type=pathlib.Path,
    ),
    help="If specified, write logs to this file.",
)
@click.option(
    "--loglevel",
    type=click.Choice(
        ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False
    ),
    default="INFO",
)
@click.option(
    "--dataset-only",
    type=str,
    help=(
        "If specified, restricts processing to only a given dataset. This is"
        "expected to be in the form of ferc1_dbf, ferc1_xbrl. "
        "This is intended for ci-integration purposes where we fan-out the "
        "execution into several parallel small jobs that should finish faster. "
        "Other operations are still going to be invoked, but they will terminate "
        "early if this setting is in use."
    ),
)
def main(
    etl_settings_yml: pathlib.Path,
    batch_size: int,
    workers: int | None,
    dagster_workers: int,
    gcs_cache_path: str,
    logfile: pathlib.Path,
    loglevel: str,
    dataset_only: str,
):
    """Use Dagster to convert FERC data from DBF and XBRL to SQLite databases.

    Reads settings specifying which forms and years to convert from ETL_SETTINGS_YML.

    Also produces JSON versions of XBRL taxonomies and datapackage descriptors which
    annotate the XBRL derived SQLite databases.
    """
    # Display logged output from the PUDL package:
    pudl.logging_helpers.configure_root_logger(logfile=logfile, loglevel=loglevel)

    etl_settings = EtlSettings.from_yaml(etl_settings_yml)

    ferc_to_sqlite_reconstructable_job = build_reconstructable_job(
        "pudl.ferc_to_sqlite.cli",
        "ferc_to_sqlite_job_factory",
        reconstructable_kwargs={
            "loglevel": loglevel,
            "logfile": logfile,
            "dataset_only": dataset_only,
        },
    )

    run_config = {
        "resources": {
            "ferc_to_sqlite_settings": {
                "config": etl_settings.ferc_to_sqlite_settings.model_dump()
            },
            "datastore": {
                "config": {"gcs_cache_path": gcs_cache_path},
            },
            "runtime_settings": {
                "config": {
                    "xbrl_num_workers": workers,
                    "xbrl_batch_size": batch_size,
                },
            },
        },
    }
    run_config.update(get_dagster_execution_config(dagster_workers))

    start_time = time.time()
    result = execute_job(
        ferc_to_sqlite_reconstructable_job,
        instance=DagsterInstance.get(),
        run_config=run_config,
        raise_on_error=True,
    )
    end_time = time.time()
    logger.info(f"FERC to SQLite job completed in {end_time - start_time} seconds.")

    # Workaround to reliably getting full stack trace
    if not result.success:
        for event in result.all_events:
            if event.event_type_value == "STEP_FAILURE":
                raise Exception(event.event_specific_data.error)


if __name__ == "__main__":
    sys.exit(main())
