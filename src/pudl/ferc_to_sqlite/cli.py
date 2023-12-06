"""A script using Dagster to convert FERC data fom DBF and XBRL to SQLite databases."""
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
    enable_xbrl: bool = True,
    enable_dbf: bool = True,
) -> Callable[[], JobDefinition]:
    """Factory for parameterizing a reconstructable ferc_to_sqlite job.

    Args:
        logfile: Path to a log file for the job's execution.
        loglevel: The log level for the job's execution.
        enable_xbrl: if True, include XBRL data processing in the job.
        enable_dbf: if True, include DBF data processing in the job.

    Returns:
        The job definition to be executed.
    """
    if not (enable_xbrl or enable_dbf):
        raise ValueError("either dbf or xbrl needs to be enabled")

    def get_ferc_to_sqlite_job():
        """Module level func for creating a job to be wrapped by reconstructable."""
        if enable_xbrl and enable_dbf:
            return ferc_to_sqlite.ferc_to_sqlite.to_job(
                resource_defs=ferc_to_sqlite.default_resources_defs,
                name="ferc_to_sqlite_job",
            )
        if enable_xbrl:
            return ferc_to_sqlite.ferc_to_sqlite_xbrl_only.to_job(
                resource_defs=ferc_to_sqlite.default_resources_defs,
                name="ferc_to_sqlite_xbrl_only_job",
            )

        # enable_dbf has to be true
        return ferc_to_sqlite.ferc_to_sqlite_dbf_only.to_job(
            resource_defs=ferc_to_sqlite.default_resources_defs,
            name="ferc_to_sqlite_dbf_only_job",
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
    "--clobber/--no-clobber",
    type=bool,
    default=False,
    help=(
        "Clobber existing FERC SQLite databases if they exist. If clobber is not "
        "specified but the SQLite database already exists the run will fail."
    ),
)
@click.option(
    "-w",
    "--workers",
    type=int,
    default=0,
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
def main(
    etl_settings_yml: pathlib.Path,
    batch_size: int,
    workers: int,
    dagster_workers: int,
    clobber: bool,
    gcs_cache_path: str,
    logfile: pathlib.Path,
    loglevel: str,
):
    """Use Dagster to convert FERC data fom DBF and XBRL to SQLite databases.

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
        reconstructable_kwargs={"loglevel": loglevel, "logfile": logfile},
    )
    run_config = {
        "resources": {
            "ferc_to_sqlite_settings": {
                "config": etl_settings.ferc_to_sqlite_settings.model_dump()
            },
            "datastore": {
                "config": {"gcs_cache_path": gcs_cache_path},
            },
        },
        "ops": {
            "xbrl2sqlite": {
                "config": {
                    "workers": workers,
                    "batch_size": batch_size,
                    "clobber": clobber,
                },
            },
            "dbf2sqlite": {
                "config": {"clobber": clobber},
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
