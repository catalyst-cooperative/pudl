"""A script for cloning the Census DP1 database into SQLite.

This script generates a SQLite database that is a clone/mirror of the original
Census DP1 database. We use this cloned database as the starting point for the
main PUDL ETL process. The underlying work in the script is being done in
:mod:`pudl.extract.ferc1`.
"""
import argparse
import sys
from collections.abc import Callable

from dagster import (
    DagsterInstance,
    JobDefinition,
    build_reconstructable_job,
    execute_job,
)

import pudl
from pudl import census_to_sqlite
from pudl.settings import EtlSettings

# Create a logger to output any messages we might have...
logger = pudl.logging_helpers.get_logger(__name__)


def parse_command_line(argv):
    """Parse command line arguments. See the -h option.

    Args:
        argv (str): Command line arguments, including caller filename.

    Returns:
        dict: Dictionary of command line arguments and their parsed values.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "settings_file", type=str, default="", help="path to YAML settings file."
    )
    parser.add_argument(
        "--logfile",
        default=None,
        type=str,
        help="If specified, write logs to this file.",
    )
    parser.add_argument(
        "-c",
        "--clobber",
        action="store_true",
        help="""Clobber existing sqlite database if it exists. If clobber is
        not included but the sqlite databse already exists the _build will
        fail.""",
        default=False,
    )
    parser.add_argument(
        "--sandbox",
        action="store_true",
        default=False,
        help="Use the Zenodo sandbox rather than production",
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


def census_to_sqlite_job_factory(
    logfile: str | None = None, loglevel: str = "INFO"
) -> Callable[[], JobDefinition]:
    """Factory for parameterizing a reconstructable census_to_sqlite job.

    Args:
        loglevel: The log level for the job's execution.
        logfile: Path to a log file for the job's execution.

    Returns:
        The job definition to be executed.
    """

    def get_census_to_sqlite_job():
        """Module level func for creating a job to be wrapped by reconstructable."""
        return census_to_sqlite.census_to_sqlite.to_job(
            resource_defs=census_to_sqlite.default_resources_defs,
            name="census_to_sqlite_job",
        )

    return get_census_to_sqlite_job


def main():  # noqa: C901
    """Clone the Census database into SQLite."""
    args = parse_command_line(sys.argv)

    # Display logged output from the PUDL package:
    pudl.logging_helpers.configure_root_logger(
        logfile=args.logfile, loglevel=args.loglevel
    )

    etl_settings = EtlSettings.from_yaml(args.settings_file)

    # Set PUDL_INPUT/PUDL_OUTPUT env vars from .pudl.yml if not set already!
    pudl.workspace.setup.get_defaults()

    census_to_sqlite_reconstructable_job = build_reconstructable_job(
        "pudl.census_to_sqlite.cli",
        "census_to_sqlite_job_factory",
        reconstructable_kwargs={"loglevel": args.loglevel, "logfile": args.logfile},
    )

    result = execute_job(
        census_to_sqlite_reconstructable_job,
        instance=DagsterInstance.get(),
        run_config={
            "resources": {
                "census_to_sqlite_settings": {
                    "config": etl_settings.census_to_sqlite_settings.dict()
                },
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
        raise_on_error=True,
    )

    # Workaround to reliably getting full stack trace
    if not result.success:
        for event in result.all_events:
            if event.event_type_value == "STEP_FAILURE":
                raise Exception(event.event_specific_data.error)


if __name__ == "__main__":
    sys.exit(main())
