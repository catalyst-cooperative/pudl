"""Process raw EPA CEMS data into a Parquet dataset outside of the PUDL ETL.

This script transforms the raw EPA CEMS data from Zip compressed CSV files into
an Apache Parquet dataset partitioned by year and state.

Processing the EPA CEMS data requires information that's stored in the main PUDL
database, so to run this script, you must already have a PUDL database
available on your system.
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
from dotenv import load_dotenv

import pudl
from pudl.metadata.classes import DataSource

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
        "-y",
        "--years",
        nargs="+",
        type=int,
        help="""Which years of EPA CEMS data should be converted to Apache
        Parquet format. Default is all available years, ranging from 1995 to
        the present. Note that data is typically incomplete before ~2000.""",
        default=DataSource.from_id("epacems").working_partitions["years"],
    )
    parser.add_argument(
        "-s",
        "--states",
        nargs="+",
        type=str.upper,
        help="""Which states EPA CEMS data should be converted to Apache
        Parquet format, as a list of two letter US state abbreviations. Default
        is everything: all 48 continental US states plus Washington DC.""",
        default=DataSource.from_id("epacems").working_partitions["states"],
    )
    parser.add_argument(
        "-c",
        "--clobber",
        action="store_true",
        help="""Clobber existing parquet files if they exist. If clobber is not
        included but the parquet directory already exists the _build will
        fail.""",
        default=False,
    )
    parser.add_argument(
        "--gcs-cache-path",
        type=str,
        help="""Load datastore resources from Google Cloud Storage.
        Should be gs://bucket[/path_prefix]""",
    )
    parser.add_argument(
        "--bypass-local-cache",
        action="store_true",
        default=False,
        help="If enabled, the local file cache for datastore will not be used.",
    )
    parser.add_argument(
        "--loglevel",
        help="Set logging level (DEBUG, INFO, WARNING, ERROR, or CRITICAL).",
        default="INFO",
    )
    parser.add_argument(
        "--logfile",
        default=None,
        help="If specified, write logs to this file.",
    )
    parser.add_argument(
        "--partition",
        action="store_true",
        default=False,
        help="If set, output year-state partitioned Parquet files",
    )
    arguments = parser.parse_args(argv[1:])
    return arguments


def epacems_job_factory(loglevel: str, logfile: str) -> Callable[[], JobDefinition]:
    """Factory for parameterizing a reconstructable epacems job.

    Args:
        loglevel: The log level for the job's execution.
        logfile: Path to a log file for the job's execution.

    Returns:
        The job definition to be executed.
    """

    def get_epacems_job():
        """Create an epacems_job wrapped by to be wrapped by reconstructable."""
        pudl.logging_helpers.configure_root_logger(logfile=logfile, loglevel=loglevel)
        return Definitions(
            assets=pudl.etl.default_assets,
            resources=pudl.etl.default_resources,
            jobs=[
                define_asset_job("epacems_job", selection="hourly_emissions_epacems")
            ],
        ).get_job_def("epacems_job")

    return get_epacems_job


def main():
    """Convert zipped EPA CEMS Hourly data to Apache Parquet format."""
    load_dotenv()
    args = parse_command_line(sys.argv)
    # Display logged output from the PUDL package:
    pudl.logging_helpers.configure_root_logger(
        logfile=args.logfile, loglevel=args.loglevel
    )

    epacems_reconstructable_job = build_reconstructable_job(
        "pudl.convert.epacems_to_parquet",
        "epacems_job_factory",
        reconstructable_kwargs={"loglevel": args.loglevel, "logfile": args.logfile},
    )
    result = execute_job(
        epacems_reconstructable_job,
        instance=DagsterInstance.get(),
        run_config={
            "resources": {
                "dataset_settings": {
                    "config": {"epacems": {"years": args.years, "states": args.states}}
                },
                "datastore": {
                    "config": {
                        "gcs_cache_path": args.gcs_cache_path
                        if args.gcs_cache_path
                        else "",
                    },
                },
            },
            "ops": {
                "hourly_emissions_epacems": {
                    "config": {
                        "partition": args.partition,
                    }
                }
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
