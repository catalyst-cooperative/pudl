"""A command line interface (CLI) to the main PUDL ETL functionality.

This script cordinates the PUDL ETL process, based on parameters provided via a YAML
settings file.

If the settings for a dataset has empty parameters (meaning there are no years or tables
included), no outputs will be generated. See :doc:`/dev/run_the_etl` for details.

The output SQLite and Parquet files will be stored in ``PUDL_OUT`` in directories named
``sqlite`` and ``parquet``.  To setup your default ``PUDL_IN`` and ``PUDL_OUT``
directories see ``pudl_setup --help``.
"""
import argparse
import os
import sys

from dagster import (
    DagsterInstance,
    Definitions,
    define_asset_job,
    execute_job,
    reconstructable,
)
from dotenv import load_dotenv

import pudl
from pudl import etl
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
    parser.add_argument(
        "--partition-epacems",
        action="store_true",
        default=False,
        help="If set, output epacems year-state partitioned Parquet files",
    )
    arguments = parser.parse_args(argv[1:])
    return arguments


def get_etl_job():
    """Module level func for creating an etl_job to be wrapped by reconstructable."""
    return Definitions(
        assets=etl.default_assets,
        resources=etl.default_resources,
        jobs=[define_asset_job("etl_job")],
    ).get_job_def("etl_job")


def main():
    """Parse command line and initialize PUDL DB."""
    load_dotenv()
    args = parse_command_line(sys.argv)

    # Display logged output from the PUDL package:
    pudl.logging_helpers.configure_root_logger(
        logfile=args.logfile, loglevel=args.loglevel
    )

    etl_settings = EtlSettings.from_yaml(args.settings_file)

    if (not os.getenv("PUDL_OUT")) or (not os.getenv("PUDL_CACHE")):
        logger.warning(
            "PUDL will attempt to use legacy settings to derive paths."
            "In the future this functionality will be deprecated in favor"
            "of environment variables PUDL_OUT and PUDL_CACHE. For more"
            "info see: https://catalystcoop-pudl.readthedocs.io/en/dev/dev/dev_setup.html"
        )
        pudl_settings = pudl.workspace.setup.derive_paths(
            pudl_in=etl_settings.pudl_in, pudl_out=etl_settings.pudl_out
        )

        os.environ["PUDL_CACHE"] = pudl_settings["data_dir"]
        os.environ["PUDL_OUTPUT"] = pudl_settings["pudl_out"]
        os.environ["DAGSTER_HOME"] = pudl_settings["pudl_in"]

    execute_job(
        reconstructable(get_etl_job),
        instance=DagsterInstance.get(),
        run_config={
            "resources": {
                "dataset_settings": {"config": etl_settings.datasets.dict()},
                "datastore": {
                    "config": {
                        "sandbox": args.sandbox,
                        "gcs_cache_path": args.gcs_cache_path
                        if args.gcs_cache_path
                        else "",
                    },
                },
            },
            "ops": {
                "hourly_emissions_epacems": {
                    "config": {
                        "partition": args.partition_epacems,
                    }
                }
            },
        },
    )


if __name__ == "__main__":
    sys.exit(main())
