"""A script for cloning the FERC Form 1 database into SQLite.

This script generates a SQLite database that is a clone/mirror of the original
FERC Form1 database. We use this cloned database as the starting point for the
main PUDL ETL process. The underlying work in the script is being done in
:mod:`pudl.extract.ferc1`.
"""
import argparse
import sys

from dotenv import load_dotenv

import pudl
from pudl import ferc_to_sqlite
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
        "--sandbox",
        action="store_true",
        default=False,
        help="Use the Zenodo sandbox rather than production",
    )
    parser.add_argument(
        "-b",
        "--batch-size",
        default=50,
        type=int,
        help="Specify number of XBRL instances to be processed at a time (defaults to 50)",
    )
    parser.add_argument(
        "-w",
        "--workers",
        default=None,
        type=int,
        help="Specify number of worker processes for parsing XBRL filings.",
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


def main():  # noqa: C901
    """Clone the FERC Form 1 FoxPro database into SQLite."""
    load_dotenv()
    args = parse_command_line(sys.argv)

    # Display logged output from the PUDL package:
    pudl.logging_helpers.configure_root_logger(
        logfile=args.logfile, loglevel=args.loglevel
    )

    ferc_to_sqlite_job = ferc_to_sqlite.defs.get_job_def("ferc_to_sqlite_fast")
    ferc_to_sqlite_job.execute_in_process(
        run_config={
            "resources": {
                "ferc_to_sqlite_settings": {
                    "config": EtlSettings.from_yaml(
                        args.settings_file
                    ).ferc_to_sqlite_settings.dict(
                        exclude={
                            "ferc1_dbf_to_sqlite_settings": {"tables"},
                            "ferc1_xbrl_to_sqlite_settings": {"tables"},
                            "ferc2_xbrl_to_sqlite_settings": {"tables"},
                            "ferc6_xbrl_to_sqlite_settings": {"tables"},
                            "ferc60_xbrl_to_sqlite_settings": {"tables"},
                            "ferc714_xbrl_to_sqlite_settings": {"tables"},
                        }
                    )
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
            "ops": {
                "xbrl2sqlite": {
                    "config": {
                        "workers": args.workers,
                        "batch_size": args.batch_size,
                    },
                },
            },
        }
    )


if __name__ == "__main__":
    sys.exit(main())
