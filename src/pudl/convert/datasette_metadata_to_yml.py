"""Export metadata to YAML for Datasette."""

import argparse
import os
import sys

from dotenv import load_dotenv

import pudl
from pudl.metadata.classes import DatasetteMetadata

logger = pudl.logging_helpers.get_logger(__name__)


def parse_command_line(argv):
    """Parse command line arguments. See the -h option.

    Args:
        argv (str): Command line arguments, including absolute path to output filename.

    Returns:
        dict: Dictionary of command line arguments and their parsed values.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-o",
        "--output",
        help="Path to the file where the YAML output should be written.",
        default=False,
    )
    parser.add_argument(
        "--logfile",
        default=None,
        type=str,
        help="If specified, write logs to this file.",
    )
    parser.add_argument(
        "--loglevel",
        help="Set logging level (DEBUG, INFO, WARNING, ERROR, or CRITICAL).",
        default="INFO",
    )
    arguments = parser.parse_args(argv[1:])
    return arguments


def main():
    """Convert metadata to YAML."""
    load_dotenv()
    args = parse_command_line(sys.argv)

    pudl.logging_helpers.configure_root_logger(
        logfile=args.logfile, loglevel=args.loglevel
    )

    logger.info(f"Exporting Datasette metadata to: {args.output}")

    dm = DatasetteMetadata.from_data_source_ids(os.getenv("PUDL_OUTPUT"))
    dm.to_yaml(path=args.output)


if __name__ == "__main__":
    sys.exit(main())
