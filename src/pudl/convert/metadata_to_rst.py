"""Export PUDL table and field metadata to RST for use in documentation."""

import argparse
import sys
from pathlib import Path

import pudl.logging_helpers
from pudl.metadata.classes import Package
from pudl.metadata.resources import RESOURCE_METADATA

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
        "--skip",
        help="List of table names that should be skipped and excluded from RST output.",
        nargs="*",
        default=[],
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Path to the file where the RST output should be written.",
        default=False,
    )
    parser.add_argument(
        "--docs_dir",
        help="Path to docs directory.",
        type=lambda x: Path(x).resolve(),
        default=Path().cwd() / "docs",
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
    """Run conversion from json to rst."""
    args = parse_command_line(sys.argv)
    pudl.logging_helpers.configure_root_logger(
        logfile=args.logfile, loglevel=args.loglevel
    )

    logger.info(f"Exporting PUDL metadata to: {args.output}")
    resource_ids = [rid for rid in sorted(RESOURCE_METADATA) if rid not in args.skip]
    package = Package.from_resource_ids(resource_ids=tuple(sorted(resource_ids)))
    # Sort fields within each resource by name:
    for resource in package.resources:
        resource.schema.fields = sorted(resource.schema.fields, key=lambda x: x.name)
    package.to_rst(docs_dir=args.docs_dir, path=args.output)


if __name__ == "__main__":
    sys.exit(main())
