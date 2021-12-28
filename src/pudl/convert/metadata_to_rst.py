"""Export PUDL table and field metadata to RST for use in documentation."""

import argparse
import logging
import sys

import coloredlogs

from pudl.metadata import RESOURCE_METADATA, Package

logger = logging.getLogger(__name__)


def parse_command_line(argv):
    """
    Parse command line arguments. See the -h option.

    Args:
        argv (str): Command line arguments, including caller filename.

    Returns:
        dict: Dictionary of command line arguments and their parsed values.

    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--skip',
        help="List of table names that should be skipped and excluded from RST output.",
        nargs="*",
        default=[],
    )
    parser.add_argument(
        '-o',
        '--output',
        help="Path to the file where the RST output should be written.",
        default=False
    )
    arguments = parser.parse_args(argv[1:])
    return arguments


def main():
    """Run conversion from json to rst."""
    pudl_logger = logging.getLogger("pudl")
    log_format = '%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s'
    coloredlogs.install(fmt=log_format, level='INFO', logger=pudl_logger)

    args = parse_command_line(sys.argv)
    logger.info(f"Exporting PUDL metadata to: {args.output}")
    resource_ids = [rid for rid in sorted(RESOURCE_METADATA) if rid not in args.skip]
    package = Package.from_resource_ids(resource_ids=tuple(sorted(resource_ids)))
    # Sort fields within each resource by name:
    for resource in package.resources:
        resource.schema.fields = sorted(
            resource.schema.fields, key=lambda x: x.name
        )
    package.to_rst(path=args.output)


if __name__ == '__main__':
    sys.exit(main())
