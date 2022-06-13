"""Export metadata to YAML for Datasette."""

import argparse
import sys

from pudl.helpers import configure_root_logger, get_logger
from pudl.metadata.classes import DatasetteMetadata

logger = get_logger(__name__)


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
    arguments = parser.parse_args(argv[1:])
    return arguments


def main():
    """Convert metadata to YAML."""
    configure_root_logger()

    args = parse_command_line(sys.argv)
    logger.info(f"Exporting Datasette metadata to: {args.output}")

    dm = DatasetteMetadata.from_data_source_ids()
    dm.to_yaml(path=args.output)


if __name__ == "__main__":
    sys.exit(main())
