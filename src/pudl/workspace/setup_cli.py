"""Set up a well-organized PUDL data management workspace.

This script creates a well-defined directory structure for use by the PUDL
package, and copies several example settings files and Jupyter notebooks into
it to get you started. If the command is run without any arguments, it will
create this workspace in your current directory.

It's also possible to specify different input and output directories, which is
useful if you want to use a single PUDL data store (which may contain many GB
of data) to support several different workspaces.  See the --pudl_in and
--pudl_out options.

By default the script will not overwrite existing files. If you want it to
replace existing files use the --clobber option.

The directory structure set up for PUDL looks like this:

PUDL_DIR
  └── settings

PUDL_INPUT
  ├── censusdp1tract
  ├── eia860
  ├── eia860m
  ├── eia861
  ├── eia923
  ...
  ├── epacems
  ├── ferc1
  ├── ferc714
  └── tmp

PUDL_OUTPUT
  ├── ferc1.sqlite
  ...
  ├── pudl.sqlite
  └── hourly_emissions_cems

Initially, the directories in the data store will be empty. The pudl_datastore or
pudl_etl commands will download data from public sources and organize it for
you there by source.
"""
import argparse
import pathlib
import sys

import pudl

logger = pudl.logging_helpers.get_logger(__name__)


def initialize_parser():
    """Parse command line arguments for the pudl_setup script."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--pudl_input",
        "-i",
        type=str,
        dest="pudl_input",
        help="""Directory where the PUDL input data should be located.""",
    )
    parser.add_argument(
        "--pudl_output",
        "-o",
        type=str,
        dest="pudl_output",
        help="""Directory where the PUDL outputs, notebooks, and example
        settings files should be located.""",
    )
    parser.add_argument(
        "--clobber",
        "-c",
        action="store_true",
        help="""Replace existing settings files, notebooks, etc. with fresh
        copies of the examples distributed with the PUDL Python package. This
        will also update your default PUDL workspace, if you have one.""",
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
    return parser


def main():
    """Set up a new default PUDL workspace."""
    # Display logged output from the PUDL package:

    parser = initialize_parser()
    args = parser.parse_args(sys.argv[1:])
    pudl.logging_helpers.configure_root_logger(
        logfile=args.logfile, loglevel=args.loglevel
    )

    if args.pudl_input:
        pudl_in = pathlib.Path(args.pudl_in).expanduser().resolve()
        if not pathlib.Path.is_dir(pudl_in):
            raise FileNotFoundError(f"Directory not found: {pudl_in}")

    if args.pudl_output:
        pudl_out = pathlib.Path(args.pudl_out).expanduser().resolve()
        if not pathlib.Path.is_dir(pudl_out):
            raise FileNotFoundError(f"Directory not found: {pudl_out}")

    settings = pudl.workspace.setup.get_defaults(
        input_dir=args.pudl_input, output_dir=args.pudl_output
    )
    pudl.workspace.setup.init(settings, clobber=args.clobber)


if __name__ == "__main__":
    sys.exit(main())
