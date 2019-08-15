"""A command line interface for PUDL workspace setup."""

import argparse
import logging
import pathlib
import sys

import coloredlogs

import pudl


def parse_command_line(argv):
    """Parse command line arguments for the pudl_setup script."""
    # Grab the default workspace if it already exists, since this will let
    # people refresh their settings files etc. with the originals:
    default_workspace = pudl.workspace.get_defaults()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--pudl_in',
        '-i',
        type=str,
        help="""Path to directory where PUDL datastore should be located.""",
        default=default_workspace["pudl_in"]
    )
    parser.add_argument(
        '--pudl_out',
        '-o',
        type=str,
        help="""Path to directory where PUDL outputs should be located.""",
        default=default_workspace["pudl_out"]
    )
    parser.add_argument(
        '--clobber',
        '-c',
        action='store_true',
        help="""Replace existing settings files, notebooks, etc. with fresh
        copies of the defaults distributed with the PUDL Python package.""",
        default=False
    )


def main():
    """Set up a new default PUDL workspace."""
    # Display logged output from the PUDL package:
    logger = logging.getLogger(pudl.__name__)
    log_format = '%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s'
    coloredlogs.install(fmt=log_format, level='INFO', logger=logger)

    args = parse_command_line(sys.argv)
    if args.pudl_in is None:
        raise ValueError(
            f"Missing required argument pudl_in. See {sys.argv[0]} --help")
    if args.pudl_out is None:
        raise ValueError(
            f"Missing required argument pudl_out. See {sys.argv[0]} --help")

    # Given pudl_in and pudl_out, create a user settings file.
    pudl_in = pathlib.Path(args.pudl_in).expanduser().resolve()
    logger.info(f"Setting default pudl_in: {pudl_in}")
    if not pathlib.Path.is_dir(pudl_in):
        raise FileNotFoundError(
            f"Directory not found: {pudl_in}")

    pudl_out = pathlib.Path(args.pudl_out).expanduser().resolve()
    logger.info(f"Setting default pudl_out: {pudl_out}")
    if not pathlib.Path.is_dir(pudl_out):
        raise FileNotFoundError(
            f"Directory not found: {pudl_out}")

    logger.info(f"You can update these default values by editing "
                f"{pathlib.Path.home()}/.pudl.yml")

    pudl.workspace.set_defaults(
        pudl_in, pudl_out, clobber=args.clobber)
    pudl.workspace.init(pudl_in=pudl_in,
                        pudl_out=pudl_out,
                        clobber=args.clobber)


if __name__ == '__main__':
    sys.exit(main())
