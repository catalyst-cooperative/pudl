#!/usr/bin/env python
"""A script for cloning the FERC Form 1 database into SQLite."""

import logging
import sys
import argparse
import pudl
import pudl.constants as pc

# Create a logger to output any messages we might have...
logger = logging.getLogger(pudl.__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# require modern python
if not sys.version_info >= (3, 6):
    raise AssertionError(
        f"fPUDL requires Python 3.6 or later. {sys.version_info} found."
    )


def parse_command_line(argv):
    """
    Parse command line arguments. See the -h option.

    :param argv: arguments on the command line must include caller file name.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("settings_file", type=str, default='',
                        help="Specify a YAML settings file.")
    arguments = parser.parse_args(argv[1:])
    return arguments


def main():
    """The main function."""

    args = parse_command_line(sys.argv)
    settings_init = pudl.settings.settings_init(
        settings_file=args.settings_file)

    # Make sure the required input files are available before we go doing a
    # bunch of work cloning the database...
    pudl.init.verify_input_files(
        ferc1_years=settings_init['ferc1_to_sqlite_years'],
        eia860_years=[],
        eia923_years=[],
        epacems_years=[],
        epacems_states=[]
    )

    # Check args for basic validity:
    for table in settings_init['ferc1_to_sqlite_tables']:
        if table not in pc.ferc1_tbl2dbf:
            raise ValueError(
                f"{table} was not found in the list of "
                f"available FERC Form 1 tables."
            )
    if settings_init['ferc1_to_sqlite_refyear'] not in pc.data_years['ferc1']:
        raise ValueError(
            f"Reference year {settings_init['ferc1_to_sqlite_refyear']} "
            f"is outside the range of available FERC Form 1 data "
            f"({min(pc.data_years['ferc1'])}-"
            f"{max(pc.data_years['ferc1'])})."
        )
    for year in settings_init['ferc1_to_sqlite_years']:
        if year not in pc.data_years['ferc1']:
            raise ValueError(
                f"Requested data from {year} is outside the range of "
                f"available FERC Form 1 data "
                f"({min(pc.data_years['ferc1'])}-"
                f"{max(pc.data_years['ferc1'])})."
            )

    pudl.extract.ferc1.dbf2sqlite(
        tables=settings_init['ferc1_to_sqlite_tables'],
        years=settings_init['ferc1_to_sqlite_years'],
        refyear=settings_init['ferc1_to_sqlite_refyear'],
        testing=settings_init['ferc1_to_sqlite_testing'],
        bad_cols=settings_init['ferc1_to_sqlite_bad_cols'])


if __name__ == '__main__':
    sys.exit(main())
