"""Entrypoint module for the PUDL ETL script."""

import sys

import pudl.cli

if __name__ == "__main__":
    sys.exit(pudl.cli.etl.main())
