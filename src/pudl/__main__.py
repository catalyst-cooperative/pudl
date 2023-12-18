"""Entrypoint module for the PUDL ETL script."""

import sys

from pudl.etl.cli import pudl_etl

if __name__ == "__main__":
    sys.exit(pudl_etl())
