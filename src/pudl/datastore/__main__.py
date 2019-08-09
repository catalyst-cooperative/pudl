"""Entrypoint module for PUDL datastore management."""

import sys

import pudl

if __name__ == "__main__":
    sys.exit(pudl.datastore.cli.main())
