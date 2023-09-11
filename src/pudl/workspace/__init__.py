"""Tools for acquiring PUDL's original input data and organizing it locally.

The datastore subpackage takes care of downloading original data form various
public sources, organizing it locally, and providing a programmatic interface
to that collection of raw inputs, which we refer to as the PUDL datastore.

These tools are available both as a library module, and via a command line
interface installed as an entrypoint script called ``pudl_datastore``.
"""
from . import datastore, resource_cache, setup, setup_cli
