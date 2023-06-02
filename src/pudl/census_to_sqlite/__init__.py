"""Dagster definitions for the FERC to SQLite process."""

from dagster import Definitions, graph

import pudl
from pudl.convert.censusdp1tract_to_sqlite import censusdp1tract_to_sqlite
from pudl.resources import datastore

logger = pudl.logging_helpers.get_logger(__name__)


@graph
def census_to_sqlite():
    """Clone the Census DP1 database into SQLite."""
    censusdp1tract_to_sqlite()


default_resources_defs = {
    "datastore": datastore,
}

census_to_sqlite = census_to_sqlite.to_job(
    resource_defs=default_resources_defs,
    name="census_to_sqlite",
)

defs: Definitions = Definitions(jobs=[census_to_sqlite])
"""A collection of dagster assets, resources, IO managers, and jobs for the FERC to
SQLite ETL."""
