"""Dagster definitions for the FERC to SQLite process."""
import importlib
from pathlib import Path

from dagster import Definitions, graph

import pudl
from pudl.extract.ferc1 import dbf2sqlite
from pudl.extract.xbrl import xbrl2sqlite
from pudl.resources import datastore, ferc_to_sqlite_settings
from pudl.settings import EtlSettings

logger = pudl.logging_helpers.get_logger(__name__)


@graph
def ferc_to_sqlite():
    """Clone the FERC Form 1 FoxPro database and XBRL filings into SQLite."""
    dbf2sqlite()
    xbrl2sqlite()


default_resources_defs = {
    "ferc_to_sqlite_settings": ferc_to_sqlite_settings,
    "datastore": datastore,
}

ferc_to_sqlite_full = ferc_to_sqlite.to_job(
    resource_defs=default_resources_defs,
    name="ferc_to_sqlite_full",
)

ferc_to_sqlite_fast = ferc_to_sqlite.to_job(
    resource_defs=default_resources_defs,
    name="ferc_to_sqlite_fast",
    config={
        "resources": {
            "ferc_to_sqlite_settings": {
                "config": EtlSettings.from_yaml(
                    importlib.resources.path(
                        "pudl.package_data.settings", "etl_fast.yml"
                    )
                ).ferc_to_sqlite_settings.dict()
            }
        }
    },
)

# Configure PUDL environment
with (Path.home() / ".pudl.yml").open() as f:
    pudl.workspace.setup.get_settings(yaml_file=f)

defs: Definitions = Definitions(jobs=[ferc_to_sqlite_full, ferc_to_sqlite_fast])
"""A collection of dagster assets, resources, IO managers, and jobs for the FERC to
SQLite ETL."""
