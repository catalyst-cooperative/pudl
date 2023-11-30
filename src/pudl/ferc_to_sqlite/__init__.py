"""Dagster definitions for the FERC to SQLite process."""
import importlib.resources

from dagster import Definitions, graph

import pudl
from pudl.extract.ferc import dbf2sqlite
from pudl.extract.xbrl import XbrlRuntimeSettings, xbrl2sqlite_op_factory
from pudl.resources import datastore, ferc_to_sqlite_settings
from pudl.settings import EtlSettings, XbrlFormNumber

logger = pudl.logging_helpers.get_logger(__name__)


@graph
def ferc_to_sqlite():
    """Clone the FERC FoxPro databases and XBRL filings into SQLite."""
    dbf2sqlite()
    for form in XbrlFormNumber:
        xbrl2sqlite_op_factory(form)()


@graph
def ferc_to_sqlite_dbf_only():
    """Clone the FERC FoxPro databases into SQLite."""
    dbf2sqlite()


@graph
def ferc_to_sqlite_xbrl_only():
    """Clone the FERC XBRL databases into SQLite."""
    for form in XbrlFormNumber:
        xbrl2sqlite_op_factory(form)()

default_resources_defs = {
    "ferc_to_sqlite_settings": ferc_to_sqlite_settings,
    "xbrl_runtime_settings": XbrlRuntimeSettings(),
    "datastore": datastore,
}

ferc_to_sqlite_full = ferc_to_sqlite.to_job(
    resource_defs=default_resources_defs,
    name="ferc_to_sqlite_full",
)

ferc_to_sqlite_fast_settings = EtlSettings.from_yaml(
    importlib.resources.files("pudl.package_data.settings") / "etl_fast.yml"
).ferc_to_sqlite_settings

ferc_to_sqlite_fast = ferc_to_sqlite.to_job(
    resource_defs=default_resources_defs,
    name="ferc_to_sqlite_fast",
    config={
        "resources": {
            "ferc_to_sqlite_settings": {
                "config": ferc_to_sqlite_fast_settings.model_dump(),
            },
            "xbrl_runtime_settings": {
                # TODO(rousik): do we need to set some defaults here?
                "config": {},
            }
        },
    },
)

defs: Definitions = Definitions(jobs=[ferc_to_sqlite_full, ferc_to_sqlite_fast])
"""A collection of dagster assets, resources, IO managers, and jobs for the FERC to
SQLite ETL."""
