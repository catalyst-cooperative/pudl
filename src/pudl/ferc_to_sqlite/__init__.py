"""Dagster definitions for the FERC to SQLite process."""
import importlib

from dagster import Definitions, graph

import pudl
from pudl.extract.ferc1 import dbf2sqlite
from pudl.extract.xbrl import xbrl2sqlite
from pudl.package_data import settings
from pudl.resources import datastore, ferc_to_sqlite_settings
from pudl.settings import EtlSettings

logger = pudl.logging_helpers.get_logger(__name__)


@graph
def ferc_to_sqlite():
    """Clone the FERC Form 1 FoxPro database and XBRL filings into SQLite."""
    dbf2sqlite()
    xbrl2sqlite()


ferc_to_sqlite_full = ferc_to_sqlite.to_job(
    resource_defs={
        "ferc_to_sqlite_settings": ferc_to_sqlite_settings,
        "datastore": datastore,
    },
    name="ferc_to_sqlite_full",
)

ferc_to_sqlite_fast = ferc_to_sqlite.to_job(
    resource_defs={
        "ferc_to_sqlite_settings": ferc_to_sqlite_settings,
        "datastore": datastore,
    },
    name="ferc_to_sqlite_fast",
    config={
        "resources": {
            "ferc_to_sqlite_settings": {
                "config": EtlSettings.from_yaml(
                    importlib.resources.path(settings, "etl_fast.yml")
                ).ferc_to_sqlite_settings.dict(
                    exclude={
                        "ferc1_dbf_to_sqlite_settings": {"tables"},
                        "ferc1_xbrl_to_sqlite_settings": {"tables"},
                        "ferc2_xbrl_to_sqlite_settings": {"tables"},
                        "ferc6_xbrl_to_sqlite_settings": {"tables"},
                        "ferc60_xbrl_to_sqlite_settings": {"tables"},
                        "ferc714_xbrl_to_sqlite_settings": {"tables"},
                    }
                )
            }
        }
    },
)

defs = Definitions(jobs=[ferc_to_sqlite_full, ferc_to_sqlite_fast])
