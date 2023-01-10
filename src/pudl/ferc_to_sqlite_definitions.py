"""Dagster repositories for PUDL."""
from dagster import Definitions, graph

import pudl
from pudl.extract.ferc1 import dbf2sqlite
from pudl.extract.xbrl import xbrl2sqlite
from pudl.settings import ferc_to_sqlite_settings
from pudl.workspace.datastore import datastore

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
                "config": {
                    "ferc1_dbf_to_sqlite_settings": {"years": [2020]},
                    "ferc1_xbrl_to_sqlite_settings": {"years": [2021]},
                    "ferc2_xbrl_to_sqlite_settings": {"years": [2021]},
                    "ferc6_xbrl_to_sqlite_settings": {"years": [2021]},
                    "ferc60_xbrl_to_sqlite_settings": {"years": [2021]},
                    "ferc714_xbrl_to_sqlite_settings": {"years": [2021]},
                }
            }
        }
    },
)

defs = Definitions(jobs=[ferc_to_sqlite_full, ferc_to_sqlite_fast])
