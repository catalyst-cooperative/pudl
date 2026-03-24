"""Dagster resources for PUDL."""

import os

import pudl
from pudl.dagster.io_managers import (
    ferc1_dbf_sqlite_io_manager,
    ferc1_xbrl_sqlite_io_manager,
    ferc714_xbrl_sqlite_io_manager,
    geoparquet_io_manager,
    parquet_io_manager,
    pudl_mixed_format_io_manager,
)
from pudl.resources import RuntimeSettings, datastore, etl_settings, zenodo_dois

default_resources = {
    "datastore": datastore,
    "zenodo_dois": zenodo_dois,
    "pudl_io_manager": pudl_mixed_format_io_manager,
    "ferc1_dbf_sqlite_io_manager": ferc1_dbf_sqlite_io_manager,
    "ferc1_xbrl_sqlite_io_manager": ferc1_xbrl_sqlite_io_manager,
    "ferc714_xbrl_sqlite_io_manager": ferc714_xbrl_sqlite_io_manager,
    "etl_settings": etl_settings,
    "runtime_settings": RuntimeSettings(),
    "parquet_io_manager": parquet_io_manager,
    "geoparquet_io_manager": geoparquet_io_manager,
    "ferceqr_extract_settings": pudl.extract.ferceqr.ExtractSettings(
        ferceqr_archive_uri=os.getenv(
            "FERCEQR_ARCHIVE_PATH", "gs://archives.catalyst.coop/ferceqr/published"
        )
    ),
}

__all__ = ["default_resources"]
