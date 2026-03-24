"""Dagster asset definitions for granular FERC-to-SQLite extraction.

This module defines the prerequisite assets that build the FERC DBF and XBRL derived
SQLite databases used elsewhere in the PUDL pipeline. It should contain asset factories,
resource requirements, and materialization metadata specific to those prerequisite
databases, rather than the downstream transforms that consume them.
"""

import dagster as dg

import pudl
from pudl.dagster.provenance import build_ferc_sqlite_provenance_metadata
from pudl.extract.ferc import (
    Ferc1DbfExtractor,
    Ferc2DbfExtractor,
    Ferc6DbfExtractor,
    Ferc60DbfExtractor,
)
from pudl.extract.xbrl import FercXbrlDatastore, convert_form
from pudl.settings import XbrlFormNumber
from pudl.workspace.setup import PudlPaths

logger = pudl.logging_helpers.get_logger(__name__)


def dbf_to_sqlite_asset_factory(
    *, key: dg.AssetKey, dataset: str, extractor_class
) -> dg.AssetsDefinition:
    """Create a DBF-to-SQLite prerequisite asset for a specific FERC dataset."""

    @dg.asset(
        key=key,
        group_name="raw_ferc_to_sqlite",
        required_resource_keys={
            "etl_settings",
            "datastore",
            "runtime_settings",
            "zenodo_dois",
        },
        tags={"dataset": dataset, "data_format": "dbf"},
    )
    def _asset(context) -> dg.MaterializeResult[str]:
        extractor_class(
            datastore=context.resources.datastore,
            settings=context.resources.etl_settings.ferc_to_sqlite,
            output_path=PudlPaths().output_dir,
        ).execute()
        return dg.MaterializeResult(
            value="complete",
            metadata=build_ferc_sqlite_provenance_metadata(
                db_name=f"{dataset}_dbf",
                etl_settings=context.resources.etl_settings,
                zenodo_dois=context.resources.zenodo_dois,
                sqlite_path=PudlPaths().sqlite_db_path(f"{dataset}_dbf"),
                status="complete",
            ),
        )

    return _asset


def xbrl_to_sqlite_asset_factory(
    *, key: dg.AssetKey, form: XbrlFormNumber
) -> dg.AssetsDefinition:
    """Create an XBRL-to-SQLite prerequisite asset for a specific FERC form."""

    @dg.asset(
        key=key,
        group_name="raw_ferc_to_sqlite",
        required_resource_keys={
            "etl_settings",
            "datastore",
            "runtime_settings",
            "zenodo_dois",
        },
        tags={"dataset": f"ferc{form.value}", "data_format": "xbrl"},
    )
    def _asset(context) -> dg.MaterializeResult[str]:
        runtime_settings = context.resources.runtime_settings
        settings = context.resources.etl_settings.get_xbrl_dataset_settings(form)
        if settings is None or settings.disabled:
            logger.info(
                f"Skipping dataset ferc{form.value}_xbrl: no config or is disabled."
            )
            return dg.MaterializeResult(
                value="skipped",
                metadata={
                    "pudl_ferc_sqlite_status": dg.MetadataValue.text("skipped"),
                },
            )

        output_path = PudlPaths().output_dir
        sqlite_path = PudlPaths().sqlite_db_path(f"ferc{form.value}_xbrl")
        if sqlite_path.exists():
            sqlite_path.unlink()
        duckdb_path = PudlPaths().duckdb_db_path(f"ferc{form.value}_xbrl")
        if duckdb_path.exists():
            duckdb_path.unlink()

        convert_form(
            settings,
            form,
            FercXbrlDatastore(context.resources.datastore),
            output_path=output_path,
            sqlite_path=sqlite_path,
            duckdb_path=duckdb_path,
            batch_size=runtime_settings.xbrl_batch_size,
            workers=runtime_settings.xbrl_num_workers,
            loglevel=runtime_settings.xbrl_loglevel,
        )
        return dg.MaterializeResult(
            value="complete",
            metadata=build_ferc_sqlite_provenance_metadata(
                db_name=f"ferc{form.value}_xbrl",
                etl_settings=context.resources.etl_settings,
                zenodo_dois=context.resources.zenodo_dois,
                sqlite_path=sqlite_path,
                status="complete",
            ),
        )

    return _asset


raw_ferc1_dbf__sqlite = dbf_to_sqlite_asset_factory(
    key=dg.AssetKey("raw_ferc1_dbf__sqlite"),
    dataset="ferc1",
    extractor_class=Ferc1DbfExtractor,
)
raw_ferc2_dbf__sqlite = dbf_to_sqlite_asset_factory(
    key=dg.AssetKey("raw_ferc2_dbf__sqlite"),
    dataset="ferc2",
    extractor_class=Ferc2DbfExtractor,
)
raw_ferc6_dbf__sqlite = dbf_to_sqlite_asset_factory(
    key=dg.AssetKey("raw_ferc6_dbf__sqlite"),
    dataset="ferc6",
    extractor_class=Ferc6DbfExtractor,
)
raw_ferc60_dbf__sqlite = dbf_to_sqlite_asset_factory(
    key=dg.AssetKey("raw_ferc60_dbf__sqlite"),
    dataset="ferc60",
    extractor_class=Ferc60DbfExtractor,
)

raw_ferc1_xbrl__sqlite = xbrl_to_sqlite_asset_factory(
    key=dg.AssetKey("raw_ferc1_xbrl__sqlite"),
    form=XbrlFormNumber.FORM1,
)
raw_ferc2_xbrl__sqlite = xbrl_to_sqlite_asset_factory(
    key=dg.AssetKey("raw_ferc2_xbrl__sqlite"),
    form=XbrlFormNumber.FORM2,
)
raw_ferc6_xbrl__sqlite = xbrl_to_sqlite_asset_factory(
    key=dg.AssetKey("raw_ferc6_xbrl__sqlite"),
    form=XbrlFormNumber.FORM6,
)
raw_ferc60_xbrl__sqlite = xbrl_to_sqlite_asset_factory(
    key=dg.AssetKey("raw_ferc60_xbrl__sqlite"),
    form=XbrlFormNumber.FORM60,
)
raw_ferc714_xbrl__sqlite = xbrl_to_sqlite_asset_factory(
    key=dg.AssetKey("raw_ferc714_xbrl__sqlite"),
    form=XbrlFormNumber.FORM714,
)

defs: dg.Definitions = dg.Definitions(
    assets=[
        raw_ferc1_dbf__sqlite,
        raw_ferc2_dbf__sqlite,
        raw_ferc6_dbf__sqlite,
        raw_ferc60_dbf__sqlite,
        raw_ferc1_xbrl__sqlite,
        raw_ferc2_xbrl__sqlite,
        raw_ferc6_xbrl__sqlite,
        raw_ferc60_xbrl__sqlite,
        raw_ferc714_xbrl__sqlite,
    ]
)
