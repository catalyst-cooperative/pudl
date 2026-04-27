"""Dagster asset definitions for granular FERC-to-SQLite extraction.

This module defines the prerequisite assets that build the FERC DBF and XBRL derived
SQLite databases used elsewhere in the PUDL pipeline. It should contain asset factories,
resource requirements, and materialization metadata specific to those prerequisite
databases, rather than the downstream transforms that consume them.
"""

import dagster as dg

import pudl
from pudl.dagster.provenance import (
    FERC_TO_SQLITE_METADATA_KEY,
    FercSQLiteProvenanceRecord,
)
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
            metadata={
                FERC_TO_SQLITE_METADATA_KEY: dg.MetadataValue.json(
                    FercSQLiteProvenanceRecord(
                        dataset=dataset,
                        data_format="dbf",
                        status="complete",
                        zenodo_doi=context.resources.zenodo_dois.get_doi(dataset),
                        years=context.resources.etl_settings.ferc_to_sqlite.get_dataset_years(
                            dataset=dataset, data_format="dbf"
                        ),
                        settings=context.resources.etl_settings.ferc_to_sqlite,
                        sqlite_path=PudlPaths().sqlite_db_path(f"{dataset}_dbf"),
                    ).model_dump(mode="json")
                )
            },
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
        settings = context.resources.etl_settings.ferc_to_sqlite.get_dataset_settings(
            dataset=f"ferc{form.value}", data_format="xbrl"
        )
        if settings is None or not settings.years:
            logger.info(
                f"No years configured for ferc{form.value}_xbrl: skipping extraction."
            )
            return dg.MaterializeResult(
                value="not_configured",
                metadata={
                    FERC_TO_SQLITE_METADATA_KEY: dg.MetadataValue.json(
                        FercSQLiteProvenanceRecord(
                            dataset=f"ferc{form.value}",
                            data_format="xbrl",
                            status="not_configured",
                        ).model_dump(mode="json")
                    )
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
            metadata={
                FERC_TO_SQLITE_METADATA_KEY: dg.MetadataValue.json(
                    FercSQLiteProvenanceRecord(
                        dataset=f"ferc{form.value}",
                        data_format="xbrl",
                        status="complete",
                        zenodo_doi=context.resources.zenodo_dois.get_doi(
                            f"ferc{form.value}"
                        ),
                        years=context.resources.etl_settings.ferc_to_sqlite.get_dataset_years(
                            dataset=f"ferc{form.value}", data_format="xbrl"
                        ),
                        settings=context.resources.etl_settings.ferc_to_sqlite,
                        sqlite_path=PudlPaths().sqlite_db_path(
                            f"ferc{form.value}_xbrl"
                        ),
                    ).model_dump(mode="json")
                )
            },
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
