"""Dagster asset definitions for granular FERC-to-SQLite extraction.

This module defines the prerequisite assets that build the FERC DBF and XBRL derived
SQLite databases used elsewhere in the PUDL pipeline. It should contain asset factories,
resource requirements, and materialization metadata specific to those prerequisite
databases, rather than the downstream transforms that consume them.
"""

import dagster as dg

import pudl.logging_helpers
from pudl.dagster.provenance import (
    FERC_TO_SQLITE_METADATA_KEY,
    FercSqliteProvenanceRecord,
)
from pudl.extract.ferc import (
    Ferc1DbfExtractor,
    Ferc2DbfExtractor,
    Ferc6DbfExtractor,
    Ferc60DbfExtractor,
)
from pudl.extract.xbrl import FercXbrlDatastore, convert_form
from pudl.settings import XbrlFormNumber

logger = pudl.logging_helpers.get_logger(__name__)


def dbf_to_sqlite_asset_factory(
    *, key: dg.AssetKey, dataset: str, extractor_class, op_tags: dict | None = None
) -> dg.AssetsDefinition:
    """Create a DBF-to-SQLite prerequisite asset for a specific FERC dataset."""

    @dg.asset(
        key=key,
        group_name="raw_ferc_to_sqlite",
        required_resource_keys={
            "global_data_config",
            "datastore",
            "pudl_paths",
            "runtime_settings",
            "zenodo_dois",
        },
        tags={"dataset": dataset, "data_format": "dbf"},
        op_tags=op_tags,
    )
    def _asset(context) -> dg.MaterializeResult[str]:
        ferc_to_sqlite = context.resources.global_data_config.ferc_to_sqlite
        pudl_paths = context.resources.pudl_paths
        data_config = ferc_to_sqlite.get_data_config(dataset=dataset, data_format="dbf")
        if data_config is None or not data_config.years:
            logger.info(f"No years configured for {dataset}_dbf: skipping extraction.")
            return dg.MaterializeResult(
                value="not_configured",
                metadata={
                    FERC_TO_SQLITE_METADATA_KEY: dg.MetadataValue.json(
                        FercSqliteProvenanceRecord(
                            dataset=dataset,
                            data_format="dbf",
                            status="not_configured",
                        ).model_dump(mode="json")
                    )
                },
            )
        extractor_class(
            datastore=context.resources.datastore,
            data_config=ferc_to_sqlite,
            output_path=pudl_paths.pudl_output,
        ).execute()
        return dg.MaterializeResult(
            value="complete",
            metadata={
                FERC_TO_SQLITE_METADATA_KEY: dg.MetadataValue.json(
                    FercSqliteProvenanceRecord(
                        dataset=dataset,
                        data_format="dbf",
                        status="complete",
                        zenodo_doi=context.resources.zenodo_dois.get_doi(dataset),
                        years=ferc_to_sqlite.get_dataset_years(
                            dataset=dataset, data_format="dbf"
                        ),
                        data_config=ferc_to_sqlite,
                        sqlite_path=pudl_paths.sqlite_db_path(f"{dataset}_dbf"),
                    ).model_dump(mode="json")
                )
            },
        )

    return _asset


def xbrl_to_sqlite_asset_factory(
    *, key: dg.AssetKey, form: XbrlFormNumber, op_tags: dict | None = None
) -> dg.AssetsDefinition:
    """Create an XBRL-to-SQLite prerequisite asset for a specific FERC form."""

    @dg.asset(
        key=key,
        group_name="raw_ferc_to_sqlite",
        required_resource_keys={
            "global_data_config",
            "datastore",
            "pudl_paths",
            "runtime_settings",
            "zenodo_dois",
        },
        tags={"dataset": str(form), "data_format": "xbrl"},
        op_tags=op_tags,
    )
    def _asset(context) -> dg.MaterializeResult[str]:
        runtime_settings = context.resources.runtime_settings
        pudl_paths = context.resources.pudl_paths
        data_config = (
            context.resources.global_data_config.ferc_to_sqlite.get_data_config(
                dataset=form, data_format="xbrl"
            )
        )
        if data_config is None or not data_config.years:
            logger.info(f"No years configured for {form}_xbrl: skipping extraction.")
            return dg.MaterializeResult(
                value="not_configured",
                metadata={
                    FERC_TO_SQLITE_METADATA_KEY: dg.MetadataValue.json(
                        FercSqliteProvenanceRecord(
                            dataset=str(form),
                            data_format="xbrl",
                            status="not_configured",
                        ).model_dump(mode="json")
                    )
                },
            )

        output_path = pudl_paths.pudl_output
        sqlite_path = pudl_paths.sqlite_db_path(f"{form}_xbrl")
        if sqlite_path.exists():
            sqlite_path.unlink()
        duckdb_path = pudl_paths.duckdb_db_path(f"{form}_xbrl")
        if duckdb_path.exists():
            duckdb_path.unlink()

        convert_form(
            form_data_config=data_config,
            form=form,
            datastore=FercXbrlDatastore(context.resources.datastore),
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
                    FercSqliteProvenanceRecord(
                        dataset=str(form),
                        data_format="xbrl",
                        status="complete",
                        zenodo_doi=context.resources.zenodo_dois.get_doi(str(form)),
                        years=context.resources.global_data_config.ferc_to_sqlite.get_dataset_years(
                            dataset=form, data_format="xbrl"
                        ),
                        data_config=context.resources.global_data_config.ferc_to_sqlite,
                        sqlite_path=pudl_paths.sqlite_db_path(f"{form}_xbrl"),
                    ).model_dump(mode="json")
                )
            },
        )

    return _asset


raw_ferc1_dbf__sqlite = dbf_to_sqlite_asset_factory(
    key=dg.AssetKey("raw_ferc1_dbf__sqlite"),
    dataset="ferc1",
    extractor_class=Ferc1DbfExtractor,
    op_tags={"dagster/priority": 10},
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
    op_tags={"dagster/priority": 10},
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
    op_tags={"dagster/priority": 10},
)
