"""Dagster asset definitions for granular FERC-to-SQLite extraction.

This module defines the prerequisite assets that build the FERC DBF and XBRL derived
SQLite databases used elsewhere in the PUDL pipeline. It should contain asset factories,
resource requirements, and materialization metadata specific to those prerequisite
databases, rather than the downstream transforms that consume them.
"""

import os
from io import BytesIO
from pathlib import Path
from zipfile import ZipFile

import dagster as dg
import fsspec

import pudl.logging_helpers
from pudl.dagster.provenance import (
    FERC_TO_SQLITE_METADATA_KEY,
    FercSqliteProvenance,
    FercSqliteProvenanceRecord,
    assert_ferc_sqlite_compatible,
    get_xbrl_extractor_version,
)
from pudl.extract.ferc import (
    Ferc1DbfExtractor,
    Ferc2DbfExtractor,
    Ferc6DbfExtractor,
    Ferc60DbfExtractor,
)
from pudl.extract.xbrl import FercXbrlDatastore, convert_form
from pudl.settings import FercToSqliteDataConfig, XbrlFormNumber
from pudl.workspace.setup import PudlPaths

logger = pudl.logging_helpers.get_logger(__name__)


def _compare_provenance_metadata(
    required_provenance: FercSqliteProvenance,
    stored_provenance: FercSqliteProvenanceRecord | None,
) -> FercSqliteProvenanceRecord | None:
    """Compare provenance metadata and return if compatible."""
    # Can be None for legacy SQLite DB's that don't contain metadata
    if stored_provenance is None:
        return None

    try:
        assert_ferc_sqlite_compatible(
            stored=stored_provenance, provenance=required_provenance
        )
        return stored_provenance
    except RuntimeError as e:
        logger.warning(
            f"SQLite DB cached at {stored_provenance.sqlite_path} is not compatible. "
            f"See the following for details: {e}"
        )
    return None


def _download_nightly_db(sqlite_path: Path):
    """Download nightly SQLite db and extract from zipfile, writing to local workspace."""
    with (
        fsspec.open(
            f"s3://pudl.catalyst.coop/nightly/{sqlite_path.name}.zip", "rb", anon=True
        ) as f,
        ZipFile(BytesIO(f.read())) as archive,
        archive.open(sqlite_path.name) as nightly_sqlite,
        sqlite_path.open("wb") as local_sqlite,
    ):
        local_sqlite.write(nightly_sqlite.read())


def _check_compatible_cached_db(
    dataset: str,
    data_format: str,
    zenodo_doi: str,
    sqlite_path: Path,
    ferc_to_sqlite: FercToSqliteDataConfig,
) -> FercSqliteProvenanceRecord | None:
    """Check to see if there is a compatible SQLite DB either locally, or in nightly builds.

    This function will first check the local SQLite DB for the specified ``dataset``
    and ``data_format`` to see if it contains a ``FercSqliteProvenanceRecord`` that
    is compatible with the requirements of the current run. If the local DB doesn't
    exist or contains an incompatible record, it will then download the DB produced
    by the most recent nightly build and perform the same check. If one of the DBs
    is found to be compatible, then it will return the associated ``FercSqliteProvenanceRecord``,
    which will trigger the ``ferc_to_sqlite`` process to skip the normal extraction,
    and use the cached DB. If this function returns ``None``, then the extraction will
    go forward as normal.

    If the environment variable, ``PUDL_FORCE_FERC_TO_SQLITE``, is set to ``true``, then
    this function will immediately return ``None``, triggering the normal extraction.
    """
    # Check if configured to force extraction
    if os.getenv("PUDL_FORCE_FERC_TO_SQLITE", default="false").lower() == "true":
        return None

    # Assemble required provenance for current run
    provenance = FercSqliteProvenance(
        dataset=dataset,
        data_format=data_format,
        zenodo_doi=zenodo_doi,
        years=ferc_to_sqlite.get_dataset_years(dataset, data_format),
        ferc_xbrl_extractor_version=get_xbrl_extractor_version(),
    )

    # Check local DB first
    stored_local = FercSqliteProvenanceRecord.from_sqlite(sqlite_path)

    # If not compatible, try nightly builds
    if (
        compatible_metadata := _compare_provenance_metadata(provenance, stored_local)
    ) is None:
        logger.info(
            f"Provenance metadata for local version of {sqlite_path.name} is incompatible."
            " Downloading version from nightly builds."
        )
        _download_nightly_db(sqlite_path)
        stored_nightly = FercSqliteProvenanceRecord.from_sqlite(sqlite_path)
        compatible_metadata = _compare_provenance_metadata(provenance, stored_nightly)
    if compatible_metadata is None:
        logger.info(
            f"Can't find a cached version of {sqlite_path.name} with compatible provenance metadata."
            " Extracting from scratch."
        )
    return compatible_metadata


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
            "runtime_settings",
            "zenodo_dois",
        },
        tags={"dataset": dataset, "data_format": "dbf"},
        op_tags=op_tags,
    )
    def _asset(context) -> dg.MaterializeResult[str]:
        ferc_to_sqlite = context.resources.global_data_config.ferc_to_sqlite
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

        zenodo_doi = context.resources.zenodo_dois.get_doi(dataset)
        sqlite_path = PudlPaths().sqlite_db_path(f"{dataset}_dbf")

        # Check if there's a cached SQLite DB that is compatible
        if (
            provenance := _check_compatible_cached_db(
                dataset=dataset,
                data_format="dbf",
                zenodo_doi=zenodo_doi,
                sqlite_path=sqlite_path,
                ferc_to_sqlite=ferc_to_sqlite,
            )
        ) is None:
            # If not, run extraction
            extractor_class(
                datastore=context.resources.datastore,
                data_config=ferc_to_sqlite,
                output_path=PudlPaths().output_dir,
            ).execute()

            provenance = FercSqliteProvenanceRecord(
                dataset=dataset,
                data_format="dbf",
                status="complete",
                zenodo_doi=zenodo_doi,
                years=ferc_to_sqlite.get_dataset_years(
                    dataset=dataset, data_format="dbf"
                ),
                data_config=ferc_to_sqlite,
                sqlite_path=sqlite_path,
                ferc_xbrl_extractor_version=get_xbrl_extractor_version(),
            )
            provenance.to_sqlite()
        else:
            logger.info(
                f"Found compatible cached SQLite DB for {sqlite_path.name}. Skipping extraction."
            )

        # Return provenance metadata
        return dg.MaterializeResult(
            value="complete",
            metadata={
                FERC_TO_SQLITE_METADATA_KEY: dg.MetadataValue.json(
                    provenance.model_dump(mode="json")
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
            "runtime_settings",
            "zenodo_dois",
        },
        tags={"dataset": str(form), "data_format": "xbrl"},
        op_tags=op_tags,
    )
    def _asset(context) -> dg.MaterializeResult[str]:
        runtime_settings = context.resources.runtime_settings
        ferc_to_sqlite = context.resources.global_data_config.ferc_to_sqlite
        data_config = ferc_to_sqlite.get_data_config(dataset=form, data_format="xbrl")
        dataset = str(form)
        zenodo_doi = context.resources.zenodo_dois.get_doi(str(form))
        if data_config is None or not data_config.years:
            logger.info(f"No years configured for {form}_xbrl: skipping extraction.")
            return dg.MaterializeResult(
                value="not_configured",
                metadata={
                    FERC_TO_SQLITE_METADATA_KEY: dg.MetadataValue.json(
                        FercSqliteProvenanceRecord(
                            dataset=dataset,
                            data_format="xbrl",
                            status="not_configured",
                        ).model_dump(mode="json")
                    )
                },
            )

        output_path = PudlPaths().output_dir
        sqlite_path = PudlPaths().sqlite_db_path(f"{form}_xbrl")
        if sqlite_path.exists():
            sqlite_path.unlink()
        duckdb_path = PudlPaths().duckdb_db_path(f"{form}_xbrl")
        if duckdb_path.exists():
            duckdb_path.unlink()

        # Check if there's a cached SQLite DB that is compatible
        if (
            provenance := _check_compatible_cached_db(
                dataset=dataset,
                data_format="xbrl",
                zenodo_doi=zenodo_doi,
                sqlite_path=sqlite_path,
                ferc_to_sqlite=ferc_to_sqlite,
            )
        ) is None:
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
            provenance = FercSqliteProvenanceRecord(
                dataset=str(form),
                data_format="xbrl",
                status="complete",
                zenodo_doi=zenodo_doi,
                years=ferc_to_sqlite.get_dataset_years(
                    dataset=form, data_format="xbrl"
                ),
                data_config=ferc_to_sqlite,
                sqlite_path=PudlPaths().sqlite_db_path(f"{form}_xbrl"),
            )
            provenance.to_sqlite()

        return dg.MaterializeResult(
            value="complete",
            metadata={
                FERC_TO_SQLITE_METADATA_KEY: dg.MetadataValue.json(
                    provenance.model_dump(mode="json")
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
