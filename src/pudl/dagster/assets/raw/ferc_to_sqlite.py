"""Dagster asset definitions for granular FERC-to-SQLite extraction.

This module defines the prerequisite assets that build the FERC DBF and XBRL derived
SQLite databases used elsewhere in the PUDL pipeline. It should contain asset factories,
resource requirements, and materialization metadata specific to those prerequisite
databases, rather than the downstream transforms that consume them.
"""

import os
from collections.abc import Callable
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
    ferc_sqlite_provenance_is_compatible,
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


def _check_for_cached_db_w_compatible_provenance(
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

    Returns:
        Compatible ``FercSqliteProvenanceRecord`` if one is found, otherwise ``None``.
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
    compatible_metadata = None

    # Check local DB first
    local_provenance = FercSqliteProvenanceRecord.from_sqlite(sqlite_path)

    # Check if local or nightly dbs contain compatible provenance metadata
    if ferc_sqlite_provenance_is_compatible(
        required_provenance=provenance, observed_provenance=local_provenance
    ):
        compatible_metadata = local_provenance
    else:
        logger.info(
            f"Provenance metadata for local version of {sqlite_path.name} is incompatible."
            " Downloading version from nightly builds."
        )
        _download_nightly_db(sqlite_path)
        nightly_provenance = FercSqliteProvenanceRecord.from_sqlite(sqlite_path)
        if ferc_sqlite_provenance_is_compatible(
            required_provenance=provenance, observed_provenance=nightly_provenance
        ):
            compatible_metadata = nightly_provenance

    if compatible_metadata is None:
        logger.info(
            f"Can't find a cached version of {sqlite_path.name} with compatible provenance metadata."
            " Extracting from scratch."
        )
    return compatible_metadata


def ferc_to_sqlite_asset_factory(
    *,
    dataset: str,
    data_format: str,
    extract_function: Callable[[dg.AssetExecutionContext], None],
    op_tags: dict | None = None,
) -> dg.AssetsDefinition:
    """Create a FERC-to-SQLite prerequisite asset for a specific FERC dataset."""

    @dg.asset(
        key=f"raw_{dataset}_{data_format}__sqlite",
        group_name="raw_ferc_to_sqlite",
        required_resource_keys={
            "global_data_config",
            "datastore",
            "runtime_settings",
            "zenodo_dois",
        },
        tags={"dataset": dataset, "data_format": data_format},
        op_tags=op_tags,
    )
    def _asset(context) -> dg.MaterializeResult[str]:
        ferc_to_sqlite = context.resources.global_data_config.ferc_to_sqlite
        data_config = ferc_to_sqlite.get_data_config(
            dataset=dataset, data_format=data_format
        )
        zenodo_doi = context.resources.zenodo_dois.get_doi(dataset)
        if data_config is None or not data_config.years:
            logger.info(
                f"No years configured for {dataset}_{data_format}: skipping extraction."
            )
            return dg.MaterializeResult(
                value="not_configured",
                metadata={
                    FERC_TO_SQLITE_METADATA_KEY: dg.MetadataValue.json(
                        FercSqliteProvenanceRecord(
                            dataset=dataset,
                            data_format=data_format,
                            status="not_configured",
                        ).model_dump(mode="json")
                    )
                },
            )

        sqlite_path = PudlPaths().sqlite_db_path(f"{dataset}_{data_format}")

        # Check if there's a cached SQLite DB that is compatible
        if (
            provenance := _check_for_cached_db_w_compatible_provenance(
                dataset=dataset,
                data_format=data_format,
                zenodo_doi=zenodo_doi,
                sqlite_path=sqlite_path,
                ferc_to_sqlite=ferc_to_sqlite,
            )
        ) is None:
            extract_function(context)

            provenance = FercSqliteProvenanceRecord(
                dataset=dataset,
                data_format=data_format,
                status="complete",
                zenodo_doi=zenodo_doi,
                years=ferc_to_sqlite.get_dataset_years(
                    dataset=dataset, data_format=data_format
                ),
                data_config=ferc_to_sqlite,
                ferc_xbrl_extractor_version=get_xbrl_extractor_version(),
            )
            provenance.to_sqlite(sqlite_path)
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


raw_ferc1_dbf__sqlite = ferc_to_sqlite_asset_factory(
    dataset="ferc1",
    data_format="dbf",
    extract_function=lambda context: Ferc1DbfExtractor(
        datastore=context.resources.datastore,
        data_config=context.resources.global_data_config.ferc_to_sqlite,
        output_path=PudlPaths().output_dir,
    ).execute(),
    op_tags={"dagster/priority": 10},
)
raw_ferc2_dbf__sqlite = ferc_to_sqlite_asset_factory(
    dataset="ferc2",
    data_format="dbf",
    extract_function=lambda context: Ferc2DbfExtractor(
        datastore=context.resources.datastore,
        data_config=context.resources.global_data_config.ferc_to_sqlite,
        output_path=PudlPaths().output_dir,
    ).execute(),
    op_tags={"dagster/priority": 10},
)
raw_ferc6_dbf__sqlite = ferc_to_sqlite_asset_factory(
    dataset="ferc6",
    data_format="dbf",
    extract_function=lambda context: Ferc6DbfExtractor(
        datastore=context.resources.datastore,
        data_config=context.resources.global_data_config.ferc_to_sqlite,
        output_path=PudlPaths().output_dir,
    ).execute(),
    op_tags={"dagster/priority": 10},
)
raw_ferc60_dbf__sqlite = ferc_to_sqlite_asset_factory(
    dataset="ferc60",
    data_format="dbf",
    extract_function=lambda context: Ferc60DbfExtractor(
        datastore=context.resources.datastore,
        data_config=context.resources.global_data_config.ferc_to_sqlite,
        output_path=PudlPaths().output_dir,
    ).execute(),
    op_tags={"dagster/priority": 10},
)

raw_ferc1_xbrl__sqlite = ferc_to_sqlite_asset_factory(
    dataset="ferc1",
    data_format="xbrl",
    extract_function=lambda context: convert_form(
        ferc_to_sqlite=context.resources.global_data_config.ferc_to_sqlite,
        form=XbrlFormNumber.FORM1,
        datastore=FercXbrlDatastore(context.resources.datastore),
        batch_size=context.resources.runtime_settings.xbrl_batch_size,
        workers=context.resources.runtime_settings.xbrl_num_workers,
        loglevel=context.resources.runtime_settings.xbrl_loglevel,
    ),
    op_tags={"dagster/priority": 10},
)
raw_ferc2_xbrl__sqlite = ferc_to_sqlite_asset_factory(
    dataset="ferc2",
    data_format="xbrl",
    extract_function=lambda context: convert_form(
        ferc_to_sqlite=context.resources.global_data_config.ferc_to_sqlite,
        form=XbrlFormNumber.FORM2,
        datastore=FercXbrlDatastore(context.resources.datastore),
        batch_size=context.resources.runtime_settings.xbrl_batch_size,
        workers=context.resources.runtime_settings.xbrl_num_workers,
        loglevel=context.resources.runtime_settings.xbrl_loglevel,
    ),
    op_tags={"dagster/priority": 10},
)
raw_ferc6_xbrl__sqlite = ferc_to_sqlite_asset_factory(
    dataset="ferc6",
    data_format="xbrl",
    extract_function=lambda context: convert_form(
        ferc_to_sqlite=context.resources.global_data_config.ferc_to_sqlite,
        form=XbrlFormNumber.FORM6,
        datastore=FercXbrlDatastore(context.resources.datastore),
        batch_size=context.resources.runtime_settings.xbrl_batch_size,
        workers=context.resources.runtime_settings.xbrl_num_workers,
        loglevel=context.resources.runtime_settings.xbrl_loglevel,
    ),
    op_tags={"dagster/priority": 10},
)
raw_ferc60_xbrl__sqlite = ferc_to_sqlite_asset_factory(
    dataset="ferc60",
    data_format="xbrl",
    extract_function=lambda context: convert_form(
        ferc_to_sqlite=context.resources.global_data_config.ferc_to_sqlite,
        form=XbrlFormNumber.FORM60,
        datastore=FercXbrlDatastore(context.resources.datastore),
        batch_size=context.resources.runtime_settings.xbrl_batch_size,
        workers=context.resources.runtime_settings.xbrl_num_workers,
        loglevel=context.resources.runtime_settings.xbrl_loglevel,
    ),
    op_tags={"dagster/priority": 10},
)
raw_ferc714_xbrl__sqlite = ferc_to_sqlite_asset_factory(
    dataset="ferc714",
    data_format="xbrl",
    extract_function=lambda context: convert_form(
        ferc_to_sqlite=context.resources.global_data_config.ferc_to_sqlite,
        form=XbrlFormNumber.FORM714,
        datastore=FercXbrlDatastore(context.resources.datastore),
        batch_size=context.resources.runtime_settings.xbrl_batch_size,
        workers=context.resources.runtime_settings.xbrl_num_workers,
        loglevel=context.resources.runtime_settings.xbrl_loglevel,
    ),
    op_tags={"dagster/priority": 10},
)
