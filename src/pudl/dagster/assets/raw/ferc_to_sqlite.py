"""Dagster asset definitions for granular FERC-to-SQLite extraction.

This module defines the prerequisite assets that build the FERC DBF and XBRL derived
SQLite databases used elsewhere in the PUDL pipeline. It should contain asset factories,
resource requirements, and materialization metadata specific to those prerequisite
databases, rather than the downstream transforms that consume them.
"""

from collections.abc import Callable
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Literal
from zipfile import ZipFile

import dagster as dg
from botocore.exceptions import (
    ConnectTimeoutError,
    EndpointConnectionError,
    ReadTimeoutError,
)
from upath import UPath

import pudl.logging_helpers
from pudl import PUDL_EEL_HOLE_BASE_PATH
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
from pudl.helpers import env_var_is_true
from pudl.settings import FercForm, FercToSqliteDataConfig
from pudl.workspace.setup import PudlPaths

NETWORK_ERRORS = (
    TimeoutError,
    ConnectionError,
    ConnectTimeoutError,
    EndpointConnectionError,
    ReadTimeoutError,
)

logger = pudl.logging_helpers.get_logger(__name__)


@dataclass
class FercPaths:
    """Helper class to get paths to various FERC paths both local and remote."""

    # Store data_format so we can use it in ``delete_local_outputs``.
    data_format: Literal["dbf", "xbrl"]
    # DBF and XBRL outputs
    local_datapackage_path: Path
    nightly_datapackage_path: UPath
    local_sqlite_path: Path
    nightly_sqlite_path: UPath
    # XBRL specific outputs
    local_duckdb_path: Path | None = None
    nightly_duckdb_path: UPath | None = None
    local_taxonomy_json_path: Path | None = None
    nightly_taxonomy_json_path: UPath | None = None
    local_parquet_dir_path: Path | None = None
    nightly_parquet_dir_path: UPath | None = None

    def delete_local_outputs(self):
        """Helper function to delete local outputs before starting extraction."""
        self.local_sqlite_path.unlink(missing_ok=True)
        self.local_datapackage_path.unlink(missing_ok=True)

        if self.data_format == "xbrl":
            self.local_duckdb_path.unlink(missing_ok=True)
            self.local_taxonomy_json_path.unlink(missing_ok=True)

            # Delete files in parquet dir
            if self.local_parquet_dir_path.exists():
                [path.unlink() for path in self.local_parquet_dir_path.iterdir()]

    @classmethod
    def from_dataset_format(
        cls, dataset: FercForm, data_format: Literal["dbf", "xbrl"], paths: PudlPaths
    ) -> "FercPaths":
        """Initialize class based on ``dataset`` and ``data_format``."""
        dataset_format = f"{dataset}_{data_format}"

        filenames = {
            "datapackage": f"{dataset_format}_datapackage.json",
            "sqlite": f"{dataset_format}.sqlite",
        }
        # XBRL has extra outputs
        if data_format == "xbrl":
            filenames |= {
                "duckdb": f"{dataset_format}.duckdb",
                "taxonomy_json": f"{dataset_format}_taxonomy_metadata.json",
                "parquet_dir": f"{dataset_format}",
            }

        # Generate local paths
        paths = {
            f"local_{key}_path": paths.pudl_output / name
            for key, name in filenames.items()
        }
        # Generate nightly paths
        paths |= {
            f"nightly_{key}_path": PUDL_EEL_HOLE_BASE_PATH / name
            # SQLite and parquet nightly build outputs are in zip files
            if key not in ["sqlite", "parquet_dir"]
            else PUDL_EEL_HOLE_BASE_PATH / f"{name}.zip"
            for key, name in filenames.items()
        }

        return cls(**paths, data_format=data_format)


def _download_zipped_outputs(
    paths: FercPaths, output_format: Literal["sqlite", "parquet"]
):
    """Download nightly zipfile containing sqlite or parquet outputs and extract to local cache."""
    if output_format == "sqlite":
        nightly_path = paths.nightly_sqlite_path
        local_path = paths.local_sqlite_path.parent
    else:
        nightly_path = paths.nightly_parquet_dir_path
        local_path = paths.local_parquet_dir_path

    local_path.mkdir(exist_ok=True)
    with ZipFile(BytesIO(nightly_path.read_bytes())) as archive:
        for member in archive.namelist():
            if not member.endswith(f".{output_format}"):
                continue
            filename = Path(member).name
            with archive.open(member) as f:
                (local_path / filename).write_bytes(f.read())


def _download_nightly_outputs(
    data_format: Literal["dbf", "xbrl"],
    paths: FercPaths,
) -> None:
    """Download ``ferc_to_sqlite`` outputs from s3.

    This will download all outputs produced by the ``ferc_to_sqlite`` process for the
    provided ``dataset`` and ``data_format``. For the 'DBF' format, this includes the
    SQLite db and a datapackage JSON file, while 'XBRL' will include both of these
    plus a DuckDB file, parquet files, and the taxonomy JSON file.
    """
    # Download sqlite DB
    _download_zipped_outputs(paths, output_format="sqlite")

    # Download datapckage JSON
    paths.local_datapackage_path.write_text(paths.nightly_datapackage_path.read_text())

    # DBF only produces sqlite and datapackage, so return
    if data_format == "dbf":
        return

    # Download taxonomy JSON
    paths.local_taxonomy_json_path.write_bytes(
        paths.nightly_taxonomy_json_path.read_bytes()
    )
    # Download duckdb DB / parquet
    paths.local_duckdb_path.write_bytes(paths.nightly_duckdb_path.read_bytes())
    _download_zipped_outputs(paths, output_format="parquet")


def _check_for_cached_db_w_compatible_provenance(
    dataset: FercForm,
    data_format: Literal["dbf", "xbrl"],
    zenodo_doi: str,
    paths: FercPaths,
    ferc_to_sqlite: FercToSqliteDataConfig,
) -> FercSqliteProvenanceRecord | None:
    """Check to see if there is a compatible outputs either locally, or in nightly builds.

    This function will first check the local datapackage for the specified ``dataset``
    and ``data_format`` to see if it contains a ``FercSqliteProvenanceRecord`` that
    is compatible with the requirements of the current run. If the local datapckage doesn't
    exist or contains an incompatible record, it will then download the datapackage produced
    by the most recent nightly build and perform the same check. If the nightly
    outputs are found to be compatible with the current run, then it will
    download all associated outputs from that run. For DBF outputs, this includes
    the SQLite file and the datapackage JSON file, while XBRL outputs also
    include a duckdb file, parquet files, and a taxonomy JSON file.

    If the environment variable, ``PUDL_FORCE_FERC_TO_SQLITE``, is set to ``true``, then
    this function will immediately return ``None``, triggering the normal extraction.

    Returns:
        Compatible ``FercSqliteProvenanceRecord`` if one is found, otherwise ``None``.
    """
    # Check if configured to force extraction
    if env_var_is_true("PUDL_FORCE_FERC_TO_SQLITE"):
        return None

    # Assemble required provenance for current run
    provenance = FercSqliteProvenance(
        dataset=str(dataset),
        data_format=data_format,
        zenodo_doi=zenodo_doi,
        years=ferc_to_sqlite.get_dataset_years(dataset, data_format),
        ferc_xbrl_extractor_version=get_xbrl_extractor_version(),
    )
    compatible_metadata = None

    # Check local datapackage first
    local_provenance = FercSqliteProvenanceRecord.from_datapackage(
        paths.local_datapackage_path, source="local_cache"
    )

    # Check if local or nightly datapackage contain compatible provenance metadata
    if ferc_sqlite_provenance_is_compatible(
        required_provenance=provenance, observed_provenance=local_provenance
    ):
        logger.info(
            f"Local outputs for {dataset}_{data_format} are compatible with current run."
        )
        return local_provenance

    # Don't try to use nightly outputs in integration tests
    # This is to avoid incompatibilities between fast / full outputs
    if env_var_is_true("PUDL_INTEGRATION_TESTS"):
        return None

    # Check nightly provenance
    try:
        nightly_provenance = FercSqliteProvenanceRecord.from_datapackage(
            paths.nightly_datapackage_path, source="nightly"
        )
    except NETWORK_ERRORS:
        logger.warning(
            f"Failed to download {dataset}_{data_format} datapackage to check provenance."
        )
        # This will cause ferc_sqlite_provenance_is_compatible to return False
        nightly_provenance = None

    if ferc_sqlite_provenance_is_compatible(
        required_provenance=provenance,
        observed_provenance=nightly_provenance,
    ):
        try:
            _download_nightly_outputs(
                data_format=data_format,
                paths=paths,
            )

            # At this point the local datapackage is overwritten by the nightly one
            # This means we can grab the nightly provenance metadata from the local file
            compatible_metadata = FercSqliteProvenanceRecord.from_datapackage(
                paths.local_datapackage_path, source="nightly"
            )
            logger.info(
                f"Nightly outputs for {dataset}_{data_format} are compatible with current run."
            )
        except NETWORK_ERRORS:
            logger.warning(
                f"Failed to download {dataset}_{data_format} outputs from"
                " nightly builds. See: \n{e}"
            )

    if compatible_metadata is None:
        logger.info(
            f"Can't find a cached version of {dataset}_{data_format} with compatible provenance metadata."
            " Extracting from scratch."
        )
    return compatible_metadata


def ferc_to_sqlite_asset_factory(
    *,
    dataset: FercForm,
    data_format: Literal["dbf", "xbrl"],
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
            "pudl_paths",
            "runtime_settings",
            "zenodo_dois",
        },
        tags={"dataset": str(dataset), "data_format": data_format},
        op_tags=op_tags,
    )
    def _asset(context) -> dg.MaterializeResult[str]:
        ferc_to_sqlite = context.resources.global_data_config.ferc_to_sqlite
        data_config = ferc_to_sqlite.get_data_config(
            dataset=dataset, data_format=data_format
        )
        zenodo_doi = context.resources.zenodo_dois.get_doi(str(dataset))
        pudl_paths = context.resources.pudl_paths
        ferc_paths = FercPaths.from_dataset_format(
            dataset=dataset, data_format=data_format, paths=pudl_paths
        )
        if data_config is None or not data_config.years:
            logger.info(
                f"No years configured for {dataset}_{data_format}: skipping extraction."
            )
            return dg.MaterializeResult(
                value="not_configured",
                metadata={
                    FERC_TO_SQLITE_METADATA_KEY: dg.MetadataValue.json(
                        FercSqliteProvenanceRecord(
                            dataset=str(dataset),
                            data_format=data_format,
                            status="not_configured",
                            source="local_new",
                        ).model_dump(mode="json")
                    )
                },
            )

        # Check if there's a cached SQLite DB that is compatible
        if (
            provenance := _check_for_cached_db_w_compatible_provenance(
                dataset=dataset,
                data_format=data_format,
                zenodo_doi=zenodo_doi,
                paths=ferc_paths,
                ferc_to_sqlite=ferc_to_sqlite,
            )
        ) is None:
            # Delete local outputs before starting extraction
            ferc_paths.delete_local_outputs()

            # Run extraction
            extract_function(context)

            provenance = FercSqliteProvenanceRecord(
                dataset=str(dataset),
                data_format=data_format,
                status="complete",
                source="local_new",
                zenodo_doi=zenodo_doi,
                years=ferc_to_sqlite.get_dataset_years(
                    dataset=dataset, data_format=data_format
                ),
                data_config=ferc_to_sqlite,
                ferc_xbrl_extractor_version=get_xbrl_extractor_version(),
            )
            provenance.to_datapackage(ferc_paths.local_datapackage_path)
        else:
            logger.info(
                f"Found compatible cached SQLite DB for {dataset}_{data_format}. Skipping extraction."
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
    dataset=FercForm.FORM1,
    data_format="dbf",
    extract_function=lambda context: Ferc1DbfExtractor(
        datastore=context.resources.datastore,
        data_config=context.resources.global_data_config.ferc_to_sqlite,
        output_path=context.resources.pudl_paths.pudl_output,
    ).execute(),
    op_tags={"dagster/priority": 10},
)
raw_ferc2_dbf__sqlite = ferc_to_sqlite_asset_factory(
    dataset=FercForm.FORM2,
    data_format="dbf",
    extract_function=lambda context: Ferc2DbfExtractor(
        datastore=context.resources.datastore,
        data_config=context.resources.global_data_config.ferc_to_sqlite,
        output_path=context.resources.pudl_paths.pudl_output,
    ).execute(),
    op_tags={"dagster/priority": 10},
)
raw_ferc6_dbf__sqlite = ferc_to_sqlite_asset_factory(
    dataset=FercForm.FORM6,
    data_format="dbf",
    extract_function=lambda context: Ferc6DbfExtractor(
        datastore=context.resources.datastore,
        data_config=context.resources.global_data_config.ferc_to_sqlite,
        output_path=context.resources.pudl_paths.pudl_output,
    ).execute(),
    op_tags={"dagster/priority": 10},
)
raw_ferc60_dbf__sqlite = ferc_to_sqlite_asset_factory(
    dataset=FercForm.FORM60,
    data_format="dbf",
    extract_function=lambda context: Ferc60DbfExtractor(
        datastore=context.resources.datastore,
        data_config=context.resources.global_data_config.ferc_to_sqlite,
        output_path=context.resources.pudl_paths.pudl_output,
    ).execute(),
    op_tags={"dagster/priority": 10},
)

raw_ferc1_xbrl__sqlite = ferc_to_sqlite_asset_factory(
    dataset=FercForm.FORM1,
    data_format="xbrl",
    extract_function=lambda context: convert_form(
        ferc_to_sqlite=context.resources.global_data_config.ferc_to_sqlite,
        form=FercForm.FORM1,
        datastore=FercXbrlDatastore(context.resources.datastore),
        pudl_paths=context.resources.pudl_paths,
        batch_size=context.resources.runtime_settings.xbrl_batch_size,
        workers=context.resources.runtime_settings.xbrl_num_workers,
        loglevel=context.resources.runtime_settings.xbrl_loglevel,
    ),
    op_tags={"dagster/priority": 10},
)
raw_ferc2_xbrl__sqlite = ferc_to_sqlite_asset_factory(
    dataset=FercForm.FORM2,
    data_format="xbrl",
    extract_function=lambda context: convert_form(
        ferc_to_sqlite=context.resources.global_data_config.ferc_to_sqlite,
        form=FercForm.FORM2,
        datastore=FercXbrlDatastore(context.resources.datastore),
        pudl_paths=context.resources.pudl_paths,
        batch_size=context.resources.runtime_settings.xbrl_batch_size,
        workers=context.resources.runtime_settings.xbrl_num_workers,
        loglevel=context.resources.runtime_settings.xbrl_loglevel,
    ),
    op_tags={"dagster/priority": 10},
)
raw_ferc6_xbrl__sqlite = ferc_to_sqlite_asset_factory(
    dataset=FercForm.FORM6,
    data_format="xbrl",
    extract_function=lambda context: convert_form(
        ferc_to_sqlite=context.resources.global_data_config.ferc_to_sqlite,
        form=FercForm.FORM6,
        datastore=FercXbrlDatastore(context.resources.datastore),
        pudl_paths=context.resources.pudl_paths,
        batch_size=context.resources.runtime_settings.xbrl_batch_size,
        workers=context.resources.runtime_settings.xbrl_num_workers,
        loglevel=context.resources.runtime_settings.xbrl_loglevel,
    ),
    op_tags={"dagster/priority": 10},
)
raw_ferc60_xbrl__sqlite = ferc_to_sqlite_asset_factory(
    dataset=FercForm.FORM60,
    data_format="xbrl",
    extract_function=lambda context: convert_form(
        ferc_to_sqlite=context.resources.global_data_config.ferc_to_sqlite,
        form=FercForm.FORM60,
        datastore=FercXbrlDatastore(context.resources.datastore),
        pudl_paths=context.resources.pudl_paths,
        batch_size=context.resources.runtime_settings.xbrl_batch_size,
        workers=context.resources.runtime_settings.xbrl_num_workers,
        loglevel=context.resources.runtime_settings.xbrl_loglevel,
    ),
    op_tags={"dagster/priority": 10},
)
raw_ferc714_xbrl__sqlite = ferc_to_sqlite_asset_factory(
    dataset=FercForm.FORM714,
    data_format="xbrl",
    extract_function=lambda context: convert_form(
        ferc_to_sqlite=context.resources.global_data_config.ferc_to_sqlite,
        form=FercForm.FORM714,
        datastore=FercXbrlDatastore(context.resources.datastore),
        pudl_paths=context.resources.pudl_paths,
        batch_size=context.resources.runtime_settings.xbrl_batch_size,
        workers=context.resources.runtime_settings.xbrl_num_workers,
        loglevel=context.resources.runtime_settings.xbrl_loglevel,
    ),
    op_tags={"dagster/priority": 10},
)
