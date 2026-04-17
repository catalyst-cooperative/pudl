"""Helpers for recording FERC SQLite provenance and checking compatibility.

Compatibility between a persisted FERC SQLite prerequisite and a downstream
PUDL run is determined by two independent criteria: do the Zenodo DOIs match,
and does the FERC SQLite DB contain all the years needed by the downstream run?
"""

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar, Literal

import dagster as dg
from pydantic import BaseModel

import pudl
from pudl.settings import EtlSettings
from pudl.workspace.datastore import ZenodoDoiSettings

logger = pudl.logging_helpers.get_logger(__name__)


def _parse_db_name(db_name: str) -> tuple[str, str]:
    """Parse a FERC SQLite db_name like 'ferc1_dbf' into (dataset, data_format)."""
    match: re.Match[str] | None = re.search(r"ferc\d+", db_name)
    if match is None:
        raise ValueError(f"Could not determine FERC dataset from db_name={db_name!r}.")

    dataset: str = match.group()
    if db_name.endswith("_dbf"):
        return dataset, "dbf"
    if db_name.endswith("_xbrl"):
        return dataset, "xbrl"

    raise ValueError(
        f"Could not determine FERC sqlite format from db_name={db_name!r}."
    )


@dataclass(frozen=True)
class FercSQLiteProvenance:
    """The provenance requirements derived from the current run's ETL settings.

    Computed from ``etl_settings`` and ``zenodo_dois`` to describe what a
    compatible FERC SQLite prerequisite must contain. Used by
    :func:`assert_ferc_sqlite_compatible` to compare against the stored
    :class:`FercSQLiteProvenanceRecord` that was written when the DB was built.
    """

    asset_key: dg.AssetKey
    dataset: str
    data_format: str
    zenodo_doi: str
    years: list[int]

    @classmethod
    def from_dataset_and_format(
        cls,
        *,
        dataset: str,
        data_format: str,
        etl_settings: EtlSettings,
        zenodo_dois: ZenodoDoiSettings,
    ) -> "FercSQLiteProvenance":
        """Build a provenance fingerprint from explicit dataset and format names."""
        settings_attr = f"{dataset}_{data_format}_to_sqlite_settings"
        settings = etl_settings.ferc_to_sqlite.model_dump(mode="json")[settings_attr]
        if settings is None:
            raise ValueError(f"Missing {settings_attr} in ETL settings.")
        return cls(
            asset_key=dg.AssetKey(f"raw_{dataset}_{data_format}__sqlite"),
            dataset=dataset,
            data_format=data_format,
            zenodo_doi=str(zenodo_dois.model_dump()[dataset]),
            years=sorted(settings["years"]),
        )


class FercSQLiteProvenanceRecord(BaseModel):
    """The provenance data recorded when a FERC SQLite prerequisite was materialized.

    Written to Dagster materialization metadata when a raw FERC SQLite asset is
    built. :func:`assert_ferc_sqlite_compatible` reads it back and compares it
    against :class:`FercSQLiteProvenance` (the current run's requirements) to
    decide whether the stored DB can be reused or must be rebuilt.

    For assets whose extraction was skipped (e.g. an XBRL form with no
    configured years), only ``dataset`` and ``status`` are populated.

    Pydantic is used here rather than a plain dataclass so that the
    ``status: Literal[...]`` constraint is enforced at construction time, not
    just by the type checker.
    """

    # Dagster materialization metadata keys -- stable across runs.
    _dataset: ClassVar[str] = "pudl_ferc_sqlite_dataset"
    _status: ClassVar[str] = "pudl_ferc_sqlite_status"
    _zenodo_doi: ClassVar[str] = "pudl_ferc_sqlite_zenodo_doi"
    _settings: ClassVar[str] = "pudl_ferc_sqlite_etl_settings"
    _years: ClassVar[str] = "pudl_ferc_sqlite_years"
    _path: ClassVar[str] = "pudl_ferc_sqlite_path"

    dataset: str
    status: Literal["complete", "skipped", "not_configured"]
    zenodo_doi: str | None = None
    years: list[int] | None = None
    settings_json: dict[str, Any] | None = None
    sqlite_path: Path | None = None

    def to_dagster_metadata(self) -> dict[str, dg.MetadataValue]:
        """Serialize to a Dagster metadata dict suitable for MaterializeResult."""
        metadata: dict[str, dg.MetadataValue] = {
            self._dataset: dg.MetadataValue.text(self.dataset),
            self._status: dg.MetadataValue.text(self.status),
        }
        if self.zenodo_doi is not None:
            metadata[self._zenodo_doi] = dg.MetadataValue.text(self.zenodo_doi)
        if self.years is not None:
            metadata[self._years] = dg.MetadataValue.json(self.years)
        if self.settings_json is not None:
            metadata[self._settings] = dg.MetadataValue.json(self.settings_json)
        if self.sqlite_path is not None:
            metadata[self._path] = dg.MetadataValue.path(str(self.sqlite_path))
        return metadata

    @classmethod
    def from_dagster_metadata(
        cls, metadata: dict[str, Any]
    ) -> "FercSQLiteProvenanceRecord":
        """Reconstruct a provenance record from stored Dagster materialization metadata."""

        def _unwrap(value: Any) -> Any:
            return value.value if hasattr(value, "value") else value

        return cls(
            dataset=_unwrap(metadata.get(cls._dataset, "")),
            status=_unwrap(metadata.get(cls._status, "skipped")),
            zenodo_doi=_unwrap(metadata[cls._zenodo_doi])
            if cls._zenodo_doi in metadata
            else None,
            years=_unwrap(metadata[cls._years]) if cls._years in metadata else None,
            settings_json=_unwrap(metadata[cls._settings])
            if cls._settings in metadata
            else None,
            sqlite_path=Path(_unwrap(metadata[cls._path]))
            if cls._path in metadata
            else None,
        )


def build_ferc_sqlite_provenance_metadata(
    *,
    dataset: str,
    data_format: str,
    etl_settings: EtlSettings,
    zenodo_dois: ZenodoDoiSettings,
    sqlite_path: Path | None,
    status: Literal["complete", "skipped", "not_configured"],
) -> FercSQLiteProvenanceRecord:
    """Build a provenance record for a FERC SQLite prerequisite asset.

    Args:
        dataset: FERC dataset name, e.g. ``"ferc1"`` or ``"ferc714"``.
        data_format: Source format, either ``"dbf"`` or ``"xbrl"``.
        etl_settings: Full ETL settings for the current run.
        zenodo_dois: Current Zenodo DOI settings.
        sqlite_path: Path to the written SQLite file, if available.
        status: ``"complete"`` if extraction ran; ``"skipped"`` if bypassed.

    Returns:
        A :class:`FercSQLiteProvenanceRecord` ready to be serialized via
        :meth:`~FercSQLiteProvenanceRecord.to_dagster_metadata`.
    """
    settings_attr = f"{dataset}_{data_format}_to_sqlite_settings"
    settings = etl_settings.ferc_to_sqlite.model_dump(mode="json")[settings_attr]
    if settings is None:
        raise ValueError(f"Missing {settings_attr} in ETL settings.")

    return FercSQLiteProvenanceRecord(
        dataset=dataset,
        status=status,
        zenodo_doi=str(zenodo_dois.model_dump()[dataset]),
        years=sorted(settings["years"]),
        settings_json=settings,
        sqlite_path=sqlite_path,
    )


def assert_ferc_sqlite_compatible(
    *,
    instance: Any | None,
    db_name: str,
    etl_settings: EtlSettings,
    zenodo_dois: ZenodoDoiSettings,
) -> None:
    """Ensure a persisted FERC SQLite prerequisite is compatible with this run.

    Compatibility requires two conditions to hold:

    1. The Zenodo DOI recorded when the FERC SQLite DB was built must match the
       current :class:`~pudl.workspace.datastore.ZenodoDoiSettings`. A mismatch
       means the raw archive has changed version and the DB must be rebuilt.

    2. The years stored in the FERC SQLite DB must be a *superset* of the years
       needed by the current downstream settings. This allows a "full" FERC SQLite
       DB to serve a "fast" downstream run without an expensive rebuild.

    The check is skipped (with a warning) in two cases:

    * No Dagster instance is available (running outside a Dagster execution context).
    * The environment variable ``PUDL_SKIP_FERC_SQLITE_PROVENANCE`` is set to a
      truthy value (``1``, ``true``, or ``yes``). This allows contributors to use
      externally-downloaded FERC databases without triggering a provenance error.
    """
    skip_env = os.environ.get("PUDL_SKIP_FERC_SQLITE_PROVENANCE", "").strip().lower()
    if skip_env in {"1", "true", "yes"}:
        logger.warning(
            "PUDL_SKIP_FERC_SQLITE_PROVENANCE is set: skipping FERC SQLite provenance "
            "check for %s. Stale or incompatible prerequisites may cause downstream "
            "failures.",
            db_name,
        )
        return

    if instance is None:
        logger.warning(
            "No Dagster instance is available; skipping FERC SQLite provenance check "
            "for %s. This is expected when running assets outside a Dagster execution "
            "context (e.g. in unit tests).",
            db_name,
        )
        return

    dataset, data_format = _parse_db_name(db_name)
    provenance: FercSQLiteProvenance = FercSQLiteProvenance.from_dataset_and_format(
        dataset=dataset,
        data_format=data_format,
        etl_settings=etl_settings,
        zenodo_dois=zenodo_dois,
    )
    event = instance.get_latest_materialization_event(provenance.asset_key)
    materialization = None if event is None else event.asset_materialization
    if materialization is None:
        raise RuntimeError(
            "No Dagster provenance metadata is available for "
            f"{provenance.asset_key.to_user_string()}. Refresh the FERC SQLite assets."
        )

    stored: FercSQLiteProvenanceRecord = (
        FercSQLiteProvenanceRecord.from_dagster_metadata(materialization.metadata or {})
    )

    if stored.status == "not_configured":
        raise RuntimeError(
            f"Stored provenance metadata for {provenance.asset_key.to_user_string()} has "
            f"status={stored.status!r}: the DB was built from a run that had no years "
            "configured for this form. Refresh the FERC SQLite assets with years configured."
        )
    if stored.status != "complete":
        raise RuntimeError(
            f"Stored provenance metadata for {provenance.asset_key.to_user_string()} has "
            f"status={stored.status!r}. Refresh the FERC SQLite assets."
        )

    if stored.zenodo_doi is None or stored.years is None:
        raise RuntimeError(
            f"Stored provenance metadata for {provenance.asset_key.to_user_string()} is "
            "missing zenodo_doi or years. The DB may have been built before provenance "
            "tracking was added. Refresh the FERC SQLite assets."
        )

    mismatches: list[str] = []
    if stored.zenodo_doi != provenance.zenodo_doi:
        mismatches.append(
            "Zenodo DOI mismatch: "
            f"stored={stored.zenodo_doi!r}, "
            f"expected={provenance.zenodo_doi!r}"
        )

    stored_years: set[int] = set(stored.years)
    required_years: set[int] = set(provenance.years)
    missing_years: set[int] = required_years - stored_years
    if missing_years:
        mismatches.append(
            "FERC SQLite DB is missing required years: "
            f"missing={sorted(missing_years)}, "
            f"stored={sorted(stored_years)}, "
            f"required={sorted(required_years)}"
        )

    if mismatches:
        mismatch_summary: str = "; ".join(mismatches)
        raise RuntimeError(
            f"Stored prerequisite asset {provenance.asset_key.to_user_string()} is not "
            f"compatible with the current run configuration. {mismatch_summary}. "
            "Refresh the FERC SQLite assets."
        )
