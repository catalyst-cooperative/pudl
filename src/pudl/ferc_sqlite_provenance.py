"""Helpers for recording FERC SQLite provenance and checking compatibility.

Compatibility between a persisted FERC SQLite prerequisite and a downstream
PUDL run is determined by two independent criteria:

1. **Zenodo DOI** — the DOI embedded in the materialization metadata must
   exactly match the DOI in the current :class:`~pudl.workspace.datastore.ZenodoDoiSettings`.
   A different DOI means the raw input data is a different archival version.

2. **Years coverage** — the set of years stored in the FERC SQLite DB (recorded
   when it was built) must be a *superset* of the years requested by the current
   downstream settings.  This lets the PUDL ETL run in a "fast" profile (a
   couple of years) against a "full" FERC SQLite DB without requiring an
   expensive rebuild, as long as the DOI matches.
"""

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import dagster as dg

from pudl.settings import EtlSettings
from pudl.workspace.datastore import ZenodoDoiSettings

PROVENANCE_METADATA_DATASET = "pudl_ferc_sqlite_dataset"
PROVENANCE_METADATA_STATUS = "pudl_ferc_sqlite_status"
PROVENANCE_METADATA_ZENODO_DOI = "pudl_ferc_sqlite_zenodo_doi"
PROVENANCE_METADATA_SETTINGS = "pudl_ferc_sqlite_etl_settings"
PROVENANCE_METADATA_YEARS = "pudl_ferc_sqlite_years"
PROVENANCE_METADATA_SQLITE_PATH = "pudl_ferc_sqlite_path"


@dataclass(frozen=True)
class FercSqliteProvenance:
    """Current provenance expectations for a FERC SQLite prerequisite asset."""

    asset_key: dg.AssetKey
    dataset: str
    data_format: str
    zenodo_doi: str
    years: list[int]


def _get_dataset_and_format(db_name: str) -> tuple[str, str]:
    match = re.search(r"ferc\d+", db_name)
    if match is None:
        raise ValueError(f"Could not determine FERC dataset from db_name={db_name!r}.")

    dataset = match.group()
    if db_name.endswith("_dbf"):
        return dataset, "dbf"
    if db_name.endswith("_xbrl"):
        return dataset, "xbrl"

    raise ValueError(
        f"Could not determine FERC sqlite format from db_name={db_name!r}."
    )


def get_ferc_sqlite_provenance(
    *,
    db_name: str,
    etl_settings: EtlSettings,
    zenodo_dois: ZenodoDoiSettings,
) -> "FercSqliteProvenance":
    """Build the expected provenance fingerprint for a FERC SQLite database."""
    dataset, data_format = _get_dataset_and_format(db_name)
    settings_attr = f"{dataset}_{data_format}_to_sqlite_settings"
    settings = getattr(etl_settings.ferc_to_sqlite, settings_attr)
    if settings is None:
        raise ValueError(f"Missing {settings_attr} in ETL settings.")

    return FercSqliteProvenance(
        asset_key=dg.AssetKey(f"raw_{db_name}__sqlite"),
        dataset=dataset,
        data_format=data_format,
        zenodo_doi=str(getattr(zenodo_dois, dataset)),
        years=sorted(settings.years),
    )


def build_ferc_sqlite_provenance_metadata(
    *,
    db_name: str,
    etl_settings: EtlSettings,
    zenodo_dois: ZenodoDoiSettings,
    sqlite_path: Path | None,
    status: str,
) -> dict[str, Any]:
    """Build materialization metadata for a FERC SQLite prerequisite asset."""
    provenance = get_ferc_sqlite_provenance(
        db_name=db_name,
        etl_settings=etl_settings,
        zenodo_dois=zenodo_dois,
    )

    # Serialize the full settings for debugging / audit; not used in compatibility checks.
    dataset, data_format = _get_dataset_and_format(db_name)
    settings_attr = f"{dataset}_{data_format}_to_sqlite_settings"
    settings_obj = getattr(etl_settings.ferc_to_sqlite, settings_attr)
    settings_json = json.loads(
        json.dumps(settings_obj.model_dump(mode="json"), sort_keys=True)
    )

    metadata: dict[str, Any] = {
        PROVENANCE_METADATA_DATASET: dg.MetadataValue.text(provenance.dataset),
        PROVENANCE_METADATA_STATUS: dg.MetadataValue.text(status),
        PROVENANCE_METADATA_ZENODO_DOI: dg.MetadataValue.text(provenance.zenodo_doi),
        PROVENANCE_METADATA_SETTINGS: dg.MetadataValue.json(settings_json),
        PROVENANCE_METADATA_YEARS: dg.MetadataValue.json(provenance.years),
    }
    if sqlite_path is not None:
        metadata[PROVENANCE_METADATA_SQLITE_PATH] = dg.MetadataValue.path(
            str(sqlite_path)
        )

    return metadata


def _unwrap_metadata_value(value: Any) -> Any:
    return value.value if hasattr(value, "value") else value


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
    """
    if instance is None:
        return

    provenance = get_ferc_sqlite_provenance(
        db_name=db_name,
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

    metadata = {
        key: _unwrap_metadata_value(value)
        for key, value in (materialization.metadata or {}).items()
    }
    required_keys = [
        PROVENANCE_METADATA_STATUS,
        PROVENANCE_METADATA_ZENODO_DOI,
        PROVENANCE_METADATA_YEARS,
    ]
    missing_keys = [key for key in required_keys if key not in metadata]
    if missing_keys:
        missing_keys_str = ", ".join(sorted(missing_keys))
        raise RuntimeError(
            f"Stored provenance metadata for {provenance.asset_key.to_user_string()} is "
            f"missing {missing_keys_str}. Refresh the FERC SQLite assets."
        )

    if metadata[PROVENANCE_METADATA_STATUS] != "complete":
        raise RuntimeError(
            f"Stored provenance metadata for {provenance.asset_key.to_user_string()} has "
            f"status={metadata[PROVENANCE_METADATA_STATUS]!r}. "
            "Refresh the FERC SQLite assets."
        )

    mismatches = []
    if metadata[PROVENANCE_METADATA_ZENODO_DOI] != provenance.zenodo_doi:
        mismatches.append(
            "Zenodo DOI mismatch: "
            f"stored={metadata[PROVENANCE_METADATA_ZENODO_DOI]!r}, "
            f"expected={provenance.zenodo_doi!r}"
        )

    stored_years = set(metadata[PROVENANCE_METADATA_YEARS])
    required_years = set(provenance.years)
    missing_years = required_years - stored_years
    if missing_years:
        mismatches.append(
            "FERC SQLite DB is missing required years: "
            f"missing={sorted(missing_years)}, "
            f"stored={sorted(stored_years)}, "
            f"required={sorted(required_years)}"
        )

    if mismatches:
        mismatch_summary = "; ".join(mismatches)
        raise RuntimeError(
            f"Stored prerequisite asset {provenance.asset_key.to_user_string()} is not "
            f"compatible with the current run configuration. {mismatch_summary}. "
            "Refresh the FERC SQLite assets."
        )
