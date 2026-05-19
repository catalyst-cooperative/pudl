"""Helpers for recording asset provenance and checking compatibility.

This module builds and interprets Dagster materialization metadata for assets so
downstream consumers can verify that the data they are using was created with compatible
inputs. Put provenance fingerprints, metadata builders, and compatibility checks here
when they describe the identity of a materialized asset, rather than the extraction
logic that produces the asset itself.

For the closest Dagster concept, see
https://docs.dagster.io/guides/build/assets/metadata-and-tags
"""

import sqlite3
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path
from typing import Any, Literal

import dagster as dg
import sqlalchemy as sa
from pydantic import BaseModel

import pudl.logging_helpers
from pudl.settings import FercToSqliteDataConfig

logger = pudl.logging_helpers.get_logger(__name__)
FERC_TO_SQLITE_METADATA_KEY = "ferc_to_sqlite"


@dataclass(frozen=True)
class FercSqliteProvenance:
    """The provenance requirements derived from the current run's data config.

    Computed from ``data_config`` and ``zenodo_dois`` to describe what a
    compatible FERC SQLite prerequisite must contain. Used by
    :func:`assert_ferc_sqlite_compatible` to compare against the stored
    :class:`FercSqliteProvenanceRecord` that was written when the DB was built.
    """

    dataset: str
    data_format: str
    zenodo_doi: str
    years: list[int]
    ferc_xbrl_extractor_version: str

    @property
    def asset_key(self) -> dg.AssetKey:
        """The AssetKey corresponding to the extracted SQLite database."""
        return dg.AssetKey(f"raw_{self.dataset}_{self.data_format}__sqlite")


class FercSqliteProvenanceRecord(BaseModel):
    """Stored provenance + extra debugging fields from materialization time."""

    dataset: str
    data_format: Literal["dbf", "xbrl"]
    status: Literal["complete", "skipped", "not_configured"]
    zenodo_doi: str | None = None
    years: list[int] | None = None
    data_config: FercToSqliteDataConfig | None = None
    sqlite_path: Path | None = None
    ferc_xbrl_extractor_version: str | None = None

    @classmethod
    def from_dagster_instance(
        cls,
        instance: Any | None,
        desired_provenance: FercSqliteProvenance,
    ) -> "FercSqliteProvenanceRecord":
        """Return FercSqliteProvenanceRecord from dagster metadata if available."""
        if instance is None:
            logger.warning(
                f"No Dagster instance is available; skipping FERC SQLite provenance "
                f"check for {desired_provenance.dataset}_{desired_provenance.data_format}. This is "
                "expected when running assets outside a Dagster execution context "
                "(e.g. in unit tests)."
            )
            return None

        event = instance.get_latest_materialization_event(desired_provenance.asset_key)
        materialization = None if event is None else event.asset_materialization
        raw_payload = (
            None
            if materialization is None
            else materialization.metadata.get(FERC_TO_SQLITE_METADATA_KEY)
        )
        payload = raw_payload.value if hasattr(raw_payload, "value") else raw_payload
        if not isinstance(payload, dict):
            raise RuntimeError(
                "No Dagster provenance metadata is available for "
                f"{desired_provenance.asset_key.to_user_string()}. Refresh the FERC SQLite assets."
            )

        return cls(**payload)

    def to_sqlite(self):
        """Write Provenance data to sqlite."""
        with sa.create_engine(f"sqlite:///{self.sqlite_path}").begin() as conn:
            conn.execute(
                sa.text("""
                CREATE TABLE IF NOT EXISTS _provenance_metadata (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    metadata TEXT NOT NULL
                )
            """)
            )

            conn.execute(
                sa.text(
                    "INSERT INTO _provenance_metadata (metadata) VALUES (:metadata)"
                ),
                {"metadata": self.model_dump_json()},
            )

    @classmethod
    def from_sqlite(cls, sqlite_path: Path) -> "FercSqliteProvenanceRecord":
        """Read SQLite provenance metadata from DB."""
        try:
            with sa.create_engine(f"sqlite:///{sqlite_path}").begin() as conn:
                row = conn.execute(
                    sa.text("SELECT metadata FROM _provenance_metadata WHERE id = 1")
                ).scalar_one()
        except (sa.exc.OperationalError, sqlite3.OperationalError):
            logger.warning(f"No provenance metadata available for {sqlite_path}.")
            return None

        return cls.model_validate_json(row)


def get_xbrl_extractor_version() -> str:
    """Return the installed version of ``catalystcoop.ferc_xbrl_extractor``."""
    return version("catalystcoop.ferc_xbrl_extractor")


def assert_ferc_sqlite_compatible(  # noqa: C901
    *,
    stored: FercSqliteProvenanceRecord,
    provenance: FercSqliteProvenance,
) -> None:
    """Ensure a persisted FERC SQLite prerequisite is compatible with this run.

    Compatibility requires two conditions to hold:

    1. The Zenodo DOI recorded when the FERC SQLite DB was built must match the
       current :class:`~pudl.workspace.datastore.ZenodoDoiSettings`. A mismatch
       means the raw archive has changed version and the DB must be rebuilt.

    2. The years stored in the FERC SQLite DB must be a *superset* of the years
       needed by the current downstream data config. This allows a "full" FERC SQLite DB
       to serve a "fast" downstream run without an expensive rebuild.
    """
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

    if stored.ferc_xbrl_extractor_version != provenance.ferc_xbrl_extractor_version:
        mismatches.append(
            "FERC SQLite DB created with incompatible version of the XBRL extractor: "
            f"stored={stored.ferc_xbrl_extractor_version}, "
            f"required={provenance.ferc_xbrl_extractor_version}"
        )

    if mismatches:
        mismatch_summary: str = "; ".join(mismatches)
        raise RuntimeError(
            f"Stored prerequisite asset {provenance.asset_key.to_user_string()} is not "
            f"compatible with the current run configuration. {mismatch_summary}. "
            "Refresh the FERC SQLite assets."
        )
