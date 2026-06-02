"""Helpers for recording asset provenance and checking compatibility.

This module builds and interprets Dagster materialization metadata for assets so
downstream consumers can verify that the data they are using was created with compatible
inputs. Put provenance fingerprints, metadata builders, and compatibility checks here
when they describe the identity of a materialized asset, rather than the extraction
logic that produces the asset itself.

For the closest Dagster concept, see
https://docs.dagster.io/guides/build/assets/metadata-and-tags
"""

import json
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path
from typing import Literal

import dagster as dg
from pydantic import BaseModel
from upath import UPath

import pudl.logging_helpers
from pudl.settings import FercToSqliteDataConfig

logger = pudl.logging_helpers.get_logger(__name__)
FERC_TO_SQLITE_METADATA_KEY = "ferc_to_sqlite"


def _get_ferc_to_sqlite_asset_key(dataset: str, data_format: str) -> dg.AssetKey:
    """Return the asset key corresponding to a ferc_to_sqlite asset from dataset/format."""
    return dg.AssetKey(f"raw_{dataset}_{data_format}__sqlite")


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
        return _get_ferc_to_sqlite_asset_key(self.dataset, self.data_format)


class FercSqliteProvenanceRecord(BaseModel):
    """Stored provenance + extra debugging fields from materialization time."""

    dataset: str
    data_format: Literal["dbf", "xbrl"]
    status: Literal["complete", "not_configured"]
    zenodo_doi: str | None = None
    years: list[int] | None = None
    data_config: FercToSqliteDataConfig | None = None
    ferc_xbrl_extractor_version: str | None = None

    @classmethod
    def from_dagster_instance(
        cls,
        instance: dg.DagsterInstance,
        dataset: str,
        data_format: str,
    ) -> "FercSqliteProvenanceRecord":
        """Return FercSqliteProvenanceRecord from dagster metadata if available.

        Raises:
            RuntimeError: if no Dagster provenance metadata is available.
        """
        asset_key = _get_ferc_to_sqlite_asset_key(dataset, data_format)
        event = instance.get_latest_materialization_event(asset_key)
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
                f"{asset_key.to_user_string()}. Refresh the FERC SQLite assets."
            )

        return cls(**payload)

    def to_datapackage(self, datapackage_path: Path):
        """Write Provenance data to datapackage JSON file."""
        json_dict = json.loads(datapackage_path.read_text())
        json_dict["provenance_metadata"] = self.model_dump(mode="json")
        datapackage_path.write_text(json.dumps(json_dict, indent=2))

    @classmethod
    def from_datapackage(cls, datapackage_path: UPath) -> "FercSqliteProvenanceRecord":
        """Read SQLite provenance metadata from datapackage JSON file.

        Note that this method accepts ``datapackage_path`` as a ``UPath`` as we read
        provenance metadata directly from nightly builds, but ``to_datapackage`` only
        accepts a regular ``Path``, as we should never try to write directly to s3.
        """
        if datapackage_path.exists():
            json_dict = json.loads(datapackage_path.read_text())
            if (provenance := json_dict.get("provenance_metadata", None)) is not None:
                return cls.model_validate(provenance)
            # Handle legacy datapackages that didn't contain
            logger.warning(f"{datapackage_path} does not contain provenance metadata.")
        else:
            # Handle missing datapackage, which typically means you haven't run ferc_sqlite yet
            logger.warning(f"{datapackage_path} does not exist.")

        # If we get here that means we couldn't find provenance metadata
        return None


def get_xbrl_extractor_version() -> str:
    """Return the installed version of ``catalystcoop.ferc_xbrl_extractor``."""
    return version("catalystcoop.ferc_xbrl_extractor")


def ferc_sqlite_provenance_is_compatible(
    *,
    observed_provenance: FercSqliteProvenanceRecord | None,
    required_provenance: FercSqliteProvenance,
) -> bool:
    """Ensure a persisted FERC SQLite prerequisite is compatible with this run.

    Compatibility requires three conditions to hold:

    1. The Zenodo DOI recorded when the FERC SQLite DB was built must match the
       current :class:`~pudl.workspace.datastore.ZenodoDoiSettings`. A mismatch
       means the raw archive has changed version and the DB must be rebuilt.

    2. The years stored in the FERC SQLite DB must be a *superset* of the years
       needed by the current downstream data config. This allows a "full" FERC SQLite DB
       to serve a "fast" downstream run without an expensive rebuild.
    3. The version of ``ferc_xbrl_extractor`` is the same for XBRL derived data.
    """
    if observed_provenance is None:
        logger.warning(
            "No observed provenance provided. This usually indicates that a datapackage was "
            "created before the provenance metadata feature was added, or that the datapackage "
            "does not exist locally."
        )
        return False
    if observed_provenance.status == "not_configured":
        logger.warning(
            f"Stored provenance metadata for {required_provenance.asset_key.to_user_string()} has "
            f"status={observed_provenance.status!r}: the DB was built from a run that had no years "
            "configured for this form. Refresh the FERC SQLite assets with years configured."
        )
        return False
    if observed_provenance.status != "complete":
        logger.warning(
            f"Stored provenance metadata for {required_provenance.asset_key.to_user_string()} has "
            f"status={observed_provenance.status!r}. Refresh the FERC SQLite assets."
        )
        return False

    if observed_provenance.zenodo_doi is None or observed_provenance.years is None:
        raise RuntimeError(
            f"Stored provenance metadata for {required_provenance.asset_key.to_user_string()} is "
            "missing zenodo_doi or years. The DB may have been built before provenance "
            "tracking was added. Refresh the FERC SQLite assets."
        )

    mismatches: list[str] = []
    if observed_provenance.zenodo_doi != required_provenance.zenodo_doi:
        mismatches.append(
            "Zenodo DOI mismatch: "
            f"stored={observed_provenance.zenodo_doi!r}, "
            f"expected={required_provenance.zenodo_doi!r}"
        )

    stored_years: set[int] = set(observed_provenance.years)
    required_years: set[int] = set(required_provenance.years)
    missing_years: set[int] = required_years - stored_years
    if missing_years:
        mismatches.append(
            "FERC SQLite DB is missing required years: "
            f"missing={sorted(missing_years)}, "
            f"stored={sorted(stored_years)}, "
            f"required={sorted(required_years)}"
        )

    if (
        observed_provenance.ferc_xbrl_extractor_version
        != required_provenance.ferc_xbrl_extractor_version
    ) and (required_provenance.data_format == "xbrl"):
        mismatches.append(
            "FERC SQLite DB created with incompatible version of the XBRL extractor: "
            f"stored={observed_provenance.ferc_xbrl_extractor_version}, "
            f"required={required_provenance.ferc_xbrl_extractor_version}"
        )

    if mismatches:
        mismatch_summary: str = "; ".join(mismatches)
        logger.warning(
            f"Stored prerequisite asset {required_provenance.asset_key.to_user_string()} is not "
            f"compatible with the current run configuration. {mismatch_summary}. "
        )
        return False
    return True
