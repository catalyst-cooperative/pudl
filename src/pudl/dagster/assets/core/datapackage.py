"""Dagster asset that generates the PUDL frictionless datapackage descriptor.

The descriptor is written to ``$PUDL_OUTPUT/parquet/datapackage.json``, which
is the canonical frictionless datapackage filename.  It is enriched with
per-resource file statistics (bytes, SHA-256 hash), runtime provenance fields
(UUID ``id``, ``git_sha``, ``git_tags``), per-source Zenodo DOIs, and links to
the PUDL documentation page for each data source.
"""

import hashlib
import json
import re
import shutil
import subprocess
import uuid
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

import dagster as dg

import pudl.logging_helpers
from pudl import PUDL_ROOT_PATH
from pudl.metadata.classes import PUDL_PACKAGE
from pudl.workspace.datastore import ZenodoDoiSettings
from pudl.workspace.setup import PudlPaths

logger = pudl.logging_helpers.get_logger(__name__)

_FERCEQR_EXCLUDE_PATTERN = re.compile(r"^core_ferceqr")

# Discover which data sources have a dedicated PUDL docs page by scanning for
# *_child.rst.jinja templates.  The template filename prefix is the source name.
# If the docs/templates directory is not present (e.g. in a non-editable install
# that omitted the docs tree), the set is empty and no docs URLs are added.
_TEMPLATES_DIR = PUDL_ROOT_PATH / "docs" / "templates"
_SOURCES_WITH_DOCS: frozenset[str] = (
    frozenset(
        p.name.removesuffix("_child.rst.jinja")
        for p in _TEMPLATES_DIR.glob("*_child.rst.jinja")
    )
    if _TEMPLATES_DIR.is_dir()
    else frozenset()
)


def _collect_dagster_file_metadata(
    instance: dg.DagsterInstance,
    parquet_asset_keys: Sequence[dg.AssetKey],
) -> dict[str, dict]:
    """Return per-resource file stats recorded in the Dagster event log.

    Each parquet IO manager stores ``bytes`` and ``sha256`` as output metadata
    at materialisation time.  This function aggregates those records into a
    dict keyed by the asset-key path tail (which equals the resource/table name).
    Resources that have no event-log entry are excluded from the result.
    """
    latest_events = instance.get_latest_materialization_events(list(parquet_asset_keys))
    file_metadata: dict[str, dict] = {}
    for asset_key, event in latest_events.items():
        resource_name = asset_key.path[-1]
        if event is None or event.asset_materialization is None:
            continue
        meta = event.asset_materialization.metadata
        bytes_entry = meta.get("bytes")
        sha256_entry = meta.get("sha256")
        if bytes_entry is not None and sha256_entry is not None:
            file_metadata[resource_name] = {
                "bytes": bytes_entry.value,
                "hash": f"sha256:{sha256_entry.value}",
            }
    return file_metadata


def _collect_git_provenance() -> dict:
    """Return a dict with ``git_sha`` and optionally ``git_tags`` for HEAD.

    Both fields are omitted silently when the git commands fail (e.g. in
    environments without git or outside a git repository).
    """
    provenance: dict = {}

    git = shutil.which("git")
    if git is None:
        return provenance

    sha_result = subprocess.run(  # noqa: S603
        [git, "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    if sha_result.returncode == 0:
        provenance["git_sha"] = sha_result.stdout.strip()
    else:
        logger.warning(
            "Could not determine git HEAD commit for datapackage provenance. "
            f"git exited with code {sha_result.returncode}: {sha_result.stderr.strip()}"
        )

    tags_result = subprocess.run(  # noqa: S603
        [git, "tag", "--points-at", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    if tags_result.returncode == 0:
        tags = [t for t in tags_result.stdout.strip().splitlines() if t]
        if tags:
            provenance["git_tags"] = tags

    return provenance


def _docs_version_slug(version: str | None = None) -> str:
    """Return the Sphinx documentation version slug for a PUDL version string.

    All non-release versions should have a .devN suffix (e.g. ``v2026.5.1.dev6``) in
    which case we point at the nightly docs. Otherwise we check that the version matches
    our basic version pattern and if so we point at the versioned docs.

    ``v0.0.0`` is a special case which means no version could be obtained via hatch-vcs
    and is treated as a dev version.
    """
    if version is None or "dev" in version or version == "v0.0.0":
        return "nightly"

    if not version.startswith("v20"):
        raise ValueError(
            f"Unexpected version format in datapackage compilation: {version}"
        )
    return version


def _enrich_sources(
    descriptor: dict,
    zenodo_dois: ZenodoDoiSettings,
    version_slug: str,
) -> None:
    """Inject Zenodo DOIs and PUDL docs URLs into each source entry in-place.

    A ``doi`` field (resolvable ``https://doi.org/…`` URL) is added for every source
    whose short name is registered in *zenodo_dois*.  A ``docs`` field pointing to the
    PUDL documentation page is added for sources whose name matches a
    ``*_child.rst.jinja`` template under ``docs/templates/``.
    """
    for source in descriptor.get("sources", []):
        source_name = source.get("name", "")
        try:
            doi = zenodo_dois.get_doi(source_name)
            source["doi"] = f"https://doi.org/{doi}"
        except KeyError:
            pass
        if source_name in _SOURCES_WITH_DOCS:
            source["documentation"] = (
                f"https://docs.catalyst.coop/pudl/en/{version_slug}"
                f"/data_sources/{source_name}.html"
            )


def _propagate_source_enrichments(descriptor: dict) -> None:
    """Copy doi and documentation from top-level sources into resource-level source entries.

    ``_enrich_sources`` mutates the top-level ``sources`` array but the same
    source objects embedded in each resource's ``sources`` array are serialised
    before enrichment and never see those additions.  This function builds a
    lookup from the (now-enriched) top-level entries and copies only the runtime
    fields into every matching resource-level source dict.
    """
    runtime_fields = ("doi", "documentation")
    enriched = {
        s["name"]: {k: s[k] for k in runtime_fields if k in s}
        for s in descriptor.get("sources", [])
    }
    for resource in descriptor.get("resources", []):
        for src in resource.get("sources", []):
            src.update(enriched.get(src.get("name", ""), {}))


def _enrich_resources(
    descriptor: dict,
    dag_metadata: dict[str, dict],
    parquet_path: Path,
) -> int:
    """Add ``bytes`` and ``hash`` to each resource descriptor; return enriched count.

    Stats are sourced from Dagster metadata (recorded by the IO managers at
    materialisation time) when available. For resources absent from the Dagster
    metadata (typically resources materialised in a prior run) the parquet file is
    located on disk and its stats are computed directly. The parquet filename is
    ``{resource_name}.parquet``.
    """
    enriched_count = 0
    for resource_desc in descriptor.get("resources", []):
        name = resource_desc.get("name")
        if name in dag_metadata:
            resource_desc["bytes"] = dag_metadata[name]["bytes"]
            resource_desc["hash"] = dag_metadata[name]["hash"]
            enriched_count += 1
        else:
            parquet_file = parquet_path / f"{name}.parquet"
            if parquet_file.exists():
                resource_desc["bytes"] = parquet_file.stat().st_size
                with parquet_file.open("rb") as fh:
                    resource_desc["hash"] = (
                        "sha256:" + hashlib.file_digest(fh, "sha256").hexdigest()
                    )
                enriched_count += 1
    return enriched_count


def build_pudl_datapackage_asset(
    parquet_asset_keys: Sequence[dg.AssetKey],
) -> dg.AssetsDefinition:
    """Return a Dagster asset that writes ``datapackage.json`` for PUDL parquet outputs.

    The asset depends on every asset in *parquet_asset_keys* so Dagster will
    only run it once all parquet outputs for the current job are materialised.

    Args:
        parquet_asset_keys: Keys of all assets that write parquet files and
            should be described in the datapackage.
    """

    @dg.asset(
        name="pudl_datapackage",
        group_name="core_pudl",
        deps=list(parquet_asset_keys),
        required_resource_keys={"zenodo_dois"},
        description=(
            "Frictionless v2 datapackage descriptor for PUDL parquet outputs. "
            "Written to $PUDL_OUTPUT/parquet/datapackage.json."
        ),
    )
    def pudl_datapackage(
        context: dg.AssetExecutionContext,
    ) -> dg.MaterializeResult:
        package = PUDL_PACKAGE.to_frictionless(
            exclude_pattern=_FERCEQR_EXCLUDE_PATTERN,
        )
        dag_metadata = _collect_dagster_file_metadata(
            context.instance, parquet_asset_keys
        )

        descriptor = json.loads(package.to_json())
        descriptor["created"] = datetime.now(UTC).isoformat()
        descriptor["id"] = str(uuid.uuid4())
        descriptor.update(_collect_git_provenance())

        zenodo_dois: ZenodoDoiSettings = context.resources.zenodo_dois
        _enrich_sources(
            descriptor, zenodo_dois, _docs_version_slug(PUDL_PACKAGE.version)
        )
        _propagate_source_enrichments(descriptor)

        parquet_path = PudlPaths().parquet_path()
        enriched_count = _enrich_resources(descriptor, dag_metadata, parquet_path)

        total_resources = len(descriptor.get("resources", []))
        if enriched_count < total_resources:
            missing = total_resources - enriched_count
            logger.warning(
                f"{missing} resource(s) in the datapackage descriptor are missing "
                "file stats (bytes/hash). The affected resources were likely not "
                "materialised in this run and have no parquet file on disk."
            )

        output_path = parquet_path / "datapackage.json"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(descriptor, indent=2))
        logger.info(f"Wrote datapackage descriptor to {output_path}")

        return dg.MaterializeResult(
            metadata={
                "path": dg.MetadataValue.path(output_path),
                "resource_count": dg.MetadataValue.int(total_resources),
                "enriched_resource_count": dg.MetadataValue.int(enriched_count),
                "bytes": dg.MetadataValue.int(output_path.stat().st_size),
            }
        )

    return pudl_datapackage


__all__ = ["build_pudl_datapackage_asset"]
