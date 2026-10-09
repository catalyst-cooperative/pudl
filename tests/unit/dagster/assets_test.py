"""Unit tests for Dagster asset helpers."""

import hashlib
import json
import uuid
from datetime import datetime
from pathlib import Path

import dagster as dg
import pytest
from pytest_mock import MockerFixture

from pudl.dagster.assets.core import datapackage
from pudl.dagster.assets.core.datapackage import (
    _collect_dagster_file_metadata,
    _collect_git_provenance,
    _docs_version_slug,
    _enrich_resources,
    _enrich_sources,
    _resource_paths,
    build_datapackage_asset,
)
from pudl.metadata.classes import FERCEQR_PACKAGE, PUDL_PACKAGE
from pudl.workspace.datastore import ZenodoDoiSettings

MODULE = "pudl.dagster.assets.core.datapackage"

FAKE_SHA256 = "ab" * 32
FAKE_BYTES = 4096


@pytest.fixture
def parquet_dir(tmp_path):
    """An empty stand-in for ``$PUDL_OUTPUT/parquet``."""
    path = tmp_path / "parquet"
    path.mkdir()
    return path


@pytest.fixture
def asset_resources(mocker, parquet_dir):
    """Resources required by the datapackage asset."""
    pudl_paths = mocker.Mock()
    pudl_paths.parquet_path.return_value = parquet_dir
    return {"zenodo_dois": ZenodoDoiSettings(), "pudl_paths": pudl_paths}


def fake_dagster_file_metadata(asset_keys) -> dict[str, dict]:
    """Fake per-resource file stats for every parquet asset key."""
    return {
        key.path[-1]: {"bytes": FAKE_BYTES, "hash": f"sha256:{FAKE_SHA256}"}
        for key in asset_keys
    }


def declared_paths(package) -> dict[str, list[str]]:
    """Map each resource name to the parquet paths its descriptor declares.

    Read from the package rather than hardcoded so these tests keep working if the
    metadata layer changes how it spells multi-file paths.
    """
    descriptor = json.loads(package.to_frictionless().to_json())
    return {
        resource["name"]: _resource_paths(resource)
        for resource in descriptor["resources"]
    }


def sha256_of(payload: bytes) -> str:
    """Return the descriptor-formatted hash of *payload*."""
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def write_parquet_files(parquet_dir: Path, paths: list[str]) -> dict[str, bytes]:
    """Create a file at each relative path, with distinct contents. Returns payloads."""
    payloads = {}
    for rel_path in paths:
        parquet_file = parquet_dir / rel_path
        parquet_file.parent.mkdir(parents=True, exist_ok=True)
        payloads[rel_path] = rel_path.encode()
        parquet_file.write_bytes(payloads[rel_path])
    return payloads


def test_datapackage_asset_writes_descriptor(mocker, asset_resources, parquet_dir):
    """The asset writes an enriched descriptor for every resource in the package."""
    parquet_asset_keys = [
        dg.AssetKey(resource.name) for resource in PUDL_PACKAGE.resources
    ]
    assert parquet_asset_keys, "Package under test has no resources."

    mocker.patch(
        f"{MODULE}._collect_dagster_file_metadata",
        return_value=fake_dagster_file_metadata(parquet_asset_keys),
    )
    mocker.patch(
        f"{MODULE}._collect_git_provenance",
        return_value={"git_sha": "deadbeef", "git_tags": ["v2026.1.1"]},
    )

    asset_def = build_datapackage_asset(
        PUDL_PACKAGE, parquet_asset_keys, "pudl_datapackage"
    )
    result = asset_def(dg.build_asset_context(resources=asset_resources))

    output_path = parquet_dir / "datapackage.json"
    assert output_path.is_file()
    descriptor = json.loads(output_path.read_text())

    # Runtime provenance fields.
    uuid.UUID(descriptor["id"])
    datetime.fromisoformat(descriptor["created"])
    assert descriptor["git_sha"] == "deadbeef"
    assert descriptor["git_tags"] == ["v2026.1.1"]

    # Every resource got file stats from the (mocked) Dagster event log.
    resources = descriptor["resources"]
    assert len(resources) == len(parquet_asset_keys)
    assert all(r["bytes"] == FAKE_BYTES for r in resources)
    assert all(r["hash"] == f"sha256:{FAKE_SHA256}" for r in resources)

    # Any DOIs that were injected are resolvable URLs.
    dois = [s["doi"] for s in descriptor.get("sources", []) if "doi" in s]
    assert all(doi.startswith("https://doi.org/") for doi in dois)

    assert isinstance(result, dg.MaterializeResult)
    assert result.metadata["resource_count"].value == len(resources)
    assert result.metadata["enriched_resource_count"].value == len(resources)
    assert result.metadata["bytes"].value == output_path.stat().st_size


def test_partitioned_datapackage_asset_describes_every_partition(
    mocker, asset_resources, parquet_dir
):
    """Partitioned resources get per-partition stats computed from disk.

    The Dagster event log holds one materialisation per partition, so aggregating
    it by asset key would describe a single partition as the whole table. The asset
    must not consult it at all when ``partitioned=True``.
    """
    paths_by_resource = declared_paths(FERCEQR_PACKAGE)
    assert any(len(paths) > 1 for paths in paths_by_resource.values()), (
        "FERCEQR_PACKAGE declares no multi-partition resources; test is vacuous."
    )

    payloads: dict[str, bytes] = {}
    for paths in paths_by_resource.values():
        payloads |= write_parquet_files(parquet_dir, paths)

    parquet_asset_keys = [
        dg.AssetKey(resource.name) for resource in FERCEQR_PACKAGE.resources
    ]
    collect_dag_metadata = mocker.patch(
        f"{MODULE}._collect_dagster_file_metadata",
        return_value=fake_dagster_file_metadata(parquet_asset_keys),
    )

    asset_def = build_datapackage_asset(
        FERCEQR_PACKAGE,
        parquet_asset_keys,
        "ferceqr_datapackage",
        group_name="core_ferceqr",
        partitioned=True,
    )
    result = asset_def(dg.build_asset_context(resources=asset_resources))

    collect_dag_metadata.assert_not_called()

    descriptor = json.loads((parquet_dir / "datapackage.json").read_text())
    resources = descriptor["resources"]
    assert result.metadata["enriched_resource_count"].value == len(resources)

    for resource in resources:
        paths = paths_by_resource[resource["name"]]
        if len(paths) > 1:
            # Multi-file resources carry per-partition stats, in declared order,
            # and no top-level hash -- there is no single file for it to describe.
            assert [part["path"] for part in resource["parts"]] == paths
            assert resource["parts"] == [
                {
                    "path": rel_path,
                    "bytes": len(payloads[rel_path]),
                    "hash": sha256_of(payloads[rel_path]),
                }
                for rel_path in paths
            ]
            assert resource["bytes"] == sum(len(payloads[p]) for p in paths)
            assert "hash" not in resource
            assert FAKE_BYTES not in {part["bytes"] for part in resource["parts"]}
        else:
            assert resource["bytes"] == len(payloads[paths[0]])
            assert resource["hash"] == sha256_of(payloads[paths[0]])
            assert "parts" not in resource


def test_pudl_datapackage_enriches_sources(mocker, asset_resources, parquet_dir):
    """The real PUDL package picks up Zenodo DOIs and docs URLs for its sources."""
    parquet_asset_keys = [
        dg.AssetKey(resource.name) for resource in PUDL_PACKAGE.resources
    ]
    mocker.patch(
        f"{MODULE}._collect_dagster_file_metadata",
        return_value=fake_dagster_file_metadata(parquet_asset_keys),
    )
    asset_def = build_datapackage_asset(
        PUDL_PACKAGE, parquet_asset_keys, "pudl_datapackage"
    )
    asset_def(dg.build_asset_context(resources=asset_resources))

    descriptor = json.loads((parquet_dir / "datapackage.json").read_text())
    sources = descriptor["sources"]
    assert any("doi" in source for source in sources)
    assert any(
        source.get("documentation", "").startswith("https://docs.catalyst.coop/pudl/")
        for source in sources
    )


def test_unpartitioned_asset_definition():
    """Unpartitioned deps are plain asset keys with no partition mapping."""
    keys = [dg.AssetKey("table_a"), dg.AssetKey("table_b")]
    asset_def = build_datapackage_asset(PUDL_PACKAGE, keys, "pudl_datapackage")

    spec = next(iter(asset_def.specs))
    assert spec.key == dg.AssetKey("pudl_datapackage")
    assert spec.group_name == "core_pudl"
    assert {dep.asset_key for dep in spec.deps} == set(keys)
    assert all(dep.partition_mapping is None for dep in spec.deps)


def test_partitioned_asset_definition():
    """Partitioned upstreams get an ``AllPartitionMapping`` on every dep."""
    keys = [dg.AssetKey("core_ferceqr__a"), dg.AssetKey("core_ferceqr__b")]
    asset_def = build_datapackage_asset(
        FERCEQR_PACKAGE,
        keys,
        "ferceqr_datapackage",
        group_name="core_ferceqr",
        partitioned=True,
    )

    spec = next(iter(asset_def.specs))
    assert spec.key == dg.AssetKey("ferceqr_datapackage")
    assert spec.group_name == "core_ferceqr"
    assert {dep.asset_key for dep in spec.deps} == set(keys)
    assert all(
        isinstance(dep.partition_mapping, dg.AllPartitionMapping) for dep in spec.deps
    )


@pytest.mark.parametrize(
    ("version", "expected_slug"),
    [
        (None, "nightly"),
        ("2026.5.1.dev6", "nightly"),
        ("0.0.0", "nightly"),
        ("2026.5.1", "v2026.5.1"),
    ],
)
def test_docs_version_slug(version: str | None, expected_slug: str) -> None:
    """Known version formats should map to the expected docs slug."""
    assert _docs_version_slug(version) == expected_slug


@pytest.mark.parametrize("version", ["v2026.5.1", "1.2.3", "main"])
def test_docs_version_slug_rejects_invalid_release_versions(version: str) -> None:
    """Unexpected non-dev version formats should raise a clear error."""
    with pytest.raises(ValueError, match="Unexpected version format"):
        _docs_version_slug(version)


def test_collect_dagster_file_metadata_uses_latest_materializations(
    mocker: MockerFixture,
) -> None:
    """Dagster output metadata should be mapped by resource name."""
    instance = mocker.Mock()
    event = mocker.Mock()
    event.asset_materialization.metadata = {
        "bytes": mocker.Mock(value=123),
        "sha256": mocker.Mock(value="abc123"),
    }
    instance.get_latest_materialization_events.return_value = {
        dg.AssetKey(["core", "resource_one"]): event,
        dg.AssetKey(["core", "resource_two"]): None,
    }

    result = _collect_dagster_file_metadata(
        instance,
        [dg.AssetKey(["core", "resource_one"]), dg.AssetKey(["core", "resource_two"])],
    )

    assert result == {"resource_one": {"bytes": 123, "hash": "sha256:abc123"}}


def test_collect_git_provenance_returns_empty_without_git(
    mocker: MockerFixture,
) -> None:
    """Missing git should silently omit provenance fields."""
    mocker.patch.object(datapackage.shutil, "which", return_value=None)

    assert _collect_git_provenance() == {}


def test_collect_git_provenance_includes_sha_and_tags(
    mocker: MockerFixture,
) -> None:
    """Successful git commands should populate commit and tag metadata."""
    mocker.patch.object(datapackage.shutil, "which", return_value="/usr/bin/git")
    mocker.patch.object(
        datapackage.subprocess,
        "run",
        side_effect=[
            mocker.Mock(returncode=0, stdout="deadbeef\n", stderr=""),
            mocker.Mock(returncode=0, stdout="v2026.5.1\nlatest\n", stderr=""),
        ],
    )

    assert _collect_git_provenance() == {
        "git_sha": "deadbeef",
        "git_tags": ["v2026.5.1", "latest"],
    }


def test_enrich_sources_adds_doi_and_documentation(
    mocker: MockerFixture,
) -> None:
    """Top-level sources should gain DOI and docs links when available."""
    descriptor = {
        "sources": [
            {"name": "eia860"},
            {"name": "ferc1"},
        ]
    }
    zenodo_dois = mocker.Mock()
    zenodo_dois.get_doi.side_effect = lambda name: {
        "eia860": "10.5281/zenodo.12345",
    }[name]
    mocker.patch.object(datapackage, "_SOURCES_WITH_DOCS", frozenset({"eia860"}))

    _enrich_sources(descriptor, zenodo_dois, "v2026.5.1")

    assert descriptor["sources"] == [
        {
            "name": "eia860",
            "doi": "https://doi.org/10.5281/zenodo.12345",
            "documentation": (
                "https://docs.catalyst.coop/pudl/en/v2026.5.1/data_sources/eia860.html"
            ),
        },
        {"name": "ferc1"},
    ]


def test_enrich_resources_prefers_dagster_metadata_and_falls_back_to_disk(
    tmp_path: Path,
) -> None:
    """Resource file stats should come from Dagster first, then parquet files."""
    parquet_bytes = b"parquet-bytes"
    (tmp_path / "from_disk.parquet").write_bytes(parquet_bytes)
    descriptor = {
        "resources": [
            {"name": "from_dag", "path": "from_dag.parquet"},
            {"name": "from_disk", "path": "from_disk.parquet"},
            {"name": "missing", "path": "missing.parquet"},
        ]
    }

    enriched_count = _enrich_resources(
        descriptor,
        {"from_dag": {"bytes": 42, "hash": "sha256:from-dag"}},
        tmp_path,
    )

    assert enriched_count == 2
    assert descriptor["resources"] == [
        {
            "name": "from_dag",
            "path": "from_dag.parquet",
            "bytes": 42,
            "hash": "sha256:from-dag",
        },
        {
            "name": "from_disk",
            "path": "from_disk.parquet",
            "bytes": len(parquet_bytes),
            "hash": sha256_of(parquet_bytes),
        },
        {"name": "missing", "path": "missing.parquet"},
    ]


def test_enrich_resources_adds_parts_for_multi_file_resources(tmp_path: Path) -> None:
    """Each declared partition gets its own path, byte count and hash."""
    paths = ["t/2013q1.parquet", "t/2013q2.parquet"]
    payloads = write_parquet_files(tmp_path, paths)
    descriptor = {
        "resources": [{"name": "t", "path": paths[0], "extrapaths": paths[1:]}]
    }

    enriched_count = _enrich_resources(descriptor, {}, tmp_path)

    (resource,) = descriptor["resources"]
    assert enriched_count == 1
    assert resource["parts"] == [
        {
            "path": rel_path,
            "bytes": len(payloads[rel_path]),
            "hash": sha256_of(payloads[rel_path]),
        }
        for rel_path in paths
    ]
    assert resource["bytes"] == sum(len(p) for p in payloads.values())
    assert "hash" not in resource


def test_enrich_resources_ignores_dagster_metadata_for_multi_file_resources(
    tmp_path: Path,
) -> None:
    """A multi-file resource is never annotated from the event log.

    Event-log metadata is keyed by asset name but recorded per partition, so using
    it here would report one partition's stats as the whole table.
    """
    paths = ["t/2013q1.parquet", "t/2013q2.parquet"]
    payloads = write_parquet_files(tmp_path, paths)
    descriptor = {
        "resources": [{"name": "t", "path": paths[0], "extrapaths": paths[1:]}]
    }

    _enrich_resources(
        descriptor,
        {"t": {"bytes": FAKE_BYTES, "hash": f"sha256:{FAKE_SHA256}"}},
        tmp_path,
    )

    (resource,) = descriptor["resources"]
    assert resource["bytes"] == sum(len(p) for p in payloads.values())
    assert FAKE_BYTES not in {part["bytes"] for part in resource["parts"]}


def test_enrich_resources_describes_only_the_partitions_present(
    tmp_path: Path, caplog
) -> None:
    """A partially materialised resource is described by the files that exist."""
    paths = ["t/2013q1.parquet", "t/2013q2.parquet", "t/2013q3.parquet"]
    payloads = write_parquet_files(tmp_path, paths[:1])
    descriptor = {
        "resources": [{"name": "t", "path": paths[0], "extrapaths": paths[1:]}]
    }

    enriched_count = _enrich_resources(descriptor, {}, tmp_path)

    (resource,) = descriptor["resources"]
    assert enriched_count == 1
    assert [part["path"] for part in resource["parts"]] == paths[:1]
    assert resource["bytes"] == len(payloads[paths[0]])
    assert "2 of 3 parquet file(s) not found" in caplog.text


def test_enrich_resources_skips_resource_with_no_files_on_disk(
    tmp_path: Path,
) -> None:
    """A resource whose partitions are all absent is left untouched and uncounted."""
    descriptor = {
        "resources": [
            {
                "name": "t",
                "path": "t/2013q1.parquet",
                "extrapaths": ["t/2013q2.parquet"],
            }
        ]
    }

    enriched_count = _enrich_resources(descriptor, {}, tmp_path)

    assert enriched_count == 0
    assert descriptor["resources"] == [
        {
            "name": "t",
            "path": "t/2013q1.parquet",
            "extrapaths": ["t/2013q2.parquet"],
        }
    ]


def test_enrich_resources_rejects_resource_without_path(tmp_path: Path) -> None:
    """A parquet-backed resource with no declared path is a metadata bug."""
    with pytest.raises(AssertionError, match="has no path"):
        _enrich_resources({"resources": [{"name": "nameless"}]}, {}, tmp_path)
