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


@pytest.mark.parametrize(
    ("package", "asset_name", "group_name", "partitioned"),
    [
        (PUDL_PACKAGE, "pudl_datapackage", "core_pudl", False),
        (FERCEQR_PACKAGE, "ferceqr_datapackage", "core_ferceqr", True),
    ],
    ids=["pudl", "ferceqr"],
)
def test_datapackage_asset_writes_descriptor(
    mocker,
    asset_resources,
    parquet_dir,
    package,
    asset_name,
    group_name,
    partitioned,
):
    """The asset writes an enriched descriptor for every resource in the package."""
    parquet_asset_keys = [dg.AssetKey(resource.name) for resource in package.resources]
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
        package,
        parquet_asset_keys,
        asset_name,
        group_name=group_name,
        partitioned=partitioned,
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
            {"name": "from_dag"},
            {"name": "from_disk"},
            {"name": "missing"},
        ]
    }

    enriched_count = _enrich_resources(
        descriptor,
        {"from_dag": {"bytes": 42, "hash": "sha256:from-dag"}},
        tmp_path,
    )

    assert enriched_count == 2
    assert descriptor["resources"] == [
        {"name": "from_dag", "bytes": 42, "hash": "sha256:from-dag"},
        {
            "name": "from_disk",
            "bytes": len(parquet_bytes),
            "hash": f"sha256:{hashlib.sha256(parquet_bytes).hexdigest()}",
        },
        {"name": "missing"},
    ]
