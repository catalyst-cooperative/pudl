"""Unit tests for Dagster asset helpers."""

import hashlib
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
    _propagate_source_enrichments,
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


def test_propagate_source_enrichments_copies_runtime_fields() -> None:
    """Resource-level source entries should inherit enriched runtime fields."""
    descriptor = {
        "sources": [
            {
                "name": "eia860",
                "doi": "https://doi.org/10.5281/zenodo.12345",
                "documentation": "https://docs.catalyst.coop/pudl/en/v2026.5.1/data_sources/eia860.html",
                "title": "EIA 860",
            }
        ],
        "resources": [
            {"name": "plants", "sources": [{"name": "eia860"}, {"name": "ferc1"}]}
        ],
    }

    _propagate_source_enrichments(descriptor)

    assert descriptor["resources"][0]["sources"] == [
        {
            "name": "eia860",
            "doi": "https://doi.org/10.5281/zenodo.12345",
            "documentation": "https://docs.catalyst.coop/pudl/en/v2026.5.1/data_sources/eia860.html",
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
