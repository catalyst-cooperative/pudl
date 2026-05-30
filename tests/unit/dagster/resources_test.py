"""Unit tests for Dagster resources used by FERCEQR deployment flows."""

import os
from pathlib import Path

import pytest
import yaml
from upath import UPath

from pudl.dagster.resources import (
    FercEqrDeploymentResource,
    FercEqrDeploymentTargetConfig,
    default_resources,
)


def test_ferceqr_deployment_targets_resource_loads_config_values(tmp_path):
    """Deployment targets resource should expose configured target paths and options."""
    resource = FercEqrDeploymentResource(
        deployment_targets=[
            FercEqrDeploymentTargetConfig(
                path="gs://my-bucket",
                storage_options={
                    "project": "my-billing-project",
                    "requester_pays": True,
                },
            ),
            FercEqrDeploymentTargetConfig(path="s3://my-bucket"),
        ],
    )

    assert len(resource.deployment_targets) == 2
    assert resource.deployment_targets[0].path == "gs://my-bucket"
    assert (
        resource.deployment_targets[0].storage_options["project"]
        == "my-billing-project"
    )
    assert resource.deployment_targets[0].storage_options["requester_pays"] is True
    assert resource.deployment_targets[1].path == "s3://my-bucket"
    assert resource.deployment_targets[1].storage_options == {}


def test_ferceqr_deployment_target_config_accepts_local_filesystem_path(tmp_path):
    """Deployment targets should accept existing absolute local directories."""
    deploy_path = tmp_path / "deploy"
    deploy_path.mkdir()
    target = FercEqrDeploymentTargetConfig(path=str(deploy_path))

    assert target.path == str(deploy_path)


def test_ferceqr_deployment_target_config_accepts_file_uri(tmp_path):
    """Deployment targets should accept existing absolute local file:// URIs."""
    deploy_path = tmp_path / "deploy_uri"
    deploy_path.mkdir()
    target = FercEqrDeploymentTargetConfig(path=deploy_path.as_uri())

    assert target.path == deploy_path.as_uri()


@pytest.mark.parametrize(
    "path",
    [
        "https://example.com/ferceqr",
        "s3://",
        "gs://",
        "file://relative/path",
        "   ",
        "relative/path",
    ],
)
def test_ferceqr_deployment_target_config_rejects_invalid_paths(path):
    """Deployment targets should reject unsupported URL schemes and empty bucket URLs."""
    with pytest.raises(ValueError):
        FercEqrDeploymentTargetConfig(path=path)


def test_ferceqr_deployment_target_config_rejects_missing_local_directory(tmp_path):
    """Deployment targets should reject local directories that do not exist."""
    with pytest.raises(ValueError, match="does not exist"):
        FercEqrDeploymentTargetConfig(path=str(tmp_path / "missing"))


def test_ferceqr_deployment_target_config_rejects_local_file(tmp_path):
    """Deployment targets should reject local paths that are not directories."""
    local_file = tmp_path / "deploy.txt"
    local_file.write_text("not a directory")

    with pytest.raises(ValueError, match="is not a directory"):
        FercEqrDeploymentTargetConfig(path=str(local_file))


def test_ferceqr_deployment_target_config_rejects_unwritable_directory(
    tmp_path, monkeypatch
):
    """Deployment targets should reject local directories that are not writable."""
    deploy_path = tmp_path / "deploy"
    deploy_path.mkdir()

    def fake_access(path, mode):
        if Path(path) == deploy_path and mode == os.W_OK:
            return False
        return os.access(path, mode)

    monkeypatch.setattr("pudl.dagster.resources.os.access", fake_access)

    with pytest.raises(ValueError, match="is not writable"):
        FercEqrDeploymentTargetConfig(path=str(deploy_path))


def test_ferceqr_deployment_resource_defaults_to_no_targets(tmp_path):
    """Resource with no explicit targets or config path should skip deployment."""
    resource = FercEqrDeploymentResource()

    configured_targets = resource.configured_targets()

    assert configured_targets == []


def test_ferceqr_deployment_resource_loads_override_config_path(tmp_path):
    """Resource should support overriding deployment targets with an alternate YAML file."""
    deployment_config_path = tmp_path / "ferceqr_targets.yml"
    deploy_path = tmp_path / "deploy"
    deploy_path.mkdir()
    deployment_config_path.write_text(
        yaml.safe_dump(
            {
                "deployment_targets": [
                    {
                        "path": str(deploy_path),
                        "storage_options": {},
                    }
                ]
            }
        )
    )

    resource = FercEqrDeploymentResource(
        deployment_config_path=str(deployment_config_path),
    )

    targets = resource.resolved_targets()

    assert targets == [UPath(deploy_path)]


def test_ferceqr_deployment_resource_appends_build_id(monkeypatch):
    """Targets can opt into build-scoped destinations by appending BUILD_ID."""
    monkeypatch.setenv("BUILD_ID", "build-123")
    resource = FercEqrDeploymentResource(
        deployment_targets=[
            FercEqrDeploymentTargetConfig(
                path="gs://test.catalyst.coop",
                append_build_id=True,
            )
        ]
    )

    assert resource.resolved_targets() == [
        UPath("gs://test.catalyst.coop") / "build-123"
    ]


def test_ferceqr_deployment_resource_requires_build_id_for_appended_targets(
    monkeypatch,
):
    """Build-scoped targets should fail if BUILD_ID is unavailable at runtime."""
    monkeypatch.delenv("BUILD_ID", raising=False)
    resource = FercEqrDeploymentResource(
        deployment_targets=[
            FercEqrDeploymentTargetConfig(
                path="gs://test.catalyst.coop",
                append_build_id=True,
            )
        ]
    )

    with pytest.raises(ValueError, match="BUILD_ID must be set"):
        resource.resolved_targets()


def test_default_resources_include_ferceqr_deployment_targets():
    """Default Dagster resources should include the FERCEQR deployment targets resource."""
    assert "ferceqr_deployment_targets" in default_resources
