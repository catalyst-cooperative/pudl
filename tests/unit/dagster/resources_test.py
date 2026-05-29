"""Unit tests for Dagster resources used by FERCEQR deployment flows."""

from upath import UPath

from pudl.dagster.resources import (
    FercEqrDeploymentTarget,
    FercEqrDeploymentTargetsResource,
    default_resources,
)
from pudl.workspace.setup import PudlPaths


def _make_pudl_paths(tmp_path) -> PudlPaths:
    return PudlPaths(
        pudl_input=str(tmp_path / "input"), pudl_output=str(tmp_path / "output")
    )


def test_ferceqr_deployment_targets_resource_loads_config_values(tmp_path):
    """Deployment targets resource should expose configured target paths and options."""
    resource = FercEqrDeploymentTargetsResource(
        pudl_paths=_make_pudl_paths(tmp_path),
        deployment_targets=[
            FercEqrDeploymentTarget(
                path="gs://my-bucket",
                storage_options={
                    "project": "my-billing-project",
                    "requester_pays": True,
                },
            ),
            FercEqrDeploymentTarget(path="s3://my-bucket"),
        ],
        build_id="build-999",
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
    assert resource.build_id == "build-999"


def test_ferceqr_deployment_targets_resource_defaults_to_local_fallback(tmp_path):
    """Resource with no explicit targets should fall back to $PUDL_OUTPUT/ferceqr_deployment."""
    pudl_paths = _make_pudl_paths(tmp_path)
    resource = FercEqrDeploymentTargetsResource(pudl_paths=pudl_paths)

    targets = resource.resolved_targets()

    assert targets == [UPath(pudl_paths.pudl_output) / "ferceqr_deployment"]
    assert resource.build_id == ""


def test_default_resources_include_ferceqr_deployment_targets():
    """Default Dagster resources should include the FERCEQR deployment targets resource."""
    assert "ferceqr_deployment_targets" in default_resources
