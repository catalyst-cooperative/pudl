"""Unit tests for Dagster resources used by FERCEQR deployment flows."""

from dagster import build_init_resource_context

from pudl.dagster.resources import (
    FercEqrBucketDeploymentResource,
    default_resources,
)


def test_ferceqr_bucket_deployment_resource_loads_config_values():
    """Bucket deployment resource should expose configured bucket/project/build fields."""
    init_context = build_init_resource_context(
        config={
            "gcs_output_bucket": "gs://my-gcs-bucket",
            "s3_output_bucket": "s3://my-s3-bucket",
            "gcp_billing_project": "my-billing-project",
            "build_id": "build-999",
        }
    )

    resource = FercEqrBucketDeploymentResource.from_resource_context(init_context)

    assert resource.gcs_output_bucket == "gs://my-gcs-bucket"
    assert resource.s3_output_bucket == "s3://my-s3-bucket"
    assert resource.gcp_billing_project == "my-billing-project"
    assert resource.build_id == "build-999"


def test_default_resources_include_ferceqr_bucket_deployment_resource():
    """Default Dagster resources should include the FERCEQR cloud bucket resource."""
    assert "ferceqr_bucket_deployment" in default_resources
