"""Unit tests for FERCEQR deployment sensors and deployment asset handlers."""

import os
from types import SimpleNamespace

import dagster as dg
import pytest

from pudl.dagster import sensors
from pudl.dagster.assets.deploy import ferceqr as deploy_ferceqr


def _build_deploy_context(tmp_path, mocker):
    """Build a Dagster asset context with a minimal pudl_paths resource."""
    cloud_deployment_resource = SimpleNamespace(
        gcs_output_bucket="gs://fake-output",
        s3_output_bucket="s3://fake-output",
        gcp_billing_project="fake-project",
        build_id="test-build",
    )
    return dg.build_asset_context(
        resources={
            "pudl_paths": SimpleNamespace(pudl_output=tmp_path),
            "ferceqr_bucket_deployment": cloud_deployment_resource,
        }
    )


def test_ferceqr_deployment_sensor_returns_success_run_request(mocker):
    """Success sensor should request the deploy asset for the triggering run."""
    context = mocker.Mock()
    context.dagster_run.run_id = "run-123"
    context.dagster_run.tags = {"dagster/partition": "2013q3"}

    run_request = sensors.ferceqr_deployment_sensor._run_status_sensor_fn(context)

    assert run_request.run_key == "ferceqr_deployment_success:run-123"
    assert run_request.asset_selection == [dg.AssetKey("deploy_ferceqr")]
    assert run_request.tags == {
        deploy_ferceqr.FERCEQR_SOURCE_PARTITION_TAG: "2013q3",
        deploy_ferceqr.FERCEQR_SOURCE_RUN_ID_TAG: "run-123",
        deploy_ferceqr.FERCEQR_SOURCE_RUN_STATUS_TAG: "SUCCESS",
    }


def test_ferceqr_deployment_sensor_skips_while_backfill_still_running(mocker):
    """Success sensor should skip if any runs in the backfill are non-terminal."""
    context = mocker.Mock()
    context.dagster_run.run_id = "run-123"
    context.dagster_run.job_name = "ferceqr"
    context.dagster_run.tags = {
        "dagster/partition": "2013q3",
        sensors.FERCEQR_BACKFILL_TAG: "bf-123",
    }
    context.instance.get_runs.return_value = [
        SimpleNamespace(status=dg.DagsterRunStatus.SUCCESS),
        SimpleNamespace(status=dg.DagsterRunStatus.STARTED),
    ]

    result = sensors.ferceqr_deployment_sensor._run_status_sensor_fn(context)

    assert isinstance(result, dg.SkipReason)
    assert "still in progress" in result.skip_message


def test_ferceqr_deployment_sensor_skips_when_backfill_has_failures(mocker):
    """Success sensor should skip if any runs in the backfill failed or were canceled."""
    context = mocker.Mock()
    context.dagster_run.run_id = "run-123"
    context.dagster_run.job_name = "ferceqr"
    context.dagster_run.tags = {
        "dagster/partition": "2013q3",
        sensors.FERCEQR_BACKFILL_TAG: "bf-123",
    }
    context.instance.get_runs.return_value = [
        SimpleNamespace(status=dg.DagsterRunStatus.SUCCESS),
        SimpleNamespace(status=dg.DagsterRunStatus.FAILURE),
    ]

    result = sensors.ferceqr_deployment_sensor._run_status_sensor_fn(context)

    assert isinstance(result, dg.SkipReason)
    assert "failed or canceled" in result.skip_message


def test_ferceqr_deployment_sensor_backfill_success_uses_backfill_run_key(mocker):
    """Success sensor should trigger once per completed successful backfill."""
    context = mocker.Mock()
    context.dagster_run.run_id = "run-123"
    context.dagster_run.job_name = "ferceqr"
    context.dagster_run.tags = {
        "dagster/partition": "2013q3",
        sensors.FERCEQR_BACKFILL_TAG: "bf-123",
    }
    context.instance.get_runs.return_value = [
        SimpleNamespace(status=dg.DagsterRunStatus.SUCCESS),
        SimpleNamespace(status=dg.DagsterRunStatus.SUCCESS),
    ]

    run_request = sensors.ferceqr_deployment_sensor._run_status_sensor_fn(context)

    assert run_request.run_key == "ferceqr_deployment_success_backfill:bf-123"
    assert run_request.asset_selection == [dg.AssetKey("deploy_ferceqr")]
    assert run_request.tags == {
        deploy_ferceqr.FERCEQR_SOURCE_PARTITION_TAG: "2013q3",
        deploy_ferceqr.FERCEQR_SOURCE_RUN_ID_TAG: "run-123",
        deploy_ferceqr.FERCEQR_SOURCE_RUN_STATUS_TAG: "SUCCESS",
    }


def test_ferceqr_deployment_failure_sensor_returns_failure_run_request(mocker):
    """Failure sensor should request the failure-handler asset for the run."""
    context = mocker.Mock()
    context.dagster_run.run_id = "run-456"
    context.dagster_run.tags = {"dagster/partition": "2013q4"}

    run_request = sensors.ferceqr_deployment_failure_sensor._run_status_sensor_fn(
        context
    )

    assert run_request.run_key == "ferceqr_deployment_failure:run-456"
    assert run_request.asset_selection == [
        dg.AssetKey("handle_ferceqr_deployment_failure")
    ]
    assert run_request.tags == {
        deploy_ferceqr.FERCEQR_SOURCE_PARTITION_TAG: "2013q4",
        deploy_ferceqr.FERCEQR_SOURCE_RUN_ID_TAG: "run-456",
        deploy_ferceqr.FERCEQR_SOURCE_RUN_STATUS_TAG: "FAILURE",
    }


def test_deploy_ferceqr_raises_when_ferceqr_build_disabled(mocker, tmp_path):
    """Deployment asset should fail fast outside FERCEQR batch builds."""
    mocker.patch.dict(os.environ, {"FERCEQR_BUILD": "0"}, clear=False)

    with pytest.raises(
        dg.Failure,
        match="FERCEQR deployment handlers only run when FERCEQR_BUILD is enabled",
    ):
        deploy_ferceqr.deploy_ferceqr(_build_deploy_context(tmp_path, mocker))


def test_deploy_ferceqr_success_path_writes_success_and_notifies(mocker, tmp_path):
    """Successful deployment should publish outputs, notify Slack, and mark SUCCESS."""
    mocker.patch.dict(
        os.environ,
        {
            "FERCEQR_BUILD": "1",
            "SLACK_TOKEN": "fake-slack-token",  # pragma: allowlist secret
            "BUILD_ID": "build-123",
            "GCS_OUTPUT_BUCKET": "gs://fake-output",
            "GCP_BILLING_PROJECT": "fake-project",
            "S3_OUTPUT_BUCKET": "s3://fake-output",
        },
        clear=False,
    )

    gcs_fs = mocker.Mock()
    s3_fs = mocker.Mock()
    deploy_context = _build_deploy_context(tmp_path, mocker)
    notify_slack = mocker.patch.object(
        deploy_ferceqr, "_notify_slack_deployments_channel"
    )
    frictionless = mocker.Mock()
    mock_package = mocker.Mock()
    frictionless.to_json.return_value = "{}"
    mock_package.to_frictionless.return_value = frictionless

    mocker.patch.object(deploy_ferceqr.gcsfs, "GCSFileSystem", return_value=gcs_fs)
    mocker.patch.object(deploy_ferceqr.s3fs, "S3FileSystem", return_value=s3_fs)
    mocker.patch.object(deploy_ferceqr, "PUDL_PACKAGE", mock_package)

    deploy_ferceqr.deploy_ferceqr(deploy_context)

    assert (tmp_path / "SUCCESS").exists()
    assert not (tmp_path / "FAILURE").exists()

    mock_package.to_frictionless.assert_called_once_with(
        include_pattern=r"core_ferceqr.*"
    )
    for fs in [gcs_fs, s3_fs]:
        assert fs.mkdirs.call_count == len(deploy_ferceqr.FERCEQR_TRANSFORM_ASSETS)
        assert fs.put.call_count == len(deploy_ferceqr.FERCEQR_TRANSFORM_ASSETS)
        fs.pipe.assert_called_once()

    notify_slack.assert_called_once()
    sent_message = notify_slack.call_args.kwargs["message"]
    assert "## FERC EQR Deployment Succeeded" in sent_message
    assert "### Failed Assets / Partitions" not in sent_message
    assert "### Step Status" in sent_message
    assert "Step Status" in sent_message
    assert "Count" in sent_message


def test_handle_ferceqr_deployment_failure_writes_failure_and_notifies(
    mocker, tmp_path
):
    """Failure handler should notify Slack and write the FAILURE sentinel file."""
    mocker.patch.dict(
        os.environ,
        {
            "FERCEQR_BUILD": "1",
            "SLACK_TOKEN": "fake-slack-token",  # pragma: allowlist secret
            "BUILD_ID": "build-456",
        },
        clear=False,
    )
    deploy_context = _build_deploy_context(tmp_path, mocker)
    notify_slack = mocker.patch.object(
        deploy_ferceqr, "_notify_slack_deployments_channel"
    )

    deploy_ferceqr.handle_ferceqr_deployment_failure(deploy_context)

    assert (tmp_path / "FAILURE").exists()
    notify_slack.assert_called_once()
    sent_message = notify_slack.call_args.kwargs["message"]
    assert "## FERC EQR Deployment Failed" in sent_message
    assert "### Failed Assets / Partitions" not in sent_message
    assert "### Step Status" in sent_message
    assert "Step Status" in sent_message
    assert "Count" in sent_message


def test_build_message_includes_partition_and_failed_assets_table():
    """Verbose message includes source partition and failed asset/partition table."""
    message = deploy_ferceqr.build_ferceqr_deployment_markdown_message(
        deploy_ferceqr.FercEqrDeploymentNotificationPayload(
            outcome="FAILURE",
            build_id="build-789",
            distribution_paths=None,
            source_run_id="run-789",
            source_run_status="FAILURE",
            source_partition="2013q4",
            step_status_counts={"SUCCESS": 10, "FAILURE": 2},
            failed_step_keys=[
                "core_ferceqr__transactions[2013q4]",
                "core_ferceqr__quarterly_identity[2013q4]",
            ],
        )
    )

    assert "- Source partition: `2013q4`" in message
    assert "### Failed Assets / Partitions" in message
    assert "Asset" in message
    assert "Partition" in message
    assert "Step Key" in message
    assert "core_ferceqr__transactions" in message
    assert "2013q4" in message
