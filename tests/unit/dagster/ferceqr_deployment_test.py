"""Unit tests for FERCEQR deployment sensors and deployment asset handlers."""

import json
from types import SimpleNamespace

import dagster as dg
import pytest
from upath import UPath

from pudl.dagster import sensors
from pudl.dagster.assets.deploy import ferceqr as deploy_ferceqr


def _build_deploy_context(tmp_path, mocker, targets=None):
    """Build a Dagster asset context with a minimal pudl_paths resource."""
    deployment_resource = SimpleNamespace(
        resolved_targets=lambda: targets or [],
    )
    return dg.build_asset_context(
        resources={
            "pudl_paths": SimpleNamespace(pudl_output=tmp_path),
            "ferceqr_deployment_targets": deployment_resource,
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
        deploy_ferceqr.FERCEQR_SOURCE_PARTITIONS_TAG: json.dumps(["2013q3"]),
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
        SimpleNamespace(
            status=dg.DagsterRunStatus.SUCCESS,
            tags={"dagster/partition": "2013q4"},
        ),
        SimpleNamespace(
            status=dg.DagsterRunStatus.SUCCESS,
            tags={"dagster/partition": "2013q3"},
        ),
    ]

    run_request = sensors.ferceqr_deployment_sensor._run_status_sensor_fn(context)

    assert run_request.run_key == "ferceqr_deployment_success_backfill:bf-123"
    assert run_request.asset_selection == [dg.AssetKey("deploy_ferceqr")]
    assert run_request.tags == {
        deploy_ferceqr.FERCEQR_SOURCE_PARTITION_TAG: "2013q3",
        deploy_ferceqr.FERCEQR_SOURCE_PARTITIONS_TAG: json.dumps(["2013q3", "2013q4"]),
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
        deploy_ferceqr.FERCEQR_SOURCE_PARTITIONS_TAG: json.dumps(["2013q4"]),
        deploy_ferceqr.FERCEQR_SOURCE_RUN_ID_TAG: "run-456",
        deploy_ferceqr.FERCEQR_SOURCE_RUN_STATUS_TAG: "FAILURE",
    }


def test_deploy_ferceqr_success_path_writes_success_and_notifies(mocker, tmp_path):
    """Successful deployment should write outputs to fallback path, notify Slack, and mark FERCEQR_SUCCESS."""
    source_root = tmp_path / "source"
    deploy_root = tmp_path / "deploy"
    deploy_context = _build_deploy_context(
        tmp_path, mocker, targets=[UPath(deploy_root)]
    )
    (tmp_path / "FERCEQR_FAILURE").write_text("stale failure")
    notify_slack = mocker.patch.object(
        deploy_ferceqr, "_notify_slack_deployments_channel"
    )
    frictionless = mocker.Mock()
    mock_package = mocker.Mock()
    frictionless.to_json.return_value = "{}"
    mock_package.to_frictionless.return_value = frictionless

    mocker.patch.object(deploy_ferceqr, "PUDL_PACKAGE", mock_package)
    mocker.patch.object(
        deploy_ferceqr,
        "_get_source_partitions",
        return_value=["2013q3", "2013q4"],
    )
    mocker.patch.object(
        deploy_ferceqr,
        "_get_source_run_step_status_summary",
        return_value=(
            {
                "core_ferceqr__contracts": {
                    "2013q3": "SUCCESS",
                    "2013q4": "SUCCESS",
                },
                "core_ferceqr__transactions": {
                    "2013q3": "SUCCESS",
                    "2013q4": "SUCCESS",
                },
            },
            "run-123",
            "SUCCESS",
            "2013q3",
            "01:23:45",
        ),
    )

    class FakeParquetData:
        def __init__(self, table_name: str):
            self.parquet_directory = source_root / table_name

    mocker.patch.object(deploy_ferceqr, "ParquetData", FakeParquetData)

    for table_name in deploy_ferceqr.FERCEQR_TRANSFORM_ASSETS:
        table_dir = source_root / table_name
        table_dir.mkdir(parents=True)
        (table_dir / "2013q3.parquet").write_bytes(b"q3")
        (table_dir / "2013q4.parquet").write_bytes(b"q4")
        (table_dir / "2014q1.parquet").write_bytes(b"q1")

    deploy_ferceqr.deploy_ferceqr(deploy_context)

    assert (tmp_path / "FERCEQR_SUCCESS").exists()
    assert not (tmp_path / "FERCEQR_FAILURE").exists()

    for table_name in deploy_ferceqr.FERCEQR_TRANSFORM_ASSETS:
        assert sorted(
            path.name for path in (deploy_root / table_name).glob("*.parquet")
        ) == [
            "2013q3.parquet",
            "2013q4.parquet",
        ]

    mock_package.to_frictionless.assert_called_once_with(
        include_pattern=r"core_ferceqr.*"
    )

    notify_slack.assert_called_once()
    sent_message = notify_slack.call_args.kwargs["message"]
    assert "core_ferceqr__contracts" in sent_message


def test_deploy_ferceqr_requires_source_partitions(mocker, tmp_path):
    """Deployment should fail closed if the run is missing source partition tags."""
    deploy_context = _build_deploy_context(tmp_path, mocker)

    with pytest.raises(RuntimeError, match="missing source partitions"):
        deploy_ferceqr.deploy_ferceqr(deploy_context)


def test_handle_ferceqr_deployment_failure_writes_failure_and_notifies(
    mocker, tmp_path
):
    """Failure handler should notify Slack and write the FAILURE sentinel file."""
    deploy_context = _build_deploy_context(tmp_path, mocker)
    (tmp_path / "FERCEQR_SUCCESS").write_text("stale success")
    notify_slack = mocker.patch.object(
        deploy_ferceqr, "_notify_slack_deployments_channel"
    )

    deploy_ferceqr.handle_ferceqr_deployment_failure(deploy_context)

    assert (tmp_path / "FERCEQR_FAILURE").exists()
    assert not (tmp_path / "FERCEQR_SUCCESS").exists()
    notify_slack.assert_called_once()


def test_build_message_includes_asset_partition_status_table():
    """Verbose message includes source partition and the asset/partition status table."""
    message = deploy_ferceqr.build_ferceqr_deployment_markdown_message(
        deploy_ferceqr.FercEqrDeploymentNotificationPayload(
            outcome="FAILURE",
            build_id="build-789",
            distribution_paths=None,
            deployed_partitions=["2013q3", "2013q4"],
            source_run_id="run-789",
            source_run_duration="00:12:34",
            source_run_status="FAILURE",
            source_partition="2013q4",
            asset_partition_statuses={
                "core_ferceqr__transactions": {
                    "2013q3": "SUCCESS",
                    "2013q4": "FAILURE",
                },
                "core_ferceqr__quarterly_identity": {
                    "2013q3": "SKIPPED",
                },
            },
        )
    )

    print("\nFERCEQR deployment notification example:\n")
    print(message)

    assert "2013q3" in message
    assert "2013q4" in message
    assert "core_ferceqr__transactions" in message
