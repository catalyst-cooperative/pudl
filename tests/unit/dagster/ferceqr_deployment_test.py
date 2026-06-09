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
    zulip_mock = mocker.Mock(spec=["send_stream_message"])
    return dg.build_asset_context(
        resources={
            "pudl_paths": SimpleNamespace(pudl_output=tmp_path),
            "ferceqr_deployment_targets": deployment_resource,
            "zulip_notification": zulip_mock,
        }
    )


def test_ferceqr_success_sensor_skips_runs_without_backfill_tag(mocker):
    """Success sensor should skip single runs that are not part of a backfill."""
    context = mocker.Mock()
    context.dagster_run.run_id = "run-123"
    context.dagster_run.tags = {"dagster/partition": "2013q3"}

    result = sensors.ferceqr_success_sensor._run_status_sensor_fn(context)

    assert isinstance(result, dg.SkipReason)
    assert "no backfill" in result.skip_message or "backfill" in result.skip_message


def test_ferceqr_success_sensor_skips_while_backfill_still_running(mocker):
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

    result = sensors.ferceqr_success_sensor._run_status_sensor_fn(context)

    assert isinstance(result, dg.SkipReason)
    assert "still in progress" in result.skip_message


def test_ferceqr_success_sensor_skips_when_backfill_has_failures(mocker):
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

    result = sensors.ferceqr_success_sensor._run_status_sensor_fn(context)

    assert isinstance(result, dg.SkipReason)
    assert "failed or canceled" in result.skip_message


def test_ferceqr_failure_sensor_skips_non_backfill_runs(mocker):
    """Failure sensor should skip single runs that are not part of a backfill."""
    context = mocker.Mock()
    context.dagster_run.run_id = "run-456"
    context.dagster_run.tags = {"dagster/partition": "2013q4"}

    result = sensors.ferceqr_failure_sensor._run_status_sensor_fn(context)

    assert isinstance(result, dg.SkipReason)


def test_ferceqr_failure_sensor_skips_while_backfill_running(mocker):
    """Failure sensor should skip if any backfill runs are still non-terminal."""
    context = mocker.Mock()
    context.dagster_run.run_id = "run-456"
    context.dagster_run.job_name = "ferceqr"
    context.dagster_run.tags = {
        "dagster/partition": "2013q4",
        sensors.FERCEQR_BACKFILL_TAG: "bf-456",
    }
    context.instance.get_runs.return_value = [
        SimpleNamespace(status=dg.DagsterRunStatus.FAILURE),
        SimpleNamespace(status=dg.DagsterRunStatus.STARTED),
    ]

    result = sensors.ferceqr_failure_sensor._run_status_sensor_fn(context)

    assert isinstance(result, dg.SkipReason)
    assert "still in progress" in result.skip_message


def test_ferceqr_failure_sensor_skips_when_all_backfill_runs_succeeded(mocker):
    """Failure sensor should skip if all backfill runs succeeded (success sensor handles it)."""
    context = mocker.Mock()
    context.dagster_run.run_id = "run-456"
    context.dagster_run.job_name = "ferceqr"
    context.dagster_run.tags = {
        "dagster/partition": "2013q4",
        sensors.FERCEQR_BACKFILL_TAG: "bf-456",
    }
    context.instance.get_runs.return_value = [
        SimpleNamespace(
            status=dg.DagsterRunStatus.SUCCESS,
            tags={"dagster/partition": "2013q3"},
        ),
        SimpleNamespace(
            status=dg.DagsterRunStatus.SUCCESS,
            tags={"dagster/partition": "2013q4"},
        ),
    ]

    result = sensors.ferceqr_failure_sensor._run_status_sensor_fn(context)

    assert isinstance(result, dg.SkipReason)
    assert "no failed runs" in result.skip_message


def test_ferceqr_failure_sensor_backfill_with_failures_aggregated(mocker):
    """Failure sensor should produce an aggregated RunRequest when all backfill runs are terminal and some failed."""
    context = mocker.Mock()
    context.dagster_run.run_id = "run-456"
    context.dagster_run.job_name = "ferceqr"
    context.dagster_run.tags = {
        "dagster/partition": "2013q4",
        sensors.FERCEQR_BACKFILL_TAG: "bf-456",
    }
    context.instance.get_runs.return_value = [
        SimpleNamespace(
            status=dg.DagsterRunStatus.SUCCESS,
            tags={"dagster/partition": "2013q3"},
        ),
        SimpleNamespace(
            status=dg.DagsterRunStatus.FAILURE,
            tags={"dagster/partition": "2013q4"},
        ),
    ]

    run_request = sensors.ferceqr_failure_sensor._run_status_sensor_fn(context)

    assert run_request.run_key == "ferceqr_deployment_failure_backfill:bf-456"
    assert run_request.asset_selection == [dg.AssetKey("handle_ferceqr_failure")]
    assert run_request.tags == {
        deploy_ferceqr.FERCEQR_SOURCE_PARTITIONS_TAG: json.dumps(["2013q3", "2013q4"]),
        deploy_ferceqr.FERCEQR_SOURCE_RUN_ID_TAG: "run-456",
    }


def test_ferceqr_success_sensor_backfill_success_uses_backfill_run_key(mocker):
    """Success sensor should trigger once per completed successful backfill with all partitions."""
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

    run_request = sensors.ferceqr_success_sensor._run_status_sensor_fn(context)

    assert run_request.run_key == "ferceqr_deployment_success_backfill:bf-123"
    assert run_request.asset_selection == [dg.AssetKey("deploy_ferceqr")]
    assert run_request.tags == {
        deploy_ferceqr.FERCEQR_SOURCE_PARTITIONS_TAG: json.dumps(["2013q3", "2013q4"]),
        deploy_ferceqr.FERCEQR_SOURCE_RUN_ID_TAG: "run-123",
    }


def test_deploy_ferceqr_success_path_writes_success_and_notifies(mocker, tmp_path):
    """Successful deployment should write outputs to fallback path, notify Zulip, and mark FERCEQR_SUCCESS."""
    source_root = tmp_path / "source"
    deploy_root = tmp_path / "deploy"
    deploy_context = _build_deploy_context(
        tmp_path, mocker, targets=[UPath(deploy_root)]
    )
    (tmp_path / "FERCEQR_FAILURE").write_text("stale failure")
    zulip_mock = deploy_context.resources.zulip_notification
    frictionless = mocker.Mock()
    mock_package = mocker.Mock()
    frictionless.to_json.return_value = "{}"
    mock_package.to_frictionless.return_value = frictionless

    mocker.patch.object(deploy_ferceqr, "PUDL_PACKAGE", mock_package)

    # Provide source partitions via context.run so deploy_ferceqr can read them.
    mocker.patch.object(
        type(deploy_context),
        "run",
        new_callable=mocker.PropertyMock,
        return_value=SimpleNamespace(
            tags={
                deploy_ferceqr.FERCEQR_SOURCE_PARTITIONS_TAG: json.dumps(
                    ["2013q3", "2013q4"]
                )
            }
        ),
    )

    # Mock the notification builder to avoid needing a full context mock.
    mocker.patch.object(
        deploy_ferceqr,
        "build_ferceqr_notification",
        return_value="mock notification containing core_ferceqr__contracts",
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

    zulip_mock.send_stream_message.assert_called_once()
    sent_content = zulip_mock.send_stream_message.call_args.kwargs["content"]
    assert "core_ferceqr__contracts" in sent_content


def test_deploy_ferceqr_requires_source_partitions(mocker, tmp_path):
    """Deployment should fail closed if the run is missing source partition tags."""
    deploy_context = _build_deploy_context(tmp_path, mocker)

    with pytest.raises(RuntimeError, match="no deployable partitions"):
        deploy_ferceqr.deploy_ferceqr(deploy_context)


def test_handle_ferceqr_failure_writes_failure_and_notifies(mocker, tmp_path):
    """Failure handler should notify Zulip and write the FAILURE sentinel file."""
    deploy_context = _build_deploy_context(tmp_path, mocker)
    (tmp_path / "FERCEQR_SUCCESS").write_text("stale success")
    zulip_mock = deploy_context.resources.zulip_notification

    mocker.patch.object(
        deploy_ferceqr,
        "build_ferceqr_notification",
        return_value="mock failure notification",
    )

    deploy_ferceqr.handle_ferceqr_failure(deploy_context)

    assert (tmp_path / "FERCEQR_FAILURE").exists()
    assert not (tmp_path / "FERCEQR_SUCCESS").exists()
    zulip_mock.send_stream_message.assert_called_once()


def test_build_message_includes_asset_partition_status_table(mocker):
    """Notification markdown includes the asset/partition status table."""
    statuses = {
        "core_ferceqr__transactions": {
            "2013q3": "SUCCESS",
            "2013q4": "FAILURE",
        },
        "core_ferceqr__quarterly_identity": {
            "2013q3": "SKIPPED",
        },
    }

    table = deploy_ferceqr._markdown_step_status_table(
        asset_partition_statuses=statuses,
        partitions=["2013q3", "2013q4"],
    )

    assert "2013q3" in table
    assert "2013q4" in table
    assert "core_ferceqr__transactions" in table
    assert ":check:" in table
    assert ":x:" in table
    assert ":ghost:" in table
