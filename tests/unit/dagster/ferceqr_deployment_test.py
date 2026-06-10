"""Unit tests for FERCEQR deployment sensors and deployment asset handlers."""

import json
from pathlib import Path
from types import SimpleNamespace

import dagster as dg
import pytest
from upath import UPath

from pudl.dagster import sensors
from pudl.dagster.assets.deploy import ferceqr as deploy_ferceqr
from pudl.dagster.resources import FercEqrDeploymentTargetConfig


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


def _mock_deploy_dependencies(mocker, deploy_context, source_root, source_partitions):
    """Set up shared mocks for deploy_ferceqr tests: PUDL_PACKAGE, ParquetData, run tags.

    Creates source parquet files under *source_root* and mocks context.run tags
    with *source_partitions*. Returns the mocked zulip resource for assertions.
    """
    frictionless = mocker.Mock()
    mock_package = mocker.Mock()

    def _to_json_side_effect(path=None):
        if path:
            with Path(path).open("w") as f:
                f.write("{}")
            return "{}"
        return "{}"

    frictionless.to_json.side_effect = _to_json_side_effect
    mock_package.to_frictionless.return_value = frictionless
    mocker.patch.object(deploy_ferceqr, "PUDL_PACKAGE", mock_package)

    mocker.patch.object(
        type(deploy_context),
        "run",
        new_callable=mocker.PropertyMock,
        return_value=SimpleNamespace(
            tags={
                deploy_ferceqr.FERCEQR_SOURCE_PARTITIONS_TAG: json.dumps(
                    source_partitions
                )
            }
        ),
    )
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
        for partition in source_partitions:
            (table_dir / f"{partition}.parquet").write_bytes(partition.encode())

    deploy_context.resources.zulip_notification.reset_mock()
    return deploy_context.resources.zulip_notification


@pytest.mark.parametrize(
    "sensor_fn",
    [sensors.ferceqr_success_sensor, sensors.ferceqr_failure_sensor],
)
def test_sensor_skips_runs_without_backfill_tag(mocker, sensor_fn):
    """Both sensors should skip single runs that are not part of a backfill."""
    context = mocker.Mock()
    context.dagster_run.run_id = "run-123"
    context.dagster_run.tags = {"dagster/partition": "2013q3"}

    result = sensor_fn._run_status_sensor_fn(context)

    assert isinstance(result, dg.SkipReason)
    assert "no backfill" in result.skip_message or "backfill" in result.skip_message


@pytest.mark.parametrize(
    "sensor_fn, backfill_statuses, skip_text",
    [
        (
            sensors.ferceqr_success_sensor,
            [dg.DagsterRunStatus.SUCCESS, dg.DagsterRunStatus.STARTED],
            "still in progress",
        ),
        (
            sensors.ferceqr_failure_sensor,
            [dg.DagsterRunStatus.FAILURE, dg.DagsterRunStatus.STARTED],
            "still in progress",
        ),
    ],
)
def test_sensor_skips_while_backfill_running(
    mocker, sensor_fn, backfill_statuses, skip_text
):
    """Both sensors should skip if any backfill runs are non-terminal."""
    context = mocker.Mock()
    context.dagster_run.run_id = "run-123"
    context.dagster_run.job_name = "ferceqr"
    context.dagster_run.tags = {
        "dagster/partition": "2013q3",
        sensors.DAGSTER_BACKFILL_TAG: "bf-123",
    }
    context.instance.get_runs.return_value = [
        SimpleNamespace(status=s) for s in backfill_statuses
    ]

    result = sensor_fn._run_status_sensor_fn(context)

    assert isinstance(result, dg.SkipReason)
    assert skip_text in result.skip_message


@pytest.mark.parametrize(
    "sensor_fn, backfill_statuses, skip_text",
    [
        (
            sensors.ferceqr_success_sensor,
            [dg.DagsterRunStatus.SUCCESS, dg.DagsterRunStatus.FAILURE],
            "failed or canceled",
        ),
        (
            sensors.ferceqr_failure_sensor,
            [dg.DagsterRunStatus.SUCCESS, dg.DagsterRunStatus.SUCCESS],
            "no failed runs",
        ),
    ],
)
def test_sensor_skips_when_outcome_handled_by_other_sensor(
    mocker, sensor_fn, backfill_statuses, skip_text
):
    """Sensors should defer to each other: success skips on failures, failure skips on successes."""
    context = mocker.Mock()
    context.dagster_run.run_id = "run-123"
    context.dagster_run.job_name = "ferceqr"
    context.dagster_run.tags = {
        "dagster/partition": "2013q3",
        sensors.DAGSTER_BACKFILL_TAG: "bf-123",
    }
    context.instance.get_runs.return_value = [
        SimpleNamespace(status=s, tags={"dagster/partition": "2013q3"})
        for s in backfill_statuses
    ]

    result = sensor_fn._run_status_sensor_fn(context)

    assert isinstance(result, dg.SkipReason)
    assert skip_text in result.skip_message


def test_ferceqr_failure_sensor_backfill_with_failures_aggregated(mocker):
    """Failure sensor should produce an aggregated RunRequest when all backfill runs are terminal and some failed."""
    context = mocker.Mock()
    context.dagster_run.run_id = "run-456"
    context.dagster_run.job_name = "ferceqr"
    context.dagster_run.tags = {
        "dagster/partition": "2013q4",
        sensors.DAGSTER_BACKFILL_TAG: "bf-456",
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
        sensors.DAGSTER_BACKFILL_TAG: "bf-123",
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
    """Successful deployment should write outputs to target, notify Zulip, and mark FERCEQR_SUCCESS."""
    source_root = tmp_path / "source"
    deploy_root = tmp_path / "deploy"
    deploy_context = _build_deploy_context(
        tmp_path, mocker, targets=[UPath(deploy_root)]
    )
    (tmp_path / "FERCEQR_FAILURE").write_text("stale failure")
    source_partitions = ["2013q3", "2013q4"]

    zulip_mock = _mock_deploy_dependencies(
        mocker, deploy_context, source_root, source_partitions
    )

    # Add an extra partition that should NOT be deployed.
    for table_name in deploy_ferceqr.FERCEQR_TRANSFORM_ASSETS:
        (source_root / table_name / "2014q1.parquet").write_bytes(b"q1")

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


# ---------------------------------------------------------------------------
# Staging helper tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "build_id,expected_pattern",
    [
        ("build-abc123", "/._staging_build-abc123"),
        (None, "._staging_"),
    ],
)
def test_staging_path_naming(
    monkeypatch: pytest.MonkeyPatch, build_id: str | None, expected_pattern: str
) -> None:
    """_staging_path should use BUILD_ID when set, otherwise a random hex suffix."""
    if build_id is not None:
        monkeypatch.setenv("BUILD_ID", build_id)
    else:
        monkeypatch.delenv("BUILD_ID", raising=False)

    dist = deploy_ferceqr._staging_path(UPath("/base"))
    assert expected_pattern in str(dist)


def test_deploy_to_staging_writes_files(mocker, tmp_path):
    """_deploy_to_staging should copy parquet files and datapackage to the staging dir."""
    source_root = tmp_path / "source"
    deploy_root = tmp_path / "deploy"
    deploy_root.mkdir()
    deployment_resource = deploy_ferceqr.FercEqrDeploymentResource(
        deployment_targets=[
            FercEqrDeploymentTargetConfig(path=str(deploy_root)),
        ],
    )
    partitions = ["2013q3", "2013q4"]

    # Write the datapackage to disk, as deploy_ferceqr would.
    datapackage_path = tmp_path / "ferceqr_parquet_datapackage.json"
    datapackage_path.write_bytes(b'{"resources": []}')

    # Create source parquet files
    for table_name in deploy_ferceqr.FERCEQR_TRANSFORM_ASSETS:
        table_dir = source_root / table_name
        table_dir.mkdir(parents=True)
        (table_dir / "2013q3.parquet").write_bytes(b"q3")
        (table_dir / "2013q4.parquet").write_bytes(b"q4")

    class FakeParquetData:
        def __init__(self, table_name: str):
            self.parquet_directory = source_root / table_name

    mocker.patch.object(deploy_ferceqr, "ParquetData", FakeParquetData)
    staging_targets = deploy_ferceqr._deploy_to_staging(
        ferceqr_deployment=deployment_resource,
        source_partitions=partitions,
        datapackage_path=datapackage_path,
    )

    assert len(staging_targets) == 1
    staging_dir = staging_targets[0]
    assert str(staging_dir).startswith(str(tmp_path / "._staging_"))

    # Verify parquet files
    for table_name in deploy_ferceqr.FERCEQR_TRANSFORM_ASSETS:
        table_dir = staging_dir / table_name
        assert table_dir.is_dir()
        parquet_files = sorted(path.name for path in table_dir.glob("*.parquet"))
        assert parquet_files == ["2013q3.parquet", "2013q4.parquet"], (
            f"Unexpected files in {table_dir}: {parquet_files}"
        )

    # Verify datapackage was copied
    datapackage = staging_dir / "ferceqr_parquet_datapackage.json"
    assert datapackage.exists()
    assert datapackage.read_bytes() == b'{"resources": []}'


def test_deploy_to_staging_missing_source_raises(mocker, tmp_path):
    """_deploy_to_staging should raise FileNotFoundError if a source parquet is missing."""
    source_root = tmp_path / "source"
    deploy_root = tmp_path / "deploy"
    deploy_root.mkdir()
    deployment_resource = deploy_ferceqr.FercEqrDeploymentResource(
        deployment_targets=[
            FercEqrDeploymentTargetConfig(path=str(deploy_root)),
        ],
    )
    partitions = ["missing_quarter"]

    # Write the datapackage to disk.
    datapackage_path = tmp_path / "ferceqr_parquet_datapackage.json"
    datapackage_path.write_bytes(b"{}")

    # Create source without the partition file
    table_dir = source_root / deploy_ferceqr.FERCEQR_TRANSFORM_ASSETS[0]
    table_dir.mkdir(parents=True)

    class FakeParquetData:
        def __init__(self, table_name: str):
            self.parquet_directory = source_root / table_name

    mocker.patch.object(deploy_ferceqr, "ParquetData", FakeParquetData)
    with pytest.raises(FileNotFoundError, match="missing_quarter"):
        deploy_ferceqr._deploy_to_staging(
            ferceqr_deployment=deployment_resource,
            source_partitions=partitions,
            datapackage_path=datapackage_path,
        )


def test_promote_staging_moves_files_to_final_path(tmp_path):
    """_promote_staging should move staging contents to the final destination."""
    deploy_root = tmp_path / "deploy"
    deploy_root.mkdir()
    staging_dir = tmp_path / "._staging_test"
    staging_dir.mkdir()

    for table_name in deploy_ferceqr.FERCEQR_TRANSFORM_ASSETS:
        table_dir = staging_dir / table_name
        table_dir.mkdir(parents=True)
        (table_dir / "2013q3.parquet").write_bytes(b"q3")
        (table_dir / "2013q4.parquet").write_bytes(b"q4")

    (staging_dir / "ferceqr_parquet_datapackage.json").write_bytes(b"{}")

    deploy_ferceqr._promote_staging(
        staging_targets=[UPath(staging_dir)],
        resolved_targets=[UPath(deploy_root)],
    )

    # Staging dir should be gone
    assert not staging_dir.exists()

    # Files should be in the final path
    for table_name in deploy_ferceqr.FERCEQR_TRANSFORM_ASSETS:
        assert (deploy_root / table_name / "2013q3.parquet").exists()
        assert (deploy_root / table_name / "2013q4.parquet").exists()

    assert (deploy_root / "ferceqr_parquet_datapackage.json").exists()


def test_remove_staging_cleans_up_directory(tmp_path):
    """_remove_staging should delete the entire staging directory tree."""
    staging_dir = tmp_path / "._staging_test"
    staging_dir.mkdir()
    (staging_dir / "ferceqr_parquet_datapackage.json").write_bytes(b"{}")
    table_dir = staging_dir / "core_ferceqr__contracts"
    table_dir.mkdir()
    (table_dir / "2013q3.parquet").write_bytes(b"q3")

    deploy_ferceqr._remove_staging(UPath(staging_dir))

    assert not staging_dir.exists()


def test_deploy_ferceqr_staging_is_cleaned_up_on_failure(mocker, tmp_path):
    """If promotion fails, the staging directory should be removed."""
    source_root = tmp_path / "source"
    deploy_root = tmp_path / "deploy"
    deploy_root.mkdir()
    deploy_context = _build_deploy_context(
        tmp_path, mocker, targets=[UPath(deploy_root)]
    )

    _mock_deploy_dependencies(mocker, deploy_context, source_root, ["2013q3"])

    # Replace _promote_staging with one that fails before renaming.
    def _broken_promote(staging_targets, resolved_targets):
        for _staging_dir, _final_dir in zip(
            staging_targets, resolved_targets, strict=True
        ):
            raise RuntimeError("promotion failed before rename")

    mocker.patch.object(deploy_ferceqr, "_promote_staging", _broken_promote)

    with pytest.raises(RuntimeError, match="promotion failed"):
        deploy_ferceqr.deploy_ferceqr(deploy_context)

    # Staging directory should have been cleaned up from the sibling location
    staging_dirs = [d for d in tmp_path.iterdir() if d.name.startswith("._staging_")]
    assert staging_dirs == [], f"Staging directories left behind: {staging_dirs}"

    # Final deployment should not exist
    assert not (deploy_root / "ferceqr_parquet_datapackage.json").exists()
