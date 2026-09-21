"""Unit tests for FERCEQR deployment sensors and deployment asset handlers."""

import json
from pathlib import Path
from types import SimpleNamespace

import dagster as dg
import pytest
from fsspec.implementations.local import LocalFileSystem
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
    assert result.skip_message is not None
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
    assert result.skip_message is not None
    assert skip_text in result.skip_message


@pytest.mark.parametrize(
    "sensor_fn, backfill_statuses, expected_run_key_prefix, expected_asset",
    [
        (
            sensors.ferceqr_success_sensor,
            [dg.DagsterRunStatus.SUCCESS, dg.DagsterRunStatus.FAILURE],
            "ferceqr_deployment_failure_backfill",
            "handle_ferceqr_failure",
        ),
        (
            sensors.ferceqr_failure_sensor,
            [dg.DagsterRunStatus.SUCCESS, dg.DagsterRunStatus.SUCCESS],
            "ferceqr_deployment_success_backfill",
            "deploy_ferceqr",
        ),
    ],
)
def test_sensors_converge_to_same_run_key_in_race_condition(
    mocker, sensor_fn, backfill_statuses, expected_run_key_prefix, expected_asset
):
    """Both sensors produce the same run_key when they race on the same completed backfill.

    Because both sensors share a single underlying function, if both happen to fire
    simultaneously (e.g. the last success and last failure land at the same time), each
    independently determines the correct outcome from the terminal run statuses and
    produces a RunRequest with the same run_key prefix. Dagster deduplicates by run_key,
    so exactly one downstream run is launched regardless of how many sensors fire.
    """
    mocker.patch.object(sensors, "logger", mocker.Mock())
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

    assert isinstance(result, dg.RunRequest)
    assert result.run_key == f"{expected_run_key_prefix}:bf-123"
    assert result.asset_selection == [dg.AssetKey(expected_asset)]


def test_ferceqr_failure_sensor_backfill_with_failures_aggregated(mocker):
    """Failure sensor should produce an aggregated RunRequest when all backfill runs are terminal and some failed."""
    mocker.patch.object(sensors, "logger", mocker.Mock())
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
    """A successful deploy lands outputs in the final layout, snapshots the previous
    deployment, notifies Zulip, and writes FERCEQR_SUCCESS."""
    source_root = tmp_path / "source"
    deploy_root = tmp_path / "deploy"
    deploy_context = _build_deploy_context(
        tmp_path, mocker, targets=[UPath(deploy_root)]
    )
    (tmp_path / "FERCEQR_FAILURE").write_text("stale failure")

    zulip_mock = _mock_deploy_dependencies(
        mocker, deploy_context, source_root, ["2013q3", "2013q4"]
    )

    # An extra built partition that is NOT in the run tags must not be deployed.
    for table_name in deploy_ferceqr.FERCEQR_TRANSFORM_ASSETS:
        (source_root / table_name / "2014q1.parquet").write_bytes(b"q1")

    # A previous deployment already occupies the final prefix.
    old_file = deploy_root / "core_ferceqr__contracts" / "2012q4.parquet"
    old_file.parent.mkdir(parents=True)
    old_file.write_bytes(b"previous build")

    deploy_ferceqr.deploy_ferceqr(deploy_context)

    assert (tmp_path / "FERCEQR_SUCCESS").exists()
    assert not (tmp_path / "FERCEQR_FAILURE").exists()

    # The built partitions land in the final layout. Promotion merges into the
    # existing prefix rather than replacing it, so 2012q4 is still there and the
    # unrequested 2014q1 was never deployed.
    for table_name in deploy_ferceqr.FERCEQR_TRANSFORM_ASSETS:
        names = {p.name for p in (deploy_root / table_name).glob("*.parquet")}
        assert {"2013q3.parquet", "2013q4.parquet"} <= names
        assert "2014q1.parquet" not in names
    assert (deploy_root / "core_ferceqr__contracts" / "2012q4.parquet").exists()
    assert (deploy_root / deploy_ferceqr.DEPLOYED_DATAPACKAGE_FILENAME).exists()

    # The previous deployment was snapshotted for rollback, and staging is gone.
    assert (
        deploy_root.parent
        / deploy_ferceqr.PREVIOUS_DIRNAME
        / "core_ferceqr__contracts"
        / "2012q4.parquet"
    ).exists()
    assert not any(
        d.name.startswith("._staging_") for d in deploy_root.parent.iterdir()
    )

    zulip_mock.send_stream_message.assert_called_once()
    assert (
        "core_ferceqr__contracts"
        in zulip_mock.send_stream_message.call_args.kwargs["content"]
    )


def test_deploy_ferceqr_merge_overwrites_same_name_and_keeps_other_files(
    mocker, tmp_path
):
    """Re-deploying a partition replaces that file; unrelated live files survive."""
    source_root = tmp_path / "source"
    deploy_root = tmp_path / "deploy"
    deploy_context = _build_deploy_context(
        tmp_path, mocker, targets=[UPath(deploy_root)]
    )
    _mock_deploy_dependencies(mocker, deploy_context, source_root, ["2013q3"])

    live = deploy_root / "core_ferceqr__contracts"
    live.mkdir(parents=True)
    (live / "2013q3.parquet").write_bytes(b"stale build")
    (live / "2012q4.parquet").write_bytes(b"untouched")

    deploy_ferceqr.deploy_ferceqr(deploy_context)

    new_bytes = (
        source_root / "core_ferceqr__contracts" / "2013q3.parquet"
    ).read_bytes()
    assert (live / "2013q3.parquet").read_bytes() == new_bytes
    assert (live / "2012q4.parquet").read_bytes() == b"untouched"


def test_deploy_ferceqr_first_deployment_needs_no_existing_target(mocker, tmp_path):
    """A target that does not exist yet is created, with nothing to snapshot."""
    source_root = tmp_path / "source"
    deploy_root = tmp_path / "deploy"
    deploy_context = _build_deploy_context(
        tmp_path, mocker, targets=[UPath(deploy_root)]
    )
    _mock_deploy_dependencies(mocker, deploy_context, source_root, ["2013q3"])

    deploy_ferceqr.deploy_ferceqr(deploy_context)

    assert (deploy_root / "core_ferceqr__contracts" / "2013q3.parquet").exists()
    assert (deploy_root / deploy_ferceqr.DEPLOYED_DATAPACKAGE_FILENAME).exists()
    assert not (tmp_path / deploy_ferceqr.PREVIOUS_DIRNAME).exists()


def test_deploy_ferceqr_no_targets_writes_datapackage_and_skips_publish(
    mocker, tmp_path
):
    """With no deployment targets configured (deployment_mode "none"), the build
    is a success and the datapackage is still written for review, but nothing is
    published -- and it is not an error."""
    source_root = tmp_path / "source"
    deploy_context = _build_deploy_context(tmp_path, mocker, targets=None)
    (tmp_path / "FERCEQR_FAILURE").write_text("stale failure")
    _mock_deploy_dependencies(mocker, deploy_context, source_root, ["2013q3", "2013q4"])
    notification = mocker.patch.object(
        deploy_ferceqr,
        "build_ferceqr_notification",
        return_value="build succeeded, deployment skipped",
    )

    deploy_ferceqr.deploy_ferceqr(deploy_context)

    assert (tmp_path / "FERCEQR_SUCCESS").exists()
    assert not (tmp_path / "FERCEQR_FAILURE").exists()
    # Datapackage is written even though nothing is published.
    assert (tmp_path / deploy_ferceqr.DATAPACKAGE_FILENAME).exists()
    # No staging directories were created -- nothing was uploaded.
    assert not any(p.name.startswith("._staging_") for p in tmp_path.iterdir())
    notification.assert_called_once_with(deploy_context, outcome="SKIPPED")
    deploy_context.resources.zulip_notification.send_stream_message.assert_called_once()


def test_deploy_ferceqr_missing_partition_fails_closed(mocker, tmp_path):
    """A missing Parquet partition aborts the deploy before anything is uploaded."""
    source_root = tmp_path / "source"
    deploy_root = tmp_path / "deploy"
    deploy_root.mkdir()
    deploy_context = _build_deploy_context(
        tmp_path, mocker, targets=[UPath(deploy_root)]
    )
    _mock_deploy_dependencies(mocker, deploy_context, source_root, ["2013q3"])
    mocker.patch.object(deploy_ferceqr, "logger", mocker.Mock())

    # Delete one of the built partition files after the fact.
    (source_root / "core_ferceqr__transactions" / "2013q3.parquet").unlink()

    with pytest.raises(FileNotFoundError, match="2013q3"):
        deploy_ferceqr.deploy_ferceqr(deploy_context)

    assert (tmp_path / "FERCEQR_FAILURE").exists()
    assert list(deploy_root.iterdir()) == []


def test_deploy_ferceqr_staging_mismatch_aborts_before_promote(mocker, tmp_path):
    """If the staged objects do not match the local outputs, the target is left
    untouched, staging is cleaned up, and a failure is reported."""
    source_root = tmp_path / "source"
    deploy_root = tmp_path / "deploy"
    deploy_root.mkdir()
    deploy_context = _build_deploy_context(
        tmp_path, mocker, targets=[UPath(deploy_root)]
    )
    zulip_mock = _mock_deploy_dependencies(
        mocker, deploy_context, source_root, ["2013q3"]
    )
    mocker.patch.object(deploy_ferceqr, "logger", mocker.Mock())

    real_put = LocalFileSystem.put
    dropped: list[str] = []

    def _drops_first_file(self, lpath, rpath, *args, **kwargs):
        """Silently lose the first file it is asked to upload."""
        if not dropped:
            dropped.append(lpath)
            return None
        return real_put(self, lpath, rpath, *args, **kwargs)

    mocker.patch.object(LocalFileSystem, "put", _drops_first_file)

    with pytest.raises(RuntimeError, match="do not match the local outputs"):
        deploy_ferceqr.deploy_ferceqr(deploy_context)

    assert (tmp_path / "FERCEQR_FAILURE").exists()
    assert list(deploy_root.iterdir()) == []
    assert not any(d.name.startswith("._staging_") for d in tmp_path.iterdir())
    zulip_mock.send_stream_message.assert_called_once()


def test_deploy_ferceqr_requires_source_partitions(mocker, tmp_path):
    """With targets configured but no source partition tags, fail closed."""
    deploy_context = _build_deploy_context(
        tmp_path, mocker, targets=[UPath(tmp_path / "deploy")]
    )
    mocker.patch.object(deploy_ferceqr, "logger", mocker.Mock())

    with pytest.raises(RuntimeError, match="no deployable partitions"):
        deploy_ferceqr.deploy_ferceqr(deploy_context)


def test_handle_ferceqr_failure_writes_failure_and_notifies(mocker, tmp_path):
    """Failure handler should notify Zulip and write the FAILURE sentinel file."""
    deploy_context = _build_deploy_context(tmp_path, mocker)
    mocker.patch.object(deploy_ferceqr, "logger", mocker.Mock())
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


def test_deploy_ferceqr_promote_failure_cleans_up_and_reports(mocker, tmp_path):
    """A failure during promotion cleans up staging and reports FERCEQR_FAILURE."""
    source_root = tmp_path / "source"
    deploy_root = tmp_path / "deploy"
    deploy_root.mkdir()
    deploy_context = _build_deploy_context(
        tmp_path, mocker, targets=[UPath(deploy_root)]
    )
    _mock_deploy_dependencies(mocker, deploy_context, source_root, ["2013q3"])
    mocker.patch.object(deploy_ferceqr, "logger", mocker.Mock())
    mocker.patch.object(
        deploy_ferceqr,
        "_promote_target",
        side_effect=RuntimeError("promotion failed before move"),
    )

    with pytest.raises(RuntimeError, match="promotion failed"):
        deploy_ferceqr.deploy_ferceqr(deploy_context)

    assert not any(d.name.startswith("._staging_") for d in tmp_path.iterdir())
    assert not (deploy_root / deploy_ferceqr.DEPLOYED_DATAPACKAGE_FILENAME).exists()
    assert (tmp_path / "FERCEQR_FAILURE").exists()


# ---------------------------------------------------------------------------
# Deployment helper tests
# ---------------------------------------------------------------------------


def test_deployment_targets_builds_sibling_scratch_prefixes(mocker, tmp_path):
    """_deployment_targets derives the staging/previous prefixes from the target URI.

    This is a unit test rather than a behavioral one because the string handling
    (sibling prefixes, BUILD_ID suffix, data/meta split) is fiddly and awkward to
    pin down through the asset.
    """
    mocker.patch.dict(deploy_ferceqr.os.environ, {"BUILD_ID": "build-abc"}, clear=False)
    deploy_root = tmp_path / "dist" / "ferceqr"
    (target,) = deploy_ferceqr._deployment_targets([UPath(deploy_root)])
    assert target.final == UPath(deploy_root)
    assert target.staging == UPath(tmp_path / "dist" / "._staging_build-abc")
    assert target.previous == UPath(tmp_path / "dist" / "._ferceqr_previous")
    assert target.staging_data == target.staging / "data"
    assert target.staging_meta == target.staging / "meta"


def test_stage_target_s3_uploads_through_boto3_and_verifies(mocker, tmp_path):
    """S3 targets upload the whole batch via s3_transfer, then verify by listing."""
    src = tmp_path / "src"
    src.mkdir()
    parquet = src / "2013q3.parquet"
    parquet.write_bytes(b"parquet")
    datapackage = src / deploy_ferceqr.DATAPACKAGE_FILENAME
    datapackage.write_text("{}")
    target = deploy_ferceqr._DeploymentTarget(
        final=UPath(tmp_path / "dist" / "ferceqr"), build_id="b"
    )

    uploaded: list[tuple[Path, str]] = []

    def _fake_upload(files):
        # Stand in for boto3: "S3" is the local tmp_path here.
        for path, dest in files:
            uploaded.append((path, dest))
            Path(dest).parent.mkdir(parents=True, exist_ok=True)
            Path(dest).write_bytes(path.read_bytes())

    mocker.patch.object(deploy_ferceqr, "_is_s3", return_value=True)
    mocker.patch.object(deploy_ferceqr, "s3_upload_files", side_effect=_fake_upload)
    with deploy_ferceqr.ThreadPoolExecutor() as executor:
        deploy_ferceqr._stage_target(
            target, {"core_ferceqr__contracts": [parquet]}, datapackage, executor
        )

    assert {Path(dest).name for _, dest in uploaded} == {
        "2013q3.parquet",
        deploy_ferceqr.DEPLOYED_DATAPACKAGE_FILENAME,
    }


def test_copy_tree_s3_copies_every_object_in_one_batch(mocker, tmp_path):
    """On S3 the copy is one boto3 batch mapping each source object to its dest."""
    src = tmp_path / "staging" / "data"
    (src / "table_a").mkdir(parents=True)
    (src / "table_a" / "2013q3.parquet").write_bytes(b"a")
    (src / "table_b").mkdir()
    (src / "table_b" / "2013q3.parquet").write_bytes(b"bb")
    dst = UPath(tmp_path / "final")

    mocker.patch.object(deploy_ferceqr, "_is_s3", return_value=True)
    copy = mocker.patch.object(deploy_ferceqr, "s3_copy_objects")
    deploy_ferceqr._copy_tree(UPath(src), dst, mocker.Mock())

    (pairs,) = copy.call_args.args
    assert sorted(pairs) == [
        (f"s3://{src}/table_a/2013q3.parquet", f"{dst}/table_a/2013q3.parquet"),
        (f"s3://{src}/table_b/2013q3.parquet", f"{dst}/table_b/2013q3.parquet"),
    ]


def test_verify_staged_reports_missing_unexpected_and_wrong_size(tmp_path):
    """_verify_staged names every discrepancy between staging and local outputs."""
    target = deploy_ferceqr._DeploymentTarget(final=UPath(tmp_path / "f"), build_id="b")
    (target.staging_data / "t").mkdir(parents=True)
    (target.staging_data / "t" / "short.parquet").write_bytes(b"12")
    (target.staging_data / "t" / "extra.parquet").write_bytes(b"x")
    expected = {
        "data/t/short.parquet": 3,
        "data/t/absent.parquet": 1,
    }
    with pytest.raises(RuntimeError) as excinfo:
        deploy_ferceqr._verify_staged(target, expected)
    message = str(excinfo.value)
    assert "data/t/absent.parquet" in message  # missing
    assert "data/t/extra.parquet" in message  # unexpected
    assert "short.parquet (expected 3 bytes, found 2)" in message  # wrong size
