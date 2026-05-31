"""Define deployment helper assets for publishing FERC EQR outputs.

These assets run during batch builds to publish transformed FERC EQR outputs,
notify Slack of success or failure, and create status files that tell the batch
job when deployment handling is complete.
"""

import json
import os
import traceback
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import dagster as dg
import pandas as pd
from slack_sdk import WebClient

from pudl.dagster.resources import (
    FercEqrDeploymentResource,
)
from pudl.helpers import ParquetData
from pudl.logging_helpers import get_logger
from pudl.metadata.classes import PUDL_PACKAGE
from pudl.workspace.setup import PudlPaths

logger = get_logger(__name__)

FERCEQR_SOURCE_RUN_ID_TAG = "ferceqr/source_run_id"
FERCEQR_SOURCE_RUN_STATUS_TAG = "ferceqr/source_run_status"
FERCEQR_SOURCE_PARTITION_TAG = "ferceqr/source_partition"
FERCEQR_SOURCE_PARTITIONS_TAG = "ferceqr/source_partitions"
FERCEQR_BACKFILL_TAG = "dagster/backfill"

FERCEQR_TRANSFORM_ASSETS = [
    "core_ferceqr__contracts",
    "core_ferceqr__transactions",
    "core_ferceqr__quarterly_identity",
    "core_ferceqr__quarterly_index_pub",
]


def _notify_slack_deployments_channel(
    message: str, attached_file_path: str | None = None
):
    """Send string message to PUDL deployments channel.

    Skips silently when ``SLACK_TOKEN`` is not set in the environment (e.g.
    during local development or pipeline tests).
    """
    if not (slack_token := os.getenv("SLACK_TOKEN")):
        logger.info("SLACK_TOKEN not set; skipping Slack notification.")
        return
    client = WebClient(token=slack_token)
    channel = "C03FHB9N0PQ"
    if attached_file_path is not None:
        client.files_upload_v2(
            channel=channel,
            file=attached_file_path,
            title=f"{os.getenv('BUILD_ID', 'no-build-id')} Status",
            initial_comment=message,
        )
    client.chat_postMessage(
        channel=channel,
        text=message,
    )


def _get_build_id() -> str:
    """Return the current build identifier for deployment notifications."""
    return os.getenv("BUILD_ID", "no-build-id")


def _get_logfile_pointer_markdown(build_id: str) -> str:
    """Return markdown links to logs for notifications."""
    return (
        f"- [Download ferceqr logs](https://storage.cloud.google.com/builds.catalyst.coop/ferceqr_logs/{build_id}.log)\n"
        f"- [Query ferceqr logs online](https://console.cloud.google.com/batch/jobsDetail/regions/us-west1/jobs/run-ferceqr-etl-{build_id}/logs?project=catalyst-cooperative-pudl)\n"
    )


def _write_status_file(
    status: Literal["FERCEQR_SUCCESS", "FERCEQR_FAILURE"], pudl_paths: PudlPaths
):
    """Notify build script that job is complete by creating a status file."""
    (Path(pudl_paths.pudl_output) / status).touch()


def _clear_status_files(pudl_paths: PudlPaths) -> None:
    """Remove any stale FERC EQR status files from the output directory."""
    for status_name in ("FERCEQR_SUCCESS", "FERCEQR_FAILURE"):
        status_path = Path(pudl_paths.pudl_output) / status_name
        status_path.unlink(missing_ok=True)


def _status_name(value: object) -> str:
    """Return normalized uppercase status name from a Dagster status object."""
    if (name := getattr(value, "name", None)) is not None:
        return str(name).upper()
    if (raw_value := getattr(value, "value", None)) is not None:
        return str(raw_value).upper()
    return str(value).split(".")[-1].upper()


def _format_elapsed_seconds(elapsed_seconds: float) -> str:
    """Format elapsed seconds as HH:MM:SS for Slack notifications."""
    total_seconds = max(int(elapsed_seconds), 0)
    elapsed_hours, remainder = divmod(total_seconds, 3600)
    elapsed_minutes, remaining_seconds = divmod(remainder, 60)
    return f"{elapsed_hours:02d}:{elapsed_minutes:02d}:{remaining_seconds:02d}"


def _get_run_duration(run_record: dg.RunRecord | None) -> str | None:
    """Return a formatted runtime for a Dagster run record when available."""
    if run_record is None:
        return None
    if run_record.start_time is None or run_record.end_time is None:
        return None
    return _format_elapsed_seconds(run_record.end_time - run_record.start_time)


def _get_source_run_step_status_summary(
    context: dg.AssetExecutionContext,
) -> tuple[dict[str, dict[str, str]], str | None, str | None, str | None, str | None]:
    """Return asset/partition step statuses and source-run metadata."""
    try:
        run = context.run
    except Exception:
        # Dagster direct-invocation contexts in tests may not have run metadata.
        # We intentionally treat this as missing source-run data for notifications.
        return {}, None, None, None, None

    run_tags = run.tags if run else {}
    source_run_id = run_tags.get(FERCEQR_SOURCE_RUN_ID_TAG)
    source_run_status = run_tags.get(FERCEQR_SOURCE_RUN_STATUS_TAG)
    source_partition = run_tags.get(FERCEQR_SOURCE_PARTITION_TAG)
    if not source_run_id:
        return {}, None, source_run_status, source_partition, None

    source_runs = []
    source_run_record = context.instance.get_run_record_by_id(source_run_id)
    source_run_duration = _get_run_duration(source_run_record)
    source_run = (
        source_run_record.dagster_run if source_run_record is not None else None
    )
    if source_run is not None:
        backfill_id = source_run.tags.get(FERCEQR_BACKFILL_TAG)
        if backfill_id:
            source_runs = context.instance.get_runs(
                filters=dg.RunsFilter(
                    job_name=source_run.job_name,
                    tags={FERCEQR_BACKFILL_TAG: backfill_id},
                )
            )
        else:
            source_runs = [source_run]

    if not source_runs:
        return (
            {},
            source_run_id,
            source_run_status,
            source_partition,
            source_run_duration,
        )

    asset_partition_statuses: dict[str, dict[str, str]] = {}
    for run_record in source_runs:
        default_partition = run_record.tags.get("dagster/partition") or source_partition
        for step in context.instance.get_run_step_stats(run_record.run_id):
            asset_name, partition_name = _parse_step_key(
                step_key=step.step_key,
                source_partition=default_partition,
            )
            asset_partition_statuses.setdefault(asset_name, {})[partition_name] = (
                _status_name(step.status)
            )

    return (
        asset_partition_statuses,
        source_run_id,
        source_run_status,
        source_partition,
        source_run_duration,
    )


def _get_source_partitions(context: dg.AssetExecutionContext) -> list[str]:
    """Return the source partitions attached to the deployment run tags."""
    try:
        run = context.run
    except Exception as exc:  # pragma: no cover - direct invocation fallback
        raise RuntimeError(
            "FERCEQR deployment run is missing source partitions."
        ) from exc

    run_tags = run.tags if run else {}
    source_partitions = run_tags.get(FERCEQR_SOURCE_PARTITIONS_TAG)
    if source_partitions is None:
        raise RuntimeError("FERCEQR deployment run is missing source partitions.")

    parsed_partitions = json.loads(source_partitions)
    if not isinstance(parsed_partitions, list) or not parsed_partitions:
        raise RuntimeError("FERCEQR deployment run has no deployable partitions.")
    if any(not isinstance(partition, str) for partition in parsed_partitions):
        raise RuntimeError("FERCEQR deployment run has invalid source partitions.")

    return parsed_partitions


def _parse_step_key(step_key: str, source_partition: str | None) -> tuple[str, str]:
    """Extract asset and partition details from a Dagster step key."""
    if "[" in step_key and step_key.endswith("]"):
        asset_name, raw_partition = step_key.rsplit("[", 1)
        return asset_name, raw_partition[:-1]

    return step_key, source_partition or "UNKNOWN"


def _format_step_status_markdown_table(
    asset_partition_statuses: dict[str, dict[str, str]],
    partitions: list[str] | None,
) -> str:
    """Format terminal step statuses as an asset-by-partition Markdown table."""
    if not asset_partition_statuses:
        partition_order = sorted(partitions or [])
        if partition_order:
            row = {"Asset": "NO_DATA"}
            for partition_name in partition_order:
                row[partition_name] = ":question:"
            table = pd.DataFrame([row])
        else:
            table = pd.DataFrame([{"Asset": "NO_DATA", "Status": ":question:"}])
        return table.to_markdown(index=False)

    status_symbols = {
        "FAILURE": ":x:",
        "SUCCESS": ":check:",
        "SKIPPED": ":ghost:",
        "UNKNOWN": ":question:",
        "NO_DATA": ":question:",
    }
    discovered_partitions = sorted(
        {
            partition_name
            for partition_statuses in asset_partition_statuses.values()
            for partition_name in partition_statuses
        }
    )
    partition_order = sorted(partitions or [])
    partition_order.extend(
        partition_name
        for partition_name in discovered_partitions
        if partition_name not in partition_order
    )

    rows = []
    for asset_name in sorted(asset_partition_statuses):
        row = {"Asset": asset_name}
        for partition_name in partition_order:
            status_name = asset_partition_statuses[asset_name].get(partition_name)
            row[partition_name] = status_symbols.get(
                status_name or "UNKNOWN", ":question:"
            )
        rows.append(row)

    table = pd.DataFrame(rows)
    return table.to_markdown(
        index=False,
        colalign=("left", *["center"] * (len(table.columns) - 1)),
    )


@dataclass(frozen=True)
class FercEqrDeploymentNotificationPayload:
    """Input payload for FERCEQR deployment notification markdown."""

    outcome: Literal["SUCCESS", "FAILURE"]
    build_id: str
    distribution_paths: list[str] | None
    deployed_partitions: list[str] | None
    source_run_id: str | None
    source_run_duration: str | None
    source_run_status: str | None
    source_partition: str | None
    asset_partition_statuses: dict[str, dict[str, str]]


def build_ferceqr_deployment_markdown_message(
    payload: FercEqrDeploymentNotificationPayload,
) -> str:
    """Build reusable Markdown notification text for deployment outcomes."""
    title = (
        "\n## :check: FERC EQR Deployment Succeeded"
        if payload.outcome == "SUCCESS"
        else "\n## :x: FERC EQR Deployment Failed"
    )
    lines = [title, ""]
    if payload.distribution_paths:
        lines.append("- Deployment Targets:")
        lines.extend(f"  - {path}" for path in payload.distribution_paths)
    lines.append(f"- Build ID: {payload.build_id}")
    if payload.source_run_id:
        lines.append(f"- Dagster run ID: `{payload.source_run_id}`")
    if payload.source_run_duration:
        lines.append(
            f"- :time: Dagster run duration: `[{payload.source_run_duration}]`"
        )
    lines.extend(
        [
            "",
            "### Asset / Partition Status",
            "- :check: = SUCCESS",
            "- :x: = FAILURE",
            "- :ghost: = SKIPPED",
            "- :question: = UNKNOWN / NO_DATA",
            "",
            _format_step_status_markdown_table(
                asset_partition_statuses=payload.asset_partition_statuses,
                partitions=payload.deployed_partitions,
            ),
        ]
    )

    lines.extend(["", "### Logs", _get_logfile_pointer_markdown(payload.build_id)])
    return "\n".join(lines)


def deployment_status_asset(
    handler: Callable,
) -> dg.AssetsDefinition:
    """Create a wrapper for deployment handler assets.

    This is useful to gracefully handle errors if the deployment status assets fail
    for any reason. When these assets fail, sometimes the logs don't show up in the
    batch job appropriately, and the status file never gets created, so the job keeps
    running until it eventually times out.
    """

    @dg.asset(
        name=handler.__name__,
        required_resource_keys={
            "pudl_paths",
            "ferceqr_deployment_targets",
        },
    )
    def _status_handler_asset(context: dg.AssetExecutionContext):
        try:
            _clear_status_files(context.resources.pudl_paths)
            handler(context)
        except Exception:
            logger.error("FERC EQR deployment handler failed!")
            logger.error(traceback.format_exc())
            _write_status_file("FERCEQR_FAILURE", context.resources.pudl_paths)
            raise

    return _status_handler_asset


@deployment_status_asset
def deploy_ferceqr(context: dg.AssetExecutionContext):
    """Publish EQR outputs to configured deployment targets."""
    pudl_paths: PudlPaths = context.resources.pudl_paths
    deployment: FercEqrDeploymentResource = context.resources.ferceqr_deployment_targets
    (
        asset_partition_statuses,
        source_run_id,
        source_run_status,
        source_partition,
        source_run_duration,
    ) = _get_source_run_step_status_summary(context)
    source_partitions = _get_source_partitions(context)

    targets = deployment.resolved_targets()

    # Get 'datapackage.json' data to write to deployment targets.
    datapackage_bytes = (
        PUDL_PACKAGE.to_frictionless(include_pattern=r"core_ferceqr.*")
        .to_json()
        .encode(encoding="utf-8")
    )

    logger.info("Build successful, deploying ferceqr data.")
    for dest in targets:
        dest.mkdir(parents=True, exist_ok=True)
        for table in FERCEQR_TRANSFORM_ASSETS:
            logger.info(f"Copying {table} to {dest}.")
            src_dir = Path(ParquetData(table_name=table).parquet_directory)
            table_dest = dest / table
            table_dest.mkdir(parents=True, exist_ok=True)
            for source_partition_name in source_partitions:
                parquet_file = src_dir / f"{source_partition_name}.parquet"
                if not parquet_file.exists():
                    raise FileNotFoundError(
                        f"Expected parquet output for {table} partition {source_partition_name}: {parquet_file}"
                    )
                (table_dest / parquet_file.name).write_bytes(parquet_file.read_bytes())
        (dest / "ferceqr_parquet_datapackage.json").write_bytes(datapackage_bytes)

    # Send Slack notification about successful build.
    logger.info("Notifying Slack about successful build.")
    notification_payload = FercEqrDeploymentNotificationPayload(
        outcome="SUCCESS",
        build_id=_get_build_id(),
        distribution_paths=[str(t) for t in targets],
        deployed_partitions=source_partitions,
        source_run_id=source_run_id,
        source_run_duration=source_run_duration,
        source_run_status=source_run_status,
        source_partition=source_partition,
        asset_partition_statuses=asset_partition_statuses,
    )
    _notify_slack_deployments_channel(
        message=build_ferceqr_deployment_markdown_message(notification_payload),
    )
    _write_status_file("FERCEQR_SUCCESS", pudl_paths)


@deployment_status_asset
def handle_ferceqr_deployment_failure(context: dg.AssetExecutionContext):
    """Send notification if EQR deployment failed."""
    pudl_paths: PudlPaths = context.resources.pudl_paths
    (
        asset_partition_statuses,
        source_run_id,
        source_run_status,
        source_partition,
        source_run_duration,
    ) = _get_source_run_step_status_summary(context)

    logger.error("Build failed, notifying Slack.")
    notification_payload = FercEqrDeploymentNotificationPayload(
        outcome="FAILURE",
        build_id=_get_build_id(),
        distribution_paths=None,
        deployed_partitions=[source_partition] if source_partition else None,
        source_run_id=source_run_id,
        source_run_duration=source_run_duration,
        source_run_status=source_run_status,
        source_partition=source_partition,
        asset_partition_statuses=asset_partition_statuses,
    )
    _notify_slack_deployments_channel(
        message=build_ferceqr_deployment_markdown_message(notification_payload),
    )
    _write_status_file("FERCEQR_FAILURE", pudl_paths)
