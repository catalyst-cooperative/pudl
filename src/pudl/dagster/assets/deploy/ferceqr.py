"""Define deployment helper assets for publishing FERC EQR outputs.

These assets run during batch builds to publish transformed FERC EQR outputs,
notify Slack of success or failure, and create status files that tell the batch
job when deployment handling is complete.
"""

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


def _status_name(value: object) -> str:
    """Return normalized uppercase status name from a Dagster status object."""
    if hasattr(value, "name"):
        return str(value.name).upper()
    if hasattr(value, "value"):
        return str(value.value).upper()
    return str(value).split(".")[-1].upper()


def _get_source_run_step_status_summary(
    context: dg.AssetExecutionContext,
) -> tuple[dict[str, int], list[str], str | None, str | None, str | None]:
    """Return step-status counts, failed step keys, and source-run metadata."""
    try:
        run = context.run
    except Exception:
        # Dagster direct-invocation contexts in tests may not have run metadata.
        # We intentionally treat this as missing source-run data for notifications.
        return {}, [], None, None, None

    run_tags = run.tags if run else {}
    source_run_id = run_tags.get(FERCEQR_SOURCE_RUN_ID_TAG)
    source_run_status = run_tags.get(FERCEQR_SOURCE_RUN_STATUS_TAG)
    source_partition = run_tags.get(FERCEQR_SOURCE_PARTITION_TAG)
    if not source_run_id:
        return {}, [], None, source_run_status, source_partition

    step_stats = context.instance.get_run_step_stats(source_run_id)
    status_counts: dict[str, int] = {}
    failed_steps: list[str] = []
    for step in step_stats:
        step_status = _status_name(step.status)
        status_counts[step_status] = status_counts.get(step_status, 0) + 1
        if step_status == "FAILURE":
            failed_steps.append(step.step_key)
    return (
        status_counts,
        failed_steps,
        source_run_id,
        source_run_status,
        source_partition,
    )


def _parse_failed_step_key(
    step_key: str, source_partition: str | None
) -> dict[str, str]:
    """Extract asset and partition details from a Dagster step key."""
    if "[" in step_key and step_key.endswith("]"):
        asset_name, raw_partition = step_key.rsplit("[", 1)
        return {
            "Asset": asset_name,
            "Partition": raw_partition[:-1],
            "Step Key": step_key,
        }

    return {
        "Asset": step_key,
        "Partition": source_partition or "UNKNOWN",
        "Step Key": step_key,
    }


def _format_failed_assets_partitions_markdown_table(
    failed_step_keys: list[str], source_partition: str | None
) -> str:
    """Format failed steps as an asset/partition/step-key Markdown table."""
    rows = [
        _parse_failed_step_key(step_key=step_key, source_partition=source_partition)
        for step_key in failed_step_keys
    ]
    return pd.DataFrame(rows).to_markdown(index=False)


def _format_step_status_markdown_table(status_counts: dict[str, int]) -> str:
    """Format step status counts as a compact Markdown table."""
    if not status_counts:
        table = pd.DataFrame([{"Step Status": "NO_DATA", "Count": 0}])
        return table.to_markdown(index=False)

    table = pd.DataFrame(
        {
            "Step Status": list(status_counts.keys()),
            "Count": list(status_counts.values()),
        }
    )
    preferred_order = ["SUCCESS", "FAILURE", "SKIPPED", "IN_PROGRESS", "STARTED"]
    table["status_order"] = pd.Categorical(
        table["Step Status"], categories=preferred_order, ordered=True
    )
    table["is_unknown_status"] = table["status_order"].isna()
    table = table.sort_values(
        by=["is_unknown_status", "status_order", "Step Status"], kind="stable"
    ).drop(columns=["status_order", "is_unknown_status"])
    return table.to_markdown(index=False)


@dataclass(frozen=True)
class FercEqrDeploymentNotificationPayload:
    """Input payload for FERCEQR deployment notification markdown."""

    outcome: Literal["SUCCESS", "FAILURE"]
    build_id: str
    distribution_paths: list[str] | None
    source_run_id: str | None
    source_run_status: str | None
    source_partition: str | None
    step_status_counts: dict[str, int]
    failed_step_keys: list[str]


def build_ferceqr_deployment_markdown_message(
    payload: FercEqrDeploymentNotificationPayload,
) -> str:
    """Build reusable Markdown notification text for deployment outcomes."""
    title = (
        "## FERC EQR Deployment Succeeded"
        if payload.outcome == "SUCCESS"
        else "## FERC EQR Deployment Failed"
    )
    lines = [title, ""]
    if payload.distribution_paths:
        lines.append(f"- Published to: {', '.join(payload.distribution_paths)}")
    lines.append(f"- Build ID: {payload.build_id}")
    if payload.source_run_id:
        lines.append(f"- Source Dagster run: `{payload.source_run_id}`")
    if payload.source_run_status:
        lines.append(f"- Source run status: `{payload.source_run_status}`")
    if payload.source_partition:
        lines.append(f"- Source partition: `{payload.source_partition}`")
    lines.extend(
        [
            "",
            "### Step Status",
            _format_step_status_markdown_table(payload.step_status_counts),
        ]
    )

    if payload.failed_step_keys:
        lines.extend(
            [
                "",
                "### Failed Assets / Partitions",
                _format_failed_assets_partitions_markdown_table(
                    failed_step_keys=payload.failed_step_keys,
                    source_partition=payload.source_partition,
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
            handler(context)
        except Exception:
            logger.error("FERCEQR deployment handler failed!")
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
        step_status_counts,
        failed_step_keys,
        source_run_id,
        source_run_status,
        source_partition,
    ) = _get_source_run_step_status_summary(context)

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
            for parquet_file in src_dir.glob("*.parquet"):
                (table_dest / parquet_file.name).write_bytes(parquet_file.read_bytes())
        (dest / "ferceqr_parquet_datapackage.json").write_bytes(datapackage_bytes)

    # Send Slack notification about successful build.
    logger.info("Notifying Slack about successful build.")
    notification_payload = FercEqrDeploymentNotificationPayload(
        outcome="SUCCESS",
        build_id=_get_build_id(),
        distribution_paths=[str(t) for t in targets],
        source_run_id=source_run_id,
        source_run_status=source_run_status,
        source_partition=source_partition,
        step_status_counts=step_status_counts,
        failed_step_keys=failed_step_keys,
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
        step_status_counts,
        failed_step_keys,
        source_run_id,
        source_run_status,
        source_partition,
    ) = _get_source_run_step_status_summary(context)

    logger.error("Build failed, notifying Slack.")
    notification_payload = FercEqrDeploymentNotificationPayload(
        outcome="FAILURE",
        build_id=_get_build_id(),
        distribution_paths=None,
        source_run_id=source_run_id,
        source_run_status=source_run_status,
        source_partition=source_partition,
        step_status_counts=step_status_counts,
        failed_step_keys=failed_step_keys,
    )
    _notify_slack_deployments_channel(
        message=build_ferceqr_deployment_markdown_message(notification_payload),
    )
    _write_status_file("FERCEQR_FAILURE", pudl_paths)
