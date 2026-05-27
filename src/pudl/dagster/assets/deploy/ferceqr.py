"""Define deployment helper assets for publishing FERC EQR outputs.

These assets run during batch builds to publish transformed FERC EQR outputs,
notify Zulip of success or failure, and create status files that tell the batch
job when deployment handling is complete.
"""

import os
import traceback
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import dagster as dg
import gcsfs
import pandas as pd
import s3fs
from upath import UPath

from pudl.dagster.resources import (
    FercEqrBucketDeploymentResource,
    ZulipNotificationResource,
)
from pudl.helpers import ParquetData
from pudl.logging_helpers import get_logger
from pudl.metadata.classes import PUDL_PACKAGE
from pudl.workspace.setup import PudlPaths

logger = get_logger(__name__)

ZULIP_STREAM = "pudl-deployments"
ZULIP_TOPIC = "FERC EQR Builds"

FERCEQR_SOURCE_RUN_ID_TAG = "ferceqr/source_run_id"
FERCEQR_SOURCE_RUN_STATUS_TAG = "ferceqr/source_run_status"

FERCEQR_TRANSFORM_ASSETS = [
    "core_ferceqr__contracts",
    "core_ferceqr__transactions",
    "core_ferceqr__quarterly_identity",
    "core_ferceqr__quarterly_index_pub",
]


def is_ferceqr_build_enabled() -> bool:
    """Return whether FERCEQR batch-only deployment behavior is enabled.

    Treat common false-like strings as disabled so ``FERCEQR_BUILD=0`` and
    ``FERCEQR_BUILD=false`` don't enable deployment behavior.
    """
    raw_value = os.getenv("FERCEQR_BUILD")
    if raw_value is None:
        return False

    normalized_value = raw_value.strip().lower()
    return normalized_value not in {"", "0", "false", "no", "off"}


def _get_logfile_pointer_markdown(build_id: str) -> str:
    """Return markdown links to logs for notifications."""
    return (
        f"- [Download ferceqr logs](https://storage.cloud.google.com/builds.catalyst.coop/ferceqr_logs/{build_id}.log)\n"
        f"- [Query ferceqr logs online](https://console.cloud.google.com/batch/jobsDetail/regions/us-west1/jobs/run-ferceqr-etl-{build_id}/logs?project=catalyst-cooperative-pudl)\n"
    )


def _write_status_file(status: Literal["SUCCESS", "FAILURE"], pudl_paths: PudlPaths):
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
) -> tuple[dict[str, int], list[str], str | None, str | None]:
    """Return step-status counts, failed step keys, and source-run metadata."""
    try:
        run = context.run
    except Exception:
        # Dagster direct-invocation contexts in tests may not have run metadata.
        # We intentionally treat this as missing source-run data for notifications.
        return {}, [], None, None

    run_tags = run.tags if run else {}
    source_run_id = run_tags.get(FERCEQR_SOURCE_RUN_ID_TAG)
    source_run_status = run_tags.get(FERCEQR_SOURCE_RUN_STATUS_TAG)
    if not source_run_id:
        return {}, [], None, source_run_status

    step_stats = context.instance.get_run_step_stats(source_run_id)
    status_counts: dict[str, int] = {}
    failed_steps: list[str] = []
    for step in step_stats:
        step_status = _status_name(step.status)
        status_counts[step_status] = status_counts.get(step_status, 0) + 1
        if step_status == "FAILURE":
            failed_steps.append(step.step_key)
    return status_counts, failed_steps, source_run_id, source_run_status


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
    lines.extend(
        [
            "",
            "### Step Status",
            _format_step_status_markdown_table(payload.step_status_counts),
        ]
    )

    if payload.failed_step_keys:
        shown_failed_steps = payload.failed_step_keys[:10]
        lines.extend(
            [
                "",
                "### Failed Steps",
                *[f"- `{step}`" for step in shown_failed_steps],
            ]
        )
        if len(payload.failed_step_keys) > len(shown_failed_steps):
            lines.append(
                f"- _...and {len(payload.failed_step_keys) - len(shown_failed_steps)} more._"
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
            "zulip_notifications",
            "ferceqr_bucket_deployment",
        },
    )
    def _status_handler_asset(context: dg.AssetExecutionContext):
        if not is_ferceqr_build_enabled():
            raise dg.Failure(
                "FERCEQR deployment handlers only run when FERCEQR_BUILD is enabled."
            )

        try:
            handler(context)
        except Exception:
            logger.error("FERCEQR deployment handler failed!")
            logger.error(traceback.format_exc())
            _write_status_file("FAILURE", context.resources.pudl_paths)

    return _status_handler_asset


@deployment_status_asset
def deploy_ferceqr(context: dg.AssetExecutionContext):
    """Publish EQR outputs to cloud storage."""
    pudl_paths: PudlPaths = context.resources.pudl_paths
    zulip_notifications: ZulipNotificationResource = (
        context.resources.zulip_notifications
    )
    bucket_deployment: FercEqrBucketDeploymentResource = (
        context.resources.ferceqr_bucket_deployment
    )
    step_status_counts, failed_step_keys, source_run_id, source_run_status = (
        _get_source_run_step_status_summary(context)
    )

    # Get output locations for S3 and GCS
    distribution_paths = [
        (
            UPath(bucket_deployment.gcs_output_bucket),
            gcsfs.GCSFileSystem(
                project=bucket_deployment.gcp_billing_project,
                requester_pays=True,
            ),
        ),
        (UPath(bucket_deployment.s3_output_bucket), s3fs.S3FileSystem()),
    ]
    # Get 'datapackage.json' data to write to distribution paths
    datapackage_bytes = (
        PUDL_PACKAGE.to_frictionless(include_pattern=r"core_ferceqr.*")
        .to_json()
        .encode(encoding="utf-8")
    )

    # Loop through output locations and copy parquet files to buckets
    logger.info("Build successful, deploying ferceqr data.")
    for distribution_path, fs in distribution_paths:
        for table in FERCEQR_TRANSFORM_ASSETS:
            logger.info(f"Copying {table} to {distribution_path}.")
            table_remote_path = distribution_path / table

            # UPath's don't work well with requester pays buckets, so use the fsspec
            # filesystem directly
            fs.mkdirs(path=table_remote_path, exist_ok=True)
            fs.put(
                f"{ParquetData(table_name=table).parquet_directory}/",
                f"{table_remote_path}/",
                recursive=True,
            )
        # Copy datapackage to distribution path
        fs.pipe(
            f"{distribution_path}/ferceqr_parquet_datapackage.json",
            value=datapackage_bytes,
        )

    # Send Zulip notification about successful build.
    logger.info("Notifying Zulip about successful build.")
    notification_payload = FercEqrDeploymentNotificationPayload(
        outcome="SUCCESS",
        build_id=bucket_deployment.build_id,
        distribution_paths=[str(path[0]) for path in distribution_paths],
        source_run_id=source_run_id,
        source_run_status=source_run_status,
        step_status_counts=step_status_counts,
        failed_step_keys=failed_step_keys,
    )
    zulip_notifications.send_stream_message(
        content=build_ferceqr_deployment_markdown_message(notification_payload),
        stream=ZULIP_STREAM,
        topic=ZULIP_TOPIC,
    )
    _write_status_file("SUCCESS", pudl_paths)


@deployment_status_asset
def handle_ferceqr_deployment_failure(context: dg.AssetExecutionContext):
    """Send notification if EQR deployment failed."""
    pudl_paths: PudlPaths = context.resources.pudl_paths
    zulip_notifications: ZulipNotificationResource = (
        context.resources.zulip_notifications
    )
    bucket_deployment: FercEqrBucketDeploymentResource = (
        context.resources.ferceqr_bucket_deployment
    )
    step_status_counts, failed_step_keys, source_run_id, source_run_status = (
        _get_source_run_step_status_summary(context)
    )

    logger.error("Build failed, notifying Zulip.")
    notification_payload = FercEqrDeploymentNotificationPayload(
        outcome="FAILURE",
        build_id=bucket_deployment.build_id,
        distribution_paths=None,
        source_run_id=source_run_id,
        source_run_status=source_run_status,
        step_status_counts=step_status_counts,
        failed_step_keys=failed_step_keys,
    )
    zulip_notifications.send_stream_message(
        content=build_ferceqr_deployment_markdown_message(notification_payload),
        stream=ZULIP_STREAM,
        topic=ZULIP_TOPIC,
    )
    _write_status_file("FAILURE", pudl_paths)
