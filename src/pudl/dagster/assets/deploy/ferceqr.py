"""Define deployment helper assets for publishing FERC EQR outputs.

These assets run during batch builds to publish transformed FERC EQR outputs,
notify Zulip of success or failure, and create status files that tell the batch
job when deployment handling is complete.
"""

import json
import os
import traceback
from collections.abc import Callable
from pathlib import Path
from typing import Literal

import dagster as dg
import pandas as pd

from pudl.dagster.resources import (
    FercEqrDeploymentResource,
    ZulipNotificationResource,
)
from pudl.helpers import ParquetData
from pudl.logging_helpers import get_logger
from pudl.metadata.classes import PUDL_PACKAGE
from pudl.workspace.setup import PudlPaths

logger = get_logger(__name__)

FERCEQR_SOURCE_RUN_ID_TAG = "ferceqr/source_run_id"
FERCEQR_SOURCE_PARTITIONS_TAG = "ferceqr/source_partitions"
FERCEQR_BACKFILL_TAG = "dagster/backfill"

FERCEQR_TRANSFORM_ASSETS = [
    "core_ferceqr__contracts",
    "core_ferceqr__transactions",
    "core_ferceqr__quarterly_identity",
    "core_ferceqr__quarterly_index_pub",
]


def _get_logfile_list(build_id: str) -> str:
    """Return pointer to logs to send in Zulip message."""
    download_url = (
        "https://storage.cloud.google.com/builds.catalyst.coop/"
        f"ferceqr_logs/{build_id}.log"
    )
    console_url = (
        "https://console.cloud.google.com/batch/jobsDetail/regions/us-west1/jobs/"
        f"run-ferceqr-etl-{build_id}/logs?project=catalyst-cooperative-pudl"
    )
    return (
        "## Review FERC EQR Build Logs\n\n"
        f"* GCS URL: `gs://builds.catalyst.coop/ferceqr_logs/{build_id}.log`\n"
        f"* [Download FERC EQR logs to review locally]({download_url})\n"
        f"* [Review FERC EQR logs in the Google Cloud Console]({console_url})\n"
    )


def _get_build_id() -> str:
    """Return the current build identifier for deployment notifications."""
    return os.getenv("BUILD_ID", "no-build-id")


def _write_status_file(
    status: Literal["FERCEQR_SUCCESS", "FERCEQR_FAILURE"],
    pudl_paths: PudlPaths,
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
    """Format elapsed seconds as HH:MM:SS for Zulip notifications."""
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


def _validate_partitions(raw: str | None) -> list[str]:
    """Validate and parse the source partitions JSON from run tags."""
    if raw is None:
        return []
    parsed_partitions = json.loads(raw)
    if not isinstance(parsed_partitions, list) or not parsed_partitions:
        raise RuntimeError("FERC EQR deployment run has no deployable partitions.")
    if any(not isinstance(partition, str) for partition in parsed_partitions):
        raise RuntimeError("FERC EQR deployment run has invalid source partitions.")
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


def _gather_step_statuses(
    context: dg.AssetExecutionContext,
    source_run_id: str,
) -> dict[str, dict[str, str]]:
    """Collect step-level execution statuses keyed by asset name and partition."""
    asset_partition_statuses: dict[str, dict[str, str]] = {}

    source_run_record = context.instance.get_run_record_by_id(source_run_id)
    source_run = (
        source_run_record.dagster_run if source_run_record is not None else None
    )
    if source_run is None:
        return asset_partition_statuses

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

    for run_record in source_runs:
        default_partition = run_record.tags.get("dagster/partition")
        for step in context.instance.get_run_step_stats(run_record.run_id):
            asset_name, partition_name = _parse_step_key(
                step_key=step.step_key,
                source_partition=default_partition,
            )
            asset_partition_statuses.setdefault(asset_name, {})[partition_name] = (
                _status_name(step.status)
            )

    return asset_partition_statuses


def build_ferceqr_notification(
    context: dg.AssetExecutionContext,
    outcome: Literal["SUCCESS", "FAILURE"],
) -> str:
    """Build a Markdown notification string for FERC EQR deployment outcomes.

    Extracts all relevant information (source partitions, run ID, duration,
    step statuses, distribution paths, build ID) from the Dagster execution
    context and returns a formatted Markdown message ready for Zulip.
    """
    build_id = _get_build_id()
    source_partitions: list[str] = []
    source_run_id: str | None = None
    source_run_duration: str | None = None
    asset_partition_statuses: dict[str, dict[str, str]] = {}
    distribution_paths: list[str] = []

    # Extract run-tag information.
    try:
        run = context.run
        run_tags = run.tags if run else {}
        partitions_raw = run_tags.get(FERCEQR_SOURCE_PARTITIONS_TAG)
        if partitions_raw:
            source_partitions = _validate_partitions(partitions_raw)
        source_run_id = run_tags.get(FERCEQR_SOURCE_RUN_ID_TAG)

        if source_run_id:
            source_run_duration = _get_run_duration(
                context.instance.get_run_record_by_id(source_run_id)
            )
            asset_partition_statuses = _gather_step_statuses(context, source_run_id)
    except Exception:
        logger.info(
            "build_ferceqr_notification: context.run not available "
            "(direct invocation in tests)"
        )

    # Extract distribution paths from the deployment resource.
    try:
        deployment: FercEqrDeploymentResource = (
            context.resources.ferceqr_deployment_targets
        )
        distribution_paths = [str(p) for p in deployment.resolved_targets()]
    except Exception:
        logger.info(
            "build_ferceqr_notification: ferceqr_deployment_targets not available "
            "(direct invocation in tests)"
        )

    # Build the Markdown.
    title = (
        "\n# :check: FERC EQR Deployment Succeeded"
        if outcome == "SUCCESS"
        else "\n# :x: FERC EQR Deployment Failed"
    )
    lines = [title, ""]
    lines.append(f"- Build ID: `{build_id}`")
    if source_run_id:
        lines.append(f"- Dagster Run ID: `{source_run_id}`")
    if distribution_paths:
        lines.append("## Deployment Targets:")
        lines.extend(f"  - `{path}`" for path in distribution_paths)
    if source_run_duration:
        lines.append(f"## :time: Dagster run duration: `[{source_run_duration}]`")
    lines.extend(
        [
            "",
            "## Asset / Partition Status",
            ":check: = SUCCESS; :x: = FAILURE; :ghost: = SKIPPED; :question: = UNKNOWN / NO_DATA",
            "",
            _format_step_status_markdown_table(
                asset_partition_statuses=asset_partition_statuses,
                partitions=source_partitions or None,
            ),
            _get_logfile_list(build_id),
        ]
    )
    return "\n".join(lines)


def deployment_status_asset(
    asset_fn: Callable,
) -> dg.AssetsDefinition:
    """Create a wrapper for deployment handler assets.

    This allows us to gracefully handle errors if the deployment assets fail for any
    reason. When these assets fail, sometimes the logs don't show up in the batch job
    appropriately, and the status file never gets created, so the job keeps running
    until it eventually times out.
    """

    @dg.asset(
        name=asset_fn.__name__,
        required_resource_keys={
            "pudl_paths",
            "ferceqr_deployment_targets",
            "zulip_notification",
        },
    )
    def _status_handler_asset(context: dg.AssetExecutionContext):
        try:
            _clear_status_files(context.resources.pudl_paths)
            asset_fn(context)
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
    zulip: ZulipNotificationResource = context.resources.zulip_notification
    ferceqr_deployment: FercEqrDeploymentResource = (
        context.resources.ferceqr_deployment_targets
    )
    source_partitions: list[str] = []

    # Extract source partitions from run tags for parquet copy
    try:
        run = context.run
        partitions_raw = (run.tags or {}).get(FERCEQR_SOURCE_PARTITIONS_TAG)
        if partitions_raw:
            source_partitions = _validate_partitions(partitions_raw)
    except Exception:
        logger.info(
            "deploy_ferceqr: context.run not available (direct invocation in tests)"
        )

    if not source_partitions:
        raise RuntimeError("FERC EQR deployment run has no deployable partitions.")

    # Get 'datapackage.json' data to write to deployment targets.
    datapackage_bytes = (
        PUDL_PACKAGE.to_frictionless(include_pattern=r"core_ferceqr.*")
        .to_json()
        .encode(encoding="utf-8")
    )

    logger.info("Build successful, deploying ferceqr data.")
    for dist_path in ferceqr_deployment.resolved_targets():
        for table in FERCEQR_TRANSFORM_ASSETS:
            logger.info(f"Copying {table} to {dist_path}.")
            src_dir = Path(ParquetData(table_name=table).parquet_directory)
            table_dest = dist_path / table
            table_dest.mkdir(parents=True, exist_ok=True)
            for source_partition_name in source_partitions:
                parquet_file = src_dir / f"{source_partition_name}.parquet"
                if not parquet_file.exists():
                    raise FileNotFoundError(
                        f"Expected parquet output for {table} partition {source_partition_name}: {parquet_file}"
                    )
                (table_dest / parquet_file.name).write_bytes(parquet_file.read_bytes())
        (dist_path / "ferceqr_parquet_datapackage.json").write_bytes(datapackage_bytes)

    # Send Zulip notification about successful build
    logger.info("FERC EQR build succeeded. Notifying Zulip.")
    # Build notification markdown content
    notification_markdown = build_ferceqr_notification(context, outcome="SUCCESS")
    zulip.send_stream_message(
        stream="pudl-deployments",
        topic="build-deploy-ferceqr",
        content=notification_markdown,
    )
    _write_status_file("FERCEQR_SUCCESS", pudl_paths)


@deployment_status_asset
def handle_ferceqr_failure(context: dg.AssetExecutionContext):
    """Send notification if EQR build failed."""
    pudl_paths: PudlPaths = context.resources.pudl_paths
    zulip: ZulipNotificationResource = context.resources.zulip_notification

    logger.error("Build failed, notifying Zulip.")
    notification_markdown = build_ferceqr_notification(context, outcome="FAILURE")
    zulip.send_stream_message(
        stream="pudl-deployments",
        topic="build-deploy-ferceqr",
        content=notification_markdown,
    )
    _write_status_file("FERCEQR_FAILURE", pudl_paths)
