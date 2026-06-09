"""Define deployment helper assets for publishing FERC EQR outputs.

These assets run during batch builds to publish transformed FERC EQR outputs,
notify Zulip of success or failure, and create status files that tell the batch
job when deployment handling is complete.
"""

import json
import os
import re
import traceback
from collections.abc import Callable
from datetime import timedelta
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
DAGSTER_BACKFILL_TAG = "dagster/backfill"

FERCEQR_TRANSFORM_ASSETS = [
    "core_ferceqr__contracts",
    "core_ferceqr__transactions",
    "core_ferceqr__quarterly_identity",
    "core_ferceqr__quarterly_index_pub",
]

# Type alias: asset name -> partition name -> status name
StepStatusTable = dict[str, dict[str, str]]


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


def _parse_step_key(step_key: str, source_partition: str | None) -> tuple[str, str]:
    """Extract asset and partition from a step key like ``asset_name[partition]``.

    FERC EQR transform steps always follow the ``asset_name[partition]`` format, so the
    regex match is expected to succeed for all relevant steps. The fallback path is a
    safety net for unexpected non-bracketed step keys (e.g. system steps). In that case,
    the raw step key is used as the asset label so the status table remains
    comprehensible, and ``source_partition or "UNKNOWN"`` avoids ``None`` as a dict key.
    """
    step_key_pattern = re.compile(r"^(.+)\[([^\]]+)\]$")
    if m := step_key_pattern.match(step_key):
        return m.group(1), m.group(2)
    return step_key, source_partition or "UNKNOWN"


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


def _markdown_step_status_table(
    asset_partition_statuses: StepStatusTable,
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
) -> tuple[StepStatusTable, str | None]:
    """Collect step statuses and total elapsed time across all source runs.

    The elapsed time is computed from the earliest ``start_time`` to the latest
    ``end_time`` across all source runs (backfill or single). Returns
    ``(statuses, formatted_duration)``.
    """
    asset_partition_statuses: StepStatusTable = {}

    source_run_record = context.instance.get_run_record_by_id(source_run_id)

    # If we can't find the triggering run we can't look up its backfill siblings.
    source_run = (
        source_run_record.dagster_run if source_run_record is not None else None
    )
    if source_run is None:
        return asset_partition_statuses, None

    backfill_id = source_run.tags.get(DAGSTER_BACKFILL_TAG)

    # Fetch all runs in the same backfill, or just the single run if not a backfill.
    if backfill_id:
        source_run_records = context.instance.get_run_records(
            filters=dg.RunsFilter(
                job_name=source_run.job_name,
                tags={DAGSTER_BACKFILL_TAG: backfill_id},
            )
        )
    else:
        source_run_records = [source_run_record]

    # Collect timing and step statuses across all source runs.
    start_times: list[float] = []
    end_times: list[float] = []

    for run_record in source_run_records:
        # Track the earliest start and latest end for the total elapsed time.
        if run_record.start_time is not None:
            start_times.append(run_record.start_time)
        if run_record.end_time is not None:
            end_times.append(run_record.end_time)

        # Parse each step's asset name and partition from its step key.
        default_partition = run_record.dagster_run.tags.get("dagster/partition")
        for step in context.instance.get_run_step_stats(run_record.dagster_run.run_id):
            asset_name, partition_name = _parse_step_key(
                step_key=step.step_key,
                source_partition=default_partition,
            )
            # Record the terminal status of each asset/partition combination.
            asset_partition_statuses.setdefault(asset_name, {})[partition_name] = (
                step.status.name
            )

    # Compute total elapsed time: earliest start → latest end.
    if not start_times or not end_times:
        return asset_partition_statuses, None

    duration = str(timedelta(seconds=max(int(max(end_times) - min(start_times)), 0)))
    return asset_partition_statuses, duration


def _markdown_logfile_list(build_id: str) -> str:
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


def build_ferceqr_notification(
    context: dg.AssetExecutionContext,
    outcome: Literal["SUCCESS", "FAILURE"],
) -> str:
    """Build a Markdown notification string for FERC EQR deployment outcomes.

    Extracts all relevant information (source partitions, run ID, duration,
    step statuses, distribution paths, build ID) from the Dagster execution
    context and returns a formatted Markdown message ready for Zulip.
    """
    build_id = os.getenv("BUILD_ID", "no-build-id")
    source_partitions: list[str] = []
    source_run_id: str | None = None
    backfill_duration: str | None = None
    asset_partition_statuses: StepStatusTable = {}
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
            asset_partition_statuses, backfill_duration = _gather_step_statuses(
                context, source_run_id
            )
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
    if backfill_duration:
        lines.append(f"## :time: Backfill duration: `[{backfill_duration}]`")
    lines.extend(
        [
            "",
            "## Asset / Partition Status",
            ":check: = SUCCESS; :x: = FAILURE; :ghost: = SKIPPED; :question: = UNKNOWN / NO_DATA",
            "",
            _markdown_step_status_table(
                asset_partition_statuses=asset_partition_statuses,
                partitions=source_partitions or None,
            ),
            _markdown_logfile_list(build_id),
        ]
    )
    return "\n".join(lines)


def deployment_status_asset(asset_fn: Callable) -> dg.AssetsDefinition:
    """Create a custom decorator for deployment handler assets.

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

    # Extract source partitions from run tags for parquet copy. Getting the
    # *specific* partitions that were built helps avoid copying a bunch of other
    # data that might be laying around... in the case of local runs especially.
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

    logger.info("FERC EQR build successful, deploying FERC EQR data.")
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
    logger.info("FERC EQR deployment succeeded. Notifying Zulip.")
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
    """Send notification if the FERC EQR build failed."""
    pudl_paths: PudlPaths = context.resources.pudl_paths
    zulip: ZulipNotificationResource = context.resources.zulip_notification

    logger.error("FERC EQR build failed. Notifying Zulip.")
    notification_markdown = build_ferceqr_notification(context, outcome="FAILURE")
    zulip.send_stream_message(
        stream="pudl-deployments",
        topic="build-deploy-ferceqr",
        content=notification_markdown,
    )
    _write_status_file("FERCEQR_FAILURE", pudl_paths)
