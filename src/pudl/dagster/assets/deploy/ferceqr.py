"""Define deployment helper assets for publishing FERC EQR outputs.

These assets run during batch builds to publish transformed FERC EQR outputs,
notify Zulip of success or failure, and create status files that tell the batch
job when deployment handling is complete.
"""

import json
import logging
import os
import re
import time
import traceback
import uuid
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path
from typing import Literal

import dagster as dg
import pandas as pd
from upath import UPath

from pudl.dagster.resources import (
    FercEqrDeploymentResource,
    ZulipNotificationResource,
)
from pudl.helpers import ParquetData
from pudl.logging_helpers import get_logger
from pudl.metadata.classes import PUDL_PACKAGE
from pudl.metadata.sources import SOURCES
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
    """Notify build script that job is complete by creating a status file.

    Flush logging handlers before writing the sentinel. The bash script that
    launched the Dagster daemon uses ``inotifywait`` to watch for this sentinel
    and runs ``killall dagster-daemon`` as soon as it appears. Any buffered log
    output written before the sentinel but not yet flushed will be lost when the
    daemon process is killed, making errors invisible in the log.
    """
    for handler in logging.root.handlers:
        handler.flush()
    (Path(pudl_paths.pudl_output) / status).touch()


def _clear_status_files(pudl_paths: PudlPaths) -> None:
    """Remove any stale FERC EQR status files from the output directory."""
    for status_name in ("FERCEQR_SUCCESS", "FERCEQR_FAILURE"):
        status_path = Path(pudl_paths.pudl_output) / status_name
        status_path.unlink(missing_ok=True)


def _staging_path(dist_path: UPath) -> UPath:
    """Return the staging path *alongside* *dist_path* for an atomic deploy.

    The staging path sits as a sibling of the deployment target, rather than a
    child. On cloud storage this avoids prefix-scoping ambiguity during the
    rename loop — the staging and final prefixes are completely disjoint:

    .. code-block:: text

        gs://bucket/
          ├── 2026-06-10-.../              (final target dir)
          └── ._staging_2026-06-10-.../    (staging dir, sibling)

    The suffix includes the BUILD_ID to tie it to a specific build run, and a
    random component to avoid collisions during concurrent development runs.

    If ``BUILD_ID`` is not set (local/testing), a random short suffix is used alone.
    """
    build_id = os.getenv("BUILD_ID")
    suffix = build_id or uuid.uuid4().hex[:8]
    return dist_path.parent / f"._staging_{suffix}"


def _deploy_to_staging(  # noqa: C901
    ferceqr_deployment: FercEqrDeploymentResource,
    source_partitions: list[str],
    datapackage_path: Path,
) -> list[UPath]:
    """Copy EQR outputs to a staging location under each target, return staging paths.

    Each table's Parquet files and the datapackage JSON are written to a temporary
    ``._staging_{BUILD_ID}_{random}`` subdirectory beneath the real deployment target.
    This ensures that a timeout or crash during copying never leaves the final target
    in a partially-deployed state.

    *datapackage_path* is the path to the ``ferceqr_parquet_datapackage.json``
    file on the local filesystem (written to ``pudl_output`` by the caller).

    Returns the list of staging :class:`~upath.UPath` objects so the caller can atomically
    promote them via rename.
    """
    resolved_targets = ferceqr_deployment.resolved_targets()
    staging_targets: list[UPath] = []
    for dist_path in resolved_targets:
        staging_dir = _staging_path(dist_path)
        staging_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Deploying to staging area: {staging_dir}")

        for table in FERCEQR_TRANSFORM_ASSETS:
            logger.info(f"  Copying {table} to {staging_dir}.")
            src_dir = Path(ParquetData(table_name=table).parquet_directory)
            table_dest = staging_dir / table
            table_dest.mkdir(parents=True, exist_ok=True)
            for source_partition_name in source_partitions:
                parquet_file = src_dir / f"{source_partition_name}.parquet"
                if not parquet_file.exists():
                    raise FileNotFoundError(
                        f"Expected parquet output for {table} "
                        f"partition {source_partition_name}: {parquet_file}"
                    )
                (table_dest / parquet_file.name).write_bytes(parquet_file.read_bytes())

        # Copy the datapackage JSON that was written alongside the parquet data.
        if not datapackage_path.exists():
            raise FileNotFoundError(
                f"FERC EQR datapackage not found at {datapackage_path}"
            )
        (staging_dir / "ferceqr_parquet_datapackage.json").write_bytes(
            datapackage_path.read_bytes()
        )
        staging_targets.append(staging_dir)

    return staging_targets


def _promote_staging(
    staging_targets: list[UPath], resolved_targets: list[UPath]
) -> None:
    """Atomically promote staging directories to their final destination paths.

    For each ``(staging_dir, final_dir)`` pair, this moves the contents of the staged
    table directories and the datapackage JSON into the final target directory. On GCS
    and S3 ``UPath.rename()`` performs a server-side copy followed by deletion of the
    original, so the metadata (owner, timestamps, storage class) is preserved and no
    data re-upload occurs. On local filesystems the rename is a fast inode-level
    operation.
    """
    for staging_dir, final_dir in zip(staging_targets, resolved_targets, strict=True):
        logger.info(f"Promoting {staging_dir} -> {final_dir}")
        final_dir.mkdir(parents=True, exist_ok=True)

        # Rename each table directory into the final target.
        for table in FERCEQR_TRANSFORM_ASSETS:
            src = staging_dir / table
            dst = final_dir / table
            dst.mkdir(parents=True, exist_ok=True)
            for child in src.iterdir():
                child.rename(dst / child.name)
            # Remove the now-empty staging subdirectory. On GCS/S3 we need
            # fs.rm() -- rmdir() fails because there is no real directory.
            # Note: on GCS rename() performs a copy+delete, so the source
            # prefix may already be gone. Guard against that.
            if src.exists():
                src.fs.rm(src.path, recursive=True)

        # Rename the datapackage JSON.
        datapackage_src = staging_dir / "ferceqr_parquet_datapackage.json"
        try:
            datapackage_src.rename(final_dir / "ferceqr_parquet_datapackage.json")
        except FileNotFoundError:
            logger.error(
                f"Expected datapackage JSON not found in staging dir: {datapackage_src}"
            )
            raise

        # Remove the now empty staging directory.
        _remove_staging(staging_dir)


def _remove_staging(staging_dir: UPath) -> None:
    """Remove a staging directory and all its contents.

    Used for cleanup if the promotion step fails — the partial staging data is
    discarded rather than leaked. Safe to call on directories that do not exist
    (e.g. if promotion already removed them before the failure occurred).

    Uses ``fs.rm(path, recursive=True)`` instead of ``rmdir()`` because cloud
    storage (GCS, S3) uses virtual prefixes rather than real directories and
    ``rmdir()`` would raise ``NotADirectoryError``.
    """
    if staging_dir.exists():
        staging_dir.fs.rm(staging_dir.path, recursive=True)


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
    """Validate and parse the source partitions JSON from run tags.

    Also verifies that each partition is one of the allowed working partitions
    defined for the ``ferceqr`` data source in ``pudl.metadata.sources``.
    """
    if raw is None:
        return []
    parsed_partitions = json.loads(raw)
    if not isinstance(parsed_partitions, list) or not parsed_partitions:
        raise RuntimeError("FERC EQR deployment run has no deployable partitions.")
    if any(not isinstance(partition, str) for partition in parsed_partitions):
        raise RuntimeError("FERC EQR deployment run has invalid source partitions.")

    allowed_partitions = set(
        SOURCES["ferceqr"]["working_partitions"].get("year_quarters", [])
    )
    unexpected = sorted(set(parsed_partitions) - allowed_partitions)
    if unexpected:
        raise RuntimeError(
            f"FERC EQR deployment run references partitions not in the "
            f"ferceqr data source working_partitions: {unexpected}"
        )
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
        "https://console.cloud.google.com/batch/jobsDetail/regions/us-east1/jobs/"
        f"run-ferceqr-etl-{build_id}/logs?project=catalyst-cooperative-pudl"
    )
    return (
        "## Review FERC EQR Build Logs\n\n"
        f"* GCS URL: `gs://builds.catalyst.coop/ferceqr_logs/{build_id}.log`\n"
        f"* [Download FERC EQR logs to review locally]({download_url})\n"
        f"* [Review FERC EQR logs in the Google Cloud Console]({console_url})\n"
    )


def _compute_deploy_duration(context: dg.AssetExecutionContext) -> str | None:
    """Return elapsed time since the current run started, or None on failure."""
    try:
        run_record = context.instance.get_run_record_by_id(context.run_id)
        if run_record is not None and run_record.start_time is not None:
            elapsed = max(int(time.time() - run_record.start_time), 0)
            return str(timedelta(seconds=elapsed))
    except Exception:
        logger.info("build_ferceqr_notification: could not compute deploy duration")
    return None


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
    deploy_duration: str | None = None
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

    deploy_duration = _compute_deploy_duration(context)

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
    if deploy_duration:
        lines.append(f"## :time: Deploy duration: `[{deploy_duration}]`")
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
    """Publish EQR outputs to configured deployment targets.

    Uses a staging-then-rename pattern: all files are first uploaded to a
    ``._staging_{BUILD_ID}_{random}`` directory beneath each target, then
    atomically moved (server-side on GCS/S3, inode-level locally) to the final
    path. This ensures the target is never partially populated — if the upload
    is interrupted, the staging directory is simply discarded.
    """
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

    # Write the datapackage alongside the parquet data in pudl_output so it can
    # be deployed like any other file and remains as a record of the build.
    datapackage_path = Path(pudl_paths.pudl_output) / "ferceqr_parquet_datapackage.json"
    PUDL_PACKAGE.to_frictionless(include_pattern=r"core_ferceqr.*").to_json(
        str(datapackage_path)
    )

    logger.info("FERC EQR build successful, deploying FERC EQR data.")

    # Phase 1: Upload everything to staging directories.
    resolved_targets = ferceqr_deployment.resolved_targets()
    staging_targets = _deploy_to_staging(
        ferceqr_deployment=ferceqr_deployment,
        source_partitions=source_partitions,
        datapackage_path=datapackage_path,
    )

    # Phase 2: Atomically promote staging to the final paths.
    try:
        _promote_staging(
            staging_targets=staging_targets,
            resolved_targets=resolved_targets,
        )
    except Exception:
        logger.error(
            "FERC EQR deployment promotion failed! "
            "Staging directories may contain partial data; "
            "cleaning up staging paths.\n" + traceback.format_exc(),
        )
        # Send failure notification inline before the exception propagates.
        # The sensor-triggered failure asset never gets to run because the
        # bash script kills the dagster daemon as soon as FERCEQR_FAILURE
        # appears, so we must notify here while the process is still alive.
        try:
            notification_markdown = build_ferceqr_notification(
                context, outcome="FAILURE"
            )
            zulip.send_stream_message(
                stream="pudl-deployments",
                topic="build-deploy-ferceqr",
                content=notification_markdown,
            )
        except Exception:
            logger.error(
                "FERC EQR failure notification also failed:\n" + traceback.format_exc()
            )
        for staging_dir in staging_targets:
            try:
                _remove_staging(staging_dir)
            except Exception:
                logger.warning(
                    f"Failed to clean up staging dir {staging_dir}:\n"
                    + traceback.format_exc()
                )
        # Write the failure sentinel HERE (inside the inline handler) so the
        # log messages above are flushed before the sentinel triggers killall.
        _write_status_file("FERCEQR_FAILURE", pudl_paths)
        raise

    # Send Zulip notification about successful build
    logger.info("FERC EQR deployment succeeded. Notifying Zulip.")
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
