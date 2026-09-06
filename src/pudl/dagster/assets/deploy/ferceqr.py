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
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
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
from pudl.deploy.object_store import ObjectStore
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

DATAPACKAGE_FILENAME = "ferceqr_parquet_datapackage.json"

# Layout beneath the per-build staging prefix. Parquet data and the datapackage
# JSON go in separate subdirectories so the promote step can move the data into
# place first and the descriptor last -- a consumer that reads the datapackage to
# discover files then never sees it reference a Parquet file that has not landed.
STAGING_DATA_SUBDIR = "data"
STAGING_META_SUBDIR = "meta"

# Sibling of each deployment target holding the previous build's data, refreshed
# just before every promote so a botched deploy can be rolled back by hand.
PREVIOUS_DIRNAME = "._ferceqr_previous"

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


@dataclass
class _DeploymentTarget:
    """One resolved deployment destination plus the scratch prefixes beside it.

    ``final``/``staging``/``previous`` are full URI strings (``gs://…``, ``s3://…``)
    or local paths. ``store`` is the :class:`~pudl.deploy.object_store.ObjectStore`
    that knows how to move bytes for that URI scheme.
    """

    store: ObjectStore
    final: str
    staging: str
    previous: str

    @property
    def staging_data(self) -> str:
        """Prefix holding staged Parquet files, one subdirectory per table."""
        return f"{self.staging}/{STAGING_DATA_SUBDIR}"

    @property
    def staging_meta(self) -> str:
        """Prefix holding the staged datapackage JSON."""
        return f"{self.staging}/{STAGING_META_SUBDIR}"


def _deployment_targets(resolved_targets: list[UPath]) -> list[_DeploymentTarget]:
    """Turn resolved deployment UPaths into :class:`_DeploymentTarget` records.

    The staging and previous-build prefixes are siblings of the final target so
    their key namespaces never overlap it. The staging suffix ties the scratch
    prefix to a single build (BUILD_ID), with a random fallback for local runs.
    """
    build_id = os.getenv("BUILD_ID") or uuid.uuid4().hex[:8]
    targets: list[_DeploymentTarget] = []
    for resolved in resolved_targets:
        final = str(resolved).rstrip("/")
        parent = final.rpartition("/")[0]
        targets.append(
            _DeploymentTarget(
                store=ObjectStore.for_uri(final, dict(resolved.storage_options)),
                final=final,
                staging=f"{parent}/._staging_{build_id}",
                previous=f"{parent}/{PREVIOUS_DIRNAME}",
            )
        )
    return targets


def _source_parquet_files(source_partitions: list[str]) -> dict[str, list[Path]]:
    """Map each transform asset to its local Parquet files for *source_partitions*.

    Raises :class:`FileNotFoundError` if any expected partition file is missing so
    an incomplete build never gets partially deployed.
    """
    table_files: dict[str, list[Path]] = {}
    for table in FERCEQR_TRANSFORM_ASSETS:
        src_dir = Path(ParquetData(table_name=table).parquet_directory)
        files: list[Path] = []
        for partition in source_partitions:
            parquet_file = src_dir / f"{partition}.parquet"
            if not parquet_file.exists():
                raise FileNotFoundError(
                    f"Expected parquet output for {table} partition {partition}: "
                    f"{parquet_file}"
                )
            files.append(parquet_file)
        table_files[table] = files
    return table_files


def _expected_object_sizes(
    table_files: dict[str, list[Path]], datapackage_path: Path
) -> dict[str, int]:
    """Return ``{staging_relative_path: size_bytes}`` for everything to be uploaded."""
    expected = {
        f"{STAGING_DATA_SUBDIR}/{table}/{parquet_file.name}": parquet_file.stat().st_size
        for table, files in table_files.items()
        for parquet_file in files
    }
    expected[f"{STAGING_META_SUBDIR}/{datapackage_path.name}"] = (
        datapackage_path.stat().st_size
    )
    return expected


def _stage_target(
    target: _DeploymentTarget,
    table_files: dict[str, list[Path]],
    datapackage_path: Path,
    expected_sizes: dict[str, int],
) -> None:
    """Upload all outputs to *target*'s staging prefix and verify they arrived.

    Raises :class:`RuntimeError` if the staged object set does not exactly match
    the local outputs by name and byte size. The final target is untouched.
    """
    logger.info(f"Staging FERC EQR outputs to {target.staging}")
    for table, files in table_files.items():
        target.store.upload_files(files, f"{target.staging_data}/{table}")
    target.store.upload_files([datapackage_path], target.staging_meta)

    staged_sizes = target.store.object_sizes(target.staging)
    if staged_sizes != expected_sizes:
        missing = sorted(set(expected_sizes) - set(staged_sizes))
        unexpected = sorted(set(staged_sizes) - set(expected_sizes))
        wrong_size = sorted(
            key
            for key in expected_sizes.keys() & staged_sizes.keys()
            if expected_sizes[key] != staged_sizes[key]
        )
        raise RuntimeError(
            f"Staged upload to {target.staging} does not match local outputs. "
            f"missing={missing} unexpected={unexpected} wrong_size={wrong_size}"
        )


def _promote_target(target: _DeploymentTarget) -> None:
    """Snapshot the live tree, then move staging into place and drop the staging dir.

    The datapackage JSON is promoted after the Parquet data so it never briefly
    references files that have not landed yet.
    """
    if target.store.object_sizes(target.final):
        logger.info(f"Snapshotting {target.final} -> {target.previous}")
        target.store.sync(target.final, target.previous)

    logger.info(f"Promoting {target.staging} -> {target.final}")
    target.store.move(target.staging_data, target.final)
    target.store.move(target.staging_meta, target.final)
    target.store.remove(target.staging)


def _run_for_targets(
    targets: list[_DeploymentTarget], step: Callable[[_DeploymentTarget], None]
) -> None:
    """Run *step* against every target, concurrently when there is more than one.

    The first exception raised by any target propagates once all have finished.
    """
    if len(targets) == 1:
        step(targets[0])
        return
    with ThreadPoolExecutor(max_workers=len(targets)) as pool:
        futures = [pool.submit(step, target) for target in targets]
        for future in as_completed(futures):
            future.result()


def _remove_all_staging(targets: list[_DeploymentTarget]) -> None:
    """Best-effort removal of every target's staging prefix after a failure."""
    for target in targets:
        try:
            target.store.remove(target.staging)
        except Exception:
            logger.warning(
                f"Failed to clean up staging prefix {target.staging}:\n"
                + traceback.format_exc()
            )


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


def _deploy_source_partitions(context: dg.AssetExecutionContext) -> list[str]:
    """Read the built partitions from run tags, empty when invoked outside a run.

    Deploying only the *specific* partitions this build produced avoids sweeping
    up unrelated data left lying around, especially on local runs.
    """
    try:
        run_tags = (context.run.tags or {}) if context.run else {}
    except Exception:
        logger.info(
            "deploy_ferceqr: context.run not available (direct invocation in tests)"
        )
        return []
    return _validate_partitions(run_tags.get(FERCEQR_SOURCE_PARTITIONS_TAG) or None)


@deployment_status_asset
def deploy_ferceqr(context: dg.AssetExecutionContext):
    """Publish EQR outputs to configured deployment targets.

    Each target is handled with a staging-then-promote pattern driven by
    :class:`~pudl.deploy.object_store.ObjectStore`:

    1. Upload every built Parquet file and the datapackage JSON into a per-build
       ``._staging_{BUILD_ID}`` prefix beside the target.
    2. Verify the staged object set matches the local outputs by name and size.
    3. Copy the current live tree into ``._ferceqr_previous`` for rollback.
    4. Server-side move staging into the final prefix (data first, datapackage
       last) and delete the staging prefix.

    A failure in steps 1-2 leaves the target untouched. A failure in steps 3-4
    may leave the target with a mix of old and new files; ``._ferceqr_previous``
    holds the prior build for a manual rollback. Targets are processed
    concurrently.
    """
    pudl_paths: PudlPaths = context.resources.pudl_paths
    zulip: ZulipNotificationResource = context.resources.zulip_notification
    ferceqr_deployment: FercEqrDeploymentResource = (
        context.resources.ferceqr_deployment_targets
    )

    source_partitions = _deploy_source_partitions(context)
    if not source_partitions:
        raise RuntimeError("FERC EQR deployment run has no deployable partitions.")

    # Write the datapackage alongside the parquet data in pudl_output so it can
    # be deployed like any other file and remains as a record of the build.
    datapackage_path = Path(pudl_paths.pudl_output) / DATAPACKAGE_FILENAME
    PUDL_PACKAGE.to_frictionless(include_pattern=r"core_ferceqr.*").to_json(
        str(datapackage_path)
    )

    table_files = _source_parquet_files(source_partitions)
    expected_sizes = _expected_object_sizes(table_files, datapackage_path)
    targets = _deployment_targets(ferceqr_deployment.resolved_targets())

    logger.info("FERC EQR build successful, deploying FERC EQR data.")
    try:
        _run_for_targets(
            targets,
            lambda target: _stage_target(
                target, table_files, datapackage_path, expected_sizes
            ),
        )
        _run_for_targets(targets, _promote_target)
    except Exception:
        logger.error(
            "FERC EQR deployment failed; cleaning up staging prefixes.\n"
            + traceback.format_exc()
        )
        # Notify inline before the exception propagates: the sensor-triggered
        # failure asset never runs because the bash script kills the dagster
        # daemon as soon as FERCEQR_FAILURE appears.
        try:
            zulip.send_stream_message(
                stream="pudl-deployments",
                topic="build-deploy-ferceqr",
                content=build_ferceqr_notification(context, outcome="FAILURE"),
            )
        except Exception:
            logger.error(
                "FERC EQR failure notification also failed:\n" + traceback.format_exc()
            )
        _remove_all_staging(targets)
        # Write the failure sentinel HERE so the log messages above are flushed
        # before the sentinel triggers killall.
        _write_status_file("FERCEQR_FAILURE", pudl_paths)
        raise

    logger.info("FERC EQR deployment succeeded. Notifying Zulip.")
    zulip.send_stream_message(
        stream="pudl-deployments",
        topic="build-deploy-ferceqr",
        content=build_ferceqr_notification(context, outcome="SUCCESS"),
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
