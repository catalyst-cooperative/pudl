"""Dagster sensors for PUDL.

This module defines sensor-based automation that watches the state of the PUDL code
location and requests follow-on work when specific conditions are met. Add sensor
definitions here when they poll Dagster state, external state, or partition progress to
trigger a run, rather than when the logic belongs inside an asset or job itself. Keep
the module focused on automation entrypoints and their shared defaults.

For the underlying Dagster concept, see https://docs.dagster.io/guides/automate/sensors
"""

import json
from collections.abc import Sequence

import dagster as dg

from pudl.dagster.assets.deploy.ferceqr import (
    DAGSTER_BACKFILL_TAG,
    FERCEQR_SOURCE_PARTITIONS_TAG,
    FERCEQR_SOURCE_RUN_ID_TAG,
)
from pudl.dagster.jobs import ferceqr_deployment_job, ferceqr_job
from pudl.logging_helpers import get_logger

logger = get_logger(__name__)

NON_TERMINAL_RUN_STATUSES = {
    dg.DagsterRunStatus.QUEUED,
    dg.DagsterRunStatus.NOT_STARTED,
    dg.DagsterRunStatus.MANAGED,
    dg.DagsterRunStatus.STARTING,
    dg.DagsterRunStatus.STARTED,
    dg.DagsterRunStatus.CANCELING,
}

FAILED_TERMINAL_RUN_STATUSES = {
    dg.DagsterRunStatus.FAILURE,
    dg.DagsterRunStatus.CANCELED,
}

ferceqr_sensor_status = dg.DefaultSensorStatus.RUNNING


def _backfill_sensor_skip_reason_or_runs(
    context: dg.RunStatusSensorContext,
) -> dg.SkipReason | tuple[str, Sequence[dg.DagsterRun]]:
    """Return SkipReason while backfill runs, ``(backfill_id, runs)`` when finished.

    Shared by the success and failure sensors so they both coordinate on the same
    backfill state checks. Only one sensor invocation per backfill will make it past
    this gate — the last one to reach terminal state.

    Returns a ``SkipReason`` when:

    - The triggering run is not part of a backfill.
    - No backfill sibling runs are found yet (state still settling).
    - Any sibling runs are still non-terminal (queued, starting, etc.).

    Returns ``(backfill_id, backfill_runs)`` when all backfill runs have reached a
    terminal status (success, failure, or canceled). Callers inspect the run statuses
    to decide whether the success sensor or the failure sensor should produce a
    ``RunRequest``.
    """
    backfill_id = context.dagster_run.tags.get(DAGSTER_BACKFILL_TAG)
    if not backfill_id:
        return dg.SkipReason(
            f"Run {context.dagster_run.run_id} has no {DAGSTER_BACKFILL_TAG} tag; "
            "FERC EQR deployment sensor only triggers on backfill runs."
        )

    backfill_runs = context.instance.get_runs(
        filters=dg.RunsFilter(
            job_name=context.dagster_run.job_name,
            tags={DAGSTER_BACKFILL_TAG: backfill_id},
        )
    )

    if not backfill_runs:
        return dg.SkipReason(
            f"No runs found for FERC EQR backfill {backfill_id}; "
            "waiting for backfill state to settle."
        )

    if any(run.status in NON_TERMINAL_RUN_STATUSES for run in backfill_runs):
        return dg.SkipReason(
            f"FERCEQR backfill {backfill_id} still in progress; "
            "waiting for all runs to complete before sending a single notification."
        )

    return backfill_id, backfill_runs


def _collect_source_partitions(backfill_runs: Sequence[dg.DagsterRun]) -> list[str]:
    """Return sorted, deduplicated partition keys from a set of backfill runs."""
    return sorted(
        {
            partition
            for run in backfill_runs
            if (partition := run.tags.get("dagster/partition")) is not None
        }
    )


def _evaluate_ferceqr_backfill_sensor_condition(
    context: dg.RunStatusSensorContext,
) -> dg.SkipReason | dg.RunRequest:
    """Apply the shared FERC EQR backfill sensor decision logic.

    Both the success and failure sensors delegate here. Once all backfill runs are
    terminal, the function checks whether any failed and routes to the appropriate
    downstream deployment asset. If both sensors happen to fire at the same time (race),
    they produce RunRequests with the same ``run_key`` prefix so Dagster deduplicates
    them automatically.
    """
    result = _backfill_sensor_skip_reason_or_runs(context)
    if isinstance(result, dg.SkipReason):
        return result

    backfill_id, backfill_runs = result
    source_partitions = _collect_source_partitions(backfill_runs)

    if any(run.status in FAILED_TERMINAL_RUN_STATUSES for run in backfill_runs):
        logger.error(
            f"FERCEQR backfill {backfill_id} completed with failures; "
            f"requesting aggregated failure notification for partitions: "
            f"{source_partitions}"
        )
        run_key_prefix = "ferceqr_deployment_failure_backfill"
        asset_name = "handle_ferceqr_failure"
    else:
        run_key_prefix = "ferceqr_deployment_success_backfill"
        asset_name = "deploy_ferceqr"

    tags = {
        FERCEQR_SOURCE_RUN_ID_TAG: context.dagster_run.run_id,
        FERCEQR_SOURCE_PARTITIONS_TAG: json.dumps(source_partitions),
    }

    return dg.RunRequest(
        run_key=f"{run_key_prefix}:{backfill_id}",
        tags=tags,
        asset_selection=[dg.AssetKey(asset_name)],
    )


ferceqr_failure_sensor = dg.run_status_sensor(
    name="ferceqr_failure_sensor",
    run_status=dg.DagsterRunStatus.FAILURE,
    monitored_jobs=[ferceqr_job],
    request_job=ferceqr_deployment_job,
    default_status=ferceqr_sensor_status,
    minimum_interval_seconds=60,
)(_evaluate_ferceqr_backfill_sensor_condition)

ferceqr_success_sensor = dg.run_status_sensor(
    name="ferceqr_success_sensor",
    run_status=dg.DagsterRunStatus.SUCCESS,
    monitored_jobs=[ferceqr_job],
    request_job=ferceqr_deployment_job,
    default_status=ferceqr_sensor_status,
    minimum_interval_seconds=60,
)(_evaluate_ferceqr_backfill_sensor_condition)

default_sensors = [
    ferceqr_success_sensor,
    ferceqr_failure_sensor,
]

__all__ = [
    "default_sensors",
    "ferceqr_failure_sensor",
    "ferceqr_success_sensor",
    "ferceqr_sensor_status",
]
