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
            run.tags.get("dagster/partition")
            for run in backfill_runs
            if run.tags.get("dagster/partition")
        }
    )


@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    monitored_jobs=[ferceqr_job],
    request_job=ferceqr_deployment_job,
    default_status=ferceqr_sensor_status,
    minimum_interval_seconds=60,
)
def ferceqr_success_sensor(context: dg.RunStatusSensorContext):
    """Request deployment only when *all* runs in a FERC EQR backfill succeeded.

    The sensor ignores non-backfill runs entirely. It coordinates with the failure
    sensor via :func:`_backfill_sensor_skip_reason_or_runs` so that exactly one
    sensor invocation per backfill produces a ``RunRequest``.

    Behavior when all backfill runs reach terminal status:

    * **All succeeded** — requests ``deploy_ferceqr`` with the full partition list.
    * **Any failed** — returns a ``SkipReason`` (the failure sensor will handle it).

    See :func:`_backfill_sensor_skip_reason_or_runs` for a description of the
    skip conditions (non-backfill, still running, etc.).

    **Tags set on the RunRequest**

    ``FERCEQR_SOURCE_RUN_ID_TAG``
        The run ID of the triggering run. The deployment asset uses this to reach back
        into the Dagster instance for step statuses and run duration.

    ``FERCEQR_SOURCE_PARTITIONS_TAG``
        A JSON-encoded list of all partition keys that completed successfully.
    """
    result = _backfill_sensor_skip_reason_or_runs(context)
    if isinstance(result, dg.SkipReason):
        return result

    backfill_id, backfill_runs = result
    if any(run.status in FAILED_TERMINAL_RUN_STATUSES for run in backfill_runs):
        return dg.SkipReason(
            f"FERCEQR backfill {backfill_id} has failed or canceled runs; "
            "the failure sensor will produce the aggregated notification."
        )

    source_partitions = _collect_source_partitions(backfill_runs)

    tags = {
        FERCEQR_SOURCE_RUN_ID_TAG: context.dagster_run.run_id,
        FERCEQR_SOURCE_PARTITIONS_TAG: json.dumps(source_partitions),
    }

    return dg.RunRequest(
        run_key=f"ferceqr_deployment_success_backfill:{backfill_id}",
        tags=tags,
        asset_selection=[dg.AssetKey("deploy_ferceqr")],
    )


@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.FAILURE,
    monitored_jobs=[ferceqr_job],
    request_job=ferceqr_deployment_job,
    default_status=ferceqr_sensor_status,
    minimum_interval_seconds=60,
)
def ferceqr_failure_sensor(context: dg.RunStatusSensorContext):
    """Request aggregated failure handling when all runs in a backfill have finished.

    Like the success sensor, this sensor waits until *all* runs in the backfill are
    terminal before producing a single ``RunRequest``. This means we'll receive one
    notification per backfill summarizing every partition's outcome.

    Behavior when all backfill runs reach terminal status:

    * **Any failed** — requests ``handle_ferceqr_failure`` with the full
      partition list. The failure asset sends one Zulip notification covering all
      partitions and writes ``FERCEQR_FAILURE``.
    * **All succeeded** — returns a ``SkipReason`` (the success sensor will handle it).

    See :func:`_backfill_sensor_skip_reason_or_runs` for a description of the
    skip conditions (non-backfill, still running, etc.).

    **Tags set on the RunRequest**

    ``FERCEQR_SOURCE_RUN_ID_TAG``
        The run ID of the triggering (failed) run.

    ``FERCEQR_SOURCE_PARTITIONS_TAG``
        A JSON-encoded list of all partition keys in the backfill.
    """
    result = _backfill_sensor_skip_reason_or_runs(context)
    if isinstance(result, dg.SkipReason):
        return result

    backfill_id, backfill_runs = result
    if not any(run.status in FAILED_TERMINAL_RUN_STATUSES for run in backfill_runs):
        return dg.SkipReason(
            f"FERCEQR backfill {backfill_id} has no failed runs; "
            "the success sensor will produce the deployment."
        )

    source_partitions = _collect_source_partitions(backfill_runs)

    logger.error(
        f"FERCEQR backfill {backfill_id} completed with failures; "
        f"requesting aggregated failure notification for partitions: "
        f"{source_partitions}"
    )

    tags = {
        FERCEQR_SOURCE_RUN_ID_TAG: context.dagster_run.run_id,
        FERCEQR_SOURCE_PARTITIONS_TAG: json.dumps(source_partitions),
    }

    return dg.RunRequest(
        run_key=f"ferceqr_deployment_failure_backfill:{backfill_id}",
        tags=tags,
        asset_selection=[dg.AssetKey("handle_ferceqr_failure")],
    )


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
