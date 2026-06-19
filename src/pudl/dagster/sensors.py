"""Dagster sensors for PUDL.

This module defines sensor-based automation that watches the state of the PUDL code
location and requests follow-on work when specific conditions are met. Add sensor
definitions here when they poll Dagster state, external state, or partition progress to
trigger a run, rather than when the logic belongs inside an asset or job itself. Keep
the module focused on automation entrypoints and their shared defaults.

For the underlying Dagster concept, see https://docs.dagster.io/guides/automate/sensors
"""

import json
from collections.abc import Callable, Sequence

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


FerceqrBackfillSensorPredicate = Callable[[Sequence[dg.DagsterRun]], bool]
FerceqrBackfillSkipMessageBuilder = Callable[[str], str]


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


def _build_ferceqr_run_request(
    *,
    context: dg.RunStatusSensorContext,
    backfill_id: str,
    backfill_runs: Sequence[dg.DagsterRun],
    run_key_prefix: str,
    asset_name: str,
) -> dg.RunRequest:
    """Build a deployment-job run request from completed FERC EQR backfill runs.

    This helper centralizes the tag construction shared by the success and failure
    sensors. It gathers the full partition set from the completed backfill, stores the
    triggering run ID for later status lookup, and targets exactly one deployment asset
    in the downstream deployment job.
    """
    source_partitions = _collect_source_partitions(backfill_runs)

    tags = {
        FERCEQR_SOURCE_RUN_ID_TAG: context.dagster_run.run_id,
        FERCEQR_SOURCE_PARTITIONS_TAG: json.dumps(source_partitions),
    }

    return dg.RunRequest(
        run_key=f"{run_key_prefix}:{backfill_id}",
        tags=tags,
        asset_selection=[dg.AssetKey(asset_name)],
    )


def _evaluate_ferceqr_backfill_sensor_condition(
    context: dg.RunStatusSensorContext,
    *,
    should_request: FerceqrBackfillSensorPredicate,
    skip_message: FerceqrBackfillSkipMessageBuilder,
    run_key_prefix: str,
    asset_name: str,
    log_on_request: bool = False,
) -> dg.SkipReason | dg.RunRequest:
    """Apply the shared FERC EQR backfill sensor decision logic.

    The success and failure sensors differ only in the terminal-state predicate they
    care about and the deployment asset they request. This function performs the common
    backfill coordination, delegates the decision to ``should_request``, and either
    returns a ``SkipReason`` or builds the downstream ``RunRequest``.
    """
    result = _backfill_sensor_skip_reason_or_runs(context)
    if isinstance(result, dg.SkipReason):
        return result

    backfill_id, backfill_runs = result
    if not should_request(backfill_runs):
        return dg.SkipReason(skip_message(backfill_id))

    if log_on_request:
        source_partitions = _collect_source_partitions(backfill_runs)
        logger.error(
            f"FERCEQR backfill {backfill_id} completed with failures; "
            f"requesting aggregated failure notification for partitions: "
            f"{source_partitions}"
        )

    return _build_ferceqr_run_request(
        context=context,
        backfill_id=backfill_id,
        backfill_runs=backfill_runs,
        run_key_prefix=run_key_prefix,
        asset_name=asset_name,
    )


def _ferceqr_run_status_sensor_factory(
    *,
    name: str,
    run_status: dg.DagsterRunStatus,
    should_request: FerceqrBackfillSensorPredicate,
    skip_message: FerceqrBackfillSkipMessageBuilder,
    run_key_prefix: str,
    asset_name: str,
    log_on_request: bool = False,
) -> dg.RunStatusSensorDefinition:
    """Create a named FERC EQR run status sensor from shared parameters.

    Dagster registers run status sensors through the decorator, so we use a small
    factory to apply the decorator twice with different run statuses and behavior while
    keeping the operational logic in one place. The returned definition is a distinct
    sensor with its own Dagster name, trigger status, and downstream asset target.
    """

    @dg.run_status_sensor(
        name=name,
        run_status=run_status,
        monitored_jobs=[ferceqr_job],
        request_job=ferceqr_deployment_job,
        default_status=ferceqr_sensor_status,
        minimum_interval_seconds=60,
    )
    def _sensor(context: dg.RunStatusSensorContext):
        """Delegate a Dagster sensor tick to the shared FERC EQR sensor logic."""
        return _evaluate_ferceqr_backfill_sensor_condition(
            context,
            should_request=should_request,
            skip_message=skip_message,
            run_key_prefix=run_key_prefix,
            asset_name=asset_name,
            log_on_request=log_on_request,
        )

    return _sensor


ferceqr_success_sensor = _ferceqr_run_status_sensor_factory(
    name="ferceqr_success_sensor",
    run_status=dg.DagsterRunStatus.SUCCESS,
    should_request=lambda runs: (
        not any(run.status in FAILED_TERMINAL_RUN_STATUSES for run in runs)
    ),
    skip_message=lambda backfill_id: (
        f"FERCEQR backfill {backfill_id} has failed or canceled runs; "
        "the failure sensor will produce the aggregated notification."
    ),
    run_key_prefix="ferceqr_deployment_success_backfill",
    asset_name="deploy_ferceqr",
)

ferceqr_failure_sensor = _ferceqr_run_status_sensor_factory(
    name="ferceqr_failure_sensor",
    run_status=dg.DagsterRunStatus.FAILURE,
    should_request=lambda runs: any(
        run.status in FAILED_TERMINAL_RUN_STATUSES for run in runs
    ),
    skip_message=lambda backfill_id: (
        f"FERCEQR backfill {backfill_id} has no failed runs; "
        "the success sensor will produce the deployment."
    ),
    run_key_prefix="ferceqr_deployment_failure_backfill",
    asset_name="handle_ferceqr_failure",
    log_on_request=True,
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
