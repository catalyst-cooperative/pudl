"""Dagster sensors for PUDL.

This module defines sensor-based automation that watches the state of the PUDL code
location and requests follow-on work when specific conditions are met. Add sensor
definitions here when they poll Dagster state, external state, or partition progress to
trigger a run, rather than when the logic belongs inside an asset or job itself. Keep
the module focused on automation entrypoints and their shared defaults.

For the underlying Dagster concept, see https://docs.dagster.io/guides/automate/sensors
"""

import json

import dagster as dg

from pudl.dagster.assets.deploy.ferceqr import (
    FERCEQR_SOURCE_PARTITION_TAG,
    FERCEQR_SOURCE_PARTITIONS_TAG,
    FERCEQR_SOURCE_RUN_ID_TAG,
    FERCEQR_SOURCE_RUN_STATUS_TAG,
)
from pudl.dagster.jobs import ferceqr_deployment_job, ferceqr_job
from pudl.logging_helpers import get_logger

logger = get_logger(__name__)

FERCEQR_BACKFILL_TAG = "dagster/backfill"

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

ferceqr_deployment_sensor_status = dg.DefaultSensorStatus.RUNNING


@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    monitored_jobs=[ferceqr_job],
    request_job=ferceqr_deployment_job,
    default_status=ferceqr_deployment_sensor_status,
    minimum_interval_seconds=60,
)
def ferceqr_deployment_sensor(context: dg.RunStatusSensorContext):
    """Trigger deployment after the FERC EQR job completes successfully.

    This sensor is only enabled when ``FERCEQR_BUILD`` is set, which is the case for
    the Google Batch jobs that run the published FERC EQR build. It reacts to the
    terminal status of the ``ferceqr`` asset job, which already selects the full raw
    and core FERC EQR graph. A success event therefore means every selected core FERC
    EQR asset finished materializing.
    """
    source_partition = context.dagster_run.tags.get("dagster/partition")
    backfill_id = context.dagster_run.tags.get(FERCEQR_BACKFILL_TAG)
    source_partitions = [source_partition] if source_partition else []

    run_key = f"ferceqr_deployment_success:{context.dagster_run.run_id}"
    if backfill_id:
        backfill_runs = context.instance.get_runs(
            filters=dg.RunsFilter(
                job_name=context.dagster_run.job_name,
                tags={FERCEQR_BACKFILL_TAG: backfill_id},
            )
        )

        if not backfill_runs:
            return dg.SkipReason(
                f"No runs found for FERCEQR backfill {backfill_id}; waiting for backfill state to settle."
            )

        backfill_statuses = [run.status for run in backfill_runs]
        if any(status in NON_TERMINAL_RUN_STATUSES for status in backfill_statuses):
            return dg.SkipReason(
                f"FERCEQR backfill {backfill_id} still in progress; waiting for all runs to complete."
            )

        if any(status in FAILED_TERMINAL_RUN_STATUSES for status in backfill_statuses):
            return dg.SkipReason(
                f"FERCEQR backfill {backfill_id} has failed or canceled runs; skipping deployment success trigger."
            )

        source_partitions = sorted(
            {
                run.tags.get("dagster/partition")
                for run in backfill_runs
                if run.tags.get("dagster/partition")
            }
        )

        run_key = f"ferceqr_deployment_success_backfill:{backfill_id}"

    tags = {
        FERCEQR_SOURCE_RUN_ID_TAG: context.dagster_run.run_id,
        FERCEQR_SOURCE_RUN_STATUS_TAG: "SUCCESS",
    }
    if source_partition:
        tags[FERCEQR_SOURCE_PARTITION_TAG] = source_partition
    tags[FERCEQR_SOURCE_PARTITIONS_TAG] = json.dumps(source_partitions)

    return dg.RunRequest(
        run_key=run_key,
        tags=tags,
        asset_selection=[dg.AssetKey("deploy_ferceqr")],
    )


@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.FAILURE,
    monitored_jobs=[ferceqr_job],
    request_job=ferceqr_deployment_job,
    default_status=ferceqr_deployment_sensor_status,
    minimum_interval_seconds=60,
)
def ferceqr_deployment_failure_sensor(context: dg.RunStatusSensorContext):
    """Trigger failure handling after the FERC EQR job fails."""
    source_partition = context.dagster_run.tags.get("dagster/partition")
    tags = {
        FERCEQR_SOURCE_RUN_ID_TAG: context.dagster_run.run_id,
        FERCEQR_SOURCE_RUN_STATUS_TAG: "FAILURE",
    }
    if source_partition:
        tags[FERCEQR_SOURCE_PARTITION_TAG] = source_partition
        tags[FERCEQR_SOURCE_PARTITIONS_TAG] = json.dumps([source_partition])

    logger.error(
        f"FERC EQR job run {context.dagster_run.run_id} failed; triggering deployment failure handler."
    )
    return dg.RunRequest(
        run_key=f"ferceqr_deployment_failure:{context.dagster_run.run_id}",
        tags=tags,
        asset_selection=[dg.AssetKey("handle_ferceqr_deployment_failure")],
    )


default_sensors = [
    ferceqr_deployment_sensor,
    ferceqr_deployment_failure_sensor,
]

__all__ = [
    "default_sensors",
    "ferceqr_deployment_failure_sensor",
    "ferceqr_deployment_sensor",
    "ferceqr_deployment_sensor_status",
]
