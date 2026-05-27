"""Dagster sensors for PUDL.

This module defines sensor-based automation that watches the state of the PUDL code
location and requests follow-on work when specific conditions are met. Add sensor
definitions here when they poll Dagster state, external state, or partition progress to
trigger a run, rather than when the logic belongs inside an asset or job itself. Keep
the module focused on automation entrypoints and their shared defaults.

For the underlying Dagster concept, see https://docs.dagster.io/guides/automate/sensors
"""

import dagster as dg

from pudl.dagster.assets.deploy.ferceqr import (
    FERCEQR_SOURCE_RUN_ID_TAG,
    FERCEQR_SOURCE_RUN_STATUS_TAG,
    is_ferceqr_build_enabled,
)
from pudl.dagster.jobs import ferceqr_deployment_job, ferceqr_job
from pudl.logging_helpers import get_logger

logger = get_logger(__name__)

ferceqr_deployment_sensor_status = (
    dg.DefaultSensorStatus.RUNNING
    if is_ferceqr_build_enabled()
    else dg.DefaultSensorStatus.STOPPED
)


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
    return dg.RunRequest(
        run_key=f"ferceqr_deployment_success:{context.dagster_run.run_id}",
        tags={
            FERCEQR_SOURCE_RUN_ID_TAG: context.dagster_run.run_id,
            FERCEQR_SOURCE_RUN_STATUS_TAG: "SUCCESS",
        },
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
    logger.error(
        f"FERC EQR job run {context.dagster_run.run_id} failed; triggering deployment failure handler."
    )
    return dg.RunRequest(
        run_key=f"ferceqr_deployment_failure:{context.dagster_run.run_id}",
        tags={
            FERCEQR_SOURCE_RUN_ID_TAG: context.dagster_run.run_id,
            FERCEQR_SOURCE_RUN_STATUS_TAG: "FAILURE",
        },
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
