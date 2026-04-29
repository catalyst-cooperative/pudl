"""Dagster sensors for PUDL.

This module defines sensor-based automation that watches the state of the PUDL code
location and requests follow-on work when specific conditions are met. Add sensor
definitions here when they poll Dagster state, external state, or partition progress to
trigger a run, rather than when the logic belongs inside an asset or job itself. Keep
the module focused on automation entrypoints and their shared defaults.

For the underlying Dagster concept, see https://docs.dagster.io/guides/automate/sensors
"""

import os

import dagster as dg
import pandas as pd

from pudl.dagster.partitions import ferceqr_year_quarters
from pudl.deploy.ferceqr import FERCEQR_TRANSFORM_ASSETS
from pudl.logging_helpers import get_logger

logger = get_logger(__name__)

ferceqr_deployment_sensor_status = (
    dg.DefaultSensorStatus.RUNNING
    if os.getenv("FERCEQR_BUILD", None)
    else dg.DefaultSensorStatus.STOPPED
)


@dg.sensor(
    default_status=ferceqr_deployment_sensor_status,
    minimum_interval_seconds=60,
    asset_selection=["deploy_ferceqr", "handle_ferceqr_deployment_failure"],
)
def ferceqr_deployment_sensor(context: dg.RunStatusSensorContext):
    """Monitor a batch FERC EQR backfill and trigger deployment handling.

    This sensor is only enabled when ``FERCEQR_BUILD`` is set, which is the case for
    the Google Batch jobs that run the published FERC EQR build. Every 60 seconds it
    polls Dagster for the partition status of the quarterly extract asset and the core
    transformed FERC EQR assets.

    While any required partition is still in progress, or has not yet been attempted,
    the sensor does nothing. Once the backfill is complete, it requests exactly one of
    the deployment handler assets: ``deploy_ferceqr`` after a successful backfill, or
    ``handle_ferceqr_deployment_failure`` if any partition failed.
    """
    asset_statuses = {}

    for asset_key in FERCEQR_TRANSFORM_ASSETS + ["extract_ferceqr"]:
        partition_statuses = context.instance.get_status_by_partition(
            asset_key=dg.AssetKey(asset_key),
            partition_keys=ferceqr_year_quarters.get_partition_keys(),
            partitions_def=ferceqr_year_quarters,
        )
        asset_statuses[asset_key] = partition_statuses

    asset_statuses = pd.DataFrame(asset_statuses)
    in_progress_parts = (asset_statuses == dg.AssetPartitionStatus.IN_PROGRESS).any(
        axis=1
    )
    not_started_parts = asset_statuses[FERCEQR_TRANSFORM_ASSETS].isnull().any(
        axis=1
    ) & (asset_statuses["extract_ferceqr"] != dg.AssetPartitionStatus.FAILED)

    if (in_progress_parts | not_started_parts).any():
        logger.info("Partitions still in progress, continuing.")
        return None

    if (asset_statuses == dg.AssetPartitionStatus.FAILED).any(axis=1).any():
        return dg.RunRequest(
            run_key="ferceqr_deployment",
            asset_selection=[dg.AssetKey("handle_ferceqr_deployment_failure")],
        )

    return dg.RunRequest(
        run_key="ferceqr_deployment",
        asset_selection=[dg.AssetKey("deploy_ferceqr")],
    )


default_sensors = [ferceqr_deployment_sensor]

__all__ = [
    "default_sensors",
    "ferceqr_deployment_sensor",
    "ferceqr_deployment_sensor_status",
]
