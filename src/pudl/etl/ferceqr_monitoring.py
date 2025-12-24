"""Define tooling for monitoring the ferceqr_etl job during batch builds."""

import os

import dagster as dg
import pandas as pd

from pudl.logging_helpers import get_logger
from pudl.settings import ferceqr_year_quarters
from pudl.workspace.setup import PudlPaths

logger = get_logger(__name__)


ferceqr_sensor_status = (
    dg.DefaultSensorStatus.RUNNING
    if os.getenv("FERCEQR_BUILD", None)
    else dg.DefaultSensorStatus.STOPPED
)


def _handle_success():
    print("EQR ETL successful!")
    (PudlPaths().output_dir / "SUCCESS").touch()


def _handle_failure(failed_parts: pd.DataFrame):
    print("EQR ETL failed!")
    (PudlPaths().output_dir / "FAILURE").touch()


@dg.sensor(
    default_status=ferceqr_sensor_status,
    minimum_interval_seconds=60,
)
def ferceqr_sensor(context: dg.RunStatusSensorContext):
    """Generate a failure summary notification after all partitions have completed."""
    asset_statuses = {}

    # Query status of all assets across all partitions
    transform_assets = [
        "core_ferceqr__contracts",
        "core_ferceqr__transactions",
        "core_ferceqr__quarterly_identity",
        "core_ferceqr__quarterly_index_pub",
    ]
    for asset_key in transform_assets + ["extract_eqr"]:
        partition_statuses = context.instance.get_status_by_partition(
            asset_key=dg.AssetKey(asset_key),
            partition_keys=ferceqr_year_quarters.get_partition_keys(),
            partitions_def=ferceqr_year_quarters,
        )
        asset_statuses[asset_key] = partition_statuses

    # Check if all partitions have been attempted
    asset_statuses = pd.DataFrame(asset_statuses)
    in_progress_parts = (asset_statuses == dg.AssetPartitionStatus.IN_PROGRESS).any(
        axis=1
    )
    not_started_parts = asset_statuses[transform_assets].isnull().any(axis=1) & (
        asset_statuses["extract_eqr"] != dg.AssetPartitionStatus.FAILED
    )

    if (in_progress_parts | not_started_parts).any():
        logger.info("Partitions still in progress, continuing.")
        return
    if (
        failed_parts := (asset_statuses == dg.AssetPartitionStatus.FAILED).any(axis=1)
    ).any():
        _handle_failure(failed_parts)
    else:
        _handle_success()
