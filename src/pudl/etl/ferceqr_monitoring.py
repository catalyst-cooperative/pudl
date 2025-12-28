"""Define tooling for monitoring the ferceqr_etl job during batch builds."""

import os
from pathlib import Path
from typing import Literal

import dagster as dg
import pandas as pd
from slack_sdk import WebClient
from upath import UPath

from pudl.helpers import ParquetData
from pudl.logging_helpers import get_logger
from pudl.settings import ferceqr_year_quarters
from pudl.workspace.setup import PudlPaths

logger = get_logger(__name__)


ferceqr_sensor_status = (
    dg.DefaultSensorStatus.RUNNING
    if os.getenv("FERCEQR_BUILD", None)
    else dg.DefaultSensorStatus.STOPPED
)

FERCEQR_TRANSFORM_ASSETS = [
    "core_ferceqr__contracts",
    "core_ferceqr__transactions",
    "core_ferceqr__quarterly_identity",
    "core_ferceqr__quarterly_index_pub",
]


def _notify_slack_deployments_channel(
    message: str, attached_file_path: str | None = None
):
    """Send string message to PUDL deployments channel."""
    client = WebClient(token=os.environ["SLACK_TOKEN"])
    channel = "C03FHB9N0PQ"
    if attached_file_path is not None:
        client.files_upload_v2(
            channel=channel,
            file=attached_file_path,
            title=f"{os.environ['BUILD_ID']} Status",
            initial_comment=message,
        )
    client.chat_postMessage(
        channel=channel,
        text=message,
    )


def _write_status_file(status: Literal["SUCCESS", "FAILURE"]):
    """To notify the top level build script that the job is complete, create a file in the output directory."""
    (PudlPaths().output_dir / status).touch()


@dg.asset
def deploy_ferceqr():
    """Publish EQR outputs to cloud storage."""
    output_location = "gs://builds.catalyst.coop/ferceqr"
    # Copy parquet files to GCS
    logger.info("Build successful, copying ferceqr data to GCS.")
    for table in FERCEQR_TRANSFORM_ASSETS:
        base_path = UPath(output_location) / table
        base_path.mkdir(exist_ok=True)

        # Loop through partitioned parquet files for table and write to GCS
        for file in ParquetData(table).parquet_directory.iterdir():
            destination_path = base_path / file.name
            destination_path.write_bytes(file.read_bytes())

    # Send slack notification about successful build
    _notify_slack_deployments_channel(
        ":large_green_circle: :sunglasses: :unicorn_face: :rainbow: ferceqr deployment succeeded!!"
        " :partygritty: :database_parrot: :blob-dance: :large_green_circle:\n\n"
        f"Parquet files written to: {output_location}"
    )
    _write_status_file("SUCCESS")


@dg.asset
def handle_ferceqr_deployment_failure():
    """Send notification if EQR deployment failed."""
    logger.info("Build failed, notifying slack.")
    _notify_slack_deployments_channel(
        message=":x: ferceqr deployment failed! See step status here:",
        attached_file_path=str(_get_etl_status_csv_path()),
    )
    _write_status_file("FAILURE")


def _get_etl_status_csv_path() -> Path:
    return PudlPaths().output_dir / "ferceqr_etl_status.csv"


@dg.sensor(
    default_status=ferceqr_sensor_status,
    minimum_interval_seconds=60,
    asset_selection=["deploy_ferceqr", "handle_ferceqr_deployment_failure"],
)
def ferceqr_sensor(context: dg.RunStatusSensorContext):
    """Generate a failure summary notification after all partitions have completed."""
    asset_statuses = {}

    # Query status of all assets across all partitions
    for asset_key in FERCEQR_TRANSFORM_ASSETS + ["extract_eqr"]:
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
    not_started_parts = asset_statuses[FERCEQR_TRANSFORM_ASSETS].isnull().any(
        axis=1
    ) & (asset_statuses["extract_eqr"] != dg.AssetPartitionStatus.FAILED)

    # Don't do anything if there are still partitions that haven't finished running
    if (in_progress_parts | not_started_parts).any():
        logger.info("Partitions still in progress, continuing.")
        return
    # The backfill must be complete to reach this point
    # Check if any partitions failed
    if (asset_statuses == dg.AssetPartitionStatus.FAILED).any(axis=1).any():
        # Write status to CSV file
        asset_statuses.to_csv(_get_etl_status_csv_path())

        # Execute asset to send slack notification about failure
        dg.RunRequest(
            run_key="ferceqr_deployment",
            asset_selection=[dg.AssetKey("handle_ferceqr_deployment_failure")],
        )
    else:
        # Publish parquet files after successful run
        dg.RunRequest(
            run_key="ferceqr_deployment",
            asset_selection=[dg.AssetKey("deploy_ferceqr")],
        )
