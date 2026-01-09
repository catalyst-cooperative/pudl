"""Define tooling for monitoring the ferceqr_etl job during batch builds.

In this module we define a Dagster Sensor that will monitor the status of
a ``ferceqr`` backfill. This sensor will only run if the environment variable
``FERCEQR_BUILD`` is set, which should only be the case when executing a job
through our google batch build infrastructure. In this context, the sensor
will get executed every 60 seconds to see if the backfill has completed.
Once it determines that the backfill is complete, it will publish the results
if the build was successful and send a notification via slack. Finally, it will
create a specific file in the ``PUDL_OUTPUT`` directory indicating the status
of the build, and triggering shutdown of the batch job.
"""

import os
from pathlib import Path
from typing import Literal

import dagster as dg
import gcsfs
import pandas as pd
import s3fs
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
    """Notify build script that job is complete by creating a status file."""
    (PudlPaths().output_dir / status).touch()


@dg.asset
def deploy_ferceqr():
    """Publish EQR outputs to cloud storage."""
    # Get output locations for S3 and GCS
    distribution_paths = [
        (
            UPath(os.environ["GCS_OUTPUT_BUCKET"]),
            gcsfs.GCSFileSystem(
                project=os.environ["GCP_BILLING_PROJECT"],
                requester_pays=True,
            ),
        ),
        (UPath(os.environ["S3_OUTPUT_BUCKET"]), s3fs.S3FileSystem()),
    ]
    # Loop through output locations and copy parquet files to buckets
    logger.info("Build successful, deploying ferceqr data.")
    for distribution_path, fs in distribution_paths:
        for table in FERCEQR_TRANSFORM_ASSETS:
            logger.info(f"Copying {table} to {distribution_path}.")
            table_remote_path = distribution_path / table

            # UPath's don't work well with requester pays buckets, so use the fsspec
            # filesystem directly
            fs.mkdirs(path=table_remote_path, exist_ok=True)
            fs.put(
                f"{ParquetData(table_name=table).parquet_directory}/",
                f"{table_remote_path}/",
                recursive=True,
            )

    # Send slack notification about successful build
    logger.info("Notifying slack about successful build.")
    _notify_slack_deployments_channel(
        ":large_green_circle: :sunglasses: :unicorn_face: :rainbow: ferceqr deployment succeeded!!"
        " :partygritty: :database_parrot: :blob-dance: :large_green_circle:\n\n"
        f"Parquet files published to: {', '.join(map(str, distribution_paths))}\n"
        f"Logfile can be found at: {os.environ['GCS_LOGS_BUCKET']}"
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
    """Check if the EQR backfill is complete and handle appropriately.

    This sensor is configured to run every 60 seconds when the EQR deployment job is
    running (it won't run at all in local development). Once it detects that the job
    has completed, it will return a ``RunRequest`` object requesting dagster to execute
    an asset to handle either a successful or failed run. We need to set ``run_key``
    in the ``RunRequest`` because dagster will only execute one run per key, so if
    the sensor executes while one of handler assets is still in progress, dagster will
    not try to execute the handler asset again. We can also make ``run_key`` a static
    key, because our batch jobs have no memory of previous executions.
    """
    asset_statuses = {}

    # Query status of all assets across all partitions
    for asset_key in FERCEQR_TRANSFORM_ASSETS + ["extract_ferceqr"]:
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
    ) & (asset_statuses["extract_ferceqr"] != dg.AssetPartitionStatus.FAILED)

    # Don't do anything if there are still partitions that haven't finished running
    if (in_progress_parts | not_started_parts).any():
        logger.info("Partitions still in progress, continuing.")
        return None
    # The backfill must be complete to reach this point
    # Check if any partitions failed
    if (asset_statuses == dg.AssetPartitionStatus.FAILED).any(axis=1).any():
        # Write status to CSV file
        asset_statuses.to_csv(_get_etl_status_csv_path())

        # Execute asset to send slack notification about failure
        return dg.RunRequest(
            run_key="ferceqr_deployment",
            asset_selection=[dg.AssetKey("handle_ferceqr_deployment_failure")],
        )
    # Publish parquet files after successful run
    return dg.RunRequest(
        run_key="ferceqr_deployment",
        asset_selection=[dg.AssetKey("deploy_ferceqr")],
    )
