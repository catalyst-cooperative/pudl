"""Define deployment helper assets for publishing FERC EQR outputs.

These assets run during batch builds to publish transformed FERC EQR outputs,
notify Slack of success or failure, and create status files that tell the batch
job when deployment handling is complete.
"""

import os
import traceback
from collections.abc import Callable
from pathlib import Path
from typing import Literal

import dagster as dg
import gcsfs
import s3fs
from slack_sdk import WebClient
from upath import UPath

from pudl.helpers import ParquetData
from pudl.logging_helpers import get_logger
from pudl.metadata.classes import PUDL_PACKAGE
from pudl.workspace.setup import PudlPaths

logger = get_logger(__name__)

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


def _get_etl_status_csv_path() -> Path:
    return PudlPaths().output_dir / "ferceqr_etl_status.csv"


def _get_logfile_pointer() -> str:
    """Return pointer to logs to send in slack message."""
    return (
        f"<https://storage.cloud.google.com/builds.catalyst.coop/ferceqr_logs/{os.environ['BUILD_ID']}.log|*Download ferceqr logs to your computer*>\n\n"
        f"<https://console.cloud.google.com/batch/jobsDetail/regions/us-west1/jobs/run-ferceqr-etl-{os.environ['BUILD_ID']}/logs?project=catalyst-cooperative-pudl|*Query ferceqr logs online*>\n\n"
    )


def _write_status_file(status: Literal["SUCCESS", "FAILURE"]):
    """Notify build script that job is complete by creating a status file."""
    (PudlPaths().output_dir / status).touch()


def deployment_status_asset(
    handler: Callable,
) -> dg.AssetsDefinition:
    """Create a wrapper for deployment handler assets.

    This is useful to gracefully handle errors if the deployment status assets fail
    for any reason. When these assets fail, sometimes the logs don't show up in the
    batch job appropriately, and the status file never gets created, so the job keeps
    running until it eventually times out.
    """

    @dg.asset(name=handler.__name__)
    def _status_handler_asset():
        try:
            handler()
        except Exception:
            logger.error("FERCEQR deployment handler failed!")
            logger.error(traceback.format_exc())
            _write_status_file("FAILURE")

    return _status_handler_asset


@deployment_status_asset
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
    # Get 'datapackage.json' data to write to distribution paths
    datapackage_bytes = (
        PUDL_PACKAGE.to_frictionless(include_pattern=r"core_ferceqr.*")
        .to_json()
        .encode(encoding="utf-8")
    )

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
        # Copy datapackage to distribution path
        fs.pipe(
            f"{distribution_path}/ferceqr_parquet_datapackage.json",
            value=datapackage_bytes,
        )

    # Send slack notification about successful build
    logger.info("Notifying slack about successful build.")
    _notify_slack_deployments_channel(
        (
            ":large_green_circle: :sunglasses: :unicorn_face: :rainbow: ferceqr deployment succeeded!!"
            " :partygritty: :database_parrot: :blob-dance: :large_green_circle:\n\n"
            f"Parquet files published to: {', '.join(str(p[0]) for p in distribution_paths)}\n"
            f"Logfile can be found at: {os.environ['GCS_LOGS_BUCKET']}\n\n"
        )
        + _get_logfile_pointer()
    )
    _write_status_file("SUCCESS")


@deployment_status_asset
def handle_ferceqr_deployment_failure():
    """Send notification if EQR deployment failed."""
    logger.error("Build failed, notifying slack.")
    _notify_slack_deployments_channel(
        message=(
            ":x: ferceqr deployment failed!\n\n"
            + _get_logfile_pointer()
            + "See individual step status in attached file: "
        ),
        attached_file_path=str(_get_etl_status_csv_path()),
    )
    _write_status_file("FAILURE")
