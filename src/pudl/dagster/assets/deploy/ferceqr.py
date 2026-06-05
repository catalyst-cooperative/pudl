"""Define deployment helper assets for publishing FERC EQR outputs.

These assets run during batch builds to publish transformed FERC EQR outputs,
notify Zulip of success or failure, and create status files that tell the batch
job when deployment handling is complete.
"""

import os
import traceback
from collections.abc import Callable
from typing import Literal

import dagster as dg
import gcsfs
import s3fs
from upath import UPath

from pudl.dagster.resources import ZulipNotificationResource
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


def _get_logfile_list(build_id: str) -> str:
    """Return pointer to logs to send in Zulip message."""
    download_url = (
        "https://storage.cloud.google.com/builds.catalyst.coop/"
        f"ferceqr_logs/{build_id}.log"
    )
    console_url = (
        "https://console.cloud.google.com/batch/jobsDetail/regions/us-west1/jobs/"
        f"run-ferceqr-etl-{build_id}/logs?project=catalyst-cooperative-pudl"
    )
    return (
        "### Review FERC EQR Build Logs\n\n"
        f"* GCS URL: `gs://builds.catalyst.coop/ferceqr_logs/{build_id}.log`\n\n"
        f"* [Download FERC EQR logs to review locally]({download_url})\n"
        f"* [Review FERC EQR logs in the Google Cloud Console]({console_url})\n"
    )


def _write_status_file(status: Literal["SUCCESS", "FAILURE"], pudl_paths: PudlPaths):
    """Notify build script that job is complete by creating a status file."""
    (pudl_paths.pudl_output / status).touch()


def deployment_status_asset(
    handler: Callable,
) -> dg.AssetsDefinition:
    """Create a wrapper for deployment handler assets.

    This is useful to gracefully handle errors if the deployment status assets fail
    for any reason. When these assets fail, sometimes the logs don't show up in the
    batch job appropriately, and the status file never gets created, so the job keeps
    running until it eventually times out.
    """

    @dg.asset(
        name=handler.__name__,
        required_resource_keys={"pudl_paths", "zulip_notifications"},
    )
    def _status_handler_asset(context):
        try:
            handler(
                context.resources.pudl_paths,
                context.resources.zulip_notifications,
            )
        except Exception:
            logger.error("FERCEQR deployment handler failed!")
            logger.error(traceback.format_exc())
            _write_status_file("FAILURE", context.resources.pudl_paths)

    return _status_handler_asset


@deployment_status_asset
def deploy_ferceqr(
    pudl_paths: PudlPaths,
    zulip: ZulipNotificationResource,
):
    """Publish EQR outputs to cloud storage."""
    # Build list of (cloud_path, filesystem) pairs for distribution
    distribution_targets = [
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
    for distribution_path, fs in distribution_targets:
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

    # Send Zulip notification about successful build
    published_paths = [str(path) for path, _fs in distribution_targets]
    logger.info("FERC EQR build succeeded. Notifying Zulip.")
    zulip.send_stream_message(
        stream="pudl-deployments",
        topic="build-deploy-pudl",
        content=(
            ":check: FERC EQR deployment **SUCCESS**!\n\n"
            f"Parquet files published to: {', '.join(published_paths)}\n\n"
        )
        + _get_logfile_list(os.getenv("BUILD_ID", "no-build-id")),
    )
    _write_status_file("SUCCESS", pudl_paths)


@deployment_status_asset
def handle_ferceqr_deployment_failure(
    pudl_paths: PudlPaths,
    zulip: ZulipNotificationResource,
):
    """Send notification if EQR deployment failed."""
    logger.error("FERC EQR build failed. Notifying Zulip.")
    zulip.send_stream_message(
        stream="pudl-deployments",
        topic="build-deploy-pudl",
        content=(
            ":x: FERC EQR deployment **FAILURE**!\n\n"
            + _get_logfile_list(os.getenv("BUILD_ID", "no-build-id"))
        ),
    )
    _write_status_file("FAILURE", pudl_paths)
