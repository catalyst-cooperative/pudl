#!/usr/bin/env python
"""Deploy PUDL ETL outputs to cloud storage and update git branches.

This CLI orchestrates deployment of completed PUDL ETL builds to public cloud
storage (GCS and S3), git branch updates, Zenodo releases, and Cloud Run deployments.

The script takes a deploy_type (nightly or stable), a source_path pointing to ETL
outputs (local or GCS), a git tag, and an optional staging flag for safe testing.

Examples:
    Deploy nightly build to production:
        pudl_deploy nightly gs://builds.catalyst.coop/2025-02-05-1234-abc123-main
            --git-tag nightly-2025-02-05

    Deploy stable release to production:
        pudl_deploy stable gs://builds.catalyst.coop/2025-02-05-1234-abc123-v2025.2.3
            --git-tag v2025.2.3

    Test deployment changes with staging mode:
        pudl_deploy nightly gs://builds.catalyst.coop/2025-02-05-1234-abc123-main
            --git-tag nightly-2025-02-05 --staging

Staging mode uploads to staging/ prefixed paths and skips git operations, Zenodo
triggers, and Cloud Run deployments. This allows safe validation of deployment
changes before production use.
"""

import logging
import sys
from pathlib import Path

import click
import coloredlogs

from pudl.etl.distribute_outputs import (
    prepare_outputs_for_distribution,
    set_gcs_temporary_hold,
    trigger_zenodo_release,
    update_cloud_run_service,
    update_git_branch,
    upload_outputs,
)
from pudl.logging_helpers import get_logger

logger = get_logger(__name__)
coloredlogs.install(
    level=logging.INFO,
    logger=logger,
    fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
)


@click.command(
    help=__doc__,
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.argument(
    "deploy_type",
    type=click.Choice(["nightly", "stable"], case_sensitive=False),
)
@click.argument(
    "source_path",
    type=str,
)
@click.option(
    "--git-tag",
    type=str,
    required=True,
    help="Git tag to merge into branch (e.g., nightly-2025-02-05 or v2025.2.3).",
)
@click.option(
    "--staging",
    is_flag=True,
    default=False,
    help="Upload to staging/ locations for validation. Skips git operations, "
    "Zenodo triggers, and Cloud Run deployments.",
    show_default=True,
)
def pudl_deploy(
    deploy_type: str,
    source_path: str,
    git_tag: str,
    staging: bool,
):
    """Deploy PUDL ETL outputs to cloud storage and external services.

    Orchestrates the full deployment workflow:
    1. Prepare outputs (compress SQLite, create parquet archive)
    2. Upload to cloud storage (GCS and S3)
    3. Update git branches (if not staging)
    4. Set GCS temporary hold for versioned releases (stable only, not staging)
    5. Trigger Zenodo release (if not staging)
    6. Update Cloud Run service (nightly only, not staging)
    """
    try:
        logger.info(
            f"Starting deployment: deploy_type={deploy_type}, "
            f"source_path={source_path}, git_tag={git_tag}, staging={staging}"
        )

        # Convert source_path to Path object for local paths
        source_dir = Path(source_path)

        # Step 1: Prepare outputs for distribution
        logger.info("Step 1: Preparing outputs for distribution")
        prepare_outputs_for_distribution(source_dir)

        # Step 2: Configure paths based on deploy_type
        logger.info(f"Step 2: Configuring paths for {deploy_type} deployment")
        if deploy_type == "nightly":
            path_suffixes = ["nightly", "eel-hole"]
            git_branch = "nightly"
            zenodo_env = "sandbox"
            zenodo_source = "s3://pudl.catalyst.coop/nightly/"
            zenodo_ignore = ""
            zenodo_publish = True
            update_cloudrun = True
        else:  # stable
            path_suffixes = [git_tag, "stable"]
            git_branch = "stable"
            zenodo_env = "production"
            zenodo_source = f"s3://pudl.catalyst.coop/{git_tag}/"
            zenodo_ignore = r".*parquet.*"
            zenodo_publish = False
            update_cloudrun = False

        # Step 3: Upload outputs to cloud storage
        logger.info("Step 3: Uploading outputs to cloud storage")
        upload_outputs(
            source_dir=source_dir,
            path_suffixes=path_suffixes,
            staging=staging,
        )

        # Step 4: Git operations (skip in staging mode)
        if not staging:
            logger.info("Step 4: Updating git branch")
            update_git_branch(tag=git_tag, branch=git_branch)
        else:
            logger.info("Step 4: Skipping git operations (staging mode)")

        # Step 5: Set GCS temporary hold for stable releases (skip in staging mode)
        if deploy_type == "stable" and not staging:
            logger.info("Step 5: Setting GCS temporary hold on versioned release")
            gcs_path = f"gs://pudl.catalyst.coop/{git_tag}/"
            set_gcs_temporary_hold(gcs_path=gcs_path)
        else:
            logger.info("Step 5: Skipping GCS temporary hold (nightly or staging mode)")

        # Step 6: Trigger Zenodo release (skip in staging mode)
        if not staging:
            logger.info("Step 6: Triggering Zenodo release")
            trigger_zenodo_release(
                env=zenodo_env,
                source_dir=zenodo_source,
                ignore_regex=zenodo_ignore,
                publish=zenodo_publish,
            )
        else:
            logger.info("Step 6: Skipping Zenodo trigger (staging mode)")

        # Step 7: Update Cloud Run service for nightly (skip in staging mode)
        if update_cloudrun and not staging:
            logger.info("Step 7: Updating Cloud Run service")
            update_cloud_run_service(service_name="pudl-viewer")
        else:
            logger.info("Step 7: Skipping Cloud Run update (stable or staging mode)")

        logger.info("Deployment completed successfully")

    except Exception:
        logger.exception("Deployment failed")
        sys.exit(1)


if __name__ == "__main__":
    pudl_deploy()
