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

import sys
import tempfile
from pathlib import Path

import click
from gcsfs import GCSFileSystem

from pudl.etl.deploy_outputs import (
    prepare_outputs_for_distribution,
    set_gcs_temporary_hold,
    trigger_zenodo_release,
    update_git_branch,
    update_pudl_viewer,
    upload_outputs,
)
from pudl.logging_helpers import get_logger

logger = get_logger(__name__)


def _deploy_nightly(source_dir: Path, git_tag: str, staging: bool, github_token: str):
    """Execute nightly deployment workflow.

    Deploys to nightly and eel-hole paths, updates nightly branch, triggers sandbox
    Zenodo release, and updates Cloud Run service.
    """
    logger.info("Executing nightly deployment workflow")

    path_suffixes = ["nightly", "eel-hole"]
    if staging:
        path_suffixes = [f"staging/{s}" for s in path_suffixes]

    upload_outputs(
        source_dir=source_dir,
        path_suffixes=path_suffixes,
    )

    update_git_branch(tag=git_tag, branch="nightly", staging=staging)
    if not staging:
        trigger_zenodo_release(
            build_ref=git_tag,
            env="sandbox",
            source_dir="s3://pudl.catalyst.coop/nightly/",
            ignore_regex="",
            publish=True,
            token=github_token,
        )

        update_pudl_viewer()
    else:
        logger.info("Skipping Zenodo and Cloud Run operations (staging mode)")


def _deploy_stable(source_dir: Path, git_tag: str, staging: bool, github_token: str):
    """Execute stable deployment workflow.

    Deploys to versioned and stable paths, updates stable branch, sets GCS temporary
    hold on versioned release, and triggers production Zenodo release (unpublished).
    """
    logger.info("Executing stable deployment workflow")

    path_suffixes = [git_tag, "stable"]
    if staging:
        path_suffixes = [f"staging/{s}" for s in path_suffixes]

    upload_outputs(
        source_dir=source_dir,
        path_suffixes=path_suffixes,
    )

    update_git_branch(tag=git_tag, branch="stable", staging=staging)

    if not staging:
        gcs_path = f"gs://pudl.catalyst.coop/{git_tag}/"
        set_gcs_temporary_hold(gcs_path=gcs_path)

        trigger_zenodo_release(
            build_ref=git_tag,
            env="production",
            source_dir=f"s3://pudl.catalyst.coop/{git_tag}/",
            ignore_regex=r".*parquet.*",
            publish=False,
            token=github_token,
        )

    else:
        logger.info("Skipping GCS hold and Zenodo operations (staging mode)")


@click.command(
    help=__doc__,
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.argument(
    "deploy_type",
    type=click.Choice(["nightly", "stable"], case_sensitive=False),
)
@click.argument("source_path", type=str)
@click.option(
    "--git-tag",
    type=str,
    required=True,
    help="Git tag to merge into branch (e.g., nightly-2026-02-09 or v2026.2.9).",
)
@click.option(
    "--github-token",
    type=str,
    required=True,
    help="GitHub token for authentication.",
)
@click.option(
    "--staging",
    type=bool,
    default=False,
    help=(
        "Upload to staging locations for validation. Skips Zenodo triggers "
        "and Cloud Run deployments."
    ),
    show_default=True,
)
def pudl_deploy(
    deploy_type: str,
    source_path: str,
    git_tag: str,
    github_token: str,
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
    # TODO 2026-02-11: munge GCS to local - pull out later
    if source_path.startswith("gs://"):
        fs = GCSFileSystem()
        local_copy_path = Path(tempfile.mkdtemp())
        fs.get(source_path, local_copy_path, recursive=True)
    else:
        local_copy_path = Path(source_path)

    try:
        logger.info(
            f"Starting deployment: deploy_type={deploy_type}, "
            f"source_path={source_path}, git_tag={git_tag}, staging={staging}"
        )

        prepare_outputs_for_distribution(local_copy_path)

        if deploy_type == "nightly":
            _deploy_nightly(local_copy_path, git_tag, staging, github_token)
        else:
            _deploy_stable(local_copy_path, git_tag, staging, github_token)

        logger.info("Deployment completed successfully")

    except Exception:
        logger.exception("Deployment failed")
        sys.exit(1)


if __name__ == "__main__":
    pudl_deploy()
