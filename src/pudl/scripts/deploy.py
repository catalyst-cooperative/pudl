"""Deploy PUDL ETL outputs to cloud storage and update git branches.

This CLI orchestrates deployment of completed PUDL ETL builds to public cloud
storage (GCS and S3), git branch updates, Zenodo releases, and Cloud Run deployments.

The script takes a git tag, and an optional staging flag for safe testing. It will
use the git tag to identify builds associated with the tag, and determine whether
the deployment is intended to be a nightly or stable deployment. It expects nightly
deployments to have tags conforming to the pattern 'nightly-YYYY-MM-DD' and stable
deployments 'vYYYY.M.D'.

Examples:
    Deploy nightly build to production:
        pudl_deploy nightly-2025-02-05

    Deploy stable release to production:
        pudl_deploy v2025.2.3

    Test deployment changes with staging mode:
        pudl_deploy nightly-2025-02-05 --staging

Staging mode uploads to staging/ prefixed paths and skips git operations, Zenodo
triggers, and Cloud Run deployments. This allows safe validation of deployment
changes before production use.
"""

import re
import tempfile
from pathlib import Path

import click

from pudl.deployment.deploy_outputs import (
    get_build_from_tag,
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
    "git-tag",
    type=str,
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
    # Check if tag is a nightly or stable build
    if re.match(r"v\d{4}\.\d{1,2}\.\d{1,2}", git_tag):
        deploy_type = "stable"
    elif re.match(r"nightly-\d{4}-\d{2}-\d{2}", git_tag):
        deploy_type = "nightly"
    else:
        raise RuntimeError(
            f"Git tag does not look like a stable or nightly tag. Input tag: {git_tag}"
        )

    # Find build associated with tag
    build_path = get_build_from_tag(git_tag)
    local_copy_path = Path(tempfile.mkdtemp())

    logger.info(
        f"Starting deployment for tag {git_tag}\n"
        f"Build path: {build_path}\n"
        f"Deployment type: {deploy_type}\n"
    )

    prepare_outputs_for_distribution(local_copy_path)

    if deploy_type == "nightly":
        _deploy_nightly(local_copy_path, git_tag, staging, github_token)
    else:
        _deploy_stable(local_copy_path, git_tag, staging, github_token)

    logger.info("Deployment completed successfully")


if __name__ == "__main__":
    pudl_deploy()
