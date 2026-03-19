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
        pudl_deploy nightly-2025-02-05 --github-token {TOKEN}

    Deploy stable release to production:
        pudl_deploy v2025.2.3 --github-token {TOKEN}

    Test deployment changes with staging mode:
        pudl_deploy nightly-2025-02-05 --staging --github-token {TOKEN}

Staging mode uploads to staging/ prefixed paths and skips git operations, Zenodo
triggers, and Cloud Run deployments. This allows safe validation of deployment
changes before production use.
"""

import tempfile
from pathlib import Path

import click

from pudl.deploy.pudl import (
    DeploymentType,
    get_build_from_tag,
    get_deployment_type_from_tag,
    prepare_outputs_for_distribution,
    set_gcs_temporary_hold,
    trigger_zenodo_release,
    update_git_branch,
    upload_outputs,
)
from pudl.logging_helpers import get_logger

logger = get_logger(__name__)


DEPLOYMENT_TYPE_STATIC_SETTINGS = {
    DeploymentType.NIGHTLY: {
        "zenodo_env": "sandbox",
        "ignore_regex": r"(^.*\\\\.parquet$|^.*pudl_parquet_datapackage\\\\.json$)",
        "publish": True,
    },
    DeploymentType.STABLE: {
        "zenodo_env": "production",
        "ignore_regex": r"(^.*\\\\.parquet$|^.*pudl_parquet_datapackage\\\\.json$)",
        "publish": False,
    },
}


def _get_deployment_path_suffixes(
    deploy_type: DeploymentType, git_tag: str, staging: bool
) -> list[str]:
    if deploy_type == DeploymentType.NIGHTLY:
        path_suffixes = ["nightly", "eel-hole"]
    else:
        path_suffixes = [git_tag, "stable"]
    if staging:
        path_suffixes = [f"staging/{s}" for s in path_suffixes]
    return path_suffixes


def _get_zenodo_release_source_dir(deploy_type: DeploymentType, git_tag: str) -> str:
    if deploy_type == DeploymentType.NIGHTLY:
        source_dir = "s3://pudl.catalyst.coop/nightly/"
    else:
        source_dir = f"s3://pudl.catalyst.coop/{git_tag}/"
    return source_dir


def _deploy_outputs(
    source_dir: Path,
    deploy_type: DeploymentType,
    git_tag: str,
    staging: bool,
    github_token: str,
):
    """Execute stable or nightly deployment workflow.

    Upload outputs to paths associated with build type, trigger zenodo release,
    and update git branch. If ``deploy_type`` is stable, also sets GCS temporary
    hold on versioned release.
    """
    path_suffixes = _get_deployment_path_suffixes(
        deploy_type=deploy_type,
        git_tag=git_tag,
        staging=staging,
    )

    upload_outputs(
        source_dir=source_dir,
        path_suffixes=path_suffixes,
    )
    update_git_branch(tag=git_tag, branch=deploy_type.value, staging=staging)

    if not staging:
        if deploy_type == DeploymentType.STABLE:
            gcs_path = f"gs://pudl.catalyst.coop/{git_tag}/"
            set_gcs_temporary_hold(gcs_path=gcs_path)

        trigger_zenodo_release(
            build_ref=git_tag,
            env=DEPLOYMENT_TYPE_STATIC_SETTINGS[deploy_type]["zenodo_env"],
            source_dir=_get_zenodo_release_source_dir(deploy_type, git_tag),
            ignore_regex=DEPLOYMENT_TYPE_STATIC_SETTINGS[deploy_type]["ignore_regex"],
            publish=DEPLOYMENT_TYPE_STATIC_SETTINGS[deploy_type]["publish"],
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
    deploy_type = get_deployment_type_from_tag(git_tag)

    # Find build associated with tag
    build_path = get_build_from_tag(git_tag)
    # Create local directory to prep clean ETL outputs
    local_copy_path = Path(tempfile.mkdtemp())

    logger.info(
        f"Starting deployment for tag: {git_tag}\n"
        f"Build path: {build_path}\n"
        f"Deployment type: {deploy_type}\n"
    )

    prepare_outputs_for_distribution(local_copy_path, build_path)

    _deploy_outputs(
        source_dir=local_copy_path,
        deploy_type=deploy_type,
        git_tag=git_tag,
        staging=staging,
        github_token=github_token,
    )

    logger.info("Deployment completed successfully")


if __name__ == "__main__":
    pudl_deploy()
