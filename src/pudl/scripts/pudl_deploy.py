"""Deploy PUDL ETL outputs to cloud storage and update git branches.

This CLI orchestrates deployment of completed PUDL ETL builds to public cloud
storage (GCS and S3), git branch updates, Zenodo releases, and Cloud Run deployments.

The script takes a git tag, and an environment option to switch between 'staging' and
'production' deployments. It will use the git tag to identify builds associated with
the tag, and determine whether the deployment is intended to be a nightly, stable, or
branch deployment. It expects nightly deployments to have tags conforming to the pattern
'nightly-YYYY-MM-DD', 'branch-YYYY-MM-DD-HHMM-{GIT_HASH}-{MY_BRANCH_NAME}', or
'vYYYY.M.D'. If doing a staging deployment, outputs will be deployed to the same
distribution paths as a production deployment, but with a 'staging' prefix added to
the path (i.e. 's3://pudl.catalyst.coop/staging/nightly').

Examples:
    Deploy nightly build to production:
        pudl_deploy nightly-2025-02-05

    Deploy stable release to production:
        pudl_deploy v2025.2.3

    Test deployment changes with staging mode:
        pudl_deploy nightly-2025-02-05 --environment staging

    Deploy branch build outputs to staging area for review:
        pudl_deploy branch-2025-02-05-0600-abc123456-my-branch --environment staging

Staging mode uploads to staging/ prefixed paths and skips git operations, Zenodo
triggers, and Cloud Run deployments. This allows safe validation of deployment
changes before production use.
"""

import os
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Literal

import click

from pudl.deploy.pudl import (
    DeploymentPlan,
    DeployStage,
    StageResult,
    StageStatus,
    build_deploy_zulip_message,
    download_build_outputs,
    get_build_from_tag,
    new_deploy_stage_results,
    prepare_outputs_for_distribution,
    run_stage,
    send_zulip_message,
    set_gcs_temporary_hold,
    trigger_zenodo_release,
    update_git_branch,
    update_pudl_viewer,
    upload_outputs,
)
from pudl.logging_helpers import configure_root_logger, get_logger

logger = get_logger(__name__)


def _deploy_outputs(
    source_dir: Path,
    plan: DeploymentPlan,
    github_token: str,
    stage_results: dict[DeployStage, StageResult],
):
    """Execute stable or nightly deployment workflow.

    Every decision about what to do -- which paths to upload to, whether to
    redeploy Eel Hole, update git branches, trigger a Zenodo release, or set a GCS
    temporary hold -- comes from ``plan`` rather than being re-derived here.

    ``stage_results`` records the status and duration of each stage for Zulip
    reporting; "Upload outputs" is a hard prerequisite for the rest (a failure
    there raises and aborts the remaining stages, leaving them recorded as
    skipped), while the remaining stages are independent of one another.
    """
    run_stage(
        stage_fn=upload_outputs,
        stage_name=DeployStage.UPLOAD_OUTPUTS,
        stage_results=stage_results,
        source_dir=source_dir,
        path_suffixes=plan.path_suffixes,
        immutable_suffixes=plan.immutable_suffixes,
    )

    if plan.redeploy_eel_hole:
        run_stage(
            stage_fn=update_pudl_viewer,
            stage_name=DeployStage.REDEPLOY_EEL_HOLE,
            stage_results=stage_results,
            fail_hard=False,
            token=github_token,
            environment=plan.environment,
        )

    if plan.update_git_branch:
        run_stage(
            stage_fn=update_git_branch,
            stage_name=DeployStage.UPDATE_GIT_BRANCH,
            stage_results=stage_results,
            fail_hard=False,
            tag=plan.git_tag,
            branch=plan.deploy_type.value,
            environment=plan.environment,
            github_token=github_token,
        )

    if plan.trigger_zenodo_release:
        run_stage(
            stage_fn=trigger_zenodo_release,
            stage_name=DeployStage.TRIGGER_ZENODO_RELEASE,
            stage_results=stage_results,
            fail_hard=False,
            build_ref=plan.git_tag,
            deploy_type=plan.deploy_type,
            source_suffix=plan.zenodo_source_suffix,
            token=github_token,
        )

    if plan.gcs_temporary_hold:
        gcs_path = f"gs://pudl.catalyst.coop/{plan.git_tag}/"
        run_stage(
            stage_fn=set_gcs_temporary_hold,
            stage_name=DeployStage.GCS_TEMPORARY_HOLD,
            stage_results=stage_results,
            fail_hard=False,
            gcs_path=gcs_path,
        )


@click.command(
    help=__doc__,
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.argument(
    "git-tag",
    type=str,
)
@click.option(
    "--environment",
    type=click.Choice(["staging", "production"]),
    default="staging",
    help=(
        "Switch between a staging and production deployment. If staging this will"
        " skip the zenodo cloud run deployments."
    ),
    show_default=True,
)
@click.pass_context
def main(
    ctx: click.Context,
    git_tag: str,
    environment: Literal["staging", "production"],
) -> None:
    """Deploy PUDL ETL outputs to cloud storage and external services.

    Orchestrates the full deployment workflow:

    0. Download build outputs from the builds bucket
    1. Prepare outputs (compress SQLite, create parquet archive)
    2. Upload to cloud storage (GCS and S3)
    3. Redeploy the PUDL Viewer (nightly only)
    4. Update git branches (skipped for branch builds)
    5. Trigger Zenodo release (skipped for branch builds)
    6. Set GCS temporary hold for versioned releases (stable + production only)

    Saves a log of the deployment and a Zulip stage-status notification, mirroring
    the nightly build's own reporting.
    """
    # Resolve and validate the full deployment plan up front -- e.g. this raises if
    # git_tag doesn't look like a nightly/stable/branch tag, or if a branch tag is
    # being deployed to production.
    plan = DeploymentPlan(git_tag=git_tag, environment=environment)

    # Find build associated with tag
    build_path = get_build_from_tag(git_tag)
    build_id = build_path.name
    # Create local directory to prep clean ETL outputs
    local_copy_path = Path(tempfile.mkdtemp())

    deploy_start_time = datetime.now()
    local_logfile = (
        Path(tempfile.mkdtemp())
        / f"{build_id}-deploy-{deploy_start_time:%Y-%m-%d-%H%M}.log"
    )
    configure_root_logger(logfile=str(local_logfile))

    logger.info(
        f"Starting deployment for tag: {git_tag}\n"
        f"Build path: {build_path}\n"
        f"Deployment type: {plan.deploy_type}\n"
    )

    stage_results = new_deploy_stage_results()
    total_start = time.monotonic()
    try:
        run_stage(
            stage_fn=download_build_outputs,
            stage_name=DeployStage.DOWNLOAD_BUILD_OUTPUTS,
            stage_results=stage_results,
            local_path=local_copy_path,
            build_path=build_path,
        )

        run_stage(
            stage_fn=prepare_outputs_for_distribution,
            stage_name=DeployStage.PREPARE_OUTPUTS,
            stage_results=stage_results,
            local_path=local_copy_path,
            build_path=build_path,
        )

        _deploy_outputs(
            source_dir=local_copy_path,
            plan=plan,
            github_token=os.environ["GITHUB_TOKEN"],
            stage_results=stage_results,
        )
    finally:
        total_duration = time.monotonic() - total_start
        zulip_api_key = os.environ.get("ZULIP_API_KEY")
        if zulip_api_key:
            message = build_deploy_zulip_message(
                build_id=build_id,
                git_tag=git_tag,
                stage_results=stage_results,
                total_duration_seconds=total_duration,
                deploy_logfile_name=local_logfile.name,
                batch_job_name=os.environ.get("BATCH_JOB_NAME"),
            )
            send_zulip_message(message, api_key=zulip_api_key)
        else:
            logger.warning("Skipping Zulip notification: ZULIP_API_KEY is unset.")

        if local_logfile.exists():
            build_path.fs.put_file(
                str(local_logfile), f"{build_path}/{local_logfile.name}"
            )

    if any(result.status == StageStatus.FAILURE for result in stage_results.values()):
        ctx.exit(1)

    logger.info("Deployment completed successfully")


if __name__ == "__main__":
    main()
