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
import time
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
    new_deploy_stage_results,
    prepare_outputs_for_distribution,
    resolve_build,
    run_stage,
    send_zulip_message,
    set_gcs_temporary_hold,
    trigger_zenodo_release,
    update_git_branch,
    update_pudl_viewer,
    upload_outputs,
)
from pudl.logging_helpers import get_logger

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

    0. Resolve the deployment plan and find the build associated with git_tag
    1. Download build outputs from the builds bucket
    2. Prepare outputs (compress SQLite, create parquet archive)
    3. Upload to cloud storage (GCS and S3)
    4. Redeploy the PUDL Viewer (nightly only)
    5. Update git branches (skipped for branch builds)
    6. Trigger Zenodo release (skipped for branch builds)
    7. Set GCS temporary hold for versioned releases (stable + production only)

    Saves a log of the deployment and a Zulip stage-status notification, mirroring
    the nightly build's own reporting -- including if step 0 itself fails, e.g. due
    to an invalid tag or a missing/failed build.
    """
    stage_results = new_deploy_stage_results()
    total_start = time.monotonic()
    zulip_api_key = os.environ.get("ZULIP_API_KEY")
    # Best-effort identifiers for the Zulip notification/log-upload in `finally`,
    # in case we fail to resolve a real build below -- e.g. an invalid tag, or (far
    # more likely in practice) a manually-dispatched tag with no matching
    # successful build yet. Without this fallback, a resolve failure would crash
    # before any of these are ever assigned.
    build_id = git_tag
    build_path = None
    local_logfile = None

    try:
        # Resolving the plan and build runs before any other tracked deploy stage,
        # so running it through run_stage (rather than as plain, untracked code)
        # ensures a failure here is recorded as an actual failure -- without that,
        # every stage would be left at its default "skipped" status, which
        # build_deploy_zulip_message reads as an overall success.
        resolved = run_stage(
            stage_fn=resolve_build,
            stage_name=DeployStage.RESOLVE_BUILD,
            stage_results=stage_results,
            git_tag=git_tag,
            environment=environment,
        )
        # run_stage's default fail_hard=True re-raises on failure instead of
        # returning, so reaching this line means resolve_build succeeded.
        assert resolved is not None  # noqa: S101
        plan = resolved.plan
        build_path = resolved.build_path
        build_id = resolved.build_id
        local_copy_path = resolved.local_copy_path
        local_logfile = resolved.local_logfile

        logger.info(
            f"Starting deployment for tag: {git_tag}\n"
            f"Build path: {build_path}\n"
            f"Deployment type: {plan.deploy_type}\n"
        )

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
        if zulip_api_key:
            message = build_deploy_zulip_message(
                build_id=build_id,
                git_tag=git_tag,
                stage_results=stage_results,
                total_duration_seconds=total_duration,
                deploy_logfile_name=(
                    local_logfile.name if local_logfile is not None else "n/a"
                ),
                batch_job_name=os.environ.get("BATCH_JOB_NAME"),
            )
            send_zulip_message(message, api_key=zulip_api_key)
        else:
            logger.warning("Skipping Zulip notification: ZULIP_API_KEY is unset.")

        if (
            build_path is not None
            and local_logfile is not None
            and local_logfile.exists()
        ):
            build_path.fs.put_file(
                str(local_logfile), f"{build_path}/{local_logfile.name}"
            )

    if any(result.status == StageStatus.FAILURE for result in stage_results.values()):
        ctx.exit(1)

    logger.info("Deployment completed successfully")


if __name__ == "__main__":
    main()
