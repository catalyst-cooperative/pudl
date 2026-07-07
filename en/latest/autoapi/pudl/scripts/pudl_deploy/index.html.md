# pudl.scripts.pudl_deploy

Deploy PUDL ETL outputs to cloud storage and update git branches.

This CLI orchestrates deployment of completed PUDL ETL builds to public cloud
storage (GCS and S3), git branch updates, Zenodo releases, and Cloud Run deployments.

The script takes a git tag, and an environment option to switch between ‘staging’ and
‘production’ deployments. It will use the git tag to identify builds associated with
the tag, and determine whether the deployment is intended to be a nightly, stable, or
branch deployment. It expects nightly deployments to have tags conforming to the pattern
‘nightly-YYYY-MM-DD’, ‘branch-YYYY-MM-DD-HHMM-{GIT_HASH}-{MY_BRANCH_NAME}’, or
‘vYYYY.M.D’. If doing a staging deployment, outputs will be deployed to the same
distribution paths as a production deployment, but with a ‘staging’ prefix added to
the path (i.e. ‘s3://pudl.catalyst.coop/staging/nightly’).

### Examples

Deploy nightly build to production:
: pudl_deploy nightly-2025-02-05

Deploy stable release to production:
: pudl_deploy v2025.2.3

Test deployment changes with staging mode:
: pudl_deploy nightly-2025-02-05 –environment staging

Deploy branch build outputs to staging area for review:
: pudl_deploy branch-2025-02-05-0600-abc123456-my-branch –environment staging

Staging mode uploads to staging/ prefixed paths and skips git operations, Zenodo
triggers, and Cloud Run deployments. This allows safe validation of deployment
changes before production use.

## Attributes

| [`logger`](#pudl.scripts.pudl_deploy.logger)   |    |
|------------------------------------------------|----|

## Functions

| [`_deploy_outputs`](#pudl.scripts.pudl_deploy._deploy_outputs)(source_dir, plan, github_token, ...)   | Execute stable or nightly deployment workflow.                  |
|-------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------|
| [`main`](#pudl.scripts.pudl_deploy.main)(→ None)                                                      | Deploy PUDL ETL outputs to cloud storage and external services. |

## Module Contents

### pudl.scripts.pudl_deploy.logger

### pudl.scripts.pudl_deploy.\_deploy_outputs(source_dir: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path), plan: [pudl.deploy.pudl.DeploymentPlan](../../deploy/pudl/index.md#pudl.deploy.pudl.DeploymentPlan), github_token: [str](https://docs.python.org/3/library/stdtypes.html#str), stage_results: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[pudl.deploy.pudl.DeployStage](../../deploy/pudl/index.md#pudl.deploy.pudl.DeployStage), [pudl.deploy.pudl.StageResult](../../deploy/pudl/index.md#pudl.deploy.pudl.StageResult)])

Execute stable or nightly deployment workflow.

Every decision about what to do – which paths to upload to, whether to
redeploy Eel Hole, update git branches, trigger a Zenodo release, or set a GCS
temporary hold – comes from `plan` rather than being re-derived here.

`stage_results` records the status and duration of each stage for Zulip
reporting; “Upload outputs” is a hard prerequisite for the rest (a failure
there raises and aborts the remaining stages, leaving them recorded as
skipped), while the remaining stages are independent of one another.

### pudl.scripts.pudl_deploy.main(ctx: click.Context, git_tag: [str](https://docs.python.org/3/library/stdtypes.html#str), environment: Literal['staging', 'production']) → [None](https://docs.python.org/3/library/constants.html#None)

Deploy PUDL ETL outputs to cloud storage and external services.

Orchestrates the full deployment workflow:

1. Resolve the deployment plan and find the build associated with git_tag
2. Download build outputs from the builds bucket
3. Prepare outputs (compress SQLite, create parquet archive)
4. Upload to cloud storage (GCS and S3)
5. Redeploy the PUDL Viewer (nightly only)
6. Update git branches (skipped for branch builds)
7. Trigger Zenodo release (skipped for branch builds)
8. Set GCS temporary hold for versioned releases (stable + production only)

Saves a log of the deployment and a Zulip stage-status notification, mirroring
the nightly build’s own reporting – including if step 0 itself fails, e.g. due
to an invalid tag or a missing/failed build.
