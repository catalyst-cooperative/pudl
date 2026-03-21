# PUDL GitHub Data Deployments Plan (Draft)

## Purpose

This document proposes a deployment-tracking design for PUDL's nightly and stable
data releases using GitHub Deployments.

It is intended to:

- Make the latest nightly and stable data deployment states visible in the GitHub
  Deployments sidebar.
- Keep deployment status aligned with the real end-to-end outcome of the data build
  and publication process, not merely the initial workflow dispatch.
- Provide a public, machine-readable and human-readable summary of the same stage
  information that is currently assembled for Slack notifications.
- Establish a concrete implementation plan that can be resumed after the current
  PR is merged.

## Problem Statement

PUDL's data release flow currently spans multiple execution contexts:

- The scheduled or tag-triggered GitHub Actions workflow in
  `.github/workflows/build-deploy-pudl.yml`.
- The long-running Google Batch execution driven by `docker/gcp_pudl_etl.sh`.
- The follow-on Zenodo publication workflow in
  `.github/workflows/zenodo-data-release.yml`.

Today, GitHub has workflow run statuses and Slack notifications, but it does not have
an accurate notion of the latest successful or failed nightly versus stable data
deployment in the Deployments sidebar.

The key issue is that the workflow run that submits the Batch job is not the same thing
as the deployment itself. The actual deployment result is only known later, after:

- Dagster ETL succeeds or fails.
- Unit, integration, and data-validation tests succeed or fail.
- Outputs are copied to their public destinations.
- Distribution-specific post-processing finishes.

## Goals

- Track nightly and stable data releases as first-class GitHub Deployments.
- Show the latest nightly deployment and latest stable deployment independently.
- Use a single authoritative deployment record per release stream.
- Update deployment status incrementally as the build proceeds.
- Publish a public deployment summary with per-stage outcomes and durations.
- Reuse as much existing status bookkeeping in `gcp_pudl_etl.sh` as possible.
- Keep Zenodo publication non-blocking for the overall deployment state.

## Non-Goals

- Rework the semantics of the current ETL/test/publication steps.
- Replace Slack notifications.
- Track each publication channel as a separate GitHub Deployment in the first
  implementation.
- Block nightly or stable deployment success on Zenodo outcomes.
- Solve Zenodo sandbox flakiness as part of this change.

## Constraints and Assumptions

- GitHub Deployments are environment-oriented and best suited to one rolling
  deployment history per environment.
- The build is kicked off from GitHub Actions but executed to completion in Google
  Batch.
- The ETL shell script already maintains stage-level status and duration variables.
- Zenodo sandbox frequently returns spurious failures, including 404s after a
  successful upload.
- Stable Zenodo data releases are intentionally not auto-published; they require
  manual review.

## Recommended Deployment Model

Use exactly two authoritative GitHub deployment environments:

- `nightly-data`
- `stable-data`

Each environment should correspond to one logical release stream:

- `nightly-data` represents the latest scheduled nightly data release.
- `stable-data` represents the latest stable or versioned data release.

Each run creates or updates one deployment object in the appropriate environment.

### Why only two environments?

This is the cleanest mapping to the GitHub Deployments sidebar.

- It answers the operator question: "what is the latest nightly deployment status?"
- It answers the release question: "what is the latest stable deployment status?"
- It avoids cluttering the sidebar with multiple partially overlapping publication
  channels.

### Why not make each publication channel a deployment?

Channels such as S3, GCS, Eel Hole, and Zenodo are operational details of a single
logical data release. Modeling each as its own deployment would:

- Multiply deployment records.
- Make the sidebar less useful as a high-level signal.
- Force operators to mentally reassemble a single release from several environment
  statuses.

If channel-specific deployment records are needed later, they can be introduced as
auxiliary environments, but they should not replace the two primary release-stream
deployments.

## Authoritative Success Semantics

### `nightly-data`

`nightly-data` should be marked `success` only when all required core deployment steps
have succeeded, including:

- Dagster ETL
- Unit tests
- Integration tests
- Data validation tests
- Datapackage generation
- Required output publication steps

It should not be blocked on Zenodo.

### `stable-data`

`stable-data` should be marked `success` only when all required stable release steps
have succeeded, including stable publication to required data distribution targets.

It should not be blocked on Zenodo publication or approval.

## Zenodo Handling

Zenodo should be treated as a non-blocking auxiliary publication channel.

### Nightly Zenodo

- The nightly Zenodo workflow may run automatically.
- Its outcome should be recorded in the public deployment summary.
- Its outcome should not flip `nightly-data` to `failure`.

This is necessary because the Zenodo sandbox is currently too flaky to be used as a
source of truth for deployment success.

### Stable Zenodo

- The stable Zenodo workflow may create an unpublished deposition or prepare a release.
- Manual human approval/publication remains outside the core deployment success path.
- The public summary should show stable Zenodo as something like
  `manual_review_pending` or `prepared_not_published`.

### Future option

If Zenodo eventually becomes reliable and operationally important enough to track
separately in GitHub, add non-authoritative auxiliary deployment environments such as:

- `nightly-zenodo`
- `stable-zenodo`

This is explicitly deferred.

## Public Deployment Summary

GitHub deployment statuses are too small and too coarse to hold a complete
stage-by-stage release report. They should carry only:

- the current overall state
- a short description
- a log URL
- an environment URL

The rich details should instead be written to a public summary artifact.

### Summary outputs

Generate at least one machine-readable summary file per deployment:

- `deployment-summary.json`

Optionally also generate:

- `deployment-summary.html`

### Summary contents

The summary should include:

- build ID
- build type (`nightly`, `stable`, `workflow_dispatch` if applicable)
- GitHub ref and SHA
- deployment environment name
- start time and last update time
- overall deployment state
- per-stage status and duration
- key URLs
- non-blocking channel outcomes such as Zenodo

### Suggested JSON shape

```json
{
  "build_id": "2026-03-21-1234-abcdef-nightly",
  "build_type": "nightly",
  "deployment_environment": "nightly-data",
  "ref": "nightly-2026-03-21",
  "sha": "abcdef123456",
  "status": "in_progress",
  "started_at": "2026-03-21T12:34:00Z",
  "updated_at": "2026-03-21T13:02:00Z",
  "run_url": "https://github.com/catalyst-cooperative/pudl/actions/runs/23386617997",
  "log_url": "https://builds.catalyst.coop/.../deployment-summary.html",
  "environment_url": "https://pudl.catalyst.coop/nightly/",
  "stages": [
    {"name": "Dagster ETL", "status": "success", "duration": "00:41:12", "required": true},
    {"name": "Unit tests", "status": "success", "duration": "00:03:11", "required": true},
    {"name": "Integration tests", "status": "success", "duration": "00:08:25", "required": true},
    {"name": "Data validation tests", "status": "success", "duration": "00:12:09", "required": true}
  ],
  "channels": {
    "gcs": {"status": "success", "url": "gs://pudl.catalyst.coop/nightly/"},
    "s3": {"status": "success", "url": "s3://pudl.catalyst.coop/nightly/"},
    "eel_hole": {"status": "success"},
    "zenodo": {
      "status": "non_blocking_failure",
      "note": "Zenodo sandbox returned a spurious failure after upload"
    }
  }
}
```

### Public URLs

The summary should be published in a stable public location for the current release
stream and optionally preserved in a per-build location.

Recommended targets:

- Nightly rolling summary:
  - `https://pudl.catalyst.coop/nightly/deployment-summary.json`
  - `https://pudl.catalyst.coop/nightly/deployment-summary.html`
- Stable per-version summary:
  - `https://pudl.catalyst.coop/<version>/deployment-summary.json`
  - `https://pudl.catalyst.coop/<version>/deployment-summary.html`

The deployment status `log_url` should point to the public summary page for the build.
The deployment `environment_url` should point to the canonical public data URL.

## End-to-End Control Flow

### 1. Build workflow creates deployment

`.github/workflows/build-deploy-pudl.yml` should:

- determine whether the run is a nightly or stable deployment
- choose `nightly-data` or `stable-data`
- create a GitHub Deployment via the Deployments API
- immediately post `queued` or `in_progress`
- pass the deployment ID and associated metadata into the Batch job

### 2. Batch script updates deployment while work proceeds

`docker/gcp_pudl_etl.sh` should:

- post `in_progress` updates during major phases
- write and refresh the public deployment summary after each stage
- post `failure` immediately when a required stage fails
- post `success` when required core deployment work completes

### 3. Zenodo workflow updates auxiliary summary data only

`.github/workflows/zenodo-data-release.yml` should:

- receive enough context to locate the summary for the release
- update Zenodo-related fields in the public deployment summary if practical
- keep Slack notifications if desired
- not post a blocking failure to the main deployment record

## GitHub Deployment API Usage

### Deployment creation

Create deployments with:

- `ref`: the relevant tag, branch, or SHA
- `environment`: `nightly-data` or `stable-data`
- `required_contexts: []`
- `auto_merge: false`
- `payload`: build metadata

### Deployment statuses

Use the deployment status API to post:

- `queued`
- `in_progress`
- `failure`
- `success`

Each status update should include:

- `description`: concise summary of current phase
- `log_url`: Actions run URL or public summary URL
- `environment_url`: canonical public data URL when available

### `auto_inactive`

Keep default inactivation behavior for successful deployments in the same environment.

This ensures:

- the latest successful `nightly-data` supersedes prior nightly deployments
- the latest successful `stable-data` supersedes prior stable deployments

Because nightly and stable use different environment names, they will not interfere with
each other.

## Required Workflow and Script Changes

## A. `.github/workflows/build-deploy-pudl.yml`

### Workflow responsibilities

- Determine the deployment environment.
- Create the deployment record.
- Persist the deployment ID for downstream use.
- Pass deployment metadata into the Batch container.

### New environment variables to derive

- `DEPLOYMENT_ENV`
- `DEPLOYMENT_ID`
- `DEPLOYMENT_LOG_URL`
- `DEPLOYMENT_ENVIRONMENT_URL` (optional initial placeholder)

### Suggested payload metadata

- `build_id`
- `build_type`
- `github_run_id`
- `github_run_url`
- `ref_name`
- `sha`

### Token requirements

The workflow or Batch job needs a token with `Deployments: write` permission.

Prefer one of:

- GitHub App installation token
- fine-grained PAT with deployments write access

The existing bot token may be sufficient if its permissions are appropriate.

## B. `docker/gcp_pudl_etl.sh`

### Shell script responsibilities

- Post incremental deployment statuses.
- Generate and refresh the public deployment summary.
- Keep Slack notifications and summary generation in sync.

### New helper functions

- `post_github_deployment_status`
- `write_deployment_summary_json`
- optionally `write_deployment_summary_html`
- optionally `build_stage_summary_json_fragment`

### Existing state to reuse

The script already tracks:

- `DAGSTER_STATUS`
- `UNIT_TEST_STATUS`
- `INTEGRATION_TEST_STATUS`
- `DATA_VALIDATION_STATUS`
- publication-stage statuses
- per-stage durations

That state should become the source of truth for both Slack and the public summary.

### Update points

Write summary and optionally post deployment status:

- after deployment starts
- after each `run_stage`
- in `exit_on_stage_failure`
- after required publication succeeds
- after non-blocking post-release steps if desired

### Success criteria in script

Only required stages should drive the main deployment status.

Zenodo-related outcomes should be recorded but should not change:

- `nightly-data` from `success` to `failure`
- `stable-data` from `success` to `failure`

## C. `.github/workflows/zenodo-data-release.yml`

### Zenodo workflow responsibilities

- Accept enough context to correlate the Zenodo run with the deployment summary.
- Publish non-blocking Zenodo outcome details.

### Suggested new inputs

- `deployment_env`
- `build_id`
- `summary_location`
- optionally `deployment_id` for future auxiliary tracking

### Current recommendation

Do not update the authoritative deployment status from this workflow.

Instead:

- update only the public summary if practical, or
- post Slack status and leave the summary untouched until later enhancement

## Data Model for Stage States

Use a normalized status vocabulary in the public summary:

- `queued`
- `in_progress`
- `success`
- `failure`
- `skipped`
- `non_blocking_failure`
- `manual_review_pending`

This should remain richer than the GitHub deployment state model, which is intentionally
coarser.

## Public Summary Rendering Options

### Option 1: JSON only

Pros:

- simplest to implement
- easy to update from shell
- easy for future tooling or dashboards to consume

Cons:

- less pleasant for humans to read directly

### Option 2: JSON plus small static HTML page

Pros:

- easy public inspection
- closer to the Slack summary format people already know

Cons:

- slightly more implementation effort

Recommendation:

- implement JSON first
- add HTML once the data model stabilizes

## Failure Handling

### Required-stage failures

Required-stage failures should:

- post `failure` to the GitHub deployment
- update the public summary immediately
- continue to preserve logs and summary artifacts if possible

### Non-blocking failures

Non-blocking failures such as flaky nightly Zenodo should:

- be recorded in the public summary
- be included in Slack notifications
- not post a final `failure` to the main deployment

## Suggested Rollout Plan

## Phase 0: Planning and review

- Review and refine this proposal.
- Confirm environment naming.
- Confirm the public summary publication target.
- Confirm available GitHub token strategy.

## Phase 1: Core deployment tracking

- Add deployment creation to `build-deploy-pudl.yml`.
- Add deployment status posting to `gcp_pudl_etl.sh`.
- Mark nightly and stable deployments accurately in GitHub.

### Phase 1 exit criteria

- `nightly-data` and `stable-data` appear in the Deployments sidebar.
- Their latest statuses reflect the true end-to-end deployment result.

## Phase 2: Public deployment summary

- Add `deployment-summary.json` generation.
- Publish it alongside build outputs.
- Point deployment `log_url` at the public summary location.

### Phase 2 exit criteria

- Each deployment has a public summary containing stage outcomes and durations.

## Phase 3: Improve public presentation

- Add HTML rendering for the summary.
- Optionally include links to logs, build buckets, and public outputs.

## Phase 4: Optional Zenodo-specific tracking

- Revisit whether Zenodo needs auxiliary deployments.
- Revisit whether stable Zenodo manual approval should have a dedicated UI surface.

## Open Questions

- Where should the canonical public deployment summary live for nightly runs?
- Should stable deployments point `environment_url` to the versioned public dataset,
  the stable alias, or both?
- Should `workflow_dispatch` runs create deployment records at all, or remain outside
  the tracked deployment history?
- Should the summary JSON be uploaded only after the first successful publication step,
  or as early as the build-bucket phase so it is visible throughout the run?
- Is the existing bot token appropriate for deployments write access, or should this be
  moved to a GitHub App?

## Recommended Initial Decision Set

To keep implementation focused, start with these assumptions:

- Track only `nightly-data` and `stable-data` as GitHub Deployments.
- Treat Zenodo as non-blocking and summary-only.
- Implement `deployment-summary.json` before any HTML summary page.
- Use the public summary URL as the deployment `log_url` once available.
- Use the canonical public data URL as the deployment `environment_url`.
- Reuse existing shell stage status bookkeeping instead of inventing a second status
  model.

## Implementation Sketch Summary

At a high level:

1. The GitHub Actions workflow creates a deployment record for either
   `nightly-data` or `stable-data`.
2. The workflow passes deployment metadata into the Batch job.
3. The Batch shell script updates the deployment state as required stages proceed.
4. The Batch shell script writes a public deployment summary showing per-stage status
   and duration information.
5. Zenodo contributes non-blocking summary details but does not determine overall
   deployment success.

That approach gives PUDL:

- accurate latest nightly and stable deployment states in GitHub
- continuity with the existing Slack stage summary model
- a public, inspectable deployment summary for users and operators
- a clear path to iterative enhancement later
