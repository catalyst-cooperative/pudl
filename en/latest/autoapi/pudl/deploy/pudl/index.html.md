# pudl.deploy.pudl

Distribute PUDL ETL outputs to cloud storage and update git branches.

This module handles distribution of completed ETL builds to public cloud storage
(GCS and S3), git branch updates, Zenodo releases, and Cloud Run deployments.

## Attributes

| [`logger`](#pudl.deploy.pudl.logger)                   |    |
|--------------------------------------------------------|----|
| [`ZULIP_API_URL`](#pudl.deploy.pudl.ZULIP_API_URL)     |    |
| [`ZULIP_BOT_EMAIL`](#pudl.deploy.pudl.ZULIP_BOT_EMAIL) |    |
| [`ZULIP_STREAM`](#pudl.deploy.pudl.ZULIP_STREAM)       |    |
| [`ZULIP_TOPIC`](#pudl.deploy.pudl.ZULIP_TOPIC)         |    |

## Classes

| [`DeploymentType`](#pudl.deploy.pudl.DeploymentType)   | Deployments can be 'nightly', 'branch', or 'stable'.                           |
|--------------------------------------------------------|--------------------------------------------------------------------------------|
| [`DeploymentPlan`](#pudl.deploy.pudl.DeploymentPlan)   | Fully resolved, validated deployment behavior for one git tag and environment. |
| [`ResolvedBuild`](#pudl.deploy.pudl.ResolvedBuild)     | Everything `pudl_deploy`'s `main()` needs after resolving a deployment.        |
| [`StageStatus`](#pudl.deploy.pudl.StageStatus)         | Possible outcomes of a single deployment stage.                                |
| [`DeployStage`](#pudl.deploy.pudl.DeployStage)         | The fixed set of tracked deployment stages.                                    |
| [`StageResult`](#pudl.deploy.pudl.StageResult)         | Outcome of a single deployment stage, for Zulip stage-table reporting.         |

## Functions

| [`_zip_parquet_files`](#pudl.deploy.pudl._zip_parquet_files)(→ None)                                       | Create a zipfile containing parquet files and an associated datapackage JSON file.    |
|------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------|
| [`_compress_sqlite_file`](#pudl.deploy.pudl._compress_sqlite_file)(→ None)                                 | Compress a SQLite database into a zip file and remove the original.                   |
| [`download_build_outputs`](#pudl.deploy.pudl.download_build_outputs)(→ None)                               | Download raw ETL build outputs from builds.catalyst.coop to local disk.               |
| [`prepare_outputs_for_distribution`](#pudl.deploy.pudl.prepare_outputs_for_distribution)(→ None)           | Prepare already-downloaded ETL outputs for distribution.                              |
| [`_run`](#pudl.deploy.pudl._run)(→ str | None)                                                             | Wrap subprocess.run so we see error output.                                           |
| [`clear_deployment_path`](#pudl.deploy.pudl.clear_deployment_path)(→ None)                                 | Empty a cloud storage prefix before writing fresh deployment outputs.                 |
| [`_upload_to_path`](#pudl.deploy.pudl._upload_to_path)(→ None)                                             | Clear (if requested) and upload all outputs to one destination path.                  |
| [`_assert_permanent_paths_are_empty`](#pudl.deploy.pudl._assert_permanent_paths_are_empty)(→ None)         | Refuse to deploy to a permanent, version-tagged path that already has content.        |
| [`upload_outputs`](#pudl.deploy.pudl.upload_outputs)() → None)                                             | Upload outputs to cloud storage paths.                                                |
| [`update_git_branch`](#pudl.deploy.pudl.update_git_branch)(→ None)                                         | Merge git tag into branch and push to origin.                                         |
| [`dispatch_github_workflow`](#pudl.deploy.pudl.dispatch_github_workflow)(→ None)                           | Trigger a workflow_dispatch event on a GitHub Actions workflow.                       |
| [`trigger_zenodo_release`](#pudl.deploy.pudl.trigger_zenodo_release)(→ None)                               | Trigger Zenodo data release GitHub Actions workflow.                                  |
| [`update_pudl_viewer`](#pudl.deploy.pudl.update_pudl_viewer)(→ None)                                       | Update PUDL Viewer Cloud Run service to latest image.                                 |
| [`set_gcs_temporary_hold`](#pudl.deploy.pudl.set_gcs_temporary_hold)(→ None)                               | Set temporary hold on GCS objects to prevent deletion.                                |
| [`check_build_success`](#pudl.deploy.pudl.check_build_success)(→ upath.UPath)                              | Raise error if success file doesn't exist in build directory.                         |
| [`get_build_from_tag`](#pudl.deploy.pudl.get_build_from_tag)(→ upath.UPath)                                | Find any builds associated with a git tag and return a GCS path to most recent build. |
| [`get_deployment_type_from_tag`](#pudl.deploy.pudl.get_deployment_type_from_tag)(→ DeploymentType)         | Check if tag looks like a 'nightly', 'branch', or 'stable' tag.                       |
| [`resolve_build`](#pudl.deploy.pudl.resolve_build)(→ ResolvedBuild)                                        | Resolve the deployment plan, locate the build, and set up local logging.              |
| [`new_deploy_stage_results`](#pudl.deploy.pudl.new_deploy_stage_results)(→ dict[DeployStage, StageResult]) | Initialize every tracked deploy stage as skipped, in table display order.             |
| [`run_stage`](#pudl.deploy.pudl.run_stage)(→ T | None)                                                     | Run a deploy stage, recording its status and duration in `stage_results`.             |
| [`format_stage_duration`](#pudl.deploy.pudl.format_stage_duration)(→ str)                                  | Format a duration in seconds as `HH:MM:SS`.                                           |
| [`stage_emoji`](#pudl.deploy.pudl.stage_emoji)(→ str)                                                      | Return the Zulip emoji corresponding to a stage status.                               |
| [`build_deploy_logfile_links`](#pudl.deploy.pudl.build_deploy_logfile_links)(→ str)                        | Build markdown links for reviewing a deployment's logs and outputs.                   |
| [`build_deploy_zulip_message`](#pudl.deploy.pudl.build_deploy_zulip_message)(→ str)                        | Build a markdown Zulip message summarizing deployment stage statuses.                 |
| [`send_zulip_message`](#pudl.deploy.pudl.send_zulip_message)(→ None)                                       | Post a message to the pudl-deployments Zulip stream.                                  |

## Module Contents

### pudl.deploy.pudl.logger

### *class* pudl.deploy.pudl.DeploymentType(\*args, \*\*kwds)

Bases: [`enum.Enum`](https://docs.python.org/3/library/enum.html#enum.Enum)

Deployments can be ‘nightly’, ‘branch’, or ‘stable’.

#### NIGHTLY *= 'nightly'*

#### STABLE *= 'stable'*

#### BRANCH *= 'branch'*

### *class* pudl.deploy.pudl.DeploymentPlan(/, \*\*data: Any)

Bases: [`pydantic.BaseModel`](https://pydantic.dev/docs/validation/latest/api/pydantic/base_model/#pydantic.BaseModel)

Fully resolved, validated deployment behavior for one git tag and environment.

This is the single source of truth both for what a deployment actually does –
every other piece of code (path suffixes, which stages run) derives from a
`DeploymentPlan` instead of independently re-deriving the same rules – and for
which `git_tag`/`environment` combinations are valid in the first place.

`deploy_type` is derived from `git_tag` (see `get_deployment_type_from_tag`)
rather than accepted as a separate input, so a plan can never be constructed with
a `deploy_type` that doesn’t match its own `git_tag`.

This intentionally only validates what’s knowable from `git_tag` and
`environment` alone – e.g. it does NOT check that a nightly/stable tag is
actually reachable from `main`, since that requires a git checkout the deploy
container doesn’t have (that check lives in the GHA workflow instead).

#### model_config

Configuration for the model, should be a dictionary conforming to [ConfigDict][pydantic.config.ConfigDict].

#### git_tag *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### environment *: Literal['staging', 'production']*

#### *property* deploy_type *: [DeploymentType](#pudl.deploy.pudl.DeploymentType)*

The deploy type implied by `git_tag`’s shape.

#### \_validate_branch_only_targets_staging() → [DeploymentPlan](#pudl.deploy.pudl.DeploymentPlan)

#### *property* path_suffixes *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

Cloud storage path suffixes this deployment uploads to.

Nightly and branch builds share the same rolling “nightly”/”eel-hole”
paths; stable releases get their own permanent version-tagged path plus
“stable”.

#### *property* zenodo_source_suffix *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

The single path suffix Zenodo should pull outputs from.

#### *property* gcs_temporary_hold *: [bool](https://docs.python.org/3/library/functions.html#bool)*

Whether this deployment’s permanent path should get a GCS temporary hold.

Only a *production* stable release gets a hold, protecting its permanent
version-tagged path. A staging deploy of the same tag is just a disposable
test output and must remain clearable.

#### *property* immutable_suffixes *: [frozenset](https://docs.python.org/3/library/stdtypes.html#frozenset)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

Path suffixes that are permanent and must never be cleared before upload.

This is also the only path that’s protected by `gcs_temporary_hold`.

#### *property* redeploy_eel_hole *: [bool](https://docs.python.org/3/library/functions.html#bool)*

Whether this deployment should redeploy the PUDL Viewer (Eel Hole).

#### *property* update_git_branch *: [bool](https://docs.python.org/3/library/functions.html#bool)*

Whether this deployment should fast-forward a git branch to its tag.

#### *property* trigger_zenodo_release *: [bool](https://docs.python.org/3/library/functions.html#bool)*

Whether this deployment should trigger a Zenodo release.

### pudl.deploy.pudl.\_zip_parquet_files(parquet_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path), output_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)) → [None](https://docs.python.org/3/library/constants.html#None)

Create a zipfile containing parquet files and an associated datapackage JSON file.

`parquet_path` should contain a set of parquet files and exactly one datapackage
JSON file that describes those parquet files.

* **Parameters:**
  * **parquet_path** – Path to directory containing parquet files.
  * **output_path** – Path to zipfile that should be created by this function.

### pudl.deploy.pudl.\_compress_sqlite_file(sqlite_file: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)) → [None](https://docs.python.org/3/library/constants.html#None)

Compress a SQLite database into a zip file and remove the original.

Safe to call concurrently across different files – each call only touches
its own independent `ZipFile` and path.

### pudl.deploy.pudl.download_build_outputs(local_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path), build_path: upath.UPath) → [None](https://docs.python.org/3/library/constants.html#None)

Download raw ETL build outputs from builds.catalyst.coop to local disk.

Split out from `prepare_outputs_for_distribution` so the network-bound
download and the CPU-bound preparation work (zipping, compression) can be
timed and reported as separate deploy stages.

* **Parameters:**
  * **local_path** – Path on local filesystem to download outputs into.
  * **build_path** – Remote path containing raw build outputs.

### pudl.deploy.pudl.prepare_outputs_for_distribution(local_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path), build_path: upath.UPath) → [None](https://docs.python.org/3/library/constants.html#None)

Prepare already-downloaded ETL outputs for distribution.

Takes raw ETL output structure and produces distribution-ready outputs:
- Moves parquet files from parquet/ subdirectory to root
- Compresses SQLite databases with maximum compression
- Creates parquet archive (no compression, already compressed)
- Removes test databases and temporary directories

In general, we want to know if these files don’t exist, so
FileNotFoundErrors are OK and we don’t need to pre-emptively try to avoid
them.

* **Parameters:**
  * **local_path** – Path on local filesystem containing raw outputs downloaded by
    `download_build_outputs`, which this prepares for distribution in place.
  * **build_path** – Remote path the raw build outputs came from – only used here to
    derive the build ID for the provenance marker file.

### pudl.deploy.pudl.\_run(cmd: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]) → [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)

Wrap subprocess.run so we see error output.

### pudl.deploy.pudl.clear_deployment_path(fs, path: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [None](https://docs.python.org/3/library/constants.html#None)

Empty a cloud storage prefix before writing fresh deployment outputs.

Cloud storage (GCS, S3) uses virtual prefixes rather than real directories, so we
use `fs.rm(path, recursive=True)` instead of `rmdir()`, which would raise
`NotADirectoryError` – the same pattern used for FERC EQR staging cleanup in
`pudl.dagster.assets.deploy.ferceqr`.

### pudl.deploy.pudl.\_upload_to_path(fs, path: [str](https://docs.python.org/3/library/stdtypes.html#str), source_dir: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path), clear_first: [bool](https://docs.python.org/3/library/functions.html#bool)) → [None](https://docs.python.org/3/library/constants.html#None)

Clear (if requested) and upload all outputs to one destination path.

Safe to call concurrently for different `(fs, path)` combinations – gcsfs
and s3fs are both designed to support concurrent use from multiple threads,
and each call here only touches its own independent bucket/path.

### pudl.deploy.pudl.\_assert_permanent_paths_are_empty(gcs_fs: gcsfs.GCSFileSystem, s3_fs: s3fs.S3FileSystem, path_suffixes: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], immutable_suffixes: [frozenset](https://docs.python.org/3/library/stdtypes.html#frozenset)[[str](https://docs.python.org/3/library/stdtypes.html#str)]) → [None](https://docs.python.org/3/library/constants.html#None)

Refuse to deploy to a permanent, version-tagged path that already has content.

Rolling paths (nightly/stable/eel-hole) are cleared before every upload, but a
permanent path like `gs://pudl.catalyst.coop/v2026.7.0/` deliberately never is
– it’s meant to be written exactly once. If it already has content, deploying
again would silently mix old and new files instead of cleanly replacing them,
which almost always means the same version tag is being deployed a second time.
That’s an invalid request, so we check and raise up front rather than silently
uploading over the top of it.

### pudl.deploy.pudl.upload_outputs(source_dir: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path), path_suffixes: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], immutable_suffixes: [frozenset](https://docs.python.org/3/library/stdtypes.html#frozenset)[[str](https://docs.python.org/3/library/stdtypes.html#str)] = frozenset()) → [None](https://docs.python.org/3/library/constants.html#None)

Upload outputs to cloud storage paths.

Uploads all files from source directory to GCS and S3 using the provided path
suffixes. Each suffix is uploaded to both gs://pudl.catalyst.coop/{suffix}/ and
s3://pudl.catalyst.coop/{suffix}/. Any existing objects at a suffix are removed
first, unless that suffix is listed in `immutable_suffixes` – a permanent,
hold-protected versioned release path is never cleared, and instead must not
exist at all yet (see `_assert_permanent_paths_are_empty`).

Each (suffix, destination) pair is uploaded concurrently: GCS and S3 are separate
network destinations, and this is I/O-bound work that releases the GIL.

* **Parameters:**
  * **source_dir** – Local directory containing prepared outputs to upload.
  * **path_suffixes** – Path suffixes to upload to (e.g., [“nightly”, “eel-hole”]).
  * **immutable_suffixes** – Path suffixes that should never be cleared before upload
    (e.g. a permanent stable-version path like “v2026.7.0”). It’s an error
    for one of these paths to already exist.
* **Raises:**
  [**RuntimeError**](https://docs.python.org/3/library/exceptions.html#RuntimeError) – If a permanent, immutable path already has content.

### pudl.deploy.pudl.update_git_branch(tag: [str](https://docs.python.org/3/library/stdtypes.html#str), branch: [str](https://docs.python.org/3/library/stdtypes.html#str), environment: Literal['staging', 'production'], github_token: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [None](https://docs.python.org/3/library/constants.html#None)

Merge git tag into branch and push to origin.

Performs fast-forward merge of a tag into a branch and pushes the result.
This updates the nightly or stable branch to point to the tagged release.

If environment is ‘staging’, this will try the checkout and merge, but skip the
git push.

* **Parameters:**
  * **tag** – Git tag to merge (e.g., “nightly-2025-02-05” or “v2025.2.3”).
  * **branch** – Target branch to update (e.g., “nightly” or “stable”).
  * **environment** – Deployment environment.
* **Raises:**
  [**subprocess.CalledProcessError**](https://docs.python.org/3/library/subprocess.html#subprocess.CalledProcessError) – If git commands fail.

### pudl.deploy.pudl.dispatch_github_workflow(repo: [str](https://docs.python.org/3/library/stdtypes.html#str), workflow_file: [str](https://docs.python.org/3/library/stdtypes.html#str), ref: [str](https://docs.python.org/3/library/stdtypes.html#str), token: [str](https://docs.python.org/3/library/stdtypes.html#str), inputs: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)] | [None](https://docs.python.org/3/library/constants.html#None) = None) → [None](https://docs.python.org/3/library/constants.html#None)

Trigger a workflow_dispatch event on a GitHub Actions workflow.

* **Parameters:**
  * **repo** – GitHub repo in “owner/name” form (e.g. “catalyst-cooperative/pudl”).
  * **workflow_file** – Workflow filename (e.g. “zenodo-data-release.yml”).
  * **ref** – Git branch or tag to run the workflow from.
  * **token** – Bearer token to authenticate to GitHub.
  * **inputs** – workflow_dispatch inputs, if the workflow takes any.

### pudl.deploy.pudl.trigger_zenodo_release(build_ref: [str](https://docs.python.org/3/library/stdtypes.html#str), deploy_type: [DeploymentType](#pudl.deploy.pudl.DeploymentType), source_suffix: [str](https://docs.python.org/3/library/stdtypes.html#str), token: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [None](https://docs.python.org/3/library/constants.html#None)

Trigger Zenodo data release GitHub Actions workflow.

Dispatches the zenodo-data-release workflow to create or update a Zenodo
deposition with PUDL data outputs.

* **Parameters:**
  * **build_ref** – The git reference for the workflow. The reference can be a branch or tag name.
  * **deploy_type** – Deployment type.
  * **source_suffix** – Suffix appended to s3 path (s3://pudl.catalyst.coop) to get
    path to data outputs which will populate zenodo deposition.
  * **token** – the bearer token to authenticate to GitHub.

### pudl.deploy.pudl.update_pudl_viewer(token: [str](https://docs.python.org/3/library/stdtypes.html#str), environment: Literal['staging', 'production']) → [None](https://docs.python.org/3/library/constants.html#None)

Update PUDL Viewer Cloud Run service to latest image.

* **Parameters:**
  * **token** – the bearer token to authenticate to GitHub.
  * **environment** – deploy staging or production version of viewer.

### pudl.deploy.pudl.set_gcs_temporary_hold(gcs_path: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [None](https://docs.python.org/3/library/constants.html#None)

Set temporary hold on GCS objects to prevent deletion.

Applies a temporary hold to protect versioned release artifacts from
accidental deletion or lifecycle policies.

* **Parameters:**
  * **gcs_path** – GCS path to objects (e.g., “gs://pudl.catalyst.coop/v2025.2.3/”).
  * **billing_project** – which project to bill for Requester Pays buckets.

### pudl.deploy.pudl.check_build_success(build_path: upath.UPath) → upath.UPath

Raise error if success file doesn’t exist in build directory.

### pudl.deploy.pudl.get_build_from_tag(tag: [str](https://docs.python.org/3/library/stdtypes.html#str)) → upath.UPath

Find any builds associated with a git tag and return a GCS path to most recent build.

### pudl.deploy.pudl.get_deployment_type_from_tag(git_tag: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [DeploymentType](#pudl.deploy.pudl.DeploymentType)

Check if tag looks like a ‘nightly’, ‘branch’, or ‘stable’ tag.

### *class* pudl.deploy.pudl.ResolvedBuild

Everything `pudl_deploy`’s `main()` needs after resolving a deployment.

#### plan *: [DeploymentPlan](#pudl.deploy.pudl.DeploymentPlan)*

#### build_path *: upath.UPath*

#### build_id *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### local_copy_path *: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)*

#### local_logfile *: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)*

### pudl.deploy.pudl.resolve_build(git_tag: [str](https://docs.python.org/3/library/stdtypes.html#str), environment: Literal['staging', 'production']) → [ResolvedBuild](#pudl.deploy.pudl.ResolvedBuild)

Resolve the deployment plan, locate the build, and set up local logging.

Raises if `git_tag` doesn’t look like a nightly/stable/branch tag, if a
branch tag is being deployed to production, or if no successful build exists
for the tag yet.

### *class* pudl.deploy.pudl.StageStatus(\*args, \*\*kwds)

Bases: [`enum.Enum`](https://docs.python.org/3/library/enum.html#enum.Enum)

Possible outcomes of a single deployment stage.

#### SKIPPED *= 'skipped'*

#### SUCCESS *= 'success'*

#### FAILURE *= 'failure'*

### *class* pudl.deploy.pudl.DeployStage(\*args, \*\*kwds)

Bases: [`enum.Enum`](https://docs.python.org/3/library/enum.html#enum.Enum)

The fixed set of tracked deployment stages.

Members are declared in the order they should appear in the Zulip notification
table, and `DeployStage` iteration preserves that order – so this is the
single source of truth for both the valid stage identifiers (used as dict keys
and passed to `run_stage`) and their display order, rather than an
unconstrained string that’s only coincidentally consistent
between call sites. `.value` gives the human-readable name shown in messages.

#### RESOLVE_BUILD *= 'Resolve build'*

#### DOWNLOAD_BUILD_OUTPUTS *= 'Download build outputs'*

#### PREPARE_OUTPUTS *= 'Prepare outputs'*

#### UPLOAD_OUTPUTS *= 'Upload outputs'*

#### REDEPLOY_EEL_HOLE *= 'Redeploy Eel Hole'*

#### UPDATE_GIT_BRANCH *= 'Update Git Branch'*

#### TRIGGER_ZENODO_RELEASE *= 'Trigger Zenodo Release'*

#### GCS_TEMPORARY_HOLD *= 'GCS Temporary Hold'*

### pudl.deploy.pudl.ZULIP_API_URL *= 'https://catalyst-cooperative.zulipchat.com/api/v1/messages'*

### pudl.deploy.pudl.ZULIP_BOT_EMAIL *= 'build-status-bot@catalyst-cooperative.zulipchat.com'*

### pudl.deploy.pudl.ZULIP_STREAM *= 'pudl-deployments'*

### pudl.deploy.pudl.ZULIP_TOPIC *= 'build-deploy-pudl'*

### *class* pudl.deploy.pudl.StageResult

Outcome of a single deployment stage, for Zulip stage-table reporting.

#### status *: [StageStatus](#pudl.deploy.pudl.StageStatus)*

#### duration_seconds *: [float](https://docs.python.org/3/library/functions.html#float)* *= 0.0*

### pudl.deploy.pudl.new_deploy_stage_results() → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[DeployStage](#pudl.deploy.pudl.DeployStage), [StageResult](#pudl.deploy.pudl.StageResult)]

Initialize every tracked deploy stage as skipped, in table display order.

### pudl.deploy.pudl.run_stage(stage_fn: [collections.abc.Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[Ellipsis, [T](../../metadata/classes/index.md#pudl.metadata.classes.T)], stage_name: [DeployStage](#pudl.deploy.pudl.DeployStage), stage_results: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[DeployStage](#pudl.deploy.pudl.DeployStage), [StageResult](#pudl.deploy.pudl.StageResult)], \*args, fail_hard: [bool](https://docs.python.org/3/library/functions.html#bool) = True, \*\*kwargs) → [T](../../metadata/classes/index.md#pudl.metadata.classes.T) | [None](https://docs.python.org/3/library/constants.html#None)

Run a deploy stage, recording its status and duration in `stage_results`.

If `stage_fn` raises and `fail_hard` is True (the default), the exception
propagates to the caller after the stage is recorded as failed. Pass
`fail_hard=False` for stages that shouldn’t block their siblings – e.g. a
failed Zenodo release shouldn’t prevent the GCS temporary hold from being
attempted – in which case the failure is logged instead of raised.

Returns whatever `stage_fn` returns (`None` if it failed with
`fail_hard=False`), so stages that produce a result – e.g. `resolve_build`
– can be run through the same tracking/reporting machinery as side-effect-only
stages.

### pudl.deploy.pudl.format_stage_duration(elapsed_seconds: [float](https://docs.python.org/3/library/functions.html#float)) → [str](https://docs.python.org/3/library/stdtypes.html#str)

Format a duration in seconds as `HH:MM:SS`.

### pudl.deploy.pudl.stage_emoji(status: [StageStatus](#pudl.deploy.pudl.StageStatus)) → [str](https://docs.python.org/3/library/stdtypes.html#str)

Return the Zulip emoji corresponding to a stage status.

### pudl.deploy.pudl.build_deploy_logfile_links(build_id: [str](https://docs.python.org/3/library/stdtypes.html#str), deploy_logfile_name: [str](https://docs.python.org/3/library/stdtypes.html#str), batch_job_name: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)) → [str](https://docs.python.org/3/library/stdtypes.html#str)

Build markdown links for reviewing a deployment’s logs and outputs.

Mirrors the “Review PUDL Build Logs” section `pudl_batch.sh` appends to the
nightly build’s own Zulip notification (see `pudl_logfile_links`).

* **Parameters:**
  * **build_id** – The build directory name under gs://builds.catalyst.coop.
  * **deploy_logfile_name** – Filename of this deployment’s logfile within that
    build directory.
  * **batch_job_name** – Name of the Google Batch job running this deployment, if
    known – omitted from the message (rather than producing a broken
    link) when unset, e.g. when testing outside of an actual Batch job.

### pudl.deploy.pudl.build_deploy_zulip_message(build_id: [str](https://docs.python.org/3/library/stdtypes.html#str), git_tag: [str](https://docs.python.org/3/library/stdtypes.html#str), stage_results: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[DeployStage](#pudl.deploy.pudl.DeployStage), [StageResult](#pudl.deploy.pudl.StageResult)], total_duration_seconds: [float](https://docs.python.org/3/library/functions.html#float), deploy_logfile_name: [str](https://docs.python.org/3/library/stdtypes.html#str), batch_job_name: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None) → [str](https://docs.python.org/3/library/stdtypes.html#str)

Build a markdown Zulip message summarizing deployment stage statuses.

### pudl.deploy.pudl.send_zulip_message(message: [str](https://docs.python.org/3/library/stdtypes.html#str), api_key: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [None](https://docs.python.org/3/library/constants.html#None)

Post a message to the pudl-deployments Zulip stream.
