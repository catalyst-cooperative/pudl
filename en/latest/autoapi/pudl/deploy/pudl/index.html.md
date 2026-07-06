# pudl.deploy.pudl

Distribute PUDL ETL outputs to cloud storage and update git branches.

This module handles distribution of completed ETL builds to public cloud storage
(GCS and S3), git branch updates, Zenodo releases, and Cloud Run deployments.

## Attributes

| [`logger`](#pudl.deploy.pudl.logger)   |    |
|----------------------------------------|----|

## Classes

| [`DeploymentType`](#pudl.deploy.pudl.DeploymentType)   | Deployments can be 'nightly', 'branch', or 'stable'.   |
|--------------------------------------------------------|--------------------------------------------------------|

## Functions

| [`_zip_parquet_files`](#pudl.deploy.pudl._zip_parquet_files)(→ None)                               | Create a zipfile containing parquet files and an associated datapackage JSON file.    |
|----------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------|
| [`prepare_outputs_for_distribution`](#pudl.deploy.pudl.prepare_outputs_for_distribution)(→ None)   | Prepare ETL outputs for distribution.                                                 |
| [`_run`](#pudl.deploy.pudl._run)(→ str | None)                                                     | Wrap subprocess.run so we see error output.                                           |
| [`upload_outputs`](#pudl.deploy.pudl.upload_outputs)(→ None)                                       | Upload outputs to cloud storage paths.                                                |
| [`update_git_branch`](#pudl.deploy.pudl.update_git_branch)(→ None)                                 | Merge git tag into branch and push to origin.                                         |
| [`trigger_zenodo_release`](#pudl.deploy.pudl.trigger_zenodo_release)(→ None)                       | Trigger Zenodo data release GitHub Actions workflow.                                  |
| [`update_pudl_viewer`](#pudl.deploy.pudl.update_pudl_viewer)(→ None)                               | Update PUDL Viewer Cloud Run service to latest image.                                 |
| [`set_gcs_temporary_hold`](#pudl.deploy.pudl.set_gcs_temporary_hold)(→ None)                       | Set temporary hold on GCS objects to prevent deletion.                                |
| [`check_build_success`](#pudl.deploy.pudl.check_build_success)(→ upath.UPath)                      | Raise error if success file doesn't exist in build directory.                         |
| [`get_build_from_tag`](#pudl.deploy.pudl.get_build_from_tag)(→ upath.UPath)                        | Find any builds associated with a git tag and return a GCS path to most recent build. |
| [`get_deployment_type_from_tag`](#pudl.deploy.pudl.get_deployment_type_from_tag)(→ DeploymentType) | Check if tag looks like a 'nightly', 'branch', or 'stable' tag.                       |

## Module Contents

### pudl.deploy.pudl.logger

### *class* pudl.deploy.pudl.DeploymentType(\*args, \*\*kwds)

Bases: [`enum.Enum`](https://docs.python.org/3/library/enum.html#enum.Enum)

Deployments can be ‘nightly’, ‘branch’, or ‘stable’.

#### NIGHTLY *= 'nightly'*

#### STABLE *= 'stable'*

#### BRANCH *= 'branch'*

### pudl.deploy.pudl.\_zip_parquet_files(parquet_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path), output_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)) → [None](https://docs.python.org/3/library/constants.html#None)

Create a zipfile containing parquet files and an associated datapackage JSON file.

`parquet_path` should contain a set of parquet files and exactly one datapackage
JSON file that describes those parquet files.

* **Parameters:**
  * **parquet_path** – Path to directory containing parquet files.
  * **output_path** – Path to zipfile that should be created by this function.

### pudl.deploy.pudl.prepare_outputs_for_distribution(local_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path), build_path: upath.UPath) → [None](https://docs.python.org/3/library/constants.html#None)

Prepare ETL outputs for distribution.

Takes raw ETL output structure and produces distribution-ready outputs:
- Moves parquet files from parquet/ subdirectory to root
- Compresses SQLite databases with maximum compression
- Creates parquet archive (no compression, already compressed)
- Removes test databases and temporary directories

In general, we want to know if these files don’t exist, so
FileNotFoundErrors are OK and we don’t need to pre-emptively try to avoid
them.

* **Parameters:**
  * **local_path** – Path on local filesystem where we will prep outputs for distribution.
  * **build_path** – Remote path containing raw build outputs.

### pudl.deploy.pudl.\_run(cmd: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]) → [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)

Wrap subprocess.run so we see error output.

### pudl.deploy.pudl.upload_outputs(source_dir: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path), path_suffixes: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]) → [None](https://docs.python.org/3/library/constants.html#None)

Upload outputs to cloud storage paths.

Uploads all files from source directory to GCS and S3 using the provided path
suffixes. Each suffix is uploaded to both gs://pudl.catalyst.coop/{suffix}/ and
s3://pudl.catalyst.coop/{suffix}/.

* **Parameters:**
  * **source_dir** – Local directory containing prepared outputs to upload.
  * **path_suffixes** – Path suffixes to upload to (e.g., [“nightly”, “eel-hole”]).

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
