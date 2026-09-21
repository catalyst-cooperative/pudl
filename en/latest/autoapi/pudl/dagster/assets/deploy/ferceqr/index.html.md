# pudl.dagster.assets.deploy.ferceqr

Define deployment helper assets for publishing FERC EQR outputs.

These assets run during batch builds to publish transformed FERC EQR outputs,
notify Zulip of success or failure, and create status files that tell the batch
job when deployment handling is complete.

## Attributes

| [`logger`](#pudl.dagster.assets.deploy.ferceqr.logger)                        |    |
|--------------------------------------------------------------------------------|----|
| [`FERCEQR_SOURCE_RUN_ID_TAG`](#pudl.dagster.assets.deploy.ferceqr.FERCEQR_SOURCE_RUN_ID_TAG)     |    |
| [`FERCEQR_SOURCE_PARTITIONS_TAG`](#pudl.dagster.assets.deploy.ferceqr.FERCEQR_SOURCE_PARTITIONS_TAG) |    |
| [`DAGSTER_BACKFILL_TAG`](#pudl.dagster.assets.deploy.ferceqr.DAGSTER_BACKFILL_TAG)          |    |
| [`FERCEQR_TRANSFORM_ASSETS`](#pudl.dagster.assets.deploy.ferceqr.FERCEQR_TRANSFORM_ASSETS)      |    |
| [`DATAPACKAGE_FILENAME`](#pudl.dagster.assets.deploy.ferceqr.DATAPACKAGE_FILENAME)          |    |
| [`DEPLOYED_DATAPACKAGE_FILENAME`](#pudl.dagster.assets.deploy.ferceqr.DEPLOYED_DATAPACKAGE_FILENAME) |    |
| [`STAGING_DATA_SUBDIR`](#pudl.dagster.assets.deploy.ferceqr.STAGING_DATA_SUBDIR)           |    |
| [`STAGING_META_SUBDIR`](#pudl.dagster.assets.deploy.ferceqr.STAGING_META_SUBDIR)           |    |
| [`PREVIOUS_DIRNAME`](#pudl.dagster.assets.deploy.ferceqr.PREVIOUS_DIRNAME)              |    |
| [`StepStatusTable`](#pudl.dagster.assets.deploy.ferceqr.StepStatusTable)               |    |

## Classes

| [`_DeploymentTarget`](#pudl.dagster.assets.deploy.ferceqr._DeploymentTarget)   | One resolved deployment destination plus the scratch prefixes beside it.   |
|----------------------------------------------------------------------|----------------------------------------------------------------------------|

## Functions

| [`_write_status_file`](#pudl.dagster.assets.deploy.ferceqr._write_status_file)(status, pudl_paths)                      | Notify build script that job is complete by creating a status file.                                                                          |
|--------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------|
| [`_clear_status_files`](#pudl.dagster.assets.deploy.ferceqr._clear_status_files)(→ None)                                 | Remove any stale FERC EQR status files from the output directory.                                                                            |
| [`_deployment_targets`](#pudl.dagster.assets.deploy.ferceqr._deployment_targets)(→ list[_DeploymentTarget])              | Turn resolved deployment UPaths into [`_DeploymentTarget`](#pudl.dagster.assets.deploy.ferceqr._DeploymentTarget) records.                             |
| [`_source_parquet_files`](#pudl.dagster.assets.deploy.ferceqr._source_parquet_files)(→ dict[str, list[pathlib.Path]])      | Map each transform asset to its local Parquet files for *source_partitions*.                                                                 |
| [`_expected_object_sizes`](#pudl.dagster.assets.deploy.ferceqr._expected_object_sizes)(→ dict[str, int])                    | Return `{staging_relative_path: size_bytes}` for everything to be uploaded.                                                                  |
| [`_wait`](#pudl.dagster.assets.deploy.ferceqr._wait)(→ None)                                               | Block until every future finishes, raising the first failure.                                                                                |
| [`_is_s3`](#pudl.dagster.assets.deploy.ferceqr._is_s3)(→ bool)                                              | Whether *path* lives on S3, where bulk transfers bypass fsspec.                                                                              |
| [`_make_local_parents`](#pudl.dagster.assets.deploy.ferceqr._make_local_parents)(→ None)                                 | Create parent directories for local *paths*; object stores need none.                                                                        |
| [`_relative_objects`](#pudl.dagster.assets.deploy.ferceqr._relative_objects)(→ dict[str, int])                         | Map each object's path relative to *root* to its size in bytes.                                                                              |
| [`_verify_staged`](#pudl.dagster.assets.deploy.ferceqr._verify_staged)(→ None)                                      | Raise [`RuntimeError`](https://docs.python.org/3/builtins/exceptions.html#RuntimeError) unless staging holds exactly the *expected* objects. |
| [`_copy_tree`](#pudl.dagster.assets.deploy.ferceqr._copy_tree)(→ None)                                          | Copy every object under *src* to the same relative path under *dst*.                                                                         |
| [`_stage_target`](#pudl.dagster.assets.deploy.ferceqr._stage_target)(→ None)                                       | Upload all outputs to *target*'s staging prefix and verify they arrived.                                                                     |
| [`_promote_target`](#pudl.dagster.assets.deploy.ferceqr._promote_target)(→ None)                                     | Snapshot the live tree, then merge staging into it and drop the staging dir.                                                                 |
| [`_remove_all_staging`](#pudl.dagster.assets.deploy.ferceqr._remove_all_staging)(→ None)                                 | Best-effort removal of every target's staging prefix after a failure.                                                                        |
| [`_parse_step_key`](#pudl.dagster.assets.deploy.ferceqr._parse_step_key)(→ tuple[str, str])                          | Extract asset and partition from a step key like `asset_name[partition]`.                                                                    |
| [`_validate_partitions`](#pudl.dagster.assets.deploy.ferceqr._validate_partitions)(→ list[str])                           | Validate and parse the source partitions JSON from run tags.                                                                                 |
| [`_markdown_step_status_table`](#pudl.dagster.assets.deploy.ferceqr._markdown_step_status_table)(→ str)                          | Format terminal step statuses as an asset-by-partition Markdown table.                                                                       |
| [`_gather_step_statuses`](#pudl.dagster.assets.deploy.ferceqr._gather_step_statuses)(→ tuple[StepStatusTable, str | None]) | Collect step statuses and total elapsed time across all source runs.                                                                         |
| [`_markdown_logfile_list`](#pudl.dagster.assets.deploy.ferceqr._markdown_logfile_list)(→ str)                               | Return pointer to logs to send in Zulip message.                                                                                             |
| [`_compute_deploy_duration`](#pudl.dagster.assets.deploy.ferceqr._compute_deploy_duration)(→ str | None)                      | Return elapsed time since the current run started, or None on failure.                                                                       |
| [`build_ferceqr_notification`](#pudl.dagster.assets.deploy.ferceqr.build_ferceqr_notification)(→ str)                           | Build a Markdown notification string for FERC EQR deployment outcomes.                                                                       |
| [`deployment_status_asset`](#pudl.dagster.assets.deploy.ferceqr.deployment_status_asset)(→ dagster.AssetsDefinition)         | Create a custom decorator for deployment handler assets.                                                                                     |
| [`_deploy_source_partitions`](#pudl.dagster.assets.deploy.ferceqr._deploy_source_partitions)(→ list[str])                      | Read the built partitions from run tags, empty when invoked outside a run.                                                                   |
| [`deploy_ferceqr`](#pudl.dagster.assets.deploy.ferceqr.deploy_ferceqr)(context)                                     | Publish EQR outputs to configured deployment targets.                                                                                        |
| [`handle_ferceqr_failure`](#pudl.dagster.assets.deploy.ferceqr.handle_ferceqr_failure)(context)                             | Send notification if the FERC EQR build failed.                                                                                              |

## Module Contents

### pudl.dagster.assets.deploy.ferceqr.logger

### pudl.dagster.assets.deploy.ferceqr.FERCEQR_SOURCE_RUN_ID_TAG *= 'ferceqr/source_run_id'*

### pudl.dagster.assets.deploy.ferceqr.FERCEQR_SOURCE_PARTITIONS_TAG *= 'ferceqr/source_partitions'*

### pudl.dagster.assets.deploy.ferceqr.DAGSTER_BACKFILL_TAG *= 'dagster/backfill'*

### pudl.dagster.assets.deploy.ferceqr.FERCEQR_TRANSFORM_ASSETS *= ['core_ferceqr_\_contracts', 'core_ferceqr_\_transactions', 'core_ferceqr_\_quarterly_identity',...*

### pudl.dagster.assets.deploy.ferceqr.DATAPACKAGE_FILENAME *= 'ferceqr_parquet_datapackage.json'*

### pudl.dagster.assets.deploy.ferceqr.DEPLOYED_DATAPACKAGE_FILENAME *= 'datapackage.json'*

### pudl.dagster.assets.deploy.ferceqr.STAGING_DATA_SUBDIR *= 'data'*

### pudl.dagster.assets.deploy.ferceqr.STAGING_META_SUBDIR *= 'meta'*

### pudl.dagster.assets.deploy.ferceqr.PREVIOUS_DIRNAME *= '._ferceqr_previous'*

### pudl.dagster.assets.deploy.ferceqr.StepStatusTable

### pudl.dagster.assets.deploy.ferceqr.\_write_status_file(status: Literal['FERCEQR_SUCCESS', 'FERCEQR_FAILURE'], pudl_paths: [pudl.workspace.setup.PudlPaths](../../../../workspace/setup/index.html.md#pudl.workspace.setup.PudlPaths))

Notify build script that job is complete by creating a status file.

Flush logging handlers before writing the sentinel. The bash script that
launched the Dagster daemon uses `inotifywait` to watch for this sentinel
and runs `killall dagster-daemon` as soon as it appears. Any buffered log
output written before the sentinel but not yet flushed will be lost when the
daemon process is killed, making errors invisible in the log.

### pudl.dagster.assets.deploy.ferceqr.\_clear_status_files(pudl_paths: [pudl.workspace.setup.PudlPaths](../../../../workspace/setup/index.html.md#pudl.workspace.setup.PudlPaths)) → [None](https://docs.python.org/3/builtins/constants.html#None)

Remove any stale FERC EQR status files from the output directory.

### *class* pudl.dagster.assets.deploy.ferceqr.\_DeploymentTarget

One resolved deployment destination plus the scratch prefixes beside it.

`final`/`staging`/`previous` are `UPath` objects on local
disk or cloud storage (`gs://…`, `s3://…`).

#### final *: upath.UPath*

#### build_id *: [str](https://docs.python.org/3/builtins/stdtypes.html#str)*

#### *property* staging *: upath.UPath*

Per-build scratch prefix holding everything staged for this build.

#### *property* staging_data *: upath.UPath*

Prefix holding staged Parquet files, one subdirectory per table.

#### *property* staging_meta *: upath.UPath*

Prefix holding the staged datapackage JSON.

#### *property* previous *: upath.UPath*

Prefix holding snapshot of previous version.

### pudl.dagster.assets.deploy.ferceqr.\_deployment_targets(resolved_targets: [list](https://docs.python.org/3/builtins/stdtypes.html#list)[upath.UPath]) → [list](https://docs.python.org/3/builtins/stdtypes.html#list)[[\_DeploymentTarget](#pudl.dagster.assets.deploy.ferceqr._DeploymentTarget)]

Turn resolved deployment UPaths into [`_DeploymentTarget`](#pudl.dagster.assets.deploy.ferceqr._DeploymentTarget) records.

The staging and previous-build prefixes are siblings of the final target so
their key namespaces never overlap it. The staging suffix ties the scratch
prefix to a single build (BUILD_ID), with a random fallback for local runs.

### pudl.dagster.assets.deploy.ferceqr.\_source_parquet_files(source_partitions: [list](https://docs.python.org/3/builtins/stdtypes.html#list)[[str](https://docs.python.org/3/builtins/stdtypes.html#str)]) → [dict](https://docs.python.org/3/builtins/stdtypes.html#dict)[[str](https://docs.python.org/3/builtins/stdtypes.html#str), [list](https://docs.python.org/3/builtins/stdtypes.html#list)[[pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)]]

Map each transform asset to its local Parquet files for *source_partitions*.

Raises [`FileNotFoundError`](https://docs.python.org/3/builtins/exceptions.html#FileNotFoundError) if any expected partition file is missing so
an incomplete build never gets partially deployed.

### pudl.dagster.assets.deploy.ferceqr.\_expected_object_sizes(table_files: [dict](https://docs.python.org/3/builtins/stdtypes.html#dict)[[str](https://docs.python.org/3/builtins/stdtypes.html#str), [list](https://docs.python.org/3/builtins/stdtypes.html#list)[[pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)]], datapackage_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)) → [dict](https://docs.python.org/3/builtins/stdtypes.html#dict)[[str](https://docs.python.org/3/builtins/stdtypes.html#str), [int](https://docs.python.org/3/builtins/functions.html#int)]

Return `{staging_relative_path: size_bytes}` for everything to be uploaded.

### pudl.dagster.assets.deploy.ferceqr.\_wait(futures: [list](https://docs.python.org/3/builtins/stdtypes.html#list)[[concurrent.futures.Future](https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.Future)]) → [None](https://docs.python.org/3/builtins/constants.html#None)

Block until every future finishes, raising the first failure.

### pudl.dagster.assets.deploy.ferceqr.\_is_s3(path: upath.UPath) → [bool](https://docs.python.org/3/builtins/functions.html#bool)

Whether *path* lives on S3, where bulk transfers bypass fsspec.

### pudl.dagster.assets.deploy.ferceqr.\_make_local_parents(paths: [list](https://docs.python.org/3/builtins/stdtypes.html#list)[upath.UPath]) → [None](https://docs.python.org/3/builtins/constants.html#None)

Create parent directories for local *paths*; object stores need none.

### pudl.dagster.assets.deploy.ferceqr.\_relative_objects(root: upath.UPath) → [dict](https://docs.python.org/3/builtins/stdtypes.html#dict)[[str](https://docs.python.org/3/builtins/stdtypes.html#str), [int](https://docs.python.org/3/builtins/functions.html#int)]

Map each object’s path relative to *root* to its size in bytes.

Empty if nothing exists under *root*. The filesystem’s listing cache is dropped
first, since S3 transfers go through boto3 and would otherwise leave it stale.

### pudl.dagster.assets.deploy.ferceqr.\_verify_staged(target: [\_DeploymentTarget](#pudl.dagster.assets.deploy.ferceqr._DeploymentTarget), expected: [dict](https://docs.python.org/3/builtins/stdtypes.html#dict)[[str](https://docs.python.org/3/builtins/stdtypes.html#str), [int](https://docs.python.org/3/builtins/functions.html#int)]) → [None](https://docs.python.org/3/builtins/constants.html#None)

Raise [`RuntimeError`](https://docs.python.org/3/builtins/exceptions.html#RuntimeError) unless staging holds exactly the *expected* objects.

The failure that matters is a transfer that died partway, so names and byte
sizes are compared; bit-level integrity is enforced by the transfer clients.

### pudl.dagster.assets.deploy.ferceqr.\_copy_tree(src: upath.UPath, dst: upath.UPath, executor: [concurrent.futures.ThreadPoolExecutor](https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor)) → [None](https://docs.python.org/3/builtins/constants.html#None)

Copy every object under *src* to the same relative path under *dst*.

Existing objects under *dst* are overwritten and others are left alone. Files
are copied one by one, since fsspec would nest a directory copied onto an existing
directory. On S3 this is one batch of server-side copies through boto3; elsewhere
each file is copied by fsspec in the thread pool.

### pudl.dagster.assets.deploy.ferceqr.\_stage_target(target: [\_DeploymentTarget](#pudl.dagster.assets.deploy.ferceqr._DeploymentTarget), table_files: [dict](https://docs.python.org/3/builtins/stdtypes.html#dict)[[str](https://docs.python.org/3/builtins/stdtypes.html#str), [list](https://docs.python.org/3/builtins/stdtypes.html#list)[[pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)]], datapackage_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path), executor: [concurrent.futures.ThreadPoolExecutor](https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor)) → [None](https://docs.python.org/3/builtins/constants.html#None)

Upload all outputs to *target*’s staging prefix and verify they arrived.

Raises [`RuntimeError`](https://docs.python.org/3/builtins/exceptions.html#RuntimeError) if the staged object set does not exactly match
the local outputs by name and byte size. The final target is untouched.

### pudl.dagster.assets.deploy.ferceqr.\_promote_target(target: [\_DeploymentTarget](#pudl.dagster.assets.deploy.ferceqr._DeploymentTarget), executor: [concurrent.futures.ThreadPoolExecutor](https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor)) → [None](https://docs.python.org/3/builtins/constants.html#None)

Snapshot the live tree, then merge staging into it and drop the staging dir.

Promotion is a merge: staged files overwrite live files of the same name and
everything else already under the final prefix is left alone, so a build that
covers only some partitions updates just those. Nothing is ever deleted from the
live prefix; stale files must be removed by hand.

The datapackage JSON is promoted after the Parquet data so it never briefly
references files that have not landed yet.

### pudl.dagster.assets.deploy.ferceqr.\_remove_all_staging(targets: [list](https://docs.python.org/3/builtins/stdtypes.html#list)[[\_DeploymentTarget](#pudl.dagster.assets.deploy.ferceqr._DeploymentTarget)]) → [None](https://docs.python.org/3/builtins/constants.html#None)

Best-effort removal of every target’s staging prefix after a failure.

### pudl.dagster.assets.deploy.ferceqr.\_parse_step_key(step_key: [str](https://docs.python.org/3/builtins/stdtypes.html#str), source_partition: [str](https://docs.python.org/3/builtins/stdtypes.html#str) | [None](https://docs.python.org/3/builtins/constants.html#None)) → [tuple](https://docs.python.org/3/builtins/stdtypes.html#tuple)[[str](https://docs.python.org/3/builtins/stdtypes.html#str), [str](https://docs.python.org/3/builtins/stdtypes.html#str)]

Extract asset and partition from a step key like `asset_name[partition]`.

FERC EQR transform steps always follow the `asset_name[partition]` format, so the
regex match is expected to succeed for all relevant steps. The fallback path is a
safety net for unexpected non-bracketed step keys (e.g. system steps). In that case,
the raw step key is used as the asset label so the status table remains
comprehensible, and `source_partition or "UNKNOWN"` avoids `None` as a dict key.

### pudl.dagster.assets.deploy.ferceqr.\_validate_partitions(raw: [str](https://docs.python.org/3/builtins/stdtypes.html#str) | [None](https://docs.python.org/3/builtins/constants.html#None)) → [list](https://docs.python.org/3/builtins/stdtypes.html#list)[[str](https://docs.python.org/3/builtins/stdtypes.html#str)]

Validate and parse the source partitions JSON from run tags.

Also verifies that each partition is one of the allowed working partitions
defined for the `ferceqr` data source in `pudl.metadata.sources`.

### pudl.dagster.assets.deploy.ferceqr.\_markdown_step_status_table(asset_partition_statuses: [StepStatusTable](#pudl.dagster.assets.deploy.ferceqr.StepStatusTable), partitions: [list](https://docs.python.org/3/builtins/stdtypes.html#list)[[str](https://docs.python.org/3/builtins/stdtypes.html#str)] | [None](https://docs.python.org/3/builtins/constants.html#None)) → [str](https://docs.python.org/3/builtins/stdtypes.html#str)

Format terminal step statuses as an asset-by-partition Markdown table.

### pudl.dagster.assets.deploy.ferceqr.\_gather_step_statuses(context: [dagster.AssetExecutionContext](https://docs.dagster.io/api/dagster/execution/#dagster.AssetExecutionContext), source_run_id: [str](https://docs.python.org/3/builtins/stdtypes.html#str)) → [tuple](https://docs.python.org/3/builtins/stdtypes.html#tuple)[[StepStatusTable](#pudl.dagster.assets.deploy.ferceqr.StepStatusTable), [str](https://docs.python.org/3/builtins/stdtypes.html#str) | [None](https://docs.python.org/3/builtins/constants.html#None)]

Collect step statuses and total elapsed time across all source runs.

The elapsed time is computed from the earliest `start_time` to the latest
`end_time` across all source runs (backfill or single). Returns
`(statuses, formatted_duration)`.

### pudl.dagster.assets.deploy.ferceqr.\_markdown_logfile_list(build_id: [str](https://docs.python.org/3/builtins/stdtypes.html#str)) → [str](https://docs.python.org/3/builtins/stdtypes.html#str)

Return pointer to logs to send in Zulip message.

### pudl.dagster.assets.deploy.ferceqr.\_compute_deploy_duration(context: [dagster.AssetExecutionContext](https://docs.dagster.io/api/dagster/execution/#dagster.AssetExecutionContext)) → [str](https://docs.python.org/3/builtins/stdtypes.html#str) | [None](https://docs.python.org/3/builtins/constants.html#None)

Return elapsed time since the current run started, or None on failure.

### pudl.dagster.assets.deploy.ferceqr.build_ferceqr_notification(context: [dagster.AssetExecutionContext](https://docs.dagster.io/api/dagster/execution/#dagster.AssetExecutionContext), outcome: Literal['SUCCESS', 'FAILURE', 'SKIPPED']) → [str](https://docs.python.org/3/builtins/stdtypes.html#str)

Build a Markdown notification string for FERC EQR deployment outcomes.

Extracts all relevant information (source partitions, run ID, duration,
step statuses, distribution paths, build ID) from the Dagster execution
context and returns a formatted Markdown message ready for Zulip.

### pudl.dagster.assets.deploy.ferceqr.deployment_status_asset(asset_fn: [collections.abc.Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)) → [dagster.AssetsDefinition](https://docs.dagster.io/api/dagster/assets/#dagster.AssetsDefinition)

Create a custom decorator for deployment handler assets.

This allows us to gracefully handle errors if the deployment assets fail for any
reason. When these assets fail, sometimes the logs don’t show up in the batch job
appropriately, and the status file never gets created, so the job keeps running
until it eventually times out.

### pudl.dagster.assets.deploy.ferceqr.\_deploy_source_partitions(context: [dagster.AssetExecutionContext](https://docs.dagster.io/api/dagster/execution/#dagster.AssetExecutionContext)) → [list](https://docs.python.org/3/builtins/stdtypes.html#list)[[str](https://docs.python.org/3/builtins/stdtypes.html#str)]

Read the built partitions from run tags, empty when invoked outside a run.

Deploying only the *specific* partitions this build produced avoids sweeping
up unrelated data left lying around, especially on local runs.

### pudl.dagster.assets.deploy.ferceqr.deploy_ferceqr(context: [dagster.AssetExecutionContext](https://docs.dagster.io/api/dagster/execution/#dagster.AssetExecutionContext))

Publish EQR outputs to configured deployment targets.

Each target is handled with a staging-then-promote pattern. S3 targets move
bytes through [`pudl.deploy.s3_transfer`](../../../../deploy/s3_transfer/index.html.md#module-pudl.deploy.s3_transfer) (much faster than `s3fs`); all
other targets, and all listing and deletion, use `fsspec`:

1. Upload every built Parquet file and the datapackage JSON into a per-build
   `._staging_{BUILD_ID}` prefix beside the target.
2. Verify the staged object set matches the local outputs by name and size.
3. Copy the current live tree into `._ferceqr_previous` for rollback.
4. Server-side merge staging into the final prefix (data first, datapackage
   last) and delete the staging prefix. Staged files overwrite live files of the
   same name; other live files are left in place.

A failure in steps 1-2 leaves the target untouched. A failure in steps 3-4
may leave the target with a mix of old and new files; `._ferceqr_previous`
holds the prior build for a manual rollback. Targets are processed
concurrently.

### pudl.dagster.assets.deploy.ferceqr.handle_ferceqr_failure(context: [dagster.AssetExecutionContext](https://docs.dagster.io/api/dagster/execution/#dagster.AssetExecutionContext))

Send notification if the FERC EQR build failed.
