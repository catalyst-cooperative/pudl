# pudl.dagster.assets.deploy.ferceqr

Define deployment helper assets for publishing FERC EQR outputs.

These assets run during batch builds to publish transformed FERC EQR outputs,
notify Zulip of success or failure, and create status files that tell the batch
job when deployment handling is complete.

## Attributes

| [`logger`](#pudl.dagster.assets.deploy.ferceqr.logger)                                               |    |
|------------------------------------------------------------------------------------------------------|----|
| [`FERCEQR_SOURCE_RUN_ID_TAG`](#pudl.dagster.assets.deploy.ferceqr.FERCEQR_SOURCE_RUN_ID_TAG)         |    |
| [`FERCEQR_SOURCE_PARTITIONS_TAG`](#pudl.dagster.assets.deploy.ferceqr.FERCEQR_SOURCE_PARTITIONS_TAG) |    |
| [`DAGSTER_BACKFILL_TAG`](#pudl.dagster.assets.deploy.ferceqr.DAGSTER_BACKFILL_TAG)                   |    |
| [`FERCEQR_TRANSFORM_ASSETS`](#pudl.dagster.assets.deploy.ferceqr.FERCEQR_TRANSFORM_ASSETS)           |    |
| [`StepStatusTable`](#pudl.dagster.assets.deploy.ferceqr.StepStatusTable)                             |    |

## Functions

| [`_write_status_file`](#pudl.dagster.assets.deploy.ferceqr._write_status_file)(status, pudl_paths)                         | Notify build script that job is complete by creating a status file.             |
|----------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------|
| [`_clear_status_files`](#pudl.dagster.assets.deploy.ferceqr._clear_status_files)(→ None)                                   | Remove any stale FERC EQR status files from the output directory.               |
| [`_staging_path`](#pudl.dagster.assets.deploy.ferceqr._staging_path)(→ upath.UPath)                                        | Return the staging path *alongside* *dist_path* for an atomic deploy.           |
| [`_deploy_to_staging`](#pudl.dagster.assets.deploy.ferceqr._deploy_to_staging)(→ list[upath.UPath])                        | Copy EQR outputs to a staging location under each target, return staging paths. |
| [`_promote_staging`](#pudl.dagster.assets.deploy.ferceqr._promote_staging)(→ None)                                         | Atomically promote staging directories to their final destination paths.        |
| [`_remove_staging`](#pudl.dagster.assets.deploy.ferceqr._remove_staging)(→ None)                                           | Remove a staging directory and all its contents.                                |
| [`_parse_step_key`](#pudl.dagster.assets.deploy.ferceqr._parse_step_key)(→ tuple[str, str])                                | Extract asset and partition from a step key like `asset_name[partition]`.       |
| [`_validate_partitions`](#pudl.dagster.assets.deploy.ferceqr._validate_partitions)(→ list[str])                            | Validate and parse the source partitions JSON from run tags.                    |
| [`_markdown_step_status_table`](#pudl.dagster.assets.deploy.ferceqr._markdown_step_status_table)(→ str)                    | Format terminal step statuses as an asset-by-partition Markdown table.          |
| [`_gather_step_statuses`](#pudl.dagster.assets.deploy.ferceqr._gather_step_statuses)(→ tuple[StepStatusTable, str | None]) | Collect step statuses and total elapsed time across all source runs.            |
| [`_markdown_logfile_list`](#pudl.dagster.assets.deploy.ferceqr._markdown_logfile_list)(→ str)                              | Return pointer to logs to send in Zulip message.                                |
| [`_compute_deploy_duration`](#pudl.dagster.assets.deploy.ferceqr._compute_deploy_duration)(→ str | None)                   | Return elapsed time since the current run started, or None on failure.          |
| [`build_ferceqr_notification`](#pudl.dagster.assets.deploy.ferceqr.build_ferceqr_notification)(→ str)                      | Build a Markdown notification string for FERC EQR deployment outcomes.          |
| [`deployment_status_asset`](#pudl.dagster.assets.deploy.ferceqr.deployment_status_asset)(→ dagster.AssetsDefinition)       | Create a custom decorator for deployment handler assets.                        |
| [`deploy_ferceqr`](#pudl.dagster.assets.deploy.ferceqr.deploy_ferceqr)(context)                                            | Publish EQR outputs to configured deployment targets.                           |
| [`handle_ferceqr_failure`](#pudl.dagster.assets.deploy.ferceqr.handle_ferceqr_failure)(context)                            | Send notification if the FERC EQR build failed.                                 |

## Module Contents

### pudl.dagster.assets.deploy.ferceqr.logger

### pudl.dagster.assets.deploy.ferceqr.FERCEQR_SOURCE_RUN_ID_TAG *= 'ferceqr/source_run_id'*

### pudl.dagster.assets.deploy.ferceqr.FERCEQR_SOURCE_PARTITIONS_TAG *= 'ferceqr/source_partitions'*

### pudl.dagster.assets.deploy.ferceqr.DAGSTER_BACKFILL_TAG *= 'dagster/backfill'*

### pudl.dagster.assets.deploy.ferceqr.FERCEQR_TRANSFORM_ASSETS *= ['core_ferceqr_\_contracts', 'core_ferceqr_\_transactions', 'core_ferceqr_\_quarterly_identity',...*

### pudl.dagster.assets.deploy.ferceqr.StepStatusTable

### pudl.dagster.assets.deploy.ferceqr.\_write_status_file(status: Literal['FERCEQR_SUCCESS', 'FERCEQR_FAILURE'], pudl_paths: [pudl.workspace.setup.PudlPaths](../../../../workspace/setup/index.md#pudl.workspace.setup.PudlPaths))

Notify build script that job is complete by creating a status file.

Flush logging handlers before writing the sentinel. The bash script that
launched the Dagster daemon uses `inotifywait` to watch for this sentinel
and runs `killall dagster-daemon` as soon as it appears. Any buffered log
output written before the sentinel but not yet flushed will be lost when the
daemon process is killed, making errors invisible in the log.

### pudl.dagster.assets.deploy.ferceqr.\_clear_status_files(pudl_paths: [pudl.workspace.setup.PudlPaths](../../../../workspace/setup/index.md#pudl.workspace.setup.PudlPaths)) → [None](https://docs.python.org/3/library/constants.html#None)

Remove any stale FERC EQR status files from the output directory.

### pudl.dagster.assets.deploy.ferceqr.\_staging_path(dist_path: upath.UPath) → upath.UPath

Return the staging path *alongside* *dist_path* for an atomic deploy.

The staging path sits as a sibling of the deployment target, rather than a
child. On cloud storage this avoids prefix-scoping ambiguity during the
rename loop — the staging and final prefixes are completely disjoint:

```text
gs://bucket/
  ├── 2026-06-10-.../              (final target dir)
  └── ._staging_2026-06-10-.../    (staging dir, sibling)
```

The suffix includes the BUILD_ID to tie it to a specific build run, and a
random component to avoid collisions during concurrent development runs.

If `BUILD_ID` is not set (local/testing), a random short suffix is used alone.

### pudl.dagster.assets.deploy.ferceqr.\_deploy_to_staging(ferceqr_deployment: [pudl.dagster.resources.FercEqrDeploymentResource](../../../resources/index.md#pudl.dagster.resources.FercEqrDeploymentResource), source_partitions: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], datapackage_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)) → [list](https://docs.python.org/3/library/stdtypes.html#list)[upath.UPath]

Copy EQR outputs to a staging location under each target, return staging paths.

Each table’s Parquet files and the datapackage JSON are written to a temporary
`._staging_{BUILD_ID}_{random}` subdirectory beneath the real deployment target.
This ensures that a timeout or crash during copying never leaves the final target
in a partially-deployed state.

*datapackage_path* is the path to the `ferceqr_parquet_datapackage.json`
file on the local filesystem (written to `pudl_output` by the caller).

Returns the list of staging `UPath` objects so the caller can atomically
promote them via rename.

### pudl.dagster.assets.deploy.ferceqr.\_promote_staging(staging_targets: [list](https://docs.python.org/3/library/stdtypes.html#list)[upath.UPath], resolved_targets: [list](https://docs.python.org/3/library/stdtypes.html#list)[upath.UPath]) → [None](https://docs.python.org/3/library/constants.html#None)

Atomically promote staging directories to their final destination paths.

For each `(staging_dir, final_dir)` pair, this moves the contents of the staged
table directories and the datapackage JSON into the final target directory. On GCS
and S3 `UPath.rename()` performs a server-side copy followed by deletion of the
original, so the metadata (owner, timestamps, storage class) is preserved and no
data re-upload occurs. On local filesystems the rename is a fast inode-level
operation.

### pudl.dagster.assets.deploy.ferceqr.\_remove_staging(staging_dir: upath.UPath) → [None](https://docs.python.org/3/library/constants.html#None)

Remove a staging directory and all its contents.

Used for cleanup if the promotion step fails — the partial staging data is
discarded rather than leaked. Safe to call on directories that do not exist
(e.g. if promotion already removed them before the failure occurred).

Uses `fs.rm(path, recursive=True)` instead of `rmdir()` because cloud
storage (GCS, S3) uses virtual prefixes rather than real directories and
`rmdir()` would raise `NotADirectoryError`.

### pudl.dagster.assets.deploy.ferceqr.\_parse_step_key(step_key: [str](https://docs.python.org/3/library/stdtypes.html#str), source_partition: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)) → [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]

Extract asset and partition from a step key like `asset_name[partition]`.

FERC EQR transform steps always follow the `asset_name[partition]` format, so the
regex match is expected to succeed for all relevant steps. The fallback path is a
safety net for unexpected non-bracketed step keys (e.g. system steps). In that case,
the raw step key is used as the asset label so the status table remains
comprehensible, and `source_partition or "UNKNOWN"` avoids `None` as a dict key.

### pudl.dagster.assets.deploy.ferceqr.\_validate_partitions(raw: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]

Validate and parse the source partitions JSON from run tags.

Also verifies that each partition is one of the allowed working partitions
defined for the `ferceqr` data source in `pudl.metadata.sources`.

### pudl.dagster.assets.deploy.ferceqr.\_markdown_step_status_table(asset_partition_statuses: [StepStatusTable](#pudl.dagster.assets.deploy.ferceqr.StepStatusTable), partitions: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)] | [None](https://docs.python.org/3/library/constants.html#None)) → [str](https://docs.python.org/3/library/stdtypes.html#str)

Format terminal step statuses as an asset-by-partition Markdown table.

### pudl.dagster.assets.deploy.ferceqr.\_gather_step_statuses(context: [dagster.AssetExecutionContext](https://docs.dagster.io/api/dagster/execution/#dagster.AssetExecutionContext), source_run_id: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[StepStatusTable](#pudl.dagster.assets.deploy.ferceqr.StepStatusTable), [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)]

Collect step statuses and total elapsed time across all source runs.

The elapsed time is computed from the earliest `start_time` to the latest
`end_time` across all source runs (backfill or single). Returns
`(statuses, formatted_duration)`.

### pudl.dagster.assets.deploy.ferceqr.\_markdown_logfile_list(build_id: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [str](https://docs.python.org/3/library/stdtypes.html#str)

Return pointer to logs to send in Zulip message.

### pudl.dagster.assets.deploy.ferceqr.\_compute_deploy_duration(context: [dagster.AssetExecutionContext](https://docs.dagster.io/api/dagster/execution/#dagster.AssetExecutionContext)) → [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)

Return elapsed time since the current run started, or None on failure.

### pudl.dagster.assets.deploy.ferceqr.build_ferceqr_notification(context: [dagster.AssetExecutionContext](https://docs.dagster.io/api/dagster/execution/#dagster.AssetExecutionContext), outcome: Literal['SUCCESS', 'FAILURE']) → [str](https://docs.python.org/3/library/stdtypes.html#str)

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

### pudl.dagster.assets.deploy.ferceqr.deploy_ferceqr(context: [dagster.AssetExecutionContext](https://docs.dagster.io/api/dagster/execution/#dagster.AssetExecutionContext))

Publish EQR outputs to configured deployment targets.

Uses a staging-then-rename pattern: all files are first uploaded to a
`._staging_{BUILD_ID}_{random}` directory beneath each target, then
atomically moved (server-side on GCS/S3, inode-level locally) to the final
path. This ensures the target is never partially populated — if the upload
is interrupted, the staging directory is simply discarded.

### pudl.dagster.assets.deploy.ferceqr.handle_ferceqr_failure(context: [dagster.AssetExecutionContext](https://docs.dagster.io/api/dagster/execution/#dagster.AssetExecutionContext))

Send notification if the FERC EQR build failed.
