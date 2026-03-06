# Distribution Logic Refactor Design

## Overview

Refactor distribution logic from `docker/gcp_pudl_etl.sh` into testable Python code. Enable independent distribution runs without full ETL execution, and provide safe testing via staging mode.

## Goals

1. **Testability** - Test distribution logic without running full ETL
2. **Reusability** - Distribute already-completed builds independently
3. **Team familiarity** - Python instead of bash
4. **Safety** - Staging mode for validating changes before production use

## Current State

Distribution logic lives in `docker/gcp_pudl_etl.sh` (lines 87-324):
- Three build types: `nightly`, `stable`, `workflow_dispatch`
- Operations: compress SQLite, create parquet zip, upload to GCS/S3, git branch updates, Zenodo releases, Cloud Run deployments
- Tightly coupled with ETL execution
- No way to test distribution independently

Existing patterns in codebase:
- `src/pudl/etl/ferceqr_deployment.py` already uses `gcsfs` and `s3fs` for cloud uploads
- `src/pudl/workspace/resource_cache.py` provides `UPath` abstraction for multi-backend storage

## Design

### Core Module: `src/pudl/etl/distribute_outputs.py`

#### Main Entry Points

```python
def distribute_nightly(
    source_path: str | Path,
    git_tag: str,
    staging: bool = False
) -> None:
    """Distribute nightly build outputs.

    Args:
        source_path: Local path or GCS path (gs://...) to build outputs
        git_tag: Nightly tag to merge into nightly branch (e.g., nightly-2025-02-05)
        staging: If True, upload to staging/ locations and skip git/Zenodo/Cloud Run
    """
```

```python
def distribute_stable(
    source_path: str | Path,
    git_tag: str,
    staging: bool = False
) -> None:
    """Distribute stable release outputs.

    Args:
        source_path: Local path or GCS path (gs://...) to build outputs
        git_tag: Version tag to merge into stable branch (e.g., v2025.2.3)
        staging: If True, upload to staging/ locations and skip git/Zenodo/GCS hold
    """
```

**Note:** Ad-hoc builds are for internal testing only and are not distributed to public buckets.

#### Helper Functions

**File preparation (always requires local filesystem):**
- `prepare_compressed_outputs(output_dir)` - Compress SQLite databases and create parquet archive
  - Zip SQLite files using parallel compression
  - Bundle parquet files (no compression, already compressed)
- `reorganize_outputs(output_dir)` - Reorganize files and remove unwanted outputs
  - Move individual parquet files to output root
  - Remove test DBs, raw parquet files, temp directories

**Source handling:**
- If source is GCS: download to temp dir, prep locally, upload from temp
- If source is local: prep in place, upload from source dir

**Distribution:**
- `remove_old_outputs(dist_path, gcs_fs, s3_fs)` - Remove existing files from distribution paths
- `upload_outputs(source_path, dist_paths)` - Upload to GCS and S3
  - Uses `gcsfs.GCSFileSystem` and `s3fs.S3FileSystem`
- `set_gcs_temporary_hold(gcs_path)` - Protect versioned releases from deletion

**Git operations:**
- `update_git_branch(tag, branch)` - Merge tag into branch and push

**External services:**
- `trigger_zenodo_release(env, source_dir, ignore_regex, publish)` - Trigger GHA workflow
- `update_cloud_run_service(service_name)` - Deploy pudl-viewer service

### CLI Script: `src/pudl/etl/cli/distribute.py`

```bash
pudl_distribute <deploy_type> <source_path> --git-tag <tag> [--staging]

Arguments:
  deploy_type: nightly | stable
  source_path: Local path or GCS path (gs://...)

Options:
  --git-tag: Git tag to merge (required)
  --staging: Upload to staging/ locations for validation (skips git/Zenodo/Cloud Run)
```

**Examples:**
```bash
# Production: Distribute completed nightly build
pudl_distribute nightly gs://builds.catalyst.coop/2025-02-05-1234-abc123-main \
  --git-tag nightly-2025-02-05

# Production: Distribute stable release
pudl_distribute stable gs://builds.catalyst.coop/2025-02-05-1234-abc123-v2025.2.3 \
  --git-tag v2025.2.3

# Staging: Test distribution changes before merging to main
pudl_distribute nightly gs://builds.catalyst.coop/2025-02-05-1234-abc123-main \
  --git-tag nightly-2025-02-05 \
  --staging

# Verify staging outputs
gsutil ls -lh gs://pudl.catalyst.coop/staging/nightly/
gsutil ls -lh gs://pudl.catalyst.coop/staging/eel-hole/

# Clean up staging after validation
gsutil -m rm -r gs://pudl.catalyst.coop/staging/**
```

### Staging Mode Behavior

| Operation | Production Mode | Staging Mode |
|-----------|-----------------|--------------|
| File preparation (compress, reorganize) | Execute | Execute |
| Upload to GCS/S3 | Upload to production paths | Upload to `staging/` prefix |
| Git operations (merge, push) | Execute | Skip |
| GCS temporary hold | Execute (stable only) | Skip |
| Zenodo trigger | Execute | Skip |
| Cloud Run deployment | Execute (nightly only) | Skip |

**Purpose:**
- Validate distribution code changes before merging to main
- Verify actual uploaded file contents and structure
- Test full upload workflow (auth, paths, permissions)
- Safe environment to test changes before production use

**Workflow:**
1. Make changes to distribution code on a branch
2. Run distribution with `--staging` flag
3. Inspect outputs in staging locations to verify correctness
4. If good, merge PR and use for production releases
5. Clean up staging area after validation

### Distribution Paths by Deploy Type

**Nightly (Production):**
- Upload to: `gs://pudl.catalyst.coop/nightly/`, `s3://pudl.catalyst.coop/nightly/`
- Upload to: `gs://pudl.catalyst.coop/eel-hole/`, `s3://pudl.catalyst.coop/eel-hole/`
- Git: Merge `nightly-YYYY-MM-DD` tag into `nightly` branch
- Zenodo: Trigger sandbox environment with `publish=true`
- Cloud Run: Update `pudl-viewer` service

**Nightly (Staging):**
- Upload to: `gs://pudl.catalyst.coop/staging/nightly/`, `s3://pudl.catalyst.coop/staging/nightly/`
- Upload to: `gs://pudl.catalyst.coop/staging/eel-hole/`, `s3://pudl.catalyst.coop/staging/eel-hole/`
- Git: Skip
- Zenodo: Skip
- Cloud Run: Skip

**Stable (Production):**
- Upload to: `gs://pudl.catalyst.coop/v2025.2.3/`, `s3://pudl.catalyst.coop/v2025.2.3/`
- Upload to: `gs://pudl.catalyst.coop/stable/`, `s3://pudl.catalyst.coop/stable/`
- Git: Merge version tag into `stable` branch
- GCS: Set temporary hold on versioned path
- Zenodo: Trigger production environment with `publish=false`
- Cloud Run: Skip

**Stable (Staging):**
- Upload to: `gs://pudl.catalyst.coop/staging/v2025.2.3/`, `s3://pudl.catalyst.coop/staging/v2025.2.3/`
- Upload to: `gs://pudl.catalyst.coop/staging/stable/`, `s3://pudl.catalyst.coop/staging/stable/`
- Git: Skip
- GCS temporary hold: Skip
- Zenodo: Skip
- Cloud Run: Skip

### GitHub Actions Workflow: `.github/workflows/distribute-outputs.yml`

```yaml
name: distribute-outputs
on:
  workflow_dispatch:
    inputs:
      deploy_type:
        description: 'Deployment type'
        required: true
        type: choice
        options: [nightly, stable]
      source_path:
        description: 'GCS path to build outputs (e.g., gs://builds.catalyst.coop/...)'
        required: true
      git_tag:
        description: 'Git tag to merge (required)'
        required: true
      staging:
        description: 'Upload to staging/ locations for validation (skips git/Zenodo/Cloud Run)'
        type: boolean
        default: false
```

**Workflow jobs:**
1. Authenticate to GCP (workload identity) and AWS (secrets)
2. Install pudl environment via pixi
3. Run `pudl_distribute` with provided parameters
4. Post status to Slack

**Evolution plan:**
- **Phase 1** (now): Create Python script + manual GHA workflow
- **Phase 2** (later): Modify `build-deploy-pudl.yml` to trigger `distribute-outputs.yml` on ETL success
- **Phase 3** (eventually): Remove distribution logic from `gcp_pudl_etl.sh`

## Testing Strategy

### Unit Tests (`test/unit/etl/test_distribute_outputs.py`)

- Mock filesystem operations (pytest with `tmp_path` fixture)
- Test helper functions in isolation
- Verify path construction logic for production and staging modes
- Test source path handling (local vs GCS)

### Integration Tests (manual)

**1. Local outputs with staging:**
```bash
pudl_distribute nightly ./test_output --git-tag test-tag --staging
```
Verifies: file preparation, upload to staging locations

**2. GCS build with staging:**
```bash
pudl_distribute nightly gs://builds.catalyst.coop/2025-02-05-1234-abc123-main \
  --git-tag nightly-2025-02-05 --staging
```
Verifies: GCS source handling, download, prep, upload to staging

**3. Inspect staging outputs:**
```bash
gsutil ls -lh gs://pudl.catalyst.coop/staging/nightly/
gsutil ls -lh gs://pudl.catalyst.coop/staging/eel-hole/
# Download and verify file contents/structure
```

**4. Manual GHA trigger with staging:**
- Trigger `distribute-outputs.yml` with `staging=true`
- Verify outputs in staging locations
- Review workflow logs

**5. Production distribution test:**
- After staging validation passes, run without `--staging`
- Verify outputs in production locations
- Verify git/Zenodo/Cloud Run operations complete

## Implementation Plan

1. **Create `src/pudl/etl/distribute_outputs.py`**
   - Implement helper functions
   - Implement two main entry points (nightly, stable)
   - Add comprehensive docstrings

2. **Create `src/pudl/etl/cli/distribute.py`**
   - CLI using typer or click
   - Argument validation
   - Clear help text

3. **Create unit tests**
   - Test all helper functions
   - Mock filesystem and git operations
   - Verify staging mode behavior

4. **Create GHA workflow**
   - Manual trigger with workflow_dispatch
   - Authenticate to GCP/AWS
   - Run pudl_distribute
   - Post to Slack

5. **Integration testing**
   - Test with staging on real builds
   - Verify all operations work correctly
   - Document any edge cases

6. **Update bash script (optional)**
   - Call Python script from bash as intermediate step
   - Or leave bash as-is until ready to fully deprecate

## Open Questions

None - design validated through brainstorming session.

## References

- Issue #3207: Previous work on nightly/stable/ad-hoc ETL builds
- `src/pudl/etl/ferceqr_deployment.py`: Existing pattern for cloud distribution
- `docker/gcp_pudl_etl.sh`: Current distribution logic
