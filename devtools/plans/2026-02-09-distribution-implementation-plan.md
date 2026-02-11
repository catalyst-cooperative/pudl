# Distribution Logic Refactor Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Extract distribution logic from bash script into testable Python code with CLI and GitHub Actions workflow.

**Architecture:** Python module with filesystem operations (gcsfs/s3fs), git operations (subprocess), external service triggers (GitHub CLI), Click-based CLI, and manual GitHub Actions workflow. Staging mode for safe testing. File preparation already complete.

**Tech Stack:** gcsfs, s3fs, fsspec, Click, subprocess (git), GitHub CLI (gh), GitHub Actions

**Status:** File preparation functions (`prepare_outputs_for_distribution`) and tests are complete. This plan covers distribution, git, external services, CLI, and GitHub Actions workflow.

---

## Task 1: Upload Helper Function

**Files:**
- Modify: `src/pudl/etl/distribute_outputs.py:73-`
- Test: `test/unit/etl/test_distribute_outputs.py:48-`

**Step 1: Write test for upload logic (mocked)**

Add to `test/unit/etl/test_distribute_outputs.py`:

```python
from unittest.mock import MagicMock, patch
from pudl.etl.distribute_outputs import upload_outputs


def test_upload_outputs_nightly(tmp_path):
    """Test upload to nightly paths (production and staging)."""
    source_dir = tmp_path / "output"
    source_dir.mkdir()
    (source_dir / "pudl.sqlite.zip").write_text("db")
    (source_dir / "table1.parquet").write_text("p1")

    path_suffixes = ["nightly", "eel-hole"]

    with patch("pudl.etl.distribute_outputs.gcsfs.GCSFileSystem") as mock_gcs_cls, \
         patch("pudl.etl.distribute_outputs.s3fs.S3FileSystem") as mock_s3_cls:

        mock_gcs = MagicMock()
        mock_s3 = MagicMock()
        mock_gcs_cls.return_value = mock_gcs
        mock_s3_cls.return_value = mock_s3

        upload_outputs(source_dir, path_suffixes, staging=False)

        # Verify GCS put called for each path suffix
        assert mock_gcs.put.call_count == 2
        # Verify S3 put called for each path suffix
        assert mock_s3.put.call_count == 2


def test_upload_outputs_staging(tmp_path):
    """Test upload to staging paths."""
    source_dir = tmp_path / "output"
    source_dir.mkdir()

    path_suffixes = ["nightly"]

    with patch("pudl.etl.distribute_outputs.gcsfs.GCSFileSystem") as mock_gcs_cls, \
         patch("pudl.etl.distribute_outputs.s3fs.S3FileSystem") as mock_s3_cls:

        mock_gcs = MagicMock()
        mock_s3 = MagicMock()
        mock_gcs_cls.return_value = mock_gcs
        mock_s3_cls.return_value = mock_s3

        upload_outputs(source_dir, path_suffixes, staging=True)

        # Check that staging/ prefix is used in the path
        gcs_call_args = mock_gcs.put.call_args_list[0][0]
        assert "staging/nightly/" in gcs_call_args[1]
```

**Step 2: Run test to verify it fails**

Run: `pytest test/unit/etl/test_distribute_outputs.py::test_upload_outputs -v`

Expected: FAIL with "ImportError: cannot import name 'upload_outputs'"

**Step 3: Implement upload function**

Add to `src/pudl/etl/distribute_outputs.py`:

```python
import gcsfs
import s3fs


def upload_outputs(
    source_dir: Path,
    path_suffixes: list[str],
    staging: bool = False,
) -> None:
    """Upload outputs to cloud storage paths.

    Uploads all files from source directory to GCS and S3 using the provided path
    suffixes. Each suffix is uploaded to both gs://pudl.catalyst.coop/{suffix}/ and
    s3://pudl.catalyst.coop/{suffix}/. In staging mode, "staging/" prefix is added.

    Args:
        source_dir: Local directory containing prepared outputs to upload.
        path_suffixes: Path suffixes to upload to (e.g., ["nightly", "eel-hole"]).
        staging: If True, prepend "staging/" to all paths for testing.
    """
    source_dir = Path(source_dir)

    # Create filesystem instances
    gcs_fs = gcsfs.GCSFileSystem()
    s3_fs = s3fs.S3FileSystem()

    # Build base paths
    gcs_base = "gs://pudl.catalyst.coop"
    s3_base = "s3://pudl.catalyst.coop"

    if staging:
        gcs_base = f"{gcs_base}/staging"
        s3_base = f"{s3_base}/staging"

    # Upload to each path suffix
    for suffix in path_suffixes:
        gcs_path = f"{gcs_base}/{suffix}/"
        s3_path = f"{s3_base}/{suffix}/"

        logger.info(f"Uploading outputs to {gcs_path}")
        gcs_fs.put(f"{source_dir}/*", gcs_path, recursive=True)

        logger.info(f"Uploading outputs to {s3_path}")
        s3_fs.put(f"{source_dir}/*", s3_path, recursive=True)

    logger.info(f"Upload complete for {len(path_suffixes)} path(s)")
```

**Step 4: Run test to verify it passes**

Run: `pytest test/unit/etl/test_distribute_outputs.py::test_upload_outputs -v`

Expected: 2 tests PASS

**Step 5: Commit**

```bash
git add test/unit/etl/test_distribute_outputs.py src/pudl/etl/distribute_outputs.py
git commit -m "feat: add cloud storage upload function"
```

---

## Task 2: Git Operations Helper Function

**Files:**
- Modify: `src/pudl/etl/distribute_outputs.py:~130-`
- Test: `test/unit/etl/test_distribute_outputs.py:~100-`

**Step 1: Write test for git operations (mocked)**

Add to `test/unit/etl/test_distribute_outputs.py`:

```python
from pudl.etl.distribute_outputs import update_git_branch


def test_update_git_branch():
    """Test git branch update merges tag and pushes."""
    with patch("pudl.etl.distribute_outputs.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0)

        update_git_branch(tag="nightly-2025-02-05", branch="nightly")

        # Verify git commands called in order
        calls = mock_run.call_args_list
        assert len(calls) == 3

        # Check checkout
        assert "checkout" in " ".join(calls[0][0][0])
        assert "nightly" in " ".join(calls[0][0][0])

        # Check merge
        assert "merge" in " ".join(calls[1][0][0])
        assert "nightly-2025-02-05" in " ".join(calls[1][0][0])

        # Check push
        assert "push" in " ".join(calls[2][0][0])
```

**Step 2: Run test to verify it fails**

Run: `pytest test/unit/etl/test_distribute_outputs.py::test_update_git_branch -v`

Expected: FAIL with "ImportError: cannot import name 'update_git_branch'"

**Step 3: Implement git operations function**

Add to `src/pudl/etl/distribute_outputs.py`:

```python
import subprocess


def update_git_branch(tag: str, branch: str) -> None:
    """Merge git tag into branch and push to origin.

    Performs fast-forward merge of a tag into a branch and pushes the result.
    This updates the nightly or stable branch to point to the tagged release.

    Args:
        tag: Git tag to merge (e.g., "nightly-2025-02-05" or "v2025.2.3").
        branch: Target branch to update (e.g., "nightly" or "stable").

    Raises:
        subprocess.CalledProcessError: If git commands fail.
    """
    logger.info(f"Updating git branch {branch} to tag {tag}")

    # Checkout target branch
    subprocess.run(
        ["git", "checkout", branch],
        check=True,
        capture_output=True,
        text=True,
    )

    # Merge tag (fast-forward only)
    subprocess.run(
        ["git", "merge", "--ff-only", tag],
        check=True,
        capture_output=True,
        text=True,
    )

    # Push to origin
    subprocess.run(
        ["git", "push", "-u", "origin", branch],
        check=True,
        capture_output=True,
        text=True,
    )

    logger.info(f"Git branch {branch} updated successfully")
```

**Step 4: Run test to verify it passes**

Run: `pytest test/unit/etl/test_distribute_outputs.py::test_update_git_branch -v`

Expected: PASS

**Step 5: Commit**

```bash
git add test/unit/etl/test_distribute_outputs.py src/pudl/etl/distribute_outputs.py
git commit -m "feat: add git branch update function"
```

---

## Task 3: External Service Helper Functions

**Files:**
- Modify: `src/pudl/etl/distribute_outputs.py:~180-`
- Test: `test/unit/etl/test_distribute_outputs.py:~130-`

**Step 1: Write tests for external service triggers**

Add to `test/unit/etl/test_distribute_outputs.py`:

```python
from pudl.etl.distribute_outputs import (
    trigger_zenodo_release,
    update_cloud_run_service,
    set_gcs_temporary_hold,
)


def test_trigger_zenodo_release():
    """Test Zenodo release workflow trigger."""
    with patch("pudl.etl.distribute_outputs.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0)

        trigger_zenodo_release(
            env="sandbox",
            source_dir="s3://pudl.catalyst.coop/nightly/",
            ignore_regex=r"(^.*\.parquet$|^pudl_parquet_datapackage\.json$)",
            publish=True,
        )

        # Verify gh workflow dispatch called
        calls = mock_run.call_args_list
        assert len(calls) == 1
        cmd = " ".join(calls[0][0][0])
        assert "gh workflow run" in cmd
        assert "zenodo-data-release.yml" in cmd
        assert "sandbox" in cmd
        assert "publish" in cmd


def test_update_cloud_run_service():
    """Test Cloud Run service update."""
    with patch("pudl.etl.distribute_outputs.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0)

        update_cloud_run_service("pudl-viewer")

        # Verify gcloud run services update called
        calls = mock_run.call_args_list
        assert len(calls) == 1
        cmd = " ".join(calls[0][0][0])
        assert "gcloud run services update" in cmd
        assert "pudl-viewer" in cmd


def test_set_gcs_temporary_hold():
    """Test GCS temporary hold for versioned releases."""
    with patch("pudl.etl.distribute_outputs.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0)

        set_gcs_temporary_hold("gs://pudl.catalyst.coop/v2025.2.3/")

        # Verify gcloud storage objects update called
        calls = mock_run.call_args_list
        assert len(calls) == 1
        cmd = " ".join(calls[0][0][0])
        assert "gcloud storage" in cmd
        assert "temporary-hold" in cmd
        assert "v2025.2.3" in cmd
```

**Step 2: Run tests to verify they fail**

Run: `pytest test/unit/etl/test_distribute_outputs.py -k "zenodo_release or cloud_run or temporary_hold" -v`

Expected: FAIL with "ImportError: cannot import name ..."

**Step 3: Implement external service functions**

Add to `src/pudl/etl/distribute_outputs.py`:

```python
def trigger_zenodo_release(
    env: str,
    source_dir: str,
    ignore_regex: str,
    publish: bool,
) -> None:
    """Trigger Zenodo data release GitHub Actions workflow.

    Dispatches the zenodo-data-release workflow to create or update a Zenodo
    deposition with PUDL data outputs.

    Args:
        env: Zenodo environment - "sandbox" or "production".
        source_dir: Cloud storage path to source data (e.g., "s3://pudl.catalyst.coop/nightly/").
        ignore_regex: Regex pattern for files to exclude from upload.
        publish: If True, automatically publish the Zenodo record.
    """
    logger.info(f"Triggering Zenodo release: env={env}, publish={publish}")

    publish_flag = "publish" if publish else "no-publish"

    subprocess.run(
        [
            "gh",
            "workflow",
            "run",
            "zenodo-data-release.yml",
            "-f",
            f"env={env}",
            "-f",
            f"source_dir={source_dir}",
            "-f",
            f"ignore_regex={ignore_regex}",
            "-f",
            f"publish={publish_flag}",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    logger.info("Zenodo release workflow triggered")


def update_cloud_run_service(service_name: str) -> None:
    """Update Cloud Run service to latest image.

    Triggers redeployment of a Cloud Run service to pull and use the latest
    container image.

    Args:
        service_name: Name of the Cloud Run service (e.g., "pudl-viewer").
    """
    logger.info(f"Updating Cloud Run service: {service_name}")

    subprocess.run(
        [
            "gcloud",
            "run",
            "services",
            "update",
            service_name,
            "--image",
            f"us-east1-docker.pkg.dev/catalyst-cooperative-pudl/{service_name}/{service_name}:latest",
            "--region",
            "us-east1",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    logger.info(f"Cloud Run service {service_name} updated")


def set_gcs_temporary_hold(gcs_path: str) -> None:
    """Set temporary hold on GCS objects to prevent deletion.

    Applies a temporary hold to protect versioned release artifacts from
    accidental deletion or lifecycle policies.

    Args:
        gcs_path: GCS path to objects (e.g., "gs://pudl.catalyst.coop/v2025.2.3/").
    """
    logger.info(f"Setting temporary hold on {gcs_path}")

    subprocess.run(
        [
            "gcloud",
            "storage",
            "objects",
            "update",
            f"{gcs_path}*",
            "--temporary-hold",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    logger.info(f"Temporary hold set on {gcs_path}")
```

**Step 4: Run tests to verify they pass**

Run: `pytest test/unit/etl/test_distribute_outputs.py -k "zenodo_release or cloud_run or temporary_hold" -v`

Expected: 3 tests PASS

**Step 5: Commit**

```bash
git add test/unit/etl/test_distribute_outputs.py src/pudl/etl/distribute_outputs.py
git commit -m "feat: add external service helper functions"
```

---

## Task 4: CLI Script with Orchestration Logic

**Files:**
- Create: `src/pudl/scripts/distribute.py`
- Modify: `pyproject.toml` (add console script entry point)

**Step 1: Create CLI script with orchestration logic**

Create `src/pudl/scripts/distribute.py`:

```python
"""CLI for distributing PUDL ETL outputs to cloud storage."""

import sys
from pathlib import Path

import click
import coloredlogs

from pudl.etl.distribute_outputs import (
    prepare_outputs_for_distribution,
    upload_outputs,
    update_git_branch,
    trigger_zenodo_release,
    update_cloud_run_service,
    set_gcs_temporary_hold,
)
from pudl.logging_helpers import get_logger

logger = get_logger(__name__)


@click.command()
@click.argument(
    "deploy_type",
    type=click.Choice(["nightly", "stable"], case_sensitive=False),
)
@click.argument(
    "source_path",
    type=click.Path(exists=False, path_type=Path),
)
@click.option(
    "--git-tag",
    required=True,
    help="Git tag to merge (e.g., nightly-2025-02-05 or v2025.2.3)",
)
@click.option(
    "--staging",
    is_flag=True,
    default=False,
    help="Upload to staging/ locations for validation (skips git/Zenodo/Cloud Run)",
)
@click.option(
    "--loglevel",
    default="INFO",
    type=click.Choice(
        ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        case_sensitive=False,
    ),
    help="Set logging level",
)
def pudl_distribute(
    deploy_type: str,
    source_path: Path,
    git_tag: str,
    staging: bool,
    loglevel: str,
) -> None:
    """Distribute PUDL ETL outputs to cloud storage.

    Uploads prepared ETL outputs to GCS and S3, updates git branches, triggers
    Zenodo releases, and updates Cloud Run services based on deployment type.

    DEPLOY_TYPE: Either 'nightly' or 'stable'

    SOURCE_PATH: Local path or GCS path (gs://...) to build outputs

    Examples:

      # Production: Distribute completed nightly build
      pudl_distribute nightly gs://builds.catalyst.coop/2025-02-05-1234-abc123-main \\
        --git-tag nightly-2025-02-05

      # Staging: Test distribution changes before merging to main
      pudl_distribute nightly gs://builds.catalyst.coop/2025-02-05-1234-abc123-main \\
        --git-tag nightly-2025-02-05 --staging

      # Production: Distribute stable release
      pudl_distribute stable gs://builds.catalyst.coop/2025-02-05-1234-abc123-v2025.2.3 \\
        --git-tag v2025.2.3
    """
    # Configure logging
    coloredlogs.install(level=loglevel, logger=logger)

    logger.info(f"PUDL Distribution: {deploy_type} (staging={staging})")
    logger.info(f"Source: {source_path}")
    logger.info(f"Git tag: {git_tag}")

    try:
        # Configure based on deploy type
        if deploy_type == "nightly":
            path_suffixes = ["nightly", "eel-hole"]
            branch = "nightly"
            zenodo_env = "sandbox"
            zenodo_source = "s3://pudl.catalyst.coop/nightly/"
            zenodo_publish = True
            update_cloudrun = True
            set_hold = False
        else:  # stable
            path_suffixes = [git_tag, "stable"]
            branch = "stable"
            zenodo_env = "production"
            zenodo_source = f"s3://pudl.catalyst.coop/{git_tag}/"
            zenodo_publish = False
            update_cloudrun = False
            set_hold = True

        # Step 1: Prepare outputs
        prepare_outputs_for_distribution(source_path)

        # Step 2: Upload to cloud storage
        upload_outputs(
            source_dir=source_path,
            path_suffixes=path_suffixes,
            staging=staging,
        )

        # Skip external operations in staging mode
        if staging:
            logger.info("Staging mode: skipping git/Zenodo/Cloud Run/GCS hold operations")
            logger.info("Distribution complete!")
            return

        # Step 3: Update git branch
        update_git_branch(tag=git_tag, branch=branch)

        # Step 4: Set GCS temporary hold (stable only)
        if set_hold:
            hold_path = f"gs://pudl.catalyst.coop/{git_tag}/"
            set_gcs_temporary_hold(hold_path)

        # Step 5: Trigger Zenodo release
        trigger_zenodo_release(
            env=zenodo_env,
            source_dir=zenodo_source,
            ignore_regex=r"(^.*\.parquet$|^pudl_parquet_datapackage\.json$)",
            publish=zenodo_publish,
        )

        # Step 6: Update Cloud Run service (nightly only)
        if update_cloudrun:
            update_cloud_run_service("pudl-viewer")

        logger.info("Distribution complete!")

    except Exception as e:
        logger.error(f"Distribution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    pudl_distribute()
```

**Step 2: Add console script entry point**

Edit `pyproject.toml`, add to `[project.scripts]`:

```toml
pudl_distribute = "pudl.scripts.distribute:pudl_distribute"
```

**Step 3: Test CLI help text**

Run: `pixi run pudl_distribute --help`

Expected: Help text displays with usage, arguments, and options

**Step 4: Commit**

```bash
git add src/pudl/scripts/distribute.py pyproject.toml
git commit -m "feat: add CLI for distribution"
```

---

## Task 5: GitHub Actions Workflow

**Files:**
- Create: `.github/workflows/deploy-pudl.yml`

**Step 1: Create GHA workflow file**

Create `.github/workflows/deploy-pudl.yml`:

```yaml
---
name: deploy-pudl

on:
  workflow_dispatch:
    inputs:
      deploy_type:
        description: 'Deployment type'
        required: true
        type: choice
        options:
          - nightly
          - stable
      source_path:
        description: 'GCS path to build outputs (e.g., gs://builds.catalyst.coop/...)'
        required: true
        type: string
      git_tag:
        description: 'Git tag to merge (required)'
        required: true
        type: string
      staging:
        description: 'Upload to staging/ locations for validation (skips git/Zenodo/Cloud Run)'
        type: boolean
        default: false

jobs:
  deploy-pudl:
    runs-on: ubuntu-latest
    permissions:
      contents: write
      id-token: write
    defaults:
      run:
        shell: bash -l {0}
    steps:
      - name: Checkout Repository
        uses: actions/checkout@v6
        with:
          fetch-depth: 0

      - name: Authenticate to Google Cloud
        uses: google-github-actions/auth@v3
        with:
          workload_identity_provider: ${{ secrets.GCP_WORKLOAD_IDENTITY_PROVIDER }}
          service_account: ${{ secrets.GCP_SERVICE_ACCOUNT }}

      - name: Set up Cloud SDK
        uses: google-github-actions/setup-gcloud@v3

      - name: Authenticate to AWS
        uses: aws-actions/configure-aws-credentials@v5
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: us-west-2

      - name: Install pudl environment using pixi
        uses: prefix-dev/setup-pixi@v0.9.4
        with:
          locked: true
          cache: true
          cache-write: ${{ github.event_name == 'push' && github.ref_name == 'main' }}

      - name: Distribute PUDL outputs
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        run: |
          pixi run pudl_distribute \
            ${{ inputs.deploy_type }} \
            "${{ inputs.source_path }}" \
            --git-tag "${{ inputs.git_tag }}" \
            ${{ inputs.staging && '--staging' || '' }}

      - name: Post status to pudl-deployments channel
        if: always()
        uses: slackapi/slack-github-action@v2
        with:
          method: chat.postMessage
          token: ${{ secrets.PUDL_DEPLOY_SLACK_TOKEN }}
          payload: |
            {
              "text": "${{ job.status == 'success' && ':white_check_mark:' || ':x:' }} deploy-pudl (`${{ inputs.deploy_type }}`, staging=${{ inputs.staging }}) finished with status: ${{ job.status }}. Logs: https://github.com/${{ github.repository }}/actions/runs/${{ github.run_id }}",
              "channel": "C03FHB9N0PQ"
            }
```

**Step 2: Validate workflow syntax**

Run: `gh workflow view deploy-pudl.yml 2>/dev/null || echo "Workflow file created, will validate on push"`

Expected: Either workflow details or message that it will validate on push

**Step 3: Commit**

```bash
git add .github/workflows/deploy-pudl.yml
git commit -m "feat: add GitHub Actions workflow for distribution"
```

---

## Task 6: Integration Documentation

**Files:**
- Create: `devtools/plans/2026-02-09-distribution-integration-testing.md`

**Step 1: Write integration testing guide**

Create `devtools/plans/2026-02-09-distribution-integration-testing.md`:

```markdown
# Distribution Integration Testing Guide

This document describes how to test the new distribution system before using it in production.

## Prerequisites

- Access to GCS bucket `gs://pudl.catalyst.coop/`
- Access to S3 bucket `s3://pudl.catalyst.coop/`
- AWS credentials configured
- GCP credentials configured
- GitHub CLI (`gh`) authenticated
- Git configured with push access to pudl repo

## Testing Workflow

### 1. Staging Mode Test (Recommended First Step)

Test distribution logic without affecting production paths or triggering external services.

```bash
# Use an existing build output
BUILD_PATH="gs://builds.catalyst.coop/2025-02-05-1234-abc123-main"

# Run in staging mode
pixi run pudl_distribute nightly "$BUILD_PATH" \
  --git-tag nightly-2025-02-05 \
  --staging
```

**What happens:**
- Files prepared and uploaded to `gs://pudl.catalyst.coop/staging/nightly/` and `staging/eel-hole/`
- Git operations skipped
- Zenodo trigger skipped
- Cloud Run update skipped

**Verification:**
```bash
# Inspect staging outputs
gsutil ls -lh gs://pudl.catalyst.coop/staging/nightly/
gsutil ls -lh gs://pudl.catalyst.coop/staging/eel-hole/

# Download and verify file contents
gsutil cp gs://pudl.catalyst.coop/staging/nightly/pudl.sqlite.zip ./test-pudl.sqlite.zip
unzip -l test-pudl.sqlite.zip

# Clean up staging area when done
gsutil -m rm -r gs://pudl.catalyst.coop/staging/**
```

### 2. GitHub Actions Staging Test

Test the full GHA workflow in staging mode.

1. Go to https://github.com/catalyst-cooperative/pudl/actions/workflows/deploy-pudl.yml
2. Click "Run workflow"
3. Fill in:
   - Deploy type: `nightly`
   - Source path: `gs://builds.catalyst.coop/2025-02-05-1234-abc123-main`
   - Git tag: `nightly-2025-02-05`
   - Staging: ✅ (checked)
4. Click "Run workflow"
5. Monitor logs and verify staging outputs as above

### 3. Production Test (After Staging Validation)

Once staging tests pass, run a production distribution.

```bash
# Production nightly distribution
pixi run pudl_distribute nightly "$BUILD_PATH" \
  --git-tag nightly-2025-02-05

# Production stable distribution
pixi run pudl_distribute stable "$BUILD_PATH" \
  --git-tag v2025.2.3
```

**What happens:**
- Files uploaded to production paths
- Git branch updated
- Zenodo workflow triggered
- Cloud Run service updated (nightly only)
- GCS temporary hold set (stable only)

**Verification:**
```bash
# Check production outputs
gsutil ls -lh gs://pudl.catalyst.coop/nightly/
aws s3 ls s3://pudl.catalyst.coop/nightly/ --human-readable

# Check git branch
git fetch origin
git log origin/nightly -1

# Check Zenodo workflow status
gh run list --workflow zenodo-data-release.yml --limit 5

# Check Cloud Run service (nightly)
gcloud run services describe pudl-viewer --region us-east1
```

## Common Issues

### Authentication Errors

**GCS access denied:**
```bash
gcloud auth application-default login
```

**S3 access denied:**
```bash
aws configure
# Or set environment variables
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
```

### Git Push Fails

Ensure your git config is set up correctly:
```bash
git config --global user.name "Your Name"
git config --global user.email "your.email@catalyst.coop"
```

### GitHub CLI Not Authenticated

```bash
gh auth login
```

## Rollback

If distribution creates incorrect outputs:

1. **Staging mode:** Simply delete staging directory
```bash
gsutil -m rm -r gs://pudl.catalyst.coop/staging/**
```

2. **Production mode:** Contact team lead before attempting rollback
   - Git branches can be reset
   - Cloud storage can be cleaned up
   - Zenodo records cannot be deleted (only unpublished if not yet published)

## Next Steps

After successful integration testing:
1. Update `docker/gcp_pudl_etl.sh` to call `pudl_distribute` instead of inline bash
2. Modify `build-deploy-pudl.yml` to trigger `distribute-outputs.yml` on ETL success
3. Eventually remove distribution logic from bash script entirely
```

**Step 2: Commit**

```bash
git add devtools/plans/2026-02-09-distribution-integration-testing.md
git commit -m "docs: add integration testing guide for distribution"
```

---

## Task 7: Run Full Test Suite

**Files:**
- None (test execution)

**Step 1: Run unit tests**

Run: `pytest test/unit/etl/test_distribute_outputs.py -v`

Expected: All tests PASS

**Step 2: Run linting**

Run: `ruff check src/pudl/etl/distribute_outputs.py src/pudl/scripts/distribute.py`

Expected: No errors

**Step 3: Run formatting check**

Run: `ruff format --check src/pudl/etl/distribute_outputs.py src/pudl/scripts/distribute.py`

Expected: No changes needed (or apply formatting if needed)

**Step 4: Fix any linting/formatting issues**

If issues found:
```bash
ruff check --fix src/pudl/etl/distribute_outputs.py src/pudl/scripts/distribute.py
ruff format src/pudl/etl/distribute_outputs.py src/pudl/scripts/distribute.py
```

**Step 5: Commit any fixes**

```bash
git add src/pudl/etl/distribute_outputs.py src/pudl/scripts/distribute.py
git commit -m "style: fix linting and formatting"
```

---

## Summary

This implementation plan creates:

1. **Upload Helper** - Simple function that takes path suffixes (e.g., `["nightly", "eel-hole"]`) and uploads to both GCS and S3
2. **Git Operations** - Merge tags into branches and push
3. **External Services** - Zenodo workflow trigger, Cloud Run update, GCS hold
4. **CLI Script** - Click-based command-line interface with all orchestration logic (decides which paths, which external services, staging vs production)
5. **GitHub Actions Workflow** - Manual trigger for distribution
6. **Documentation** - Integration testing guide
7. **Test Suite** - Validation and linting

The code is built incrementally with TDD, comprehensive tests, and clear docstrings. Each task is self-contained and can be implemented and tested independently.

**Key design principle:** Simple helper functions in the module, all orchestration logic lives in the CLI script. No intermediate entry point functions needed - the CLI directly configures and calls the helpers based on deploy_type.
