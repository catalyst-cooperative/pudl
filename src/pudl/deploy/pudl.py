"""Distribute PUDL ETL outputs to cloud storage and update git branches.

This module handles distribution of completed ETL builds to public cloud storage
(GCS and S3), git branch updates, Zenodo releases, and Cloud Run deployments.
"""

import logging
import re
import shutil
import subprocess
import zipfile
from datetime import datetime
from enum import Enum
from pathlib import Path

import gcsfs
import requests
import s3fs
from upath import UPath

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DeploymentType(Enum):
    """Deployments can be 'nightly' or 'stable'."""

    NIGHTLY = "nightly"
    STABLE = "stable"


def prepare_outputs_for_distribution(local_path: Path, build_path: UPath) -> None:
    """Prepare ETL outputs for distribution.

    Takes raw ETL output structure and produces distribution-ready outputs:
    - Moves parquet files from parquet/ subdirectory to root
    - Compresses SQLite databases with maximum compression
    - Creates parquet archive (no compression, already compressed)
    - Removes test databases and temporary directories

    In general, we want to know if these files don't exist, so
    FileNotFoundErrors are OK and we don't need to pre-emptively try to avoid
    them.

    Args:
        local_path: Path on local filesystem where we will prep outputs for distribution.
        build_path: Remote path containing raw build outputs.
    """
    # Copy raw build outputs to local path
    local_path = Path(local_path)
    fs = build_path.fs
    fs.get(build_path, str(local_path), recursive=True)

    logger.info(f"Preparing outputs in {local_path} for distribution")
    logger.info(f"Contents: {local_path.glob('*')}")

    # Move files around
    parquet_dir = local_path / "parquet"
    parquet_files = parquet_dir.glob("*.parquet")
    logger.info(f"Found parquets: {parquet_files}")
    for parquet_file in parquet_dir.glob("*.parquet"):
        shutil.move(str(parquet_file), str(local_path / parquet_file.name))

    datapackage = parquet_dir / "pudl_parquet_datapackage.json"
    if datapackage.exists():
        shutil.move(str(datapackage), str(local_path / datapackage.name))

    shutil.rmtree(parquet_dir)

    # Compress SQLite databases
    sqlite_files = list(local_path.glob("*.sqlite"))
    for sqlite_file in sqlite_files:
        zip_path = local_path / f"{sqlite_file.name}.zip"
        with zipfile.ZipFile(
            zip_path, "w", zipfile.ZIP_DEFLATED, compresslevel=9
        ) as zf:
            zf.write(sqlite_file, arcname=sqlite_file.name)
        sqlite_file.unlink()
        logger.info(f"Compressed {sqlite_file.name}")

    # Create parquet archive (store mode, no compression)
    parquet_files = list(local_path.glob("*.parquet"))
    assert len(parquet_files) > 0, f"No parquet files in {local_path}."
    archive_path = local_path / "pudl_parquet.zip"
    with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_STORED) as zf:
        for parquet_file in parquet_files:
            zf.write(parquet_file, arcname=parquet_file.name)

        datapackage = local_path / "pudl_parquet_datapackage.json"
        if datapackage.exists():
            zf.write(datapackage, arcname=datapackage.name)

    logger.info(f"Created parquet archive: {archive_path}")

    logger.info("Removing dbt database.")
    test_db = local_path / "pudl_dbt_tests.duckdb"
    test_db.unlink()

    logger.info("Output preparation complete")


def _run(cmd: list[str]) -> str | None:
    """Wrap subprocess.run so we see error output."""
    return subprocess.run(cmd, check=True, capture_output=True, text=True).stdout  # noqa: S603


def upload_outputs(
    source_dir: Path,
    path_suffixes: list[str],
) -> None:
    """Upload outputs to cloud storage paths.

    Uploads all files from source directory to GCS and S3 using the provided path
    suffixes. Each suffix is uploaded to both gs://pudl.catalyst.coop/{suffix}/ and
    s3://pudl.catalyst.coop/{suffix}/.

    Args:
        source_dir: Local directory containing prepared outputs to upload.
        path_suffixes: Path suffixes to upload to (e.g., ["nightly", "eel-hole"]).
    """
    logger.info("Uploading outputs to cloud storage")

    if not source_dir.exists():
        raise ValueError(f"Source directory does not exist: {source_dir}")
    if not any(source_dir.iterdir()):
        raise ValueError(f"Source directory is empty: {source_dir}")

    # NOTE (2026-02-11): our GCS distribution bucket is requester pays.
    gcs_fs = gcsfs.GCSFileSystem(requester_pays=True)
    s3_fs = s3fs.S3FileSystem()

    # actually upload
    for suffix in path_suffixes:
        gcs_path = f"gs://pudl.catalyst.coop/{suffix}/"
        logger.info(f"Uploading outputs to {gcs_path}")
        gcs_fs.put(f"{source_dir}/*", gcs_path, recursive=True)

        s3_path = f"s3://pudl.catalyst.coop/{suffix}/"
        logger.info(f"Uploading outputs to {s3_path}")
        s3_fs.put(f"{source_dir}/*", s3_path, recursive=True)

    logger.info(f"Upload complete for {len(path_suffixes)} path(s)")


def update_git_branch(tag: str, branch: str, staging: bool) -> None:
    """Merge git tag into branch and push to origin.

    Performs fast-forward merge of a tag into a branch and pushes the result.
    This updates the nightly or stable branch to point to the tagged release.

    In staging, will try the checkout and merge, but skip the git push.

    Args:
        tag: Git tag to merge (e.g., "nightly-2025-02-05" or "v2025.2.3").
        branch: Target branch to update (e.g., "nightly" or "stable").
        staging: True if this is a staging environment.

    Raises:
        subprocess.CalledProcessError: If git commands fail.
    """
    if get_deployment_type_from_tag(tag).value != branch:
        raise RuntimeError(
            f"Git tag, {tag}, does not match deployment branch, {branch}."
        )
    logger.info(f"Updating git branch {branch} to tag {tag}")

    _run(["git", "checkout", branch])
    _run(["git", "merge", "--ff-only", tag])
    if not staging:
        _run(["git", "push", "-u", "origin", branch])

    logger.info(f"Git branch {branch} updated successfully")


def trigger_zenodo_release(
    build_ref: str,
    env: str,
    source_dir: str,
    ignore_regex: str,
    publish: bool,
    token: str,
) -> None:
    """Trigger Zenodo data release GitHub Actions workflow.

    Dispatches the zenodo-data-release workflow to create or update a Zenodo
    deposition with PUDL data outputs.

    Args:
        build_ref: The git reference for the workflow. The reference can be a branch or tag name.
        env: Zenodo environment - "sandbox" or "production".
        source_dir: Cloud storage path to source data (e.g., "s3://pudl.catalyst.coop/nightly/").
        ignore_regex: Regex pattern for files to exclude from upload.
        publish: If True, automatically publish the Zenodo record.
        token: the bearer token to authenticate to GitHub.
    """
    logger.info(f"Triggering Zenodo release: env={env}, publish={publish}")

    if env not in ("sandbox", "production"):
        raise ValueError(
            f"Invalid Zenodo environment: {env}. Must be 'sandbox' or 'production'."
        )

    # convert bool to string option that the workflow takes
    publish_flag = "publish" if publish else "no-publish"

    response = requests.post(
        "https://api.github.com/repos/catalyst-cooperative/pudl/actions/workflows/zenodo-data-release.yml/dispatches",
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
        },
        data={
            "ref": build_ref,
            "inputs": {
                "env": env,
                "source_dir": source_dir,
                "ignore_regex": ignore_regex,
                "publish": publish_flag,
            },
        },
        timeout=10,
    )

    if response.status_code != 200:
        raise RuntimeError(f"Zenodo release request failed: {response.content}")

    logger.info("Zenodo release workflow triggered")


def update_pudl_viewer() -> None:
    """Update PUDL Viewer Cloud Run service to latest image.

    Args:
        service_name: Name of the Cloud Run service (e.g., "pudl-viewer").
    """
    logger.info("Updating PUDL Viewer Cloud Run service")

    _run(
        [
            "gcloud",
            "run",
            "services",
            "update",
            "pudl_viewer",
            "--image",
            "us-east1-docker.pkg.dev/catalyst-cooperative-pudl/pudl-viewer/pudl-viewer:latest",
            "--region",
            "us-east1",
        ],
    )

    logger.info("PUDL Viewer Cloud Run service updated")


def set_gcs_temporary_hold(gcs_path: str) -> None:
    """Set temporary hold on GCS objects to prevent deletion.

    Applies a temporary hold to protect versioned release artifacts from
    accidental deletion or lifecycle policies.

    Args:
        gcs_path: GCS path to objects (e.g., "gs://pudl.catalyst.coop/v2025.2.3/").
        billing_project: which project to bill for Requester Pays buckets.
    """
    logger.info(f"Setting temporary hold on {gcs_path}")

    _run(
        [
            "gcloud",
            "storage",
            "objects",
            "update",
            f"{gcs_path}*",
            "--temporary-hold",
        ]
    )

    logger.info(f"Temporary hold set on {gcs_path}")


def check_build_success(build_path: UPath) -> UPath:
    """Raise error if success file doesn't exist in build directory."""
    if not (build_path / "success").exists():
        raise RuntimeError("Can't find 'success' file in build directory!")
    return build_path


def get_build_from_tag(tag: str) -> UPath:
    """Find any builds associated with a git tag and return a GCS path to most recent build."""
    build_bucket = UPath("gs://builds.catalyst.coop")
    try:
        git_ref = _run(["git", "rev-parse", "--short", f"{tag}^{{}}"]).strip()
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Can't find git tag: {tag}") from e

    # Loop through all builds associated with git ref and find most recent one
    most_recent_build_dt = datetime.min
    most_recent_build_path = None
    build_path_pattern = re.compile(r"(\d{4}-\d{2}-\d{2}-\d{4})-([a-f|0-9]{9})-(.+)")
    for build_path in build_bucket.glob(f"*-{git_ref}-*"):
        if (match := build_path_pattern.search(str(build_path))) is None:
            logger.warning(
                f"Found build path with unexpected name format associated with ref, {git_ref}: {build_path}"
            )
            continue

        if (
            next_dt := datetime.strptime(match.group(1), "%Y-%m-%d-%H%M")
        ) > most_recent_build_dt:
            most_recent_build_dt = next_dt
            most_recent_build_path = build_path

    # Check that we found a build
    if most_recent_build_path is None:
        raise RuntimeError(
            f"Can't find a build associated with tag: {tag}, ref: {git_ref}"
        )
    logger.info(
        f"Most recent build associated with tag {tag}: {most_recent_build_path.as_uri()}"
    )
    return check_build_success(most_recent_build_path)


def get_deployment_type_from_tag(git_tag: str) -> DeploymentType:
    """Check if tag looks like a 'nightly' or 'stable' tag."""
    if re.match(r"v\d{4}\.\d{1,2}\.\d{1,2}", git_tag):
        deploy_type = DeploymentType.STABLE
    elif re.match(r"nightly-\d{4}-\d{2}-\d{2}", git_tag):
        deploy_type = DeploymentType.NIGHTLY
    else:
        raise RuntimeError(
            f"Git tag does not look like a stable or nightly tag. Input tag: {git_tag}"
        )
    return deploy_type
