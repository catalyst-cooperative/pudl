"""Distribute PUDL ETL outputs to cloud storage and update git branches.

This module handles distribution of completed ETL builds to public cloud storage
(GCS and S3), git branch updates, Zenodo releases, and Cloud Run deployments.
"""

import logging
import shutil
import subprocess
import zipfile
from pathlib import Path

import gcsfs
import requests
import s3fs

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def prepare_outputs_for_distribution(output_dir: Path) -> None:
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
        output_dir: Directory containing ETL outputs to prepare.
    """
    output_dir = Path(output_dir)
    logger.info(f"Preparing outputs in {output_dir} for distribution")
    logger.info(f"Contents: {output_dir.glob('*')}")

    # Move files around
    parquet_dir = output_dir / "parquet"
    parquet_files = parquet_dir.glob("*.parquet")
    logger.info(f"Found parquets: {parquet_files}")
    for parquet_file in parquet_dir.glob("*.parquet"):
        shutil.move(str(parquet_file), str(output_dir / parquet_file.name))

    datapackage = parquet_dir / "pudl_parquet_datapackage.json"
    if datapackage.exists():
        shutil.move(str(datapackage), str(output_dir / datapackage.name))

    shutil.rmtree(parquet_dir)

    # Compress SQLite databases
    sqlite_files = list(output_dir.glob("*.sqlite"))
    for sqlite_file in sqlite_files:
        zip_path = output_dir / f"{sqlite_file.name}.zip"
        with zipfile.ZipFile(
            zip_path, "w", zipfile.ZIP_DEFLATED, compresslevel=9
        ) as zf:
            zf.write(sqlite_file, arcname=sqlite_file.name)
        sqlite_file.unlink()
        logger.info(f"Compressed {sqlite_file.name}")

    # Create parquet archive (store mode, no compression)
    parquet_files = list(output_dir.glob("*.parquet"))
    assert len(parquet_files) > 0, f"No parquet files in {output_dir}."
    archive_path = output_dir / "pudl_parquet.zip"
    with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_STORED) as zf:
        for parquet_file in parquet_files:
            zf.write(parquet_file, arcname=parquet_file.name)

        datapackage = output_dir / "pudl_parquet_datapackage.json"
        if datapackage.exists():
            zf.write(datapackage, arcname=datapackage.name)

    logger.info(f"Created parquet archive: {archive_path}")

    logger.info("Removing dbt database.")
    test_db = output_dir / "pudl_dbt_tests.duckdb"
    test_db.unlink()

    logger.info("Output preparation complete")


def _run(cmd: list[str]) -> None:
    """Wrap subprocess.run so we see error output."""
    subprocess.run(cmd, check=True, capture_output=True, text=True)  # noqa: S603


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
        gcs_path: GCS path to objects (e.g., "gs://pudl.catalyst.coop/v2025.2.3/*").
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
