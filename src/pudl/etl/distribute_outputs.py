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
import s3fs

logger = logging.getLogger(__name__)


def prepare_outputs_for_distribution(output_dir: Path) -> None:
    """Prepare ETL outputs for distribution.

    Takes raw ETL output structure and produces distribution-ready outputs:
    - Moves parquet files from parquet/ subdirectory to root
    - Compresses SQLite databases with maximum compression
    - Creates parquet archive (no compression, already compressed)
    - Removes test databases and temporary directories

    Args:
        output_dir: Directory containing ETL outputs to prepare.
    """
    output_dir = Path(output_dir)
    logger.info(f"Preparing outputs in {output_dir} for distribution")

    # Move parquet files to root and clean up subdirectory
    parquet_dir = output_dir / "parquet"
    if parquet_dir.exists():
        for parquet_file in parquet_dir.glob("*.parquet"):
            shutil.move(str(parquet_file), str(output_dir / parquet_file.name))

        datapackage = parquet_dir / "pudl_parquet_datapackage.json"
        if datapackage.exists():
            shutil.move(str(datapackage), str(output_dir / datapackage.name))

        shutil.rmtree(parquet_dir)

    # Remove test database
    test_db = output_dir / "pudl_dbt_tests.duckdb"
    if test_db.exists():
        test_db.unlink()

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
    if parquet_files:
        archive_path = output_dir / "pudl_parquet.zip"
        with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_STORED) as zf:
            for parquet_file in parquet_files:
                zf.write(parquet_file, arcname=parquet_file.name)

            datapackage = output_dir / "pudl_parquet_datapackage.json"
            if datapackage.exists():
                zf.write(datapackage, arcname=datapackage.name)

        logger.info(f"Created parquet archive: {archive_path}")

    logger.info("Output preparation complete")


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

    if not source_dir.exists():
        raise ValueError(f"Source directory does not exist: {source_dir}")

    # Check if directory has any content to upload
    if not any(source_dir.iterdir()):
        raise ValueError(f"Source directory is empty: {source_dir}")

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
    subprocess.run(  # noqa: S603
        ["git", "checkout", branch],  # noqa: S607
        check=True,
        capture_output=True,
        text=True,
    )

    # Merge tag (fast-forward only)
    subprocess.run(  # noqa: S603
        ["git", "merge", "--ff-only", tag],  # noqa: S607
        check=True,
        capture_output=True,
        text=True,
    )

    # Push to origin
    subprocess.run(  # noqa: S603
        ["git", "push", "-u", "origin", branch],  # noqa: S607
        check=True,
        capture_output=True,
        text=True,
    )

    logger.info(f"Git branch {branch} updated successfully")


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

    if env not in ("sandbox", "production"):
        raise ValueError(
            f"Invalid Zenodo environment: {env}. Must be 'sandbox' or 'production'."
        )

    publish_flag = "publish" if publish else "no-publish"

    subprocess.run(  # noqa: S603
        [  # noqa: S607
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

    subprocess.run(  # noqa: S603
        [  # noqa: S607
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

    # Ensure path ends with / for wildcard matching
    if not gcs_path.endswith("/"):
        gcs_path = f"{gcs_path}/"

    subprocess.run(  # noqa: S603
        [  # noqa: S607
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
