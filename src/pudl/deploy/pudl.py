"""Distribute PUDL ETL outputs to cloud storage and update git branches.

This module handles distribution of completed ETL builds to public cloud storage
(GCS and S3), git branch updates, Zenodo releases, and Cloud Run deployments.
"""

import os
import re
import shutil
import subprocess
import tempfile
import time
import zipfile
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Literal

import gcsfs
import requests
import s3fs
from google.cloud import storage
from pydantic import BaseModel, ConfigDict, model_validator
from upath import UPath

from pudl import PUDL_ROOT_PATH
from pudl.deploy.zenodo_metadata import get_latest_release_tag
from pudl.helpers import run_git
from pudl.logging_helpers import configure_root_logger, get_logger

logger = get_logger(__name__)


class DeploymentType(Enum):
    """Deployments can be 'nightly', 'branch', or 'stable'."""

    NIGHTLY = "nightly"
    STABLE = "stable"
    BRANCH = "branch"


class DeploymentPlan(BaseModel):
    """Fully resolved, validated deployment behavior for one git tag and environment.

    This is the single source of truth both for what a deployment actually does --
    every other piece of code (path suffixes, which stages run) derives from a
    ``DeploymentPlan`` instead of independently re-deriving the same rules -- and for
    which ``git_tag``/``environment`` combinations are valid in the first place.

    ``deploy_type`` is derived from ``git_tag`` (see ``get_deployment_type_from_tag``)
    rather than accepted as a separate input, so a plan can never be constructed with
    a ``deploy_type`` that doesn't match its own ``git_tag``.

    This intentionally only validates what's knowable from ``git_tag`` and
    ``environment`` alone -- e.g. it does NOT check that a nightly/stable tag is
    actually reachable from ``main``, since that requires a git checkout the deploy
    container doesn't have (that check lives in the GHA workflow instead).
    """

    model_config = ConfigDict(frozen=True)

    git_tag: str
    environment: Literal["staging", "production"]

    @property
    def deploy_type(self) -> DeploymentType:
        """The deploy type implied by ``git_tag``'s shape."""
        return get_deployment_type_from_tag(self.git_tag)

    @model_validator(mode="after")
    def _validate_branch_only_targets_staging(self) -> "DeploymentPlan":
        if self.deploy_type == DeploymentType.BRANCH and self.environment != "staging":
            raise ValueError(
                f"Branch deployments can only target staging, got "
                f"environment={self.environment!r} for git_tag={self.git_tag!r}."
            )
        return self

    @property
    def path_suffixes(self) -> list[str]:
        """Cloud storage path suffixes this deployment uploads to.

        Nightly and branch builds share the same rolling "nightly"/"eel-hole"
        paths; stable releases get their own permanent version-tagged path plus
        "stable".
        """
        if self.deploy_type in (DeploymentType.NIGHTLY, DeploymentType.BRANCH):
            suffixes = ["nightly", "eel-hole"]
        else:
            suffixes = [self.git_tag, "stable"]
        if self.environment == "staging":
            suffixes = [f"staging/{s}" for s in suffixes]
        return suffixes

    @property
    def zenodo_source_suffix(self) -> str:
        """The single path suffix Zenodo should pull outputs from."""
        suffix = (
            self.git_tag if self.deploy_type == DeploymentType.STABLE else "nightly"
        )
        if self.environment == "staging":
            suffix = f"staging/{suffix}"
        return suffix

    @property
    def gcs_temporary_hold(self) -> bool:
        """Whether this deployment's permanent path should get a GCS temporary hold.

        Only a *production* stable release gets a hold, protecting its permanent
        version-tagged path. A staging deploy of the same tag is just a disposable
        test output and must remain clearable.
        """
        return (
            self.deploy_type == DeploymentType.STABLE
            and self.environment == "production"
        )

    @property
    def immutable_suffixes(self) -> frozenset[str]:
        """Path suffixes that are permanent and must never be cleared before upload.

        This is also the only path that's protected by ``gcs_temporary_hold``.
        """
        return frozenset({self.git_tag}) if self.gcs_temporary_hold else frozenset()

    @property
    def redeploy_eel_hole(self) -> bool:
        """Whether this deployment should redeploy the PUDL Viewer (Eel Hole)."""
        return self.deploy_type == DeploymentType.NIGHTLY

    @property
    def update_git_branch(self) -> bool:
        """Whether this deployment should fast-forward a git branch to its tag."""
        return self.deploy_type != DeploymentType.BRANCH

    @property
    def trigger_zenodo_release(self) -> bool:
        """Whether this deployment should trigger a Zenodo release."""
        return self.deploy_type != DeploymentType.BRANCH


def _zip_parquet_files(parquet_path: Path, output_path: Path) -> None:
    """Create a zipfile containing parquet files and an associated datapackage JSON file.

    ``parquet_path`` should contain a set of parquet files and exactly one datapackage
    JSON file that describes those parquet files.

    Args:
        parquet_path: Path to directory containing parquet files.
        output_path: Path to zipfile that should be created by this function.
    """
    parquet_files = list(parquet_path.glob("*.parquet"))
    assert len(parquet_files) > 0, f"No parquet files in {parquet_path}."

    # Create parquet archive (store mode, no compression)
    with zipfile.ZipFile(output_path, "w", zipfile.ZIP_STORED) as zf:
        for parquet_file in parquet_files:
            if parquet_file.exists():
                zf.write(parquet_file, arcname=parquet_file.name)
            else:
                raise RuntimeError(f"{parquet_file} must be a file!")

        # There should be exactly one datapackage JSON file in each parquet directory
        [datapackage] = parquet_path.glob("*datapackage.json")
        if datapackage.is_file():
            zf.write(datapackage, arcname="datapackage.json")
        else:
            raise RuntimeError(f"{datapackage} must be a file!")

    logger.info(f"Created parquet archive: {output_path}")


def _compress_sqlite_file(sqlite_file: Path) -> None:
    """Compress a SQLite database into a zip file and remove the original.

    Safe to call concurrently across different files -- each call only touches
    its own independent ``ZipFile`` and path.
    """
    logger.info(f"Compressing {sqlite_file.name}")
    zip_path = sqlite_file.parent / f"{sqlite_file.name}.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        zf.write(sqlite_file, arcname=sqlite_file.name)
    sqlite_file.unlink()
    logger.info(f"Compressed {sqlite_file.name}")


def download_build_outputs(local_path: Path, build_path: UPath) -> None:
    """Download raw ETL build outputs from builds.catalyst.coop to local disk.

    Split out from ``prepare_outputs_for_distribution`` so the network-bound
    download and the CPU-bound preparation work (zipping, compression) can be
    timed and reported as separate deploy stages.

    Args:
        local_path: Path on local filesystem to download outputs into.
        build_path: Remote path containing raw build outputs.
    """
    logger.info(f"Downloading build outputs from {build_path} to {local_path}")
    fs = build_path.fs
    fs.get(f"{build_path.as_uri()}/", str(local_path), recursive=True)


def prepare_outputs_for_distribution(local_path: Path, build_path: UPath) -> None:
    """Prepare already-downloaded ETL outputs for distribution.

    Takes raw ETL output structure and produces distribution-ready outputs:
    - Moves parquet files from parquet/ subdirectory to root
    - Compresses SQLite databases with maximum compression
    - Creates parquet archive (no compression, already compressed)
    - Removes test databases and temporary directories

    In general, we want to know if these files don't exist, so
    FileNotFoundErrors are OK and we don't need to pre-emptively try to avoid
    them.

    Args:
        local_path: Path on local filesystem containing raw outputs downloaded by
            ``download_build_outputs``, which this prepares for distribution in place.
        build_path: Remote path the raw build outputs came from -- only used here to
            derive the build ID for the provenance marker file.
    """
    logger.info(f"Preparing outputs in {local_path} for distribution")

    # Build and deploy logs live under builds.catalyst.coop for operators to
    # review, but must never be distributed publicly -- they can contain stack
    # traces or other details we don't want to expose.
    for log_file in local_path.glob("*.log"):
        logger.info(f"Excluding {log_file.name} from public distribution.")
        log_file.unlink()

    # The "success" sentinel is internal build-completion plumbing (see
    # check_build_success/get_build_from_tag) with no meaning for consumers of the
    # distributed outputs.
    success_marker = local_path / "success"
    if success_marker.exists():
        success_marker.unlink()

    # Write a file named after the build ID (containing that same build ID) so
    # anyone looking at the distributed outputs can trace them back to the build
    # that produced them -- this used to be implicit in the build log's filename,
    # which we no longer distribute. The file can't be empty: Zenodo rejects
    # zero-byte uploads.
    build_id = build_path.name
    (local_path / build_id).write_text(build_id)

    # Zip parquet files (for main pudl outputs + ferc extracted outputs)
    pudl_parquet_dir = local_path / "parquet"
    _zip_parquet_files(pudl_parquet_dir, local_path / "pudl_parquet.zip")
    for ferc_db in ["ferc1", "ferc2", "ferc6", "ferc60", "ferc714"]:
        if ferc_db != "ferc714":
            _zip_parquet_files(
                local_path / f"{ferc_db}_dbf",
                local_path / f"{ferc_db}_dbf.zip",
            )
        _zip_parquet_files(
            local_path / f"{ferc_db}_xbrl",
            local_path / f"{ferc_db}_xbrl.zip",
        )

    # Move parquet files to base directory
    for parquet_file in pudl_parquet_dir.glob("*.parquet"):
        shutil.move(str(parquet_file), str(local_path / parquet_file.name))

    # Move parquet datapackage to base directory
    datapackage = pudl_parquet_dir / "datapackage.json"
    shutil.move(str(datapackage), str(local_path / "pudl_parquet_datapackage.json"))

    # Remove parquet directory
    shutil.rmtree(pudl_parquet_dir)

    # Compress SQLite databases in parallel. zlib (used by ZIP_DEFLATED) releases
    # the GIL during actual compression, and each file is fully independent, so a
    # thread pool gives real wall-clock speedup for this otherwise-serial,
    # CPU/IO-heavy step without the complexity of multiprocessing.
    sqlite_files = list(local_path.glob("*.sqlite"))
    if sqlite_files:
        max_workers = min(len(sqlite_files), os.cpu_count() or 1)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            list(executor.map(_compress_sqlite_file, sqlite_files))

    logger.info("Removing dbt database.")
    test_db = local_path / "pudl_dbt_tests.duckdb"
    test_db.unlink()

    logger.info("Output preparation complete")


def clear_deployment_path(fs, path: str) -> None:
    """Empty a cloud storage prefix before writing fresh deployment outputs.

    Cloud storage (GCS, S3) uses virtual prefixes rather than real directories, so we
    use ``fs.rm(path, recursive=True)`` instead of ``rmdir()``, which would raise
    ``NotADirectoryError`` -- the same pattern used for FERC EQR staging cleanup in
    ``pudl.dagster.assets.deploy.ferceqr``.
    """
    if fs.exists(path):
        fs.rm(path, recursive=True)


def _upload_to_path(fs, path: str, source_dir: Path, clear_first: bool) -> None:
    """Clear (if requested) and upload all outputs to one destination path.

    Safe to call concurrently for different ``(fs, path)`` combinations -- gcsfs
    and s3fs are both designed to support concurrent use from multiple threads,
    and each call here only touches its own independent bucket/path.
    """
    if clear_first:
        logger.info(f"Clearing existing outputs at {path}")
        clear_deployment_path(fs, path)

    logger.info(f"Uploading outputs to {path}")
    fs.mkdirs(path, exist_ok=True)
    fs.put(f"{source_dir}/*", path, recursive=True)


def _assert_permanent_paths_are_empty(
    gcs_fs: gcsfs.GCSFileSystem,
    s3_fs: s3fs.S3FileSystem,
    path_suffixes: list[str],
    immutable_suffixes: frozenset[str],
) -> None:
    """Refuse to deploy to a permanent, version-tagged path that already has content.

    Rolling paths (nightly/stable/eel-hole) are cleared before every upload, but a
    permanent path like ``gs://pudl.catalyst.coop/v2026.7.0/`` deliberately never is
    -- it's meant to be written exactly once. If it already has content, deploying
    again would silently mix old and new files instead of cleanly replacing them,
    which almost always means the same version tag is being deployed a second time.
    That's an invalid request, so we check and raise up front rather than silently
    uploading over the top of it.
    """
    for suffix in path_suffixes:
        if suffix not in immutable_suffixes:
            continue
        for fs, path in (
            (gcs_fs, f"gs://pudl.catalyst.coop/{suffix}/"),
            (s3_fs, f"s3://pudl.catalyst.coop/{suffix}/"),
        ):
            if fs.exists(path):
                raise RuntimeError(
                    f"Refusing to deploy to {path}: it's a permanent, "
                    f"version-tagged path that must never be overwritten, and it "
                    f"already has content. This usually means version {suffix!r} "
                    f"has already been deployed."
                )


def upload_outputs(
    source_dir: Path,
    path_suffixes: list[str],
    immutable_suffixes: frozenset[str] = frozenset(),
) -> None:
    """Upload outputs to cloud storage paths.

    Uploads all files from source directory to GCS and S3 using the provided path
    suffixes. Each suffix is uploaded to both gs://pudl.catalyst.coop/{suffix}/ and
    s3://pudl.catalyst.coop/{suffix}/. Any existing objects at a suffix are removed
    first, unless that suffix is listed in ``immutable_suffixes`` -- a permanent,
    hold-protected versioned release path is never cleared, and instead must not
    exist at all yet (see ``_assert_permanent_paths_are_empty``).

    Each (suffix, destination) pair is uploaded concurrently: GCS and S3 are separate
    network destinations, and this is I/O-bound work that releases the GIL.

    Args:
        source_dir: Local directory containing prepared outputs to upload.
        path_suffixes: Path suffixes to upload to (e.g., ["nightly", "eel-hole"]).
        immutable_suffixes: Path suffixes that should never be cleared before upload
            (e.g. a permanent stable-version path like "v2026.7.0"). It's an error
            for one of these paths to already exist.

    Raises:
        RuntimeError: If a permanent, immutable path already has content.
    """
    logger.info("Uploading outputs to cloud storage")

    if not source_dir.exists():
        raise ValueError(f"Source directory does not exist: {source_dir}")
    if not any(source_dir.iterdir()):
        raise ValueError(f"Source directory is empty: {source_dir}")

    # NOTE (2026-02-11): our GCS distribution bucket is requester pays.
    gcs_fs = gcsfs.GCSFileSystem(requester_pays=True)
    s3_fs = s3fs.S3FileSystem()

    _assert_permanent_paths_are_empty(gcs_fs, s3_fs, path_suffixes, immutable_suffixes)

    upload_targets = []
    for suffix in path_suffixes:
        clear_first = suffix not in immutable_suffixes
        upload_targets.append(
            (gcs_fs, f"gs://pudl.catalyst.coop/{suffix}/", clear_first)
        )
        upload_targets.append(
            (s3_fs, f"s3://pudl.catalyst.coop/{suffix}/", clear_first)
        )

    with ThreadPoolExecutor(max_workers=len(upload_targets)) as executor:
        futures = [
            executor.submit(_upload_to_path, fs, path, source_dir, clear_first)
            for fs, path, clear_first in upload_targets
        ]
        for future in futures:
            future.result()

    logger.info(f"Upload complete for {len(path_suffixes)} path(s)")


def update_git_branch(
    tag: str,
    branch: str,
    environment: Literal["staging", "production"],
    github_token: str,
) -> None:
    """Merge git tag into branch and push to origin.

    Performs fast-forward merge of a tag into a branch and pushes the result.
    This updates the nightly or stable branch to point to the tagged release.

    If environment is 'staging', this will try the checkout and merge, but skip the
    git push.

    Args:
        tag: Git tag to merge (e.g., "nightly-2025-02-05" or "v2025.2.3").
        branch: Target branch to update (e.g., "nightly" or "stable").
        environment: Deployment environment.

    Raises:
        subprocess.CalledProcessError: If git commands fail.
    """
    if get_deployment_type_from_tag(tag).value != branch:
        raise RuntimeError(
            f"Git tag, {tag}, does not match deployment branch, {branch}."
        )
    logger.info(f"Updating git branch {branch} to tag {tag}")

    run_git(["config", "user.email", "pudl@catalyst.coop"])
    run_git(["config", "user.name", "pudlbot"])
    run_git(
        [
            "remote",
            "set-url",
            "origin",
            f"https://pudlbot:{github_token}@github.com/catalyst-cooperative/pudl.git",
        ]
    )
    run_git(["fetch", "--force", "--tags", "origin", tag])
    run_git(["fetch", "origin", f"{branch}:{branch}"])
    run_git(["checkout", branch])
    run_git(["merge", "--ff-only", tag])
    if environment != "staging":
        run_git(["push", "-u", "origin", branch])

    logger.info(f"Git branch {branch} updated successfully")


def dispatch_github_workflow(
    repo: str,
    workflow_file: str,
    ref: str,
    token: str,
    inputs: dict[str, str] | None = None,
) -> None:
    """Trigger a workflow_dispatch event on a GitHub Actions workflow.

    Args:
        repo: GitHub repo in "owner/name" form (e.g. "catalyst-cooperative/pudl").
        workflow_file: Workflow filename (e.g. "zenodo-data-release.yml").
        ref: Git branch or tag to run the workflow from.
        token: Bearer token to authenticate to GitHub.
        inputs: workflow_dispatch inputs, if the workflow takes any.
    """
    payload: dict[str, str | dict[str, str]] = {"ref": ref}
    if inputs:
        payload["inputs"] = inputs

    response = requests.post(
        f"https://api.github.com/repos/{repo}/actions/workflows/{workflow_file}/dispatches",
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
        },
        json=payload,
        timeout=10,
    )
    response.raise_for_status()


def trigger_zenodo_release(
    build_ref: str,
    deploy_type: DeploymentType,
    source_suffix: str,
    token: str,
) -> None:
    """Trigger Zenodo data release GitHub Actions workflow.

    Dispatches the zenodo-data-release workflow to create or update a Zenodo
    deposition with PUDL data outputs.

    Args:
        build_ref: The git reference for the workflow. The reference can be a branch or tag name.
        deploy_type: Deployment type.
        source_suffix: Suffix appended to s3 path (s3://pudl.catalyst.coop) to get
            path to data outputs which will populate zenodo deposition.
        token: the bearer token to authenticate to GitHub.
    """
    ignore_regex = r"^.*\.parquet$"
    if deploy_type == DeploymentType.STABLE:
        publish_flag = "no-publish"
        env = "production"
        # build_ref is a real 'vYYYY.M.n' release tag for stable deploys, matching a
        # release notes section.
        pudl_version = build_ref
    else:
        publish_flag = "publish"
        env = "sandbox"
        # Nightly/branch builds don't have a real release tag or release notes
        # section of their own -- these are just sandbox smoke tests of the Zenodo
        # release machinery, so fall back to the most recently published version.
        pudl_version = get_latest_release_tag(PUDL_ROOT_PATH)

    logger.info(
        f"Triggering Zenodo release: env={env}, publish={publish_flag}, "
        f"pudl_version={pudl_version}"
    )

    dispatch_github_workflow(
        repo="catalyst-cooperative/pudl",
        workflow_file="zenodo-data-release.yml",
        ref=build_ref,
        token=token,
        inputs={
            "env": env,
            "source_dir": f"s3://pudl.catalyst.coop/{source_suffix}",
            "ignore_regex": ignore_regex,
            "publish": publish_flag,
            "pudl_version": pudl_version,
        },
    )

    logger.info("Zenodo release workflow triggered")


def update_pudl_viewer(
    token: str,
    environment: Literal["staging", "production"],
) -> None:
    """Update PUDL Viewer Cloud Run service to latest image.

    Args:
        token: the bearer token to authenticate to GitHub.
        environment: deploy staging or production version of viewer.
    """
    logger.info("Updating PUDL Viewer Cloud Run service")

    workflow_file = (
        "build-deploy-staging.yml" if environment == "staging" else "build-deploy.yml"
    )

    dispatch_github_workflow(
        repo="catalyst-cooperative/eel-hole",
        workflow_file=workflow_file,
        ref="main",
        token=token,
    )

    logger.info("PUDL Viewer Cloud Run service updated")


def set_gcs_temporary_hold(gcs_path: str, billing_project: str = "") -> None:
    """Set temporary hold on GCS objects to prevent deletion.

    Applies a temporary hold to protect versioned release artifacts from
    accidental deletion or lifecycle policies.

    Args:
        gcs_path: GCS path to objects (e.g., "gs://pudl.catalyst.coop/v2025.2.3/").
        billing_project: which project to bill for Requester Pays access to this
            bucket. If not given, falls back to whatever ``storage.Client()``
            resolves via Application Default Credentials -- the ``GOOGLE_CLOUD_
            PROJECT``/``GCLOUD_PROJECT`` env vars, a service account key file's
            embedded project, the active ``gcloud config set project``, or GCE/
            Cloud Run instance metadata, in that order. That's the same
            resolution the ``gcloud`` CLI itself uses, so e.g. a local dev shell
            with a configured ``gcloud`` project needs no explicit argument.

    Raises:
        RuntimeError: If no objects are found at ``gcs_path`` or a post-hold
            sweep finds objects still missing the hold.
    """
    logger.info(f"Setting temporary hold on {gcs_path}")

    bucket_name, _, prefix = gcs_path.removeprefix("gs://").partition("/")
    # Passing project=None explicitly (rather than omitting it) disables
    # Application Default Credentials project auto-detection entirely, so the
    # kwarg has to be left out when there's no explicit override.
    client_kwargs = {"project": billing_project} if billing_project else {}
    client = storage.Client(**client_kwargs)
    bucket = client.bucket(bucket_name, user_project=client.project)

    num_held = 0
    for blob in bucket.list_blobs(prefix=prefix):
        blob.temporary_hold = True
        blob.patch()
        num_held += 1

    if num_held == 0:
        raise RuntimeError(f"No objects found at {gcs_path}; nothing to hold.")

    logger.info(f"Temporary hold set on {num_held} object(s) at {gcs_path}")

    # Re-list rather than trust the objects patched above: a patch() call could
    # fail to actually persist despite not raising, and any object created
    # concurrently with the loop above (e.g. a retried upload) wouldn't have been
    # seen by it at all. Both would otherwise leave a "protected" release with
    # unprotected objects in it.
    missing_hold = [
        blob.name
        for blob in bucket.list_blobs(prefix=prefix)
        if not blob.temporary_hold
    ]
    if missing_hold:
        raise RuntimeError(
            f"Temporary hold verification failed for {gcs_path}: "
            f"{len(missing_hold)} object(s) still missing the hold: {missing_hold}"
        )

    logger.info(f"Verified temporary hold on all objects at {gcs_path}")


def check_build_success(build_path: UPath) -> UPath:
    """Raise error if success file doesn't exist in build directory."""
    if not (build_path / "success").exists():
        raise RuntimeError("Can't find 'success' file in build directory!")
    return build_path


def get_build_from_tag(tag: str) -> UPath:
    """Find any builds associated with a git tag and return a GCS path to most recent build."""
    build_bucket = UPath("gs://builds.catalyst.coop")
    try:
        git_ref = run_git(["rev-parse", "--short=9", f"{tag}^{{}}"]).strip()
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Can't find git tag: {tag}") from e

    # Loop through all builds associated with git ref and find most recent one
    most_recent_build_dt = datetime.min
    most_recent_build_path = None
    build_path_pattern = re.compile(
        r"(nightly|branch|stable)-(\d{4}-\d{2}-\d{2}-\d{4})-([a-f|0-9]{9})-.+"
    )
    checked = []
    for build_path in build_bucket.glob(f"*-{git_ref}-*"):
        checked.append(str(build_path))
        if (match := build_path_pattern.search(str(build_path))) is None:
            raise RuntimeError(
                f"Found build path with unexpected name format associated with ref, {git_ref}: {build_path}"
            )

        if (
            next_dt := datetime.strptime(match.group(2), "%Y-%m-%d-%H%M")
        ) > most_recent_build_dt:
            most_recent_build_dt = next_dt
            most_recent_build_path = build_path

    # Check that we found a build
    if most_recent_build_path is None:
        raise RuntimeError(
            f"Can't find a build associated with tag: {tag}, ref: {git_ref}; checked:\n{'\n'.join(checked)}"
        )
    logger.info(
        f"Most recent build associated with tag {tag}: {most_recent_build_path.as_uri()}"
    )
    return check_build_success(most_recent_build_path)


def get_deployment_type_from_tag(git_tag: str) -> DeploymentType:
    """Check if tag looks like a 'nightly', 'branch', or 'stable' tag."""
    if re.match(r"v\d{4}\.\d{1,2}\.\d{1,2}", git_tag):
        deploy_type = DeploymentType.STABLE
    elif re.match(r"nightly-\d{4}-\d{2}-\d{2}", git_tag):
        deploy_type = DeploymentType.NIGHTLY
    elif re.match(r"branch-\d{4}-\d{2}-\d{2}-\d{4}-.*-.*", git_tag):
        deploy_type = DeploymentType.BRANCH
    else:
        raise RuntimeError(
            f"Git tag does not look like a stable or nightly tag. Input tag: {git_tag}"
        )
    return deploy_type


@dataclass(frozen=True)
class ResolvedBuild:
    """Everything ``pudl_deploy``'s ``main()`` needs after resolving a deployment."""

    plan: DeploymentPlan
    build_path: UPath
    build_id: str
    local_copy_path: Path
    local_logfile: Path


def resolve_build(
    git_tag: str, environment: Literal["staging", "production"]
) -> ResolvedBuild:
    """Resolve the deployment plan, locate the build, and set up local logging.

    Raises if ``git_tag`` doesn't look like a nightly/stable/branch tag, if a
    branch tag is being deployed to production, or if no successful build exists
    for the tag yet.
    """
    plan = DeploymentPlan(git_tag=git_tag, environment=environment)

    build_path = get_build_from_tag(git_tag)
    build_id = build_path.name
    local_copy_path = Path(tempfile.mkdtemp())

    deploy_start_time = datetime.now()
    local_logfile = (
        Path(tempfile.mkdtemp())
        / f"{build_id}-deploy-{deploy_start_time:%Y-%m-%d-%H%M}.log"
    )
    configure_root_logger(logfile=str(local_logfile))

    return ResolvedBuild(
        plan=plan,
        build_path=build_path,
        build_id=build_id,
        local_copy_path=local_copy_path,
        local_logfile=local_logfile,
    )


class StageStatus(Enum):
    """Possible outcomes of a single deployment stage."""

    SKIPPED = "skipped"
    SUCCESS = "success"
    FAILURE = "failure"


class DeployStage(Enum):
    """The fixed set of tracked deployment stages.

    Members are declared in the order they should appear in the Zulip notification
    table, and ``DeployStage`` iteration preserves that order -- so this is the
    single source of truth for both the valid stage identifiers (used as dict keys
    and passed to ``run_stage``) and their display order, rather than an
    unconstrained string that's only coincidentally consistent
    between call sites. ``.value`` gives the human-readable name shown in messages.
    """

    RESOLVE_BUILD = "Resolve build"
    DOWNLOAD_BUILD_OUTPUTS = "Download build outputs"
    PREPARE_OUTPUTS = "Prepare outputs"
    UPLOAD_OUTPUTS = "Upload outputs"
    REDEPLOY_EEL_HOLE = "Redeploy Eel Hole"
    UPDATE_GIT_BRANCH = "Update Git Branch"
    TRIGGER_ZENODO_RELEASE = "Trigger Zenodo Release"
    GCS_TEMPORARY_HOLD = "GCS Temporary Hold"


ZULIP_API_URL = "https://catalyst-cooperative.zulipchat.com/api/v1/messages"
ZULIP_BOT_EMAIL = "build-status-bot@catalyst-cooperative.zulipchat.com"
ZULIP_STREAM = "pudl-deployments"
ZULIP_TOPIC = "build-deploy-pudl"


@dataclass
class StageResult:
    """Outcome of a single deployment stage, for Zulip stage-table reporting."""

    status: StageStatus = StageStatus.SKIPPED
    duration_seconds: float = 0.0


def new_deploy_stage_results() -> dict[DeployStage, StageResult]:
    """Initialize every tracked deploy stage as skipped, in table display order."""
    return {stage: StageResult() for stage in DeployStage}


def run_stage[T](
    stage_fn: Callable[..., T],
    stage_name: DeployStage,
    stage_results: dict[DeployStage, StageResult],
    *args,
    fail_hard: bool = True,
    **kwargs,
) -> T | None:
    """Run a deploy stage, recording its status and duration in ``stage_results``.

    If ``stage_fn`` raises and ``fail_hard`` is True (the default), the exception
    propagates to the caller after the stage is recorded as failed. Pass
    ``fail_hard=False`` for stages that shouldn't block their siblings -- e.g. a
    failed Zenodo release shouldn't prevent the GCS temporary hold from being
    attempted -- in which case the failure is logged instead of raised.

    Returns whatever ``stage_fn`` returns (``None`` if it failed with
    ``fail_hard=False``), so stages that produce a result -- e.g. ``resolve_build``
    -- can be run through the same tracking/reporting machinery as side-effect-only
    stages.
    """
    start = time.monotonic()
    status = StageStatus.FAILURE
    result = None
    try:
        result = stage_fn(*args, **kwargs)
        status = StageStatus.SUCCESS
    except Exception:
        if fail_hard:
            raise
        logger.exception(f"Deploy stage {stage_name.value!r} failed; continuing.")
    finally:
        stage_results[stage_name] = StageResult(
            status=status, duration_seconds=time.monotonic() - start
        )
    return result


def format_stage_duration(elapsed_seconds: float) -> str:
    """Format a duration in seconds as ``HH:MM:SS``."""
    elapsed_seconds = int(elapsed_seconds)
    hours, remainder = divmod(elapsed_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def stage_emoji(status: StageStatus) -> str:
    """Return the Zulip emoji corresponding to a stage status."""
    if status == StageStatus.SKIPPED:
        return ":ghost:"
    if status == StageStatus.SUCCESS:
        return ":check:"
    return ":x:"


def build_deploy_logfile_links(
    build_id: str,
    deploy_logfile_name: str,
    batch_job_name: str | None,
) -> str:
    """Build markdown links for reviewing a deployment's logs and outputs.

    Mirrors the "Review PUDL Build Logs" section ``pudl_batch.sh`` appends to the
    nightly build's own Zulip notification (see ``pudl_logfile_links``).

    Args:
        build_id: The build directory name under gs://builds.catalyst.coop.
        deploy_logfile_name: Filename of this deployment's logfile within that
            build directory.
        batch_job_name: Name of the Google Batch job running this deployment, if
            known -- omitted from the message (rather than producing a broken
            link) when unset, e.g. when testing outside of an actual Batch job.
    """
    nl = "\n"
    gcs_relative_path = f"builds.catalyst.coop/{build_id}/{deploy_logfile_name}"
    download_url = f"https://storage.cloud.google.com/{gcs_relative_path}"
    browser_url = (
        "https://console.cloud.google.com/storage/browser/"
        f"builds.catalyst.coop/{build_id}"
    )

    message = f"## Review PUDL Deploy Logs{nl}{nl}"
    message += f"* GCS URL: `gs://{gcs_relative_path}`{nl}"
    message += f"* [Download PUDL deploy logs to review locally]({download_url}){nl}"
    if batch_job_name:
        console_url = (
            "https://console.cloud.google.com/batch/jobsDetail/regions/us-east1/"
            f"jobs/{batch_job_name}/logs?project=catalyst-cooperative-pudl"
        )
        message += (
            "* [Review PUDL deploy logs in the Google Cloud "
            f"Console]({console_url}){nl}"
        )
    message += (
        f"* [Browse full build outputs in the Google Cloud Console]({browser_url}){nl}"
    )

    return message


def build_deploy_zulip_message(
    build_id: str,
    git_tag: str,
    stage_results: dict[DeployStage, StageResult],
    total_duration_seconds: float,
    deploy_logfile_name: str,
    batch_job_name: str | None = None,
) -> str:
    """Build a markdown Zulip message summarizing deployment stage statuses."""
    succeeded = all(
        result.status in (StageStatus.SUCCESS, StageStatus.SKIPPED)
        for result in stage_results.values()
    )
    nl = "\n"

    if succeeded:
        message = f"{nl}# :check: PUDL Deployment Succeeded!! :partygritty:{nl}{nl}"
    else:
        message = f"{nl}# :x: PUDL Deployment Failed :sob:{nl}{nl}"

    message += f"- Build ID: `{build_id}`{nl}"
    message += f"- Git Tag: `{git_tag}`{nl}"
    message += (
        f"## :time: Total Deploy Duration: "
        f"`[{format_stage_duration(total_duration_seconds)}]`{nl}{nl}"
    )
    message += f"## Deploy Stage Status{nl}{nl}"
    message += f":check: = SUCCESS; :x: = FAILURE; :ghost: = SKIPPED{nl}{nl}"
    message += f"| Stage | Status | Duration |{nl}"
    message += f"|:---|:---:|:---:|{nl}"
    for stage in DeployStage:
        result = stage_results[stage]
        message += (
            f"| {stage.value} | {stage_emoji(result.status)} | "
            f"`[{format_stage_duration(result.duration_seconds)}]` |{nl}"
        )
    message += nl
    message += build_deploy_logfile_links(
        build_id=build_id,
        deploy_logfile_name=deploy_logfile_name,
        batch_job_name=batch_job_name,
    )

    return message


def send_zulip_message(message: str, api_key: str) -> None:
    """Post a message to the pudl-deployments Zulip stream."""
    try:
        response = requests.post(
            ZULIP_API_URL,
            auth=(ZULIP_BOT_EMAIL, api_key),
            data={
                "type": "stream",
                "to": ZULIP_STREAM,
                "topic": ZULIP_TOPIC,
                "content": message,
            },
            timeout=10,
        )
        response.raise_for_status()
    except requests.RequestException:
        logger.warning("Zulip notification failed.", exc_info=True)
