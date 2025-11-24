#!/usr/bin/env python
"""Upload a prepared PUDL data release directory to Zenodo.

The PUDL data release process produces a directory of artifacts (zipped Parquet files,
SQLite databases, JSON metadata, logs, etc.) that are uploaded to CERN's Zenodo data
repository for long-term archival access. Each new versioned release of PUDL is
associated with the same original PUDL concept DOI.

This module provides a CLI that handles the process of uploading a new PUDL data release
to Zenodo, given a prepared directory of artifacts typically produced by the PUDL builds.

It uses state objects to ensure that Zenodo API calls happen in a valid order. The files
to upload are read using ``fsspec`` and remote files are staged locally one at a time
so uploads can be retried, but without using excessive local disk space.

Retries are implemented for all upload requests to recover from transient network issues
and Zenodo server flakiness. Zero-byte uploads are prevented.

NOTE: PUDL nightly build outputs are NOT suitable for producing a Zenodo data release
unless the Parquet outputs are filtered out with an appropriate ignore_regex. Double
check what files should actually be distributed before running the script.

Run ``zenodo_data_release --help`` for CLI usage instructions.
"""

import datetime
import logging
import os
import re
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import IO

import click
import coloredlogs
import fsspec
import requests
from pydantic import AnyHttpUrl, BaseModel, Field

from pudl.logging_helpers import get_logger

SANDBOX = "sandbox"
PRODUCTION = "production"
RETRYABLE_STATUS_CODES = {
    408,  # Request Timeout
    500,  # Internal Server Error
    502,  # Bad Gateway
    503,  # Service Unavailable
    504,  # Gateway Timeout
    520,  # Web server returned an unknown error (proxy)
    522,  # Connection timed out (proxy)
    524,  # A timeout occurred (proxy)
}

logger = get_logger(__name__)
coloredlogs.install(
    level=logging.INFO,
    logger=logger,
    fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
)


class _LegacyLinks(BaseModel):
    html: AnyHttpUrl
    bucket: AnyHttpUrl


class _LegacyMetadata(BaseModel):
    upload_type: str = "dataset"
    title: str
    access_right: str
    creators: list[dict]
    license: str = "cc-by-4.0"  # noqa: A003
    publication_date: str = ""
    description: str = ""


class _LegacyDeposition(BaseModel):
    id_: int = Field(alias="id")
    conceptrecid: int
    links: _LegacyLinks
    metadata: _LegacyMetadata


class _NewFile(BaseModel):
    id_: str = Field(alias="id")


class _NewRecord(BaseModel):
    id_: int = Field(alias="id")
    files: list[_NewFile]


class ZenodoClient:
    """Thin wrapper over Zenodo REST API.

    Mostly legacy calls (https://developers.zenodo.org/) (archive:
    https://web.archive.org/web/20231212025359/https://developers.zenodo.org/)
    but due to inconsistent behavior of legacy API on sandbox environment, we
    need some of the unreleased new API endpoints too:
    https://inveniordm.docs.cern.ch/reference/rest_api_drafts_records/
    """

    def __init__(self, env: str):
        """Constructor.

        Args:
            env: "sandbox" or "production".
        """
        if env == SANDBOX:
            self.base_url = "https://sandbox.zenodo.org/api"
            token = os.environ["ZENODO_SANDBOX_TOKEN_PUBLISH"]
        elif env == PRODUCTION:
            self.base_url = "https://zenodo.org/api"
            token = os.environ["ZENODO_TOKEN_UPLOAD"]
        else:
            raise ValueError(
                f"Got unexpected {env=}, expected {SANDBOX} or {PRODUCTION}"
            )

        self.auth_headers = {"Authorization": f"Bearer {token}"}

        logger.info(f"Using Zenodo token: {token[:4]}...{token[-4:]}")

    def retry_request(
        self,
        *,
        method,
        url,
        max_tries: int = 6,
        request_timeout: float | None = None,
        data_factory: Callable[[], IO[bytes]] | None = None,
        **kwargs,
    ) -> requests.Response:
        """Retry calls to ``requests.request`` with exponential backoff.

        Args:
            method: HTTP method to use for the request (e.g. ``GET``).
            url: Fully-qualified URL to which the request is sent.
            max_tries: Maximum number of attempts before surfacing an error.
            request_timeout: Optional per-request timeout in seconds. When ``None`` the
                timeout grows exponentially (``2**attempt``).
            data_factory: Optional callable that yields a fresh binary stream for each
                attempt. Useful for uploads that require reopening a file-like object.
            **kwargs: Additional keyword arguments passed through directly to
                ``requests.request``.

        Returns:
            The ``requests.Response`` produced by the successful attempt.

        Raises:
            requests.RequestException: If all attempts fail with a requests error.
            OSError: If reading from disk fails when preparing a payload.
            RuntimeError: If no response object is produced (should be rare).
        """
        response: requests.Response | None = None
        for attempt in range(1, max_tries + 1):
            # Create a fresh set of kwargs for each attempt so that data_factory()
            # can produce a fresh payload.
            attempt_kwargs = dict(kwargs)
            payload: IO[bytes] | None = None
            timeout_value = (
                request_timeout if request_timeout is not None else 2**attempt
            )
            try:
                if data_factory:
                    payload = data_factory()
                    attempt_kwargs["data"] = payload
                response = requests.request(
                    method=method,
                    url=url,
                    timeout=timeout_value,
                    **attempt_kwargs,
                )
                if response.status_code in RETRYABLE_STATUS_CODES:
                    raise requests.HTTPError(
                        f"Retryable status {response.status_code} from {url}",
                        response=response,
                    )
                return response
            except (requests.RequestException, OSError, requests.HTTPError) as exc:
                if attempt == max_tries:
                    raise
                wait = 2**attempt
                logger.warning(
                    f"Attempt #{attempt} for {url} failed with {exc}, retrying in {wait}s"
                )
                time.sleep(wait)
            finally:
                if payload:
                    payload.close()
        if response is None:
            raise RuntimeError(
                f"Failed to complete request to {url} after {max_tries} tries"
            )
        return response

    def get_deposition(self, deposition_id: int) -> _LegacyDeposition:
        """LEGACY API: Get JSON describing a deposition.

        Depositions can be published *or* unpublished.
        """
        response = self.retry_request(
            method="GET",
            url=f"{self.base_url}/deposit/depositions/{deposition_id}",
            headers=self.auth_headers,
        )
        logger.debug(
            f"License from JSON for {deposition_id} is "
            f"{response.json()['metadata'].get('license')}"
        )
        return _LegacyDeposition(**response.json())

    def get_record(self, record_id: int) -> _NewRecord:
        """NEW API: Get JSON describing a record.

        All records are published records.
        """
        response = self.retry_request(
            method="GET",
            url=f"{self.base_url}/records/{record_id}",
            headers=self.auth_headers,
        )
        return _NewRecord(**response.json())

    def new_record_version(self, record_id: int) -> _NewRecord:
        """NEW API: get or create the draft associated with a record ID.

        Finds the latest record in the concept that record_id points to, and
        makes a new version unless one exists already.
        """
        response = self.retry_request(
            method="POST",
            url=f"{self.base_url}/records/{record_id}/versions",
            headers=self.auth_headers,
        )
        return _NewRecord(**response.json())

    def update_deposition_metadata(
        self, deposition_id: int, metadata: _LegacyMetadata
    ) -> _LegacyDeposition:
        """LEGACY API: Update deposition metadata.

        Replaces the existing metadata completely - so make sure to pass in complete
        metadata. You cannot update metadata fields one at a time.
        """
        url = f"{self.base_url}/deposit/depositions/{deposition_id}"
        data = {"metadata": metadata.model_dump()}
        logger.debug(f"Setting metadata for {deposition_id} to {data}")
        response = self.retry_request(
            method="PUT", url=url, json=data, headers=self.auth_headers
        )
        return _LegacyDeposition(**response.json())

    def delete_deposition_file(self, deposition_id: int, file_id) -> requests.Response:
        """LEGACY API: Delete file from deposition.

        Note: file_id is not always the file name.
        """
        return self.retry_request(
            method="DELETE",
            url=f"{self.base_url}/deposit/depositions/{deposition_id}/files/{file_id}",
            headers=self.auth_headers,
        )

    def create_bucket_file(
        self,
        bucket_url: AnyHttpUrl,
        file_path: Path,
        max_tries: int = 6,
    ) -> requests.Response:
        """LEGACY API: Upload a file to a deposition's file bucket.

        We prefer this API this over the /deposit/depositions/{id}/files endpoint
        because it allows for files >100MB.

        Args:
            bucket_url: Upload destination returned by Zenodo for the draft.
            file_path: Local path to the artifact being uploaded.
            max_tries: Maximum number of upload attempts before failing.

        Returns:
            The ``requests.Response`` from the successful upload attempt.

        Raises:
            ValueError: If ``file_path`` is empty.
            requests.RequestException: If all upload attempts fail.
        """
        actual_name = file_path.name
        url = f"{bucket_url}/{actual_name}"
        logger.info(f"Uploading file to {url}")

        size = file_path.stat().st_size
        if size == 0:
            raise ValueError(f"Upload source for {actual_name} has zero bytes")

        return self.retry_request(
            method="PUT",
            url=url,
            max_tries=max_tries,
            request_timeout=60,
            headers=self.auth_headers,
            stream=True,
            data_factory=lambda: file_path.open("rb"),
        )

    def publish_deposition(self, deposition_id: int) -> _LegacyDeposition:
        """LEGACY API: publish deposition."""
        response = self.retry_request(
            method="POST",
            url=f"{self.base_url}/deposit/depositions/{deposition_id}/actions/publish",
            headers=self.auth_headers,
        )
        return _LegacyDeposition(**response.json())


@dataclass
class State:
    """Parent class for dataset states.

    Provides an abstraction layer that hides Zenodo's data model from the caller.

    Subclasses + their limited method definitions provide a way to avoid calling the
    operations in the wrong order.
    """

    record_id: int
    zenodo_client: ZenodoClient


class InitialDataset(State):
    """Represent initial dataset state.

    At this point, we don't know if there is an existing draft or not - the only thing
    we can do is try to get a fresh draft.
    """

    def get_empty_draft(self) -> "EmptyDraft":
        """Get an empty draft for this dataset.

        Use new API to get any draft, then use legacy API to delete any files
        in the draft.
        """
        logger.info(f"Getting new version for {self.record_id}")
        latest_record = self.zenodo_client.get_record(self.record_id)
        new_version = self.zenodo_client.new_record_version(latest_record.id_)
        new_rec_id = new_version.id_
        existing_files = new_version.files
        logger.info(
            f"Draft {new_rec_id} has {len(existing_files)} existing files, deleting..."
        )
        for f in existing_files:
            self.zenodo_client.delete_deposition_file(new_rec_id, f.id_)
        return EmptyDraft(record_id=new_rec_id, zenodo_client=self.zenodo_client)


class EmptyDraft(State):
    """We can only sync the directory once we've gotten an empty draft."""

    @staticmethod
    def _sync_local_path(
        openable_file: fsspec.core.OpenFile, staging_dir: Path
    ) -> Path:
        """Ensure the given ``fsspec`` file exists on the local filesystem.

        When ``openable_file`` already resides on the local filesystem we avoid
        copying and return its existing path. Remote files are downloaded into
        ``staging_dir`` (a shared temporary directory) so the rest of the upload
        pipeline can treat every artifact as a simple ``Path`` without caring
        where it came from.

        Args:
            openable_file: ``fsspec`` handle pointing to the source artifact.
            staging_dir: Directory used to cache remote files locally.

        Returns:
            A ``Path`` pointing to a readable local copy of ``openable_file``.
        """
        # fsspec supports chained protocols, so this isn't always just a string
        protocol = openable_file.fs.protocol
        protocol_parts = protocol if isinstance(protocol, (list, tuple)) else [protocol]
        if "local" in protocol_parts:
            return Path(openable_file.path)

        tmp_path = Path(staging_dir, Path(openable_file.path).name)
        # Remove the tmp_path if it already exists to avoid conflicts
        tmp_path.unlink(missing_ok=True)
        # Actually download the remote file to the tmp_path that mirrors the original
        openable_file.fs.get(openable_file.path, tmp_path)
        return tmp_path

    def sync_directory(self, source_dir: str, ignore: tuple[str]) -> "ContentComplete":
        """Upload every file in ``source_dir`` to the draft bucket.

        The method enumerates files (not subdirectories) via ``fsspec`` so the source
        can live on local disk, GCS, S3, etc. Remote objects are first staged into a
        temporary directory to ensure uploads always come from local ``Path`` objects
        that can be rewound for retries. Regex patterns provided via ``ignore`` are
        applied to the full path of each candidate file, allowing us to drop logs,
        intermediate data, or other nightlies-only artifacts before hitting Zenodo.

        Args:
            source_dir: Directory (local or remote) whose contents will be sent to
                Zenodo.
            ignore: Tuple of regex patterns; any path matching one is skipped.

        Returns:
            A ``ContentComplete`` state ready for metadata updates.
        """
        logger.info(f"Syncing files from {source_dir} to draft {self.record_id}...")
        bucket_url = self.zenodo_client.get_deposition(self.record_id).links.bucket
        dir_fs, dir_path = fsspec.core.url_to_fs(source_dir)
        if not dir_fs.isdir(dir_path):
            raise ValueError(f"{source_dir} is not a directory!")

        # fsspec supports chained protocols, so this isn't always just a string
        protocol = dir_fs.protocol
        protocol_prefix = (
            protocol[0] if isinstance(protocol, (list, tuple)) else protocol
        )
        files = fsspec.open_files(
            [f"{protocol_prefix}://{path}" for path in dir_fs.ls(dir_path)]
        )
        all_ignore_regex = re.compile("|".join(ignore)) if ignore else None

        with tempfile.TemporaryDirectory() as staging_dir:
            staging_path = Path(staging_dir)
            for openable_file in files:
                name = Path(openable_file.path).name
                if all_ignore_regex and all_ignore_regex.search(openable_file.path):
                    logger.debug(
                        f"Ignoring {openable_file.path} because it matched {all_ignore_regex}"
                    )
                    continue

                local_path = self._sync_local_path(openable_file, staging_path)
                try:
                    response = self.zenodo_client.create_bucket_file(
                        bucket_url=bucket_url,
                        file_path=local_path,
                    )
                    logger.info(f"Uploading to {bucket_url}/{name} got {response.text}")
                finally:
                    # Remove the local copy if we put it in the temporary staging area
                    if local_path.is_relative_to(staging_path):
                        local_path.unlink(missing_ok=True)

        return ContentComplete(
            record_id=self.record_id, zenodo_client=self.zenodo_client
        )


class ContentComplete(State):
    """Now that we've uploaded all the data, we need to update metadata."""

    def update_metadata(self):
        """Copy over old metadata and update publication date.

        We need to make sure there is complete metadata, including a publication date.

        To do this, we:

        1. use the *legacy* API to get the concept record ID associated with the draft
        2. use the *new* API to get the latest record associated with the concept
        3. use the *legacy* API to get the metadata from the latest record
        4. use the *legacy* API to update the draft's metadata

        Since we are using the legacy API to publish, we need the legacy
        metadata format. But the legacy concept DOI -> published record mapping
        is broken, so we have to take a detour through the new API.
        """
        deposition_info = self.zenodo_client.get_deposition(self.record_id)
        concept_rec_id = deposition_info.conceptrecid
        concept_info = self.zenodo_client.get_record(concept_rec_id)
        latest_published_deposition = self.zenodo_client.get_deposition(
            concept_info.id_
        )
        base_metadata = {
            k: v
            for k, v in latest_published_deposition.metadata.model_dump().items()
            if k not in {"doi", "prereserve_doi", "publication_date"}
        }
        logger.info(
            f"Using metadata from {latest_published_deposition.id_} to publish {self.record_id}..."
        )
        pub_date = {"publication_date": datetime.date.today().isoformat()}
        metadata = _LegacyMetadata(**(base_metadata | pub_date))
        self.zenodo_client.update_deposition_metadata(self.record_id, metadata=metadata)
        return CompleteDraft(record_id=self.record_id, zenodo_client=self.zenodo_client)


class CompleteDraft(State):
    """Now that we've uploaded all the data, we can publish."""

    def publish(self) -> None:
        """Publish the draft."""
        self.zenodo_client.publish_deposition(self.record_id)

    def get_html_url(self):
        """A URL for viewing this draft."""
        return self.zenodo_client.get_deposition(self.record_id).links.html


@click.command(
    help=__doc__,
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.option(
    "--env",
    type=click.Choice([SANDBOX, PRODUCTION], case_sensitive=False),
    default=SANDBOX,
    help="Whether to use the Zenodo sandbox server (for testing) or the production server.",
    show_default=True,
)
@click.option(
    "--source-dir",
    type=str,
    required=True,
    help="Path to a directory whose contents will be uploaded to Zenodo. "
    "Subdirectories are ignored. Accepts GCS or S3 URLs as well. Prefix remote paths "
    "with e.g. gs:// or s3://.",
)
@click.option(
    "--ignore",
    multiple=True,
    help="Filenames that match these regex patterns will be ignored.",
)
@click.option(
    "--publish/--no-publish",
    default=False,
    help="Whether to publish the new record without confirmation, or leave it as a "
    "draft to be reviewed and approved manually.",
    show_default=True,
)
def pudl_zenodo_data_release(
    env: str, source_dir: str, publish: bool, ignore: tuple[str]
):
    """Publish a new PUDL data release to Zenodo."""
    zenodo_client = ZenodoClient(env)
    if env == SANDBOX:
        rec_id = 5563
    elif env == PRODUCTION:
        rec_id = 3653158
    else:
        raise ValueError(f"{env=}, expected {SANDBOX} or {PRODUCTION}")
    completed_draft = (
        InitialDataset(zenodo_client=zenodo_client, record_id=rec_id)
        .get_empty_draft()
        .sync_directory(source_dir, ignore)
        .update_metadata()
    )

    if publish:
        completed_draft.publish()
        logger.info(f"Published at {completed_draft.get_html_url()}")
    else:
        logger.info(f"Completed draft at {completed_draft.get_html_url()}")


if __name__ == "__main__":
    pudl_zenodo_data_release()
