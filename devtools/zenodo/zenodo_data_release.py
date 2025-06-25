#!/usr/bin/env python
"""Script to sync a directory up to Zenodo."""

import datetime
import logging
import os
import re
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import IO

import click
import fsspec
import requests
from pydantic import AnyHttpUrl, BaseModel, Field

SANDBOX = "sandbox"
PRODUCTION = "production"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s: %(levelname)s - %(message)s (%(filename)s:%(lineno)s)",
)
logger = logging.getLogger()


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

    def retry_request(self, *, method, url, max_tries=6, timeout=2, **kwargs):
        """Wrap requests.request in retry logic.

        Passes method, url, and **kwargs to requests.request.
        """
        for try_num in range(1, max_tries):
            try:
                return requests.request(
                    method=method, url=url, timeout=timeout**try_num, **kwargs
                )
            except (requests.RequestException, OSError) as e:
                logger.warning(
                    f"Attempt #{try_num} Got {e}, retrying in {timeout**try_num} s"
                )
                time.sleep(timeout**try_num)

        # don't catch errors on the last try.
        return requests.request(
            method=method, url=url, timeout=timeout**max_tries, **kwargs
        )

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

        Replaces the existing metadata completely - so make sure to pass in
        complete metadata. You cannot update metadata fields one at a time.
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
        self, bucket_url: AnyHttpUrl, file_name: str, file_content: IO[bytes]
    ) -> requests.Response:
        """LEGACY API: Upload a file to a deposition's file bucket.

        Prefer this over the /deposit/depositions/{id}/files endpoint because
        it allows for files >100MB.
        """
        url = f"{bucket_url}/{file_name}"
        logger.info(f"Uploading file to {url}")

        # don't worry about timeout=5s - that's "time for the server to
        # connect", not "time to upload whole file."
        response = self.retry_request(
            method="PUT",
            url=url,
            headers=self.auth_headers,
            data=file_content,
            stream=True,
        )
        return response

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

    Provides an abstraction layer that hides Zenodo's data model from the
    caller.

    Subclasses + their limited method definitions provide a way to avoid
    calling the operations in the wrong order.
    """

    record_id: int
    zenodo_client: ZenodoClient


class InitialDataset(State):
    """Represent initial dataset state.

    At this point, we don't know if there is an existing draft or not - the
    only thing we can do is try to get a fresh draft.
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

    def _open_fsspec_file(self, openable_file: fsspec.core.OpenFile) -> IO[bytes]:
        """Open a file that may be remote.

        If the fsspec file is local already, just return the output of the
        .open() method, which returns something file-like.

        Otherwise, download to a tempfile and return the tempfile, which is
        also file-like.

        This is similar to just using fsspec cache in that it avoids
        re-downloading the source file when the upload fails, but lets us log
        how long downloads take.
        """
        logger.info(f"Reading {openable_file.full_name}")
        if "local" in openable_file.fs.protocol:
            return openable_file.open()

        tmpfile = tempfile.NamedTemporaryFile()  # noqa: SIM115
        openable_file.fs.get(openable_file.path, tmpfile.name)
        return tmpfile

    def sync_directory(self, source_dir: str, ignore: tuple[str]) -> "ContentComplete":
        """Read data from source_dir and upload it."""
        logger.info(f"Syncing files from {source_dir} to draft {self.record_id}...")
        bucket_url = self.zenodo_client.get_deposition(self.record_id).links.bucket

        # TODO: if we want to have 'resumable' archives - we would need to get
        # hashes from iter_files() and we'd also need to do deletion of all the
        # extra files in the draft. in that case we won't want to delete all
        # the files before getting to this state, so EmptyDraft would become
        # InProgressDraft.

        maybe_dir = fsspec.open_files(source_dir, mode="rb")[0]
        if not maybe_dir.fs.isdir(maybe_dir.path):
            raise ValueError(f"{source_dir} is not a directory!")

        files = fsspec.open_files(
            [
                f"{maybe_dir.fs.protocol[0]}://{p}"
                for p in maybe_dir.fs.ls(maybe_dir.path)
            ]
        )
        all_ignore_regex = re.compile("|".join(ignore))
        for openable_file in files:
            name = Path(openable_file.path).name
            if all_ignore_regex.search(openable_file.path):
                logger.info(
                    f"Ignoring {openable_file.path} because it matched {all_ignore_regex}"
                )
                continue
            with self._open_fsspec_file(openable_file) as upload_source:
                response = self.zenodo_client.create_bucket_file(
                    bucket_url=bucket_url, file_name=name, file_content=upload_source
                )
            logger.info(f"Uploading to {bucket_url}/{name} got {response.text}")

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


@click.command()
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
    "Subdirectories are ignored. Can get files from GCS as well - just prefix "
    "with gs://. NOTE: nightly build outputs are NOT suitable for creating a Zenodo "
    "data release, as they include hundreds of individual Parquet files, which we "
    "archive on Zenodo as a single zipfile. Check what files should actually be "
    "distributed. E.g. it may be *.log *.zip *.json ",
)
@click.option(
    "--ignore",
    multiple=True,
    help="Filenames that match these regex patterns will be ignored.",
)
@click.option(
    "--publish/--no-publish",
    default=False,
    help="Whether to publish the new record without confirmation, or leave it as a draft to be reviewed.",
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
