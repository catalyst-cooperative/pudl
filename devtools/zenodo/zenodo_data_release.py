#!/usr/bin/env python
"""Script to sync a directory up to Zenodo."""
import datetime
import logging
import os
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

import click
import requests

SANDBOX = "sandbox"
PRODUCTION = "production"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


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
            self.auth_headers = {
                "Authorization": f"Bearer {os.environ['ZENODO_SANDBOX_TOKEN_PUBLISH']}"
            }
        elif env == PRODUCTION:
            self.base_url = "https://zenodo.org/api"
            self.auth_headers = {
                "Authorization": f"Bearer {os.environ['ZENODO_TOKEN_PUBLISH']}"
            }
        else:
            raise ValueError(
                f"Got unexpected {env=}, expected {SANDBOX} or {PRODUCTION}"
            )

    def get_deposition(self, deposition_id: int) -> requests.Response:
        """LEGACY API: Get JSON describing a deposition.

        Depositions can be published *or* unpublished.
        """
        return requests.get(
            f"{self.base_url}/deposit/depositions/{deposition_id}",
            headers=self.auth_headers,
            timeout=10,
        )

    def get_record(self, record_id: int) -> requests.Response:
        """NEW API: Get JSON describing a record.

        All records are published records.
        """
        return requests.get(
            f"{self.base_url}/records/{record_id}",
            headers=self.auth_headers,
            timeout=10,
        )

    def new_record_version(self, record_id: int) -> requests.Response:
        """NEW API: get or create the draft associated with a record ID.

        Finds the latest record in the concept that record_id points to, and
        makes a new version unless one exists already.
        """
        return requests.post(
            f"{self.base_url}/records/{record_id}/versions",
            headers=self.auth_headers,
            timeout=10,
        )

    def update_deposition_metadata(
        self, deposition_id: int, metadata: dict
    ) -> requests.Response:
        """LEGACY API: Update deposition metadata.

        Replaces the existing metadata completely - so make sure to pass in
        complete metadata. You cannot update metadata fields one at a time.
        """
        url = f"{self.base_url}/deposit/depositions/{deposition_id}"
        data = {"metadata": metadata}
        response = requests.put(url, json=data, headers=self.auth_headers, timeout=10)
        return response

    def delete_deposition_file(self, deposition_id: int, file_id) -> requests.Response:
        """LEGACY API: Delete file from deposition.

        Note: file_id is not always the file name.
        """
        return requests.delete(
            f"{self.base_url}/deposit/depositions/{deposition_id}/files/{file_id}",
            headers=self.auth_headers,
            timeout=10,
        )

    def create_deposition_file(
        self, deposition_id: int, file_name: str, file_content: BinaryIO
    ) -> requests.Response:
        """LEGACY API: Create file in deposition."""
        url = f"{self.base_url}/deposit/depositions/{deposition_id}/files"
        files = {"file": file_content, "name": file_name}
        response = requests.post(
            url,
            headers=self.auth_headers,
            files=files,
            timeout=1_000,
        )
        return response

    def publish_deposition(self, deposition_id: int) -> requests.Response:
        """LEGACY API: publish deposition."""
        resp = requests.post(
            f"{self.base_url}/deposit/depositions/{deposition_id}/actions/publish",
            headers=self.auth_headers,
            timeout=10,
        )
        return resp


class SourceData:
    """Interface for retrieving the data we actually want to store."""

    def iter_files(self) -> Iterable[tuple[str, BinaryIO]]:
        """Get the names and contents of each file in the SourceData."""
        raise NotImplementedError


@dataclass
class LocalSource(SourceData):
    """Get files from local directory."""

    path: Path

    def iter_files(self) -> Iterable[tuple[str, BinaryIO]]:
        """Loop through all files in the directory."""
        for f in self.path.iterdir():
            yield f.name, f.open()


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
        latest_record = self.zenodo_client.get_record(self.record_id).json()
        new_version = self.zenodo_client.new_record_version(latest_record["id"]).json()
        new_rec_id = new_version["id"]
        existing_files = new_version["files"]
        logger.info(
            f"Draft {new_rec_id} has {len(existing_files)} existing files, deleting..."
        )
        new_version = self.zenodo_client.new_record_version(self.record_id).json()
        for f in existing_files:
            self.zenodo_client.delete_deposition_file(new_rec_id, f["id"])
        return EmptyDraft(record_id=new_rec_id, zenodo_client=self.zenodo_client)


class EmptyDraft(State):
    """We can only sync the directory once we've gotten an empty draft."""

    def sync_directory(self, source_dir: SourceData) -> "CompleteDraft":
        """Read data from source_dir and upload it."""
        logger.info(f"Syncing files from {source_dir} to draft {self.record_id}...")
        for name, blob in source_dir.iter_files():
            self.zenodo_client.create_deposition_file(self.record_id, name, blob)

        return CompleteDraft(record_id=self.record_id, zenodo_client=self.zenodo_client)


class CompleteDraft(State):
    """Now that we've uploaded all the data, we can publish."""

    def publish(self) -> None:
        """Publish the draft.

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
        deposition_info = self.zenodo_client.get_deposition(self.record_id).json()
        concept_rec_id = deposition_info["conceptrecid"]
        concept_info = self.zenodo_client.get_record(concept_rec_id).json()
        latest_published_deposition = self.zenodo_client.get_deposition(
            concept_info["id"]
        ).json()
        base_metadata = {
            k: v
            for k, v in latest_published_deposition["metadata"].items()
            if k not in {"doi", "prereserve_doi", "publication_date"}
        }
        logger.info(
            f"Using metadata from {latest_published_deposition['id']} to publish {self.record_id}..."
        )
        pub_date = {"publication_date": datetime.date.today().isoformat()}
        self.zenodo_client.update_deposition_metadata(
            self.record_id, metadata=base_metadata | pub_date
        )
        self.zenodo_client.publish_deposition(self.record_id)

    def get_html_url(self):
        """A URL for viewing this draft."""
        return self.zenodo_client.get_deposition(self.record_id).json()["links"]["html"]


@click.command()
@click.option(
    "--env",
    type=click.Choice([SANDBOX, PRODUCTION], case_sensitive=False),
    default=SANDBOX,
    help="Use the Zenodo sandbox rather than the production server.",
)
@click.option(
    "--concept-rec-id",
    type=int,
    required=True,
    help="The concept record ID we'd like to sync to.",
)
@click.option(
    "--source-dir",
    type=Path,
    required=True,
    help="What directory to sync up to this concept DOI.",
)
@click.option(
    "--publish/--no-publish",
    default=False,
    help="Whether to publish automatically after syncing, or to give you a chance to review.",
)
def pudl_zenodo_data_release(env: bool, concept_rec_id, source_dir, publish):
    """Publish a new PUDL data release to Zenodo."""
    # TODO (daz): if the source-dir is actually an S3 or GCS uri, then, well. Do that instead.
    source_data = LocalSource(path=source_dir)
    zenodo_client = ZenodoClient(env)
    completed_draft = (
        InitialDataset(zenodo_client=zenodo_client, record_id=concept_rec_id)
        .get_empty_draft()
        .sync_directory(source_data)
    )

    if publish:
        completed_draft.publish()
        logger.info(f"Published at {completed_draft.get_html_url()}")
    else:
        logger.info(f"Completed draft at {completed_draft.get_html_url()}")


if __name__ == "__main__":
    pudl_zenodo_data_release()
