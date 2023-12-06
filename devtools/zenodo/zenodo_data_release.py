#!/usr/bin/env python
import os
from pathlib import Path

import click
import requests
from pydantic import AnyHttpUrl

from pudl.workspace.datastore import ZenodoDoi

# All of these pertaining
ZENODO_TOKEN: str = os.environ["ZENODO_SANDBOX_TOKEN_PUBLISH"]
ZENODO_API_BASE: AnyHttpUrl = AnyHttpUrl("https://sandbox.zenodo.org/api")
CONCEPT_DOI: ZenodoDoi = ZenodoDoi("10.5072/zenodo.5563")
CONCEPT_REC_ID: str = CONCEPT_DOI.split(".")[-1]
HEADERS = {"Content-Type": "application/json"}
AUTH_PARAM = {"access_token": ZENODO_TOKEN}


def get_current_record():
    response = requests.get(
        f"{ZENODO_API_BASE}/records{CONCEPT_REC_ID}",
        params=AUTH_PARAM,
        headers=HEADERS,
    )
    return response


def discard_draft():
    rec_id = get_current_record.json()["id"]
    response = requests.get(
        f"{ZENODO_API_BASE}/deposit/depositions/{rec_id}/actions/discard",
        params=AUTH_PARAM,
        headers=HEADERS,
    )
    return response


def create_draft():
    """Create a new draft deposit based on the current version."""
    response = requests.get(
        str(ZENODO_API_BASE),
    )
    return response


def wipe_draft():
    """Remove all files from the current draft."""


@click.command()
@click.option(
    "--sandbox/--production",
    "sandbox",
    type=bool,
    default=True,  # Change this to False once it's really working...
    help="Use the Zenodo sandbox rather than the production server.",
)
def pudl_zenodo_data_release(sandbox: bool):
    """Publish a new PUDL data release to Zenodo."""
    pudl_out = Path(os.environ["PUDL_OUTPUT"])

    pass
