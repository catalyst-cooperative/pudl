"""Integration tests that verify that Zenodo datapackages are correct."""

import pytest
from requests.exceptions import ConnectionError, RetryError
from urllib3.exceptions import MaxRetryError, ResponseError

from pudl.workspace.datastore import Datastore


class TestZenodoDatapackages:
    """Ensure all DOIs in Datastore point to valid datapackages."""

    @pytest.mark.xfail(
        raises=(
            MaxRetryError,
            ConnectionError,
            RetryError,
            ResponseError,
        )
    )
    def test_prod_datapackages(self):
        """All datasets point to valid descriptors with 1 or more resources."""
        ds = Datastore()
        for dataset in ds.get_known_datasets():
            desc = ds.get_datapackage_descriptor(dataset)
            assert list(desc.get_resources())
