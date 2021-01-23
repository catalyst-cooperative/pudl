"""Integration tests that verify that zenodo datapackages are correct."""

import pytest
from requests.exceptions import ConnectionError
from urllib3.exceptions import MaxRetryError

from pudl.workspace.datastore import Datastore


class TestZenodoDatapackages:
    """Ensure both prod & sandbox Datastores point to valid datapackage descriptors."""

    @pytest.mark.xfail(raises=(MaxRetryError, ConnectionError))
    def test_sandbox_datapackages(self):
        """All datasets point to valid descriptors and each specifies non-zero resources."""
        ds = Datastore(sandbox=True)
        for dataset in ds.get_known_datasets():
            desc = ds.get_datapackage_descriptor(dataset)
            assert list(desc.get_resources())

    @pytest.mark.xfail(raises=(MaxRetryError, ConnectionError))
    def test_prod_datapackages(self):
        """All datasets point to valid descriptors and each specifies non-zero resources."""
        ds = Datastore(sandbox=False)
        for dataset in ds.get_known_datasets():
            desc = ds.get_datapackage_descriptor(dataset)
            assert list(desc.get_resources())
