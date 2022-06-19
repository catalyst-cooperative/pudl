"""Integration tests that verify that Zenodo datapackages are correct."""

import pytest

from pudl.workspace.datastore import Datastore


class TestZenodoDatapackages:
    """Ensure production & sandbox Datastores point to valid datapackages."""

    @pytest.mark.skip(reason="Zenodo API is unreliable, causing spurious CI failures.")
    def test_sandbox_datapackages(self):
        """All datasets point to valid descriptors with 1 or more resources."""
        ds = Datastore(sandbox=True)
        for dataset in ds.get_known_datasets():
            desc = ds.get_datapackage_descriptor(dataset)
            assert list(desc.get_resources())  # nosec: B101

    @pytest.mark.skip(reason="Zenodo API is unreliable, causing spurious CI failures.")
    def test_prod_datapackages(self):
        """All datasets point to valid descriptors with 1 or more resources."""
        ds = Datastore(sandbox=False)
        for dataset in ds.get_known_datasets():
            desc = ds.get_datapackage_descriptor(dataset)
            assert list(desc.get_resources())  # nosec: B101
