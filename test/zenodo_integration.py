"""Integration tests that verify that zenodo datapackages are correct."""

import unittest

from pudl.workspace.datastore import Datastore


class TestZenodoDatapackages(unittest.TestCase):
    """Ensures that both prod and sandbox Datastores point to valid datapackage descriptors."""

    def test_sandbox_datapackages(self):
        """All datasets point to valid descriptors and each specifies non-zero resources."""
        ds = Datastore(sandbox=True)
        for dataset in ds.get_known_datasets():
            desc = ds.get_datapackage_descriptor(dataset)
            self.assertTrue(list(desc.get_resources()))

    def test_prod_datapackages(self):
        """All datasets point to valid descriptors and each specifies non-zero resources."""
        ds = Datastore(sandbox=False)
        for dataset in ds.get_known_datasets():
            desc = ds.get_datapackage_descriptor(dataset)
            self.assertTrue(list(desc.get_resources()))
