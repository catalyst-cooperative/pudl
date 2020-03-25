"""
Exercise the functionality in the datastore management module.
"""

import random
import pudl.workspace.datastore as datastore

class TestDatastore:
    ds = datastore.Datastore(sandbox=True)

    def test_doi_to_url(self):
        """Get the DOI url right"""
        number = random.randint(100000, 999999)
        fake_doi = "10.5072/zenodo.%d" % number

        assert ds.doi_to_url("fake_doi") == \
            "https://sandbox.zenodo.org//api/deposit/depositions/%d" % number
