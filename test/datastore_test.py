"""Exercise the functionality in the datastore management module."""
import random
import re

import datapackage

from pudl.workspace import datastore as datastore


class TestDatastore:
    """Test the Datastore functions."""

    def test_doi_to_url(self, pudl_datastore_fixture):
        """Get the DOI url right."""
        number = random.randint(100000, 999999)  # nosec
        fake_doi = f"10.5072/zenodo.{number}"

        assert pudl_datastore_fixture.doi_to_url(fake_doi) == \
            f"{pudl_datastore_fixture.api_root}/deposit/depositions/{number}"

    def test_sandbox_vs_production_dois(self):
        """Check that Sandbox/Production DOIs are formatted as expected."""
        for doi in datastore.DOI["sandbox"].values():
            assert re.match(r"^10\.5072/zenodo\.[0-9]{5,10}$", doi) is not None  # noqa: FS003
        for doi in datastore.DOI["production"].values():
            assert re.match(r"^10\.5281/zenodo\.[0-9]{5,10}$", doi) is not None  # noqa: FS003

    def test_all_datapackage_json_available(self, pudl_datastore_fixture):
        """
        Ensure that every recorded DOI has a valid datapackage.json file.

        Integration test!
        """

        def test_it(client, doi):
            jsr = client.remote_datapackage_json(doi)
            dp = datapackage.Package(jsr)
            if not dp.valid:
                raise AssertionError(
                    f"Invalid datapackage.json found for {doi} "
                    f"({jsr['name']})."
                )

        for doi in pudl_datastore_fixture._dois.values():
            test_it(pudl_datastore_fixture, doi)
