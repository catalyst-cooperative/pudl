"""Unit tests for Datastore module."""

import json
import re
import unittest

import responses

from pudl.workspace import datastore
from pudl.workspace.resource_cache import PudlResourceKey


class TestDatapackageDescriptor(unittest.TestCase):
    """Unit tests for the DatapackageDescriptor class."""

    MOCK_DATAPACKAGE = {
        "resources": [
            {
                "name": "first-red",
                "path": "http://localhost/first",
                "parts": {"color": "red", "order": 1},
            },
            {
                "name": "second-blue",
                "path": "http://localhost/second",
                "parts": {"color": "blue", "order": 2},
            },
            {
                "name": "mixed-case",
                "path": "http://localhost/mixed",
                "parts": {"upper": "UPPER", "lower": "lower", "mixed": "miXED"},
            },
        ]
    }

    def setUp(self):
        """Builds DatapackageDescriptor based on MOCK_DATAPACKAGE."""
        self.descriptor = datastore.DatapackageDescriptor(
            self.MOCK_DATAPACKAGE, dataset="epacems", doi="123"
        )

    def test_get_resource_path_for_existing_resources(self):
        """Checks that get_resource_path() works."""
        self.assertEqual(
            "http://localhost/first", self.descriptor.get_resource_path("first-red")
        )
        self.assertEqual(
            "http://localhost/second", self.descriptor.get_resource_path("second-blue")
        )

    def test_get_resource_path_throws_exception(self):
        """Verifies that KeyError is thrown when resource does not exist."""
        self.assertRaises(
            KeyError, self.descriptor.get_resource_path, "third-orange"
        )  # this resource does not exist

    def test_get_resources_filtering(self):
        """Verifies correct operation of get_resources()."""
        self.assertEqual(
            [
                PudlResourceKey("epacems", "123", "first-red"),
                PudlResourceKey("epacems", "123", "second-blue"),
                PudlResourceKey("epacems", "123", "mixed-case"),
            ],
            list(self.descriptor.get_resources()),
        )
        self.assertEqual(
            [PudlResourceKey("epacems", "123", "first-red")],
            list(self.descriptor.get_resources(color="red")),
        )
        self.assertEqual([], list(self.descriptor.get_resources(flavor="blueberry")))

    def test_get_resources_filtering_case_insensitive(self):
        """Verifies that values for the parts are treated case-insensitive."""
        self.assertEqual(
            [PudlResourceKey("epacems", "123", "mixed-case")],
            list(self.descriptor.get_resources(upper="uppeR")),
        )
        self.assertEqual(
            [PudlResourceKey("epacems", "123", "mixed-case")],
            list(self.descriptor.get_resources(upper="uppeR", lower="Lower")),
        )
        # Lookups are, however, case-sensitive for the keys.
        self.assertEqual([], list(self.descriptor.get_resources(Upper="UPPER")))

    def test_get_resources_by_name(self):
        """Verifies that get_resources() work when name is specified."""
        self.assertEqual(
            [PudlResourceKey("epacems", "123", "second-blue")],
            list(self.descriptor.get_resources(name="second-blue")),
        )

    def test_json_string_representation(self):
        """Checks that json representation parses to the same dict."""
        self.assertEqual(
            self.MOCK_DATAPACKAGE, json.loads(self.descriptor.get_json_string())
        )


class MockableZenodoFetcher(datastore.ZenodoFetcher):
    """Test friendly version of ZenodoFetcher.

    Allows populating _descriptor_cache at the initialization time.
    """

    def __init__(
        self, descriptors: dict[str, datastore.DatapackageDescriptor], **kwargs
    ):
        """Construct a test-friendly ZenodoFetcher with descriptors pre-loaded."""
        super().__init__(**kwargs)
        self._descriptor_cache = dict(descriptors)


class TestZenodoFetcher(unittest.TestCase):
    """Unit tests for ZenodoFetcher class."""

    MOCK_EPACEMS_DEPOSITION = {
        "files": [
            {"filename": "random.zip"},
            {
                "filename": "datapackage.json",
                "links": {"download": "http://localhost/my/datapackage.json"},
            },
        ]
    }

    MOCK_EPACEMS_DATAPACKAGE = {
        "resources": [
            {
                "name": "first",
                "path": "http://localhost/first",
                "hash": "6f1ed002ab5595859014ebf0951522d9",
            },  # md5sum of "blah"
            {
                "name": "second",
                "path": "http://localhost/second",
                "hash": "6f1ed002ab5595859014ebf0951522d9",
            },
        ]
    }
    PROD_EPACEMS_DOI = "10.5281/zenodo.6910058"
    PROD_EPACEMS_ZEN_ID = 6910058  # This is the last numeric part of doi

    def setUp(self):
        """Constructs mockable Zenodo fetcher based on MOCK_EPACEMS_DATAPACKAGE."""
        self.fetcher = MockableZenodoFetcher(
            descriptors={
                self.PROD_EPACEMS_DOI: datastore.DatapackageDescriptor(
                    self.MOCK_EPACEMS_DATAPACKAGE,
                    dataset="epacems",
                    doi=self.PROD_EPACEMS_DOI,
                )
            }
        )

    def test_sandbox_doi_format_is_correct(self):
        """Verifies that sandbox ZenodoFetcher DOIs have the right format."""
        ds = datastore.ZenodoFetcher(sandbox=True)
        self.assertTrue(ds.get_known_datasets())
        for dataset in ds.get_known_datasets():
            print(f"doi for {dataset} is {ds.get_doi(dataset)}")
            self.assertTrue(
                re.fullmatch(
                    r"10\.5072/zenodo\.[0-9]{5,10}", ds.get_doi(dataset)
                ),  # noqa: FS003
                msg=f"doi for {dataset} is {ds.get_doi(dataset)}",
            )

    def test_prod_doi_format_is_correct(self):
        """Verifies that production ZenodoFetcher DOIs have the right format."""
        ds = datastore.ZenodoFetcher(sandbox=False)
        self.assertTrue(ds.get_known_datasets())
        for dataset in ds.get_known_datasets():
            self.assertTrue(
                re.fullmatch(
                    r"10\.5281/zenodo\.[0-9]{5,10}", ds.get_doi(dataset)
                ),  # noqa: FS003
                msg=f"doi for {dataset} is {ds.get_doi(dataset)}",
            )

    def test_get_known_datasets(self):
        """Call to get_known_datasets() produces the expected results."""
        self.assertEqual(
            sorted(datastore.ZenodoFetcher.DOI["production"]),
            self.fetcher.get_known_datasets(),
        )

    def test_doi_of_prod_epacems_matches(self):
        """Most of the tests assume specific DOI for production epacems dataset.

        This test verifies that the expected value is in use.
        """
        self.assertEqual(self.PROD_EPACEMS_DOI, self.fetcher.get_doi("epacems"))

    @responses.activate
    def test_get_descriptor_http_calls(self):
        """Tests that correct http requests are fired when loading datapackage.json."""
        fetcher = datastore.ZenodoFetcher()
        responses.add(
            responses.GET,
            f"https://zenodo.org/api/deposit/depositions/{self.PROD_EPACEMS_ZEN_ID}",
            json=self.MOCK_EPACEMS_DEPOSITION,
        )
        responses.add(
            responses.GET,
            "http://localhost/my/datapackage.json",
            json=self.MOCK_EPACEMS_DATAPACKAGE,
        )
        desc = fetcher.get_descriptor("epacems")
        self.assertEqual(self.MOCK_EPACEMS_DATAPACKAGE, desc.datapackage_json)
        # self.assertTrue(responses.assert_call_count("http://localhost/my/datapackage.json", 1))

    def test_get_resource_key(self):
        """Tests normal operation of get_resource_key()."""
        self.assertEqual(
            PudlResourceKey("epacems", self.PROD_EPACEMS_DOI, "blob.zip"),
            self.fetcher.get_resource_key("epacems", "blob.zip"),
        )

    def test_get_resource_key_for_unknown_dataset_fails(self):
        """When get_resource_key() is called for unknown dataset it throws KeyError."""
        self.assertRaises(
            KeyError, self.fetcher.get_resource_key, "unknown", "blob.zip"
        )

    @responses.activate
    def test_get_resource(self):
        """Test that get_resource() calls expected http request and returns content."""
        responses.add(responses.GET, "http://localhost/first", body="blah")
        res = self.fetcher.get_resource(
            PudlResourceKey("epacems", self.PROD_EPACEMS_DOI, "first")
        )
        self.assertEqual(b"blah", res)

    @responses.activate
    def test_get_resource_with_invalid_checksum(self):
        """Test that retrieving resource with bad checksum raises ChecksumMismatch."""
        responses.add(responses.GET, "http://localhost/first", body="wrongContent")
        res = PudlResourceKey("epacems", self.PROD_EPACEMS_DOI, "first")
        self.assertRaises(datastore.ChecksumMismatch, self.fetcher.get_resource, res)

    def test_get_resource_with_nonexistent_resource_fails(self):
        """If resource does not exist, get_resource() throws KeyError."""
        res = PudlResourceKey("epacems", self.PROD_EPACEMS_DOI, "nonexistent")
        self.assertRaises(KeyError, self.fetcher.get_resource, res)


# TODO(rousik): add unit tests for Datasource class as well
