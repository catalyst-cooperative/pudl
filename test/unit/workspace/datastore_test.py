"""Unit tests for Datastore module."""

import json
import re
import unittest
from typing import Any

import responses

from pudl.workspace import datastore
from pudl.workspace.resource_cache import PudlResourceKey


def _make_resource(name: str, **partitions) -> dict[str, Any]:
    """Returns json representation of a resource."""
    return {
        "name": name,
        "path": f"http://localhost/{name}",
        "parts": dict(partitions),
    }


def _make_descriptor(
    dataset: str, doi: str, *resources: dict[str, Any]
) -> datastore.DatapackageDescriptor:
    """Returns new instance of DatapackageDescriptor containing given resources.

    This is a helper for quickly making descriptors for unit testing. You can use
    it in a following fashion:

    desc = _make_descriptor("dataset_name", "doi-123", _make_resource(...), _make_resource(...), ...)

    Args:
        dataset: name of the dataset
        doi: doi identifier
        resources: list of resources that should be attached to the resource. This should
         be json representation, e.g. constructed by calling _make_resource().
    """
    return datastore.DatapackageDescriptor(
        {"resources": list(resources)},
        dataset=dataset,
        doi=doi,
    )


class TestDatapackageDescriptor(unittest.TestCase):
    """Unit tests for the DatapackageDescriptor class."""

    def test_get_partition_filters(self):
        desc = _make_descriptor(
            "blabla",
            "doi-123",
            _make_resource("foo", group="first", color="red"),
            _make_resource("bar", group="first", color="blue"),
            _make_resource("baz", group="second", color="black", order=1),
        )
        self.assertEqual(
            [
                dict(group="first", color="red"),
                dict(group="first", color="blue"),
                dict(group="second", color="black", order=1),
            ],
            list(desc.get_partition_filters()),
        )
        self.assertEqual(
            [
                dict(group="first", color="red"),
                dict(group="first", color="blue"),
            ],
            list(desc.get_partition_filters(group="first")),
        )
        self.assertEqual(
            [
                dict(group="first", color="blue"),
            ],
            list(desc.get_partition_filters(color="blue")),
        )
        self.assertEqual(
            [], list(desc.get_partition_filters(color="blue", group="second"))
        )

    def test_get_resource_path(self):
        """Check that get_resource_path returns correct paths."""
        desc = _make_descriptor(
            "blabla",
            "doi-123",
            _make_resource("foo", group="first", color="red"),
            _make_resource("bar", group="first", color="blue"),
        )
        self.assertEqual("http://localhost/foo", desc.get_resource_path("foo"))
        self.assertEqual("http://localhost/bar", desc.get_resource_path("bar"))
        # The following resource does not exist and should throw KeyError
        self.assertRaises(KeyError, desc.get_resource_path, "other")

    def test_get_resources_filtering(self):
        """Verifies correct operation of get_resources()."""
        desc = _make_descriptor(
            "data",
            "doi-123",
            _make_resource("foo", group="first", color="red"),
            _make_resource("bar", group="first", color="blue", rank=5),
            _make_resource(
                "baz", group="second", color="blue", rank=5, mood="VeryHappy"
            ),
        )
        self.assertEqual(
            [
                PudlResourceKey("data", "doi-123", "foo"),
                PudlResourceKey("data", "doi-123", "bar"),
                PudlResourceKey("data", "doi-123", "baz"),
            ],
            list(desc.get_resources()),
        )
        # Simple filtering by one attribute.
        self.assertEqual(
            [
                PudlResourceKey("data", "doi-123", "foo"),
                PudlResourceKey("data", "doi-123", "bar"),
            ],
            list(desc.get_resources(group="first")),
        )
        # Filter by two attributes
        self.assertEqual(
            [
                PudlResourceKey("data", "doi-123", "bar"),
            ],
            list(desc.get_resources(group="first", rank=5)),
        )
        # Attributes that do not match anything
        self.assertEqual(
            [],
            list(desc.get_resources(group="second", shape="square")),
        )
        # Search attribute values are cast to lowercase strings
        self.assertEqual(
            [
                PudlResourceKey("data", "doi-123", "baz"),
            ],
            list(desc.get_resources(rank="5", mood="VERYhappy")),
        )
        # Test lookup by name
        self.assertEqual(
            [
                PudlResourceKey("data", "doi-123", "foo"),
            ],
            list(desc.get_resources("foo")),
        )

    def test_json_string_representation(self):
        """Checks that json representation parses to the same dict."""
        desc = _make_descriptor(
            "data",
            "doi-123",
            _make_resource("foo", group="first"),
            _make_resource("bar", group="second"),
            _make_resource("baz"),
        )
        self.assertEqual(
            {
                "resources": [
                    {
                        "name": "foo",
                        "path": "http://localhost/foo",
                        "parts": {"group": "first"},
                    },
                    {
                        "name": "bar",
                        "path": "http://localhost/bar",
                        "parts": {"group": "second"},
                    },
                    {
                        "name": "baz",
                        "path": "http://localhost/baz",
                        "parts": {},
                    },
                ],
            },
            json.loads(desc.get_json_string()),
        )


class MockableZenodoFetcher(datastore.ZenodoFetcher):
    """Test friendly version of ZenodoFetcher.

    Allows populating _descriptor_cache at the initialization time.
    """

    def __init__(
        self, descriptors: dict[str, datastore.DatapackageDescriptor], **kwargs
    ):
        """Construct a test-friendly ZenodoFetcher with descriptors pre-loaded."""
        super().__init__(**kwargs, _descriptor_cache=descriptors)


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

    def test_doi_format_is_correct(self):
        """Verifies ZenodoFetcher DOIs have correct format and are not sandbox DOIs.

        Sandbox DOIs are only meant for use in testing and development, and should not
        be checked in, thus this test will fail if a sandbox DOI with prefix 10.5072 is
        identified.
        """
        ds = datastore.ZenodoFetcher()
        self.assertTrue(ds.get_known_datasets())
        for dataset in ds.get_known_datasets():
            doi = ds.get_doi(dataset)
            self.assertFalse(
                re.fullmatch(r"10\.5072/zenodo\.[0-9]{5,10}", doi),
                msg=f"Zenodo sandbox DOI found for {dataset}: {doi}",
            )
            self.assertTrue(
                re.fullmatch(r"10\.5281/zenodo\.[0-9]{5,10}", doi),
                msg=f"Zenodo production DOI for {dataset} is {doi}",
            )

    def test_get_known_datasets(self):
        """Call to get_known_datasets() produces the expected results."""
        self.assertEqual(
            sorted(datastore.ZenodoFetcher().zenodo_dois),
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
