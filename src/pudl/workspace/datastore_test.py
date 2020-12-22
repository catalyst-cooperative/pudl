import re
import os
import shutil
import tempfile
import unittest
import hashlib
import responses
from pathlib import Path
from unittest.mock import patch
from unittest import mock
import json
from typing import Dict

from pudl.workspace import datastore
from pudl.workspace.resource_cache import PudlResourceKey


class TestDatapackageDescriptor(unittest.TestCase):
    MOCK_DATAPACKAGE = {
        "resources": [
            {"name": "first-red",
             "path": "http://localhost/first",
             "parts": {"color": "red", "order": 1}},
            {"name": "second-blue",
             "path": "http://localhost/second",
             "parts": {"color": "blue", "order": 2}},
        ]
    }
    def setUp(self):
        self.descriptor = datastore.DatapackageDescriptor(
            self.MOCK_DATAPACKAGE,
            dataset="epacems",
            doi="123")

    def testGetResourcePath(self):
        self.assertEqual(
            "http://localhost/first",
            self.descriptor.get_resource_path("first-red"))
        self.assertEqual(
            "http://localhost/second",
            self.descriptor.get_resource_path("second-blue"))
        self.assertRaises(
            KeyError, 
            self.descriptor.get_resource_path,
            "third-orange")  # this resource does not exist

    def testGetResources(self):
        res = list(self.descriptor.get_resources())
        self.assertEqual(
            [PudlResourceKey("epacems", "123", "first-red"),
             PudlResourceKey("epacems", "123", "second-blue")],
            list(self.descriptor.get_resources()))
        self.assertEqual(
            [PudlResourceKey("epacems", "123", "first-red")],
            list(self.descriptor.get_resources(color="red")))
        self.assertEqual(
            [],
            list(self.descriptor.get_resources(flavor="blueberry")))

    def testGetResourcesByName(self):
        self.assertEqual(
            [PudlResourceKey("epacems", "123", "second-blue")],
            list(self.descriptor.get_resources(name="second-blue")))

    def testGetJsonString(self):
        self.assertEqual(
            self.MOCK_DATAPACKAGE,
            json.loads(self.descriptor.get_json_string()))


class MockableZenodoFetcher(datastore.ZenodoFetcher):
    """Test friendly version of ZenodoFetcher.

    Allows populating _descriptor_cache at the initialization time.
    """
    def __init__(
        self,
        descriptors: Dict[str, datastore.DatapackageDescriptor],
        **kwargs):
        super().__init__(**kwargs)
        self._descriptor_cache = dict(descriptors)


class TestZenodoFetcher(unittest.TestCase):
    # mock http interactions
    MOCK_EPACEMS_DEPOSITION = {
        "files": [
            {"filename": "random.zip"},
            {
                "filename": "datapackage.json",
                "links": { 
                    "download": "http://localhost/my/datapackage.json" 
                },
            },
        ]
    }

    MOCK_EPACEMS_DATAPACKAGE = {
        "resources": [
            {"name": "first",
             "path": "http://localhost/first",
             "hash": "6f1ed002ab5595859014ebf0951522d9"},  # md5sum of "blah"
            {"name": "second",
             "path": "http://localhost/second",
             "hash": "6f1ed002ab5595859014ebf0951522d9"},
        ]
    }
    PROD_EPACEMS_DOI = "10.5281/zenodo.4127055"
    PROD_EPACEMS_ZEN_ID = 4127055  # This is the last numeric part of doi

    def setUp(self):
        self.fetcher = MockableZenodoFetcher(descriptors={
            self.PROD_EPACEMS_DOI: datastore.DatapackageDescriptor(
                self.MOCK_EPACEMS_DATAPACKAGE,
                dataset="epacems",
                doi=self.PROD_EPACEMS_DOI)})

    def testGetKnownDatasets(self):
        self.assertEqual(
                sorted(datastore.ZenodoFetcher.DOI["production"]),
                self.fetcher.get_known_datasets())

    def testProdEpacemsDoiMatches(self):
        """Most of the tests assume specific DOI for production epacems dataset.

        This test verifies that the expected value is in use.
        """
        self.assertEqual(self.PROD_EPACEMS_DOI, self.fetcher.get_doi("epacems"))

    @responses.activate
    def testGetDescriptorRequests(self):
        """Tests that the right http requests are fired when loading datapackage.json."""
        fetcher = datastore.ZenodoFetcher()
        responses.add(responses.GET, 
            f"https://zenodo.org/api/deposit/depositions/{self.PROD_EPACEMS_ZEN_ID}",
            json=self.MOCK_EPACEMS_DEPOSITION)
        responses.add(responses.GET,
            "http://localhost/my/datapackage.json",
            json=self.MOCK_EPACEMS_DATAPACKAGE)
        desc = fetcher.get_descriptor('epacems')
        self.assertEqual(self.MOCK_EPACEMS_DATAPACKAGE, desc.datapackage_json)
#        self.assertEqual(self.MOCK_EPACEMS_DATAPACKAGE, desc.datapackage_json)
#        self.assertTrue(responses.assert_call_count("http://localhost/my/datapackage.json", 1))

    def testGetResourceKey(self):
        self.assertEqual(
            PudlResourceKey("epacems", self.PROD_EPACEMS_DOI, "blob.zip"),
            self.fetcher.get_resource_key("epacems", "blob.zip"))

    def testGetResourceKeyForInvalidDataset(self):
        self.assertRaises(
            KeyError,
            self.fetcher.get_resource_key,
            "unknown", "blob.zip")

    @responses.activate
    def testGetResource(self):
        responses.add(responses.GET,
            "http://localhost/first", body="blah")
        res = self.fetcher.get_resource(
            PudlResourceKey("epacems", self.PROD_EPACEMS_DOI, "first"))
        self.assertEqual(b"blah", res)

    @responses.activate
    def testGetResourceWithInvalidChecksum(self):
        responses.add(responses.GET,
                "http://localhost/first", body="wrongContent")
        res = PudlResourceKey("epacems", self.PROD_EPACEMS_DOI, "first")
        self.assertRaises(ValueError, self.fetcher.get_resource, res)

    def testGetResourceWithNonexistentResource(self):
        res = PudlResourceKey("epacems", self.PROD_EPACEMS_DOI, "nonexistent")
        self.assertRaises(KeyError, self.fetcher.get_resource, res)

# TODO(rousik): add tests for Datastore itself
