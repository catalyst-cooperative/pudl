import re
import os
import shutil
import tempfile
import unittest
import hashlib
import responses
from pathlib import Path
from unittest import mock as mock
from unittest.mock import patch
from pathlib import Path
from unittest import mock
import responses
import json
from typing import Dict

from pudl.workspace import datastore
from pudl.workspace.datastore import PudlResourceKey


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
            {"name": "first", "path": "http://localhost/first"},
            {"name": "second", "path": "http://localhost/second"},
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


# TODO(rousik): add tests for the caching layers
# TODO(rousik): add tests for resource filtering

class TestLocalFileCache(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.cache = datastore.LocalFileCache(Path(self.test_dir))

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def testAddSingleResource(self):
        res = datastore.PudlResourceKey("ds", "doi", "file.txt")
        self.assertFalse(self.cache.contains(res))
        self.cache.set(res, b"blah")
        self.assertTrue(self.cache.contains(res))
        self.assertEqual(b"blah", self.cache.get(res))

    def testTwoCacheObjectShareStorage(self):
        second_cache = datastore.LocalFileCache(Path(self.test_dir))
        res = datastore.PudlResourceKey("dataset", "doi", "file.txt")
        self.assertFalse(self.cache.contains(res))
        self.assertFalse(second_cache.contains(res))
        self.cache.set(res, b"testContents")
        self.assertTrue(self.cache.contains(res))
        self.assertTrue(second_cache.contains(res))
        self.assertEqual(b"testContents", second_cache.get(res))

    def testDeletion(self):
        res = datastore.PudlResourceKey("a", "b", "c")
        self.assertFalse(self.cache.contains(res))
        self.cache.set(res, b"sampleContents")
        self.assertTrue(self.cache.contains(res))
        self.cache.delete(res)
        self.assertFalse(self.cache.contains(res))

    def testReadOnlySetAndDelete(self):
        res = datastore.PudlResourceKey("a", "b", "c")
        ro_cache = datastore.LocalFileCache(Path(self.test_dir), read_only=True)
        self.assertTrue(ro_cache.is_read_only())
        
        ro_cache.set(res, b"sample")
        self.assertFalse(ro_cache.contains(res))

        # Use read-write cache to insert resource
        self.cache.set(res, b"sample")
        self.assertFalse(self.cache.is_read_only())
        self.assertTrue(ro_cache.contains(res))

        # Deleting via ro cache should not happen
        ro_cache.delete(res)
        self.assertTrue(ro_cache.contains(res))


class TestLayeredCache(unittest.TestCase):
    def setUp(self):
        self.layered_cache = datastore.LayeredCache()
        self.test_dir_1 = tempfile.mkdtemp()
        self.test_dir_2 = tempfile.mkdtemp()
        self.cache_1 = datastore.LocalFileCache(self.test_dir_1)
        self.cache_2 = datastore.LocalFileCache(self.test_dir_2)

    def tearDown(self):
        shutil.rmtree(self.test_dir_1)
        shutil.rmtree(self.test_dir_2)

    def testAddLayers(self):
        self.assertEqual(0, self.layered_cache.num_layers())
        self.layered_cache.add_cache_layer(self.cache_1)
        self.assertEqual(1, self.layered_cache.num_layers())
        self.layered_cache.add_cache_layer(self.cache_2)
        self.assertEqual(2, self.layered_cache.num_layers())

    def testAddToFirstLayer(self):
        self.layered_cache.add_cache_layer(self.cache_1)
        self.layered_cache.add_cache_layer(self.cache_2)
        res = datastore.PudlResourceKey("a", "b", "x.txt")
        self.assertFalse(self.layered_cache.contains(res))
        self.layered_cache.set(res, b"sampleContent")
        self.assertTrue(self.layered_cache.contains(res))
        self.assertTrue(self.cache_1.contains(res))
        self.assertFalse(self.cache_2.contains(res))

    def testFetchFromInnermostLayer(self):
        res = datastore.PudlResourceKey("a", "b", "x.txt")
        self.layered_cache.add_cache_layer(self.cache_1)
        self.layered_cache.add_cache_layer(self.cache_2)
        # self.cache_1.set(res, "firstLayer")
        self.cache_2.set(res, b"secondLayer")
        self.assertEqual(b"secondLayer", self.layered_cache.get(res))

        self.cache_1.set(res, b"firstLayer")
        self.assertEqual(b"firstLayer", self.layered_cache.get(res))
        # Set on layered cache updates innermost layer
        self.layered_cache.set(res, b"newContents")
        self.assertEqual(b"newContents", self.layered_cache.get(res))
        self.assertEqual(b"newContents", self.cache_1.get(res))
        self.assertEqual(b"secondLayer", self.cache_2.get(res))

        # Deletion also only affects innermost layer
        self.layered_cache.delete(res)
        self.assertTrue(self.layered_cache.contains(res))
        self.assertFalse(self.cache_1.contains(res))
        self.assertTrue(self.cache_2.contains(res))
        self.assertEqual(b"secondLayer", self.layered_cache.get(res))

    def testSetWithNoLayers(self):
        res = datastore.PudlResourceKey("a", "b", "c")
        self.assertFalse(self.layered_cache.contains(res))
        self.layered_cache.set(res, b"sample")
        self.assertFalse(self.layered_cache.contains(res))
        self.layered_cache.delete(res)

    def testReadOnlyLayersSkipped(self):
        c1 = datastore.LocalFileCache(self.test_dir_1, read_only=True)
        c2 = datastore.LocalFileCache(self.test_dir_2)
        lc = datastore.LayeredCache(c1, c2)

        res = datastore.PudlResourceKey("a", "b", "c")

        self.assertFalse(lc.contains(res))
        self.assertFalse(c1.contains(res))
        self.assertFalse(c2.contains(res))

        lc.set(res, b"test")
        self.assertTrue(lc.contains(res))
        self.assertFalse(c1.contains(res))
        self.assertTrue(c2.contains(res))

        lc.delete(res)
        self.assertFalse(lc.contains(res))
        self.assertFalse(c1.contains(res))
        self.assertFalse(c2.contains(res))

    def testReadOnlyLayeredCache(self):
        r1 = datastore.PudlResourceKey("a", "b", "r1")
        r2 = datastore.PudlResourceKey("a", "b", "r2")
        self.cache_1.set(r1, b"xxx")
        self.cache_2.set(r2, b"yyy")
        self.assertTrue(self.cache_1.contains(r1))
        self.assertTrue(self.cache_2.contains(r2))
        lc = datastore.LayeredCache(self.cache_1, self.cache_2, read_only=True)

        self.assertTrue(lc.contains(r1))
        self.assertTrue(lc.contains(r2))

        lc.delete(r1)
        lc.delete(r2)
        self.assertTrue(lc.contains(r1))
        self.assertTrue(lc.contains(r2))
        self.assertTrue(self.cache_1.contains(r1))
        self.assertTrue(self.cache_2.contains(r2))

        r_new = datastore.PudlResourceKey("a", "b", "new")
        lc.set(r_new, b"xyz")
        self.assertFalse(lc.contains(r_new))
        self.assertFalse(self.cache_1.contains(r_new))
        self.assertFalse(self.cache_2.contains(r_new))


# TODO(rousik): add tests for Datastore itself
