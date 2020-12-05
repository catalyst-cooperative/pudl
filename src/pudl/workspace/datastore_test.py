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

from pudl.workspace import datastore


class TestPudlFileResource(unittest.TestCase):
    def testMatches(self):
        res = datastore.PudlFileResource(dataset='ds', name='foo', doi='doi',
            metadata={
                'parts': dict(color='blue', shape='square', quantity=2)
                })
        self.assertTrue(res.matches())
        self.assertTrue(res.matches(color='blue'))
        self.assertTrue(res.matches(color='blue', shape='square'))
        self.assertFalse(res.matches(nonexistentAttribute=1))
        self.assertFalse(res.matches(shape='round'))
        self.assertFalse(res.matches(color='blue', shape='round'))

    def testGetPathForLegacyJsons(self):
        res = datastore.PudlFileResource('ds', 'foo', 'doi', metadata={
                'path': 'local/path/to/file.zip',
                'remote_url': 'http://remote.host/file.zip'
            })
        self.assertEqual('http://remote.host/file.zip', res.get_path())

    def testPathForUnmodifiedJson(self):
        res = datastore.PudlFileResource('ds', 'foo', 'doi', metadata={
                'path': 'arbitrary/path.zip'
            })
        self.assertEqual('arbitrary/path.zip', res.get_path())

    def testPathWithoutMetadata(self):
        res = datastore.PudlFileResource('ds', 'foo', 'doi')
        self.assertRaises(KeyError, res.get_path)

    def testLocalPath(self):
        res = datastore.PudlFileResource(dataset='stuff', name='foo.zip', doi='10.5072/zenodo.672199')
        self.assertEqual(Path('stuff/10.5072-zenodo.672199/foo.zip'), res.get_local_path())

    def testValidateChecksum(self):
        first = os.urandom(50)
        second = os.urandom(50)

        # Calculate hash for the first random sequence and associate it with the resource.
        m = hashlib.md5()
        m.update(first)
        res = datastore.PudlFileResource('ds', 'foo', 'doi', metadata={"hash": m.hexdigest()})
        self.assertTrue(res.content_matches_checksum(first))
        self.assertFalse(res.content_matches_checksum(second))


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

    def testProdEpacemsDoiMatches(self):
        """Most of the tests assume specific DOI for production epacems dataset.

        This test verifies that the expected value is in use.
        """
        fetcher = datastore.ZenodoFetcher()
        doi = fetcher.get_doi("epacems")
        self.assertEqual(self.PROD_EPACEMS_DOI, fetcher.get_doi("epacems"))

    @responses.activate
    def testFetchDatapackageDescriptor(self):
        fetcher = datastore.ZenodoFetcher()
        responses.add(responses.GET, 
            f"https://zenodo.org/api/deposit/depositions/{self.PROD_EPACEMS_ZEN_ID}",
            json=self.MOCK_EPACEMS_DEPOSITION)
        responses.add(responses.GET,
            "http://localhost/my/datapackage.json",
            json=self.MOCK_EPACEMS_DATAPACKAGE)
        desc = fetcher.fetch_datapackage_descriptor('epacems')
        self.assertEqual(self.MOCK_EPACEMS_DATAPACKAGE, desc.datapackage_json)

    @responses.activate
    def testFetchResource(self):
        fetcher = datastore.ZenodoFetcher()
        file_path = "http://somehost/somefile"
        res = datastore.PudlFileResource(
            dataset="epacems",
            doi=self.PROD_EPACEMS_DOI, 
            name="blah",
            metadata={
                "path": file_path,
                "hash": "6f1ed002ab5595859014ebf0951522d9"
            })
        responses.add(responses.GET, file_path, body="blah")
        self.assertEqual(b"blah", fetcher.fetch_resource(res))


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

    def testGetResources(self):
        descriptor = datastore.DatapackageDescriptor(
            self.MOCK_DATAPACKAGE,
            dataset="epacems",
            doi="123")
        self.assertEqual(
            [],
            descriptor.get_resources(strange_property=True))
        self.assertEqual(
            ["Resource(epacems/123/first-red)"],
            [str(x) for x in descriptor.get_resources(color="red")])
        self.assertEqual(
            [
                "Resource(epacems/123/first-red)",

                "Resource(epacems/123/second-blue)",
            ],
            [str(x) for x in descriptor.get_resources()])


# class TestLocalFileCache(unittest.TestCase):
#     """Validates basic operation of the LocalFileCache."""

#     def setUp(self):
#         self.test_dir = tempfile.mkdtemp()
#         self.cache = datastore.LocalFileCache(Path(self.test_dir))

#     def tearDown(self):
#         shutil.rmtree(self.test_dir)

#     def testAddSingleResource(self):
#         res = datastore.PudlFileResource(dataset="test", doi="123", name="x")

#         res.content = "test_content"
#         self.assertFalse(self.cache.exists(res))
#         self.cache.add_resource(res)
#         self.assertTrue(self.cache.exists(res))
#         self.assertEqual(res.content, self.cache.get_resource(res))

#     def testRemoveTwice(self):
#         res = datastore.PudlFileResource("a", "b", "c")
#         res.content = "123"
#         self.cache.remove_resource(res)
#         self.assertFalse(self.cache.exists(res))
#         self.cache.add_resource(res)
#         self.assertTrue(self.cache.exists(res))
#         self.cache.remove_resource(res)
#         self.assertFalse(self.cache.exists(res))
#         self.cache.remove_resource(res)
