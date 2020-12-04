import os
import shutil
import tempfile
import unittest
import hashlib
from pathlib import Path
from unittest import mock as mock
from unittest.mock import patch
from pathlib import Path

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
        self.assertEqual(Path('http://remote.host/file.zip'), res.get_path())

    def testPathForUnmodifiedJson(self):
        res = datastore.PudlFileResource('ds', 'foo', 'doi', metadata={
                'path': 'arbitrary/path.zip'
            })
        self.assertEqual(Path('arbitrary/path.zip'), res.get_path())

    def testPathWithoutMetadata(self):
        res = datastore.PudlFileResource('ds', 'foo', 'doi')
        self.assertEqual(None, res.get_path())

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
