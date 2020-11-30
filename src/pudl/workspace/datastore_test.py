import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock as mock
from unittest.mock import patch

from pudl.workspace import datastore


class TestLocalFileCache(unittest.TestCase):
    """Validates basic operation of the LocalFileCache."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.cache = datastore.LocalFileCache(Path(self.test_dir))

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def testAddSingleResource(self):
        res = datastore.PudlFileResource(dataset="test", doi="123", name="x")

        res.content = "test_content"
        self.assertFalse(self.cache.exists(res))
        self.cache.add_resource(res)
        self.assertTrue(self.cache.exists(res))
        self.assertEquals(res.content, self.cache.get_resource(res))

    def testRemoveTwice(self):
        res = datastore.PudlFileResource("a", "b", "c")
        res.content = "123"
        self.cache.remove_resource(res)
        self.assertFalse(self.cache.exists(res))
        self.cache.add_resource(res)
        self.assertTrue(self.cache.exists(res))
        self.cache.remove_resource(res)
        self.assertFalse(self.cache.exists(res))
        self.cache.remove_resource(res)
