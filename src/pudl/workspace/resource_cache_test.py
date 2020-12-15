"""Unit tests for resource_cache."""
from pathlib import Path
from pudl.workspace import resource_cache
from pudl.workspace.resource_cache import PudlResourceKey
import unittest
import tempfile
import shutil
import tempfile


class TestLocalFileCache(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.cache = resource_cache.LocalFileCache(Path(self.test_dir))

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def testAddSingleResource(self):
        res = PudlResourceKey("ds", "doi", "file.txt")
        self.assertFalse(self.cache.contains(res))
        self.cache.set(res, b"blah")
        self.assertTrue(self.cache.contains(res))
        self.assertEqual(b"blah", self.cache.get(res))

    def testTwoCacheObjectShareStorage(self):
        second_cache = resource_cache.LocalFileCache(Path(self.test_dir))
        res = PudlResourceKey("dataset", "doi", "file.txt")
        self.assertFalse(self.cache.contains(res))
        self.assertFalse(second_cache.contains(res))
        self.cache.set(res, b"testContents")
        self.assertTrue(self.cache.contains(res))
        self.assertTrue(second_cache.contains(res))
        self.assertEqual(b"testContents", second_cache.get(res))

    def testDeletion(self):
        res = PudlResourceKey("a", "b", "c")
        self.assertFalse(self.cache.contains(res))
        self.cache.set(res, b"sampleContents")
        self.assertTrue(self.cache.contains(res))
        self.cache.delete(res)
        self.assertFalse(self.cache.contains(res))

    def testReadOnlySetAndDelete(self):
        res = PudlResourceKey("a", "b", "c")
        ro_cache = resource_cache.LocalFileCache(Path(self.test_dir), read_only=True)
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
        self.layered_cache = resource_cache.LayeredCache()
        self.test_dir_1 = tempfile.mkdtemp()
        self.test_dir_2 = tempfile.mkdtemp()
        self.cache_1 = resource_cache.LocalFileCache(self.test_dir_1)
        self.cache_2 = resource_cache.LocalFileCache(self.test_dir_2)

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
        res = PudlResourceKey("a", "b", "x.txt")
        self.assertFalse(self.layered_cache.contains(res))
        self.layered_cache.set(res, b"sampleContent")
        self.assertTrue(self.layered_cache.contains(res))
        self.assertTrue(self.cache_1.contains(res))
        self.assertFalse(self.cache_2.contains(res))

    def testFetchFromInnermostLayer(self):
        res = PudlResourceKey("a", "b", "x.txt")
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
        res = PudlResourceKey("a", "b", "c")
        self.assertFalse(self.layered_cache.contains(res))
        self.layered_cache.set(res, b"sample")
        self.assertFalse(self.layered_cache.contains(res))
        self.layered_cache.delete(res)

    def testReadOnlyLayersSkipped(self):
        c1 = resource_cache.LocalFileCache(self.test_dir_1, read_only=True)
        c2 = resource_cache.LocalFileCache(self.test_dir_2)
        lc = resource_cache.LayeredCache(c1, c2)

        res = PudlResourceKey("a", "b", "c")

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
        r1 = PudlResourceKey("a", "b", "r1")
        r2 = PudlResourceKey("a", "b", "r2")
        self.cache_1.set(r1, b"xxx")
        self.cache_2.set(r2, b"yyy")
        self.assertTrue(self.cache_1.contains(r1))
        self.assertTrue(self.cache_2.contains(r2))
        lc = resource_cache.LayeredCache(self.cache_1, self.cache_2, read_only=True)

        self.assertTrue(lc.contains(r1))
        self.assertTrue(lc.contains(r2))

        lc.delete(r1)
        lc.delete(r2)
        self.assertTrue(lc.contains(r1))
        self.assertTrue(lc.contains(r2))
        self.assertTrue(self.cache_1.contains(r1))
        self.assertTrue(self.cache_2.contains(r2))

        r_new = PudlResourceKey("a", "b", "new")
        lc.set(r_new, b"xyz")
        self.assertFalse(lc.contains(r_new))
        self.assertFalse(self.cache_1.contains(r_new))
        self.assertFalse(self.cache_2.contains(r_new))


