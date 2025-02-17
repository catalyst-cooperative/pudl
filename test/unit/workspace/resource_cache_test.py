"""Unit tests for resource_cache."""

import shutil
import tempfile
from pathlib import Path

import requests.exceptions as requests_exceptions
from google.api_core.exceptions import BadRequest
from google.cloud.storage.retry import _should_retry

from pudl.workspace import resource_cache
from pudl.workspace.resource_cache import PudlResourceKey, extend_gcp_retry_predicate


class TestGoogleCloudStorageCache:
    """Unit tests for the GoogleCloudStorageCache class."""

    def test_bad_request_predicate(self):
        """Check extended predicate catches BadRequest and default exceptions."""
        bad_request_predicate = extend_gcp_retry_predicate(_should_retry, BadRequest)

        # Check default exceptions.
        assert not _should_retry(BadRequest(message="Bad request!"))
        assert _should_retry(requests_exceptions.Timeout())

        # Check extended predicate handles default exceptions and BadRequest.
        assert bad_request_predicate(requests_exceptions.Timeout())
        assert bad_request_predicate(BadRequest(message="Bad request!"))


class TestLocalFileCache:
    """Unit tests for the LocalFileCache class."""

    def setup_method(self):
        """Prepares temporary directory for storing cache contents."""
        self.test_dir = tempfile.mkdtemp()
        self.cache = resource_cache.LocalFileCache(Path(self.test_dir))

    def teardown_method(self):
        """Deletes content of the temporary directories."""
        shutil.rmtree(self.test_dir)

    def test_add_single_resource(self):
        """Adding resource has expected effect on later get() and contains() calls."""
        res = PudlResourceKey("ds", "doi", "file.txt")
        assert not self.cache.contains(res)
        self.cache.add(res, b"blah")
        assert self.cache.contains(res)
        assert self.cache.get(res) == b"blah"

    def test_that_two_cache_objects_share_storage(self):
        """Two LocalFileCache instances with the same path share the object storage."""
        second_cache = resource_cache.LocalFileCache(Path(self.test_dir))
        res = PudlResourceKey("dataset", "doi", "file.txt")
        assert not self.cache.contains(res)
        assert not second_cache.contains(res)
        self.cache.add(res, b"testContents")
        assert self.cache.contains(res)
        assert second_cache.contains(res)
        assert second_cache.get(res) == b"testContents"

    def test_deletion(self):
        """Deleting resources has expected effect on later get() / contains() calls."""
        res = PudlResourceKey("a", "b", "c")
        assert not self.cache.contains(res)
        self.cache.add(res, b"sampleContents")
        assert self.cache.contains(res)
        self.cache.delete(res)
        assert not self.cache.contains(res)

    def test_read_only_add_and_delete_do_nothing(self):
        """Test that in read_only mode, add() and delete() calls are ignored."""
        res = PudlResourceKey("a", "b", "c")
        ro_cache = resource_cache.LocalFileCache(Path(self.test_dir), read_only=True)
        assert ro_cache.is_read_only()

        ro_cache.add(res, b"sample")
        assert not ro_cache.contains(res)

        # Use read-write cache to insert resource
        self.cache.add(res, b"sample")
        assert not self.cache.is_read_only()
        assert ro_cache.contains(res)

        # Deleting via ro cache should not happen
        ro_cache.delete(res)
        assert ro_cache.contains(res)


class TestLayeredCache:
    """Unit tests for LayeredCache class."""

    def setup_method(self):
        """Constructs two LocalFileCache layers pointed at temporary directories."""
        self.layered_cache = resource_cache.LayeredCache()
        self.test_dir_1 = tempfile.mkdtemp()
        self.test_dir_2 = tempfile.mkdtemp()
        self.cache_1 = resource_cache.LocalFileCache(self.test_dir_1)
        self.cache_2 = resource_cache.LocalFileCache(self.test_dir_2)

    def tearDown(self):
        """Remove temporary directories storing the cache contents."""
        shutil.rmtree(self.test_dir_1)
        shutil.rmtree(self.test_dir_2)

    def test_add_caching_layers(self):
        """Adding layers has expected effect on the subsequent num_layers() calls."""
        # self.assertEqual(0, self.layered_cache.num_layers())
        assert self.layered_cache.num_layers() == 0
        self.layered_cache.add_cache_layer(self.cache_1)
        assert self.layered_cache.num_layers() == 1
        self.layered_cache.add_cache_layer(self.cache_2)
        assert self.layered_cache.num_layers() == 2

    def test_add_to_first_layer(self):
        """Adding to layered cache by default stores entires in the first layer."""
        self.layered_cache.add_cache_layer(self.cache_1)
        self.layered_cache.add_cache_layer(self.cache_2)
        res = PudlResourceKey("a", "b", "x.txt")
        # self.assertFalse(self.layered_cache.contains(res))
        assert not self.layered_cache.contains(res)
        self.layered_cache.add(res, b"sampleContent")
        assert self.layered_cache.contains(res)
        assert self.cache_1.contains(res)
        assert not self.cache_2.contains(res)

    def test_get_uses_innermost_layer(self):
        """Resource is retrieved from the leftmost layer that contains it."""
        res = PudlResourceKey("a", "b", "x.txt")
        self.layered_cache.add_cache_layer(self.cache_1)
        self.layered_cache.add_cache_layer(self.cache_2)
        self.cache_1.add(res, b"firstLayer")
        self.cache_2.add(res, b"secondLayer")
        # assert self.layered_cache.get(res) == b"secondLayer"
        assert self.layered_cache.get(res) == b"firstLayer"

        self.cache_1.add(res, b"firstLayer")
        assert self.layered_cache.get(res) == b"firstLayer"
        # Set on layered cache updates innermost layer
        self.layered_cache.add(res, b"newContents")
        assert self.layered_cache.get(res) == b"newContents"
        assert self.cache_1.get(res) == b"newContents"
        assert self.cache_2.get(res) == b"secondLayer"

        # Deletion also only affects innermost layer
        self.layered_cache.delete(res)
        assert self.layered_cache.contains(res)
        assert not self.cache_1.contains(res)
        assert self.cache_2.contains(res)
        assert self.cache_2.get(res) == b"secondLayer"

    def test_add_with_no_layers_does_nothing(self):
        """When add() is called on cache with no layers nothing happens."""
        res = PudlResourceKey("a", "b", "c")
        assert not self.layered_cache.contains(res)
        self.layered_cache.add(res, b"sample")
        assert not self.layered_cache.contains(res)
        self.layered_cache.delete(res)

    def test_read_only_layers_skipped_when_adding(self):
        """When add() is called, layers that are marked as read_only are skipped."""
        c1 = resource_cache.LocalFileCache(self.test_dir_1, read_only=True)
        c2 = resource_cache.LocalFileCache(self.test_dir_2)
        lc = resource_cache.LayeredCache(c1, c2)

        res = PudlResourceKey("a", "b", "c")

        assert not lc.contains(res)
        assert not c1.contains(res)
        assert not c2.contains(res)

        lc.add(res, b"test")
        assert lc.contains(res)
        assert not c1.contains(res)
        assert c2.contains(res)

        lc.delete(res)
        assert not lc.contains(res)
        assert not c1.contains(res)
        assert not c2.contains(res)

    def test_read_only_cache_ignores_modifications(self):
        """When cache is marked as read_only, add() and delete() calls are ignored."""
        r1 = PudlResourceKey("a", "b", "r1")
        r2 = PudlResourceKey("a", "b", "r2")
        self.cache_1.add(r1, b"xxx")
        self.cache_2.add(r2, b"yyy")
        assert self.cache_1.contains(r1)
        assert self.cache_2.contains(r2)
        lc = resource_cache.LayeredCache(self.cache_1, self.cache_2, read_only=True)

        assert lc.contains(r1)
        assert lc.contains(r2)

        lc.delete(r1)
        lc.delete(r2)
        assert lc.contains(r1)
        assert lc.contains(r2)
        assert self.cache_1.contains(r1)
        assert self.cache_2.contains(r2)

        r_new = PudlResourceKey("a", "b", "new")
        lc.add(r_new, b"xyz")
        assert not lc.contains(r_new)
        assert not self.cache_1.contains(r_new)
        assert not self.cache_2.contains(r_new)
