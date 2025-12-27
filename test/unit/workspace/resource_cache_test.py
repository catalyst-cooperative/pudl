"""Unit tests for resource_cache."""

import shutil
import tempfile

import pytest
from upath import UPath

from pudl.workspace.resource_cache import LayeredCache, PudlResourceKey, UPathCache


class TestLayeredCache:
    """Unit tests for LayeredCache class."""

    def setup_method(self):
        """Constructs two UPathCache layers pointed at temporary directories."""
        self.layered_cache = LayeredCache()
        self.test_dir_1 = tempfile.mkdtemp()
        self.test_dir_2 = tempfile.mkdtemp()
        self.cache_1 = UPathCache(UPath(f"file://{self.test_dir_1}"))
        self.cache_2 = UPathCache(UPath(f"file://{self.test_dir_2}"))

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
        """Adding to layered cache stores entries in all writable layers."""
        self.layered_cache.add_cache_layer(self.cache_1)
        self.layered_cache.add_cache_layer(self.cache_2)
        res = PudlResourceKey("a", "b", "x.txt")
        # self.assertFalse(self.layered_cache.contains(res))
        assert not self.layered_cache.contains(res)
        self.layered_cache.add(res, b"sampleContent")
        assert self.layered_cache.contains(res)
        # Should be in both layers since both are writable
        assert self.cache_1.contains(res)
        assert self.cache_2.contains(res)
        # Content should match in both layers
        assert self.cache_1.get(res) == b"sampleContent"
        assert self.cache_2.get(res) == b"sampleContent"

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
        # Set on layered cache updates all writable layers
        self.layered_cache.add(res, b"newContents")
        assert self.layered_cache.get(res) == b"newContents"
        assert self.cache_1.get(res) == b"newContents"
        # Now both layers should have the new content
        assert self.cache_2.get(res) == b"newContents"

        # Deletion now affects all writable layers
        self.layered_cache.delete(res)
        # Should be deleted from all layers
        assert not self.layered_cache.contains(res)
        assert not self.cache_1.contains(res)
        assert not self.cache_2.contains(res)

    def test_add_with_no_layers_does_nothing(self):
        """When add() is called on cache with no layers nothing happens."""
        res = PudlResourceKey("a", "b", "c")
        assert not self.layered_cache.contains(res)
        self.layered_cache.add(res, b"sample")
        assert not self.layered_cache.contains(res)
        self.layered_cache.delete(res)

    def test_read_only_layers_skipped_when_adding(self):
        """When add() is called, layers that are marked as read_only are skipped."""
        c1 = UPathCache(UPath(f"file://{self.test_dir_1}"), read_only=True)
        c2 = UPathCache(UPath(f"file://{self.test_dir_2}"))
        lc = LayeredCache(c1, c2)
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
        lc = LayeredCache(self.cache_1, self.cache_2, read_only=True)

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


class TestUPathCache:
    """Unit tests for the UPathCache class."""

    @pytest.fixture
    def temp_dir(self):
        """Fixture providing a temporary directory for local filesystem tests."""
        test_dir = tempfile.mkdtemp()
        yield test_dir
        shutil.rmtree(test_dir)

    @pytest.fixture
    def mock_s3_credentials(self, mocker):
        """Fixture providing mocked boto3 components with credentials."""
        mock_session = mocker.patch("pudl.workspace.resource_cache.boto3.Session")
        mock_creds = mocker.Mock()
        mock_session.return_value.get_credentials.return_value = mock_creds
        return {"session": mock_session, "credentials": mock_creds}

    @pytest.fixture
    def mock_s3_no_credentials(self, mocker):
        """Fixture providing mocked boto3 components without credentials."""
        mock_session = mocker.patch("pudl.workspace.resource_cache.boto3.Session")
        mock_session.return_value.get_credentials.return_value = None
        return {"session": mock_session}

    @pytest.fixture
    def mock_gcp_credentials(self, mocker):
        """Fixture providing mocked GCP credentials."""
        mock_auth = mocker.patch("pudl.workspace.resource_cache.google.auth.default")
        mock_creds = mocker.Mock()
        project_id = "test-project"
        mock_auth.return_value = (mock_creds, project_id)
        return {"auth": mock_auth, "credentials": mock_creds, "project_id": project_id}

    @pytest.fixture
    def mock_gcp_no_credentials(self, mocker):
        """Fixture providing mocked GCP auth that fails."""
        mock_auth = mocker.patch("pudl.workspace.resource_cache.google.auth.default")
        mock_auth.side_effect = Exception("No credentials found")
        return {"auth": mock_auth}

    def test_invalid_scheme_raises_value_error(self):
        """Test that invalid storage schemes raise ValueError."""
        with pytest.raises(ValueError, match="Unsupported storage scheme"):
            UPathCache(UPath("http://example.com/path"))

        with pytest.raises(ValueError, match="Unsupported storage scheme"):
            UPathCache(UPath("ftp://example.com/path"))

    def test_local_filesystem_initialization(self, temp_dir):
        """Test UPathCache initialization with local filesystem."""
        cache = UPathCache(UPath(f"file://{temp_dir}"))
        assert cache._protocol == "file"
        assert cache._storage_options == {}

    def test_local_filesystem_with_file_scheme(self, temp_dir):
        """Test UPathCache initialization with file:// scheme."""
        cache = UPathCache(UPath(f"file://{temp_dir}"))

        assert cache._protocol == "file"

    def test_s3_initialization_with_credentials(self, mock_s3_credentials):
        """Test UPathCache initialization with S3 when credentials are available."""
        cache = UPathCache(UPath("s3://test-bucket/prefix"))

        assert cache._protocol == "s3"
        assert cache._storage_options.get("anon") is False

    def test_s3_initialization_without_credentials(self, mock_s3_no_credentials):
        """Test UPathCache initialization with S3 when no credentials available."""
        cache = UPathCache(UPath("s3://public-bucket/path"))

        assert cache._protocol == "s3"
        assert cache._storage_options.get("anon") is True

    def test_gcs_initialization_with_credentials(self, mock_gcp_credentials):
        """Test UPathCache initialization with GCS when credentials are available."""
        cache = UPathCache(UPath("gs://test-bucket/prefix"))

        assert cache._protocol == "gs"
        assert cache._storage_options.get("project") == "test-project"

    def test_gcs_initialization_without_credentials(self, mock_gcp_no_credentials):
        """Test UPathCache initialization with GCS when no credentials available."""
        cache = UPathCache(UPath("gs://public-bucket/path"))

        assert cache._protocol == "gs"
        assert cache._storage_options.get("token") == "anon"

    def test_local_add_and_get(self, temp_dir):
        """Test adding and retrieving a resource from local filesystem."""
        cache = UPathCache(UPath(f"file://{temp_dir}"))
        res = PudlResourceKey("dataset", "doi", "file.txt")

        assert not cache.contains(res)

        cache.add(res, b"test content")

        assert cache.contains(res)
        assert cache.get(res) == b"test content"

    def test_local_get_not_found_raises_key_error(self, temp_dir):
        """Test that getting a non-existent resource raises KeyError."""
        cache = UPathCache(UPath(f"file://{temp_dir}"))
        res = PudlResourceKey("dataset", "doi", "missing.txt")

        with pytest.raises(KeyError, match="not found"):
            cache.get(res)

    def test_local_delete(self, temp_dir):
        """Test deleting a resource from local filesystem."""
        cache = UPathCache(UPath(f"file://{temp_dir}"))
        res = PudlResourceKey("dataset", "doi", "file.txt")

        cache.add(res, b"test content")
        assert cache.contains(res)

        cache.delete(res)
        assert not cache.contains(res)

    def test_local_two_cache_objects_share_storage(self, temp_dir):
        """Test that two UPathCache instances with same path share storage."""
        cache1 = UPathCache(UPath(f"file://{temp_dir}"))
        cache2 = UPathCache(UPath(f"file://{temp_dir}"))
        res = PudlResourceKey("dataset", "doi", "file.txt")

        cache1.add(res, b"shared content")

        assert cache2.contains(res)
        assert cache2.get(res) == b"shared content"

    def test_read_only_add_does_nothing(self, temp_dir):
        """Test that add() does nothing when cache is read-only."""
        cache = UPathCache(UPath(f"file://{temp_dir}"), read_only=True)
        res = PudlResourceKey("dataset", "doi", "file.txt")

        assert cache.is_read_only()
        cache.add(res, b"content")

        assert not cache.contains(res)

    def test_read_only_delete_does_nothing(self, temp_dir):
        """Test that delete() does nothing when cache is read-only."""
        # Create resource with writable cache
        write_cache = UPathCache(UPath(f"file://{temp_dir}"))
        res = PudlResourceKey("dataset", "doi", "file.txt")
        write_cache.add(res, b"content")

        # Try to delete with read-only cache
        ro_cache = UPathCache(UPath(f"file://{temp_dir}"), read_only=True)
        ro_cache.delete(res)

        assert ro_cache.contains(res)

    def test_s3_add_without_credentials_raises_error(self, mock_s3_no_credentials):
        """Test that adding to S3 without credentials raises RuntimeError."""
        cache = UPathCache(UPath("s3://public-bucket/path"))
        res = PudlResourceKey("dataset", "doi", "file.txt")

        with pytest.raises(
            RuntimeError, match="Cannot write to S3 without credentials"
        ):
            cache.add(res, b"content")

    def test_s3_delete_without_credentials_raises_error(self, mock_s3_no_credentials):
        """Test that deleting from S3 without credentials raises RuntimeError."""
        cache = UPathCache(UPath("s3://public-bucket/path"))
        res = PudlResourceKey("dataset", "doi", "file.txt")

        with pytest.raises(
            RuntimeError, match="Cannot delete from S3 without credentials"
        ):
            cache.delete(res)

    def test_gcs_add_without_credentials_raises_error(self, mock_gcp_no_credentials):
        """Test that adding to GCS without credentials raises RuntimeError."""
        cache = UPathCache(UPath("gs://public-bucket/path"))
        res = PudlResourceKey("dataset", "doi", "file.txt")

        with pytest.raises(
            RuntimeError, match="Cannot write to GCS without credentials"
        ):
            cache.add(res, b"content")

    def test_gcs_delete_without_credentials_raises_error(self, mock_gcp_no_credentials):
        """Test that deleting from GCS without credentials raises RuntimeError."""
        cache = UPathCache(UPath("gs://public-bucket/path"))
        res = PudlResourceKey("dataset", "doi", "file.txt")

        with pytest.raises(
            RuntimeError, match="Cannot delete from GCS without credentials"
        ):
            cache.delete(res)

    def test_resource_path_construction(self, temp_dir):
        """Test that resource paths are constructed correctly."""
        cache = UPathCache(UPath(f"file://{temp_dir}"))
        res = PudlResourceKey("dataset", "10.5281/zenodo.123", "data.csv")

        path = cache._resource_path(res)

        # DOI slashes get replaced with dashes in get_local_path()
        assert str(path).endswith("dataset/10.5281-zenodo.123/data.csv")

    def test_contains_returns_false_for_missing_resource(self, temp_dir):
        """Test that contains() returns False for non-existent resources."""
        cache = UPathCache(UPath(f"file://{temp_dir}"))
        res = PudlResourceKey("dataset", "doi", "missing.txt")

        assert not cache.contains(res)

    def test_contains_handles_exceptions_gracefully(self, temp_dir, mocker):
        """Test that contains() returns False when UPath.exists() raises exception."""
        cache = UPathCache(UPath(f"file://{temp_dir}"))
        res = PudlResourceKey("dataset", "doi", "file.txt")

        # Mock the exists() method to raise an exception
        mock_path = mocker.patch.object(cache, "_resource_path")
        mock_upath = mocker.Mock()
        mock_upath.exists.side_effect = Exception("Connection error")
        mock_path.return_value = mock_upath

        # Should return False instead of raising
        assert not cache.contains(res)
