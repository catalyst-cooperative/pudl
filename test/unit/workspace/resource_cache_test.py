"""Unit tests for resource_cache."""

import pytest
from upath import UPath

from pudl.workspace.resource_cache import LayeredCache, PudlResourceKey, UPathCache


class TestLayeredCache:
    """Unit tests for LayeredCache class."""

    @pytest.fixture(autouse=True)
    def setup_caches(self, tmp_path):
        """Constructs two UPathCache layers pointed at temporary directories."""
        self.layered_cache = LayeredCache()
        self.test_dir_1 = tmp_path / "cache1"
        self.test_dir_2 = tmp_path / "cache2"
        self.test_dir_1.mkdir()
        self.test_dir_2.mkdir()
        self.cache_1 = UPathCache(UPath(f"file://{self.test_dir_1}"))
        self.cache_2 = UPathCache(UPath(f"file://{self.test_dir_2}"))

    def test_layering_and_multi_layer_operations(self):
        """Test layer management, multi-layer add/delete, and priority retrieval."""
        # Test layer counting
        assert self.layered_cache.num_layers() == 0
        self.layered_cache.add_cache_layer(self.cache_1)
        assert self.layered_cache.num_layers() == 1
        self.layered_cache.add_cache_layer(self.cache_2)
        assert self.layered_cache.num_layers() == 2

        # Test adding to all writable layers
        res = PudlResourceKey("a", "b", "x.txt")
        assert not self.layered_cache.contains(res)
        self.layered_cache.add(res, b"sampleContent")
        assert self.layered_cache.contains(res)
        assert self.cache_1.contains(res)
        assert self.cache_2.contains(res)
        assert self.cache_1.get(res) == b"sampleContent"
        assert self.cache_2.get(res) == b"sampleContent"

        # Test retrieval from first layer
        self.cache_1.add(res, b"firstLayer")
        self.cache_2.add(res, b"secondLayer")
        assert self.layered_cache.get(res) == b"firstLayer"

        # Test updating all layers
        self.layered_cache.add(res, b"newContents")
        assert self.layered_cache.get(res) == b"newContents"
        assert self.cache_1.get(res) == b"newContents"
        assert self.cache_2.get(res) == b"newContents"

        # Test deletion from all layers
        self.layered_cache.delete(res)
        assert not self.layered_cache.contains(res)
        assert not self.cache_1.contains(res)
        assert not self.cache_2.contains(res)

        # Test operations on cache with no layers
        empty_cache = LayeredCache()
        res2 = PudlResourceKey("a", "b", "c")
        assert not empty_cache.contains(res2)
        empty_cache.add(res2, b"sample")
        assert not empty_cache.contains(res2)

    def test_read_only_behavior(self):
        """Test read-only layer and cache behavior."""
        # Test read-only layer is skipped for writes
        c1 = UPathCache(UPath(f"file://{self.test_dir_1}"), read_only=True)
        c2 = UPathCache(UPath(f"file://{self.test_dir_2}"))
        lc = LayeredCache(c1, c2)
        res = PudlResourceKey("a", "b", "c")

        lc.add(res, b"test")
        assert lc.contains(res)
        assert not c1.contains(res)  # Read-only layer not written
        assert c2.contains(res)

        lc.delete(res)
        assert not lc.contains(res)
        assert not c2.contains(res)

        # Test read-only cache ignores all modifications
        r1 = PudlResourceKey("a", "b", "r1")
        r2 = PudlResourceKey("a", "b", "r2")
        self.cache_1.add(r1, b"xxx")
        self.cache_2.add(r2, b"yyy")
        ro_cache = LayeredCache(self.cache_1, self.cache_2, read_only=True)

        assert ro_cache.contains(r1)
        assert ro_cache.contains(r2)

        # Deletion ignored on read-only cache
        ro_cache.delete(r1)
        ro_cache.delete(r2)
        assert ro_cache.contains(r1)
        assert ro_cache.contains(r2)

        # Addition ignored on read-only cache
        r_new = PudlResourceKey("a", "b", "new")
        ro_cache.add(r_new, b"xyz")
        assert not ro_cache.contains(r_new)


class TestUPathCache:
    """Unit tests for the UPathCache class."""

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

    def test_invalid_schemes(self):
        """Test that invalid storage schemes raise ValueError."""
        with pytest.raises(ValueError, match="Unsupported storage scheme"):
            UPathCache(UPath("http://example.com/path"))
        with pytest.raises(ValueError, match="Unsupported storage scheme"):
            UPathCache(UPath("ftp://example.com/path"))

    def test_local_initialization(self, tmp_path):
        """Test local filesystem cache initialization."""
        local_cache = UPathCache(UPath(f"file://{tmp_path}"))
        assert local_cache._protocol == "file"
        assert local_cache._storage_options == {}

    def test_s3_with_credentials(self, mock_s3_credentials):
        """Test S3 cache initialization with credentials."""
        s3_cache = UPathCache(UPath("s3://test-bucket/prefix"))
        assert s3_cache._protocol == "s3"
        assert s3_cache._storage_options.get("anon") is False

    def test_s3_without_credentials(self, mock_s3_no_credentials):
        """Test S3 cache initialization without credentials uses anonymous access."""
        s3_cache = UPathCache(UPath("s3://public-bucket/path"))
        assert s3_cache._protocol == "s3"
        assert s3_cache._storage_options.get("anon") is True

    def test_gcs_with_credentials(self, mock_gcp_credentials):
        """Test GCS cache initialization with credentials."""
        gcs_cache = UPathCache(UPath("gs://test-bucket/prefix"))
        assert gcs_cache._protocol == "gs"
        assert gcs_cache._storage_options.get("project") == "test-project"

    def test_gcs_without_credentials(self, mock_gcp_no_credentials):
        """Test GCS cache initialization without credentials uses anonymous access."""
        gcs_cache = UPathCache(UPath("gs://public-bucket/path"))
        assert gcs_cache._protocol == "gs"
        assert gcs_cache._storage_options.get("token") == "anon"

    def test_local_filesystem_operations(self, tmp_path):
        """Test add, get, delete, contains, and resource path construction on local filesystem."""
        cache = UPathCache(UPath(f"file://{tmp_path}"))
        res = PudlResourceKey("dataset", "doi", "file.txt")

        # Test add and get
        assert not cache.contains(res)
        cache.add(res, b"test content")
        assert cache.contains(res)
        assert cache.get(res) == b"test content"

        # Test delete
        cache.delete(res)
        assert not cache.contains(res)

        # Test get on missing resource raises KeyError
        missing_res = PudlResourceKey("dataset", "doi", "missing.txt")
        with pytest.raises(KeyError, match="not found"):
            cache.get(missing_res)
        assert not cache.contains(missing_res)

        # Test resource path construction with DOI
        doi_res = PudlResourceKey("dataset", "10.5281/zenodo.123", "data.csv")
        path = cache._resource_path(doi_res)
        assert str(path).endswith("dataset/10.5281-zenodo.123/data.csv")

        # Test that two cache instances share storage
        cache2 = UPathCache(UPath(f"file://{tmp_path}"))
        cache.add(res, b"shared content")
        assert cache2.contains(res)
        assert cache2.get(res) == b"shared content"

    def test_read_only_behavior(self, tmp_path):
        """Test read-only cache behavior for add and delete operations."""
        # Create resource with writable cache
        write_cache = UPathCache(UPath(f"file://{tmp_path}"))
        res = PudlResourceKey("dataset", "doi", "file.txt")
        write_cache.add(res, b"content")

        # Read-only cache should not allow modifications
        ro_cache = UPathCache(UPath(f"file://{tmp_path}"), read_only=True)
        assert ro_cache.is_read_only()
        assert ro_cache.contains(res)

        # Add should do nothing
        new_res = PudlResourceKey("dataset", "doi", "new.txt")
        ro_cache.add(new_res, b"content")
        assert not ro_cache.contains(new_res)

        # Delete should do nothing
        ro_cache.delete(res)
        assert ro_cache.contains(res)

    def test_s3_write_operations_without_credentials(self, mock_s3_no_credentials):
        """Test that S3 write operations without credentials raise appropriate errors."""
        res = PudlResourceKey("dataset", "doi", "file.txt")
        s3_cache = UPathCache(UPath("s3://public-bucket/path"))

        with pytest.raises(
            RuntimeError, match="Cannot write to S3 without credentials"
        ):
            s3_cache.add(res, b"content")
        with pytest.raises(
            RuntimeError, match="Cannot delete from S3 without credentials"
        ):
            s3_cache.delete(res)

    def test_gcs_write_operations_without_credentials(self, mock_gcp_no_credentials):
        """Test that GCS write operations without credentials raise appropriate errors."""
        res = PudlResourceKey("dataset", "doi", "file.txt")
        gcs_cache = UPathCache(UPath("gs://public-bucket/path"))

        with pytest.raises(
            RuntimeError, match="Cannot write to GS without credentials"
        ):
            gcs_cache.add(res, b"content")
        with pytest.raises(
            RuntimeError, match="Cannot delete from GS without credentials"
        ):
            gcs_cache.delete(res)
