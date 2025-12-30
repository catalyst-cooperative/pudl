"""Unit tests for resource_cache."""

import shutil
import tempfile
from pathlib import Path

import pytest
import requests.exceptions as requests_exceptions
from botocore.exceptions import ClientError
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


class TestS3Cache:
    """Unit tests for the S3Cache class."""

    @pytest.fixture
    def mock_s3_with_credentials(self, mocker):
        """Fixture providing mocked boto3 components with credentials."""
        mock_session = mocker.patch("pudl.workspace.resource_cache.boto3.Session")
        mock_client = mocker.patch("pudl.workspace.resource_cache.boto3.client")
        mock_creds = mocker.Mock()
        mock_session.return_value.get_credentials.return_value = mock_creds
        mock_s3_client = mocker.MagicMock()
        mock_client.return_value = mock_s3_client

        return {
            "session": mock_session,
            "client": mock_client,
            "credentials": mock_creds,
            "s3_client": mock_s3_client,
        }

    @pytest.fixture
    def mock_s3_without_credentials(self, mocker):
        """Fixture providing mocked boto3 components without credentials."""
        mock_session = mocker.patch("pudl.workspace.resource_cache.boto3.Session")
        mock_client = mocker.patch("pudl.workspace.resource_cache.boto3.client")
        mock_session.return_value.get_credentials.return_value = None
        mock_s3_client = mocker.MagicMock()
        mock_client.return_value = mock_s3_client

        return {
            "session": mock_session,
            "client": mock_client,
            "s3_client": mock_s3_client,
        }

    def test_invalid_s3_path_raises_value_error(self):
        """Test that invalid S3 paths raise ValueError."""
        with pytest.raises(ValueError, match="s3_path should start with s3://"):
            resource_cache.S3Cache("gs://wrong-scheme/path")

        with pytest.raises(ValueError, match="s3_path should start with s3://"):
            resource_cache.S3Cache("https://example.com/path")

    def test_initialization_with_credentials(self, mock_s3_with_credentials):
        """Test S3Cache initialization when AWS credentials are available."""
        mocks = mock_s3_with_credentials
        cache = resource_cache.S3Cache("s3://test-bucket/prefix")

        # Verify it created a signed client
        assert not cache._unsigned
        assert cache._bucket_name == "test-bucket"
        mocks["client"].assert_called_once_with("s3")

    def test_initialization_without_credentials(self, mock_s3_without_credentials):
        """Test S3Cache initialization when no AWS credentials are available."""
        mocks = mock_s3_without_credentials
        cache = resource_cache.S3Cache("s3://public-bucket/path")

        # Verify it created an unsigned client
        assert cache._unsigned
        assert cache._bucket_name == "public-bucket"
        # Check that client was called with unsigned config
        call_args = mocks["client"].call_args
        assert call_args[0][0] == "s3"
        assert "config" in call_args[1]

    def test_get_success(self, mock_s3_with_credentials, mocker):
        """Test successfully retrieving an object from S3."""
        mocks = mock_s3_with_credentials

        # Mock successful get_object response
        mock_response = {"Body": mocker.Mock()}
        mock_response["Body"].read.return_value = b"test content"
        mocks["s3_client"].get_object.return_value = mock_response

        cache = resource_cache.S3Cache("s3://test-bucket/prefix")
        res = PudlResourceKey("dataset", "doi", "file.txt")

        content = cache.get(res)

        assert content == b"test content"
        mocks["s3_client"].get_object.assert_called_once()

    def test_get_not_found_raises_key_error(self, mock_s3_with_credentials):
        """Test that getting a non-existent object raises KeyError."""
        mocks = mock_s3_with_credentials

        # Mock NoSuchKey error
        error_response = {"Error": {"Code": "NoSuchKey"}}
        mocks["s3_client"].get_object.side_effect = ClientError(
            error_response, "GetObject"
        )

        cache = resource_cache.S3Cache("s3://test-bucket/prefix")
        res = PudlResourceKey("dataset", "doi", "missing.txt")

        with pytest.raises(KeyError, match="not found in S3 bucket"):
            cache.get(res)

    def test_add_with_credentials(self, mock_s3_with_credentials):
        """Test adding an object to S3 with credentials."""
        mocks = mock_s3_with_credentials
        cache = resource_cache.S3Cache("s3://test-bucket/prefix")
        res = PudlResourceKey("dataset", "doi", "file.txt")

        cache.add(res, b"new content")

        mocks["s3_client"].put_object.assert_called_once()
        call_args = mocks["s3_client"].put_object.call_args
        assert call_args[1]["Bucket"] == "test-bucket"
        assert call_args[1]["Body"] == b"new content"

    def test_add_without_credentials_raises_error(self, mock_s3_without_credentials):
        """Test that adding without credentials raises RuntimeError."""
        cache = resource_cache.S3Cache("s3://public-bucket/path")
        res = PudlResourceKey("dataset", "doi", "file.txt")

        with pytest.raises(
            RuntimeError, match="Cannot write to S3 without credentials"
        ):
            cache.add(res, b"content")

    def test_add_read_only_does_nothing(self, mock_s3_with_credentials):
        """Test that add() does nothing when cache is read-only."""
        mocks = mock_s3_with_credentials
        cache = resource_cache.S3Cache("s3://test-bucket/prefix", read_only=True)
        res = PudlResourceKey("dataset", "doi", "file.txt")

        cache.add(res, b"content")

        # Verify put_object was not called
        mocks["s3_client"].put_object.assert_not_called()

    def test_delete_with_credentials(self, mock_s3_with_credentials):
        """Test deleting an object from S3 with credentials."""
        mocks = mock_s3_with_credentials
        cache = resource_cache.S3Cache("s3://test-bucket/prefix")
        res = PudlResourceKey("dataset", "doi", "file.txt")

        cache.delete(res)

        mocks["s3_client"].delete_object.assert_called_once()
        call_args = mocks["s3_client"].delete_object.call_args
        assert call_args[1]["Bucket"] == "test-bucket"

    def test_delete_without_credentials_raises_error(self, mock_s3_without_credentials):
        """Test that deleting without credentials raises RuntimeError."""
        cache = resource_cache.S3Cache("s3://public-bucket/path")
        res = PudlResourceKey("dataset", "doi", "file.txt")

        with pytest.raises(
            RuntimeError, match="Cannot delete from S3 without credentials"
        ):
            cache.delete(res)

    def test_delete_read_only_does_nothing(self, mock_s3_with_credentials):
        """Test that delete() does nothing when cache is read-only."""
        mocks = mock_s3_with_credentials
        cache = resource_cache.S3Cache("s3://test-bucket/prefix", read_only=True)
        res = PudlResourceKey("dataset", "doi", "file.txt")

        cache.delete(res)

        # Verify delete_object was not called
        mocks["s3_client"].delete_object.assert_not_called()

    def test_contains_returns_true_when_object_exists(self, mock_s3_with_credentials):
        """Test that contains() returns True when object exists in S3."""
        mocks = mock_s3_with_credentials

        # Mock successful head_object (object exists)
        mocks["s3_client"].head_object.return_value = {}

        cache = resource_cache.S3Cache("s3://test-bucket/prefix")
        res = PudlResourceKey("dataset", "doi", "file.txt")

        assert cache.contains(res)

    def test_contains_returns_false_when_object_not_found(
        self, mock_s3_with_credentials
    ):
        """Test that contains() returns False when object doesn't exist."""
        mocks = mock_s3_with_credentials

        # Mock 404 error
        error_response = {
            "Error": {"Code": "404"},
            "ResponseMetadata": {"HTTPStatusCode": 404},
        }
        mocks["s3_client"].head_object.side_effect = ClientError(
            error_response, "HeadObject"
        )

        cache = resource_cache.S3Cache("s3://test-bucket/prefix")
        res = PudlResourceKey("dataset", "doi", "missing.txt")

        assert not cache.contains(res)

    def test_contains_returns_false_for_nosuchkey_error(self, mock_s3_with_credentials):
        """Test that contains() returns False for NoSuchKey error."""
        mocks = mock_s3_with_credentials

        # Mock NoSuchKey error
        error_response = {"Error": {"Code": "NoSuchKey"}}
        mocks["s3_client"].head_object.side_effect = ClientError(
            error_response, "HeadObject"
        )

        cache = resource_cache.S3Cache("s3://test-bucket/prefix")
        res = PudlResourceKey("dataset", "doi", "missing.txt")

        assert not cache.contains(res)

    def test_contains_raises_on_other_errors(self, mock_s3_with_credentials):
        """Test that contains() raises exceptions for non-404 errors."""
        mocks = mock_s3_with_credentials

        # Mock access denied error
        error_response = {"Error": {"Code": "AccessDenied"}}
        mocks["s3_client"].head_object.side_effect = ClientError(
            error_response, "HeadObject"
        )

        cache = resource_cache.S3Cache("s3://test-bucket/prefix")
        res = PudlResourceKey("dataset", "doi", "file.txt")

        with pytest.raises(ClientError):
            cache.contains(res)

    def test_get_object_key_with_prefix(self, mock_s3_with_credentials):
        """Test that object keys are constructed correctly with a prefix."""
        cache = resource_cache.S3Cache("s3://test-bucket/my/prefix")
        res = PudlResourceKey("dataset", "10.5281/zenodo.123", "data.csv")

        key = cache._get_object_key(res)

        # DOI slashes get replaced with dashes
        assert key == "my/prefix/dataset/10.5281-zenodo.123/data.csv"

    def test_get_object_key_without_prefix(self, mock_s3_with_credentials):
        """Test that object keys are constructed correctly without a prefix."""
        cache = resource_cache.S3Cache("s3://test-bucket")
        res = PudlResourceKey("dataset", "10.5281/zenodo.456", "file.txt")

        key = cache._get_object_key(res)

        assert key == "dataset/10.5281-zenodo.456/file.txt"
