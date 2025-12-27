"""Integration tests for resource_cache module.

These tests verify that the cache implementations can actually interact with real
storage backends (S3, GCS, local filesystem) to cache PUDL data.
"""

import contextlib
import shutil
import tempfile
from pathlib import Path

import pytest
from google.cloud import storage
from requests.exceptions import ConnectionError, RetryError  # noqa: A004
from upath import UPath
from urllib3.exceptions import MaxRetryError, ResponseError

from pudl.workspace.datastore import Datastore, ZenodoDoiSettings
from pudl.workspace.resource_cache import LayeredCache, PudlResourceKey, UPathCache


@pytest.fixture
def temp_local_dir():
    """Create a temporary directory for local filesystem caching."""
    test_dir = tempfile.mkdtemp()
    yield test_dir
    shutil.rmtree(test_dir, ignore_errors=True)


@pytest.fixture
def gcs_test_cache_path():
    """Provide the GCS test cache path and ensure cleanup after tests."""
    cache_path = "gs://test.catalyst.coop/zenodo"
    yield cache_path

    # Cleanup: Remove all objects under the test path
    try:
        client = storage.Client()
        bucket = client.bucket("test.catalyst.coop")

        # List and delete all blobs with the zenodo prefix
        blobs = bucket.list_blobs(prefix="zenodo/")
        for blob in blobs:
            blob.delete()
    except Exception as e:
        # Log but don't fail if cleanup has issues
        print(f"Warning: Could not clean up GCS test cache: {e}")


@pytest.fixture
def sample_resource():
    """Provide a sample resource key for testing."""
    # Use a small file from eia176 dataset for testing
    # Get the current DOI from datastore instead of hard-coding it
    return PudlResourceKey(
        dataset="eia176",
        doi=ZenodoDoiSettings().eia176,
        name="eia176-1997.zip",
    )


class TestUPathCacheIntegration:
    """Integration tests for UPathCache with real storage backends."""

    def test_local_filesystem_via_upath(self, temp_local_dir, sample_resource):
        """Test UPathCache with local filesystem."""
        cache = UPathCache(UPath(f"file://{temp_local_dir}"))

        test_content = b"UPath local test data"
        cache.add(sample_resource, test_content)

        assert cache.contains(sample_resource)
        assert cache.get(sample_resource) == test_content

        cache.delete(sample_resource)
        assert not cache.contains(sample_resource)

    @pytest.mark.xfail(
        raises=(
            MaxRetryError,
            ConnectionError,
            RetryError,
            ResponseError,
        )
    )
    def test_read_from_s3_via_upath(self, temp_local_dir):
        """Test reading from PUDL's public S3 bucket using UPathCache."""
        # Create UPath caches for S3 (read-only) and local (read-write)
        s3_cache = UPathCache(UPath("s3://pudl.catalyst.coop/zenodo"), read_only=True)
        local_cache = UPathCache(UPath(f"file://{temp_local_dir}"))
        layered = LayeredCache(local_cache, s3_cache)

        # Get a real resource from eia176
        ds = Datastore()
        descriptor = ds.get_datapackage_descriptor("eia176")
        resource = list(descriptor.get_resources())[0]

        # Verify not in local cache initially
        assert not local_cache.contains(resource)

        # Retrieve through layered cache
        content = layered.get(resource)
        assert content is not None
        assert len(content) > 0

        # Explicitly add the resource to the local cache via the layered cache
        layered.add(resource, content)

        # Should now be in local cache
        assert local_cache.contains(resource)

    def test_read_write_gcs_via_upath(self, gcs_test_cache_path, sample_resource):
        """Test read-write operations on GCS using UPathCache."""
        try:
            cache = UPathCache(UPath(gcs_test_cache_path), read_only=False)
        except Exception as e:
            pytest.skip(f"Could not initialize GCS cache: {e}")

        # Test content
        test_content = b"UPath GCS integration test data"

        # Verify resource doesn't exist initially
        assert not cache.contains(sample_resource)

        # Write to GCS
        try:
            cache.add(sample_resource, test_content)
        except RuntimeError as e:
            if "without credentials" in str(e):
                pytest.skip("GCS credentials not available for write operations")
            raise

        # Verify it was written
        assert cache.contains(sample_resource)

        # Read back and verify content
        retrieved = cache.get(sample_resource)
        assert retrieved == test_content

        # Delete the resource
        cache.delete(sample_resource)

        # Verify deletion
        assert not cache.contains(sample_resource)


class TestLayeredCacheIntegration:
    """Integration tests for LayeredCache with real storage backends."""

    @pytest.mark.xfail(
        raises=(
            MaxRetryError,
            ConnectionError,
            RetryError,
            ResponseError,
        )
    )
    def test_three_layer_cache_with_s3(self, temp_local_dir, gcs_test_cache_path):
        """Test a three-layer cache: local -> GCS (read-write) -> S3 (read-only)."""
        # Create three cache layers
        local_cache = UPathCache(UPath(f"file://{temp_local_dir}"))

        try:
            gcs_cache = UPathCache(UPath(gcs_test_cache_path))
        except Exception:
            # The test will be skipped if we do not have access to GCS credentials
            pytest.skip("Could not initialize GCS cache")

        s3_cache = UPathCache(UPath("s3://pudl.catalyst.coop/zenodo"), read_only=True)

        # Build a layered cache
        layered = LayeredCache(local_cache, gcs_cache, s3_cache)

        assert layered.num_layers() == 3

        # Get a real resource from eia176
        ds = Datastore()
        descriptor = ds.get_datapackage_descriptor("eia176")
        resource = list(descriptor.get_resources())[0]

        # Verify not in any writable layer initially
        assert not local_cache.contains(resource)
        assert not gcs_cache.contains(resource)

        # Make sure the resource exists in S3
        assert s3_cache.contains(resource)

        # Retrieve through layered cache (should pull from S3)
        content = layered.get(resource)
        assert content is not None
        assert len(content) > 0

        # LayeredCache doesn't automatically populate earlier layers
        # Explicitly add to populate local cache
        layered.add(resource, content)

        # Should now be in the first writable layer (local)
        assert local_cache.contains(resource)

        # Verify we can read it directly from local cache
        assert local_cache.get(resource) == content

        # Clean up
        local_cache.delete(resource)
        if gcs_cache.contains(resource):
            with contextlib.suppress(Exception):
                gcs_cache.delete(resource)


class TestCacheInteroperability:
    """Test that different cache implementations can interoperate."""

    def test_multiple_upath_caches_in_layered_cache(
        self, temp_local_dir, sample_resource
    ):
        """Test using multiple UPathCache instances in the same LayeredCache."""
        # Create different UPath caches pointing to different directories
        cache1 = UPathCache(UPath(f"file://{Path(temp_local_dir) / 'cache1'}"))
        cache2 = UPathCache(UPath(f"file://{Path(temp_local_dir) / 'cache2'}"))

        # Build layered cache
        layered = LayeredCache(cache1, cache2)

        test_content = b"Multiple UPath caches test"

        # Add to layered cache (should go to all writable layers)
        layered.add(sample_resource, test_content)

        # Should be in both layers since both are writable
        assert cache1.contains(sample_resource)
        assert cache2.contains(sample_resource)
        # Content should match in both layers
        assert cache1.get(sample_resource) == test_content
        assert cache2.get(sample_resource) == test_content

        # Should be retrievable through layered cache
        assert layered.get(sample_resource) == test_content

        # Clean up - should remove from all layers
        layered.delete(sample_resource)
        assert not layered.contains(sample_resource)
        assert not cache1.contains(sample_resource)
        assert not cache2.contains(sample_resource)
