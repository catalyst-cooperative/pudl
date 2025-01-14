"""Unit tests for Datastore module."""

import io
import json
import re
import zipfile
from typing import Any

import pytest
import responses

from pudl.workspace import datastore
from pudl.workspace.resource_cache import PudlResourceKey


def _make_resource(name: str, **partitions) -> dict[str, Any]:
    """Returns json representation of a resource."""
    return {
        "name": name,
        "path": f"http://localhost/{name}",
        "parts": dict(partitions),
    }


def _make_descriptor(
    dataset: str, doi: str, *resources: dict[str, Any]
) -> datastore.DatapackageDescriptor:
    """Returns new instance of DatapackageDescriptor containing given resources.

    This is a helper for quickly making descriptors for unit testing. You can use
    it in a following fashion:

    desc = _make_descriptor("dataset_name", "doi-123", _make_resource(...), _make_resource(...), ...)

    Args:
        dataset: name of the dataset
        doi: doi identifier
        resources: list of resources that should be attached to the resource. This should
         be json representation, e.g. constructed by calling _make_resource().
    """
    return datastore.DatapackageDescriptor(
        {"resources": list(resources)},
        dataset=dataset,
        doi=doi,
    )


def test_get_partition_filters():
    desc = _make_descriptor(
        "blabla",
        "doi-123",
        _make_resource("foo", group="first", color="red"),
        _make_resource("bar", group="first", color="blue"),
        _make_resource("baz", group="second", color="black", order=1),
    )
    assert list(desc.get_partition_filters()) == [
        {"group": "first", "color": "red"},
        {"group": "first", "color": "blue"},
        {"group": "second", "color": "black", "order": 1},
    ]
    assert list(desc.get_partition_filters(group="first")) == [
        {"group": "first", "color": "red"},
        {"group": "first", "color": "blue"},
    ]
    assert list(desc.get_partition_filters(color="blue")) == [
        {"group": "first", "color": "blue"},
    ]
    assert list(desc.get_partition_filters(color="blue", group="second")) == []


def test_get_resource_path():
    """Check that get_resource_path returns correct paths."""
    desc = _make_descriptor(
        "blabla",
        "doi-123",
        _make_resource("foo", group="first", color="red"),
        _make_resource("bar", group="first", color="blue"),
    )
    assert desc.get_resource_path("foo") == "http://localhost/foo"
    assert desc.get_resource_path("bar") == "http://localhost/bar"
    with pytest.raises(KeyError):
        desc.get_resource_path("other")


def test_modernize_zenodo_legacy_api_url():
    legacy_url = "https://zenodo.org/api/files/082e4932-c772-4e9c-a670-376a1acc3748/datapackage.json"

    descriptor = datastore.DatapackageDescriptor(
        {"resources": [{"name": "datapackage.json", "path": legacy_url}]},
        dataset="test",
        doi="10.5281/zenodo.123123",
    )

    assert (
        descriptor.get_resource_path("datapackage.json")
        == "https://zenodo.org/records/123123/files/datapackage.json"
    )


def test_get_resources_filtering():
    """Verifies correct operation of get_resources()."""
    desc = _make_descriptor(
        "data",
        "doi-123",
        _make_resource("foo", group="first", color="red"),
        _make_resource("bar", group="first", color="blue", rank=5),
        _make_resource("baz", group="second", color="blue", rank=5, mood="VeryHappy"),
    )
    assert list(desc.get_resources()) == [
        PudlResourceKey("data", "doi-123", "foo"),
        PudlResourceKey("data", "doi-123", "bar"),
        PudlResourceKey("data", "doi-123", "baz"),
    ]
    # Simple filtering by one attribute.
    assert list(desc.get_resources(group="first")) == [
        PudlResourceKey("data", "doi-123", "foo"),
        PudlResourceKey("data", "doi-123", "bar"),
    ]
    # Filter by two attributes
    assert list(desc.get_resources(group="first", rank=5)) == [
        PudlResourceKey("data", "doi-123", "bar"),
    ]
    # Attributes that do not match anything
    assert list(desc.get_resources(group="second", shape="square")) == []
    # Search attribute values are cast to lowercase strings
    assert list(desc.get_resources(rank="5", mood="VERYhappy")) == [
        PudlResourceKey("data", "doi-123", "baz"),
    ]
    # Test lookup by name
    assert list(desc.get_resources("foo")) == [
        PudlResourceKey("data", "doi-123", "foo"),
    ]


def test_json_string_representation():
    """Checks that json representation parses to the same dict."""
    desc = _make_descriptor(
        "data",
        "doi-123",
        _make_resource("foo", group="first"),
        _make_resource("bar", group="second"),
        _make_resource("baz"),
    )
    assert json.loads(desc.get_json_string()) == {
        "resources": [
            {
                "name": "foo",
                "path": "http://localhost/foo",
                "parts": {"group": "first"},
            },
            {
                "name": "bar",
                "path": "http://localhost/bar",
                "parts": {"group": "second"},
            },
            {
                "name": "baz",
                "path": "http://localhost/baz",
                "parts": {},
            },
        ],
    }


class MockableZenodoFetcher(datastore.ZenodoFetcher):
    """Test friendly version of ZenodoFetcher.

    Allows populating _descriptor_cache at the initialization time.
    """

    def __init__(
        self, descriptors: dict[str, datastore.DatapackageDescriptor], **kwargs
    ):
        """Construct a test-friendly ZenodoFetcher with descriptors pre-loaded."""
        super().__init__(**kwargs)
        self._descriptor_cache = descriptors


class TestZenodoFetcher:
    """Unit tests for ZenodoFetcher class."""

    MOCK_EPACEMS_DEPOSITION = {
        "entries": [
            {"key": "random.zip"},
            {
                "key": "datapackage.json",
                "links": {"content": "http://localhost/my/datapackage.json"},
            },
        ]
    }

    MOCK_EPACEMS_DATAPACKAGE = {
        "resources": [
            {
                "name": "first",
                "path": "http://localhost/first",
                "hash": "6f1ed002ab5595859014ebf0951522d9",
            },  # md5sum of "blah"
            {
                "name": "second",
                "path": "http://localhost/second",
                "hash": "6f1ed002ab5595859014ebf0951522d9",
            },
        ]
    }
    PROD_EPACEMS_DOI = datastore.ZenodoDoiSettings().epacems
    # last numeric part of doi
    PROD_EPACEMS_ZEN_ID = re.search(
        r"^10\.(5072|5281)/zenodo\.(\d+)$", PROD_EPACEMS_DOI
    ).group(2)

    @pytest.fixture(autouse=True)
    def setup(self):
        """Constructs mockable Zenodo fetcher based on MOCK_EPACEMS_DATAPACKAGE."""
        self.fetcher = MockableZenodoFetcher(
            descriptors={
                self.PROD_EPACEMS_DOI: datastore.DatapackageDescriptor(
                    self.MOCK_EPACEMS_DATAPACKAGE,
                    dataset="epacems",
                    doi=self.PROD_EPACEMS_DOI,
                )
            }
        )

    def test_doi_format_is_correct(self):
        """Verifies ZenodoFetcher DOIs have correct format and are not sandbox DOIs.

        Sandbox DOIs are only meant for use in testing and development, and should not
        be checked in, thus this test will fail if a sandbox DOI with prefix 10.5072 is
        identified.
        """
        zf = datastore.ZenodoFetcher()
        assert zf.get_known_datasets()
        for dataset, doi in zf.zenodo_dois:
            assert zf.get_doi(dataset) == doi, (
                f"Zenodo DOI for {dataset} matches result of get_doi()"
            )
            assert not re.fullmatch(r"10\.5072/zenodo\.[0-9]{5,10}", doi), (
                f"Zenodo sandbox DOI found for {dataset}: {doi}"
            )
            assert re.fullmatch(r"10\.5281/zenodo\.[0-9]{5,10}", doi), (
                f"Zenodo production DOI for {dataset} is {doi}"
            )

    def test_get_known_datasets(self):
        """Call to get_known_datasets() produces the expected results."""
        assert (
            sorted(name for name, doi in datastore.ZenodoFetcher().zenodo_dois)
            == self.fetcher.get_known_datasets()
        )

    def test_get_unknown_dataset(self):
        """Ensure that we get a failure when attempting to access an invalid dataset."""
        with pytest.raises(AttributeError):
            self.fetcher.get_doi("unknown")

    def test_doi_of_prod_epacems_matches(self):
        """Most of the tests assume specific DOI for production epacems dataset.

        This test verifies that the expected value is in use.
        """
        assert self.fetcher.get_doi("epacems") == self.PROD_EPACEMS_DOI

    @responses.activate
    def test_get_descriptor_http_calls(self):
        """Tests that correct http requests are fired when loading datapackage.json."""
        fetcher = datastore.ZenodoFetcher()
        responses.add(
            responses.GET,
            f"https://zenodo.org/api/records/{self.PROD_EPACEMS_ZEN_ID}/files",
            json=self.MOCK_EPACEMS_DEPOSITION,
        )
        responses.add(
            responses.GET,
            "http://localhost/my/datapackage.json",
            json=self.MOCK_EPACEMS_DATAPACKAGE,
        )
        desc = fetcher.get_descriptor("epacems")
        assert desc.datapackage_json == self.MOCK_EPACEMS_DATAPACKAGE

    @responses.activate
    def test_get_resource(self):
        """Test that get_resource() calls expected http request and returns content."""
        responses.add(responses.GET, "http://localhost/first", body="blah")
        res = self.fetcher.get_resource(
            PudlResourceKey("epacems", self.PROD_EPACEMS_DOI, "first")
        )
        assert res == b"blah"

    @responses.activate
    def test_get_resource_with_invalid_checksum(self):
        """Test that resource with bad checksum raises ChecksumMismatchError."""
        responses.add(responses.GET, "http://localhost/first", body="wrongContent")
        res = PudlResourceKey("epacems", self.PROD_EPACEMS_DOI, "first")
        with pytest.raises(datastore.ChecksumMismatchError):
            self.fetcher.get_resource(res)

    def test_get_resource_with_nonexistent_resource_fails(self):
        """If resource does not exist, get_resource() throws KeyError."""
        res = PudlResourceKey("epacems", self.PROD_EPACEMS_DOI, "nonexistent")
        with pytest.raises(KeyError):
            self.fetcher.get_resource(res)


def test_get_zipfile_resource_failure(mocker):
    ds = datastore.Datastore()
    ds.get_unique_resource = mocker.MagicMock(return_value=b"")
    sleep_mock = mocker.MagicMock()
    with (
        mocker.patch("time.sleep", sleep_mock),
        mocker.patch("zipfile.ZipFile", side_effect=zipfile.BadZipFile),
        pytest.raises(zipfile.BadZipFile),
    ):
        ds.get_zipfile_resource("test_dataset")


def test_get_zipfile_resource_eventual_success(mocker):
    file_contents = "aaa"
    zipfile_bytes = io.BytesIO()
    with zipfile.ZipFile(zipfile_bytes, "w") as a_zipfile:
        a_zipfile.writestr("file_name", file_contents)

    ds = datastore.Datastore()
    ds.get_unique_resource = mocker.MagicMock(return_value=b"")
    with (
        mocker.patch("time.sleep"),
        mocker.patch(
            "zipfile.ZipFile",
            side_effect=[
                zipfile.BadZipFile,
                zipfile.BadZipFile,
                zipfile.ZipFile(zipfile_bytes),
            ],
        ),
    ):
        observed_zipfile = ds.get_zipfile_resource("test_dataset")
        test_file = observed_zipfile.open("file_name")
        assert test_file.read().decode(encoding="utf-8") == file_contents


def test_get_zipfile_resources_eventual_success(mocker):
    file_contents = "aaa"
    zipfile_bytes = io.BytesIO()
    with zipfile.ZipFile(zipfile_bytes, "w") as a_zipfile:
        a_zipfile.writestr("file_name", file_contents)

    ds = datastore.Datastore()
    ds.get_resources = mocker.MagicMock(
        return_value=iter(
            [
                (
                    PudlResourceKey("test_dataset", "test_doi", "test_name_0"),
                    zipfile_bytes,
                ),
                (
                    PudlResourceKey("test_dataset", "test_doi", "test_name_1"),
                    zipfile_bytes,
                ),
            ]
        )
    )
    with (
        mocker.patch(
            "zipfile.ZipFile",
            side_effect=[
                zipfile.BadZipFile,
                zipfile.BadZipFile,
                zipfile.ZipFile(zipfile_bytes),
                zipfile.BadZipFile,
                zipfile.BadZipFile,
                zipfile.ZipFile(zipfile_bytes),
            ],
        ),
        mocker.patch("time.sleep"),
    ):
        observed_zipfiles = ds.get_zipfile_resources("test_dataset")
        for _key, observed_zipfile in observed_zipfiles:
            with observed_zipfile.open("file_name") as test_file:
                assert test_file.read().decode(encoding="utf-8") == file_contents


# TODO: add unit tests for Datasource class as well
