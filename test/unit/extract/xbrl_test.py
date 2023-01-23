"""Tests for xbrl extraction module."""
import io
import json
from datetime import datetime, timedelta

import pytest

from pudl.extract.xbrl import FercXbrlDatastore
from pudl.settings import XbrlFormNumber


def test_ferc_xbrl_datastore_get_taxonomy(mocker):
    """Test FercXbrlDatastore class."""
    datastore_mock = mocker.MagicMock()
    datastore_mock.get_unique_resource.return_value = b"Fake taxonomy data."

    ferc_datastore = FercXbrlDatastore(datastore_mock)
    raw_archive, taxonomy_entry_point = ferc_datastore.get_taxonomy(
        2021, XbrlFormNumber.FORM1
    )

    # Check that get_unique_resource was called correctly
    datastore_mock.get_unique_resource.assert_called_with(
        "ferc1", year=2021, data_format="XBRL"
    )

    # Check return values
    assert raw_archive.getvalue() == b"Fake taxonomy data."
    assert (
        taxonomy_entry_point
        == "taxonomy/form1/2021-01-01/form/form1/form-1_2021-01-01.xsd"
    )


@pytest.mark.parametrize(
    "file_map,selected_filings",
    [
        (
            {
                "rssfeed": io.StringIO(
                    json.dumps(
                        {
                            "filer 1Q4": {
                                "id1": {
                                    "entry_id": "id1",
                                    "title": "filer 1",
                                    "download_url": "www.fake.url",
                                    "published_parsed": str(datetime.now()),
                                    "ferc_formname": "FercForm.FORM_1",
                                    "ferc_year": 2021,
                                    "ferc_period": "Q4",
                                },
                                "id2": {
                                    "entry_id": "id2",
                                    "title": "filer 1",
                                    "download_url": "www.other_fake.url",
                                    "published_parsed": str(
                                        datetime.now() - timedelta(days=1)
                                    ),
                                    "ferc_formname": "FercForm.FORM_1",
                                    "ferc_year": 2021,
                                    "ferc_period": "Q4",
                                },
                            },
                            "filer 2Q4": {
                                "id3": {
                                    "entry_id": "id3",
                                    "title": "filer 2",
                                    "download_url": "www.fake.url",
                                    "published_parsed": str(
                                        datetime.now() - timedelta(days=100)
                                    ),
                                    "ferc_formname": "FercForm.FORM_1",
                                    "ferc_year": 2021,
                                    "ferc_period": "Q4",
                                },
                                "id4": {
                                    "entry_id": "id4",
                                    "title": "filer 2",
                                    "download_url": "www.fake.url",
                                    "published_parsed": str(datetime.now()),
                                    "ferc_formname": "FercForm.FORM_1",
                                    "ferc_year": 2021,
                                    "ferc_period": "Q4",
                                },
                            },
                        }
                    )
                ),
                "id1.xbrl": io.BytesIO(b"filer 1 fake filing."),
                "id2.xbrl": io.BytesIO(b"filer 1 old filing (shouldn't be used)."),
                "id3.xbrl": io.BytesIO(b"filer 2 old filing (shouldn't be used)."),
                "id4.xbrl": io.BytesIO(b"filer 2 fake filing."),
            },
            {
                "id1": b"filer 1 fake filing.",
                "id4": b"filer 2 fake filing.",
            },
        ),
    ],
)
def test_ferc_xbrl_datastore_get_filings(mocker, file_map, selected_filings):
    """Test FercXbrlDatastore class."""
    datastore_mock = mocker.MagicMock()

    # Get mock of archive zipfile
    archive_mock = datastore_mock.get_zipfile_resource.return_value
    archive_mock.open.side_effect = file_map.get

    # Call method
    ferc_datastore = FercXbrlDatastore(datastore_mock)
    filings = ferc_datastore.get_filings(2021, XbrlFormNumber.FORM1)

    # Check that get_zipfile_resource was called correctly
    datastore_mock.get_zipfile_resource.assert_called_with(
        "ferc1", year=2021, data_format="XBRL"
    )

    # Loop through filings and verify the contents
    for filing in filings:
        assert filing.name in selected_filings
        assert filing.file.getvalue() == selected_filings[filing.name]
