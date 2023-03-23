"""Tests for xbrl extraction module."""
import io
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from dagster import build_op_context

from pudl.extract.xbrl import FercXbrlDatastore, convert_form, xbrl2sqlite
from pudl.settings import (
    Ferc1DbfToSqliteSettings,
    Ferc1XbrlToSqliteSettings,
    Ferc2XbrlToSqliteSettings,
    Ferc6XbrlToSqliteSettings,
    Ferc60XbrlToSqliteSettings,
    Ferc714XbrlToSqliteSettings,
    FercGenericXbrlToSqliteSettings,
    FercToSqliteSettings,
    XbrlFormNumber,
)


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


@pytest.mark.parametrize(
    "settings,forms",
    [
        (
            FercToSqliteSettings(
                ferc1_xbrl_to_sqlite_settings=Ferc1XbrlToSqliteSettings(),
                ferc2_xbrl_to_sqlite_settings=Ferc2XbrlToSqliteSettings(),
                ferc6_xbrl_to_sqlite_settings=Ferc6XbrlToSqliteSettings(),
                ferc60_xbrl_to_sqlite_settings=Ferc60XbrlToSqliteSettings(),
                ferc714_xbrl_to_sqlite_settings=Ferc714XbrlToSqliteSettings(),
            ),
            [form for form in XbrlFormNumber],
        ),
        (
            FercToSqliteSettings(
                ferc1_xbrl_to_sqlite_settings=None,
                ferc2_xbrl_to_sqlite_settings=Ferc2XbrlToSqliteSettings(),
                ferc6_xbrl_to_sqlite_settings=Ferc6XbrlToSqliteSettings(),
                ferc60_xbrl_to_sqlite_settings=Ferc60XbrlToSqliteSettings(),
                ferc714_xbrl_to_sqlite_settings=Ferc714XbrlToSqliteSettings(),
            ),
            [form for form in XbrlFormNumber if form != XbrlFormNumber.FORM1],
        ),
        (
            FercToSqliteSettings(
                ferc1_dbf_to_sqlite_settings=Ferc1DbfToSqliteSettings(),
                ferc1_xbrl_to_sqlite_settings=None,
                ferc2_xbrl_to_sqlite_settings=None,
                ferc6_xbrl_to_sqlite_settings=None,
                ferc60_xbrl_to_sqlite_settings=None,
                ferc714_xbrl_to_sqlite_settings=None,
            ),
            [],
        ),
    ],
)
def test_xbrl2sqlite(settings, forms, mocker):
    """Test xbrl2sqlite function."""
    convert_form_mock = mocker.MagicMock()
    mocker.patch("pudl.extract.xbrl.convert_form", new=convert_form_mock)

    mocker.patch("pudl.extract.xbrl._get_sqlite_engine", return_value="sqlite_engine")

    # Mock datastore object to allow comparison
    mocker.patch("pudl.extract.xbrl.FercXbrlDatastore", return_value="datastore")

    # Construct xbrl2sqlite op context
    context = build_op_context(
        resources={
            "ferc_to_sqlite_settings": settings,
            "datastore": "datastore",
        },
        config={
            "workers": 10,
            "batch_size": 20,
            "clobber": True,
        },
    )

    xbrl2sqlite(context)

    if len(forms) == 0:
        convert_form_mock.assert_not_called()

    for form in forms:
        convert_form_mock.assert_any_call(
            settings.get_xbrl_dataset_settings(form),
            form,
            "datastore",
            "sqlite_engine",
            output_path=Path(os.getenv("PUDL_OUTPUT")),
            batch_size=20,
            workers=10,
        )


def test_convert_form(mocker):
    """Test convert_form method is properly calling extractor."""
    extractor_mock = mocker.MagicMock()
    mocker.patch("pudl.extract.xbrl.xbrl.extract", new=extractor_mock)

    # Create fake datastore class for testing
    class FakeDatastore:
        def get_taxonomy(self, year, form):
            return f"raw_archive_{year}_{form}", f"taxonomy_entry_point_{year}_{form}"

        def get_filings(self, year, form):
            return f"filings_{year}_{form}"

    settings = FercGenericXbrlToSqliteSettings(
        taxonomy="https://www.fake.taxonomy.url",
        years=[2020, 2021],
    )

    output_path = Path("/output/path/")

    # Test convert_form for every form number
    for form in XbrlFormNumber:
        convert_form(
            settings,
            form,
            FakeDatastore(),
            "sqlite_engine",
            output_path=output_path,
            batch_size=10,
            workers=5,
        )

        # Verify extractor is called correctly
        for year in settings.years:
            extractor_mock.assert_any_call(
                f"filings_{year}_{form}",
                "sqlite_engine",
                f"raw_archive_{year}_{form}",
                form.value,
                batch_size=10,
                workers=5,
                datapackage_path=str(
                    output_path / f"ferc{form.value}_xbrl_datapackage.json"
                ),
                metadata_path=str(
                    output_path / f"ferc{form.value}_xbrl_taxonomy_metadata.json"
                ),
                archive_file_path=f"taxonomy_entry_point_{year}_{form}",
            )
