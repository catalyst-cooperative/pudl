"""Tests for xbrl extraction module."""

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
from pudl.workspace.setup import PudlPaths


def test_ferc_xbrl_datastore_get_taxonomy(mocker):
    datastore_mock = mocker.MagicMock()
    datastore_mock.get_unique_resource.return_value = b"Fake taxonomy data."

    ferc_datastore = FercXbrlDatastore(datastore_mock)
    raw_archive, taxonomy_entry_point = ferc_datastore.get_taxonomy(
        2021, XbrlFormNumber.FORM1
    )

    # 2021 data is published with 2022 taxonomy!
    datastore_mock.get_unique_resource.assert_called_with(
        "ferc1", year=2022, data_format="xbrl_taxonomy"
    )

    assert raw_archive.getvalue() == b"Fake taxonomy data."
    assert (
        taxonomy_entry_point
        == "taxonomy/form1/2022-01-01/form/form1/form-1_2022-01-01.xsd"
    )


def test_ferc_xbrl_datastore_get_filings(mocker):
    datastore_mock = mocker.MagicMock()
    datastore_mock.get_unique_resource = mocker.MagicMock(
        return_value=b"Just some bogus bytes"
    )

    # Call method
    ferc_datastore = FercXbrlDatastore(datastore_mock)
    ferc_datastore.get_filings(2021, XbrlFormNumber.FORM1)

    # Check that get_unique_resource was called correctly
    datastore_mock.get_unique_resource.assert_called_with(
        "ferc1", year=2021, data_format="xbrl"
    )


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
            list(XbrlFormNumber),
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
    convert_form_mock = mocker.MagicMock()
    mocker.patch("pudl.extract.xbrl.convert_form", new=convert_form_mock)

    # Mock datastore object to allow comparison
    mock_datastore = mocker.MagicMock()
    mocker.patch("pudl.extract.xbrl.FercXbrlDatastore", return_value=mock_datastore)

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

    assert convert_form_mock.call_count == len(forms)

    for form in forms:
        convert_form_mock.assert_any_call(
            settings.get_xbrl_dataset_settings(form),
            form,
            mock_datastore,
            output_path=PudlPaths().output_dir,
            batch_size=20,
            workers=10,
            clobber=True,
        )


def test_convert_form(mocker):
    """Test convert_form method is properly calling extractor."""
    extractor_mock = mocker.MagicMock()
    mocker.patch("pudl.extract.xbrl.run_main", new=extractor_mock)

    # Create fake datastore class for testing
    class FakeDatastore:
        def get_taxonomy(self, year, form: XbrlFormNumber):
            return (
                f"raw_archive_{year}_{form.value}",
                f"taxonomy_entry_point_{year}_{form.value}",
            )

        def get_filings(self, year, form: XbrlFormNumber):
            return f"filings_{year}_{form.value}"

    settings = FercGenericXbrlToSqliteSettings(
        taxonomy="https://www.fake.taxonomy.url",
        years=[2020, 2021],
    )

    output_path = PudlPaths().pudl_output

    # Test convert_form for every form number
    for form in XbrlFormNumber:
        convert_form(
            settings,
            form,
            FakeDatastore(),
            output_path=output_path,
            clobber=True,
            batch_size=10,
            workers=5,
        )

        # Verify extractor is called correctly
        expected_calls = []
        for year in settings.years:
            expected_calls.append(
                mocker.call(
                    instance_path=f"filings_{year}_{form.value}",
                    sql_path=str(output_path / f"ferc{form.value}_xbrl.sqlite"),
                    clobber=True,
                    taxonomy=f"raw_archive_{year}_{form.value}",
                    entry_point=f"taxonomy_entry_point_{year}_{form.value}",
                    form_number=form.value,
                    metadata_path=str(
                        output_path / f"ferc{form.value}_xbrl_taxonomy_metadata.json"
                    ),
                    datapackage_path=str(
                        output_path / f"ferc{form.value}_xbrl_datapackage.json"
                    ),
                    workers=5,
                    batch_size=10,
                    loglevel="INFO",
                    logfile=None,
                )
            )
        assert extractor_mock.mock_calls == expected_calls
        extractor_mock.reset_mock()
