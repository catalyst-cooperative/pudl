"""Tests for xbrl extraction module."""

from pathlib import Path

import dagster as dg
import pytest
from dagster import ResourceDefinition
from dagster._core.definitions.assets.definition.assets_definition import (
    AssetsDefinition,
)
from dagster._core.execution.execute_in_process_result import ExecuteInProcessResult

from pudl.dagster.assets.raw import ferc_to_sqlite
from pudl.dagster.resources import FercXbrlRuntimeSettings
from pudl.extract.ferc1 import Ferc1DbfExtractor
from pudl.extract.xbrl import FercXbrlDatastore, convert_form
from pudl.settings import (
    Ferc1DbfToSqliteDataConfig,
    Ferc1XbrlToSqliteDataConfig,
    Ferc2XbrlToSqliteDataConfig,
    Ferc6XbrlToSqliteDataConfig,
    Ferc60XbrlToSqliteDataConfig,
    Ferc714XbrlToSqliteDataConfig,
    FercGenericXbrlToSqliteDataConfig,
    FercToSqliteDataConfig,
    GlobalDataConfig,
    XbrlFormNumber,
)
from pudl.workspace.datastore import ZenodoDoiSettings
from pudl.workspace.setup import PudlPaths


def test_ferc_xbrl_datastore_get_taxonomy(mocker):
    datastore_mock = mocker.MagicMock()
    datastore_mock.get_unique_resource.return_value = b"Fake taxonomy data."

    ferc_datastore = FercXbrlDatastore(datastore_mock)
    raw_archive = ferc_datastore.get_taxonomy(XbrlFormNumber.FORM1)

    # 2021 data is published with 2022 taxonomy!
    datastore_mock.get_unique_resource.assert_called_with(
        "ferc1", data_format="xbrl_taxonomy"
    )

    assert raw_archive.getvalue() == b"Fake taxonomy data."


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
    "data_config,forms",
    [
        (
            FercToSqliteDataConfig(
                ferc1_xbrl=Ferc1XbrlToSqliteDataConfig(),
                ferc2_xbrl=Ferc2XbrlToSqliteDataConfig(),
                ferc6_xbrl=Ferc6XbrlToSqliteDataConfig(),
                ferc60_xbrl=Ferc60XbrlToSqliteDataConfig(),
                ferc714_xbrl=Ferc714XbrlToSqliteDataConfig(),
            ),
            list(XbrlFormNumber),
        ),
        (
            FercToSqliteDataConfig(
                ferc1_xbrl=None,
                ferc2_xbrl=Ferc2XbrlToSqliteDataConfig(),
                ferc6_xbrl=Ferc6XbrlToSqliteDataConfig(),
                ferc60_xbrl=Ferc60XbrlToSqliteDataConfig(),
                ferc714_xbrl=Ferc714XbrlToSqliteDataConfig(),
            ),
            [form for form in XbrlFormNumber if form != XbrlFormNumber.FORM1],
        ),
        (
            FercToSqliteDataConfig(
                ferc1_dbf=Ferc1DbfToSqliteDataConfig(),
                ferc1_xbrl=None,
                ferc2_xbrl=None,
                ferc6_xbrl=None,
                ferc60_xbrl=None,
                ferc714_xbrl=None,
            ),
            [],
        ),
        (
            FercToSqliteDataConfig(
                ferc1_xbrl=Ferc1XbrlToSqliteDataConfig(years=[]),
                ferc2_xbrl=Ferc2XbrlToSqliteDataConfig(years=[]),
                ferc6_xbrl=Ferc6XbrlToSqliteDataConfig(years=[]),
                ferc60_xbrl=Ferc60XbrlToSqliteDataConfig(years=[]),
                ferc714_xbrl=Ferc714XbrlToSqliteDataConfig(years=[]),
            ),
            [],
        ),
    ],
)
def test_xbrl2sqlite(data_config, forms, mocker, tmp_path):
    convert_form_mock = mocker.MagicMock()
    mocker.patch(
        "pudl.dagster.assets.raw.ferc_to_sqlite.convert_form", new=convert_form_mock
    )

    # Mock datastore object to allow comparison
    mock_datastore = mocker.MagicMock()
    mocker.patch(
        "pudl.dagster.assets.raw.ferc_to_sqlite.FercXbrlDatastore",
        return_value=mock_datastore,
    )

    xbrl_assets: list[AssetsDefinition] = [
        ferc_to_sqlite.raw_ferc1_xbrl__sqlite,
        ferc_to_sqlite.raw_ferc2_xbrl__sqlite,
        ferc_to_sqlite.raw_ferc6_xbrl__sqlite,
        ferc_to_sqlite.raw_ferc60_xbrl__sqlite,
        ferc_to_sqlite.raw_ferc714_xbrl__sqlite,
    ]

    result: ExecuteInProcessResult = dg.materialize(
        assets=xbrl_assets,
        resources={
            "global_data_config": GlobalDataConfig(ferc_to_sqlite=data_config),
            "datastore": ResourceDefinition.mock_resource(),
            "runtime_settings": FercXbrlRuntimeSettings(
                xbrl_batch_size=20,
                xbrl_num_workers=10,
            ),
            "zenodo_dois": ZenodoDoiSettings(),
        },
    )

    assert result.success

    assert convert_form_mock.call_count == len(forms)

    for form in forms:
        convert_form_mock.assert_any_call(
            form_data_config=data_config.get_xbrl_data_config(form),
            form=form,
            datastore=mock_datastore,
            output_path=PudlPaths().output_dir,
            sqlite_path=PudlPaths().output_dir / f"ferc{form.value}_xbrl.sqlite",
            duckdb_path=PudlPaths().output_dir / f"ferc{form.value}_xbrl.duckdb",
            batch_size=20,
            workers=10,
            loglevel="INFO",
        )


def test_convert_form(mocker):
    """Test convert_form method is properly calling extractor."""
    extractor_mock = mocker.MagicMock()
    mocker.patch("pudl.extract.xbrl.run_main", new=extractor_mock)

    # Create fake datastore class for testing
    class FakeDatastore:
        def get_taxonomy(self, form: XbrlFormNumber):
            return f"raw_archive_{form.value}"

        def get_filings(self, year, form: XbrlFormNumber):
            return f"filings_{year}_{form.value}"

    settings = FercGenericXbrlToSqliteDataConfig(
        years=[2020, 2021],
    )

    output_path: Path = PudlPaths().pudl_output

    # Test convert_form for every form number
    for form in XbrlFormNumber:
        convert_form(
            settings,
            form,
            FakeDatastore(),
            output_path=output_path,
            sqlite_path=output_path / f"ferc{form.value}_xbrl.sqlite",
            duckdb_path=output_path / f"ferc{form.value}_xbrl.duckdb",
            batch_size=10,
            workers=5,
        )

        # Verify extractor is called correctly
        filings: list[str] = [f"filings_{year}_{form.value}" for year in settings.years]
        extractor_mock.assert_called_with(
            filings=filings,
            sqlite_path=output_path / f"ferc{form.value}_xbrl.sqlite",
            duckdb_path=output_path / f"ferc{form.value}_xbrl.duckdb",
            taxonomy=f"raw_archive_{form.value}",
            form_number=form.value,
            metadata_path=output_path / f"ferc{form.value}_xbrl_taxonomy_metadata.json",
            datapackage_path=output_path / f"ferc{form.value}_xbrl_datapackage.json",
            workers=5,
            batch_size=10,
            loglevel="INFO",
            logfile=None,
        )
        extractor_mock.reset_mock()


def test_ferc_dbf_extractor_skips_with_empty_years(mocker, tmp_path):
    """FercDbfExtractor.execute() should return early when years=[]."""
    mocker.patch.object(
        Ferc1DbfExtractor, "get_dbf_reader", return_value=mocker.MagicMock()
    )
    mocker.patch("pudl.extract.dbf.sa.create_engine", return_value=mocker.MagicMock())
    mocker.patch("pudl.extract.dbf.sa.MetaData", return_value=mocker.MagicMock())

    data_config = FercToSqliteDataConfig(
        ferc1_dbf=Ferc1DbfToSqliteDataConfig(years=[]),
    )
    extractor = Ferc1DbfExtractor(
        datastore=mocker.MagicMock(),
        data_config=data_config,
        output_path=tmp_path,
    )

    delete_schema_mock = mocker.patch.object(extractor, "delete_schema")
    extractor.execute()

    delete_schema_mock.assert_not_called()
