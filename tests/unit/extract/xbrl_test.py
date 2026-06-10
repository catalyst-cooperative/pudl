"""Tests for xbrl extraction module."""

import json
from pathlib import Path
from zipfile import ZipFile

import dagster as dg
import pytest
from dagster import ResourceDefinition
from dagster._core.definitions.assets.definition.assets_definition import (
    AssetsDefinition,
)
from dagster._core.execution.execute_in_process_result import ExecuteInProcessResult

from pudl import PUDL_EEL_HOLE_BASE_PATH
from pudl.dagster.assets.raw import ferc_to_sqlite
from pudl.dagster.provenance import (
    FercSqliteProvenanceRecord,
    get_xbrl_extractor_version,
)
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
    FercForm,
    FercToSqliteDataConfig,
    GlobalDataConfig,
)
from pudl.workspace.datastore import ZenodoDoiSettings


def test_ferc_xbrl_datastore_get_taxonomy(mocker):
    datastore_mock = mocker.MagicMock()
    datastore_mock.get_unique_resource.return_value = b"Fake taxonomy data."

    ferc_datastore = FercXbrlDatastore(datastore_mock)
    raw_archive = ferc_datastore.get_taxonomy(FercForm.FORM1)

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
    ferc_datastore.get_filings(2021, FercForm.FORM1)

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
            list(FercForm),
        ),
        (
            FercToSqliteDataConfig(
                ferc1_xbrl=None,
                ferc2_xbrl=Ferc2XbrlToSqliteDataConfig(),
                ferc6_xbrl=Ferc6XbrlToSqliteDataConfig(),
                ferc60_xbrl=Ferc60XbrlToSqliteDataConfig(),
                ferc714_xbrl=Ferc714XbrlToSqliteDataConfig(),
            ),
            [form for form in FercForm if form != FercForm.FORM1],
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
def test_xbrl2sqlite(data_config, forms, mocker, pudl_test_paths):
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

    # Mock compatibility check to avoid looking at local / remote dbs
    mocker.patch(
        "pudl.dagster.assets.raw.ferc_to_sqlite._check_for_cached_db_w_compatible_provenance",
        return_value=None,
    )
    # Skip writing provenance metadata to sqlite
    mocker.patch(
        "pudl.dagster.assets.raw.ferc_to_sqlite.FercSqliteProvenanceRecord.to_datapackage"
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
            "pudl_paths": pudl_test_paths,
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
            ferc_to_sqlite=data_config,
            form=form,
            datastore=mock_datastore,
            pudl_paths=pudl_test_paths,
            batch_size=20,
            workers=10,
            loglevel="INFO",
        )


def test_convert_form(mocker, pudl_test_paths: Path):
    """Test convert_form method is properly calling extractor."""
    extractor_mock = mocker.MagicMock()
    mocker.patch("pudl.extract.xbrl.run_main", new=extractor_mock)

    # Create fake datastore class for testing
    class FakeDatastore:
        def get_taxonomy(self, form: FercForm):
            return f"raw_archive_{form.value}"

        def get_filings(self, year, form: FercForm):
            return f"filings_{year}_{form.value}"

    settings = FercToSqliteDataConfig(
        ferc1_xbrl=Ferc1XbrlToSqliteDataConfig(years=[2020, 2021]),
        ferc2_xbrl=Ferc2XbrlToSqliteDataConfig(years=[2020, 2021]),
        ferc6_xbrl=Ferc6XbrlToSqliteDataConfig(years=[2020, 2021]),
        ferc60_xbrl=Ferc60XbrlToSqliteDataConfig(years=[2020, 2021]),
        ferc714_xbrl=Ferc714XbrlToSqliteDataConfig(years=[2020, 2021]),
    )

    output_path: Path = pudl_test_paths.pudl_output

    # Test convert_form for every form number
    for form in FercForm:
        convert_form(
            settings,
            form,
            FakeDatastore(),
            pudl_test_paths,
            batch_size=10,
            workers=5,
        )

        # Verify extractor is called correctly
        filings: list[str] = [
            f"filings_{year}_{form.value}"
            for year in settings.get_dataset_years("ferc1", "xbrl")
        ]
        extractor_mock.assert_called_with(
            filings=filings,
            sqlite_path=output_path / f"{form}_xbrl.sqlite",
            duckdb_path=output_path / f"{form}_xbrl.duckdb",
            taxonomy=f"raw_archive_{form.value}",
            form_number=form.value,
            output_dir=output_path,
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

    initialize_database_mock = mocker.patch.object(extractor, "initialize_database")
    extractor.execute()

    initialize_database_mock.assert_not_called()


def _run_ferc_to_sqlite_asset(
    zenodo_doi: str, test_asset, data_config, pudl_paths
) -> FercSqliteProvenanceRecord:
    """Run test ferc_to_sqlite asset then return provenance metadata output by it."""
    result: ExecuteInProcessResult = dg.materialize(
        assets=[test_asset],
        resources={
            "global_data_config": GlobalDataConfig(ferc_to_sqlite=data_config),
            "datastore": ResourceDefinition.mock_resource(),
            "runtime_settings": FercXbrlRuntimeSettings(),
            "zenodo_dois": ZenodoDoiSettings(ferc1=zenodo_doi),
            "pudl_paths": pudl_paths,
        },
    )
    materialization_events = result.get_asset_materialization_events()

    assert len(materialization_events) == 1
    metadata = materialization_events[0].materialization.metadata["ferc_to_sqlite"].data
    return FercSqliteProvenanceRecord(**metadata)


def test_ferc_to_sqlite_asset_factory(mocker, pudl_test_paths):
    """Test the logic for checking for a compatible cached FERC SQLite DB."""
    # Basic parameters
    dataset = "ferc1"
    data_format = "dbf"
    status = "complete"
    data_config = FercToSqliteDataConfig(ferc1_dbf=Ferc1DbfToSqliteDataConfig())
    years = data_config.get_dataset_years(dataset, data_format)
    local_doi = "10.5072/zenodo.1"
    nightly_doi = "10.5072/zenodo.2"
    uncached_doi = "10.5072/zenodo.3"
    local_datapackage_path = (
        pudl_test_paths.pudl_output / f"{dataset}_{data_format}_datapackage.json"
    )
    nightly_datapackage_path = (
        PUDL_EEL_HOLE_BASE_PATH / f"{dataset}_{data_format}_datapackage.json"
    )

    # Create test asset
    mock_extract_function = mocker.MagicMock(
        side_effect=lambda _: local_datapackage_path.write_text("{}")
    )
    test_asset = ferc_to_sqlite.ferc_to_sqlite_asset_factory(
        dataset=dataset,
        data_format=data_format,
        extract_function=mock_extract_function,
    )

    # Construct provenance records for local and nightly DB's
    local_provenance = FercSqliteProvenanceRecord(
        dataset=dataset,
        data_format=data_format,
        status=status,
        zenodo_doi=local_doi,
        years=years,
        ferc_xbrl_extractor_version=get_xbrl_extractor_version(),
    )
    nightly_provenance = FercSqliteProvenanceRecord(
        dataset=dataset,
        data_format=data_format,
        status=status,
        zenodo_doi=nightly_doi,
        years=years,
        ferc_xbrl_extractor_version=get_xbrl_extractor_version(),
    )

    # Mock load provenance
    original_from_datapackage = FercSqliteProvenanceRecord.from_datapackage
    mocker.patch(
        "pudl.dagster.assets.raw.ferc_to_sqlite.FercSqliteProvenanceRecord.from_datapackage",
        side_effect=lambda path: (
            nightly_provenance
            if path == nightly_datapackage_path
            else original_from_datapackage(path)
        ),
    )

    # Test with local doi to return local_provenance record
    local_datapackage_path.write_text("{}")
    local_provenance.to_datapackage(local_datapackage_path)
    assert local_provenance == _run_ferc_to_sqlite_asset(
        local_doi, test_asset, data_config, pudl_test_paths
    )
    mock_extract_function.assert_not_called()

    # Test with nightly doi to return nightly_provenance record
    # Mock nightly downloads
    mocker.patch(
        "pudl.dagster.assets.raw.ferc_to_sqlite._download_nightly_outputs",
        side_effect=lambda *args, **kwargs: nightly_provenance.to_datapackage(
            local_datapackage_path
        ),
    )

    assert nightly_provenance == _run_ferc_to_sqlite_asset(
        nightly_doi, test_asset, data_config, pudl_test_paths
    )
    mock_extract_function.assert_not_called()

    # Test with doi from neither local / nightly, which should trigger extraction
    uncached_provenance = _run_ferc_to_sqlite_asset(
        uncached_doi, test_asset, data_config, pudl_test_paths
    )
    assert uncached_provenance.zenodo_doi == uncached_doi
    assert uncached_provenance == FercSqliteProvenanceRecord.model_validate(
        json.loads(local_datapackage_path.read_text())["provenance_metadata"]
    )
    mock_extract_function.assert_called_once()


@pytest.mark.parametrize("dataset,data_format", [("ferc1", "dbf"), ("ferc1", "xbrl")])
def test_download_nightly_outputs(
    dataset, data_format, pudl_test_paths, tmp_path, mocker
):
    """Test that all nightly build outputs are downloaded correctly."""
    # Prepare directory full of mock nightly build outputs
    mocker.patch(
        "pudl.dagster.assets.raw.ferc_to_sqlite.PUDL_EEL_HOLE_BASE_PATH", tmp_path
    )
    ferc_paths = ferc_to_sqlite.FercPaths.from_dataset_format(
        dataset, data_format, pudl_test_paths
    )
    ferc_paths.nightly_datapackage_path.write_text("test datapackage")

    # Create zip file for sqlite file
    with (
        ZipFile(ferc_paths.nightly_sqlite_path, mode="w") as archive,
        archive.open(ferc_paths.local_sqlite_path.name, mode="w") as sqlite_file,
    ):
        sqlite_file.write(b"test sqlite")

    if data_format == "xbrl":
        ferc_paths.nightly_taxonomy_json_path.write_text("test taxonomy json")
        ferc_paths.nightly_duckdb_path.write_text("test duckdb")
        with ZipFile(ferc_paths.nightly_parquet_dir_path, mode="w") as archive:
            with archive.open(
                f"{dataset}_{data_format}/test1.parquet", mode="w"
            ) as pq_file_1:
                pq_file_1.write(b"test parquet 1")
            with archive.open(
                f"{dataset}_{data_format}/test2.parquet", mode="w"
            ) as pq_file_2:
                pq_file_2.write(b"test parquet 2")

    ferc_to_sqlite._download_nightly_outputs(data_format, ferc_paths)

    assert (
        ferc_paths.nightly_datapackage_path.read_bytes()
        == ferc_paths.local_datapackage_path.read_bytes()
    )
    assert ferc_paths.local_sqlite_path.read_text() == "test sqlite"

    if data_format == "xbrl":
        assert (
            ferc_paths.nightly_duckdb_path.read_bytes()
            == ferc_paths.local_duckdb_path.read_bytes()
        )
        assert (
            ferc_paths.nightly_taxonomy_json_path.read_bytes()
            == ferc_paths.local_taxonomy_json_path.read_bytes()
        )
        assert (
            ferc_paths.local_parquet_dir_path / "test1.parquet"
        ).read_text() == "test parquet 1"
        assert (
            ferc_paths.local_parquet_dir_path / "test2.parquet"
        ).read_text() == "test parquet 2"
