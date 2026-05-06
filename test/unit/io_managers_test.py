"""Test Dagster IO Managers."""

from pathlib import Path

import alembic.config
import pandas as pd
import pytest
import sqlalchemy as sa
from dagster import AssetKey, DagsterInstance, build_input_context, build_output_context
from dagster._core.execution.context.input import InputContext
from dagster._core.execution.context.output import OutputContext
from sqlalchemy.exc import IntegrityError, OperationalError

from pudl.etl.check_foreign_keys import (
    ForeignKeyError,
    ForeignKeyErrors,
    check_foreign_keys,
)
from pudl.ferc_sqlite_provenance import (
    FERC_TO_SQLITE_METADATA_KEY,
    FercSQLiteProvenanceRecord,
)
from pudl.io_managers import (
    FercDbfSQLiteConfigurableIOManager,
    FercDbfSQLiteIOManager,
    FercXbrlSQLiteConfigurableIOManager,
    FercXbrlSQLiteIOManager,
    PudlMixedFormatIOManager,
    PudlParquetIOManager,
    PudlSQLiteIOManager,
    SQLiteIOManager,
    ferc1_dbf_sqlite_io_manager,
    ferc1_xbrl_sqlite_io_manager,
)
from pudl.metadata import PUDL_PACKAGE
from pudl.metadata.classes import Package, Resource
from pudl.settings import (
    DatasetsSettings,
    EtlSettings,
    Ferc1Settings,
    FercToSqliteSettings,
)
from pudl.workspace.datastore import ZenodoDoiSettings


@pytest.fixture
def test_pkg() -> Package:
    """Create a test metadata package for the io manager tests."""
    fields = [
        {"name": "artistid", "type": "integer", "description": "artistid"},
        {
            "name": "artistname",
            "type": "string",
            "constraints": {"required": True},
            "description": "artistid",
        },
    ]
    schema = {"fields": fields, "primary_key": ["artistid"]}
    artist_resource = Resource(name="artist", schema=schema, description="Artist")

    fields = [
        {"name": "artistid", "type": "integer", "description": "artistid"},
        {
            "name": "artistname",
            "type": "string",
            "constraints": {"required": True},
            "description": "artistname",
        },
    ]
    schema = {"fields": fields, "primary_key": ["artistid"]}
    view_resource = Resource(
        name="artist_view",
        schema=schema,
        description="Artist view",
        create_database_schema=False,
    )

    fields = [
        {"name": "trackid", "type": "integer", "description": "trackid"},
        {
            "name": "trackname",
            "type": "string",
            "constraints": {"required": True},
            "description": "trackname",
        },
        {"name": "trackartist", "type": "integer", "description": "trackartist"},
    ]
    fkeys = [
        {
            "fields": ["trackartist"],
            "reference": {"resource": "artist", "fields": ["artistid"]},
        }
    ]
    schema = {"fields": fields, "primary_key": ["trackid"], "foreign_keys": fkeys}
    track_resource = Resource(name="track", schema=schema, description="Track")
    return Package(
        name="music", resources=[track_resource, artist_resource, view_resource]
    )


@pytest.fixture
def sqlite_io_manager_fixture(tmp_path, test_pkg) -> SQLiteIOManager:
    """Create a SQLiteIOManager fixture with a simple database schema."""
    md: sa.MetaData = test_pkg.to_sql()
    return SQLiteIOManager(base_dir=tmp_path, db_name="pudl", md=md)


def test_sqlite_io_manager_delete_stmt(sqlite_io_manager_fixture):
    """Test we are replacing the data without dropping the table schema."""
    manager: SQLiteIOManager = sqlite_io_manager_fixture

    asset_key = "artist"
    artist = pd.DataFrame({"artistid": [1], "artistname": ["Co-op Mop"]})
    output_context: OutputContext = build_output_context(asset_key=AssetKey(asset_key))
    manager.handle_output(output_context, artist)

    # Read the table back into pandas
    input_context: InputContext = build_input_context(asset_key=AssetKey(asset_key))
    returned_df = manager.load_input(input_context)
    assert len(returned_df) == 1

    # Rerun the asset
    # Load the dataframe to a sqlite table
    output_context: OutputContext = build_output_context(asset_key=AssetKey(asset_key))
    manager.handle_output(output_context, artist)

    # Read the table back into pandas
    input_context: InputContext = build_input_context(asset_key=AssetKey(asset_key))
    returned_df: pd.DataFrame = manager.load_input(input_context)
    assert len(returned_df) == 1


def test_foreign_key_failure(sqlite_io_manager_fixture):
    """Ensure ForeignKeyErrors are raised when there are foreign key errors."""
    manager = sqlite_io_manager_fixture

    asset_key = "artist"
    artist = pd.DataFrame({"artistid": [1], "artistname": ["Co-op Mop"]})
    output_context: OutputContext = build_output_context(asset_key=AssetKey(asset_key))
    manager.handle_output(output_context, artist)

    asset_key = "track"
    track = pd.DataFrame(
        {"trackid": [1], "trackname": ["FERC Ya!"], "trackartist": [2]}
    )
    output_context: OutputContext = build_output_context(asset_key=AssetKey(asset_key))
    manager.handle_output(output_context, track)

    with pytest.raises(ForeignKeyErrors) as excinfo:
        check_foreign_keys(manager.engine)

    assert excinfo.value[0] == ForeignKeyError(
        child_table="track",
        parent_table="artist",
        foreign_key="(artistid)",
        rowids=[1],
    )


def test_extra_column_error(sqlite_io_manager_fixture):
    """Ensure an error is thrown when there is an extra column in the dataframe."""
    manager: SQLiteIOManager = sqlite_io_manager_fixture

    asset_key = "artist"
    artist = pd.DataFrame(
        {"artistid": [1], "artistname": ["Co-op Mop"], "artistmanager": [1]}
    )
    output_context: OutputContext = build_output_context(asset_key=AssetKey(asset_key))
    with pytest.raises(OperationalError):
        manager.handle_output(output_context, artist)


def test_missing_column_error(sqlite_io_manager_fixture):
    """Ensure an error is thrown when a dataframe is missing a column in the schema."""
    manager: SQLiteIOManager = sqlite_io_manager_fixture

    asset_key = "artist"
    artist = pd.DataFrame(
        {
            "artistid": [1],
        }
    )
    output_context: OutputContext = build_output_context(asset_key=AssetKey(asset_key))
    with pytest.raises(ValueError):
        manager.handle_output(output_context, artist)


def test_nullable_column_error(sqlite_io_manager_fixture):
    """Ensure an error is thrown when a non nullable column is missing data."""
    manager: SQLiteIOManager = sqlite_io_manager_fixture

    asset_key = "artist"
    artist = pd.DataFrame({"artistid": [1, 2], "artistname": ["Co-op Mop", pd.NA]})
    output_context: OutputContext = build_output_context(asset_key=AssetKey(asset_key))

    with pytest.raises(IntegrityError):
        manager.handle_output(output_context, artist)


@pytest.mark.xfail(reason="SQLite autoincrement behvior is breaking this test.")
def test_null_primary_key_column_error(sqlite_io_manager_fixture):
    """Ensure an error is thrown when a primary key contains a nullable value."""
    manager: SQLiteIOManager = sqlite_io_manager_fixture

    asset_key = "artist"
    artist = pd.DataFrame(
        {"artistid": [1, pd.NA], "artistname": ["Co-op Mop", "Cxtxlyst"]}
    )
    output_context: OutputContext = build_output_context(asset_key=AssetKey(asset_key))
    with pytest.raises(IntegrityError):
        manager.handle_output(output_context, artist)


def test_primary_key_column_error(sqlite_io_manager_fixture):
    """Ensure an error is thrown when a primary key is violated."""
    manager: SQLiteIOManager = sqlite_io_manager_fixture

    asset_key = "artist"
    artist = pd.DataFrame({"artistid": [1, 1], "artistname": ["Co-op Mop", "Cxtxlyst"]})
    output_context: OutputContext = build_output_context(asset_key=AssetKey(asset_key))
    with pytest.raises(IntegrityError):
        manager.handle_output(output_context, artist)


def test_incorrect_type_error(sqlite_io_manager_fixture):
    """Ensure an error is thrown when dataframe type doesn't match the table schema."""
    manager: SQLiteIOManager = sqlite_io_manager_fixture

    asset_key = "artist"
    artist = pd.DataFrame({"artistid": ["abc"], "artistname": ["Co-op Mop"]})
    output_context: OutputContext = build_output_context(asset_key=AssetKey(asset_key))
    with pytest.raises(IntegrityError):
        manager.handle_output(output_context, artist)


def test_missing_schema_error(sqlite_io_manager_fixture):
    """Test a ValueError is raised when a table without a schema is loaded."""
    manager: SQLiteIOManager = sqlite_io_manager_fixture

    asset_key = "venues"
    venue = pd.DataFrame({"venueid": [1], "venuename": "Vans Dive Bar"})
    output_context: OutputContext = build_output_context(asset_key=AssetKey(asset_key))
    with pytest.raises(ValueError):
        manager.handle_output(output_context, venue)


@pytest.fixture
def fake_pudl_sqlite_io_manager_fixture(
    tmp_path, test_pkg, monkeypatch
) -> PudlSQLiteIOManager:
    """Create a SQLiteIOManager fixture with a fake database schema."""
    db_path: Path = tmp_path / "fake.sqlite"

    # Create the database and schemas
    engine: sa.Engine = sa.create_engine(f"sqlite:///{db_path}")
    md: sa.MetaData = test_pkg.to_sql()
    md.create_all(engine)
    return PudlSQLiteIOManager(base_dir=tmp_path, db_name="fake", package=test_pkg)


def test_pudl_sqlite_io_manager_delete_stmt(fake_pudl_sqlite_io_manager_fixture):
    """Test we are replacing the data without dropping the table schema."""
    manager: PudlSQLiteIOManager = fake_pudl_sqlite_io_manager_fixture

    asset_key = "artist"
    artist = pd.DataFrame({"artistid": [1], "artistname": ["Co-op Mop"]})
    output_context: OutputContext = build_output_context(asset_key=AssetKey(asset_key))
    manager.handle_output(output_context, artist)

    # Read the table back into pandas
    input_context: InputContext = build_input_context(asset_key=AssetKey(asset_key))
    returned_df: pd.DataFrame = manager.load_input(input_context)
    assert len(returned_df) == 1

    # Rerun the asset
    # Load the dataframe to a sqlite table
    output_context: OutputContext = build_output_context(asset_key=AssetKey(asset_key))
    manager.handle_output(output_context, artist)

    # Read the table back into pandas
    input_context: InputContext = build_input_context(asset_key=AssetKey(asset_key))
    returned_df: pd.DataFrame = manager.load_input(input_context)
    assert len(returned_df) == 1


@pytest.mark.slow
def test_migrations_match_metadata(tmp_path, monkeypatch):
    """If you create a `PudlSQLiteIOManager` that points at a non-existing
    `pudl.sqlite` - it will initialize the DB based on the `package`.

    If you create a `PudlSQLiteIOManager` that points at an existing
    `pudl.sqlite`, like one initialized via `alembic upgrade head`, it
    will compare the existing db schema with the db schema in `package`.

    We want to make sure that the schema defined in `package` is the same as
    the one we arrive at by applying all the migrations.
    """
    # alembic wants current directory to be the one with `alembic.ini` in it
    monkeypatch.chdir(Path(__file__).parent.parent.parent)
    # alembic knows to use PudlPaths().pudl_db - so we need to set PUDL_OUTPUT env var
    monkeypatch.setenv("PUDL_OUTPUT", str(tmp_path))
    # run all the migrations on a fresh DB at tmp_path/pudl.sqlite
    alembic.config.main(["upgrade", "head"])

    PudlSQLiteIOManager(base_dir=tmp_path, db_name="pudl", package=PUDL_PACKAGE)

    # all we care about is that it didn't raise an error
    assert True


def test_empty_read_fails(fake_pudl_sqlite_io_manager_fixture):
    """Reading empty table fails."""
    with pytest.raises(AssertionError):
        context: InputContext = build_input_context(asset_key=AssetKey("artist"))
        fake_pudl_sqlite_io_manager_fixture.load_input(context)


def test_mixed_format_io_manager_invalid_config():
    """The mixed-format manager should reject parquet-read without parquet-write."""
    with pytest.raises(RuntimeError):
        PudlMixedFormatIOManager(
            write_to_parquet=False,
            read_from_parquet=True,
        )


def test_mixed_format_io_manager_initializes_backends(mocker):
    """The migrated mixed-format IO manager should lazily expose both backends."""
    sqlite_manager: PudlSQLiteIOManager = mocker.MagicMock(spec=PudlSQLiteIOManager)
    parquet_manager: PudlParquetIOManager = mocker.MagicMock()
    mocker.patch("pudl.io_managers.PudlSQLiteIOManager", return_value=sqlite_manager)
    mocker.patch("pudl.io_managers.PudlParquetIOManager", return_value=parquet_manager)

    manager = PudlMixedFormatIOManager()

    assert manager._sqlite_io_manager is sqlite_manager
    assert manager._parquet_io_manager is parquet_manager


def test_ferc_dbf_io_manager_uses_injected_dataset_settings(mocker):
    """The migrated FERC DBF IO manager should read years from injected settings."""
    dataset_settings = DatasetsSettings(ferc1=Ferc1Settings(years=[2020, 2021]))
    etl_settings: EtlSettings = EtlSettings(
        datasets=dataset_settings,
        ferc_to_sqlite_settings=FercToSqliteSettings(),
    )
    zenodo_dois: ZenodoDoiSettings = ZenodoDoiSettings()
    fake_manager: FercDbfSQLiteIOManager = mocker.MagicMock()
    fake_manager._query.return_value = pd.DataFrame(
        {"sched_table_name": ["f1_respondent_id"]}
    )
    mocker.patch("pudl.io_managers.FercDbfSQLiteIOManager", return_value=fake_manager)

    manager: FercDbfSQLiteConfigurableIOManager = (
        ferc1_dbf_sqlite_io_manager.model_copy(
            update={"etl_settings": etl_settings, "zenodo_dois": zenodo_dois}
        )
    )
    instance: DagsterInstance = mocker.MagicMock()
    instance.is_ephemeral = False

    instance.get_latest_materialization_event.return_value = mocker.MagicMock(
        asset_materialization=mocker.MagicMock(
            metadata={
                FERC_TO_SQLITE_METADATA_KEY: FercSQLiteProvenanceRecord(
                    dataset="ferc1",
                    data_format="dbf",
                    status="complete",
                    years=etl_settings.ferc_to_sqlite.get_dataset_years("ferc1", "dbf"),
                    zenodo_doi=zenodo_dois.get_doi("ferc1"),
                    sqlite_path=Path("test-data/ferc1_dbf.sqlite"),
                ).model_dump(mode="json")
            }
        )
    )
    context: InputContext = build_input_context(
        asset_key=AssetKey("raw_ferc1_dbf__f1_respondent_id"),
        instance=instance,
    )

    observed: pd.DataFrame = manager.load_input(context)

    assert observed["sched_table_name"].eq("f1_respondent_id").all()
    fake_manager._query.assert_called_once_with(
        "f1_respondent_id",
        dataset_settings.ferc1.dbf_years,
    )


def test_ferc_xbrl_io_manager_uses_injected_dataset_settings(mocker):
    """The migrated FERC XBRL IO manager should pass years from injected settings."""
    dataset_settings = DatasetsSettings(ferc1=Ferc1Settings(years=[2021]))
    etl_settings = EtlSettings(
        datasets=dataset_settings,
        ferc_to_sqlite_settings=FercToSqliteSettings(),
    )
    zenodo_dois = ZenodoDoiSettings()
    fake_manager = mocker.MagicMock()
    fake_manager._query.return_value = pd.DataFrame(
        {"report_year": [2021], "sched_table_name": ["plant_in_service"]}
    )
    mocker.patch("pudl.io_managers.FercXbrlSQLiteIOManager", return_value=fake_manager)

    manager: FercXbrlSQLiteConfigurableIOManager = (
        ferc1_xbrl_sqlite_io_manager.model_copy(
            update={"etl_settings": etl_settings, "zenodo_dois": zenodo_dois}
        )
    )
    instance: DagsterInstance = mocker.MagicMock()
    instance.is_ephemeral = False
    instance.get_latest_materialization_event.return_value = mocker.MagicMock(
        asset_materialization=mocker.MagicMock(
            metadata={
                FERC_TO_SQLITE_METADATA_KEY: FercSQLiteProvenanceRecord(
                    dataset="ferc1",
                    data_format="dbf",
                    status="complete",
                    years=etl_settings.ferc_to_sqlite.get_dataset_years(
                        "ferc1", "xbrl"
                    ),
                    zenodo_doi=zenodo_dois.get_doi("ferc1"),
                    sqlite_path=Path("test-data/ferc1_dbf.sqlite"),
                ).model_dump(mode="json")
            }
        )
    )
    context: InputContext = build_input_context(
        asset_key=AssetKey("raw_ferc1_xbrl__plant_in_service_duration"),
        instance=instance,
    )

    observed: pd.DataFrame = manager.load_input(context)

    assert observed["report_year"].eq(2021).all()
    assert observed["sched_table_name"].eq("plant_in_service").all()
    fake_manager._query.assert_called_once_with(
        "plant_in_service_duration",
        dataset_settings.ferc1.xbrl_years,
    )


def test_ferc_dbf_io_manager_rejects_stale_provenance(mocker):
    """The migrated FERC DBF IO manager should fail fast on stale prerequisites."""
    dataset_settings = DatasetsSettings(ferc1=Ferc1Settings(years=[2020, 2021]))
    etl_settings = EtlSettings(
        datasets=dataset_settings,
        ferc_to_sqlite_settings=FercToSqliteSettings(),
    )
    zenodo_dois = ZenodoDoiSettings()

    fake_engine = mocker.MagicMock()
    fake_engine.begin.return_value.__enter__.return_value = mocker.MagicMock()
    fake_manager = mocker.MagicMock()
    fake_manager.engine: sa.Engine = fake_engine
    mocker.patch("pudl.io_managers.FercDbfSQLiteIOManager", return_value=fake_manager)
    read_sql_query = mocker.patch("pudl.io_managers.pd.read_sql_query")

    manager: FercDbfSQLiteConfigurableIOManager = (
        ferc1_dbf_sqlite_io_manager.model_copy(
            update={"etl_settings": etl_settings, "zenodo_dois": zenodo_dois}
        )
    )
    stale_metadata = {
        FERC_TO_SQLITE_METADATA_KEY: FercSQLiteProvenanceRecord(
            dataset="ferc1",
            data_format="dbf",
            status="complete",
            years=etl_settings.ferc_to_sqlite.get_dataset_years("ferc1", "dbf"),
            zenodo_doi="stale DOI",
            sqlite_path=Path("test-data/ferc1_dbf.sqlite"),
        ).model_dump(mode="json")
    }
    instance: DagsterInstance = mocker.MagicMock()
    instance.is_ephemeral = False
    instance.get_latest_materialization_event.return_value = mocker.MagicMock(
        asset_materialization=mocker.MagicMock(metadata=stale_metadata)
    )
    context: InputContext = build_input_context(
        asset_key=AssetKey("raw_ferc1_dbf__f1_respondent_id"),
        instance=instance,
    )

    with pytest.raises(RuntimeError, match="Zenodo DOI mismatch"):
        manager.load_input(context)

    read_sql_query.assert_not_called()


def test_ferc_dbf_io_manager_requires_provenance_metadata(mocker):
    """The migrated FERC DBF IO manager should fail fast when no provenance exists."""
    dataset_settings = DatasetsSettings.model_validate(
        {"ferc1": {"years": [2020, 2021]}}
    )
    etl_settings = EtlSettings(
        datasets=dataset_settings,
        ferc_to_sqlite_settings=FercToSqliteSettings(),
    )
    zenodo_dois = ZenodoDoiSettings()

    fake_engine: sa.Engine = mocker.MagicMock()
    fake_engine.begin.return_value.__enter__.return_value = mocker.MagicMock()
    fake_manager = mocker.MagicMock()
    fake_manager.engine = fake_engine
    mocker.patch("pudl.io_managers.FercDbfSQLiteIOManager", return_value=fake_manager)
    read_sql_query = mocker.patch("pudl.io_managers.pd.read_sql_query")

    manager: FercDbfSQLiteConfigurableIOManager = (
        ferc1_dbf_sqlite_io_manager.model_copy(
            update={"etl_settings": etl_settings, "zenodo_dois": zenodo_dois}
        )
    )
    instance: DagsterInstance = mocker.MagicMock()
    instance.is_ephemeral = False
    instance.get_latest_materialization_event.return_value = None
    context: InputContext = build_input_context(
        asset_key=AssetKey("raw_ferc1_dbf__f1_respondent_id"),
        instance=instance,
    )

    with pytest.raises(RuntimeError, match="No Dagster provenance metadata"):
        manager.load_input(context)

    read_sql_query.assert_not_called()


def test_replace_on_insert(fake_pudl_sqlite_io_manager_fixture):
    """Tests that two runs of the same asset overwrite existing contents."""
    artist_df = pd.DataFrame({"artistid": [1], "artistname": ["Co-op Mop"]})
    output_context: OutputContext = build_output_context(asset_key=AssetKey("artist"))
    input_context: InputContext = build_input_context(asset_key=AssetKey("artist"))

    # Write then read.
    fake_pudl_sqlite_io_manager_fixture.handle_output(output_context, artist_df)
    read_df: pd.DataFrame = fake_pudl_sqlite_io_manager_fixture.load_input(
        input_context
    )
    pd.testing.assert_frame_equal(artist_df, read_df, check_dtype=False)
    # check_dtype=False, because int64 != Int64. /o\

    # Rerunning the asset overrwrites contents, leaves only
    # one artist in the database.
    new_artist_df = pd.DataFrame({"artistid": [2], "artistname": ["Cxtxlyst"]})
    fake_pudl_sqlite_io_manager_fixture.handle_output(output_context, new_artist_df)
    read_df: pd.DataFrame = fake_pudl_sqlite_io_manager_fixture.load_input(
        input_context
    )
    pd.testing.assert_frame_equal(new_artist_df, read_df, check_dtype=False)


def test_report_year_fixing_instant():
    instant_df: pd.DataFrame = pd.DataFrame.from_records(
        [
            {
                "entity_id": "123",
                "date": "2020-07-01",
                "report_year": 3021,
                "factoid": "replace report year with date year",
            },
        ]
    )

    observed: pd.Series = FercXbrlSQLiteIOManager.refine_report_year(
        instant_df, xbrl_years=[2021, 2022]
    ).report_year
    expected = pd.Series([2020])
    assert (observed == expected).all()


def test_report_year_fixing_duration():
    duration_df: pd.DataFrame = pd.DataFrame.from_records(
        [
            {
                "entity_id": "123",
                "start_date": "2004-01-01",
                "end_date": "2004-12-31",
                "report_year": 3021,
                "factoid": "filter out since the report year is out of bounds",
            },
            {
                "entity_id": "123",
                "start_date": "2021-01-01",
                "end_date": "2021-12-31",
                "report_year": 3021,
                "factoid": "replace report year with date year",
            },
        ]
    )

    observed: pd.Series = FercXbrlSQLiteIOManager.refine_report_year(
        duration_df, xbrl_years=[2021, 2022]
    ).report_year
    expected: pd.Series = pd.Series([2021])
    assert (observed == expected).all()


@pytest.mark.parametrize(
    "df, match",
    [
        (
            pd.DataFrame.from_records(
                [
                    {"entity_id": "123", "report_year": 3021, "date": ""},
                ]
            ),
            "date has null values",
        ),
        (
            pd.DataFrame.from_records(
                [
                    {
                        "entity_id": "123",
                        "report_year": 3021,
                        "start_date": "",
                        "end_date": "2020-12-31",
                    },
                ]
            ),
            "start_date has null values",
        ),
        (
            pd.DataFrame.from_records(
                [
                    {
                        "entity_id": "123",
                        "report_year": 3021,
                        "start_date": "2020-06-01",
                        "end_date": "2021-05-31",
                    },
                ]
            ),
            "start_date and end_date are in different years",
        ),
        (
            pd.DataFrame.from_records(
                [
                    {
                        "entity_id": "123",
                        "report_year": 3021,
                    },
                ]
            ),
            "Attempted to read a non-instant, non-duration table",
        ),
    ],
)
def test_report_year_fixing_bad_values(df, match):
    with pytest.raises(ValueError, match=match):
        FercXbrlSQLiteIOManager.refine_report_year(df, xbrl_years=[2021, 2022])
