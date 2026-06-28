"""Test Dagster IO Managers."""

import json
import logging
from importlib.metadata import version
from pathlib import Path

import alembic.config
import duckdb
import geopandas as gpd  # noqa: ICN002
import pandas as pd
import polars as pl
import pytest
import sqlalchemy as sa
from dagster import AssetKey, DagsterInstance, build_input_context, build_output_context
from dagster._core.execution.context.input import InputContext
from dagster._core.execution.context.output import OutputContext
from shapely.geometry import Point
from sqlalchemy.exc import IntegrityError, OperationalError

from pudl.dagster.io_managers import (
    FercDbfSqliteIOManager,
    FercXbrlSqliteIOManager,
    PudlMixedFormatIOManager,
    PudlParquetIOManager,
    PudlSqliteIOManager,
    SqliteIOManager,
)
from pudl.dagster.provenance import (
    FERC_TO_SQLITE_METADATA_KEY,
    FercSqliteProvenanceRecord,
)
from pudl.metadata.classes import PUDL_PACKAGE, Package, Resource
from pudl.settings import (
    Ferc1DataConfig,
    FercToSqliteDataConfig,
    GlobalDataConfig,
    PudlDataConfig,
)
from pudl.validate.integrity import (
    ForeignKeyError,
    ForeignKeyErrors,
    check_foreign_keys,
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


@pytest.fixture(autouse=True)
def suppress_sqlalchemy_pool_noise():
    """Suppress SQLAlchemy NullPool teardown errors from Dagster's ephemeral instance.

    build_output_context() creates a Dagster ephemeral instance backed by its own
    internal SQLite database. When an OutputContext is GC'd after a failed
    handle_output(), Dagster's teardown code accesses pathlib internals (_str, _drv)
    that were removed in Python 3.13, causing a cascade: GeneratorExit propagates
    through build_resources, the finally block tries to roll back an already-closed
    SQLite connection, and sqlalchemy.pool logs the ProgrammingError at ERROR level.
    The tests are correct; this is a Dagster + Python 3.13 compatibility issue.
    Setting the level here (not in a with-block) ensures it's active during GC
    teardown, which occurs after the test function returns.
    """
    sa_pool_logger = logging.getLogger("sqlalchemy.pool")
    original_level = sa_pool_logger.level
    sa_pool_logger.setLevel(logging.CRITICAL)
    yield
    sa_pool_logger.setLevel(original_level)


@pytest.fixture
def sqlite_io_manager_fixture(tmp_path, test_pkg) -> SqliteIOManager:
    """Create a SqliteIOManager fixture with a simple database schema."""
    md: sa.MetaData = test_pkg.to_sql()
    return SqliteIOManager(base_dir=tmp_path, db_name="pudl", md=md)


def test_sqlite_io_manager_delete_stmt(sqlite_io_manager_fixture):
    """Test we are replacing the data without dropping the table schema."""
    manager: SqliteIOManager = sqlite_io_manager_fixture

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
    manager: SqliteIOManager = sqlite_io_manager_fixture

    asset_key = "artist"
    artist = pd.DataFrame(
        {"artistid": [1], "artistname": ["Co-op Mop"], "artistmanager": [1]}
    )
    output_context: OutputContext = build_output_context(asset_key=AssetKey(asset_key))
    with pytest.raises(OperationalError):
        manager.handle_output(output_context, artist)


def test_missing_column_error(sqlite_io_manager_fixture):
    """Ensure an error is thrown when a dataframe is missing a column in the schema."""
    manager: SqliteIOManager = sqlite_io_manager_fixture

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
    manager: SqliteIOManager = sqlite_io_manager_fixture

    asset_key = "artist"
    artist = pd.DataFrame({"artistid": [1, 2], "artistname": ["Co-op Mop", pd.NA]})
    output_context: OutputContext = build_output_context(asset_key=AssetKey(asset_key))

    with pytest.raises(IntegrityError):
        manager.handle_output(output_context, artist)


def test_null_primary_key_column_error(sqlite_io_manager_fixture):
    """Ensure IntegrityError is raised when a primary key column contains NULL.

    SQLite's ``INTEGER PRIMARY KEY`` is a ROWID alias: inserting NULL into such a
    column silently assigns an auto-incremented value rather than raising a constraint
    error. This is a documented SQLite deviation from the SQL standard and affects all
    write paths (pandas ``to_sql``, SQLAlchemy core, raw SQL). The IO manager therefore
    enforces the NOT NULL constraint on primary key columns explicitly before writing,
    raising ``IntegrityError`` to match the error that a spec-compliant database would
    raise.
    """
    manager: SqliteIOManager = sqlite_io_manager_fixture

    asset_key = "artist"
    artist = pd.DataFrame(
        {"artistid": [1, pd.NA], "artistname": ["Co-op Mop", "Cxtxlyst"]}
    )
    output_context: OutputContext = build_output_context(asset_key=AssetKey(asset_key))
    with pytest.raises(IntegrityError):
        manager.handle_output(output_context, artist)


def test_primary_key_column_error(sqlite_io_manager_fixture):
    """Ensure an error is thrown when a primary key is violated."""
    manager: SqliteIOManager = sqlite_io_manager_fixture

    asset_key = "artist"
    artist = pd.DataFrame({"artistid": [1, 1], "artistname": ["Co-op Mop", "Cxtxlyst"]})
    output_context: OutputContext = build_output_context(asset_key=AssetKey(asset_key))
    with pytest.raises(IntegrityError):
        manager.handle_output(output_context, artist)


def test_incorrect_type_error(sqlite_io_manager_fixture):
    """Ensure an error is thrown when dataframe type doesn't match the table schema."""
    manager: SqliteIOManager = sqlite_io_manager_fixture

    asset_key = "artist"
    artist = pd.DataFrame({"artistid": ["abc"], "artistname": ["Co-op Mop"]})
    output_context: OutputContext = build_output_context(asset_key=AssetKey(asset_key))
    with pytest.raises(IntegrityError):
        manager.handle_output(output_context, artist)


def test_missing_schema_error(sqlite_io_manager_fixture):
    """Test a ValueError is raised when a table without a schema is loaded."""
    manager: SqliteIOManager = sqlite_io_manager_fixture

    asset_key = "venues"
    venue = pd.DataFrame({"venueid": [1], "venuename": "Vans Dive Bar"})
    output_context: OutputContext = build_output_context(asset_key=AssetKey(asset_key))
    with pytest.raises(ValueError):
        manager.handle_output(output_context, venue)


@pytest.fixture
def fake_pudl_sqlite_io_manager_fixture(
    tmp_path, test_pkg, monkeypatch
) -> PudlSqliteIOManager:
    """Create a SqliteIOManager fixture with a fake database schema."""
    db_path: Path = tmp_path / "fake.sqlite"

    # Create the database and schemas
    engine: sa.Engine = sa.create_engine(f"sqlite:///{db_path}")
    md: sa.MetaData = test_pkg.to_sql()
    md.create_all(engine)
    return PudlSqliteIOManager(base_dir=tmp_path, db_name="fake", package=test_pkg)


def test_pudl_sqlite_io_manager_delete_stmt(fake_pudl_sqlite_io_manager_fixture):
    """Test we are replacing the data without dropping the table schema."""
    manager: PudlSqliteIOManager = fake_pudl_sqlite_io_manager_fixture

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
    """If you create a `PudlSqliteIOManager` that points at a non-existing
    `pudl.sqlite` - it will initialize the DB based on the `package`.

    If you create a `PudlSqliteIOManager` that points at an existing
    `pudl.sqlite`, like one initialized via `alembic upgrade head`, it
    will compare the existing db schema with the db schema in `package`.

    We want to make sure that the schema defined in `package` is the same as
    the one we arrive at by applying all the migrations.
    """
    # alembic wants current directory to be the one with `alembic.ini` in it
    monkeypatch.chdir(Path(__file__).parent.parent.parent.parent)
    # alembic knows to use PudlPaths().pudl_db - so we need to set PUDL_OUTPUT env var
    monkeypatch.setenv("PUDL_OUTPUT", str(tmp_path))
    # run all the migrations on a fresh DB at tmp_path/pudl.sqlite
    alembic.config.main(["upgrade", "head"])

    PudlSqliteIOManager(base_dir=tmp_path, db_name="pudl", package=PUDL_PACKAGE)

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
    sqlite_manager: PudlSqliteIOManager = mocker.MagicMock(spec=PudlSqliteIOManager)
    parquet_manager: PudlParquetIOManager = mocker.MagicMock()
    mocker.patch(
        "pudl.dagster.io_managers.PudlSqliteIOManager", return_value=sqlite_manager
    )
    mocker.patch(
        "pudl.dagster.io_managers.PudlParquetIOManager", return_value=parquet_manager
    )

    manager = PudlMixedFormatIOManager(pudl_paths=mocker.MagicMock())

    assert manager._sqlite_io_manager is sqlite_manager
    assert manager._parquet_io_manager is parquet_manager


def test_ferc_dbf_io_manager_uses_injected_pudl_data_config(mocker):
    """The migrated FERC DBF IO manager should read years from injected data config."""
    pudl_data_config = PudlDataConfig(ferc1=Ferc1DataConfig(years=[2020, 2021]))
    global_data_config: GlobalDataConfig = GlobalDataConfig(
        pudl=pudl_data_config,
        ferc_to_sqlite=FercToSqliteDataConfig(),
    )
    zenodo_dois = ZenodoDoiSettings()
    manager: FercDbfSqliteIOManager = FercDbfSqliteIOManager(
        global_data_config=global_data_config,
        zenodo_dois=zenodo_dois,
        dataset="ferc1",
    )
    mocker.patch.object(
        FercDbfSqliteIOManager, "metadata", new_callable=mocker.PropertyMock
    )
    query = mocker.patch.object(
        FercDbfSqliteIOManager,
        "_query",
        return_value=pd.DataFrame({"sched_table_name": ["f1_respondent_id"]}),
    )
    instance: DagsterInstance = mocker.MagicMock()
    instance.is_ephemeral = False

    instance.get_latest_materialization_event.return_value = mocker.MagicMock(
        asset_materialization=mocker.MagicMock(
            metadata={
                FERC_TO_SQLITE_METADATA_KEY: FercSqliteProvenanceRecord(
                    dataset="ferc1",
                    data_format="dbf",
                    status="complete",
                    years=global_data_config.ferc_to_sqlite.get_dataset_years(
                        "ferc1", "dbf"
                    ),
                    zenodo_doi=zenodo_dois.get_doi("ferc1"),
                    sqlite_path=Path("test-data/ferc1_dbf.sqlite"),
                    ferc_xbrl_extractor_version=version(
                        "catalystcoop.ferc_xbrl_extractor"
                    ),
                    source="local_new",
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
    query.assert_called_once_with(
        "f1_respondent_id",
        global_data_config.pudl.ferc1.dbf_years,
    )


def test_ferc_xbrl_io_manager_uses_injected_pudl_data_config(mocker):
    """The migrated FERC XBRL IO manager should pass years from injected data config."""
    pudl_data_config = PudlDataConfig(ferc1=Ferc1DataConfig(years=[2021]))
    global_data_config: GlobalDataConfig = GlobalDataConfig(
        pudl=pudl_data_config,
        ferc_to_sqlite=FercToSqliteDataConfig(),
    )
    zenodo_dois = ZenodoDoiSettings()
    manager: FercXbrlSqliteIOManager = FercXbrlSqliteIOManager(
        global_data_config=global_data_config,
        zenodo_dois=zenodo_dois,
        dataset="ferc1",
    )
    mocker.patch.object(
        FercXbrlSqliteIOManager, "metadata", new_callable=mocker.PropertyMock
    )
    query = mocker.patch.object(
        FercXbrlSqliteIOManager,
        "_query",
        return_value=pd.DataFrame(
            {"report_year": [2021], "sched_table_name": ["plant_in_service"]}
        ),
    )
    instance: DagsterInstance = mocker.MagicMock()
    instance.is_ephemeral = False
    instance.get_latest_materialization_event.return_value = mocker.MagicMock(
        asset_materialization=mocker.MagicMock(
            metadata={
                FERC_TO_SQLITE_METADATA_KEY: FercSqliteProvenanceRecord(
                    dataset="ferc1",
                    data_format="dbf",
                    status="complete",
                    years=global_data_config.ferc_to_sqlite.get_dataset_years(
                        "ferc1", "xbrl"
                    ),
                    zenodo_doi=zenodo_dois.get_doi("ferc1"),
                    sqlite_path=Path("test-data/ferc1_dbf.sqlite"),
                    ferc_xbrl_extractor_version=version(
                        "catalystcoop.ferc_xbrl_extractor"
                    ),
                    source="local_new",
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
    query.assert_called_once_with(
        "plant_in_service_duration",
        global_data_config.pudl.ferc1.xbrl_years,
    )


def test_ferc_dbf_io_manager_rejects_stale_provenance(mocker, caplog):
    """The migrated FERC DBF IO manager should fail fast on stale prerequisites."""
    pudl_data_config = PudlDataConfig(ferc1=Ferc1DataConfig(years=[2020, 2021]))
    global_data_config: GlobalDataConfig = GlobalDataConfig(
        pudl=pudl_data_config,
        ferc_to_sqlite=FercToSqliteDataConfig(),
    )
    zenodo_dois = ZenodoDoiSettings()

    manager: FercDbfSqliteIOManager = FercDbfSqliteIOManager(
        global_data_config=global_data_config,
        zenodo_dois=zenodo_dois,
        dataset="ferc1",
    )
    mocker.patch.object(
        FercDbfSqliteIOManager, "metadata", new_callable=mocker.PropertyMock
    )
    query = mocker.patch.object(FercDbfSqliteIOManager, "_query")
    stale_metadata = {
        FERC_TO_SQLITE_METADATA_KEY: FercSqliteProvenanceRecord(
            dataset="ferc1",
            data_format="dbf",
            status="complete",
            years=global_data_config.ferc_to_sqlite.get_dataset_years("ferc1", "dbf"),
            zenodo_doi="stale DOI",
            sqlite_path=Path("test-data/ferc1_dbf.sqlite"),
            source="local_new",
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

    with (
        pytest.raises(RuntimeError, match="provenace metadata is not compatible"),
        caplog.at_level(logging.WARNING),
    ):
        manager.load_input(context)
        assert "Zenodo DOI mismatch" in caplog.text

    query.assert_not_called()


def test_ferc_dbf_io_manager_requires_provenance_metadata(mocker):
    """The migrated FERC DBF IO manager should fail fast when no provenance exists."""
    pudl_data_config = PudlDataConfig.model_validate({"ferc1": {"years": [2020, 2021]}})
    global_data_config: GlobalDataConfig = GlobalDataConfig(
        pudl=pudl_data_config,
        ferc_to_sqlite=FercToSqliteDataConfig(),
    )
    zenodo_dois = ZenodoDoiSettings()

    manager: FercDbfSqliteIOManager = FercDbfSqliteIOManager(
        global_data_config=global_data_config,
        zenodo_dois=zenodo_dois,
        dataset="ferc1",
    )
    mocker.patch.object(
        FercDbfSqliteIOManager, "metadata", new_callable=mocker.PropertyMock
    )
    query = mocker.patch.object(FercDbfSqliteIOManager, "_query")
    instance: DagsterInstance = mocker.MagicMock()
    instance.is_ephemeral = False
    instance.get_latest_materialization_event.return_value = None
    context: InputContext = build_input_context(
        asset_key=AssetKey("raw_ferc1_dbf__f1_respondent_id"),
        instance=instance,
    )

    with pytest.raises(RuntimeError, match="No Dagster provenance metadata"):
        manager.load_input(context)

    query.assert_not_called()


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
    observed: pd.Series = FercXbrlSqliteIOManager.refine_report_year(
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
    observed: pd.Series = FercXbrlSqliteIOManager.refine_report_year(
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
        FercXbrlSqliteIOManager.refine_report_year(df, xbrl_years=[2021, 2022])


# ---------------------------------------------------------------------------
# GeoDataFrame / GeoParquet tests for PudlParquetIOManager
# ---------------------------------------------------------------------------


@pytest.fixture
def minimal_geo_resource() -> "Resource":
    """Minimal PUDL Resource with a geometry column for IO manager tests."""
    return Resource(
        name="test_geo",
        schema={
            "fields": [
                {"name": "geo_id", "type": "integer", "description": "geo id"},
                {"name": "geometry", "type": "geometry", "description": "shape"},
            ],
            "primary_key": ["geo_id"],
        },
        description="Test geo resource",
    )


@pytest.fixture
def geo_parquet_output_path(
    tmp_path: Path, minimal_geo_resource: "Resource", mocker
) -> Path:
    """Write a small GeoDataFrame through PudlParquetIOManager; return the file path."""
    mocker.patch(
        "pudl.dagster.io_managers.Resource.from_id",
        return_value=minimal_geo_resource,
    )
    mock_paths = mocker.MagicMock()
    out_path = tmp_path / "test_geo.parquet"
    mock_paths.parquet_path.return_value = out_path

    manager = PudlParquetIOManager(pudl_paths=mock_paths)
    gdf = gpd.GeoDataFrame(
        {
            "geo_id": pd.array([1, 2], dtype="Int64"),
            "geometry": gpd.GeoSeries(
                [Point(0.0, 0.0), Point(1.0, 1.0)], crs="EPSG:4326"
            ),
        }
    )
    context: OutputContext = build_output_context(asset_key=AssetKey("test_geo"))
    manager.handle_output(context, gdf)
    return out_path


def test_parquet_io_manager_writes_geodataframe(geo_parquet_output_path: Path) -> None:
    """PudlParquetIOManager should write a GeoDataFrame to a Parquet file."""
    assert geo_parquet_output_path.exists()
    assert geo_parquet_output_path.stat().st_size > 0


def test_geoparquet_output_has_valid_geo_metadata(
    geo_parquet_output_path: Path,
) -> None:
    """Written file must carry spec-compliant GeoParquet 1.0.0 metadata with PROJJSON CRS."""
    import pyarrow.parquet as pq

    raw_meta = pq.read_metadata(geo_parquet_output_path).metadata
    assert b"geo" in raw_meta, "GeoParquet 'geo' metadata key is missing"

    geo = json.loads(raw_meta[b"geo"].decode())
    assert geo.get("version") == "1.0.0"
    assert geo["primary_column"] == "geometry"
    col_meta = geo["columns"]["geometry"]
    assert col_meta["encoding"] == "WKB"

    # CRS must be a PROJJSON dict, not a WKT string — required by DuckDB >= 1.5.
    crs = col_meta["crs"]
    assert isinstance(crs, dict), (
        "CRS must be PROJJSON dict, not WKT string (DuckDB 1.5 compatibility)"
    )
    # $schema must point to the PROJJSON schema — presence distinguishes PROJJSON
    # from an arbitrary dict (e.g. a raw authority dict).
    assert crs.get("$schema", "").startswith("https://proj.org/schemas/"), (
        f"CRS '$schema' is not a PROJJSON schema URL: {crs.get('$schema')!r}"
    )
    # GeographicCRS is the correct PROJJSON type for EPSG:4326 (WGS 84).
    assert crs.get("type") == "GeographicCRS", (
        f"Expected PROJJSON type 'GeographicCRS', got {crs.get('type')!r}"
    )
    # Top-level 'id' must identify EPSG:4326 so consumers can resolve the CRS.
    crs_id = crs.get("id", {})
    assert crs_id.get("authority") == "EPSG", (
        f"Expected CRS authority 'EPSG', got {crs_id.get('authority')!r}"
    )
    assert crs_id.get("code") == 4326, (
        f"Expected CRS code 4326, got {crs_id.get('code')!r}"
    )
    # Coordinate system must be ellipsoidal (lat/lon axes), not projected (x/y).
    cs = crs.get("coordinate_system", {})
    assert cs.get("subtype") == "ellipsoidal", (
        f"Expected ellipsoidal coordinate system, got {cs.get('subtype')!r}"
    )


def test_geoparquet_roundtrip_via_geopandas(geo_parquet_output_path: Path) -> None:
    """gpd.read_parquet() must restore the original CRS and geometry column."""
    gdf = gpd.read_parquet(geo_parquet_output_path)
    assert gdf.crs is not None
    assert gdf.crs.to_epsg() == 4326
    assert gdf.geometry.name == "geometry"
    assert len(gdf) == 2


def test_geoparquet_readable_by_pandas(geo_parquet_output_path: Path) -> None:
    """pd.read_parquet() must not raise even though it cannot reconstruct geometries."""
    df = pd.read_parquet(geo_parquet_output_path)
    assert "geometry" in df.columns
    assert len(df) == 2


def test_geoparquet_readable_by_polars(geo_parquet_output_path: Path) -> None:
    """pl.read_parquet() must not raise; geometry column appears as Binary."""
    df = pl.read_parquet(geo_parquet_output_path)
    assert "geometry" in df.columns
    assert len(df) == 2


def test_geoparquet_duckdb_recognizes_geometry_column(
    geo_parquet_output_path: Path,
) -> None:
    """DuckDB >= 1.5 with the spatial extension must expose geometry as GEOMETRY type."""
    con = duckdb.connect()
    con.load_extension("spatial")
    schema = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{geo_parquet_output_path}')"  # noqa: S608
    ).df()
    geo_row = schema[schema["column_name"] == "geometry"]
    assert not geo_row.empty
    col_type: str = geo_row["column_type"].iloc[0]
    assert col_type.startswith("GEOMETRY"), (
        f"Expected GEOMETRY type but got {col_type!r}. "
        "This likely means the geo metadata CRS format is unreadable by DuckDB."
    )


def test_parquet_io_manager_rejects_unsupported_output_type(
    tmp_path: Path, minimal_geo_resource: "Resource", mocker
) -> None:
    """handle_output must raise TypeError for objects that are not DataFrame/GDF/LazyFrame."""
    mocker.patch(
        "pudl.dagster.io_managers.Resource.from_id",
        return_value=minimal_geo_resource,
    )
    mock_paths = mocker.MagicMock()
    mock_paths.parquet_path.return_value = tmp_path / "test_geo.parquet"
    manager = PudlParquetIOManager(pudl_paths=mock_paths)
    context: OutputContext = build_output_context(asset_key=AssetKey("test_geo"))
    with pytest.raises(TypeError, match="PudlParquetIOManager only supports"):
        manager.handle_output(context, {"not": "a dataframe"})
