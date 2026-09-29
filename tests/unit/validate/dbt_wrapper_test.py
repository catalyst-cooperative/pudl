import dagster as dg
import pytest

from pudl.validate.dbt import (
    DbtPass,
    dagster_to_dbt_selection,
    duckdb_settings,
    split_by_table_size,
)


@pytest.fixture(scope="session")
def dummy_dagster():
    """Minimal dagster defs with some dependencies for us to test against."""

    @dg.asset
    def raw():
        return "raw"

    @dg.asset
    def core(raw):
        return "core"

    @dg.asset
    def out(core):
        return "out"

    defs = dg.Definitions(assets=[raw, core, out])
    return defs


@pytest.fixture
def dummy_dbt_manifest(tmp_path):
    """Minimal manifest required for getting the source selector.

    Only core and out are defined as sources - we're assuming that raw is not
    persisted & thus shouldn't be tracked in dbt.
    """
    manifest = {
        "metadata": {
            "project_name": "test_project",
        },
        "sources": {
            "source.test_project.test.core": {
                "name": "core",
                "source_name": "test",
                "package_name": "test_project",
                "schema": "main",
                "unique_id": "source.test_project.test.core",
            },
            "source.test_project.test.out": {
                "name": "out",
                "source_name": "test",
                "package_name": "test_project",
                "schema": "main",
                "unique_id": "source.test_project.test.out",
            },
        },
        "nodes": {},
        "parent_map": {},
        "child_map": {},
    }
    return manifest


@pytest.mark.parametrize(
    "dagster_selection,dbt_selection",
    [
        ("key:core", "source:test.core"),
        ("+key:core", "source:test.core"),
        ("+key:core+", "source:test.core source:test.out"),
        ("key:*cor*", "source:test.core"),
    ],
)
def test_dagster_to_dbt_selection(
    dagster_selection, dbt_selection, dummy_dagster, dummy_dbt_manifest
):
    observed = dagster_to_dbt_selection(
        dagster_selection, defs=dummy_dagster, manifest=dummy_dbt_manifest
    )
    expected = dbt_selection
    assert sorted(observed.split(" ")) == sorted(expected.split(" "))


LARGE = "config.meta.large:true"


def test_split_by_table_size_gives_large_tables_their_own_pass():
    """Everything but the large tables is built with the target we were given."""
    passes = split_by_table_size("*", None, "etl-full")

    assert passes == [
        DbtPass("etl-full", "*", LARGE),
        DbtPass("etl-full-large", f"*,{LARGE}", None),
    ]


def test_split_by_table_size_intersects_each_selector_in_a_union():
    """dbt can't intersect a whole union, so each selector gets the large filter."""
    passes = split_by_table_size(
        "source:pudl.a source:ferceqr.b,test_name:c", "test_name:d", "etl-full"
    )

    assert passes == [
        DbtPass(
            "etl-full",
            "source:pudl.a source:ferceqr.b,test_name:c",
            f"test_name:d {LARGE}",
        ),
        DbtPass(
            "etl-full-large",
            f"source:pudl.a,{LARGE} source:ferceqr.b,test_name:c,{LARGE}",
            "test_name:d",
        ),
    ]


def test_split_by_table_size_leaves_other_targets_alone():
    """A target without a large-table counterpart runs everything in one pass."""
    assert split_by_table_size("*", "test_name:d", "etl-fast") == [
        DbtPass("etl-fast", "*", "test_name:d")
    ]


@pytest.mark.parametrize(
    "target,environ,expected",
    [
        ("etl-full", {}, {}),
        (
            "etl-full",
            {"PUDL_DBT_MEMORY_LIMIT": "8GB", "PUDL_DBT_LARGE_MEMORY_LIMIT": "64GB"},
            {"memory_limit": "8GB"},
        ),
        (
            "etl-full-large",
            {"PUDL_DBT_MEMORY_LIMIT": "8GB", "PUDL_DBT_LARGE_MEMORY_LIMIT": "64GB"},
            {"memory_limit": "64GB"},
        ),
        # Large settings fall back to the general ones, one setting at a time.
        (
            "etl-full-large",
            {"PUDL_DBT_MEMORY_LIMIT": "8GB", "PUDL_DBT_LARGE_THREADS": "2"},
            {"memory_limit": "8GB", "threads": "2"},
        ),
        # Empty means unset, like in the ``configure_duckdb`` macro.
        ("etl-full", {"PUDL_DBT_TEMP_DIR": ""}, {}),
    ],
)
def test_duckdb_settings(mocker, target, environ, expected):
    """Settings come from PUDL_DBT_* variables, like in ``configure_duckdb``."""
    mocker.patch.dict("os.environ", environ, clear=True)

    assert duckdb_settings(target) == {"preserve_insertion_order": "false"} | expected
