import dagster as dg
import pytest

from pudl.dbt_wrapper import dagster_to_dbt_selection


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
