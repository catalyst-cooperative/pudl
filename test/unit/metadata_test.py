"""Tests for metadata not covered elsewhere."""
import pytest

from pudl.metadata.classes import DataSource, Package
from pudl.metadata.helpers import format_errors
from pudl.metadata.resources import RESOURCE_METADATA
from pudl.metadata.sources import SOURCES


def test_all_resources_valid() -> None:
    """All resources in metadata pass validation tests."""
    _ = Package.from_resource_ids()


@pytest.mark.parametrize("src", list(SOURCES))
def test_all_data_sources_valid(src) -> None:
    """Test that all stored DataSource definitions are valid."""
    _ = DataSource.from_id(src)


def test_all_excluded_resources_exist() -> None:
    """All resources excluded from foreign key rules exist."""
    errors = []
    for name, meta in RESOURCE_METADATA.items():
        rule = meta.get("schema", {}).get("foreign_key_rules")
        if rule:
            missing = [x for x in rule.get("exclude", []) if x not in RESOURCE_METADATA]
            if missing:
                errors.append(f"{name}: {missing}")
    if errors:
        raise ValueError(
            format_errors(
                *errors, title="Invalid resources in foreign_key_rules.exclude"
            )
        )


def test_get_etl_group_tables() -> None:
    """Test that a Value error is raised for non existent etl group."""
    with pytest.raises(ValueError):
        Package.get_etl_group_tables("not_an_etl_group")


def test_get_sorted_resources() -> None:
    """Test that resources are returned in this order (out, core, _out)."""
    # Grab resources that
    resource_ids = (
        "_out_eia__plants_utilities",
        "core_eia__entity_boilers",
        "out_eia__yearly_boilers",
    )
    resources = Package.from_resource_ids(
        resource_ids=resource_ids, resolve_foreign_keys=True
    ).get_sorted_resources()

    first_resource_name = resources[0].name
    last_resource_name = resources[-1].name
    assert first_resource_name.startswith(
        "out"
    ), f"{first_resource_name} is the first resource. Expected a resource with the prefix 'out'"
    assert last_resource_name.startswith(
        "_out"
    ), f"{last_resource_name} is the last resource. Expected a resource with the prefix '_out'"
