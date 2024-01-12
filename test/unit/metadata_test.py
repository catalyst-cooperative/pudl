"""Tests for metadata not covered elsewhere."""
import pytest

from pudl.metadata.classes import DataSource, Field, Package
from pudl.metadata.fields import FIELD_METADATA
from pudl.metadata.helpers import format_errors
from pudl.metadata.resources import RESOURCE_METADATA
from pudl.metadata.sources import SOURCES

PUDL_RESOURCES = {r.name: r for r in Package.from_resource_ids().resources}


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


@pytest.mark.parametrize("resource_name", sorted(PUDL_RESOURCES.keys()))
def test_pyarrow_schemas(resource_name: str):
    """Verify that we can produce pyarrow schemas for all defined Resources."""
    _ = PUDL_RESOURCES[resource_name].to_pyarrow()


@pytest.mark.parametrize("field_name", sorted(FIELD_METADATA.keys()))
def test_field_definitions(field_name: str):
    """Check that all defined fields are valid."""
    _ = Field(name=field_name, **FIELD_METADATA[field_name])


@pytest.mark.xfail(reason="Need to purge unused fields. See issue #3224")
def test_defined_fields_are_used():
    """Check that all fields which are defined are actually used."""
    used_fields = set()
    for resource in PUDL_RESOURCES.values():
        used_fields |= {f.name for f in resource.schema.fields}
    defined_fields = set(FIELD_METADATA.keys())
    unused_fields = sorted(defined_fields - used_fields)
    if len(unused_fields) > 0:
        raise AssertionError(
            f"Found {len(unused_fields)} unused fields: {unused_fields}"
        )


@pytest.mark.xfail(reason="Incomplete descriptions. See issue #3224")
def test_fields_have_descriptions():
    """Check that all fields have a description and report any that do not."""
    fields_without_description = []
    for field_name in FIELD_METADATA:
        field = Field(name=field_name, **FIELD_METADATA[field_name])
        if field.description is None:
            fields_without_description.append(field_name)
    fields_without_description = sorted(fields_without_description)
    if len(fields_without_description) > 0:
        raise AssertionError(
            f"Found {len(fields_without_description)} fields without descriptions: "
            f"{fields_without_description}"
        )


@pytest.mark.xfail(reason="Incomplete descriptions. See issue #3224")
def test_resources_have_descriptions():
    """Check that all resources have a description and report any that do not."""
    resources_without_description = []
    for resource_name, resource in PUDL_RESOURCES.items():
        if resource.description is None:
            resources_without_description.append(resource_name)
    resources_without_description = sorted(resources_without_description)
    if len(resources_without_description) > 0:
        raise AssertionError(
            f"Found {len(resources_without_description)} resources without descriptions: "
            f"{resources_without_description}"
        )
