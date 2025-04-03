"""Tests for metadata not covered elsewhere."""

import pandas as pd
import pandera as pr
import pytest
from docutils.core import publish_doctree

from pudl.metadata import PUDL_PACKAGE
from pudl.metadata.classes import (
    DataSource,
    Field,
    Package,
    PudlResourceDescriptor,
    Resource,
    SnakeCase,
)
from pudl.metadata.fields import FIELD_METADATA, apply_pudl_dtypes
from pudl.metadata.helpers import format_errors
from pudl.metadata.resources import RESOURCE_METADATA
from pudl.metadata.sources import SOURCES

PUDL_RESOURCES = {r.name: r for r in PUDL_PACKAGE.resources}
PUDL_ENCODERS = PUDL_PACKAGE.encoders


def test_all_resources_valid() -> None:
    """All resources in metadata pass validation tests."""
    _ = PUDL_PACKAGE


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


@pytest.mark.parametrize("encoder_name", sorted(PUDL_ENCODERS.keys()))
def test_encoders(encoder_name: SnakeCase):
    """Verify that Encoders work on the kinds of values they're supposed to."""
    encoder = PUDL_ENCODERS[encoder_name]
    test_data = encoder.generate_encodable_data(size=100)
    _ = encoder.encode(test_data)


@pytest.mark.parametrize("field_name", sorted(FIELD_METADATA.keys()))
def test_field_definitions(field_name: str):
    """Check that all defined fields are valid."""
    _ = Field(name=field_name, **FIELD_METADATA[field_name])


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


def test_get_sorted_resources() -> None:
    """Test that resources are returned in this order (out, core, _out)."""
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
    assert first_resource_name.startswith("out"), (
        f"{first_resource_name} is the first resource. Expected a resource with the prefix 'out'"
    )
    assert last_resource_name.startswith("_out"), (
        f"{last_resource_name} is the last resource. Expected a resource with the prefix '_out'"
    )


def test_resource_descriptors_valid():
    # just make sure these validate properly
    descriptors = {
        name: PudlResourceDescriptor.model_validate(desc)
        for name, desc in RESOURCE_METADATA.items()
    }
    assert len(descriptors) > 0


@pytest.fixture()
def dummy_pandera_schema():
    resource_descriptor = PudlResourceDescriptor.model_validate(
        {
            "description": "test resource based on core_eia__entity_plants",
            "schema": {
                "fields": ["plant_id_eia", "city", "state"],
                "primary_key": ["plant_id_eia"],
            },
            "sources": ["eia860", "eia923"],
            "etl_group": "entity_eia",
            "field_namespace": "eia",
        }
    )
    resource = Resource.model_validate(
        Resource.dict_from_resource_descriptor(
            "test_eia__entity_plants", resource_descriptor
        )
    )
    return resource.schema.to_pandera()


def test_resource_descriptors_can_encode_schemas(dummy_pandera_schema):
    good_dataframe = pd.DataFrame(
        {
            "plant_id_eia": [12345, 12346],
            "city": ["Bloomington", "Springfield"],
            "state": ["IL", "IL"],
        }
    ).pipe(apply_pudl_dtypes)
    assert not dummy_pandera_schema.validate(good_dataframe).empty


@pytest.mark.parametrize(
    "error_msg,data",
    [
        pytest.param(
            "column 'plant_id_eia' not in dataframe",
            pd.DataFrame([]),
            id="empty dataframe",
        ),
        pytest.param(
            "expected series 'plant_id_eia' to have type Int64",
            pd.DataFrame(
                {
                    "plant_id_eia": ["non_number"],
                    "city": ["Bloomington"],
                    "state": ["IL"],
                }
            ).astype(str),
            id="bad dtype",
        ),
        pytest.param(
            "columns .* not unique",
            pd.DataFrame(
                {
                    "plant_id_eia": [12345, 12345],
                    "city": ["Bloomington", "Springfield"],
                    "state": ["IL", "IL"],
                }
            ).pipe(apply_pudl_dtypes),
            id="duplicate PK",
        ),
    ],
)
def test_resource_descriptor_schema_failures(error_msg, data, dummy_pandera_schema):
    with pytest.raises(pr.errors.SchemaError, match=error_msg):
        dummy_pandera_schema.validate(data)


def test_frictionless_data_package_non_empty():
    datapackage = PUDL_PACKAGE.to_frictionless()
    assert len(datapackage.resources) == len(RESOURCE_METADATA)


def test_frictionless_data_package_resources_populated():
    datapackage = PUDL_PACKAGE.to_frictionless()
    for resource in datapackage.resources:
        assert resource.name in RESOURCE_METADATA
        expected_resource = RESOURCE_METADATA[resource.name]
        assert expected_resource["description"] == resource.description
        assert expected_resource["schema"]["fields"] == [
            f.name for f in resource.schema.fields
        ]
        assert (
            expected_resource["schema"].get("primary_key", [])
            == resource.schema.primary_key
        )


description_compliant_tables = [
    "_core_eia860__fgd_equipment",
    # "core_eia861__yearly_demand_side_management_ee_dr", # noncompliant for testing unit test
]


@pytest.mark.parametrize(
    "resource_name", sorted(description_compliant_tables)
)  # someday: sorted(PUDL_RESOURCES.keys()))
def test_description_compliance(resource_name):
    meta = RESOURCE_METADATA[resource_name]
    desc = meta["description"]
    tree = publish_doctree(desc)
    front_matter = []
    for c in tree.children:
        if c.tagname == "paragraph":
            front_matter.append(c.astext())
        else:
            break
    layer = "core"  # todo: replace with cg's extractor
    source = "eia860"  # todo: replace with cg's extractor. or maybe just use meta["sources"]?
    source_labels = [
        SOURCES[s]["label"] for s in meta["sources"]
    ]  # todo: what if source extracted from table name isn't in meta["sources"]???
    asset_type = (
        "scd" if "_scd_" in resource_name else None
    )  # todo: replace with cg's extractor
    has_pk = "primary_key" in meta["schema"]
    # todo: break each category of check out into its own function
    # todo: layer-based checks
    # source-based checks
    for paragraph in front_matter:
        if paragraph.startswith("Derived from"):
            if any((s in paragraph) for s in source_labels):
                break
    else:
        # todo: refer to a template or wizard for additional help
        assert False, f"""Table {resource_name} has sources {str(meta["sources"])},
but the description does not explain them in the required format. We expect a
paragraph in the front matter starting with 'Derived from' and including any of the
source labels {str(source_labels)} as exact strings."""
    # todo: asset_type-based checks
    # pk-based checks
    if not has_pk:
        # some paragraph should declare no primary key and explain why
        for paragraph in front_matter:
            if paragraph.startswith("This table has no primary key."):
                paragraph_sentences = paragraph.split(".")
                if len(paragraph_sentences[1].strip()) > 5:
                    # okay so technically "This table has no primary key. lololol" will
                    # pass this test but ideally that kind of hijinks gets caught in
                    # review
                    break
        else:
            # todo: refer to a template or wizard for additional help
            assert False, f"""Table {resource_name} has no primary key, but the
description does not explain it in the required format. We expect a paragraph in the
front matter to start with 'This table has no primary key.' and go on to briefly
describe what each record represents and, if needed, why no primary key is
possible."""
