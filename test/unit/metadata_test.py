"""Tests for metadata not covered elsewhere."""

import pandas as pd
import pandera.pandas as pr
import pytest

from pudl.metadata import PUDL_PACKAGE
from pudl.metadata.classes import (
    DataSource,
    Field,
    Package,
    PudlResourceDescriptor,
    Resource,
    SnakeCase,
)
from pudl.metadata.descriptions import ResourceDescriptionBuilder
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
    """Test that resources are returned in this order (out, core, _core)."""
    resource_ids = (
        "_core_eia860__fgd_equipment",
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
    assert last_resource_name.startswith("_core"), (
        f"{last_resource_name} is the last resource. Expected a resource with the prefix '_core'"
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
                "fields": ["plant_id_eia", "city", "capacity_mw"],
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
            "capacity_mw": [1.3, 1.0],
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
                    "capacity_mw": [1.3],
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
                    "capacity_mw": [1.3, 1.0],
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


METADATA_OVERRIDE_KEYS = [
    "layer_code",
    "table_type_code",
    "timeseries_resolution_code",
    "additional_summary_text",
    "additional_layer_text",
    "additional_source_text",
    "additional_primary_key_text",
    "additional_details_text",
]


def test_frictionless_data_package_resources_populated():
    datapackage = PUDL_PACKAGE.to_frictionless()
    for resource in datapackage.resources:
        assert resource.name in RESOURCE_METADATA
        expected_resource = RESOURCE_METADATA[resource.name]
        # TODO: remove str option after metadata migration
        strings_to_find = []
        if isinstance(expected_resource["description"], str):
            strings_to_find.append(expected_resource["description"])
        else:
            for k in METADATA_OVERRIDE_KEYS:
                if k in expected_resource["description"]:
                    strings_to_find.append(expected_resource["description"][k])
        assert any(
            resource.description.find(candidate) >= 0 for candidate in strings_to_find
        )
        assert expected_resource["schema"]["fields"] == [
            f.name for f in resource.schema.fields
        ]
        assert (
            expected_resource["schema"].get("primary_key", [])
            == resource.schema.primary_key
        )


# TODO: flip this to true after we do the second pass to set description_primary_key
# everywhere that needs it
CHECK_DESCRIPTION_PRIMARY_KEYS = False


@pytest.mark.parametrize(
    # todo: back this off to sorted(PUDL_RESOURCES.keys()) after the migration.
    # only check migrated tables. a table is migrated if "description" has been converted from a string to a dict.
    "resource_id",
    sorted(
        r
        for r in PUDL_RESOURCES
        if isinstance(RESOURCE_METADATA[r]["description"], dict)
    ),
)
def test_description_compliance(resource_id):
    resource_dict = RESOURCE_METADATA[resource_id]
    assert isinstance(resource_dict["description"], dict), (
        f"""Table {resource_id} is listed as description-compliant in metadata_test.py, but the "description" key is not a dictionary. (In theory pydantic should have screamed about that before you got this far)"""
    )
    builder = ResourceDescriptionBuilder(
        resource_id=resource_id, settings=resource_dict
    )
    name_parse = {
        "layer_code": builder.layer.type,
        "source_code": builder.source.type,
        "table_type_code": (
            (builder.summary.type.split("[")[0] != "None")
            or len(builder.summary.description) > 0
        ),
        "timeseries_resolution_code": (
            (not builder.summary.type.startswith("timeseries"))
            or len(builder.summary.type.split("[")[1]) > 1
        ),
    }
    for key, has_value in name_parse.items():
        assert has_value, (
            f"""Table {resource_id} could not be parsed as layer_source__tabletype_slug and no hints were set in the table metadata. Rename {resource_id} or set the following keys in RESOURCE_METADATA["{resource_id}"]["description"]: {key}"""
        )
    # todo: layer-based checks
    # todo: asset_type-based checks
    # pk-based checks
    has_pk = builder.primary_key.type == "True"
    if CHECK_DESCRIPTION_PRIMARY_KEYS and not has_pk:  # pragma: no cover
        assert "additional_primary_key_text" in resource_dict["description"], (
            f"""Table {resource_id} has no primary key, but the table metadata does not include an explanation in the required format. We expect the key "additional_primary_key_text" to briefly describe what each record represents and, if needed, why no primary key is possible."""
        )
