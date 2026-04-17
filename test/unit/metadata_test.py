"""Tests for metadata not covered elsewhere."""

import json
import re

import frictionless
import geopandas as gpd  # noqa: ICN002
import pandas as pd
import pandera.pandas as pr_pandas
import pandera.polars as pr_polars
import polars as pl
import pytest
from shapely import Point

from pudl.metadata import PUDL_PACKAGE
from pudl.metadata.classes import (
    DataSource,
    Field,
    Package,
    PudlResourceDescriptor,
    Resource,
    SnakeCase,
)
from pudl.metadata.descriptions import (
    PARTITION_OFFSETS,
    ResourceDescriptionBuilder,
    ResourceTrait,
)
from pudl.metadata.fields import FIELD_METADATA
from pudl.metadata.helpers import format_errors
from pudl.metadata.resource_helpers import merge_descriptions
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
def dummy_resource_dict():
    return {
        "description": "test resource based on core_eia__entity_plants",
        "schema": {
            "fields": ["plant_id_eia", "city", "capacity_mw"],
            "primary_key": ["plant_id_eia"],
        },
        "sources": ["eia860", "eia923"],
        "etl_group": "entity_eia",
        "field_namespace": "eia",
    }


@pytest.fixture()
def dummy_resource_dict_w_geometry():
    return {
        "description": "test resource based on core_eia__entity_plants with added geometry field",
        "schema": {
            "fields": ["plant_id_eia", "city", "capacity_mw", "geometry"],
            "primary_key": ["plant_id_eia"],
        },
        "sources": ["eia860", "eia923"],
        "etl_group": "entity_eia",
        "field_namespace": "eia",
    }


@pytest.fixture()
def dummy_pandera_schema(dummy_resource_dict):
    resource_descriptor = PudlResourceDescriptor.model_validate(dummy_resource_dict)
    resource = Resource.model_validate(
        Resource.dict_from_resource_descriptor(
            "test_eia__entity_plants", resource_descriptor
        )
    )
    return resource.schema.to_pandera()


@pytest.fixture()
def dummy_pandera_schema_w_geometry(dummy_resource_dict_w_geometry):
    resource_descriptor = PudlResourceDescriptor.model_validate(
        dummy_resource_dict_w_geometry
    )
    resource = Resource.model_validate(
        Resource.dict_from_resource_descriptor(
            "test_eia__entity_plants_w_geometry", resource_descriptor
        )
    )
    return resource.schema.to_pandera()


@pytest.mark.parametrize(
    "data,backend",
    [
        (
            pl.DataFrame(
                {
                    "plant_id_eia": [12345, 12346],
                    "city": ["Bloomington", "Springfield"],
                    "capacity_mw": [1.3, 1.0],
                }
            ),
            "polars",
        ),
        (
            gpd.GeoDataFrame(
                {
                    "plant_id_eia": [12345, 12346],
                    "city": pd.Series(["Bloomington", "Springfield"], dtype="string"),
                    "capacity_mw": [1.3, 1.0],
                    "geometry": [Point(0, 0), Point(1, 0)],
                }
            ),
            "pandas",
        ),
    ],
)
def test_resource_descriptors_can_encode_schemas(
    data, backend, dummy_pandera_schema, dummy_pandera_schema_w_geometry
):
    if backend == "polars":
        schema = dummy_pandera_schema
        assert not schema.validate(data).is_empty()
    else:
        schema = dummy_pandera_schema_w_geometry
        assert not schema.validate(data).empty


@pytest.mark.parametrize(
    "error_msg,data",
    [
        pytest.param(
            "column 'plant_id_eia' not in dataframe",
            pl.DataFrame([]),
            id="empty dataframe",
        ),
        pytest.param(
            "expected column 'plant_id_eia' to have type Int64, got String",
            pl.DataFrame(
                {
                    "plant_id_eia": ["non_number"],
                    "city": ["Bloomington"],
                    "capacity_mw": ["1.3"],
                }
            ),
            id="bad dtype",
        ),
        pytest.param(
            "columns .* not unique",
            pl.DataFrame(
                {
                    "plant_id_eia": [12345, 12345],
                    "city": ["Bloomington", "Springfield"],
                    "capacity_mw": [1.3, 1.0],
                }
            ),
            id="duplicate PK",
        ),
    ],
)
def test_resource_descriptor_schema_failures(error_msg, data, dummy_pandera_schema):
    with pytest.raises(pr_polars.errors.SchemaError, match=error_msg):
        dummy_pandera_schema.validate(data)


@pytest.mark.parametrize(
    "error_msg,data",
    [
        pytest.param(
            "column 'plant_id_eia' not in dataframe",
            gpd.GeoDataFrame([]),
            id="empty dataframe",
        ),
        pytest.param(
            "expected series 'plant_id_eia' to have type Int64",
            gpd.GeoDataFrame(
                {
                    "plant_id_eia": ["non_number"],
                    "city": ["Bloomington"],
                    "capacity_mw": ["1.3"],
                    "geometry": [Point(0, 0)],
                }
            ).astype(str),
            id="bad dtype",
        ),
        pytest.param(
            "columns .* not unique",
            gpd.GeoDataFrame(
                {
                    "plant_id_eia": [12345, 12345],
                    "city": pd.Series(["Bloomington", "Springfield"], dtype="string"),
                    "capacity_mw": [1.3, 1.0],
                    "geometry": [Point(0, 0), Point(1, 0)],
                }
            ),
            id="duplicate PK",
        ),
    ],
)
def test_resource_descriptor_schema_failures_w_geometry(
    error_msg, data, dummy_pandera_schema_w_geometry
):
    with pytest.raises(pr_pandas.errors.SchemaError, match=error_msg):
        dummy_pandera_schema_w_geometry.validate(data)


def test_frictionless_data_package_non_empty(tmp_path):
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


def test_frictionless_data_package_resources_populated(tmp_path):
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


def test_merge_descriptions():
    "Ensure descriptions are merged properly."
    left = {
        "additional_summary_text": "one",
        "additional_details_text": "two",
        "usage_warnings": ["red"],
    }
    right = {
        "additional_summary_text": "fish",
        "additional_details_text": "poisson",
        "usage_warnings": ["blue"],
    }
    result = {
        "additional_summary_text": "one fish",
        "additional_details_text": "two\n\npoisson",
        "usage_warnings": ["red", "blue"],
    }
    assert merge_descriptions(left, right) == result
    # make sure we didn't accidentally modify the source list during the merge
    assert left["usage_warnings"] == ["red"]


def test_multiple_path_resources(dummy_resource_dict):
    """Test that resources paths are output to json as expected after converting to frictionless."""
    default_path_resource = Resource(
        **Resource.dict_from_resource_descriptor(
            "out_pudl__default_path_resource",
            PudlResourceDescriptor.model_validate(dummy_resource_dict),
        )
    )
    frictionless_resource = frictionless.Resource(
        json.loads(default_path_resource.to_frictionless().to_json())
    )
    assert frictionless_resource.path == "out_pudl__default_path_resource.parquet"

    override_single_path_resource = Resource(
        **Resource.dict_from_resource_descriptor(
            "out_pudl__override_single_path_resource",
            PudlResourceDescriptor.model_validate(
                dummy_resource_dict | {"path": "fake_path.parquet"}
            ),
        )
    )
    frictionless_resource = frictionless.Resource(
        json.loads(override_single_path_resource.to_frictionless().to_json())
    )
    assert frictionless_resource.path == "fake_path.parquet"

    paths = [f"fake_path{i}" for i in range(1, 5)]
    multiple_path_resource = Resource(
        **Resource.dict_from_resource_descriptor(
            "out_pudl__multiple_path_resource",
            PudlResourceDescriptor.model_validate(
                dummy_resource_dict
                | {
                    "path": paths[0],
                    "extrapaths": paths[1:],
                }
            ),
        )
    )
    frictionless_resource = frictionless.Resource(
        json.loads(multiple_path_resource.to_frictionless().to_json())
    )
    assert frictionless_resource.path == paths[0]
    assert set(frictionless_resource.extrapaths) == set(paths[1:])
    assert set(frictionless_resource.normpaths) == set(paths)


def test_frictionless_data_package_filter_resources():
    """Test that filtering resources when converting Package to frictionless works as expected."""
    eqr_pattern = r"core_ferceqr.*"
    expected_num_eqr_resources = 4
    all_resources = PUDL_PACKAGE.to_frictionless().resources
    no_eqr_resources = PUDL_PACKAGE.to_frictionless(
        exclude_pattern=eqr_pattern
    ).resources
    only_eqr_resources = PUDL_PACKAGE.to_frictionless(
        include_pattern=eqr_pattern
    ).resources

    assert len(no_eqr_resources) == (len(all_resources) - expected_num_eqr_resources)
    assert len(only_eqr_resources) == expected_num_eqr_resources
    assert not any("eqr" in r.name for r in no_eqr_resources)
    assert all("eqr" in r.name for r in only_eqr_resources)


@pytest.mark.parametrize(
    "partition_key,period,offset,expected",
    [
        ("years", "2026", 0, "2026"),
        ("years", "2026", 1, "2027"),
        ("years", "2026", -1, "2025"),
        ("year_quarters", "2026q1", 0, "2026q1"),
        ("year_quarters", "2026q1", 1, "2026q2"),
        ("year_quarters", "2026q1", -1, "2025q4"),
        ("half_years", "2026half1", 0, "2026half1"),
        ("half_years", "2026half1", 1, "2026half2"),
        ("half_years", "2026half1", -1, "2025half2"),
        ("year_months", "2026-01", 0, "2026-01"),
        ("year_months", "2026-01", 1, "2026-02"),
        ("year_months", "2026-01", -1, "2025-12"),
    ],
)
def test_availability_offsets(partition_key, period, offset, expected):
    """Check edge cases for temporal partition arithmetic."""
    assert PARTITION_OFFSETS[partition_key](period, offset) == expected


@pytest.mark.parametrize("source_id", sorted(r for r in SOURCES))
def test_source_availability(source_id):
    """Check that all sources have a reasonable temporal availability.

    Sources with only non-temporal partitions will evaluate to None; all others
    should show after 1990.

    We check this because if you get the data types wrong in pd.Timestamp, it
    spits out 1970 instead of the proper year.
    """
    src = DataSource.from_id(source_id)
    availability = ResourceDescriptionBuilder.offset_source_availability(src, 0)
    # Checking the lexical ordering of strings using > is a bit brittle, but has
    # the bonus of handling years, year quarters, half years, and year months.
    # If this breaks it probably means we're running on a machine that orders
    # strings by weird criteria -- we can revisit this at that time.
    assert (availability is None) or (availability > "1990")


@pytest.mark.parametrize(
    "given_name,given_settings,given_rowcounts,given_source,expected",
    [
        (  # manually specified
            "test_eia__entity_test",
            {"availability_text": "manual", "sources": []},
            [None],
            [],
            ResourceTrait(type="True", description="manual"),
        ),
        (  # using row counts
            "test_eia__entity_test",
            {"sources": []},
            ["row counts"],
            [],
            ResourceTrait(type="True", description="row counts"),
        ),
        (  # using source: least recent among at least one temporal partition
            "test_eia__entity_test",
            {"sources": ["1", "x", "2"]},
            [None],
            ["1 source", None, "2 source"],
            ResourceTrait(type="True", description="1 source"),
        ),
        (  # using source; none temporal
            "test_eia__entity_test",
            {"sources": ["x", "y"]},
            [None],
            [None, None],
            ResourceTrait(type="False", description="Unknown"),
        ),
        (  # nobody knows
            "test_eia__entity_test",
            {"sources": []},
            [None],
            [],
            ResourceTrait(type="False", description="Unknown"),
        ),
    ],
)
def test_resolve_resource_availability(
    mocker, given_name, given_settings, given_rowcounts, given_source, expected
):
    """Verify fallback flow is correct for resolving most recently available data."""
    mocker.patch(
        "pudl.metadata.descriptions.ResourceDescriptionBuilder.compute_rowcounts_availability",
        mocker.Mock(side_effect=given_rowcounts, __is_component=False),
    )
    mocker.patch(
        "pudl.metadata.descriptions.ResourceDescriptionBuilder.offset_source_availability",
        mocker.Mock(side_effect=given_source, __is_component=False),
    )
    resolved = ResourceDescriptionBuilder(
        resource_id=given_name, settings=given_settings
    ).build()

    assert (resolved.availability.type == expected.type) and (
        resolved.availability.description == expected.description
    )


# TODO: flip this to true after we do the second pass to set description_primary_key
# everywhere that needs it
CHECK_DESCRIPTION_PRIMARY_KEYS = False

RE_CAPS = re.compile("[A-Z]")

EXPECT_NO_AVAILABILITY = {
    "core_ferc__entity_companies",
    "core_gridpathratoolkit__assn_generator_aggregation_group",
    "core_pudl__assn_utilities_plants",
    "core_pudl__codes_datasources",
    "core_pudl__codes_imputation_reasons",
    "core_pudl__codes_subdivisions",
    "core_pudl__entity_plants_pudl",
    "core_pudl__entity_utilities_pudl",
    "core_rus__codes_fuel_types",
    "core_rus__codes_investment_types",
}


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
    description_dict = resource_dict["description"]
    assert isinstance(description_dict, dict), (
        f"""Table {resource_id} must have a dictionary under the "description" key, but instead I found a {type(description_dict)}"""
    )
    resolved = ResourceDescriptionBuilder(
        resource_id=resource_id,
        settings=Resource._resolve_references_from_resource_descriptor(
            resource_id, PudlResourceDescriptor.model_validate(resource_dict)
        ),
    ).build()
    name_parse = {
        "layer_code": resolved.layer.type,
        "source_code": resolved.source.type,
        "table_type_code": (
            (resolved.summary.type.split("[")[0] != "None")
            or (
                (len(resolved.summary.description) > 0)
                and RE_CAPS.match(resolved.summary.description[0])
            )
        ),
        "timeseries_resolution_code": (
            (not resolved.summary.type.startswith("timeseries"))
            or (len(resolved.summary.type.split("[")[1]) > 1)
            or RE_CAPS.match(resolved.summary.description[0])
        ),
    }
    fix_with_summary = f"""Ensure RESOURCE_METADATA["{resource_id}"]["description"]["additional_summary_text"] is a complete sentence starting with a capital letter"""
    for key, has_value in name_parse.items():
        assert has_value, f"""Table {resource_id} could not be parsed as layer_source__tabletype_slug and insufficient hints were set in the table metadata. Repair using one of the following:
\t1. Rename {resource_id}
\t2. Set the following keys in RESOURCE_METADATA["{resource_id}"]["description"]: {key}{("\n\t3. " + fix_with_summary) if key in {"table_type_code", "timeseries_resolution_code"} else ""}"""
    # todo: layer-based checks
    # todo: asset_type-based checks
    # pk-based checks
    has_pk = resolved.primary_key.type == "True"
    if CHECK_DESCRIPTION_PRIMARY_KEYS and not has_pk:  # pragma: no cover
        assert "additional_primary_key_text" in description_dict, (
            f"""Table {resource_id} has no primary key, but the table metadata does not include an explanation in the required format. We expect the key "additional_primary_key_text" to briefly describe what each record represents and, if needed, why no primary key is possible."""
        )

    # availability-based checks
    assert ("availability_text" not in description_dict) or (
        "availability_offset" not in description_dict
    ), (
        f"Table {resource_id} has set both availability_text and availability_offset; you can't have both."
    )
    if resource_id not in EXPECT_NO_AVAILABILITY:
        assert resolved.availability.type == "True", (
            f"Missing availability for {resource_id}"
        )
