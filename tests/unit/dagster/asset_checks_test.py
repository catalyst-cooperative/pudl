"""Regression tests for Dagster asset-check input typing.

:func:`~pudl.dagster.asset_checks.asset_check_from_schema` is a factory that generates
one Dagster asset check per PUDL table. Each generated check function has a single
parameter, ``asset_value``, whose type annotation Dagster inspects **at runtime** to
decide which IO manager to use when loading the asset. The annotation must be the exact
type object appropriate for that specific asset:

* ``pl.LazyFrame`` for normal Parquet-backed assets
* ``gpd.GeoDataFrame`` for assets containing geometry columns
* ``ParquetData`` for DuckDB-produced assets

The factory stores this in a local variable (``asset_type``) and uses it directly as
the annotation, which requires a ``# type: ignore[valid-type]`` comment to silence the
static type checker.

The tempting "cleanup" is to replace that computed annotation with a tidy static union::

    def pandera_schema_check(
        asset_value: pl.LazyFrame | gpd.GeoDataFrame | ParquetData,  # looks fine!
    ) -> dg.AssetCheckResult:

This satisfies the type checker and removes the ``type: ignore``, but it silently breaks
every generated check: Dagster sees the union and can no longer determine the correct IO
manager, so it will attempt to load every asset through the wrong path.

The tests below guard against both classes of "cleanup" by asserting that the annotation
on each generated check is the *exact* expected type object (using ``is``, not ``==``).
"""

import io
from datetime import date

import dagster as dg
import geopandas as gpd  # noqa: ICN002
import pandas as pd
import pint
import polars as pl
import pytest
from dagster._core.definitions.asset_checks.asset_checks_definition import (
    AssetChecksDefinition,
)
from shapely.geometry import Point

from pudl.dagster.asset_checks import (
    _CHUNKED_PK_CHECK_TABLES,
    _build_registry_from_descriptor,
    _chunk_filters,
    _find_duplicate_primary_keys,
    _validate_datapackage_unit_strings,
    _validate_primary_key_uniqueness,
    asset_check_from_schema,
    group_mean_continuity_check,
)
from pudl.helpers import ParquetData
from pudl.metadata.classes import PUDL_PACKAGE, Package, Resource
from pudl.metadata.units import PUDL_UNIT_DEFINITIONS


@pytest.mark.parametrize(
    ("asset_name", "duckdb_asset", "expected_type"),
    [
        ("core_pudl__codes_subdivisions", False, pl.LazyFrame),
        ("core_ferceqr__contracts", True, ParquetData),
    ],
)
def test_asset_checks_preserve_runtime_input_types(
    asset_name: str, duckdb_asset: bool, expected_type: type
) -> None:
    """Generated checks should advertise the IO-manager input type they expect."""
    check: AssetChecksDefinition | None = asset_check_from_schema(
        dg.AssetKey([asset_name]),
        PUDL_PACKAGE,
        duckdb_asset=duckdb_asset,
    )

    assert check is not None
    assert (
        check.node_def.compute_fn.decorated_fn.__annotations__["asset_value"]
        is expected_type
    )


@pytest.mark.parametrize(
    "column,threshold,n_outliers_allowed,expected_pass",
    [
        # Test cases that should PASS
        ("stable_metric", 0.1, 0, True),
        ("gradual_growth", 0.2, 0, True),
        ("sudden_jump", 5.0, 0, True),
        ("volatile_metric", 0.2, 1, True),
        ("negative_change", 0.2, 0, True),
        # Test cases that should FAIL
        ("sudden_jump", 0.1, 0, False),
        ("volatile_metric", 0.1, 0, False),
        ("sudden_jump", 1.0, 0, False),
        ("gradual_growth", 0.05, 0, False),
    ],
)
def test_group_mean_continuity_check(
    column, threshold, n_outliers_allowed, expected_pass
):
    """Test the group_mean_continuity_check function with various scenarios.

    Uses a test dataframe with different column patterns:
    - stable_metric: Values around 100 with minimal variation
    - gradual_growth: Fixed growth of 10 per year
    - sudden_jump: Large 5x jump from 2022 to 2023
    - volatile_metric: Random fluctuations around 100, one of which is larger
    - negative_change: Fixed decline of 100 per year
    """
    # Test data for group_mean_continuity_check function
    # This dataframe contains various patterns for testing different scenarios
    mean_continuity_df = pd.read_csv(
        io.StringIO(
            """year,stable_metric,gradual_growth,sudden_jump,volatile_metric,negative_change
    2020,100,100,100,100,1000
    2021,101,110,102,95,900
    2022,99,120,105,130,800
    2023,102,130,500,105,700
    2024,98,140,510,90,600
    """
        )
    )

    result = group_mean_continuity_check(
        df=mean_continuity_df,
        thresholds={column: threshold},
        groupby_col="year",
        n_outliers_allowed=n_outliers_allowed,
    )

    assert result.passed == expected_pass

    # Verify metadata structure
    assert hasattr(result, "metadata")
    assert isinstance(result.metadata, dict)

    # If test failed, metadata should contain information about the failing column
    if not expected_pass:
        assert column in result.metadata
        # The metadata values are wrapped in JsonMetadataValue objects
        # Access the underlying data using the .data attribute
        column_metadata = result.metadata[column].data
        assert isinstance(column_metadata, dict)
        assert "threshold" in column_metadata
        assert column_metadata["threshold"] == threshold
        assert "top5" in column_metadata


# ---------------------------------------------------------------------------
# Tests for descriptor-embedded unit registry helpers
# ---------------------------------------------------------------------------

_MINIMAL_DESCRIPTOR = {
    "unit_registry": {"format": "pint", "definitions": PUDL_UNIT_DEFINITIONS},
    "resources": [
        {
            "name": "test_resource",
            "schema": {
                "fields": [
                    {"name": "energy_mwh", "unit": "MWh"},
                    {"name": "heat_mmbtu", "unit": "MMBtu"},
                    {"name": "cost_usd_per_mwh", "unit": "USD / MWh"},
                    {"name": "no_unit_field"},
                ]
            },
        }
    ],
}


def test_build_registry_from_descriptor_parses_custom_units() -> None:
    """Registry built from descriptor definitions must parse PUDL custom units."""
    ureg = _build_registry_from_descriptor(_MINIMAL_DESCRIPTOR)
    assert isinstance(ureg, pint.UnitRegistry)
    ureg.parse_units("MMBtu")
    ureg.parse_units("VAr")
    ureg.parse_units("USD / MWh")


def test_build_registry_from_descriptor_missing_key() -> None:
    """KeyError is raised when the descriptor has no unit_registry field."""
    with pytest.raises(KeyError):
        _build_registry_from_descriptor({})


def test_validate_datapackage_unit_strings_all_valid() -> None:
    """No errors are returned for a descriptor with only valid unit strings."""
    errors = _validate_datapackage_unit_strings(_MINIMAL_DESCRIPTOR)
    assert errors == [], f"Expected no errors for valid unit strings, but got: {errors}"


def test_validate_datapackage_unit_strings_bad_unit() -> None:
    """An unparseable unit string produces exactly one error entry."""
    descriptor = {
        "unit_registry": {"format": "pint", "definitions": PUDL_UNIT_DEFINITIONS},
        "resources": [
            {
                "name": "bad_resource",
                "schema": {"fields": [{"name": "col", "unit": "not_a_unit"}]},
            }
        ],
    }
    errors = _validate_datapackage_unit_strings(descriptor)
    assert len(errors) == 1
    assert "bad_resource.col" in errors[0]
    assert "not_a_unit" in errors[0]


def test_validate_datapackage_unit_strings_missing_registry() -> None:
    """A descriptor without unit_registry produces an error rather than crashing."""
    errors = _validate_datapackage_unit_strings({"resources": []})
    assert len(errors) == 1
    assert "Could not build unit registry" in errors[0], (
        f"Unexpected error message: {errors[0]}"
    )


# ---------------------------------------------------------------------------
# Tests for content-level (not just schema-level) validation
# ---------------------------------------------------------------------------
#
# Pandera's Polars backend forces SCHEMA_ONLY validation depth for LazyFrame
# inputs unless validation depth is explicitly configured (see
# pandera.api.polars.utils.get_validation_depth). PUDL never configures it, so
# pandera_schema_check currently verifies column presence and dtypes for every
# Polars LazyFrame asset (the vast majority of PUDL tables), but silently skips
# every Check.ge/Check.le/Check.isin/uniqueness constraint declared in PUDL's
# metadata. The pandas/geopandas backend has no such override and already
# enforces those checks. These tests use synthetic, minimal resources (rather
# than real PUDL tables) so the fixtures stay small and independent of
# unrelated metadata changes.


def _content_check_fields(*, with_geometry: bool) -> list[dict]:
    """Shared field set for the polars and geopandas content-check fixtures."""
    fields = [
        {"name": "id", "type": "integer", "description": "Primary key."},
        {
            "name": "value",
            "type": "integer",
            "description": "Value bounded to [0, 100].",
            "constraints": {"minimum": 0, "maximum": 100},
        },
        {
            "name": "code",
            "type": "string",
            "description": "Enum-constrained code.",
            "constraints": {"enum": ["a", "b", "c"]},
        },
    ]
    if with_geometry:
        fields.append(
            {"name": "geometry", "type": "geometry", "description": "Geometry."}
        )
    return fields


def _content_check_fn(*, with_geometry: bool):
    """Build the generated ``pandera_schema_check`` function for a synthetic resource."""
    resource = Resource(
        name="_test__geopandas_content_checks"
        if with_geometry
        else "_test__polars_content_checks",
        description="Synthetic resource for asset-check content-validation tests.",
        schema={
            "fields": _content_check_fields(with_geometry=with_geometry),
            "primary_key": ["id"],
        },
    )
    package = Package(name="_test_package", resources=[resource])
    check = asset_check_from_schema(
        dg.AssetKey([resource.name]), package, duckdb_asset=False
    )
    assert check is not None
    return check.node_def.compute_fn.decorated_fn


@pytest.mark.parametrize(
    ("value", "code", "expected_pass"),
    [
        (30, "a", True),
        (150, "a", False),  # violates maximum=100
        (30, "z", False),  # violates enum=["a", "b", "c"]
    ],
    ids=["valid", "value_out_of_range", "code_not_in_enum"],
)
def test_polars_lazyframe_content_checks(value, code, expected_pass) -> None:
    """Content (not just schema) violations should fail the generated asset check.

    As of this writing, pandera's Polars backend does not enforce content-level
    checks (Check.ge/le/isin/etc.) on ``pl.LazyFrame`` inputs by default, so the
    two violation cases here are expected to incorrectly report ``passed=True``
    until that is fixed.
    """
    fn = _content_check_fn(with_geometry=False)
    lf = pl.LazyFrame(
        {"id": [1, 2, 3], "value": [10, 20, value], "code": ["a", "b", code]}
    )
    lf = lf.with_columns(pl.col("code").cast(pl.Categorical))
    result = fn(lf)
    assert result.passed == expected_pass


@pytest.mark.parametrize(
    ("value", "code", "expected_pass"),
    [
        (30, "a", True),
        (150, "a", False),  # violates maximum=100
        (30, "z", False),  # violates enum=["a", "b", "c"]
    ],
    ids=["valid", "value_out_of_range", "code_not_in_enum"],
)
def test_geopandas_content_checks(value, code, expected_pass) -> None:
    """Content violations are already correctly caught on the geopandas/pandas path.

    Geometry-bearing tables use pandera's pandas backend
    (:func:`pudl.metadata.classes.Schema.to_pandera`), which has no
    LazyFrame-specific validation-depth override, so these checks already run
    today and all three cases are expected to pass as written.
    """
    fn = _content_check_fn(with_geometry=True)
    gdf = gpd.GeoDataFrame(
        {
            "id": pd.array([1, 2, 3], dtype="Int64"),
            "value": pd.array([10, 20, value], dtype="Int64"),
            "code": pd.Categorical(["a", "b", code]),
            "geometry": [Point(0, 0), Point(1, 1), Point(2, 2)],
        }
    )
    result = fn(gdf)
    assert result.passed == expected_pass


# ---------------------------------------------------------------------------
# Tests for nullable/uniqueness validation
# ---------------------------------------------------------------------------
#
# These constraints are structurally different from the Check-based value/enum
# constraints above: `required` maps to `Column(nullable=...)`, single-column
# `unique` maps to `Column(unique=...)`, and the primary key maps to a
# DataFrame-level `unique=[...]` composite check
# (see Field.to_pandera_column and Schema.to_pandera). They go through
# different pandera code than Check.ge/le/isin, so they're tested separately
# rather than assumed to be fixed by the same code path.


def _uniqueness_check_fields(*, with_geometry: bool) -> list[dict]:
    """Shared field set for the polars and geopandas uniqueness-check fixtures."""
    fields = [
        {"name": "id", "type": "integer", "description": "Primary key, part 1."},
        {"name": "id2", "type": "integer", "description": "Primary key, part 2."},
        {
            "name": "required_field",
            "type": "integer",
            "description": "Non-nullable, non-key field.",
            "constraints": {"required": True},
        },
        {
            "name": "unique_field",
            "type": "integer",
            "description": "Single-column uniqueness constraint.",
            "constraints": {"unique": True},
        },
    ]
    if with_geometry:
        fields.append(
            {"name": "geometry", "type": "geometry", "description": "Geometry."}
        )
    return fields


def _uniqueness_check_fn(*, with_geometry: bool):
    """Build the generated ``pandera_schema_check`` function for a synthetic resource."""
    resource = Resource(
        name="_test__geopandas_uniqueness_checks"
        if with_geometry
        else "_test__polars_uniqueness_checks",
        description="Synthetic resource for asset-check uniqueness-validation tests.",
        schema={
            "fields": _uniqueness_check_fields(with_geometry=with_geometry),
            "primary_key": ["id", "id2"],
        },
    )
    package = Package(name="_test_package", resources=[resource])
    check = asset_check_from_schema(
        dg.AssetKey([resource.name]), package, duckdb_asset=False
    )
    assert check is not None
    return check.node_def.compute_fn.decorated_fn


_UNIQUENESS_CASES = [
    ([1, 2, 3], [1, 2, 3], [1, 2, 3], [1, 2, 3], True),
    ([1, 2, 3], [1, 2, 3], [1, None, 3], [1, 2, 3], False),  # null required_field
    ([1, 2, 3], [1, 2, 3], [1, 2, 3], [1, 1, 3], False),  # duplicate unique_field
    ([1, 1, 3], [1, 1, 3], [1, 2, 3], [1, 2, 3], False),  # duplicate composite PK
]
_UNIQUENESS_IDS = [
    "valid",
    "null_in_required_field",
    "duplicate_unique_field",
    "duplicate_primary_key",
]


@pytest.mark.parametrize(
    ("id_", "id2", "required_field", "unique_field", "expected_pass"),
    _UNIQUENESS_CASES,
    ids=_UNIQUENESS_IDS,
)
def test_polars_lazyframe_uniqueness_checks(
    id_, id2, required_field, unique_field, expected_pass
) -> None:
    """Nullable and uniqueness violations should fail the generated asset check.

    This resource isn't in ``_CHUNKED_PK_CHECK_TABLES``, so composite
    primary-key uniqueness goes through pandera's normal, unchunked check
    (see ``test_validate_primary_key_by_temporal_chunk*`` for the chunked
    path used by PUDL's few oversized tables).
    """
    fn = _uniqueness_check_fn(with_geometry=False)
    lf = pl.LazyFrame(
        {
            "id": id_,
            "id2": id2,
            "required_field": required_field,
            "unique_field": unique_field,
        }
    )
    result = fn(lf)
    assert result.passed == expected_pass


@pytest.mark.parametrize(
    ("id_", "id2", "required_field", "unique_field", "expected_pass"),
    _UNIQUENESS_CASES,
    ids=_UNIQUENESS_IDS,
)
def test_geopandas_uniqueness_checks(
    id_, id2, required_field, unique_field, expected_pass
) -> None:
    """Nullable and uniqueness violations are already correctly caught on the
    geopandas/pandas path.
    """
    fn = _uniqueness_check_fn(with_geometry=True)
    gdf = gpd.GeoDataFrame(
        {
            "id": pd.array(id_, dtype="Int64"),
            "id2": pd.array(id2, dtype="Int64"),
            "required_field": pd.array(required_field, dtype="Int64"),
            "unique_field": pd.array(unique_field, dtype="Int64"),
            "geometry": [Point(0, 0), Point(1, 1), Point(2, 2)],
        }
    )
    result = fn(gdf)
    assert result.passed == expected_pass


# ---------------------------------------------------------------------------
# Tests for chunked primary-key uniqueness checking
# ---------------------------------------------------------------------------
#
# `_validate_primary_key_uniqueness` never uses pandera's built-in composite-
# uniqueness check (too memory-hungry, see polars-comment.md); it always uses
# its own group-by/count-based `_find_duplicate_primary_keys`, optionally run
# once per chunk instead of once on the whole table for PUDL's few oversized
# tables (`_CHUNKED_PK_CHECK_TABLES`). Chunking is only correct because the
# chunking column is itself part of the primary key -- two rows with an
# identical composite key necessarily share the same value of it, so
# duplicates can never span chunk boundaries, whether the column is
# date/datetime (chunked by year) or anything else (chunked by distinct
# value). These tests exercise the machinery directly with synthetic
# resources, independent of which real tables are currently enumerated.


def _temporal_pk_resource() -> Resource:
    return Resource(
        name="_test__temporal_pk_chunking",
        description="Synthetic resource with a temporal primary key.",
        schema={
            "fields": [
                {"name": "event_date", "type": "date", "description": "Event date."},
                {"name": "unit_id", "type": "integer", "description": "Unit ID."},
            ],
            "primary_key": ["event_date", "unit_id"],
        },
    )


def _categorical_pk_resource() -> Resource:
    return Resource(
        name="_test__categorical_pk_chunking",
        description="Synthetic resource with a string primary-key column.",
        schema={
            "fields": [
                {"name": "filer_id", "type": "string", "description": "Filer ID."},
                {
                    "name": "record_id",
                    "type": "integer",
                    "description": "Record ID.",
                },
            ],
            "primary_key": ["filer_id", "record_id"],
        },
    )


def test_validate_primary_key_uniqueness_temporal_valid() -> None:
    """Repeated non-key values across different years are not false positives.

    ``_temporal_pk_resource`` isn't in ``_CHUNKED_PK_CHECK_TABLES``, so this
    exercises ``_validate_primary_key_uniqueness``'s default, unchunked
    dispatch path; ``test_chunk_filters_temporal_*`` below separately verify
    the chunking mechanism itself preserves the same correctness.
    """
    resource = _temporal_pk_resource()
    lf = pl.LazyFrame(
        {
            "event_date": [
                date(2020, 1, 1),
                date(2020, 1, 2),
                date(2021, 1, 1),
                date(2021, 1, 2),
            ],
            # unit_id repeats across years -- fine, since event_date differs.
            "unit_id": [1, 2, 1, 2],
        }
    )
    errors = _validate_primary_key_uniqueness(lf, resource)
    assert errors == []


def test_validate_primary_key_uniqueness_temporal_catches_duplicate() -> None:
    """A true duplicate composite key within one year is still caught."""
    resource = _temporal_pk_resource()
    lf = pl.LazyFrame(
        {
            "event_date": [date(2020, 1, 1), date(2020, 1, 1), date(2021, 1, 1)],
            "unit_id": [1, 1, 1],
        }
    )
    errors = _validate_primary_key_uniqueness(lf, resource)
    assert len(errors) > 0


def test_validate_primary_key_uniqueness_categorical_valid() -> None:
    """Repeated non-key values across different filers are not false positives."""
    resource = _categorical_pk_resource()
    lf = pl.LazyFrame(
        {
            "filer_id": ["A", "A", "B", "B"],
            # record_id repeats across filers -- fine, since filer_id differs.
            "record_id": [1, 2, 1, 2],
        }
    )
    errors = _validate_primary_key_uniqueness(lf, resource)
    assert errors == []


def test_validate_primary_key_uniqueness_categorical_catches_duplicate() -> None:
    """A true duplicate composite key within one filer is still caught."""
    resource = _categorical_pk_resource()
    lf = pl.LazyFrame(
        {
            "filer_id": ["A", "A", "B"],
            "record_id": [1, 1, 1],
        }
    )
    errors = _validate_primary_key_uniqueness(lf, resource)
    assert len(errors) > 0


def test_chunk_filters_temporal_partitions_by_year() -> None:
    """Temporal chunk filters split rows into non-overlapping, year-aligned sets."""
    lf = pl.LazyFrame(
        {"event_date": [date(2020, 1, 1), date(2020, 6, 1), date(2021, 3, 1)]}
    )
    filters = _chunk_filters(lf, "event_date")
    assert len(filters) == 2  # 2020 and 2021
    row_counts = sorted(lf.filter(f).select(pl.len()).collect().item() for f in filters)
    assert row_counts == [1, 2]


def test_chunk_filters_categorical_partitions_by_value() -> None:
    """Categorical chunk filters split rows by each distinct value, exactly."""
    lf = pl.LazyFrame({"filer_id": ["A", "A", "B"]})
    filters = _chunk_filters(lf, "filer_id")
    assert len(filters) == 2  # "A" and "B"
    counts_by_value = {}
    for f in filters:
        chunk = lf.filter(f).collect()
        counts_by_value[chunk["filer_id"][0]] = chunk.height
    assert counts_by_value == {"A": 2, "B": 1}


def test_validate_primary_key_uniqueness_no_primary_key() -> None:
    """Resources without a primary key are trivially valid."""
    resource = Resource(
        name="_test__no_pk",
        description="Synthetic resource without a primary key.",
        schema={"fields": [{"name": "x", "type": "integer", "description": "X."}]},
    )
    lf = pl.LazyFrame({"x": [1, 1, 1]})
    assert _validate_primary_key_uniqueness(lf, resource) == []


def test_validate_primary_key_uniqueness_rejects_bad_chunk_field(mocker) -> None:
    """A chunk field that isn't part of the primary key is a config error.

    Chunking is only exact (see `_chunk_filters`) when the chunk column is
    part of the primary key -- if `_CHUNKED_PK_CHECK_TABLES` is ever
    misconfigured with a column that isn't, that has to fail loudly rather
    than silently produce an incomplete uniqueness check.
    """
    resource = Resource(
        name="_test__bad_chunk_field",
        description="Synthetic resource for testing chunk-field misconfiguration.",
        schema={
            "fields": [
                {"name": "id", "type": "integer", "description": "Primary key."},
                {"name": "other", "type": "integer", "description": "Not in the PK."},
            ],
            "primary_key": ["id"],
        },
    )
    mocker.patch.dict(_CHUNKED_PK_CHECK_TABLES, {resource.name: "other"})
    lf = pl.LazyFrame({"id": [1, 2], "other": [1, 2]})
    with pytest.raises(AssertionError, match="isn't part of"):
        _validate_primary_key_uniqueness(lf, resource)


def test_find_duplicate_primary_keys() -> None:
    """The underlying group-by/count reduction correctly identifies duplicates."""
    lf = pl.LazyFrame({"a": [1, 1, 2], "b": ["x", "x", "y"]})
    duplicates = _find_duplicate_primary_keys(lf, ["a", "b"])
    assert duplicates.height == 1
    assert duplicates["a"][0] == 1
    assert duplicates["b"][0] == "x"
    assert duplicates["_pk_count"][0] == 2


def test_chunked_pk_check_tables_are_valid() -> None:
    """Every entry in _CHUNKED_PK_CHECK_TABLES must name a real, matching field.

    Guards against the mapping silently going stale if a listed resource's
    primary key changes -- the chunking column must remain part of the
    primary key, since that's what makes chunking by it exact rather than
    approximate.
    """
    for resource_name, field_name in _CHUNKED_PK_CHECK_TABLES.items():
        resource = PUDL_PACKAGE.get_resource(resource_name)
        assert field_name in resource.schema.primary_key, (
            f"{field_name} is not part of {resource_name}'s primary key"
        )


def test_pandera_schema_check_combines_schema_and_content_errors() -> None:
    """Schema-level and content-level errors are reported together, not either/or.

    Regression test: a naive implementation runs pandera's schema-only pass
    first and lets a `SchemaErrors` from it propagate immediately, which
    skips the content checks entirely -- so a caller only ever sees whichever
    kind of error happened to be found first, and has to fix-and-rerun to
    discover the other. The two are independent, so both should show up in
    one report.
    """
    resource = Resource(
        name="_test__combined_errors",
        description="Synthetic resource with both a missing column and a "
        "content-constraint violation.",
        schema={
            "fields": [
                {"name": "id", "type": "integer", "description": "Primary key."},
                {
                    "name": "value",
                    "type": "integer",
                    "description": "Bounded value.",
                    "constraints": {"minimum": 0, "maximum": 100},
                },
                {
                    "name": "missing_field",
                    "type": "string",
                    "description": "A required field absent from the data.",
                    "constraints": {"required": True},
                },
            ],
            "primary_key": ["id"],
        },
    )
    package = Package(name="_test_package", resources=[resource])
    check = asset_check_from_schema(
        dg.AssetKey([resource.name]), package, duckdb_asset=False
    )
    assert check is not None
    fn = check.node_def.compute_fn.decorated_fn

    # `missing_field` is omitted entirely (schema error: column not present),
    # and `value=150` violates its maximum=100 constraint (content error).
    lf = pl.LazyFrame({"id": [1, 2], "value": [10, 150]})
    result = fn(lf)

    assert result.passed is False
    detailed_errors = result.metadata["detailed_errors"].data
    messages = [e["error_message"] for e in detailed_errors]
    assert any("missing_field" in m for m in messages), (
        f"Expected a missing-column schema error, got: {messages}"
    )
    assert any("value" in m and "100" in m for m in messages), (
        f"Expected a value-constraint content error, got: {messages}"
    )
