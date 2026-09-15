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
from types import SimpleNamespace

import dagster as dg
import geopandas as gpd  # noqa: ICN002
import pandas as pd
import pint
import polars as pl
import pytest
from dagster._core.definitions.asset_checks.asset_checks_definition import (
    AssetChecksDefinition,
)
from dagster._core.definitions.decorators.op_decorator import DecoratedOpFunction
from shapely.geometry import Point

from pudl.dagster.asset_checks import (
    _build_registry_from_descriptor,
    _validate_datapackage_unit_strings,
    asset_check_from_schema,
    group_mean_continuity_check,
    summarize_check_failures,
)
from pudl.helpers import ParquetData
from pudl.metadata.classes import PUDL_PACKAGE, Package, Resource
from pudl.metadata.units import PUDL_UNIT_DEFINITIONS


@pytest.mark.parametrize(
    ("asset_name", "duckdb_asset", "expected_type"),
    [
        ("core_pudl__codes_subdivisions", False, pl.LazyFrame),
        ("core_ferceqr__contracts", True, ParquetData),
        ("out_censusdp1tract__counties", False, gpd.GeoDataFrame),
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
    node_def = check.node_def
    assert isinstance(node_def, dg.OpDefinition)
    compute_fn = node_def.compute_fn
    assert isinstance(compute_fn, DecoratedOpFunction)
    assert compute_fn.decorated_fn.__annotations__["asset_value"] is expected_type


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


def _build_check_fn(name: str, fields: list[dict], primary_key: list[str]):
    """Build the generated ``pandera_schema_check`` function for a synthetic resource.

    Shared by every synthetic-resource asset-check test below, so each one only
    has to supply what actually varies: the resource name, its fields, and its
    primary key.
    """
    resource = Resource(
        name=name,
        description="Synthetic resource for asset-check tests.",
        schema={"fields": fields, "primary_key": primary_key},
    )
    package = Package(name="_test_package", resources=[resource])
    check = asset_check_from_schema(
        dg.AssetKey([resource.name]), package, duckdb_asset=False
    )
    assert check is not None
    node_def = check.node_def
    assert isinstance(node_def, dg.OpDefinition)
    # compute_fn is typed as Callable | DecoratedOpFunction; the @asset_check
    # decorator always produces the latter, but DecoratedOpFunction is a
    # private dagster type we can't import to narrow against.
    return node_def.compute_fn.decorated_fn  # type: ignore[missing-attribute]


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


def test_polars_lazyframe_content_checks_valid() -> None:
    """Content-valid data should pass the generated asset check."""
    fn = _build_check_fn(
        "_test__polars_content_checks",
        _content_check_fields(with_geometry=False),
        ["id"],
    )
    lf = pl.LazyFrame({"id": [1, 2, 3], "value": [10, 20, 30], "code": ["a", "b", "a"]})
    lf = lf.with_columns(pl.col("code").cast(pl.Categorical))
    result = fn(lf)
    assert result.passed is True
    assert "unexpected_error" not in result.metadata


@pytest.mark.parametrize(
    ("value", "code", "expected_metadata_error"),
    [
        (150, "a", "'value': 150"),  # violates maximum=100
        (30, "z", "'code': 'z'"),  # violates enum=["a", "b", "c"]
    ],
    ids=["value_out_of_range", "code_not_in_enum"],
)
def test_polars_lazyframe_content_checks_errors(
    value, code, expected_metadata_error
) -> None:
    """Content (not just schema) violations should fail with informative detail.

    Checks the actual failure-case content in ``detailed_errors``, not just
    ``passed=False`` -- that's what would catch a regression where the check
    fails for the right row but reports the wrong column, or a generic/empty
    error in place of the real failure case.

    Regression guard: a failure must also go through the clean SchemaErrors path
    (populating ``detailed_errors``), not fall through to the generic exception
    handler -- see ``Resource._duplicate_primary_key_error``'s history.
    """
    fn = _build_check_fn(
        "_test__polars_content_checks",
        _content_check_fields(with_geometry=False),
        ["id"],
    )
    lf = pl.LazyFrame(
        {"id": [1, 2, 3], "value": [10, 20, value], "code": ["a", "b", code]}
    )
    lf = lf.with_columns(pl.col("code").cast(pl.Categorical))
    result = fn(lf)

    assert result.passed is False
    assert "unexpected_error" not in result.metadata
    messages = [e["error_message"] for e in result.metadata["detailed_errors"].data]
    assert any(expected_metadata_error in m for m in messages), (
        f"Expected {expected_metadata_error!r} in one of: {messages}"
    )


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

    All errors must produce SchemaErrors, not generic exceptions.
    """
    fn = _build_check_fn(
        "_test__geopandas_content_checks",
        _content_check_fields(with_geometry=True),
        ["id"],
    )
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
    assert "unexpected_error" not in result.metadata


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


_UNIQUENESS_BASE_DATA = {
    "id": [1, 2, 3],
    "id2": [1, 2, 3],
    "required_field": [1, 2, 3],
    "unique_field": [1, 2, 3],
}
"""Happy-path column data for the uniqueness-check fixtures below.

Every error case is built from this via :func:`_uniqueness_data`, overriding only
the column(s) it actually violates.
"""


def _uniqueness_data(**overrides: list) -> dict[str, list]:
    """Override columns of :data:`_UNIQUENESS_BASE_DATA` to build a test case."""
    return {**_UNIQUENESS_BASE_DATA, **overrides}


def test_polars_lazyframe_uniqueness_checks_valid() -> None:
    """Nullable- and uniqueness-valid data should pass the generated asset check."""
    fn = _build_check_fn(
        "_test__polars_uniqueness_checks",
        _uniqueness_check_fields(with_geometry=False),
        ["id", "id2"],
    )
    result = fn(pl.LazyFrame(_UNIQUENESS_BASE_DATA))
    assert result.passed is True
    assert "unexpected_error" not in result.metadata


_UNIQUENESS_ERROR_CASES = [
    (
        _uniqueness_data(required_field=[1, None, 3]),
        "non-nullable column 'required_field'",
    ),
    (_uniqueness_data(unique_field=[1, 1, 3]), "column 'unique_field' not unique"),
    (
        _uniqueness_data(id=[1, 1, 3], id2=[1, 1, 3]),
        "('id', 'id2') not unique",
    ),
]
_UNIQUENESS_ERROR_IDS = [
    "null_in_required_field",
    "duplicate_unique_field",
    "duplicate_primary_key",
]


@pytest.mark.parametrize(
    ("data", "expected_metadata_error"),
    _UNIQUENESS_ERROR_CASES,
    ids=_UNIQUENESS_ERROR_IDS,
)
def test_polars_lazyframe_uniqueness_checks_errors(
    data, expected_metadata_error
) -> None:
    """Nullable and uniqueness violations should fail with informative detail.

    This resource's schema doesn't set ``pk_check_chunk_field``, so composite primary-key
    uniqueness goes through :meth:`Resource.check_primary_key_polars`'s normal,
    unchunked path (see ``tests/unit/metadata/metadata_test.py`` for the chunked path
    used by PUDL's few oversized tables).

    Checks the actual failure content in ``detailed_errors``, not just
    ``passed=False`` -- that's what would catch a regression where the check fails
    for the right row but reports the wrong column or constraint.

    Regression guard: in particular, the "duplicate_primary_key" case must fail via
    the clean SchemaErrors path, not the generic exception handler -- see
    ``Resource._duplicate_primary_key_error``'s history.
    """
    fn = _build_check_fn(
        "_test__polars_uniqueness_checks",
        _uniqueness_check_fields(with_geometry=False),
        ["id", "id2"],
    )
    result = fn(pl.LazyFrame(data))

    assert result.passed is False
    assert "unexpected_error" not in result.metadata
    messages = [e["error_message"] for e in result.metadata["detailed_errors"].data]
    assert any(expected_metadata_error in m for m in messages), (
        f"Expected {expected_metadata_error!r} in one of: {messages}"
    )


def _uniqueness_geodataframe(data: dict[str, list]) -> gpd.GeoDataFrame:
    """Build the GeoDataFrame counterpart of a :func:`_uniqueness_data` case."""
    return gpd.GeoDataFrame(
        {
            "id": pd.array(data["id"], dtype="Int64"),
            "id2": pd.array(data["id2"], dtype="Int64"),
            "required_field": pd.array(data["required_field"], dtype="Int64"),
            "unique_field": pd.array(data["unique_field"], dtype="Int64"),
            "geometry": [Point(0, 0), Point(1, 1), Point(2, 2)],
        }
    )


def test_geopandas_uniqueness_checks_valid() -> None:
    """Nullable- and uniqueness-valid data should pass the generated asset check."""
    fn = _build_check_fn(
        "_test__geopandas_uniqueness_checks",
        _uniqueness_check_fields(with_geometry=True),
        ["id", "id2"],
    )
    result = fn(_uniqueness_geodataframe(_UNIQUENESS_BASE_DATA))
    assert result.passed is True
    assert "unexpected_error" not in result.metadata


_GEOPANDAS_UNIQUENESS_ERROR_CASES = [
    (
        _uniqueness_data(required_field=[1, None, 3]),
        "'required_field' contains null values",
    ),
    (_uniqueness_data(unique_field=[1, 1, 3]), "'unique_field' contains duplicate"),
    (
        _uniqueness_data(id=[1, 1, 3], id2=[1, 1, 3]),
        "('id', 'id2')' not unique",
    ),
]


@pytest.mark.parametrize(
    ("data", "expected_metadata_error"),
    _GEOPANDAS_UNIQUENESS_ERROR_CASES,
    ids=_UNIQUENESS_ERROR_IDS,
)
def test_geopandas_uniqueness_checks_errors(data, expected_metadata_error) -> None:
    """Nullable and uniqueness violations are already correctly caught on the
    geopandas/pandas path, with informative detail.

    Checks the actual failure content in ``detailed_errors``, not just
    ``passed=False`` -- see the equivalent polars test for why. The pandas
    backend's error phrasing ("series ... contains duplicate values") differs
    from the polars backend's ("column ... not unique"), so these expected
    substrings are backend-specific even though the underlying cases are the
    same as :func:`test_polars_lazyframe_uniqueness_checks_errors`.
    """
    fn = _build_check_fn(
        "_test__geopandas_uniqueness_checks",
        _uniqueness_check_fields(with_geometry=True),
        ["id", "id2"],
    )
    result = fn(_uniqueness_geodataframe(data))

    assert result.passed is False
    assert "unexpected_error" not in result.metadata
    messages = [e["error_message"] for e in result.metadata["detailed_errors"].data]
    assert any(expected_metadata_error in m for m in messages), (
        f"Expected {expected_metadata_error!r} in one of: {messages}"
    )


def test_pandera_schema_check_combines_schema_and_content_errors() -> None:
    """Schema-level and content-level errors are reported together, not either/or.

    Check that we don't let `SchemaErrors` from schema-only checks propagate
    immediately, skipping the content checks entirely, meaning the caller only ever sees
    whichever kind of error happened first, requiring them to to fix-and-rerun to
    discover the other family of errors. The two are independent, so both should show up
    in one error report.
    """
    fn = _build_check_fn(
        "_test__combined_errors",
        [
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
        ["id"],
    )

    # `missing_field` is omitted entirely (schema error: column not present),
    # and `value=150` violates its maximum=100 constraint (content error).
    lf = pl.LazyFrame({"id": [1, 2], "value": [10, 150]})
    result = fn(lf)

    assert result.passed is False
    assert "unexpected_error" not in result.metadata
    detailed_errors = result.metadata["detailed_errors"].data
    messages = [e["error_message"] for e in detailed_errors]
    assert any("missing_field" in m for m in messages), (
        f"Expected a missing-column schema error, got: {messages}"
    )
    assert any("value" in m and "100" in m for m in messages), (
        f"Expected a value-constraint content error, got: {messages}"
    )


@pytest.mark.parametrize(
    ("passed", "metadata", "expected"),
    [
        (True, {}, ""),
        (False, {}, "FAILED (no detail available)"),
        (
            False,
            {
                "detailed_errors": dg.MetadataValue.json(
                    [
                        {
                            "check": "multiple_fields_uniqueness",
                            "error_message": "2 duplicate combinations",
                            "failure_case_count": 2,
                        }
                    ]
                )
            },
            "2x multiple_fields_uniqueness (2 duplicate combinations)",
        ),
    ],
)
def test_summarize_check_failures(passed, metadata, expected):
    """A passing check summarizes to "", a failing one names the failure(s)."""
    # SimpleNamespace stands in for AssetCheckEvaluation, which only needs to
    # supply the two attributes summarize_check_failures actually reads.
    evaluation = SimpleNamespace(passed=passed, metadata=metadata)
    assert summarize_check_failures(evaluation) == expected  # type: ignore[bad-argument-type]
