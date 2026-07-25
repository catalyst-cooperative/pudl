"""Programmatically defined Dagster asset checks for PUDL.

This module should contain Dagster asset-check definitions and helper functions that
evaluate the quality or structural correctness of already-materialized assets. Put
checks here when they belong in the Dagster asset graph and should run as blocking or
reporting validations attached to specific assets, especially when they can be derived
from metadata or shared validation patterns. Keep business transformations and dbt-only
data tests out of this module so it remains focused on Dagster-native asset validation.

For the underlying Dagster concept see https://docs.dagster.io/guides/test/asset-checks

For data validation we almost entirely rely on :mod:`dbt` data tests defined using SQL
and executed across our Parquet outputs using :mod:`duckdb`.

We primarily use Dagster asset checks to validate the schemas of PUDL tables throughout
the pipeline. We use :mod:`pandera` to programmatically define dataframe schemas based
on the PUDL metadata with the asset check factory :func:`asset_check_from_schema`
defined below. A handful of asset checks that were particularly difficult to translate
to SQL/dbt data tests are also defined here, but in general all data validation tests
should go in dbt.
"""

import datetime
import itertools
import json
from typing import Any

import dagster as dg
import frictionless
import geopandas as gpd  # noqa: ICN002
import pandas as pd
import pandera.pandas as pr_pandas
import pandera.polars as pr_polars
import pint
import polars as pl
from pandera.config import ValidationDepth, config_context
from pandera.errors import SchemaError, SchemaErrorReason, SchemaErrors

from pudl.dagster.assets import all_asset_modules, asset_keys
from pudl.dagster.partitions import ferceqr_year_quarters
from pudl.helpers import ParquetData, get_parquet_table_polars
from pudl.metadata.classes import PUDL_PACKAGE, Field, Package, Resource


def _collect_asset_metadata(asset_value) -> dict[str, Any]:
    """Collect basic metadata about the asset."""
    if isinstance(asset_value, pl.LazyFrame):
        shape = (
            asset_value.select(pl.len()).collect(engine="streaming").item(),
            asset_value.collect_schema().len(),
        )
    else:
        shape = asset_value.shape
    return {
        "asset_type": str(type(asset_value)),
        "asset_shape": list(shape),
    }


def _extract_actual_columns_and_dtypes(
    asset_value: pl.LazyFrame | pd.DataFrame,
) -> tuple[list[str], dict[str, str], bool]:
    """Extract actual column names and dtypes from supported dataframe objects."""
    use_pandas_backend = False

    if isinstance(asset_value, pl.LazyFrame):
        schema = asset_value.collect_schema()
        actual_columns = schema.names()
        actual_dtypes = {
            col: str(dtype)
            for col, dtype in zip(actual_columns, schema.dtypes(), strict=True)
        }
        return actual_columns, actual_dtypes, use_pandas_backend

    if isinstance(asset_value, pd.DataFrame):
        use_pandas_backend = True
        actual_columns = list(asset_value.columns)
        actual_dtypes = {
            str(col): str(dtype) for col, dtype in asset_value.dtypes.items()
        }
        return actual_columns, actual_dtypes, use_pandas_backend

    raise ValueError(
        f"Unsupported asset type for dtype collection: {type(asset_value)}"
    )


def _collect_dtype_metadata(
    asset_value: pl.LazyFrame | pd.DataFrame,
    resource: Resource,
) -> dict[str, Any]:
    """Build metadata comparing actual dataframe dtypes to metadata-driven expectations.

    Args:
        asset_value: Asset output to introspect. Supported types are
            :class:`pandas.DataFrame` and :class:`polars.LazyFrame`.
        resource: PUDL metadata resource whose schema fields define expected columns and
            dtypes.

    Returns:
        A metadata dictionary with:
        - ``field_details``: per-column expected and actual dtype details.
        - ``column_comparison``: expected/actual column counts and optional missing
          or extra column lists.
        - ``type_mismatches``: only present when common columns have differing dtype
          strings.

    Raises:
        ValueError: If ``asset_value`` is not a supported dataframe type.

    Notes:
        Expected dtypes are captured as strings from ``field.to_pandera_column()``.
        Any errors while computing expected dtypes are recorded inline as
        ``"Error: ..."`` values rather than raised.
    """
    dtype_errors: dict[str, str] = {}
    actual_columns, actual_dtypes, use_pandas_backend = (
        _extract_actual_columns_and_dtypes(asset_value)
    )

    expected_columns = [field.name for field in resource.schema.fields]

    pandera_dtypes = {}
    for field in resource.schema.fields:
        try:
            pandera_dtypes[field.name] = str(
                field.to_pandera_column(use_pandas_backend=use_pandas_backend).dtype
            )
        except Exception as exc:
            error_text = str(exc)
            pandera_dtypes[field.name] = f"Error: {error_text}"
            dtype_errors[field.name] = error_text

    field_details = {
        field.name: {
            "pudl_field_dtype": field.type,
            "expected_pandera_dtype": pandera_dtypes.get(field.name, "Unknown"),
            "actual_dtype": actual_dtypes.get(field.name, "Column not present"),
        }
        for field in resource.schema.fields
    }

    missing_columns = sorted(set(expected_columns) - set(actual_columns))
    extra_columns = sorted(set(actual_columns) - set(expected_columns))
    column_comparison: dict[str, Any] = {
        "expected_count": len(expected_columns),
        "actual_count": len(actual_columns),
    }
    if missing_columns:
        column_comparison["missing_columns"] = missing_columns
    if extra_columns:
        column_comparison["extra_columns"] = extra_columns

    common_columns = sorted(set(expected_columns) & set(actual_columns))
    type_mismatches = {}
    for column in common_columns:
        expected_type = pandera_dtypes.get(column, "Unknown")
        actual_type = actual_dtypes.get(column, "Unknown")
        if expected_type != actual_type and expected_type != "Unknown":
            type_mismatches[column] = {
                "expected": expected_type,
                "actual": actual_type,
            }

    metadata = {
        "field_details": field_details,
        "column_comparison": column_comparison,
    }
    if dtype_errors:
        metadata["expected_dtype_errors"] = dtype_errors
    if type_mismatches:
        metadata["type_mismatches"] = type_mismatches

    return metadata


def _collect_geometry_metadata(asset_value) -> dict[str, Any]:
    """Collect GeoPandas-specific metadata."""
    if not isinstance(asset_value, gpd.GeoDataFrame):
        return {}

    metadata = {
        "geometry_column": (
            asset_value.geometry.name
            if hasattr(asset_value, "geometry")
            else "No geometry attribute"
        )
    }

    if hasattr(asset_value, "geometry") and hasattr(asset_value.geometry, "dtype"):
        metadata["geometry_dtype"] = str(asset_value.geometry.dtype)

    return metadata


def _process_schema_errors(schema_errors: SchemaErrors) -> dict[str, Any]:
    """Process Pandera schema errors into structured metadata."""
    detailed_errors = []

    for err in schema_errors.schema_errors:
        error_info = {
            "error_type": type(err).__name__,
            "error_message": str(err),
            "failure_cases": str(err.failure_cases)
            if hasattr(err, "failure_cases")
            else "No failure_cases",
            "data": str(err.data) if hasattr(err, "data") else "No data",
        }

        # Add optional error attributes
        for attr in ["schema", "check", "args"]:
            if hasattr(err, attr):
                error_info[f"{attr}_info"] = str(getattr(err, attr))

        detailed_errors.append(error_info)

    return {
        "detailed_errors": detailed_errors,
        "num_errors": len(schema_errors.schema_errors),
    }


def group_mean_continuity_check(
    df: pd.DataFrame,
    thresholds: dict[str, float],
    groupby_col: str,
    n_outliers_allowed: int = 0,
) -> dg.AssetCheckResult:
    """Check that certain variables don't vary too much on average between groups.

    Groups and sorts the data by ``groupby_col``, then takes the mean across
    each group. Useful for saying something like "the average water usage of
    cooling systems didn't jump by 10x from 2012-2013."

    Args:
        df: the df with the actual data
        thresholds: a mapping from column names to the ratio by which those
            columns are allowed to fluctuate from one group to the next.
        groupby_col: the column by which we will group the data.
        n_outliers_allowed: how many data points are allowed to be above the
            threshold.
    """
    pct_change = (
        df.loc[:, [groupby_col] + list(thresholds.keys())]
        .groupby(groupby_col, sort=True)
        .mean()
        .pct_change()
        .abs()
        .dropna()
    )
    discontinuity = pct_change >= thresholds
    metadata = {
        col: {
            "top5": list(pct_change[col][discontinuity[col]].nlargest(n=5)),
            "threshold": thresholds[col],
        }
        for col in thresholds
        if discontinuity[col].sum() > 0
    }
    if (discontinuity.sum() > n_outliers_allowed).any():
        return dg.AssetCheckResult(passed=False, metadata=metadata)

    return dg.AssetCheckResult(passed=True, metadata=metadata)


def _field_has_content_constraints(field: Field) -> bool:
    """Whether a field's constraints require reading its actual values to check."""
    c = field.constraints
    return any(
        [
            c.required,
            c.unique,
            c.minimum is not None,
            c.maximum is not None,
            c.min_length is not None,
            c.max_length is not None,
            c.pattern is not None,
            bool(c.enum),
        ]
    )


# Tables large enough that checking composite primary-key uniqueness in a
# single pass is worth chunking (see NOTE in `_validate_primary_key_uniqueness`
# and polars-comment.md for the memory measurements that motivated this). Maps
# resource name to a primary-key field to chunk by: a date/datetime field
# chunks by year; anything else chunks by each of its distinct values. Either
# way, the column must be part of the primary key -- see
# `_chunk_filters` for why that's what makes chunking exact rather than
# approximate.
#
# `core_ferceqr__transactions` is deliberately NOT listed here: its metadata
# currently declares no primary key at all (see `additional_primary_key_text`
# in `pudl.metadata.resources.ferceqr`), since real duplicate records were
# found for the columns that would otherwise form one -- see
# polars-comment.md and the PUDL issue tracking that investigation. Once that
# investigation lands on a real, enforceable primary key, add it back here if
# the table still needs chunking at that point.
_CHUNKED_PK_CHECK_TABLES: dict[str, str] = {
    "core_epacems__hourly_emissions": "operating_datetime_utc",
    "out_vcerare__hourly_available_capacity_factor": "datetime_utc",
}


def _find_duplicate_primary_keys(
    lf: pl.LazyFrame, primary_key: list[str]
) -> pl.DataFrame:
    """Return the primary-key combinations that appear more than once in ``lf``.

    Uses a single group-by/count/filter reduction, run with
    ``engine="streaming"``, rather than pandera's built-in composite-uniqueness
    check (`DataFrameSchema(unique=...)`), which collects the same LazyFrame
    up to three times and calls the eager, non-lazy `.is_duplicated()`. On our
    largest table's largest quarterly partition (228M rows) this measured at
    ~33GB / 3.4s, versus ~109GB / 152s for pandera's built-in check on the
    exact same data -- see polars-comment.md.
    """
    return (
        lf.group_by(primary_key)
        .agg(pl.len().alias("_pk_count"))
        .filter(pl.col("_pk_count") > 1)
        .collect(engine="streaming")
    )


def _duplicate_primary_key_error(
    resource: Resource, primary_key: list[str], duplicates: pl.DataFrame
) -> SchemaError:
    """Build a SchemaError describing duplicate primary-key combinations."""
    return SchemaError(
        schema=None,
        data=duplicates,
        message=f"columns {tuple(primary_key)!r} not unique:\n{duplicates}",
        check="multiple_fields_uniqueness",
        reason_code=SchemaErrorReason.DUPLICATES,
        failure_cases=duplicates,
        column_name=resource.name,
    )


def _chunk_filters(lf: pl.LazyFrame, chunk_field: str) -> list[pl.Expr]:
    """Build filter expressions that partition ``lf`` by ``chunk_field``.

    Every filter is a literal comparison directly against ``chunk_field``'s
    own stored values -- a year-range comparison for date/datetime columns,
    an equality comparison against each distinct value otherwise -- never a
    derived expression. This matters for two reasons:

    1. Correctness: since ``chunk_field`` is required to be part of the
       primary key, two rows sharing a composite key necessarily share the
       same value of it, so a boundary drawn directly on that column's own
       value can never split a duplicate pair across chunks. Deriving the
       boundary from some other, only approximately-correlated column would
       not have this guarantee -- e.g. EPACEMS' separately stored `year`
       field reflects reporting period, not the UTC timestamp, and disagrees
       with `operating_datetime_utc.dt.year()` for hundreds of rows a year
       near timezone-shifted boundaries.
    2. Efficiency: a literal comparison against a stored column stays
       eligible for Parquet row-group pruning, since Polars can compare it
       directly to each row group's min/max statistics without decoding any
       data. A filter on a *derived* expression (e.g. `.dt.year() == y`)
       cannot be pushed down the same way, so Polars must read and decode
       every row group on every chunk iteration regardless of whether that
       chunk's rows are actually present. Measured on our billion-row table:
       ~0.02s to select one year's rows via a literal range filter, vs ~0.56s
       via a `.dt.year()` filter for the same result.

    PUDL's long time-series tables are written and physically stored in
    temporal (or, for `core_ferceqr__transactions`, filer) order, so this
    pruning is not theoretical: EPACEMS' row groups are each confined to a
    single calendar year, and most of `core_ferceqr__transactions`' row
    groups are confined to a single seller.
    """
    dtype = lf.select(chunk_field).collect_schema()[chunk_field]

    if dtype in (pl.Date, pl.Datetime) or isinstance(dtype, pl.Datetime):
        bounds = lf.select(
            pl.col(chunk_field).min().alias("_min"),
            pl.col(chunk_field).max().alias("_max"),
        ).collect(engine="streaming")
        min_year = bounds["_min"][0].year
        max_year = bounds["_max"][0].year
        boundary = datetime.date if dtype == pl.Date else datetime.datetime
        return [
            pl.col(chunk_field).is_between(
                boundary(year, 1, 1), boundary(year + 1, 1, 1), closed="left"
            )
            for year in range(min_year, max_year + 1)
        ]

    values = (
        lf.select(chunk_field)
        .unique()
        .collect(engine="streaming")
        .get_column(chunk_field)
        .to_list()
    )
    return [pl.col(chunk_field) == value for value in values]


def _validate_primary_key_uniqueness(
    asset_value: pl.LazyFrame, resource: Resource
) -> list[SchemaError]:
    """Validate composite primary-key uniqueness, chunked for oversized tables.

    Never uses pandera's built-in composite-uniqueness check (see
    `_find_duplicate_primary_keys` for why). For the handful of tables large
    enough to matter (`_CHUNKED_PK_CHECK_TABLES`), this checks uniqueness one
    chunk at a time instead of all at once, which is exact -- not an
    approximation -- as long as the chunking column is part of the primary
    key (see `_chunk_filters`). Every other table just runs the check once
    on the whole (already narrow, primary-key-only) LazyFrame, which is fine
    at their scale.
    """
    primary_key = resource.schema.primary_key
    if not primary_key:
        return []

    narrow = asset_value.select(primary_key)
    chunk_field = _CHUNKED_PK_CHECK_TABLES.get(resource.name)
    if chunk_field:
        assert chunk_field in primary_key, (
            f"_CHUNKED_PK_CHECK_TABLES lists {chunk_field!r} as the chunk column "
            f"for {resource.name!r}, but it isn't part of that resource's "
            f"primary key ({primary_key!r}). Chunking is only exact when the "
            "chunk column is part of the primary key -- see _chunk_filters."
        )
        chunks = [narrow.filter(f) for f in _chunk_filters(narrow, chunk_field)]
    else:
        chunks = [narrow]

    errors: list[SchemaError] = []
    for chunk in chunks:
        duplicates = _find_duplicate_primary_keys(chunk, primary_key)
        if duplicates.height > 0:
            errors.append(
                _duplicate_primary_key_error(resource, primary_key, duplicates)
            )
    return errors


def _validate_polars_content(
    asset_value: pl.LazyFrame, resource: Resource
) -> list[SchemaError]:
    """Validate per-column value constraints and primary-key uniqueness.

    Pandera's Polars backend only runs Check/nullable/unique validation
    (as opposed to just column presence and dtype validation) when the
    validation depth is explicitly forced to ``SCHEMA_AND_DATA``, and even
    then it does so by repeatedly collecting the *entire* dataframe passed to
    it -- once per check. For PUDL's largest tables (hundreds of millions to
    billions of rows) collecting the full table is not tractable, even though
    only a handful of columns actually carry constraints worth checking.

    Instead, this validates one narrow slice at a time: for each field with an
    actual constraint, ``.select()`` just that column before collecting.
    Columns with no constraints at all are never read. This scales uniformly
    regardless of table size, since a single narrow column is always small
    enough to collect, even when the full table is not -- measured at ~1.2GB
    peak memory total for all four checked columns on our billion-row table.
    Composite primary-key uniqueness is handled separately -- see
    `_validate_primary_key_uniqueness`.

    Columns that are entirely missing from ``asset_value`` are skipped here
    rather than selected: ``.select()`` on a genuinely absent column raises
    immediately (a raw ``pl.exceptions.ColumnNotFoundError``, not a pandera
    ``SchemaError``), and a missing column is already reported separately by
    the schema-level check in ``pandera_schema_check``.
    """
    errors: list[SchemaError] = []
    present_columns = set(asset_value.collect_schema().names())

    with config_context(validation_depth=ValidationDepth.SCHEMA_AND_DATA):
        for field in resource.schema.fields:
            if field.name not in present_columns:
                continue
            if not _field_has_content_constraints(field):
                continue
            column_schema = pr_polars.DataFrameSchema(
                {field.name: field.to_pandera_column(use_pandas_backend=False)}
            )
            try:
                column_schema.validate(asset_value.select(field.name), lazy=True)
            except SchemaErrors as schema_errors:
                errors.extend(schema_errors.schema_errors)

        primary_key = resource.schema.primary_key
        if primary_key and all(col in present_columns for col in primary_key):
            errors.extend(_validate_primary_key_uniqueness(asset_value, resource))

    return errors


def asset_check_from_schema(  # noqa: C901
    asset_key: dg.AssetKey,
    package: Package,
    duckdb_asset: bool,
) -> dg.AssetChecksDefinition | None:
    """Create a Dagster asset check based on the resource schema, if defined.

    The vast majority of assets will be loaded as Polars LazyFrames directly using
    the ``PudlParquetIOManager`` and validated with Pandera's Polars backend, but
    there are two exceptions to this. The first exception are assets which contain
    a geometry data type. These assets will all be loaded as geopandas GeoDataFrames
    and use Pandera's Pandas backend as Polars does not support geometry data types.
    The second exception are assets produced entirely using DuckDB. These assets
    return ``ParquetData`` objects, which are handled by the default io-manager. In
    this case, the resulting parquet file(s) will be scanned with Polars to produce
    a LazyFrame, then handled exactly the same as a typical asset.
    """
    resource_id = asset_key.to_user_string()
    try:
        resource = package.get_resource(resource_id)
    except ValueError:
        return None

    pandera_schema = resource.schema.to_pandera()
    partitions = ferceqr_year_quarters if "ferceqr" in resource_id else None
    if duckdb_asset:
        asset_type = ParquetData
    elif isinstance(pandera_schema, pr_polars.DataFrameSchema):
        asset_type = pl.LazyFrame
    elif isinstance(pandera_schema, pr_pandas.DataFrameSchema):
        asset_type = gpd.GeoDataFrame
    else:
        raise ValueError(
            "Unexpected return type from `Resource.schema.to_pandera()`."
            f"Expected a pandera `DataFrameSchema`, but got: `{type(pandera_schema)}`"
        )

    @dg.asset_check(asset=asset_key, blocking=True, partitions_def=partitions)
    # Dagster uses this runtime annotation to select the correct IO manager load type,
    # but static type checkers may reject the computed local variable in a type expression.
    def pandera_schema_check(
        asset_value: asset_type,  # type: ignore[valid-type]
    ) -> dg.AssetCheckResult:
        if isinstance(asset_value, ParquetData):
            asset_value = get_parquet_table_polars(
                table_name=resource_id,
                partitions=asset_value.partitions,
            )

        # Collect all metadata
        metadata = (
            _collect_asset_metadata(asset_value)
            | _collect_dtype_metadata(asset_value, resource)
            | _collect_geometry_metadata(asset_value)
        )

        try:
            if isinstance(asset_value, pl.LazyFrame):
                # Column presence and dtypes are checked here, using only the
                # cheap `collect_schema()` metadata -- no data is read. Value
                # constraints (ranges, enums, uniqueness, etc.) are checked
                # separately below, one narrow column at a time, so that even
                # PUDL's largest tables remain tractable to validate. Errors
                # from both passes are collected and reported together,
                # rather than stopping at whichever one fails first, since
                # they're independent and a caller fixing one shouldn't have
                # to re-run the check to discover the other.
                assert isinstance(pandera_schema, pr_polars.DataFrameSchema)
                errors: list[SchemaError] = []
                try:
                    pandera_schema.validate(asset_value, lazy=True)
                except SchemaErrors as schema_errors:
                    errors.extend(schema_errors.schema_errors)

                errors.extend(_validate_polars_content(asset_value, resource))

                if errors:
                    raise SchemaErrors(
                        schema=pandera_schema,
                        schema_errors=errors,
                        data=asset_value,
                    )
            else:
                assert isinstance(pandera_schema, pr_pandas.DataFrameSchema)
                pandera_schema.validate(asset_value, lazy=True)
            return dg.AssetCheckResult(passed=True, metadata=metadata)

        except SchemaErrors as schema_errors:
            metadata.update(_process_schema_errors(schema_errors))
            return dg.AssetCheckResult(passed=False, metadata=metadata)

        except Exception as exc:
            metadata["unexpected_error"] = {
                "error_type": type(exc).__name__,
                "error_message": str(exc),
                "error_args": str(exc.args) if hasattr(exc, "args") else "No args",
            }
            return dg.AssetCheckResult(passed=False, metadata=metadata)

    return pandera_schema_check


def _validate_datapackage_descriptor(descriptor: dict) -> list[str]:
    """Validate the descriptor against the frictionless spec; return error list.

    ``frictionless.Package.metadata_validate`` validates recursively through
    resources, schemas, and fields.  For certain structural errors (e.g. an
    unrecognised field type) it raises ``FrictionlessException`` rather than
    yielding; this wrapper ensures list[str] output whether an error
    occurs or not.
    """
    try:
        return [str(e) for e in frictionless.Package.metadata_validate(descriptor)]
    except frictionless.FrictionlessException as exc:
        return [str(exc)]


def valid_datapackage_check(
    asset_key: dg.AssetKey | str,
    *,
    description: str,
    blocking: bool = True,
) -> dg.AssetChecksDefinition:
    """Return a Dagster asset check that validates a frictionless datapackage descriptor.

    The check reads ``$PUDL_OUTPUT/parquet/datapackage.json`` from the injected
    ``pudl_paths`` Dagster resource at run time and validates it recursively through
    resources, schemas, and fields against the frictionless spec using
    :func:`frictionless.Package.metadata_validate`.

    Args:
        asset_key: Key of the asset that produces the datapackage descriptor.
        description: Human-readable description attached to the check in the
            Dagster UI.
        blocking: Whether the check is blocking (default ``True``).
    """

    @dg.asset_check(
        asset=asset_key,
        blocking=blocking,
        description=description,
        required_resource_keys={"pudl_paths"},
    )
    def _datapackage_check(
        context: dg.AssetCheckExecutionContext,
    ) -> dg.AssetCheckResult:
        descriptor_path = (
            context.resources.pudl_paths.parquet_path() / "datapackage.json"
        )
        descriptor = json.loads(descriptor_path.read_text())
        errors = _validate_datapackage_descriptor(descriptor)
        return dg.AssetCheckResult(
            passed=not errors,
            metadata={"errors": dg.MetadataValue.json(errors)},
        )

    return _datapackage_check


default_asset_checks = list(
    itertools.chain.from_iterable(
        dg.load_asset_checks_from_modules(modules)
        for modules in all_asset_modules.values()
    )
)

duckdb_assets = [
    "core_ferceqr__quarterly_identity",
    "core_ferceqr__contracts",
    "core_ferceqr__quarterly_index_pub",
    "core_ferceqr__transactions",
]

default_asset_checks += [
    check
    for check in (
        asset_check_from_schema(
            asset_key,
            PUDL_PACKAGE,
            duckdb_asset=asset_key.to_user_string() in duckdb_assets,
        )
        for asset_key in asset_keys
    )
    if check is not None
]

default_asset_checks.append(
    valid_datapackage_check(
        "pudl_datapackage",
        description=(
            "Validate the PUDL datapackage descriptor against the frictionless v2 spec. "
            "Checks structure recursively through resources, schemas, and fields."
        ),
    )
)


def _build_registry_from_descriptor(descriptor: dict) -> pint.UnitRegistry:
    """Build a Pint registry from the ``unit_registry`` embedded in a datapackage descriptor.

    Raises ``KeyError`` if the descriptor has no ``unit_registry`` field, or
    ``ValueError`` if the field is missing the expected ``definitions`` list.
    """
    unit_registry_meta = descriptor["unit_registry"]
    definitions = unit_registry_meta["definitions"]
    unit_registry = pint.UnitRegistry()
    for definition in definitions:
        unit_registry.define(definition)
    return unit_registry


def _validate_datapackage_unit_strings(descriptor: dict) -> list[str]:
    """Walk descriptor fields and parse each ``unit`` value; return error strings.

    Builds a Pint registry from the ``unit_registry`` field embedded in
    ``descriptor`` and uses it to parse every ``unit`` value found in resource
    field schemas.  Returns one error string per unparsable unit.
    """
    errors = []
    try:
        ureg = _build_registry_from_descriptor(descriptor)
    except (KeyError, ValueError) as exc:
        return [f"Could not build unit registry from descriptor: {exc}"]

    for resource in descriptor.get("resources", []):
        resource_name = resource.get("name", "<unnamed>")
        for field in resource.get("schema", {}).get("fields", []):
            unit = field.get("unit")
            if unit is None:
                continue
            try:
                ureg.parse_units(unit)
            except Exception as exc:
                field_name = field.get("name", "<unnamed>")
                errors.append(f"{resource_name}.{field_name}: unit={unit!r} — {exc}")
    return errors


def valid_datapackage_unit_strings_check(
    asset_key: dg.AssetKey | str,
    *,
    description: str,
    blocking: bool = True,
) -> dg.AssetChecksDefinition:
    """Return a Dagster asset check that validates unit strings in a datapackage descriptor.

    Reads the descriptor from ``$PUDL_OUTPUT/parquet/datapackage.json``, builds a
    Pint unit registry from the ``unit_registry`` field embedded in the descriptor,
    and attempts to parse every ``unit`` field value with that registry.  All
    failures are collected before the check reports so a single run surfaces every
    bad unit string.

    Args:
        asset_key: Key of the asset that produces the datapackage descriptor.
        description: Human-readable description attached to the check in the
            Dagster UI.
        blocking: Whether the check is blocking (default ``True``).
    """

    @dg.asset_check(
        asset=asset_key,
        blocking=blocking,
        description=description,
        required_resource_keys={"pudl_paths"},
    )
    def _unit_strings_check(
        context: dg.AssetCheckExecutionContext,
    ) -> dg.AssetCheckResult:
        descriptor_path = (
            context.resources.pudl_paths.parquet_path() / "datapackage.json"
        )
        descriptor = json.loads(descriptor_path.read_text())
        errors = _validate_datapackage_unit_strings(descriptor)
        return dg.AssetCheckResult(
            passed=not errors,
            metadata={"errors": dg.MetadataValue.json(errors)},
        )

    return _unit_strings_check


default_asset_checks.append(
    valid_datapackage_unit_strings_check(
        "pudl_datapackage",
        description=(
            "Validate that all unit strings in the PUDL datapackage descriptor "
            "are parseable using the unit definitions embedded in the descriptor."
        ),
    )
)

__all__ = [
    "valid_datapackage_check",
    "valid_datapackage_unit_strings_check",
    "asset_check_from_schema",
    "group_mean_continuity_check",
    "default_asset_checks",
    "duckdb_assets",
]
