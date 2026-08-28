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

import itertools
import json
import time
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
from pandera.errors import SchemaError, SchemaErrors

from pudl.dagster.assets import all_asset_modules, asset_keys
from pudl.dagster.assets.core.unmapped_ids import UNMAPPED_ID_ASSET_NAMES
from pudl.dagster.partitions import ferceqr_year_quarters
from pudl.helpers import ParquetData, get_parquet_table_polars
from pudl.metadata.classes import (
    PUDL_PACKAGE,
    Package,
    Resource,
)


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
        - ``field_details``: per-column expected/actual dtype details, declared
          constraints, and whether the column is content-checked (see
          :meth:`~pudl.metadata.classes.FieldConstraints.requires_content_validation`).
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
            "content_checked": field.constraints.requires_content_validation(),
            "constraints": field.constraints.model_dump_json(exclude_defaults=True),
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


def _collect_primary_key_metadata(
    resource: Resource, actual_columns: list[str]
) -> dict[str, Any]:
    """Describe the primary-key check that will run for this resource, if any.

    Whether a composite-uniqueness check is chunked (see
    :attr:`~pudl.metadata.classes.Schema.pk_check_chunk_field`) is a cheap attribute lookup,
    not a re-run of the (potentially expensive, for our largest tables) chunking
    logic itself.
    """
    primary_key = resource.schema.primary_key
    if not primary_key:
        return {"primary_key": {"declared": False}}

    missing_columns = sorted(set(primary_key) - set(actual_columns))
    return {
        "primary_key": {
            "declared": True,
            "columns": primary_key,
            "chunked": resource.schema.pk_check_chunk_field is not None,
            "checked": not missing_columns,
            "missing_columns": missing_columns or None,
        }
    }


def _failure_cases_sample(
    failure_cases: Any, max_failure_samples: int = 20
) -> tuple[int | None, Any]:
    """Return ``(total_count, bounded_json_safe_sample)`` for an error's failure cases.

    Structured (list of records) rather than a stringified table dump: the latter embeds
    Polars' box-drawing-character table rendering as a giant escaped-newline blob,
    unreadable once JSON-encoded for Dagster's UI. Falls back to a plain string only for
    failure-case types we don't specifically recognize.

    ``total_count`` always reflects the true total; max_failure_samples just bounds how
    much actual data gets rendered inline, so a violation affecting millions of rows
    doesn't balloon the check's metadata payload.

    Args:
        failure_cases: The ``failure_cases`` attribute of a Pandera ``SchemaError``.
            Can be a Polars or Pandas dataframe, a Pandas series, a list, or None.
        max_failure_samples: Maximum number of rows to include in the returned sample.
    """
    if isinstance(failure_cases, pl.DataFrame):
        return (
            failure_cases.height,
            failure_cases.head(max_failure_samples).to_dicts(),
        )
    if isinstance(failure_cases, pd.DataFrame):
        return (
            len(failure_cases),
            failure_cases.head(max_failure_samples).to_dict(orient="records"),
        )
    if isinstance(failure_cases, pd.Series):
        return (
            len(failure_cases),
            failure_cases.head(max_failure_samples).tolist(),
        )
    if isinstance(failure_cases, list):
        return len(failure_cases), failure_cases[:max_failure_samples]
    if failure_cases is None:
        return None, None
    return None, str(failure_cases)


def _process_schema_errors(schema_errors: SchemaErrors) -> dict[str, Any]:
    """Process Pandera schema errors into compact, structured metadata.

    Each error is reduced to its essentials -- a one-line message, the reason
    and check that triggered it, which column/table it came from, and a
    bounded sample of the actual offending rows -- rather than several
    overlapping stringified dumps of the same underlying data (as ``args``,
    ``data``, and a table-formatted ``failure_cases`` all tend to be).
    """
    detailed_errors = []

    for err in schema_errors.schema_errors:
        failure_case_count, failure_cases_sample = _failure_cases_sample(
            getattr(err, "failure_cases", None)
        )
        schema_obj = getattr(err, "schema", None)

        detailed_errors.append(
            {
                "error_type": type(err).__name__,
                "reason_code": str(err.reason_code)
                if getattr(err, "reason_code", None) is not None
                else None,
                "error_message": str(err),
                "check": str(err.check)
                if getattr(err, "check", None) is not None
                else None,
                "schema_name": getattr(schema_obj, "name", None),
                "failure_case_count": failure_case_count,
                "failure_cases_sample": failure_cases_sample,
            }
        )

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


def _validate_polars_content(
    asset_value: pl.LazyFrame, resource: Resource
) -> tuple[list[SchemaError], dict[str, float]]:
    """Validate per-column value constraints and primary-key uniqueness.

    Returns the combined error list alongside a ``{"content_check_seconds": ...,
    "pk_check_seconds": ...}`` timing breakdown, so slow phases are visible in
    asset-check metadata without needing to reproduce the check locally.

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

    Composite primary-key uniqueness is handled separately -- see
    :meth:`Resource.check_primary_key`.
    """
    errors: list[SchemaError] = []
    timings: dict[str, float] = {}
    present_columns = set(asset_value.collect_schema().names())

    content_start = time.perf_counter()
    with config_context(validation_depth=ValidationDepth.SCHEMA_AND_DATA):
        for field in resource.schema.fields:
            if field.name not in present_columns:
                continue
            if not field.constraints.requires_content_validation():
                continue
            column_schema = pr_polars.DataFrameSchema(
                {field.name: field.to_pandera_column(use_pandas_backend=False)}
            )
            try:
                column_schema.validate(asset_value.select(field.name), lazy=True)
            except SchemaErrors as schema_errors:
                errors.extend(schema_errors.schema_errors)
    timings["content_check_seconds"] = time.perf_counter() - content_start

    pk_start = time.perf_counter()
    primary_key = resource.schema.primary_key
    if primary_key and all(col in present_columns for col in primary_key):
        errors.extend(resource.check_primary_key(asset_value))
    timings["pk_check_seconds"] = time.perf_counter() - pk_start

    return errors, timings


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
        source_partitions = (
            asset_value.partitions if isinstance(asset_value, ParquetData) else None
        )
        if isinstance(asset_value, ParquetData):
            asset_value = get_parquet_table_polars(
                table_name=resource_id,
                partitions=asset_value.partitions,
            )

        actual_columns, _, _ = _extract_actual_columns_and_dtypes(asset_value)

        # Collect all metadata that's cheap and available regardless of outcome,
        # up front -- so it's present on every return path below, including one
        # that hits an entirely unexpected exception rather than a SchemaErrors.
        metadata: dict[str, Any] = (
            _collect_asset_metadata(asset_value)
            | _collect_dtype_metadata(asset_value, resource)
            | _collect_geometry_metadata(asset_value)
            | _collect_primary_key_metadata(resource, actual_columns)
        )
        metadata["is_duckdb_asset"] = duckdb_asset
        if source_partitions is not None:
            metadata["partitions"] = source_partitions
        timings: dict[str, float] = {}

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
                schema_check_start = time.perf_counter()
                try:
                    pandera_schema.validate(asset_value, lazy=True)
                except SchemaErrors as schema_errors:
                    errors.extend(schema_errors.schema_errors)
                finally:
                    timings["schema_check_seconds"] = (
                        time.perf_counter() - schema_check_start
                    )

                content_errors, content_timings = _validate_polars_content(
                    asset_value, resource
                )
                errors.extend(content_errors)
                timings.update(content_timings)

                if errors:
                    raise SchemaErrors(
                        schema=pandera_schema,
                        schema_errors=errors,
                        data=asset_value,
                    )
            else:
                assert isinstance(pandera_schema, pr_pandas.DataFrameSchema)
                schema_check_start = time.perf_counter()
                try:
                    pandera_schema.validate(asset_value, lazy=True)
                finally:
                    timings["schema_check_seconds"] = (
                        time.perf_counter() - schema_check_start
                    )
            metadata["timing"] = timings
            return dg.AssetCheckResult(passed=True, metadata=metadata)

        except SchemaErrors as schema_errors:
            metadata["timing"] = timings
            metadata.update(_process_schema_errors(schema_errors))
            return dg.AssetCheckResult(passed=False, metadata=metadata)

        except Exception as exc:
            metadata["timing"] = timings
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


def unmapped_ids_check(
    asset_key: dg.AssetKey | str,
    *,
    blocking: bool = True,
) -> dg.AssetChecksDefinition:
    """Return a check that fails if an unmapped FERC1/EIA ID asset has any rows.

    Attach to assets from :mod:`pudl.dagster.assets.core.unmapped_ids`, which
    identify FERC1 or EIA plant/utility IDs missing from the manual PUDL ID mapping
    spreadsheet (see :mod:`pudl.glue.ferc1_eia`). We expect these assets to always be
    empty; any rows indicate IDs that need to be added to the mapping spreadsheet.

    Args:
        asset_key: Key of the unmapped-IDs asset to check.
        blocking: Whether the check is blocking (default ``True``).
    """

    @dg.asset_check(asset=asset_key, blocking=blocking)
    def _unmapped_ids_check(asset_value: pd.DataFrame) -> dg.AssetCheckResult:
        return dg.AssetCheckResult(
            passed=asset_value.empty,
            metadata={"n_unmapped_ids": len(asset_value)},
        )

    return _unmapped_ids_check


default_asset_checks = list(
    itertools.chain.from_iterable(
        dg.load_asset_checks_from_modules(modules)
        for modules in all_asset_modules.values()
    )
)

default_asset_checks += [
    unmapped_ids_check(asset_name) for asset_name in UNMAPPED_ID_ASSET_NAMES
]

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
