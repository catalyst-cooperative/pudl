"""Transform FERC Electric Quarterly Report (EQR) data.

This module implements the transformation stage of the PUDL pipeline for FERC EQR data.
Raw EQR data is ingested as quarterly-partitioned Apache Parquet files and transformed
into clean, typed core tables that are written back out as Parquet.

The module is structured in two layers:

Reusable DuckDB transformation helpers operate on :class:`duckdb.DuckDBPyRelation`
objects and are composed together inside each Dagster asset definition.

Private expression factory functions that accept a column name (and optional parameters)
and return a :class:`duckdb.Expression` suitable for use inside
:func:`apply_column_transforms`.

Dagster assets apply these helpers to produce four core FERC EQR tables, each of which
is partitioned by ``year_quarter``:

- :ref:`core_ferceqr__quarterly_identity`
- :ref:`core_ferceqr__transactions`
- :ref:`core_ferceqr__contracts`
- :ref:`core_ferceqr__quarterly_index_pub`
"""

from collections.abc import Callable
from typing import Any

import dagster as dg
import duckdb

from pudl.dagster.partitions import ferceqr_year_quarters
from pudl.helpers import (
    ParquetData,
    duckdb_relation_from_parquet,
    persist_table_as_parquet,
)
from pudl.logging_helpers import get_logger
from pudl.metadata.classes import Resource

logger = get_logger(__name__)

_EXTRACTION_STATS_ASSET_KEY = dg.AssetKey(["raw_ferceqr__extract_errors"])
_EXTRACTION_STATS_METADATA_KEY = "extraction_stats"
_EXTRACTION_STATS_FETCH_LIMIT = 500
"""Comfortably more than the number of ferceqr quarters, even with some
re-materialized more than once -- ``fetch_materializations`` returns
most-recent-first, so the first record seen for each partition is already the
one we want."""

_CORE_FERCEQR_TABLE_LABELS: dict[dg.AssetKey, str] = {
    dg.AssetKey(["core_ferceqr__quarterly_identity"]): "identity",
    dg.AssetKey(["core_ferceqr__contracts"]): "contracts",
    dg.AssetKey(["core_ferceqr__transactions"]): "transactions",
    dg.AssetKey(["core_ferceqr__quarterly_index_pub"]): "index_pub",
}
_PANDERA_CHECK_NAME = "pandera_schema_check"
_CHECK_EVALUATION_FETCH_LIMIT = 2000
"""4 core ferceqr tables x ~52 quarters, with headroom for re-run partitions."""


def apply_duckdb_dtypes(
    table_data: duckdb.DuckDBPyRelation,
    table_name: str,
    conn: duckdb.DuckDBPyConnection,
):
    """Cast each column to the dtype declared in the PUDL metadata schema for the table.

    Column types are looked up from the :class:`pudl.metadata.classes.Resource` for
    ``table_name``. Any custom enum types required by the schema are created in ``conn``
    before the cast is applied.

    Args:
        table_data: DuckDB relation whose columns will be cast.
        table_name: PUDL table name used to look up the schema from the metadata.
        conn: DuckDB connection used to register custom enum types as needed.
    """
    dtypes = Resource.from_id(table_name).to_duckdb_dtypes(conn)

    # Cast columns
    return table_data.select(
        duckdb.StarExpression(exclude=dtypes.keys()),
        *[
            duckdb.ColumnExpression(col).cast(dtype).alias(col)
            for col, dtype in dtypes.items()
        ],
    )


def rename_duckdb_columns(
    table_data: duckdb.DuckDBPyRelation, mapping: dict[str, str]
) -> duckdb.DuckDBPyRelation:
    """Rename one or more columns in a DuckDB relation, passing all others through unchanged.

    Args:
        table_data: DuckDB relation containing the columns to rename.
        mapping: Maps existing column names to their new names.
    """
    return table_data.select(
        duckdb.StarExpression(exclude=mapping.keys()),
        *[
            duckdb.ColumnExpression(name).alias(new_name)
            for name, new_name in mapping.items()
        ],
    )


def apply_column_transforms(
    table_data: duckdb.DuckDBPyRelation,
    columns: list[str],
    transform: Callable[[str], duckdb.Expression],
) -> duckdb.DuckDBPyRelation:
    """Apply a DuckDB expression factory to a set of columns, replacing each in place.

    The ``transform`` callable is invoked once per column name and must return a
    :class:`duckdb.Expression` whose result will be aliased back to the original column
    name. All columns not listed in *columns* are passed through unchanged.

    Args:
        table_data: DuckDB relation containing the columns to transform.
        columns: Names of the columns to which ``transform`` will be applied.
        transform: Callable that accepts a column name and returns a DuckDB Expression
            defining the transformation for that column.
    """
    return table_data.select(
        duckdb.StarExpression(exclude=columns),
        *[transform(col).alias(col) for col in columns],
    )


def _yn_to_bool(col: str) -> duckdb.Expression:
    """Return a DuckDB expression that converts ``'Y'``/``'N'`` strings to booleans.

    The comparison is case-insensitive. Any value other than ``'Y'`` or ``'N'`` is
    mapped to ``NULL``.
    """
    return duckdb.SQLExpression(f"""
CASE
    WHEN UPPER({col}) = 'Y' THEN TRUE
    WHEN UPPER({col}) = 'N' THEN FALSE
    ELSE NULL
END""")


def _na_to_null(col_name: str) -> duckdb.Expression:
    """Return a DuckDB expression that converts ``'N/A'`` or ``'NA'`` strings to NULL.

    The comparison is case-insensitive. All other values are uppercased and returned
    unchanged.
    """
    return duckdb.SQLExpression(
        f"CASE WHEN UPPER({col_name}) IN ('N/A', 'NA') THEN NULL ELSE UPPER({col_name}) END"
    )


def _parse_datetimes(col_name: str, fmt: str) -> duckdb.Expression:
    """Return a DuckDB expression that parses a datetime string column using ``fmt``.

    Uses DuckDB's ``TRY_STRPTIME``, so values that cannot be parsed return ``NULL``
    rather than raising an error.
    """
    return duckdb.SQLExpression(f"TRY_STRPTIME({col_name}, '{fmt}')")


def _recode_categoricals(
    col_name: str, replace_mapping: dict[str, str]
) -> duckdb.Expression:
    """Return a DuckDB expression that replaces exact categorical values in a column.

    Generates a ``CASE WHEN`` expression with one equality branch per entry in
    ``replace_mapping``. Keys not in the mapping are passed through unchanged via the
    ``ELSE`` clause.

    Args:
        col_name: Name of the DuckDB column whose values will be recoded.
        replace_mapping: Maps each observed bad value (key) to its correct
            canonical replacement (value).
    """
    when_clauses = " ".join(
        f"WHEN {col_name} = '{to_replace}' THEN '{value}'"
        for to_replace, value in replace_mapping.items()
    )
    return duckdb.SQLExpression(f"CASE {when_clauses} ELSE {col_name} END")


@dg.asset(partitions_def=ferceqr_year_quarters, kinds={"duckdb"})
def core_ferceqr__quarterly_identity(
    context: dg.AssetExecutionContext, raw_ferceqr__ident: ParquetData
):
    """Transform the raw FERC EQR filer identity table."""
    year_quarter = context.partition_key
    logger.info(f"Transforming ferceqr identity table for {year_quarter}")
    table_name = "core_ferceqr__quarterly_identity"

    with duckdb_relation_from_parquet(raw_ferceqr__ident, use_all_partitions=True) as (
        table,
        conn,
    ):
        table_data = apply_column_transforms(
            table_data=table,
            columns=["transactions_reported_to_index_price_publishers"],
            transform=_yn_to_bool,
        )
        table_data = rename_duckdb_columns(
            table_data,
            {
                "company_identifier": "company_id_ferc",
            },
        )
        table_data = apply_duckdb_dtypes(table_data, table_name, conn)

        return persist_table_as_parquet(
            table_name=table_name,
            table_data=table_data,
            partitions={"year_quarter": year_quarter},
        )


@dg.asset(
    partitions_def=ferceqr_year_quarters,
    kinds={"duckdb"},
)
def core_ferceqr__transactions(context, raw_ferceqr__transactions: ParquetData):
    """Transform the raw FERC EQR electricity transactions table."""
    year_quarter = context.partition_key
    logger.info(f"Transforming ferceqr transactions table for {year_quarter}")
    table_name = "core_ferceqr__transactions"

    with duckdb_relation_from_parquet(
        raw_ferceqr__transactions, use_all_partitions=True
    ) as (table, conn):
        table_data = apply_column_transforms(
            table_data=table,
            columns=[
                "exchange_brokerage_service",
                "type_of_rate",
                "time_zone",
                "class_name",
                "term_name",
                "increment_name",
                "increment_peaking_name",
                "product_name",
                "rate_units",
            ],
            transform=_na_to_null,
        )
        # Normalize categorical string
        table_data = apply_column_transforms(
            table_data=table_data,
            columns=["product_name"],
            transform=lambda col: _recode_categoricals(
                col, {"NEGOTIATED RATE TRANSMISSION": "NEGOTIATED-RATE TRANSMISSION"}
            ),
        )
        table_data = apply_column_transforms(
            table_data=table_data,
            columns=[
                "transaction_begin_date",
                "transaction_end_date",
            ],
            transform=lambda col: _parse_datetimes(col, "%Y%m%d%H%M"),
        )
        table_data = apply_column_transforms(
            table_data=table_data,
            columns=["trade_date"],
            transform=lambda col: _parse_datetimes(col, "%Y%m%d"),
        )
        table_data = apply_column_transforms(
            table_data=table_data,
            columns=["time_zone"],
            transform=lambda col: _recode_categoricals(
                col,
                {
                    "CDT": "CD",
                    "CST": "CS",
                    "CPT": "CP",
                    "EDT": "ED",
                    "EPT": "EP",
                    "EST": "ES",
                    "MDT": "MD",
                    "MPT": "MP",
                    "MST": "MS",
                    "PDT": "PD",
                    "PPT": "PP",
                    "PST": "PS",
                },
            ),
        )
        table_data = rename_duckdb_columns(
            table_data,
            {
                "company_identifier": "seller_company_id_ferc",
                "contract_service_agreement": "contract_service_agreement_id",
                "transaction_unique_identifier": "seller_transaction_id",
                "time_zone": "timezone",
            },
        )
        table_data = apply_duckdb_dtypes(table_data, table_name, conn)

        return persist_table_as_parquet(
            table_name=table_name,
            table_data=table_data,
            partitions={"year_quarter": year_quarter},
        )


@dg.asset(partitions_def=ferceqr_year_quarters, kinds={"duckdb"})
def core_ferceqr__contracts(context, raw_ferceqr__contracts: ParquetData):
    """Transform the raw FERC EQR electricity contracts table."""
    year_quarter = context.partition_key
    logger.info(f"Transforming ferceqr contracts table for {year_quarter}")
    table_name = "core_ferceqr__contracts"

    with duckdb_relation_from_parquet(
        raw_ferceqr__contracts, use_all_partitions=True
    ) as (table, conn):
        table_data = apply_column_transforms(
            table_data=table,
            columns=[
                "class_name",
                "term_name",
                "increment_name",
                "increment_peaking_name",
                "product_type_name",
                "product_name",
                "units",
                "rate_units",
            ],
            transform=_na_to_null,
        )
        table_data = apply_column_transforms(
            table_data=table_data,
            columns=["contract_affiliate"],
            transform=_yn_to_bool,
        )

        # Drop seller_history_name
        if "seller_history_name" in table_data.columns:
            table_data = table_data.select(
                duckdb.StarExpression(exclude=["seller_history_name"]),
            )

        # Normalize categorical string
        table_data = apply_column_transforms(
            table_data=table_data,
            columns=["product_name"],
            transform=lambda _: duckdb.SQLExpression(
                "replace(product_name, 'NEGOTIATED RATE TRANSMISSION', 'NEGOTIATED-RATE TRANSMISSION')"
            ),
        )
        table_data = apply_column_transforms(
            table_data=table_data,
            columns=[
                "contract_execution_date",
                "commencement_date_of_contract_term",
                "contract_termination_date",
                "actual_termination_date",
            ],
            transform=lambda col: _parse_datetimes(col, "%Y%m%d"),
        )
        table_data = apply_column_transforms(
            table_data=table_data,
            columns=[
                "begin_date",
                "end_date",
            ],
            transform=lambda col: _parse_datetimes(col, "%Y%m%d%H%M"),
        )
        table_data = rename_duckdb_columns(
            table_data,
            {
                "company_identifier": "seller_company_id_ferc",
            },
        )
        table_data = apply_duckdb_dtypes(table_data, table_name, conn)

        return persist_table_as_parquet(
            table_name=table_name,
            table_data=table_data,
            partitions={"year_quarter": year_quarter},
        )


@dg.asset(partitions_def=ferceqr_year_quarters, kinds={"duckdb"})
def core_ferceqr__quarterly_index_pub(context, raw_ferceqr__index_pub: ParquetData):
    """Transform the raw FERC EQR index price publisher table."""
    year_quarter = context.partition_key
    logger.info(f"Transforming ferceqr indexPub table for {year_quarter}")
    table_name = "core_ferceqr__quarterly_index_pub"

    with duckdb_relation_from_parquet(
        raw_ferceqr__index_pub, use_all_partitions=True
    ) as (table, conn):
        table_data = apply_column_transforms(
            table_data=table,
            columns=[
                "Index_Price_Publishers_To_Which_Sales_Transactions_Have_Been_Reported"
            ],
            transform=_na_to_null,
        )
        table_data = rename_duckdb_columns(
            table_data,
            mapping={
                "company_identifier": "company_id_ferc",
                "Seller_Company_Name": "seller_company_name",
                "Index_Price_Publishers_To_Which_Sales_Transactions_Have_Been_Reported": "index_price_publisher_name",
                "Transactions_Reported": "transactions_reported",
            },
        )
        table_data = apply_duckdb_dtypes(table_data, table_name, conn)
        return persist_table_as_parquet(
            table_name=table_name,
            table_data=table_data,
            partitions={"year_quarter": year_quarter},
        )


def _latest_extraction_stats_by_quarter(
    instance: dg.DagsterInstance,
) -> dict[str, dict[str, Any]]:
    """Return ``{year_quarter: extraction_stats}`` from each partition's latest run.

    Reads the ``extraction_stats`` JSON metadata that :func:`pudl.extract.ferceqr.
    extract_ferceqr` attaches to ``raw_ferceqr__extract_errors``, directly from this
    Dagster instance's event log -- no Parquet data is read.
    """
    result = instance.fetch_materializations(
        _EXTRACTION_STATS_ASSET_KEY, limit=_EXTRACTION_STATS_FETCH_LIMIT
    )
    stats_by_quarter: dict[str, dict[str, Any]] = {}
    for record in result.records:
        year_quarter = record.partition_key
        if year_quarter is None or year_quarter in stats_by_quarter:
            continue  # already have a more recent materialization for this quarter
        materialization = record.asset_materialization
        if materialization is None:
            continue
        metadata_value = materialization.metadata.get(_EXTRACTION_STATS_METADATA_KEY)
        if isinstance(metadata_value, dg.JsonMetadataValue) and isinstance(
            metadata_value.data, dict
        ):
            stats_by_quarter[year_quarter] = metadata_value.data
    return stats_by_quarter


def _latest_check_evaluations_by_quarter(
    instance: dg.DagsterInstance,
) -> dict[str, dict[str, dg.AssetCheckEvaluation]]:
    """Return ``{year_quarter: {table_label: evaluation}}`` from each check's latest run.

    Reads ``pandera_schema_check`` asset check evaluations for the four
    ``core_ferceqr__*`` tables directly from this Dagster instance's event log --
    no Parquet data is read.
    """
    records = instance.get_event_records(
        dg.EventRecordsFilter(event_type=dg.DagsterEventType.ASSET_CHECK_EVALUATION),
        limit=_CHECK_EVALUATION_FETCH_LIMIT,
        ascending=False,
    )
    evaluations_by_quarter: dict[str, dict[str, dg.AssetCheckEvaluation]] = {}
    for record in records:
        dagster_event = record.event_log_entry.dagster_event
        if dagster_event is None:
            continue
        evaluation = dagster_event.event_specific_data
        if not isinstance(evaluation, dg.AssetCheckEvaluation):
            continue
        table_label = _CORE_FERCEQR_TABLE_LABELS.get(evaluation.asset_key)
        year_quarter = evaluation.partition
        if (
            evaluation.check_name != _PANDERA_CHECK_NAME
            or table_label is None
            or year_quarter is None
        ):
            continue
        seen_for_quarter = evaluations_by_quarter.setdefault(year_quarter, {})
        if table_label not in seen_for_quarter:
            seen_for_quarter[table_label] = evaluation
    return evaluations_by_quarter


def _summarize_check_failures(evaluation: dg.AssetCheckEvaluation) -> str:
    """Return a short human-readable summary of a failed check, or "" if it passed."""
    if evaluation.passed:
        return ""
    detailed_errors = evaluation.metadata.get("detailed_errors")
    if not isinstance(detailed_errors, dg.JsonMetadataValue) or not isinstance(
        detailed_errors.data, list
    ):
        return "FAILED (no detail available)"
    parts = [
        f"{error['failure_case_count']}x {error['check']} ({error['error_message']})"
        for error in detailed_errors.data
    ]
    return "; ".join(parts)


def _build_ferceqr_diagnostics_rows(
    extraction_stats_by_quarter: dict[str, dict[str, Any]],
    check_evaluations_by_quarter: dict[str, dict[str, dg.AssetCheckEvaluation]],
) -> list[dict[str, Any]]:
    """Flatten per-quarter extraction stats and check results into one wide table."""
    all_quarters = sorted(
        set(extraction_stats_by_quarter) | set(check_evaluations_by_quarter)
    )
    all_table_types = sorted(
        {
            t
            for stats in extraction_stats_by_quarter.values()
            for t in stats["table_file_counts"]
        }
    )
    all_reject_reasons = sorted(
        {
            reason
            for stats in extraction_stats_by_quarter.values()
            for reason in stats["rejected_record_counts"]
        }
    )
    all_core_tables = sorted(_CORE_FERCEQR_TABLE_LABELS.values())

    rows = []
    for year_quarter in all_quarters:
        stats = extraction_stats_by_quarter.get(year_quarter)
        row: dict[str, Any] = {"year_quarter": year_quarter}
        if stats is not None:
            row["total_filings"] = stats["total_filings"]
            row["corrupt_filings"] = stats["corrupt_filings"]
            for table_type in all_table_types:
                row[f"n_{table_type}"] = stats["table_file_counts"].get(table_type, 0)
            for reason in all_reject_reasons:
                row[f"rejected_{reason.lower().replace(' ', '_')}"] = stats[
                    "rejected_record_counts"
                ].get(reason, 0)

        evaluations = check_evaluations_by_quarter.get(year_quarter, {})
        failure_summaries = []
        for table_label in all_core_tables:
            evaluation = evaluations.get(table_label)
            if evaluation is None:
                row[f"rows_{table_label}"] = None
                continue
            asset_shape = evaluation.metadata.get("asset_shape")
            row[f"rows_{table_label}"] = (
                asset_shape.data[0]
                if isinstance(asset_shape, dg.JsonMetadataValue)
                and isinstance(asset_shape.data, list)
                else None
            )
            summary = _summarize_check_failures(evaluation)
            if summary:
                failure_summaries.append(f"{table_label}: {summary}")
        row["check_failures"] = "; ".join(failure_summaries)

        rows.append(row)

    return rows


@dg.asset(
    deps=[
        dg.AssetDep(
            "raw_ferceqr__extract_errors", partition_mapping=dg.AllPartitionMapping()
        ),
        dg.AssetDep(
            "core_ferceqr__quarterly_identity",
            partition_mapping=dg.AllPartitionMapping(),
        ),
        dg.AssetDep(
            "core_ferceqr__contracts", partition_mapping=dg.AllPartitionMapping()
        ),
        dg.AssetDep(
            "core_ferceqr__transactions", partition_mapping=dg.AllPartitionMapping()
        ),
        dg.AssetDep(
            "core_ferceqr__quarterly_index_pub",
            partition_mapping=dg.AllPartitionMapping(),
        ),
    ],
)
def ferceqr_pipeline_diagnostics(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    """Compile a cross-quarter summary of ferceqr extraction and schema-check anomalies.

    This asset produces no data of its own -- it exists purely to compile
    diagnostics that other ferceqr assets already record, into a single table
    visible in the Dagster UI without opening each quarter's materialization one
    at a time: the per-quarter ``extraction_stats`` metadata attached to
    ``raw_ferceqr__extract_errors`` (filing counts, corrupt archives, rejected
    records by reason), and the ``pandera_schema_check`` asset check results for
    the four ``core_ferceqr__*`` tables (row counts, primary-key violations, and
    other schema check failures).

    Depending on *all* partitions of several partitioned upstream assets via
    :class:`dagster.AllPartitionMapping` means materializing this asset re-scans
    the full history already recorded in this Dagster instance's event log each
    time -- cheap, since it only reads metadata, never the underlying Parquet
    data.
    """
    instance = context.instance
    extraction_stats_by_quarter = _latest_extraction_stats_by_quarter(instance)
    check_evaluations_by_quarter = _latest_check_evaluations_by_quarter(instance)
    rows = _build_ferceqr_diagnostics_rows(
        extraction_stats_by_quarter, check_evaluations_by_quarter
    )

    return dg.MaterializeResult(
        metadata={
            "summary": dg.MetadataValue.table(
                records=[dg.TableRecord(data=row) for row in rows]
            ),
            "n_quarters": dg.MetadataValue.int(len(rows)),
        }
    )
