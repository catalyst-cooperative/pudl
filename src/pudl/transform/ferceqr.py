"""Transform FERC EQR data."""

from collections.abc import Callable

import dagster as dg
import duckdb

from pudl.helpers import (
    ParquetData,
    duckdb_relation_from_parquet,
    persist_table_as_parquet,
)
from pudl.logging_helpers import get_logger
from pudl.metadata.classes import Resource
from pudl.settings import ferceqr_year_quarters

logger = get_logger(__name__)


def apply_duckdb_dtypes(
    table_data: duckdb.DuckDBPyRelation,
    table_name: str,
    conn: duckdb.DuckDBPyConnection,
):
    """Cast columns to dtype as defined in schema.

    Args:
        table_data: Duckdb table to transform.
        table_name: Name of table.
        conn: Connection to duckdb database, which is required to create custom enum types.
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
    """Rename columns of a duckdb table relation and return.

    Args:
        table_data: Duckdb table to transform.
        mapping: Maps column names to new names
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
    """Apply a single transformation to a set of columns in a duckdb table.

    Args:
        table_data: Duckdb table to transform.
        columns: List of columns to apply transform to.
        transform: Callable which expects a column name and returns an Expression
            defining the transform.
    """
    return table_data.select(
        duckdb.StarExpression(exclude=columns),
        *[transform(col).alias(col) for col in columns],
    )


def _yn_to_bool(col: str):
    return duckdb.SQLExpression(f"""
CASE
    WHEN UPPER({col}) = 'Y' THEN TRUE
    WHEN UPPER({col}) = 'N' THEN FALSE
    ELSE NULL
END""")


def _na_to_null(col_name: str) -> duckdb.Expression:
    """Convert string NA values to NULL."""
    return duckdb.SQLExpression(
        f"CASE WHEN UPPER({col_name}) IN ('N/A', 'NA') THEN NULL ELSE UPPER({col_name}) END"
    )


def _parse_datetimes(col_name: str, fmt: str) -> duckdb.Expression:
    """Return a duckdb expression to parse datetimes from strings."""
    return duckdb.SQLExpression(f"TRY_STRPTIME({col_name}, '{fmt}')")


@dg.asset(partitions_def=ferceqr_year_quarters)
def core_ferceqr__quarterly_identity(
    context: dg.AssetExecutionContext, raw_ferceqr__ident: ParquetData
):
    """Apply data types to EQR ident table."""
    year_quarter = context.partition_key
    logger.info(f"Transforming ferceqr identity table for {year_quarter}")
    table_name = "core_ferceqr__quarterly_identity"

    with duckdb_relation_from_parquet(raw_ferceqr__ident, use_all_partitions=True) as (
        table,
        conn,
    ):
        table_data = apply_column_transforms(
            table,
            ["transactions_reported_to_index_price_publishers"],
            _yn_to_bool,
        )
        table_data = rename_duckdb_columns(
            table_data,
            {
                "company_identifier": "company_id_ferc",
            },
        )

        return persist_table_as_parquet(
            table_name=table_name,
            table_data=apply_duckdb_dtypes(table_data, table_name, conn),
            partitions={"year_quarter": year_quarter},
        )


@dg.asset(
    partitions_def=ferceqr_year_quarters,
)
def core_ferceqr__transactions(context, raw_ferceqr__transactions: ParquetData):
    """Perform basic transforms on transactions table table."""
    year_quarter = context.partition_key
    logger.info(f"Transforming ferceqr transactions table for {year_quarter}")
    table_name = "core_ferceqr__transactions"

    with duckdb_relation_from_parquet(
        raw_ferceqr__transactions, use_all_partitions=True
    ) as (table, conn):
        table_data = apply_column_transforms(
            table,
            [
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
            _na_to_null,
        )
        # Normalize categorical string
        table_data = apply_column_transforms(
            table_data,
            ["product_name"],
            lambda _: duckdb.SQLExpression(
                "replace(product_name, 'NEGOTIATED RATE TRANSMISSION', 'NEGOTIATED-RATE TRANSMISSION')"
            ),
        )
        table_data = apply_column_transforms(
            table_data,
            [
                "transaction_begin_date",
                "transaction_end_date",
            ],
            lambda col: _parse_datetimes(col, "%Y%m%d%H%M"),
        )
        table_data = apply_column_transforms(
            table_data,
            [
                "trade_date",
            ],
            lambda col: _parse_datetimes(col, "%Y%m%d"),
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

        return persist_table_as_parquet(
            table_name=table_name,
            table_data=apply_duckdb_dtypes(table_data, table_name, conn),
            partitions={"year_quarter": year_quarter},
        )


@dg.asset(partitions_def=ferceqr_year_quarters)
def core_ferceqr__contracts(context, raw_ferceqr__contracts: ParquetData):
    """Perform basic transforms on contracts table table."""
    year_quarter = context.partition_key
    logger.info(f"Transforming ferceqr contracts table for {year_quarter}")
    table_name = "core_ferceqr__contracts"

    with duckdb_relation_from_parquet(
        raw_ferceqr__contracts, use_all_partitions=True
    ) as (table, conn):
        table_data = apply_column_transforms(
            table,
            [
                "class_name",
                "term_name",
                "increment_name",
                "increment_peaking_name",
                "product_type_name",
                "product_name",
                "units",
                "rate_units",
            ],
            _na_to_null,
        )
        table_data = apply_column_transforms(
            table_data,
            ["contract_affiliate"],
            _yn_to_bool,
        )

        # Drop seller_history_name
        if "seller_history_name" in table_data.columns():
            table_data = table_data.select(
                duckdb.StarExpression(exclude=["seller_history_name"]),
            )

        # Normalize categorical string
        table_data = apply_column_transforms(
            table_data,
            ["product_name"],
            lambda _: duckdb.SQLExpression(
                "replace(product_name, 'NEGOTIATED RATE TRANSMISSION', 'NEGOTIATED-RATE TRANSMISSION')"
            ),
        )
        table_data = apply_column_transforms(
            table_data,
            [
                "contract_execution_date",
                "commencement_date_of_contract_term",
                "contract_termination_date",
                "actual_termination_date",
                "product_type_name",
                "product_name",
                "units",
                "rate_units",
            ],
            lambda col: _parse_datetimes(col, "%Y%m%d"),
        )
        table_data = apply_column_transforms(
            table_data,
            [
                "begin_date",
                "end_date",
            ],
            lambda col: _parse_datetimes(col, "%Y%m%d%H%M"),
        )
        table_data = rename_duckdb_columns(
            table_data,
            {
                "company_identifier": "seller_company_id_ferc",
            },
        )

        return persist_table_as_parquet(
            table_name=table_name,
            table_data=apply_duckdb_dtypes(table_data, table_name, conn),
            partitions={"year_quarter": year_quarter},
        )


@dg.asset(partitions_def=ferceqr_year_quarters)
def core_ferceqr__quarterly_index_pub(context, raw_ferceqr__index_pub: ParquetData):
    """Perform basic transforms on indexPub table table."""
    year_quarter = context.partition_key
    logger.info(f"Transforming ferceqr indexPub table for {year_quarter}")
    table_name = "core_ferceqr__quarterly_index_pub"

    with duckdb_relation_from_parquet(
        raw_ferceqr__index_pub, use_all_partitions=True
    ) as (table, conn):
        table_data = apply_column_transforms(
            table,
            ["Index_Price_Publishers_To_Which_Sales_Transactions_Have_Been_Reported"],
            _na_to_null,
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
        return persist_table_as_parquet(
            table_name=table_name,
            table_data=apply_duckdb_dtypes(table_data, table_name, conn),
            partitions={"year_quarter": year_quarter},
        )
