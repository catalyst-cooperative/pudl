"""Transform FERC EQR data."""

import dagster as dg
import duckdb

from pudl.helpers import (
    ParquetData,
    duckdb_relation_from_parquet,
    persist_table_as_parquet,
)
from pudl.metadata.classes import Resource
from pudl.settings import ferceqr_year_quarters


def apply_dtypes_to_duckdb_table(
    table_name: str,
    table_data: duckdb.DuckDBPyRelation,
    **col_expressions: dict[str, duckdb.Expression],
):
    """Generate a select statement containing transforms to all columns and cast to correct dtypes.

    EQR transforms are implemented using the duckdb expression api, which allows us
    to dynamically build SQL statements to operate on each column. By collecting all
    of these expressions into a single select statement, we create a query that will
    transform data from raw parquet files.

    Args:
        table_name: Name of table, which should have corresponding table level metadata.
        col_expressions: Map column names to pre-generated expressions. Each of these
            expressions will have a ``cast`` added on to it to apply correct dtypes.
            For columns defined in the table schema, but
    """
    dtypes = Resource.from_id(table_name).to_duckdb_dtypes()
    return table_data.select(
        *[
            expression.cast(dtypes[col]).alias(col)
            for col, expression in col_expressions.items()
        ],
        *[
            duckdb.ColumnExpression(col).cast(dtype).alias(col)
            for col, dtype in dtypes.items()
            if col not in col_expressions
        ],
    )


def _na_to_null(col_name: str) -> duckdb.CaseExpression:
    """Convert string NA values to NULL."""
    # Standardize all strings to be uppercase
    upper_col_expression = duckdb.SQLExpression(f"UPPER({col_name})")

    # If you don't include an `otherwise` statement, those values will default to NULL
    return duckdb.CaseExpression(
        condition=upper_col_expression.isnotin(["N/A", "NA"]),
        value=upper_col_expression,
    )


@dg.asset(partitions_def=ferceqr_year_quarters)
def core_ferceqr__identity(
    context: dg.AssetExecutionContext, raw_ferceqr__ident: ParquetData
):
    """Apply data types to EQR ident table."""
    year_quarter = context.partition_key
    with duckdb_relation_from_parquet(raw_ferceqr__ident, use_all_partitions=True) as (
        table,
        _,
    ):
        return persist_table_as_parquet(
            apply_dtypes_to_duckdb_table(
                table_name="core_ferceqr__identity",
                table_data=table,
                transactions_reported_to_index_price_publishers=_na_to_null(
                    "transactions_reported_to_index_price_publishers"
                ),
            ),
            table_name="core_ferceqr__identity",
            partitions={"year_quarter": year_quarter},
            use_output_dir=True,
        )


@dg.asset(
    partitions_def=ferceqr_year_quarters,
)
def core_ferceqr__transactions(context, raw_ferceqr__transactions: ParquetData):
    """Perform basic transforms on transactions table table."""
    year_quarter = context.partition_key
    with duckdb_relation_from_parquet(
        raw_ferceqr__transactions, use_all_partitions=True
    ) as (table, conn):
        return persist_table_as_parquet(
            apply_dtypes_to_duckdb_table(
                table_name="core_ferceqr__transactions",
                table_data=table,
                transaction_begin_date=duckdb.SQLExpression(
                    "TRY_STRPTIME(transaction_begin_date, '%Y%m%d%H%M')"
                ),
                transaction_end_date=duckdb.SQLExpression(
                    "TRY_STRPTIME(transaction_end_date, '%Y%m%d%H%M')"
                ),
                trade_date=duckdb.SQLExpression("TRY_STRPTIME(trade_date, '%Y%m%d')"),
                contract_service_agreement_id=duckdb.ColumnExpression(
                    "contract_service_agreement"
                ),
                exchange_brokerage_service=_na_to_null("exchange_brokerage_service"),
                type_of_rate=_na_to_null("type_of_rate"),
                time_zone=_na_to_null("time_zone"),
                class_name=_na_to_null("class_name"),
                term_name=_na_to_null("term_name"),
                increment_name=_na_to_null("increment_name"),
                increment_peaking_name=_na_to_null("increment_peaking_name"),
                product_name=_na_to_null("product_name"),
                rate_units=_na_to_null("rate_units"),
            ),
            table_name="core_ferceqr__transactions",
            partitions={"year_quarter": year_quarter},
            use_output_dir=True,
        )


@dg.asset(partitions_def=ferceqr_year_quarters)
def core_ferceqr__contracts(context, raw_ferceqr__contracts: ParquetData):
    """Perform basic transforms on contracts table table."""
    year_quarter = context.partition_key
    with duckdb_relation_from_parquet(
        raw_ferceqr__contracts, use_all_partitions=True
    ) as (table, _):
        return persist_table_as_parquet(
            apply_dtypes_to_duckdb_table(
                table_name="core_ferceqr__contracts",
                table_data=table,
                contract_execution_date=duckdb.SQLExpression(
                    "TRY_STRPTIME(contract_execution_date, '%Y%m%d')"
                ),
                commencement_date_of_contract_term=duckdb.SQLExpression(
                    "TRY_STRPTIME(commencement_date_of_contract_term, '%Y%m%d')"
                ),
                contract_termination_date=duckdb.SQLExpression(
                    "TRY_STRPTIME(contract_termination_date, '%Y%m%d')"
                ),
                actual_termination_date=duckdb.SQLExpression(
                    "TRY_STRPTIME(actual_termination_date, '%Y%m%d')"
                ),
                class_name=_na_to_null("class_name"),
                term_name=_na_to_null("term_name"),
                increment_name=_na_to_null("increment_name"),
                increment_peaking_name=_na_to_null("increment_peaking_name"),
                product_type_name=_na_to_null("product_type_name"),
                product_name=_na_to_null("product_name"),
                rate_units=_na_to_null("rate_units"),
                units=_na_to_null("units"),
                begin_date=duckdb.SQLExpression(
                    "TRY_STRPTIME(begin_date, '%Y%m%d%H%M')"
                ),
                end_date=duckdb.SQLExpression("TRY_STRPTIME(end_date, '%Y%m%d%H%M')"),
                contract_affiliate=(
                    duckdb.CaseExpression(
                        condition=duckdb.ColumnExpression("contract_affiliate")
                        == duckdb.ConstantExpression("Y"),
                        value=True,
                    ).when(
                        condition=duckdb.ColumnExpression("contract_affiliate")
                        == duckdb.ConstantExpression("N"),
                        value=False,
                    )
                ),
            ),
            table_name="core_ferceqr__contracts",
            partitions={"year_quarter": year_quarter},
            use_output_dir=True,
        )
