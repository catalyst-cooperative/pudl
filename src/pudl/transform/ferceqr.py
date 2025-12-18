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


def transform_eqr_table(
    table_name: str,
    table_data: duckdb.DuckDBPyRelation,
    year_quarter: str,
    col_expressions: dict[str, duckdb.Expression],
) -> ParquetData:
    """Generate a SQL query to transform an EQR table, then write outputs to parquet.

    EQR transforms are implemented using the duckdb expression api, which allows us
    to dynamically build SQL statements to operate on each column. By collecting all
    of these expressions into a single select statement, we create a query that will
    transform data from raw parquet files.

    Args:
        table_name: Name of table, which should have corresponding table level metadata.
        col_expressions: Map column names to pre-generated expressions. Each of these
            expressions will have a ``cast`` added on to it to apply correct dtypes.
    """
    dtypes = Resource.from_id(table_name).to_duckdb_dtypes()
    return persist_table_as_parquet(
        table_data=table_data.select(
            *[
                expression.cast(dtypes[col]).alias(col)
                for col, expression in col_expressions.items()
            ],
        ),
        table_name=table_name,
        partitions={"year_quarter": year_quarter},
        use_output_dir=True,
    )


def _na_to_null(col_name: str) -> duckdb.Expression:
    """Convert string NA values to NULL."""
    return duckdb.SQLExpression(
        f"CASE WHEN UPPER({col_name}) IN ('N/A', 'NA') THEN NULL ELSE UPPER({col_name}) END"
    )


@dg.asset(partitions_def=ferceqr_year_quarters)
def core_ferceqr__quarterly_identity(
    context: dg.AssetExecutionContext, raw_ferceqr__ident: ParquetData
):
    """Apply data types to EQR ident table."""
    year_quarter = context.partition_key
    with duckdb_relation_from_parquet(raw_ferceqr__ident, use_all_partitions=True) as (
        table,
        _,
    ):
        return transform_eqr_table(
            table_name="core_ferceqr__quarterly_identity",
            table_data=table,
            year_quarter=year_quarter,
            col_expressions={
                "year_quarter": duckdb.ColumnExpression("year_quarter"),
                "company_id_ferc": duckdb.ColumnExpression("company_identifier"),
                "filer_unique_id": duckdb.ColumnExpression("filer_unique_id"),
                "company_name": duckdb.ColumnExpression("company_name"),
                "contact_name": duckdb.ColumnExpression("contact_name"),
                "contact_title": duckdb.ColumnExpression("contact_title"),
                "contact_address": duckdb.ColumnExpression("contact_address"),
                "contact_city": duckdb.ColumnExpression("contact_city"),
                "contact_state": duckdb.ColumnExpression("contact_state"),
                "contact_zip": duckdb.ColumnExpression("contact_zip"),
                "contact_country_name": duckdb.ColumnExpression("contact_country_name"),
                "contact_phone": duckdb.ColumnExpression("contact_phone"),
                "contact_email": duckdb.ColumnExpression("contact_email"),
                "transactions_reported_to_index_price_publishers": (
                    duckdb.CaseExpression(
                        condition=duckdb.SQLExpression(
                            "UPPER(transactions_reported_to_index_price_publishers)"
                        )
                        == duckdb.ConstantExpression("Y"),
                        value=True,
                    ).when(
                        condition=duckdb.SQLExpression(
                            "UPPER(transactions_reported_to_index_price_publishers)"
                        )
                        == duckdb.ConstantExpression("N"),
                        value=False,
                    )
                ),
            },
        )


@dg.asset(
    partitions_def=ferceqr_year_quarters,
)
def core_ferceqr__transactions(context, raw_ferceqr__transactions: ParquetData):
    """Perform basic transforms on transactions table table."""
    year_quarter = context.partition_key
    with duckdb_relation_from_parquet(
        raw_ferceqr__transactions, use_all_partitions=True
    ) as (table, _):
        return transform_eqr_table(
            table_name="core_ferceqr__transactions",
            table_data=table,
            year_quarter=year_quarter,
            col_expressions={
                "year_quarter": duckdb.ColumnExpression("year_quarter"),
                "seller_company_id_ferc": duckdb.ColumnExpression("company_identifier"),
                "transaction_unique_id": duckdb.ColumnExpression(
                    "transaction_unique_id"
                ),
                "seller_company_name": duckdb.ColumnExpression("seller_company_name"),
                "customer_company_name": duckdb.ColumnExpression(
                    "customer_company_name"
                ),
                "ferc_tariff_reference": duckdb.ColumnExpression(
                    "ferc_tariff_reference"
                ),
                "contract_service_agreement_id": duckdb.ColumnExpression(
                    "contract_service_agreement"
                ),
                "seller_transaction_id": duckdb.ColumnExpression(
                    "transaction_unique_identifier"
                ),
                "transaction_begin_date": duckdb.SQLExpression(
                    "TRY_STRPTIME(transaction_begin_date, '%Y%m%d%H%M')"
                ),
                "transaction_end_date": duckdb.SQLExpression(
                    "TRY_STRPTIME(transaction_end_date, '%Y%m%d%H%M')"
                ),
                "trade_date": duckdb.SQLExpression(
                    "TRY_STRPTIME(trade_date, '%Y%m%d')"
                ),
                "exchange_brokerage_service": _na_to_null("exchange_brokerage_service"),
                "type_of_rate": _na_to_null("type_of_rate"),
                "timezone": _na_to_null("time_zone"),
                "class_name": _na_to_null("class_name"),
                "term_name": _na_to_null("term_name"),
                "increment_name": _na_to_null("increment_name"),
                "increment_peaking_name": _na_to_null("increment_peaking_name"),
                "product_name": _na_to_null("product_name"),
                "rate_units": _na_to_null("rate_units"),
                "point_of_delivery_balancing_authority": duckdb.ColumnExpression(
                    "point_of_delivery_balancing_authority"
                ),
                "point_of_delivery_specific_location": duckdb.ColumnExpression(
                    "point_of_delivery_specific_location"
                ),
                "transaction_quantity": duckdb.ColumnExpression("transaction_quantity"),
                "price": duckdb.ColumnExpression("price"),
                "standardized_quantity": duckdb.ColumnExpression(
                    "standardized_quantity"
                ),
                "standardized_price": duckdb.ColumnExpression("standardized_price"),
                "total_transmission_charge": duckdb.ColumnExpression(
                    "total_transmission_charge"
                ),
            },
        )


@dg.asset(partitions_def=ferceqr_year_quarters)
def core_ferceqr__contracts(context, raw_ferceqr__contracts: ParquetData):
    """Perform basic transforms on contracts table table."""
    year_quarter = context.partition_key
    with duckdb_relation_from_parquet(
        raw_ferceqr__contracts, use_all_partitions=True
    ) as (table, _):
        return transform_eqr_table(
            table_name="core_ferceqr__contracts",
            table_data=table,
            year_quarter=year_quarter,
            col_expressions={
                "year_quarter": duckdb.ColumnExpression("year_quarter"),
                "seller_company_id_ferc": duckdb.ColumnExpression("company_identifier"),
                "contract_unique_id": duckdb.ColumnExpression("contract_unique_id"),
                "seller_company_name": duckdb.ColumnExpression("seller_company_name"),
                "customer_company_name": duckdb.ColumnExpression(
                    "customer_company_name"
                ),
                "contract_affiliate": (
                    duckdb.CaseExpression(
                        condition=duckdb.SQLExpression("UPPER(contract_affiliate)")
                        == duckdb.ConstantExpression("Y"),
                        value=True,
                    ).when(
                        condition=duckdb.SQLExpression("UPPER(contract_affiliate)")
                        == duckdb.ConstantExpression("N"),
                        value=False,
                    )
                ),
                "ferc_tariff_reference": duckdb.ColumnExpression(
                    "ferc_tariff_reference"
                ),
                "contract_service_agreement_id": duckdb.ColumnExpression(
                    "contract_service_agreement_id"
                ),
                "contract_execution_date": duckdb.SQLExpression(
                    "TRY_STRPTIME(contract_execution_date, '%Y%m%d')"
                ),
                "commencement_date_of_contract_term": duckdb.SQLExpression(
                    "TRY_STRPTIME(commencement_date_of_contract_term, '%Y%m%d')"
                ),
                "contract_termination_date": duckdb.SQLExpression(
                    "TRY_STRPTIME(contract_termination_date, '%Y%m%d')"
                ),
                "actual_termination_date": duckdb.SQLExpression(
                    "TRY_STRPTIME(actual_termination_date, '%Y%m%d')"
                ),
                "extension_provision_description": duckdb.ColumnExpression(
                    "extension_provision_description"
                ),
                "class_name": _na_to_null("class_name"),
                "term_name": _na_to_null("term_name"),
                "increment_name": _na_to_null("increment_name"),
                "increment_peaking_name": _na_to_null("increment_peaking_name"),
                "product_type_name": _na_to_null("product_type_name"),
                "product_name": _na_to_null("product_name"),
                "quantity": duckdb.ColumnExpression("quantity"),
                "units": _na_to_null("units"),
                "rate": duckdb.ColumnExpression("rate"),
                "rate_minimum": duckdb.ColumnExpression("rate_minimum"),
                "rate_maximum": duckdb.ColumnExpression("rate_maximum"),
                "rate_description": duckdb.ColumnExpression("rate_description"),
                "rate_units": _na_to_null("rate_units"),
                "point_of_receipt_balancing_authority": duckdb.ColumnExpression(
                    "point_of_receipt_balancing_authority"
                ),
                "point_of_receipt_specific_location": duckdb.ColumnExpression(
                    "point_of_receipt_specific_location"
                ),
                "point_of_delivery_balancing_authority": duckdb.ColumnExpression(
                    "point_of_delivery_balancing_authority"
                ),
                "point_of_delivery_specific_location": duckdb.ColumnExpression(
                    "point_of_delivery_specific_location"
                ),
                "begin_date": duckdb.SQLExpression(
                    "TRY_STRPTIME(begin_date, '%Y%m%d%H%M')"
                ),
                "end_date": duckdb.SQLExpression(
                    "TRY_STRPTIME(end_date, '%Y%m%d%H%M')"
                ),
            },
        )


@dg.asset(partitions_def=ferceqr_year_quarters)
def core_ferceqr__quarterly_index_pub(context, raw_ferceqr__index_pub: ParquetData):
    """Perform basic transforms on indexPub table table."""
    year_quarter = context.partition_key
    with duckdb_relation_from_parquet(
        raw_ferceqr__index_pub, use_all_partitions=True
    ) as (table, _):
        return transform_eqr_table(
            table_name="core_ferceqr__quarterly_index_pub",
            table_data=table,
            year_quarter=year_quarter,
            col_expressions={
                "year_quarter": duckdb.ColumnExpression("year_quarter"),
                "company_id_ferc": duckdb.ColumnExpression("company_identifier"),
                "filer_unique_id": duckdb.ColumnExpression("filer_unique_id"),
                "seller_company_name": duckdb.ColumnExpression("Seller_Company_Name"),
                "index_price_publisher_name": _na_to_null(
                    "Index_Price_Publishers_To_Which_Sales_Transactions_Have_Been_Reported"
                ),
                "transactions_reported": duckdb.ColumnExpression(
                    "Transactions_Reported"
                ),
            },
        )
