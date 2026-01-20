"""Table definitions for the FERC EQR data group."""

from typing import Any

TABLE_DESCRIPTIONS = {
    "identity": {
        "additional_summary_text": (
            "individuals who filed FERC EQR for a company in a given quarter."
        ),
        "additional_primary_key_text": (
            "The primary key ought to be ['year_quarter', 'company_id_ferc', "
            "'filer_unique_id'], where filer_unique_id is an employee-level ID. "
            "However, a handful of companies have erroneously reported the same "
            "filer_unique_id for multiple employees, resulting in duplicate records. "
            "In other cases, there appear to be multiple filings in a given quarter "
            "for the same company and filer, resulting in additional duplicates. Thus, "
            "there is no reliable natural primary key for the identity table."
        ),
        "usage_warnings": ["experimental_wip"],
    },
    "contracts": {
        "additional_summary_text": (
            "Contains information about contracts between companies selling"
            " and buying electricity market products."
        ),
        "additional_primary_key_text": "The FERC EQR contracts table has no natural primary key.",
        "usage_warnings": ["experimental_wip"],
    },
    "transactions": {
        "additional_summary_text": (
            "Contains information about individual electricity market transactions that took place"
            " during a given reporting quarter. Reported by the seller."
        ),
        "usage_warnings": ["experimental_wip"],
    },
    "index_pub": {
        "additional_summary_text": "electricity market price indices that individual EQR filers"
        " reported transactions to.",
        "usage_warnings": ["experimental_wip"],
    },
}

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_ferceqr__quarterly_identity": {
        "description": TABLE_DESCRIPTIONS["identity"],
        "schema": {
            "fields": [
                "year_quarter",
                "company_id_ferc",
                "filer_unique_id",
                "company_name",
                "contact_name",
                "contact_title",
                "contact_address",
                "contact_city",
                "contact_state",
                "contact_zip",
                "contact_country_name",
                "contact_phone",
                "contact_email",
                "transactions_reported_to_index_price_publishers",
            ],
        },
        "create_database_schema": False,
        "sources": ["ferceqr"],
        "etl_group": "ferceqr",
        "field_namespace": "ferceqr",
        "output_partition_source_key": {
            "datasource": "ferceqr",
            "working_partition_key": "year_quarters",
        },
    },
    "core_ferceqr__contracts": {
        "description": TABLE_DESCRIPTIONS["contracts"],
        "schema": {
            "fields": [
                "year_quarter",
                "seller_company_id_ferc",
                "contract_unique_id",
                "seller_company_name",
                "customer_company_name",
                "contract_affiliate",
                "ferc_tariff_reference",
                "contract_service_agreement_id",
                "contract_execution_date",
                "commencement_date_of_contract_term",
                "contract_termination_date",
                "actual_termination_date",
                "extension_provision_description",
                "class_name",
                "term_name",
                "increment_name",
                "increment_peaking_name",
                "product_type_name",
                "product_name",
                "quantity",
                "units",
                "rate",
                "rate_minimum",
                "rate_maximum",
                "rate_description",
                "rate_units",
                "point_of_receipt_balancing_authority",
                "point_of_receipt_specific_location",
                "point_of_delivery_balancing_authority",
                "point_of_delivery_specific_location",
                "begin_date",
                "end_date",
            ],
        },
        "create_database_schema": False,
        "sources": ["ferceqr"],
        "etl_group": "ferceqr",
        "field_namespace": "ferceqr",
        "output_partition_source_key": {
            "datasource": "ferceqr",
            "working_partition_key": "year_quarters",
        },
    },
    "core_ferceqr__transactions": {
        "description": TABLE_DESCRIPTIONS["transactions"],
        "schema": {
            "fields": [
                "year_quarter",
                "seller_company_id_ferc",
                "transaction_unique_id",
                "seller_company_name",
                "customer_company_name",
                "ferc_tariff_reference",
                "contract_service_agreement_id",
                "seller_transaction_id",
                "transaction_begin_date",
                "transaction_end_date",
                "trade_date",
                "exchange_brokerage_service",
                "type_of_rate",
                "timezone",
                "class_name",
                "term_name",
                "increment_name",
                "increment_peaking_name",
                "product_name",
                "rate_units",
                "point_of_delivery_balancing_authority",
                "point_of_delivery_specific_location",
                "transaction_quantity",
                "price",
                "standardized_quantity",
                "standardized_price",
                "total_transmission_charge",
                "total_transaction_charge",
            ],
            "primary_key": [
                "year_quarter",
                "seller_company_id_ferc",
                "transaction_unique_id",
            ],
        },
        "create_database_schema": False,
        "sources": ["ferceqr"],
        "etl_group": "ferceqr",
        "field_namespace": "ferceqr",
        "output_partition_source_key": {
            "datasource": "ferceqr",
            "working_partition_key": "year_quarters",
        },
    },
    "core_ferceqr__quarterly_index_pub": {
        "description": TABLE_DESCRIPTIONS["index_pub"],
        "schema": {
            "fields": [
                "year_quarter",
                "company_id_ferc",
                "filer_unique_id",
                "seller_company_name",
                "index_price_publisher_name",
                "transactions_reported",
            ],
            "primary_key": [
                "year_quarter",
                "company_id_ferc",
                "filer_unique_id",
            ],
        },
        "create_database_schema": False,
        "sources": ["ferceqr"],
        "etl_group": "ferceqr",
        "field_namespace": "ferceqr",
        "output_partition_source_key": {
            "datasource": "ferceqr",
            "working_partition_key": "year_quarters",
        },
    },
}
