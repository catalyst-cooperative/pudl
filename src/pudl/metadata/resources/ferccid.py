"""Table definitions for the FERC Company Identifier table."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_ferccid__data": {
        "description": {
            "additional_summary_text": "the companies that submit required filings to FERC."
        },
        "schema": {
            "fields": [
                "company_id_ferccid",
                "company_name",
                "program",
                "company_website",
                "street_address",
                "address_2",
                "city",
                "state",
                "zip_code",
                "zip_code_4",
            ],
            "primary_key": [
                "company_id_ferccid",
            ],
        },
        "sources": ["ferccid"],
        "etl_group": "ferccid",
        "field_namespace": "ferccid",
    },
}
