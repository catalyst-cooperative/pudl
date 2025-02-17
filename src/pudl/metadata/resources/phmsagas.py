"""Tables definitions for data coming from PHMSA natural gas data."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_phmsagas__yearly_distribution_operators": {
        "description": (
            "This table contains operator-level natural gas distribution"
            "data, corresponding to Parts A and D-I of the 2023 PHMSA gas "
            "distribution system annual report. That includes data on the "
            "operator name and location, the type of operator "
            "(e.g., investor-owned, municipally-owned), the type of gas "
            "being transported, and information on system-wide excavation "
            "damage, leaks, and unaccounted for gas. Each row corresponds "
            "to one report for one operator in one state, where revisions "
            "to the original submission have a different report ID."
        ),
        "schema": {
            "fields": [
                "report_date",
                "report_number",
                "report_submission_type",
                "report_year",
                "operator_id_phmsa",
                "operator_name_phmsa",
                "office_address_street",
                "office_address_city",
                "office_address_state",
                "office_address_zip",
                "office_address_county",
                "headquarters_address_street",
                "headquarters_address_city",
                "headquarters_address_state",
                "headquarters_address_zip",
                "headquarters_address_county",
                "excavation_damage_excavation_practices",
                "excavation_damage_locating_practices",
                "excavation_damage_one_call_notification",
                "excavation_damage_other",
                "excavation_damage_total",
                "excavation_tickets",
                "services_efv_in_system",
                "services_efv_installed",
                "services_shutoff_valve_in_system",
                "services_shutoff_valve_installed",
                "federal_land_leaks_repaired_or_scheduled",
                "percent_unaccounted_for_gas",
                "additional_information",
                "preparer_email",
                "preparer_fax",
                "preparer_name",
                "preparer_phone",
                "preparer_title",
            ],
            "primary_key": ["operator_id_phmsa", "report_number"],
        },
        "sources": ["phmsagas"],
        "field_namespace": "phmsagas",
        "etl_group": "phmsagas",
    }
}
