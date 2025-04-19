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
                "office_street_address",
                "office_city",
                "office_county",
                "office_zip",
                "office_state",
                "headquarters_street_address",
                "headquarters_city",
                "headquarters_county",
                "headquarters_state",
                "headquarters_zip",
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
                "unaccounted_for_gas_fraction",
                "additional_information",
                "preparer_email",
                "preparer_fax",
                "preparer_name",
                "preparer_phone",
                "preparer_title",
            ],
            "primary_key": ["report_number"],
        },
        "sources": ["phmsagas"],
        "field_namespace": "phmsagas",
        "etl_group": "phmsagas",
    }
}
