"""Definitions of data tables primarily coming from EIA-861."""
from typing import Any, Dict

RESOURCE_METADATA: Dict[str, Dict[str, Any]] = {
    # "advanced_metering_infrastructure_eia861": {
    #     "description": "",
    #     "schema": {
    #         "fields": [],
    #         "primary_key": [],
    #         "foreign_key_rules": {
    #             "fields": [[]]
    #         },
    #     },
    #     "group": "eia",
    #     "sources": ["eia861"],
    # },
    "balancing_authority_assn_eia861": {
        "description": "",
        "schema": {
            "fields": [
                'report_date',
                'balancing_authority_id_eia',
                'utility_id_eia',
                'state',
            ],
            "primary_key": [
                'report_date',
                'balancing_authority_id_eia',
                'utility_id_eia',
                'state',
            ],
            "foreign_key_rules": {
                "fields": [[]]
            },
        },
        "group": "eia",
        "sources": ["eia861"],
    },
    "balancing_authority_eia861": {
        "description": "",
        "schema": {
            "fields": [
                'report_date',
                'balancing_authority_id_eia',
                'balancing_authority_code_eia',
                'balancing_authority_name_eia',
            ],
            "primary_key": [
                'report_date',
                'balancing_authority_id_eia',
            ],
            "foreign_key_rules": {
                "fields": [[]]
            },
        },
        "group": "eia",
        "sources": ["eia861"],
    },
    # "demand_response_eia861": {
    #     "description": "",
    #     "schema": {
    #         "fields": [],
    #         "primary_key": [],
    #         "foreign_key_rules": {
    #             "fields": [[]]
    #         },
    #     },
    #     "group": "eia",
    #     "sources": ["eia861"],
    # },
    # "demand_response_water_heater_eia861": {
    #     "description": "",
    #     "schema": {
    #         "fields": [],
    #         "primary_key": [],
    #         "foreign_key_rules": {
    #             "fields": [[]]
    #         },
    #     },
    #     "group": "eia",
    #     "sources": ["eia861"],
    # },
    # "demand_side_management_ee_dr_eia861": {
    #     "description": "",
    #     "schema": {
    #         "fields": [],
    #         "primary_key": [],
    #         "foreign_key_rules": {
    #             "fields": [[]]
    #         },
    #     },
    #     "group": "eia",
    #     "sources": ["eia861"],
    # },
    # "demand_side_management_misc_eia861": {
    #     "description": "",
    #     "schema": {
    #         "fields": [],
    #         "primary_key": [],
    #         "foreign_key_rules": {
    #             "fields": [[]]
    #         },
    #     },
    #     "group": "eia",
    #     "sources": ["eia861"],
    # },
    # "demand_side_management_sales_eia861": {
    #     "description": "",
    #     "schema": {
    #         "fields": [],
    #         "primary_key": [],
    #         "foreign_key_rules": {
    #             "fields": [[]]
    #         },
    #     },
    #     "group": "eia",
    #     "sources": ["eia861"],
    # },
    # "distributed_generation_fuel_eia861": {
    #     "description": "",
    #     "schema": {
    #         "fields": [],
    #         "primary_key": [],
    #         "foreign_key_rules": {
    #             "fields": [[]]
    #         },
    #     },
    #     "group": "eia",
    #     "sources": ["eia861"],
    # },
    # "distributed_generation_misc_eia861": {
    #     "description": "",
    #     "schema": {
    #         "fields": [],
    #         "primary_key": [],
    #         "foreign_key_rules": {
    #             "fields": [[]]
    #         },
    #     },
    #     "group": "eia",
    #     "sources": ["eia861"],
    # },
    # "distributed_generation_tech_eia861": {
    #     "description": "",
    #     "schema": {
    #         "fields": [],
    #         "primary_key": [],
    #         "foreign_key_rules": {
    #             "fields": [[]]
    #         },
    #     },
    #     "group": "eia",
    #     "sources": ["eia861"],
    # },
    # "distribution_systems_eia861": {
    #     "description": "",
    #     "schema": {
    #         "fields": [],
    #         "primary_key": [],
    #         "foreign_key_rules": {
    #             "fields": [[]]
    #         },
    #     },
    #     "group": "eia",
    #     "sources": ["eia861"],
    # },
    # "dynamic_pricing_eia861": {
    #     "description": "",
    #     "schema": {
    #         "fields": [],
    #         "primary_key": [],
    #         "foreign_key_rules": {
    #             "fields": [[]]
    #         },
    #     },
    #     "group": "eia",
    #     "sources": ["eia861"],
    # },
    # "energy_efficiency_eia861": {
    #     "description": "",
    #     "schema": {
    #         "fields": [],
    #         "primary_key": [],
    #         "foreign_key_rules": {
    #             "fields": [[]]
    #         },
    #     },
    #     "group": "eia",
    #     "sources": ["eia861"],
    # },
    # "green_pricing_eia861": {
    #     "description": "",
    #     "schema": {
    #         "fields": [],
    #         "primary_key": [],
    #         "foreign_key_rules": {
    #             "fields": [[]]
    #         },
    #     },
    #     "group": "eia",
    #     "sources": ["eia861"],
    # },
    # "mergers_eia861": {
    #     "description": "",
    #     "schema": {
    #         "fields": [],
    #         "primary_key": [],
    #         "foreign_key_rules": {
    #             "fields": [[]]
    #         },
    #     },
    #     "group": "eia",
    #     "sources": ["eia861"],
    # },
    # "net_metering_customer_fuel_class_eia861": {
    #     "description": "",
    #     "schema": {
    #         "fields": [],
    #         "primary_key": [],
    #         "foreign_key_rules": {
    #             "fields": [[]]
    #         },
    #     },
    #     "group": "eia",
    #     "sources": ["eia861"],
    # },
    # "net_metering_misc_eia861": {
    #     "description": "",
    #     "schema": {
    #         "fields": [],
    #         "primary_key": [],
    #         "foreign_key_rules": {
    #             "fields": [[]]
    #         },
    #     },
    #     "group": "eia",
    #     "sources": ["eia861"],
    # },
    # "non_net_metering_customer_fuel_class_eia861": {
    #     "description": "",
    #     "schema": {
    #         "fields": [],
    #         "primary_key": [],
    #         "foreign_key_rules": {
    #             "fields": [[]]
    #         },
    #     },
    #     "group": "eia",
    #     "sources": ["eia861"],
    # },
    # "non_net_metering_misc_eia861": {
    #     "description": "",
    #     "schema": {
    #         "fields": [],
    #         "primary_key": [],
    #         "foreign_key_rules": {
    #             "fields": [[]]
    #         },
    #     },
    #     "group": "eia",
    #     "sources": ["eia861"],
    # },
    # "operational_data_misc_eia861": {
    #     "description": "",
    #     "schema": {
    #         "fields": [],
    #         "primary_key": [],
    #         "foreign_key_rules": {
    #             "fields": [[]]
    #         },
    #     },
    #     "group": "eia",
    #     "sources": ["eia861"],
    # },
    # "operational_data_revenue_eia861": {
    #     "description": "",
    #     "schema": {
    #         "fields": [],
    #         "primary_key": [],
    #         "foreign_key_rules": {
    #             "fields": [[]]
    #         },
    #     },
    #     "group": "eia",
    #     "sources": ["eia861"],
    # },
    # "reliability_eia861": {
    #     "description": "",
    #     "schema": {
    #         "fields": [],
    #         "primary_key": [],
    #         "foreign_key_rules": {
    #             "fields": [[]]
    #         },
    #     },
    #     "group": "eia",
    #     "sources": ["eia861"],
    # },
    "sales_eia861": {
        "description": "",
        "schema": {
            "fields": [
                'utility_id_eia',
                'state',
                'report_date',
                'balancing_authority_code_eia',
                'business_model',
                'data_observed',
                'entity_type',
                'service_type',
                'short_form',
                'utility_name_eia',
                'customer_class',
                'customers',
                'sales_mwh',
                'sales_revenue'
            ],
            "primary_key": [],
            "foreign_key_rules": {
                "fields": [[]]
            },
        },
        "group": "eia",
        "sources": ["eia861"],
    },
    "service_territory_eia861": {
        "description": "",
        "schema": {
            "fields": [],
            "primary_key": [],
            "foreign_key_rules": {
                "fields": [[]]
            },
        },
        "group": "eia",
        "sources": ["eia861"],
    },
    "utility_assn_eia861": {
        "description": "",
        "schema": {
            "fields": [],
            "primary_key": [],
            "foreign_key_rules": {
                "fields": [[]]
            },
        },
        "group": "eia",
        "sources": ["eia861"],
    },
    # "utility_data_misc_eia861": {
    #     "description": "",
    #     "schema": {
    #         "fields": [],
    #         "primary_key": [],
    #         "foreign_key_rules": {
    #             "fields": [[]]
    #         },
    #     },
    #     "group": "eia",
    #     "sources": ["eia861"],
    # },
    # "utility_data_nerc_eia861": {
    #     "description": "",
    #     "schema": {
    #         "fields": [],
    #         "primary_key": [],
    #         "foreign_key_rules": {
    #             "fields": [[]]
    #         },
    #     },
    #     "group": "eia",
    #     "sources": ["eia861"],
    # },
    # "utility_data_rto_eia861": {
    #     "description": "",
    #     "schema": {
    #         "fields": [],
    #         "primary_key": [],
    #         "foreign_key_rules": {
    #             "fields": [[]]
    #         },
    #     },
    #     "group": "eia",
    #     "sources": ["eia861"],
    # },
}
