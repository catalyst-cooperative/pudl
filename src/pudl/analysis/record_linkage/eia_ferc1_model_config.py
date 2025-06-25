"""The model parameters for the FERC1 to EIA splink record linkage model.

This module enumerates the blocking rules as well as the comparison levels
for the matching columns that are used in the FERC1 to EIA record linkage
model.
"""

import splink.comparison_level_library as cll
import splink.comparison_library as cl
from splink import block_on

blocking_rule_1 = "l.report_year = r.report_year and substr(l.plant_name_mphone,1,3) = substr(r.plant_name_mphone,1,3)"
blocking_rule_2 = "l.report_year = r.report_year and substr(l.utility_name_mphone,1,2) = substr(r.utility_name_mphone,1,2) and substr(l.plant_name_mphone,1,2) = substr(r.plant_name_mphone,1,2)"
blocking_rule_3 = "l.report_year = r.report_year and l.installation_year = r.installation_year and substr(l.utility_name_mphone,1,2) = substr(r.utility_name_mphone,1,2)"
blocking_rule_4 = "l.report_year = r.report_year and l.fuel_type_code_pudl = r.fuel_type_code_pudl and substr(l.plant_name_mphone,1,2) = substr(r.plant_name_mphone,1,2)"
blocking_rule_5 = "l.report_year = r.report_year and l.fuel_type_code_pudl = r.fuel_type_code_pudl and substr(l.utility_name_mphone,1,3) = substr(r.utility_name_mphone,1,3)"
blocking_rule_6 = "l.report_year = r.report_year and l.construction_year = r.construction_year and substr(l.utility_name_mphone,1,2) = substr(r.utility_name_mphone,1,2)"
blocking_rule_7 = "l.report_year = r.report_year and l.capacity_mw = r.capacity_mw and substr(l.plant_name_mphone,1,2) = substr(r.plant_name_mphone,1,2)"
blocking_rule_8 = "l.report_year = r.report_year and l.installation_year = r.installation_year and substr(l.plant_name_mphone,1,2) = substr(r.plant_name_mphone,1,2)"
blocking_rule_9 = "l.report_year = r.report_year and l.construction_year = r.construction_year and substr(l.plant_name_mphone,1,2) = substr(r.plant_name_mphone,1,2)"
blocking_rule_10 = block_on("report_year", "net_generation_mwh")
BLOCKING_RULES = [
    blocking_rule_1,
    blocking_rule_2,
    blocking_rule_3,
    blocking_rule_4,
    blocking_rule_5,
    blocking_rule_6,
    blocking_rule_7,
    blocking_rule_8,
    blocking_rule_9,
    blocking_rule_10,
]

plant_name_comparison = cl.NameComparison(
    "plant_name",
    jaro_winkler_thresholds=[0.9, 0.8, 0.7],
)
utility_name_comparison = cl.NameComparison(
    "utility_name",
    jaro_winkler_thresholds=[0.9, 0.8, 0.7],
)
utility_name_comparison.configure(term_frequency_adjustments=True)
fuel_type_code_pudl_comparison = cl.ExactMatch("fuel_type_code_pudl")
fuel_type_code_pudl_comparison.configure(term_frequency_adjustments=True)

capacity_comparison = {
    "output_column_name": "capacity_mw",
    "comparison_levels": [
        cll.NullLevel("capacity_mw"),
        cll.PercentageDifferenceLevel(
            "capacity_mw",
            0.0 + 1e-4,
        ),
        cll.PercentageDifferenceLevel("capacity_mw", 0.05),
        cll.PercentageDifferenceLevel("capacity_mw", 0.1),
        cll.PercentageDifferenceLevel("capacity_mw", 0.2),
        cll.ElseLevel(),
    ],
    "comparison_description": "0% different vs. 5% different vs. 10% different vs. 20% different vs. anything else",
}

net_gen_comparison = {
    "output_column_name": "net_generation_mwh",
    "comparison_levels": [
        cll.NullLevel("net_generation_mwh"),
        cll.PercentageDifferenceLevel(
            "net_generation_mwh", 0.0 + 1e-4
        ),  # could add an exact match level too
        cll.PercentageDifferenceLevel("net_generation_mwh", 0.01),
        cll.PercentageDifferenceLevel("net_generation_mwh", 0.1),
        cll.PercentageDifferenceLevel("net_generation_mwh", 0.2),
        cll.ElseLevel(),
    ],
    "comparison_description": "0% different vs. 1% different vs. 10% different vs. 20% different vs. anything else",
}


def get_date_comparison(column_name):
    """Get date comparison template for column."""
    return cl.DateOfBirthComparison(
        column_name,
        input_is_string=False,
        datetime_thresholds=[1, 2],
        datetime_metrics=["year", "year"],
    )


installation_year_comparison = get_date_comparison("installation_year")
construction_year_comparison = get_date_comparison("construction_year")

COMPARISONS = [
    plant_name_comparison,
    utility_name_comparison,
    construction_year_comparison,
    installation_year_comparison,
    capacity_comparison,
    fuel_type_code_pudl_comparison,
    net_gen_comparison,
]
