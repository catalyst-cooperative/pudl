"""The model parameters for the FERC1 to EIA splink record linkage model.

This module enumerates the blocking rules as well as the comparison levels
for the matching columns that are used in the FERC1 to EIA record linkage
model.
"""

import splink.comparison_level_library as cll
import splink.comparison_library as cl
from splink.blocking_rule_library import CustomRule

blocking_rule_1 = CustomRule(
    "l.report_year = r.report_year and substr(l.plant_name_mphone,1,3) = substr(r.plant_name_mphone,1,3)"
)
blocking_rule_2 = CustomRule(
    "l.report_year = r.report_year and substr(l.utility_name_mphone,1,2) = substr(r.utility_name_mphone,1,2) and substr(l.plant_name_mphone,1,2) = substr(r.plant_name_mphone,1,2)"
)
blocking_rule_3 = CustomRule(
    "l.report_year = r.report_year and l.installation_year = r.installation_year and substr(l.utility_name_mphone,1,2) = substr(r.utility_name_mphone,1,2)"
)
blocking_rule_4 = CustomRule(
    "l.report_year = r.report_year and l.fuel_type_code_pudl = r.fuel_type_code_pudl and substr(l.plant_name_mphone,1,2) = substr(r.plant_name_mphone,1,2)"
)
blocking_rule_5 = CustomRule(
    "l.report_year = r.report_year and l.fuel_type_code_pudl = r.fuel_type_code_pudl and substr(l.utility_name_mphone,1,3) = substr(r.utility_name_mphone,1,3)"
)
blocking_rule_6 = CustomRule(
    "l.report_year = r.report_year and l.construction_year = r.construction_year and substr(l.utility_name_mphone,1,2) = substr(r.utility_name_mphone,1,2)"
)
blocking_rule_7 = CustomRule(
    "l.report_year = r.report_year and round(l.capacity_mw) = round(r.capacity_mw) and substr(l.plant_name_mphone,1,2) = substr(r.plant_name_mphone,1,2)"
)
blocking_rule_8 = CustomRule(
    "l.report_year = r.report_year and l.installation_year = r.installation_year and substr(l.plant_name_mphone,1,2) = substr(r.plant_name_mphone,1,2)"
)
blocking_rule_9 = CustomRule(
    "l.report_year = r.report_year and l.construction_year = r.construction_year and substr(l.plant_name_mphone,1,2) = substr(r.plant_name_mphone,1,2)"
)
blocking_rule_10 = CustomRule(
    "l.report_year = r.report_year and round(l.net_generation_mwh) = round(r.net_generation_mwh)"
)
# Rules 7 and 10 block on values rounded to the nearest integer, rather than on exact
# floating point equality. The same quantity is often reported at different precisions
# (or summed in a different order) in EIA and FERC, so exactly equal floats miss many
# true matches. The rounding is only used for blocking; the comparison levels still use
# the unrounded values.
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


def get_capacity_comparison() -> cl.CustomComparison:
    """Get the comparison of plant capacity."""
    return cl.CustomComparison(
        output_column_name="capacity_mw",
        comparison_levels=[
            cll.NullLevel("capacity_mw"),
            cll.PercentageDifferenceLevel("capacity_mw", 0.0 + 1e-4),
            cll.PercentageDifferenceLevel("capacity_mw", 0.05),
            cll.PercentageDifferenceLevel("capacity_mw", 0.1),
            cll.PercentageDifferenceLevel("capacity_mw", 0.2),
            cll.ElseLevel(),
        ],
        comparison_description="0% different vs. 5% different vs. 10% different vs. 20% different vs. anything else",
    )


def get_net_gen_comparison() -> cl.CustomComparison:
    """Get the comparison of net generation."""
    return cl.CustomComparison(
        output_column_name="net_generation_mwh",
        comparison_levels=[
            cll.NullLevel("net_generation_mwh"),
            # could add an exact match level too
            cll.PercentageDifferenceLevel("net_generation_mwh", 0.0 + 1e-4),
            cll.PercentageDifferenceLevel("net_generation_mwh", 0.01),
            cll.PercentageDifferenceLevel("net_generation_mwh", 0.1),
            cll.PercentageDifferenceLevel("net_generation_mwh", 0.2),
            cll.ElseLevel(),
        ],
        comparison_description="0% different vs. 1% different vs. 10% different vs. 20% different vs. anything else",
    )


def get_date_comparison(column_name: str) -> cl.DateOfBirthComparison:
    """Get date comparison template for column."""
    return cl.DateOfBirthComparison(
        column_name,
        input_is_string=False,
        datetime_thresholds=[1, 2],
        datetime_metrics=["year", "year"],
    )


def get_comparisons() -> list[cl.ComparisonCreator]:
    """Build a fresh list of the model's comparisons.

    Comparison objects are configured in place (e.g. term frequency adjustments), so we
    construct new ones on every call rather than sharing module-level instances.
    """
    return [
        cl.NameComparison("plant_name", jaro_winkler_thresholds=[0.9, 0.8, 0.7]),
        cl.NameComparison(
            "utility_name", jaro_winkler_thresholds=[0.9, 0.8, 0.7]
        ).configure(term_frequency_adjustments=True),
        get_date_comparison("construction_year"),
        get_date_comparison("installation_year"),
        get_capacity_comparison(),
        cl.ExactMatch("fuel_type_code_pudl").configure(term_frequency_adjustments=True),
        get_net_gen_comparison(),
    ]
