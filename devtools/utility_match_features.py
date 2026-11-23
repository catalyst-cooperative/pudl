"""Numeric utility-year features shared by the FERC1-EIA utility matching tools.

Two features are used because they are reported on both sides with almost no
interpretation needed, and they agree closely for utilities the crosswalk already links:

- ``customers``: FERC Form 1 average customers per month for sales to ultimate
  consumers (schedule 300) vs. EIA-861 customers summed over every row of the yearly
  sales table.
- ``sales_mwh``: FERC Form 1 MWh sold to ultimate consumers (schedule 300) vs. EIA-861
  sales MWh summed over every row of the yearly sales table.

On crosswalk-linked pairs the two sides agree within 2% in about 90% of utility-years
for customers and about 85% for sales, against about 0.5% for random pairs. That
comparison is misleading on its own, though: utilities of similar size also agree, and
sizes drift together over the years. With customers within 5% and sales within 10%
only about a third of agreeing utility-year pairs are crosswalk-linked; with customers
within 2% and sales within 3% about three quarters are (82% of linked utility-years
still pass). So tight tolerances, and agreement in several years, matter. EIA values
are summed over all service types, customer classes, states and balancing
authorities, with no filtering, because that matched better than any subset.
"""

from collections.abc import Callable

import pandas as pd

FEATURE_COLS = ["customers", "sales_mwh"]
# Smaller values are placeholders (FERC reports 1 customer for wholesale-only
# utilities, for example) and agree by coincidence, so they are treated as missing.
MINIMUM_VALUES = {"customers": 100, "sales_mwh": 1000}


def load_numeric_features(
    load_table: Callable[[str], pd.DataFrame],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return utility-year ``customers`` and ``sales_mwh`` for FERC1 and EIA.

    Args:
        load_table: Function that loads a PUDL table by name.

    Returns:
        ``(ferc_features, eia_features)`` keyed by ``utility_id_ferc1`` /
        ``utility_id_eia`` and ``report_year``. Values below ``MINIMUM_VALUES`` are
        set to null.
    """
    revenues = load_table("core_ferc1__yearly_operating_revenues_sched300")
    ferc = (
        revenues.loc[revenues["revenue_type"] == "sales_to_ultimate_consumers"]
        .rename(columns={"avg_customers_per_month": "customers"})
        .groupby(["utility_id_ferc1", "report_year"], as_index=False)[FEATURE_COLS]
        .sum(min_count=1)
    )
    sales = load_table("core_eia861__yearly_sales")
    sales = sales.assign(report_year=pd.to_datetime(sales["report_date"]).dt.year)
    eia = sales.groupby(["utility_id_eia", "report_year"], as_index=False)[
        FEATURE_COLS
    ].sum(min_count=1)
    for frame in (ferc, eia):
        for col in FEATURE_COLS:
            frame[col] = frame[col].where(frame[col] >= MINIMUM_VALUES[col])
    return ferc, eia


def numeric_comparisons() -> list:
    """Splink comparisons on the numeric features (null, then closeness levels)."""
    import splink.comparison_level_library as cll
    import splink.comparison_library as cl

    return [
        cl.CustomComparison(
            [
                cll.NullLevel(col),
                *[
                    cll.PercentageDifferenceLevel(col, threshold)
                    for threshold in (0.01, 0.025, 0.05, 0.10, 0.25)
                ],
                cll.ElseLevel(),
            ],
            output_column_name=col,
            comparison_description=f"{col} percentage difference",
        )
        for col in FEATURE_COLS
    ]


def numeric_blocking_rule():
    """Splink blocking rule: same year, customers within 2.5%, sales within 5%."""
    from splink.blocking_rule_library import CustomRule

    return CustomRule(
        "l.report_year = r.report_year "
        "and l.customers is not null and r.customers is not null "
        "and abs(l.customers - r.customers) "
        "<= 0.025 * greatest(l.customers, r.customers) "
        "and (l.sales_mwh is null or r.sales_mwh is null "
        "or abs(l.sales_mwh - r.sales_mwh) "
        "<= 0.05 * greatest(l.sales_mwh, r.sales_mwh))"
    )
