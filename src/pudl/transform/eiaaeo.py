"""Transform raw AEO tables into normalized assets.

Raw AEO tables often contain many different types of data which are split out
along different dimensions. For example, one table may contain generation
split out by fuel type as well as prices split out by service category.

As a result, we need to split these large tables into smaller tables that have
more uniform data, which we do by filtering the large table to its relevant
subsets, and then transforming some human-readable string fields into useful
metadata fields.
"""

import numpy as np
import pandas as pd
from dagster import asset


def __sanitize(series: pd.Series) -> pd.Series:
    return series.str.lower().str.strip().str.replace(r"\W+", "_", regex=True)


def get_series_info(series_name: pd.Series) -> pd.DataFrame:
    """Break human-readable series name into machine-readable fields.

    The series name contains several comma-separated fields: the variable,
    the region, the case, and the report year.

    The variable then contains its own colon-separated fields: a general topic,
    a less general subtopic, and specific variable name. It may also contain a
    fourth field for a specific dimension such as fuel type.
    """
    fields = series_name.str.split(",", expand=True).rename(
        columns={
            0: "variable_fields",
            1: "region_series",
            2: "case_series",
            3: "report_year_series",
        }
    )
    variable_fields = fields.variable_fields.str.split(" : ", expand=True).rename(
        columns={0: "topic", 1: "subtopic", 2: "variable_name", 3: "dimension"}
    )
    return fields.join(variable_fields)


def get_category_info(category_name: pd.Series) -> pd.Series:
    """Break human-readable category name into machine-readable fields.

    Fortunately the only field we're pulling out of the category so far is the
    region, which is the last of two comma-separated fields.
    """
    fields = category_name.str.rsplit(",", n=1).str.get(-1).rename("region_category")
    return fields


@asset
def core_eiaaeo__electric_sector_generation_capacity_by_technology(
    raw_eiaaeo__electric_power_projections_regional,
):
    """Projected net summer generation capacity and additions/retirements."""
    # TODO 2024-04-20: we need to add some asset checks!
    relevant_series_names = (
        "Electricity : Electric Power Sector : Capacity",
        "Electricity : Electric Power Sector : Cumulative Retirements",
        "Electricity : Electric Power Sector : Cumulative Planned Additions",
        "Electricity : Electric Power Sector : Cumulative Unplanned Additions",
        "Electricity : Electric Power Sector : Cumulative Total Additions",
    )

    # TODO 2024-04-24: this thru sanitized could maybe be pulled out into its own utility function
    filtered = raw_eiaaeo__electric_power_projections_regional.loc[
        raw_eiaaeo__electric_power_projections_regional.series_name.str.startswith(
            relevant_series_names
        )
    ]

    enriched = pd.concat(
        [
            filtered,
            get_series_info(filtered.series_name),
            get_category_info(filtered.category_name),
        ],
        axis="columns",
    )

    sanitized = enriched.assign(
        **{
            colname: __sanitize(enriched[colname])
            for colname in enriched.columns
            if colname not in {"projection_year", "value"}
        }
    ).pipe(
        lambda df: df.assign(
            report_year_series=df.report_year_series.str.replace("aeo", ""),
            value=pd.to_numeric(df.value.str.replace("- -", "")),
            dimension=df.dimension.str.replace(r"^total.*$", "total", regex=True),
        )
    )

    # TODO 2024-04-20: one day we'll include report year in the extract step -
    # at that point we'll have to compare that with the report year extracted
    # from the series name.
    assert (
        sanitized.region_series == sanitized.region_category
    ).all(), "Series and category must agree on region!"
    assert (
        sanitized.case_series == sanitized.modeling_case_aeo
    ).all(), "Series and taxonomy must agree on case!"
    assert set(sanitized.topic.unique()) == {"electricity"}
    assert set(sanitized.subtopic.unique()) == {"electric_power_sector"}
    assert set(sanitized.units.unique()) == {"gw"}
    assert sanitized.category_name.str.startswith("table_54_").all()

    trimmed = sanitized.drop(
        columns=[
            "series_name",
            "category_name",
            "variable_fields",
            "region_series",
            "case_series",
            "topic",
            "subtopic",
            "units",
        ]
    ).rename(
        columns={
            "region_category": "electricity_market_module_region_aeo",
            "report_year_series": "report_year",
            "dimension": "technology_description_aeo",
        }
    )

    unstacked = trimmed.set_index(
        [
            "report_year",
            "modeling_case_aeo",
            "electricity_market_module_region_aeo",
            "projection_year",
            "variable_name",
            "technology_description_aeo",
        ]
    ).unstack(level="variable_name")
    unstacked.columns = unstacked.columns.droplevel()

    # TODO 2024-04-24: decumulate the cumulative columns

    # TODO 2024-04-23: if we ever find that the units are no longer uniformly
    # "gw" then we need to do a more unit-aware conversion to MW.
    for gw_col in [
        "capacity",
        "planned_additions",
        "unplanned_additions",
        "total_additions",
        "retirements",
    ]:
        unstacked[gw_col] *= 1000

    ratio_close_additions_to_total, ratio_close_reported_calculated = (
        check_totals_add_up(unstacked)
    )
    assert 0.999 < ratio_close_additions_to_total <= 1.0
    assert 0.999 < ratio_close_reported_calculated <= 1.0

    # TODO 2024-04-24: drop the totals now that we've verified that they're OK.

    return unstacked.rename(
        columns={
            "capacity": "summer_capacity_mw",
            "unplanned_additions": "summer_capacity_unplanned_additions_mw",
            "planned_additions": "summer_capacity_planned_additions_mw",
            "total_additions": "summer_capacity_total_additions_mw",
            "retirements": "summer_capacity_retirements_mw",
        }
    )


def check_totals_add_up(capacity) -> tuple[float, float]:
    """Check that reported totals match with calculated totals.

    We want to check that:

    * the "total" value is equal to the sum of the non-total values for
      "cumulative planned/unplanned additions/retirements."
    * cumulative planned + unplanned additions == cumulative total additions
    """
    grouped_by_variable = capacity.reset_index().groupby(
        [
            "report_year",
            "modeling_case_aeo",
            "electricity_market_module_region_aeo",
            "projection_year",
        ]
    )

    additions_retirements = [
        "planned_additions",
        "unplanned_additions",
        "retirements",
    ]

    # TODO 2024-04-23 (daz): this is slow as hell, those groupby.applies are
    # killing us.
    addition_retirement_reported_totals = grouped_by_variable.apply(
        lambda df: df.loc[
            df.technology_description_aeo == "total", additions_retirements
        ].sum()
    )

    addition_retirement_calculated_totals = grouped_by_variable.apply(
        lambda df: df.loc[
            df.technology_description_aeo.notna()
            & (df.technology_description_aeo != "total"),
            additions_retirements,
        ].sum()
    )

    total_additions_reported = (
        capacity.xs(pd.NA, level="technology_description_aeo").fillna(0).total_additions
    )

    ratio_close_reported_calculated = (
        np.isclose(
            addition_retirement_reported_totals, addition_retirement_calculated_totals
        ).sum()
        / addition_retirement_calculated_totals.size
    )

    ratio_close_additions_to_total = (
        np.isclose(
            addition_retirement_reported_totals.planned_additions
            + addition_retirement_reported_totals.unplanned_additions,
            total_additions_reported,
        ).sum()
        / total_additions_reported.size
    )

    return ratio_close_additions_to_total, ratio_close_reported_calculated
