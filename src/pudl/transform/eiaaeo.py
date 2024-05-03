"""Transform raw AEO tables into normalized assets.

Raw AEO tables often contain many different types of data which are split out
along different dimensions. For example, one table may contain generation
split out by fuel type as well as prices split out by service category.

As a result, we need to split these large tables into smaller tables that have
more uniform data, which we do by filtering the large table to its relevant
subsets, and then transforming some human-readable string fields into useful
metadata fields.
"""

from dataclasses import dataclass

import numpy as np
import pandas as pd
from dagster import AssetCheckResult, AssetChecksDefinition, asset, asset_check


def __sanitize_string(series: pd.Series) -> pd.Series:
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
    return fields.merge(variable_fields, left_index=True, right_index=True, how="inner")


def get_category_info(category_name: pd.Series) -> pd.Series:
    """Break human-readable category name into machine-readable fields.

    Fortunately the only field we're pulling out of the category so far is the
    region, which is the last of two comma-separated fields.
    """
    fields = category_name.str.rsplit(",", n=1).str.get(-1).rename("region_category")
    return fields


def filter_enrich_sanitize(
    raw_df: pd.DataFrame, relevant_series_names: tuple[str]
) -> pd.DataFrame:
    """Basic cleaning steps common to all AEO tables.

    1. Filter the AEO rows based on the series name
    2. Break the series name and category names into useful fields
    3. Sanitize strings & turn data values into a numeric field
    4. Make some defensive checks about data from multiple sources that
       *should* agree.
    """
    filtered = raw_df.loc[raw_df.series_name.str.startswith(relevant_series_names)]

    enriched = pd.concat(
        [
            filtered,
            get_series_info(filtered.series_name),
            get_category_info(filtered.category_name),
        ],
        axis="columns",
    ).drop(
        columns=[
            "series_name",
            "category_name",
            "variable_fields",
        ]
    )

    sanitized = enriched.assign(
        **{
            colname: __sanitize_string(enriched[colname])
            for colname in enriched.columns
            if colname not in {"projection_year", "value"}
        }
    ).pipe(
        lambda df: df.assign(
            report_year_series=df.report_year_series.str.replace("aeo", ""),
            value=pd.to_numeric(df.value.str.replace("- -", "")),
        )
    )

    assert (
        sanitized.region_series == sanitized.region_category
    ).all(), "Series and category must agree on region!"
    assert (
        sanitized.case_series == sanitized.model_case_eiaaeo
    ).all(), "Series and taxonomy must agree on case!"

    # TODO 2024-04-20: one day we'll include report year in the extract step -
    # at that point we'll have to compare that with the report year extracted
    # from the series name.
    return sanitized.drop(columns=["region_series", "case_series"]).rename(
        columns={"region_category": "region", "report_year_series": "report_year"}
    )


def unstack(df: pd.DataFrame, eventual_pk: list[str]):
    """Unstack the values by the various variable names provided."""
    unstacked = (
        df.set_index(eventual_pk + ["variable_name"])
        .unstack(level="variable_name")
        .sort_index()
    )
    unstacked.columns = unstacked.columns.droplevel()
    unstacked.columns.name = None
    return unstacked


@asset(io_manager_key="pudl_io_manager")
def core_eiaaeo__yearly_projected_generation_in_electric_sector_by_technology(
    raw_eiaaeo__electric_power_projections_regional,
):
    """Projected net summer generation capacity and additions/retirements."""

    def _drop_aggregated_generation_data(df: pd.DataFrame) -> pd.DataFrame:
        """These "aggregate" data series aren't split out by technology.

        We'll report them elsewhere - but for this resource we filter them out.
        """
        generation_mask = df.variable_name == "generation"
        is_aggregated_mask = df.dimension.isin(
            {
                "total_generation",
                "sales_to_customers",
                "generation_for_own_use",
            }
        )
        return df.loc[~(generation_mask & is_aggregated_mask)]

    def _collect_totals(df: pd.DataFrame) -> pd.DataFrame:
        """Various columns have different names for their "total" fact.

        We should combine them into one "total" dimension.
        """
        return df.assign(
            dimension=df.dimension.str.replace(r"^total.*$", "total", regex=True)
        )

    def _decumulate(df: pd.DataFrame, decumulate_columns: list[str]) -> pd.DataFrame:
        """De-accumulate the cumulative columns so we get yearly values.

        Cumulative values are less useful because they require the user to know
        what year the accumulation starts.


        Raises:
        AssertionError: It's important that ``projection_year`` is the last
            level of index, so that when we diff() we are actually subtracting
            year over year. The index is set when passing ``eventual_pk`` to
            ``unstack()``.
        """
        assert (
            df.index.names[-1] == "projection_year"
        ), "Projection year must be last level of index!"
        new_columns = {}
        for decumulate_column in decumulate_columns:
            cumulative_colname = f"cumulative_{decumulate_column}"
            column = df[cumulative_colname]
            new_columns[decumulate_column] = column.diff().fillna(column)
        return df.assign(**new_columns).drop(
            columns=[f"cumulative_{c}" for c in decumulate_columns]
        )

    sanitized = (
        filter_enrich_sanitize(
            raw_df=raw_eiaaeo__electric_power_projections_regional,
            relevant_series_names=(
                "Electricity : Electric Power Sector : Capacity",
                "Electricity : Electric Power Sector : Generation",
                "Electricity : Electric Power Sector : Cumulative Retirements",
                "Electricity : Electric Power Sector : Cumulative Planned Additions",
                "Electricity : Electric Power Sector : Cumulative Unplanned Additions",
                "Electricity : Electric Power Sector : Cumulative Total Additions",
            ),
        )
        .pipe(_drop_aggregated_generation_data)
        .pipe(_collect_totals)
    )

    assert set(sanitized.topic.unique()) == {"electricity"}
    assert set(sanitized.subtopic.unique()) == {"electric_power_sector"}

    trimmed = sanitized.drop(
        columns=[
            "topic",
            "subtopic",
            "units",
        ]
    )

    unstacked = unstack(
        df=trimmed,
        eventual_pk=[
            "report_year",
            "model_case_eiaaeo",
            "region",
            "dimension",
            "projection_year",
        ],
    )

    # check that the totals all add up *before* decumulating, because
    # decumulating introduces some small amount more floating point error.
    ratio_close_additions_to_total, ratio_close_reported_calculated = (
        check_totals_add_up(unstacked)
    )
    assert (
        0.999 < ratio_close_additions_to_total <= 1.0
    ), f"planned + unplanned vs. total: {ratio_close_additions_to_total}"
    assert (
        0.999 < ratio_close_reported_calculated <= 1.0
    ), f"reported vs. calculated totals: {ratio_close_reported_calculated}"
    # Now that the totals add up, we can drop the total_additions col
    unstacked = unstacked.drop(columns="cumulative_total_additions")

    decumulate_columns = [
        "planned_additions",
        "unplanned_additions",
        "retirements",
    ]
    decumulated = _decumulate(unstacked, decumulate_columns=decumulate_columns)

    # convert units: GW -> MW
    for gw_col in [
        "capacity",
        "planned_additions",
        "unplanned_additions",
        "retirements",
    ]:
        decumulated[gw_col] *= 1000
    # from billion KWh (1e12 Wh) to MWh (1e6 Wh)
    decumulated.generation *= 1_000_000

    renamed_for_pudl = (
        decumulated.dropna(how="all")
        .reset_index()
        .pipe(lambda df: df.loc[(df.dimension != "total")])
        .rename(
            columns={
                "capacity": "summer_capacity_mw",
                "unplanned_additions": "summer_capacity_unplanned_additions_mw",
                "planned_additions": "summer_capacity_planned_additions_mw",
                "retirements": "summer_capacity_retirements_mw",
                "generation": "gross_generation_mwh",
                "region": "electricity_market_module_region_eiaaeo",
                "dimension": "technology_description_eiaaeo",
            }
        )
    )

    return renamed_for_pudl


def check_totals_add_up(capacity) -> tuple[float, float]:
    """Check that reported totals match with calculated totals.

    We want to check that:

    * the "total" value is equal to the sum of the non-total values for
      "cumulative planned/unplanned additions/retirements."
    * cumulative planned + unplanned additions == cumulative total additions
    """
    additions_retirements = [
        "cumulative_planned_additions",
        "cumulative_unplanned_additions",
        "cumulative_retirements",
    ]

    pk = [
        "report_year",
        "model_case_eiaaeo",
        "region",
        "projection_year",
    ]
    addition_retirement_calculated_totals = (
        capacity.reset_index()
        .pipe(
            lambda df: df.loc[
                df.dimension.notna() & (df.dimension != "total"),
                pk + additions_retirements,
            ]
        )
        .groupby(pk)
        .sum()
    )

    addition_retirement_reported_totals = (
        capacity.xs("total", level="dimension").loc[:, additions_retirements].fillna(0)
    )

    total_additions_reported = (
        capacity.xs(pd.NA, level="dimension").fillna(0).cumulative_total_additions
    )

    ratio_close_reported_calculated = (
        np.isclose(
            addition_retirement_reported_totals, addition_retirement_calculated_totals
        ).sum()
        / addition_retirement_calculated_totals.size
    )

    ratio_close_additions_to_total = (
        np.isclose(
            addition_retirement_reported_totals.cumulative_planned_additions
            + addition_retirement_reported_totals.cumulative_unplanned_additions,
            total_additions_reported,
        ).sum()
        / total_additions_reported.size
    )

    return ratio_close_additions_to_total, ratio_close_reported_calculated


@dataclass
class AeoCheckSpec:
    """Define some simple checks that can run on any AEO asset."""

    name: str
    asset: str
    num_rows_by_report_year: dict[str, int]
    num_in_categories: dict[str, int]


check_specs = [
    AeoCheckSpec(
        name="gen_in_electric_sector_by_tech",
        asset="core_eiaaeo__yearly_projected_generation_in_electric_sector_by_technology",
        num_rows_by_report_year={2023: 166972},
        num_in_categories={"model_case_eiaaeo": 17, "projection_year": 30},
    )
]


def make_check(spec: AeoCheckSpec) -> AssetChecksDefinition:
    """Turn the AeoCheckSpec into an actual Dagster asset check."""

    @asset_check(asset=spec.asset)
    def _check(df):
        errors = []
        for year, expected_rows in spec.num_rows_by_report_year.items():
            if (num_rows := len(df.loc[df.report_year == year])) != expected_rows:
                errors.append(
                    f"Expected {expected_rows} for report year {year}, found {num_rows}"
                )
        for category, expected_num in spec.num_in_categories.items():
            if (num_values := len(df[category].value_counts())) != expected_num:
                errors.append(
                    f"Expected {expected_num} values for {category}, found {num_values}"
                )
        if errors:
            return AssetCheckResult(passed=False, metadata={"errors": errors})

        return AssetCheckResult(passed=True)

    return _check


_checks = [make_check(spec) for spec in check_specs]
