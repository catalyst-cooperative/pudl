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


def subtotals_match_reported_totals_ratio(
    df: pd.DataFrame, pk: list[str], fact_columns: list[str], dimension_column: str
) -> float:
    """When subtotals and totals are reported in the same column, check their sums.

    Group by some key, then check that within each group the non-``"total"``
    values sum up to the corresponding ``"total"`` value.

    Checks the list of fact columns to in aggregate, but if you want to check
    that *each* column sums up correctly, individually, you can call this
    function once per column.

    TODO 2024-05-06: it may make sense to pass the threshold into this
    function, which would clean up the call sites.

    Args:
        df: the dataframe to investigate
        pk: the key to group facts by
        fact_columns: the columns containing facts you'd like to sum
        dimension_column: the column which tells you if a fact is a sub-total
            or a total.

    Returns:
        The ratio of reported totals that are np.isclose() to the sum of their
        component parts.
    """
    reported_totals = (
        (
            df.loc[df[dimension_column] == "total"]
            .set_index(pk)
            .drop(columns=dimension_column)
        )
        .loc[:, fact_columns]
        .fillna(0)
    )
    calculated_totals = (
        df.loc[df[dimension_column] != "total", pk + fact_columns].groupby(pk).sum()
    )
    joined = calculated_totals.join(
        reported_totals, lsuffix="_calc", rsuffix="_reported", how="inner"
    )
    close = np.isclose(
        joined.loc[:, [f"{c}_calc" for c in fact_columns]],
        joined.loc[:, [f"{c}_reported" for c in fact_columns]],
    )
    return close.sum() / reported_totals.size


def series_sum_ratio(summands: pd.DataFrame, total: pd.Series) -> float:
    """Find how well multiple columns sum to another column.

    Args:
        summands: the columns that should sum to total
        total: the target total column

    Returns:
        the ratio of values in ``total`` that are np.isclose() to the sum of
        ``summands``.
    """
    return (
        np.isclose(
            summands.fillna(0).sum(axis="columns"),
            total.fillna(0),
        ).sum()
        / total.size
    )


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

    assert (sanitized.region_series == sanitized.region_category).all(), (
        "Series and category must agree on region!"
    )
    assert (sanitized.case_series == sanitized.model_case_eiaaeo).all(), (
        "Series and taxonomy must agree on case!"
    )

    # TODO 2024-04-20: one day we'll include report year in the extract step -
    # at that point we'll have to compare that with the report year extracted
    # from the series name.
    return sanitized.drop(columns=["region_series", "case_series"]).rename(
        columns={"region_category": "region", "report_year_series": "report_year"}
    )


def _collect_totals(df: pd.DataFrame, total_colname="dimension") -> pd.DataFrame:
    """Various columns have different names for their "total" fact.

    This combines them into one "total" dimension.
    """
    return df.assign(
        dimension=df.dimension.str.replace(r"^total.*$", "total", regex=True).fillna(
            "total"
        )
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
        assert df.index.names[-1] == "projection_year", (
            "Projection year must be last level of index!"
        )
        new_columns = {}
        for decumulate_column in decumulate_columns:
            cumulative_colname = f"cumulative_{decumulate_column}"
            column = df[cumulative_colname]
            new_columns[decumulate_column] = column.diff().fillna(column)
        return df.assign(**new_columns).drop(
            columns=[f"cumulative_{c}" for c in decumulate_columns]
        )

    def _check_totals_add_up(unstacked) -> None:
        """Check that reported totals match with calculated totals.

        We want to check that:

        * the "total" value is equal to the sum of the non-total values for
          "cumulative planned/unplanned additions/retirements."
        * cumulative planned + unplanned additions == cumulative total additions
        """
        ratio_close_reported_calculated = subtotals_match_reported_totals_ratio(
            unstacked.reset_index(),
            pk=["report_year", "model_case_eiaaeo", "region", "projection_year"],
            fact_columns=[
                "cumulative_planned_additions",
                "cumulative_unplanned_additions",
                "cumulative_retirements",
            ],
            dimension_column="dimension",
        )
        assert 0.999 < ratio_close_reported_calculated <= 1.0, (
            f"reported vs. calculated totals: {ratio_close_reported_calculated}"
        )

        totals = unstacked.xs("total", level="dimension")
        ratio_close_additions_to_total = series_sum_ratio(
            summands=totals.loc[
                :, ["cumulative_planned_additions", "cumulative_unplanned_additions"]
            ],
            total=totals.cumulative_total_additions,
        )
        assert 0.999 < ratio_close_additions_to_total <= 1.0, (
            f"planned + unplanned vs. total: {ratio_close_additions_to_total}"
        )
        return

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
    # 1 BKWh = 1e12 Wh = 1e6 MWh
    sanitized.loc[sanitized.units == "bkwh", "value"] *= 1e6
    # 1 GW = 1e9 W = 1e3 MW
    sanitized.loc[sanitized.units == "gw", "value"] *= 1e3

    assert set(sanitized.topic.unique()) == {"electricity"}
    assert set(sanitized.subtopic.unique()) == {"electric_power_sector"}
    assert set(sanitized.units.unique()) == {"bkwh", "gw"}

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
    _check_totals_add_up(unstacked)
    # Now that the totals add up, we can drop the total_additions col
    unstacked = unstacked.drop(columns="cumulative_total_additions")

    decumulate_columns = [
        "planned_additions",
        "unplanned_additions",
        "retirements",
    ]
    decumulated = _decumulate(unstacked, decumulate_columns=decumulate_columns)

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


@asset(io_manager_key="pudl_io_manager")
def core_eiaaeo__yearly_projected_electric_sales(
    raw_eiaaeo__electric_power_projections_regional,
):
    """Projected electricity sales by customer class."""
    # Our series names here only have 3 fields:
    # 1. "Electricity": broad topic
    # 2. "Electricity Demand": the variable name
    # 3. "Commercial", "Industrial": etc. the actual dimension
    # So we rename some of the extracted columns.
    #
    # TODO 2024-05-06 (daz): consider parametrizing the field assignments -
    # though for tables which have a mix of 3/4 field series, we will always
    # have to do some munging at the end.
    sanitized = (
        filter_enrich_sanitize(
            raw_df=raw_eiaaeo__electric_power_projections_regional,
            relevant_series_names=("Electricity : Electricity Demand",),
        )
        .rename(columns={"variable_name": "dimension", "subtopic": "variable_name"})
        .pipe(_collect_totals)
    )
    # 1 BKWh = 1e12 Wh = 1e6 MWh
    sanitized.loc[sanitized.units == "bkwh", "value"] *= 1e6

    assert set(sanitized.topic.unique()) == {"electricity"}
    assert set(sanitized.variable_name.unique()) == {"electricity_demand"}
    assert set(sanitized.units.unique()) == {"bkwh"}

    trimmed = sanitized.drop(
        columns=[
            "topic",
            "units",
        ]
    )

    subtotals_totals_match_ratio = subtotals_match_reported_totals_ratio(
        trimmed,
        pk=["report_year", "model_case_eiaaeo", "region", "projection_year"],
        fact_columns=["value"],
        dimension_column="dimension",
    )
    assert subtotals_totals_match_ratio == 1.0

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

    renamed_for_pudl = (
        unstacked.dropna(how="all")
        .reset_index()
        .pipe(lambda df: df.loc[(df.dimension != "total")])
        .rename(
            columns={
                "region": "electricity_market_module_region_eiaaeo",
                "dimension": "customer_class",
                "electricity_demand": "sales_mwh",
            }
        )
    )

    return renamed_for_pudl


@asset(io_manager_key="pudl_io_manager")
def core_eiaaeo__yearly_projected_generation_in_end_use_sectors_by_fuel_type(
    raw_eiaaeo__electric_power_projections_regional,
):
    """Projected generation capacity + gross generation in end-use sectors.

    This includes data that's reported by fuel type and ignores data that's
    only reported at the system-wide level, such as total generation, sales to
    grid, and generation for own use. Those three facts are reported in
    core_eiaaeo__yearly_projected_generation_in_end_use_sectors instead.
    """
    sanitized = filter_enrich_sanitize(
        raw_df=raw_eiaaeo__electric_power_projections_regional,
        relevant_series_names=(
            "Electricity : End-Use Sectors : Capacity",
            "Electricity : End-Use Sectors : Generation",
        ),
    )

    # 1 BKWh = 1e12 Wh = 1e6 MWh
    sanitized.loc[sanitized.units == "bkwh", "value"] *= 1e6
    # 1 GW = 1e9 W = 1e3 MW
    sanitized.loc[sanitized.units == "gw", "value"] *= 1e3

    assert set(sanitized.topic.unique()) == {"electricity"}
    assert set(sanitized.subtopic.unique()) == {"end_use_sectors"}
    assert set(sanitized.units.unique()) == {"bkwh", "gw"}
    assert (sanitized.loc[sanitized.units == "gw"].variable_name == "capacity").all()
    assert (
        sanitized.loc[sanitized.units == "bkwh"].variable_name == "generation"
    ).all()

    trimmed = sanitized.drop(
        columns=[
            "topic",
            "subtopic",
            "units",
        ]
    )

    # check that totals add up
    ratio_totals_add_up = subtotals_match_reported_totals_ratio(
        trimmed,
        pk=[
            "report_year",
            "model_case_eiaaeo",
            "region",
            "variable_name",
            "projection_year",
        ],
        fact_columns=["value"],
        dimension_column="dimension",
    )
    assert ratio_totals_add_up == 1.0

    trimmed = trimmed.loc[
        ~trimmed.dimension.isin(
            {"total", "sales_to_the_grid", "generation_for_own_use"}
        )
    ]

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

    renamed_for_pudl = unstacked.reset_index().rename(
        columns={
            "capacity": "summer_capacity_mw",
            "generation": "gross_generation_mwh",
            "region": "electricity_market_module_region_eiaaeo",
            "dimension": "fuel_type_eiaaeo",
        }
    )
    return renamed_for_pudl


@asset(io_manager_key="pudl_io_manager")
def core_eiaaeo__yearly_projected_energy_use_by_sector_and_type(
    raw_eiaaeo__energy_consumption_by_sector_and_source,
):
    """Projected energy use for commercial, electric power, industrial, residential, and transportation sectors.

    The "Energy Use" series in Table 2 which track figures by sector do not always
    define each type of usage the same way across sectors. There is detailed
    information about what is included or excluded in each usage type for each sector
    in the footnotes of the EIA's online AEO data browser:

      https://www.eia.gov/outlooks/aeo/data/browser/#/?id=2-AEO2023

    The data browser also gives some visibility into the tricky system of subtotals
    within the Energy Use series. To identify and map subtotal usage types, look for
    the following features in the data browser display: subtotal series are displayed
    indented, and include all lines above them which are one level out, up to the
    next indented line. Delivered Energy and Total are special cases which include
    those plus all subtotals above. In this way, "Delivered Energy" includes
    purchased electricity, renewable energy, and an array of fuels based on sector,
    and explicitly excludes electricity-related losses.

    AEO Energy Use figures are variously referred to as delivered energy, energy
    consumption, energy use, and energy demand, depending on which usage types are
    being discussed, and which org and which document is describing them. In PUDL we
    say energy use or energy consumption.
    """
    sanitized = filter_enrich_sanitize(
        raw_df=raw_eiaaeo__energy_consumption_by_sector_and_source,
        relevant_series_names=(
            "Energy Use : Commercial",
            "Energy Use : Electric Power",
            "Energy Use : Industrial",
            "Energy Use : Residential",
            "Energy Use : Total",
            "Energy Use : Transportation",
            "Energy Use : Unspecified",
        ),
    ).rename(columns={"subtopic": "dimension"})

    expected_values = {
        "topic": {"energy_use"},
        "units": {"quads"},
        "dimension": {
            "commercial",
            "electric_power",
            "industrial",
            "residential",
            "total",
            "transportation",
            "unspecified",
        },
    }
    for column, expected in expected_values.items():
        assert (actual := set(sanitized[column].unique())) == expected, (
            f"Unexpected values in {column}: Expected {expected}; found {actual}"
        )

    # 1 quad = 1 quadrillion btu = 1e9 mmbtu
    sanitized.loc[sanitized.units == "quads", "value"] *= 1e9

    trimmed = (
        sanitized.drop(
            columns=[
                "topic",
                "units",
            ]
        )
        .set_index(
            [
                "report_year",
                "model_case_eiaaeo",
                "region",
                "dimension",
                "projection_year",
                "variable_name",
            ]
        )
        .sort_index()
        .assign(region_type="us_census_division")
        .reset_index()
    )
    trimmed.loc[trimmed.region == "united_states", "region_type"] = "country"

    renamed_for_pudl = trimmed.rename(
        columns={
            "region": "region_name_eiaaeo",
            "region_type": "region_type_eiaaeo",
            "dimension": "energy_use_sector",
            "variable_name": "energy_use_type",
            "value": "energy_use_mmbtu",
        }
    )

    return renamed_for_pudl


@asset(io_manager_key="pudl_io_manager")
def core_eiaaeo__yearly_projected_fuel_cost_in_electric_sector_by_type(
    raw_eiaaeo__electric_power_projections_regional,
):
    """Projected fuel cost for the electric power sector.

    Includes 2022 US dollars per million BTU and nominal US dollars per million
    BTU.

    In future report years, the base year for the real cost will change, so we
    store that base year as well.
    """
    sanitized = filter_enrich_sanitize(
        raw_df=raw_eiaaeo__electric_power_projections_regional,
        relevant_series_names=("Electricity : Fuel Prices",),
    ).rename(columns={"subtopic": "variable_name", "variable_name": "dimension"})

    assert set(sanitized.topic.unique()) == {"electricity"}
    assert set(sanitized.variable_name.unique()) == {"fuel_prices"}
    assert set(sanitized.units.unique()) == {"2022_mmbtu", "nom_mmbtu"}
    # turn variable_name into `nominal_fuel_prices` and `real_fuel_prices` based on unit
    sanitized.variable_name = sanitized.units + "_" + sanitized.variable_name

    trimmed = sanitized.drop(
        columns=[
            "topic",
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
    ).assign(real_cost_basis_year=2022)

    renamed_for_pudl = unstacked.reset_index().rename(
        columns={
            "capacity": "summer_capacity_mw",
            "generation": "gross_generation_mwh",
            "region": "electricity_market_module_region_eiaaeo",
            "dimension": "fuel_type_eiaaeo",
            "2022_mmbtu_fuel_prices": "fuel_cost_real_per_mmbtu_eiaaeo",
            "nom_mmbtu_fuel_prices": "fuel_cost_per_mmbtu",
        }
    )
    return renamed_for_pudl


@dataclass
class AeoCheckSpec:
    """Define some simple checks that can run on any AEO asset."""

    name: str
    asset: str
    num_rows_by_report_year: dict[int, int]
    category_counts: dict[str, int]


BASE_AEO_CATEGORIES = {
    "model_case_eiaaeo": 17,
    "projection_year": 30,
    "electricity_market_module_region_eiaaeo": 26,
}
check_specs = [
    AeoCheckSpec(
        name="gen_in_electric_sector_by_tech",
        asset="core_eiaaeo__yearly_projected_generation_in_electric_sector_by_technology",
        num_rows_by_report_year={2023: 166972},
        category_counts=BASE_AEO_CATEGORIES
        | {
            "technology_description_eiaaeo": 13,
        },
    ),
    AeoCheckSpec(
        name="gen_in_electric_sector_by_tech",
        asset="core_eiaaeo__yearly_projected_generation_in_end_use_sectors_by_fuel_type",
        num_rows_by_report_year={2023: 77064},
        category_counts=BASE_AEO_CATEGORIES
        | {
            "fuel_type_eiaaeo": 6,
        },
    ),
    AeoCheckSpec(
        name="electricity_sales",
        asset="core_eiaaeo__yearly_projected_electric_sales",
        num_rows_by_report_year={2023: 51376},
        category_counts=BASE_AEO_CATEGORIES
        | {
            "customer_class": 4,
        },
    ),
    AeoCheckSpec(
        name="electricity_sales",
        asset="core_eiaaeo__yearly_projected_fuel_cost_in_electric_sector_by_type",
        num_rows_by_report_year={2023: 50882},
        category_counts=BASE_AEO_CATEGORIES
        | {
            "fuel_type_eiaaeo": 4,
        },
    ),
]


def make_check(spec: AeoCheckSpec) -> AssetChecksDefinition:
    """Turn the AeoCheckSpec into an actual Dagster asset check."""

    @asset_check(asset=spec.asset, blocking=True)
    def _check(df):
        errors = []
        for year, expected_rows in spec.num_rows_by_report_year.items():
            if (num_rows := len(df.loc[df.report_year == year])) != expected_rows:
                errors.append(
                    f"Expected {expected_rows} for report year {year}, found {num_rows}"
                )
        for category, expected_num in spec.category_counts.items():
            value_counts = df[category].value_counts()
            non_zero_values = value_counts.loc[value_counts > 0]
            if len(non_zero_values) != expected_num:
                errors.append(
                    f"Expected {expected_num} values for {category}, "
                    f"found {len(non_zero_values)}: {non_zero_values}"
                )
        if errors:
            return AssetCheckResult(passed=False, metadata={"errors": errors})

        return AssetCheckResult(passed=True)

    return _check


_checks = [make_check(spec) for spec in check_specs]
