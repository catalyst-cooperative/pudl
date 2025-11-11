"""Module to perform data cleaning functions on EIA176 data tables."""

from typing import cast

import pandas as pd
from dagster import (
    AssetCheckResult,
    AssetIn,
    AssetOut,
    Output,
    asset,
    asset_check,
    multi_asset,
)

from pudl.helpers import simplify_columns
from pudl.logging_helpers import get_logger

logger = get_logger(__name__)


@multi_asset(
    outs={
        "_core_eia176__yearly_company_data": AssetOut(),
        "_core_eia176__yearly_aggregate_data": AssetOut(),
    },
)
def _core_eia176__data(
    raw_eia176__data: pd.DataFrame,
) -> tuple[Output, Output]:
    """Take raw list and return two wide tables with primary keys and one column per variable.

    One table with data for each year and company, one with state- and US-level aggregates per year.
    """
    raw_eia176__data = raw_eia176__data.astype({"report_year": int, "value": float})

    aggregate_primary_key = ["report_year", "operating_state"]
    company_primary_key = aggregate_primary_key + ["operator_id_eia", "operator_name"]
    company_drop_columns = ["form_line_numbers", "unit_type", "line"]
    # We must drop 'id' here and cannot use as primary key because its arbitrary/duplicate in aggregate records
    # 'id' is a reliable ID only in the context of granular company data
    aggregate_drop_columns = company_drop_columns + ["operator_id_eia", "operator_name"]

    # Clean up text columns to ensure string matching is precise
    for col in ["operator_name", "operating_state"]:
        raw_eia176__data[col] = raw_eia176__data[col].str.strip().str.lower()

    long_company = raw_eia176__data.loc[
        raw_eia176__data.operator_name != "total of all companies"
    ]
    wide_company = get_wide_table(
        long_table=long_company.drop(columns=company_drop_columns),
        primary_key=company_primary_key,
    )

    long_aggregate = raw_eia176__data.loc[
        raw_eia176__data.operator_name == "total of all companies"
    ]
    wide_aggregate = get_wide_table(
        long_table=long_aggregate.drop(columns=aggregate_drop_columns).dropna(
            subset=aggregate_primary_key
        ),
        primary_key=aggregate_primary_key,
    )

    return (
        Output(output_name="_core_eia176__yearly_company_data", value=wide_company),
        Output(output_name="_core_eia176__yearly_aggregate_data", value=wide_aggregate),
    )


def get_wide_table(long_table: pd.DataFrame, primary_key: list[str]) -> pd.DataFrame:
    """Take a 'long' or entity-attribute-value table and return a wide table with one column per attribute/variable."""
    unstacked = long_table.set_index(primary_key + ["variable_name"]).unstack(
        level="variable_name"
    )
    unstacked.columns = unstacked.columns.droplevel(0)
    unstacked = simplify_columns(unstacked)  # Clean up column names
    unstacked.columns.name = None  # gets rid of "item" name of columns index
    return unstacked.reset_index()


@asset_check(
    asset="_core_eia176__yearly_company_data",
    additional_ins={"_core_eia176__yearly_aggregate_data": AssetIn()},
    blocking=True,
)
def validate_totals(
    _core_eia176__yearly_company_data: pd.DataFrame,
    _core_eia176__yearly_aggregate_data: pd.DataFrame,
) -> AssetCheckResult:
    """Compare reported and calculated totals for different geographical aggregates, report any differences.

    EIA reports an adjustment company at the area-level, so these values are expected to be identical.
    Once we validate this, we can preserve the detailed data and discard the aggregate data to
    remove duplicate information.
    """
    # First make it so we can directly compare reported aggregates to groupings of granular data
    comparable_aggregates = _core_eia176__yearly_aggregate_data.sort_values(
        ["report_year", "operating_state"]
    ).fillna(0)

    # Group company data into state-level data and compare to reported totals
    state_data = (
        _core_eia176__yearly_company_data.drop(
            columns=["operator_id_eia", "operator_name"]
        )
        .groupby(["report_year", "operating_state"])
        .sum()
        .round(4)
        .reset_index()
    )
    aggregate_state = comparable_aggregates[
        comparable_aggregates.operating_state != "u.s. total"
    ].reset_index(drop=True)
    # Compare using the same columns
    state_diff = aggregate_state[state_data.columns].compare(state_data)

    # Group calculated state-level data into US-level data and compare to reported totals
    us_data = (
        state_data.drop(columns="operating_state")
        .groupby("report_year")
        .sum()
        .sort_values("report_year")
        .reset_index()
    )
    aggregate_us = (
        comparable_aggregates[comparable_aggregates.operating_state == "u.s. total"]
        .drop(columns="operating_state")
        .sort_values("report_year")
        .reset_index(drop=True)
    )
    # Compare using the same columns
    us_diff = aggregate_us[us_data.columns].compare(us_data)

    # 2024-11-28: "alternative_fuel_fleet_1_yes_0_no" is reported as 1 in aggregate data from 2005
    # through 2015. If we run into cases where the totals don't add up, check
    # to see that they are all this specific case and then ignore them.
    if not state_diff.empty:
        assert (
            state_diff.columns.levels[0] == ["alternative_fuel_fleet_1_yes_0_no"]
        ).all()
        assert (state_diff["alternative_fuel_fleet_1_yes_0_no"]["self"] == 1.0).all()
        assert (
            aggregate_us.loc[us_diff.index].report_year.unique() == range(2005, 2016)
        ).all()

    if not us_diff.empty:
        assert (
            us_diff.columns.levels[0] == ["alternative_fuel_fleet_1_yes_0_no"]
        ).all()
        assert (us_diff["alternative_fuel_fleet_1_yes_0_no"]["self"] == 1.0).all()
        assert (
            aggregate_us.loc[us_diff.index].report_year.unique() == (range(2005, 2016))
        ).all()

    return AssetCheckResult(passed=True)


@asset(io_manager_key="pudl_io_manager")
def core_eia176__yearly_gas_disposition_by_consumer(
    _core_eia176__yearly_company_data: pd.DataFrame,
    core_pudl__codes_subdivisions: pd.DataFrame,
) -> pd.DataFrame:
    """Produce annual company-level gas disposition by consumer class (EIA-176).

    Transforms company-level EIA-176 data into a normalized table with one row per
    ("report_year", "operator_id_eia", "operating_state", "customer_class", "revenue_class")
    and three value columns: "consumers", "revenue", and "volume_mcf".

    Processing:

    * Select sales and transport metrics for residential, commercial, industrial,
      electric_power, vehicle_fuel, and other.
    * Validate that ``sales_volume + transport_volume == total volume`` per customer
      class.
    * Normalize ``operating_state`` to two-letter subdivision codes via
      ``core_pudl__codes_subdivisions``; drop rows with unknown states (these rows must
      contain zeros across value columns).
    * Drop rows where all of ``consumers``/``revenue``/``volume_mcf`` are NULL.

    Args:
        _core_eia176__yearly_company_data: Wide company-level EIA-176 data with
            per-metric columns.
        core_pudl__codes_subdivisions: Mapping from ``subdivision_name`` to
            ``subdivision_code`` used to normalize ``operating_state``.

    Raises:
        AssertionError: If component volumes don’t sum to totals, or if rows with
            unknown ``operating_state`` contain non-zero values.

    Notes:
        - ``volume_mcf`` is thousand cubic feet (MCF).
        - ``consumers`` is a count; ``revenue`` is nominal USD as reported.
        - ``customer_class`` and ``revenue_class`` are returned as categoricals.
    """
    primary_key = ["report_year", "operator_id_eia", "operating_state"]

    keep = [
        # 1 ==========================================
        # 10.1
        "residential_sales_consumers",
        "residential_sales_revenue",
        "residential_sales_volume",
        # 11.1
        "residential_transport_consumers",
        "residential_transport_revenue",
        "residential_transport_volume",
        # 10.1+11.1
        # "residential_volume",
        # 2 ==========================================
        # 10.2
        "commercial_sales_consumers",
        "commercial_sales_revenue",
        "commercial_sales_volume",
        # 11.2
        "commercial_transport_consumers",
        "commercial_transport_revenue",
        "commercial_transport_volume",
        # 10.2 + 11.2
        # "commercial_volume",
        # 3 ==========================================
        # 10.3
        "industrial_sales_consumers",
        "industrial_sales_revenue",
        "industrial_sales_volume",
        # 11.3
        "industrial_transport_consumers",
        "industrial_transport_revenue",
        "industrial_transport_volume",
        # 10.3 + 11.3
        # "industrial_volume",
        # 4 ==========================================
        # 10.4
        "electric_power_sales_consumers",
        "electric_power_sales_revenue",
        "electric_power_sales_volume",
        # 11.4
        "electric_power_transport_consumers",
        "electric_power_transport_revenue",
        "electric_power_transport_volume",
        # 10.4 + 11.4
        # "electric_power_volume",
        # 5 ==========================================
        # 10.5
        "vehicle_fuel_sales_consumers",
        "vehicle_fuel_sales_revenue",
        "vehicle_fuel_sales_volume",
        # 11.5
        "vehicle_fuel_transport_consumers",
        "vehicle_fuel_transport_revenue",
        "vehicle_fuel_transport_volume",
        # 10.5 + 11.5
        # "vehicle_fuel_volume",
        # 6 ==========================================
        # 10.6
        "other_sales_consumers",
        "other_sales_revenue",
        "other_sales_volume",
        # 11.6
        "other_transport_consumers",
        "other_transport_volume",
        # 10.6 + 11.6
        # "other_volume",
    ]

    volumes = [
        "other_volume",
        "vehicle_fuel_volume",
        "electric_power_volume",
        "industrial_volume",
        "residential_volume",
        "commercial_volume",
    ]

    customer_classes = (
        "residential",
        "commercial",
        "industrial",
        "electric_power",
        "vehicle_fuel",
        "other",
    )

    # Assert that volume sums match, ignoring NA cases
    df = _core_eia176__yearly_company_data
    mismatched = sum(
        sum(
            (
                df[f"{customer_class}_transport_volume"].notna()
                & df[f"{customer_class}_sales_volume"].notna()
                & df[f"{customer_class}_volume"].notna()
                & (
                    (
                        df[f"{customer_class}_transport_volume"]
                        + df[f"{customer_class}_sales_volume"]
                    )
                    != df[f"{customer_class}_volume"]
                )
            )
        )
        for customer_class in customer_classes
    )
    assert not mismatched, f"found {mismatched} mismatched total volumes"

    # Assert that there is an expected number of mismatched volume totals where one of the columns is null
    df = _core_eia176__yearly_company_data.fillna(0)
    mismatched = sum(
        sum(
            (
                (
                    df[f"{customer_class}_transport_volume"]
                    + df[f"{customer_class}_sales_volume"]
                )
                != df[f"{customer_class}_volume"]
            )
        )
        for customer_class in customer_classes
    )
    assert (
        mismatched == 28
    ), f"{mismatched} mismatched volume totals found, expected 28."

    df = _core_eia176__yearly_company_data.filter(primary_key + keep)

    # Normalize operating states, those that are missing in subdivisions will be NA
    codes = (
        core_pudl__codes_subdivisions.assign(
            key=lambda d: d["subdivision_name"].str.strip().str.casefold()
        )
        .drop_duplicates("key")
        .set_index("key")["subdivision_code"]
    )
    df["operating_state"] = (
        df["operating_state"].str.strip().str.casefold().map(codes.get)
    )

    df = pd.melt(
        df, id_vars=primary_key, var_name="metric", value_name="value"
    ).reset_index()
    df[["customer_class", "revenue_class", "metric_type"]] = df["metric"].str.extract(
        rf"({'|'.join(customer_classes)})_(sales|transport)_(.+)$"
    )

    df = df.drop(columns="metric").reset_index()
    primary_key.extend(["customer_class", "revenue_class"])
    df = df.pivot(
        index=primary_key, columns="metric_type", values="value"
    ).reset_index()
    df.columns.name = None

    # Rename columns as needed
    df = df.rename(columns={"volume": "volume_mcf"})

    # Ensure all values in rows with null operating state are zeroes, and drop them
    assert (
        df[df["operating_state"].isna()]
        .sum()[["consumers", "revenue", "volume_mcf"]]
        .sum()
        == 0
    )
    df = df.dropna(subset=["operating_state"])
    df = df.dropna(subset=["consumers", "revenue", "volume_mcf"], how="all")

    return df
