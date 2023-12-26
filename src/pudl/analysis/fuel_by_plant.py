"""Calculates useful FERC Form 1 fuel metrics on a per plant-year basis."""

import re

import numpy as np
import pandas as pd


def revert_filled_in_string_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """Revert the filled nulls from string columns."""
    for col in [
        "plant_type",
        "construction_type",
        "fuel_type_code_pudl",
        "primary_fuel_by_cost",
        "primary_fuel_by_mmbtu",
    ]:
        if col in df.columns:
            # the replace to_replace={column_name: {"", pd.NA}} mysteriously doesn't work.
            df[col] = df[col].replace(
                to_replace=[""],
                value=pd.NA,
            )
    return df


def revert_filled_in_float_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """Revert the filled nulls from float columns."""
    float_cols = list(df.select_dtypes(include=[float]))
    if float_cols:
        df.loc[:, float_cols] = df.loc[:, float_cols].replace(0, np.nan)
    return df


def fuel_by_plant_ferc1(
    fuel_df: pd.DataFrame, fuel_categories: list[str], thresh: float = 0.5
) -> pd.DataFrame:
    """Calculates useful FERC Form 1 fuel metrics on a per plant-year basis.

    Each record in the FERC Form 1 corresponds to a particular type of fuel. Many plants
    -- especially coal plants -- use more than one fuel, with gas and/or diesel serving
    as startup fuels. In order to be able to classify the type of plant based on
    relative proportions of fuel consumed or fuel costs it is useful to aggregate these
    per-fuel records into a single record for each plant.

    Fuel cost (in nominal dollars) and fuel heat content (in mmBTU) are calculated for
    each fuel based on the cost and heat content per unit, and the number of units
    consumed, and then summed by fuel type (there can be more than one record for a
    given type of fuel in each plant because we are simplifying the fuel categories).
    The per-fuel records are then pivoted to create one column per fuel type. The total
    is summed and stored separately, and the individual fuel costs & heat contents are
    divided by that total, to yield fuel proportions.  Based on those proportions and a
    minimum threshold that's passed in, a "primary" fuel type is then assigned to the
    plant-year record and given a string label.

    Args:
        fuel_df: Pandas DataFrame resembling the post-transform
            result for the fuel_ferc1 table.
        thresh: A value between 0.5 and 1.0 indicating the minimum fraction of
            overall heat content that must have been provided by a fuel in a plant-year
            for it to be considered the "primary" fuel for the plant in that year.
            Default value: 0.5.

    Returns:
        DataFrame with a single record for each plant-year, including the columns
        required to merge it with the plants_steam_ferc1 table/DataFrame (report_year,
        utility_id_ferc1, and plant_name) as well as totals for fuel mmbtu consumed in
        that plant-year, and the cost of fuel in that year, the proportions of heat
        content and fuel costs for each fuel in that year, and a column that labels the
        plant's primary fuel for that year.

    Raises:
        AssertionError: If the DataFrame input does not have the columns required to
            run the function.
    """
    keep_cols = [
        "report_year",  # key
        "utility_id_ferc1",  # key
        "plant_name_ferc1",  # key
        "fuel_type_code_pudl",  # pivot
        "fuel_consumed_units",  # value
        "fuel_mmbtu_per_unit",  # value
        "fuel_cost_per_unit_burned",  # value
    ]

    # Ensure that the dataframe we've gotten has all the information we need:
    missing_cols = [col for col in keep_cols if col not in fuel_df.columns]
    if missing_cols:
        raise AssertionError(
            f"Required columns not found in input fuel_df: {missing_cols}"
        )

    # Calculate per-fuel derived values and add them to the DataFrame
    df = (
        # Really there should *not* be any duplicates here but... there's a
        # bug somewhere that introduces them into the fuel_ferc1 table.
        fuel_df[keep_cols]
        .drop_duplicates()
        # Calculate totals for each record based on per-unit values:
        .assign(fuel_mmbtu=lambda x: x.fuel_consumed_units * x.fuel_mmbtu_per_unit)
        .assign(fuel_cost=lambda x: x.fuel_consumed_units * x.fuel_cost_per_unit_burned)
        # Drop the ratios and heterogeneous fuel "units"
        .drop(
            ["fuel_mmbtu_per_unit", "fuel_cost_per_unit_burned", "fuel_consumed_units"],
            axis=1,
        )
        # Group by the keys and fuel type, and sum:
        .groupby(
            [
                "utility_id_ferc1",
                "plant_name_ferc1",
                "report_year",
                "fuel_type_code_pudl",
            ],
            observed=True,
        )
        .sum()
        .reset_index()
        # Set the index to the keys, and pivot to get per-fuel columns:
        .set_index(["utility_id_ferc1", "plant_name_ferc1", "report_year"])
        .pivot(columns="fuel_type_code_pudl")
        .fillna(0.0)
    )

    # Undo pivot. Could refactor this old function
    plant_year_totals = df.stack("fuel_type_code_pudl").groupby(level=[0, 1, 2]).sum()

    # Calculate total heat content burned for each plant, and divide it out
    mmbtu_group = (
        pd.merge(
            # Sum up all the fuel heat content, and divide the individual fuel
            # heat contents by it (they are all contained in single higher
            # level group of columns labeled fuel_mmbtu)
            df.loc[:, "fuel_mmbtu"].div(
                df.loc[:, "fuel_mmbtu"].sum(axis=1), axis="rows"
            ),
            # Merge that same total into the dataframe separately as well.
            plant_year_totals.loc[:, "fuel_mmbtu"],
            right_index=True,
            left_index=True,
        )
        .rename(columns=lambda x: re.sub(r"$", "_fraction_mmbtu", x))
        .rename(columns=lambda x: re.sub(r"_mmbtu_fraction_mmbtu$", "_mmbtu", x))
    )

    # Calculate total fuel cost for each plant, and divide it out
    cost_group = (
        pd.merge(
            # Sum up all the fuel costs, and divide the individual fuel
            # costs by it (they are all contained in single higher
            # level group of columns labeled fuel_cost)
            df.loc[:, "fuel_cost"].div(df.loc[:, "fuel_cost"].sum(axis=1), axis="rows"),
            # Merge that same total into the dataframe separately as well.
            plant_year_totals.loc[:, "fuel_cost"],
            right_index=True,
            left_index=True,
        )
        .rename(columns=lambda x: re.sub(r"$", "_fraction_cost", x))
        .rename(columns=lambda x: re.sub(r"_cost_fraction_cost$", "_cost", x))
    )

    # Re-unify the cost and heat content information:
    df = pd.merge(
        mmbtu_group, cost_group, left_index=True, right_index=True
    ).reset_index()

    # Label each plant-year record by primary fuel:
    df.loc[:, ["primary_fuel_by_cost", "primary_fuel_by_mmbtu"]] = pd.NA
    df = df.astype(
        {
            "primary_fuel_by_cost": pd.StringDtype(),
            "primary_fuel_by_mmbtu": pd.StringDtype(),
        }
    )
    for fuel_str in fuel_categories:
        try:
            mmbtu_mask = df[f"{fuel_str}_fraction_mmbtu"] > thresh
            df.loc[mmbtu_mask, "primary_fuel_by_mmbtu"] = fuel_str
        except KeyError:
            pass

        try:
            cost_mask = df[f"{fuel_str}_fraction_cost"] > thresh
            df.loc[cost_mask, "primary_fuel_by_cost"] = fuel_str
        except KeyError:
            pass

    df[["primary_fuel_by_cost", "primary_fuel_by_mmbtu"]] = df[
        ["primary_fuel_by_cost", "primary_fuel_by_mmbtu"]
    ].fillna("")

    return df
