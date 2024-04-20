"""Module to perform data cleaning functions on EIA930 data tables."""

import pandas as pd
from dagster import AssetOut, Output, multi_asset

import pudl

logger = pudl.logging_helpers.get_logger(__name__)


@multi_asset(
    outs={
        "core_eia930__hourly_balancing_authority_net_generation": AssetOut(),
        "core_eia930__hourly_balancing_authority_demand": AssetOut(),
    },
    compute_kind="pandas",
)
def core_eia930__hourly_balancing_authority_assets(
    raw_eia930__balance: pd.DataFrame,
):
    """Transforms raw_eia923__balance dataframe.

    Extract the net generation information from the balance table and extract fuel type
    from the net generation columns.
    """
    qual_cols = [
        "report_datetime_local",
        "report_datetime_utc",
        "balancing_authority_code_eia",
        # report_date
        # eia_region_code,
        # report_hour_local
    ]
    # Select only the columns relevant to the BA net generation table
    netgen = raw_eia930__balance[
        qual_cols
        + list(raw_eia930__balance.filter(like="net_generation"))
        + list(raw_eia930__balance.filter(like="interchange"))
    ]

    # Rename columns so that they contain only the energy source and the level of
    # processing, so it's easy to construct a multi-index to unstack below
    # Doing these manipulations while the values are in the column names rather than
    # after they've been turned into a categorical column with millions of entries
    # is much faster.
    netgen_renamed = netgen.rename(
        lambda col: col.removeprefix("net_generation_").removesuffix("_mw"),
        axis="columns",
    ).set_index(qual_cols)

    # Prepare a multi-index for the columns so that we can stack cleanly
    netgen_renamed.columns = pd.MultiIndex.from_tuples(
        [x.split("_") for x in netgen_renamed.columns], names=["energy_source", None]
    )
    netgen_stacked = (
        netgen_renamed.stack(level=0, future_stack=True)
        .rename(columns=lambda x: f"net_generation_{x}_mw")
        .reset_index()
        .astype({"energy_source": "string"})
        .astype(
            {
                "balancing_authority_code_eia": pd.CategoricalDtype(),
                "energy_source": pd.CategoricalDtype(),
            }
        )
    )
    # TODO[zaneselvans] 2024-04-20: Verify that sum of net generation from all fuels
    # adds up to the total And then drop the total rows.
    # NOTE: currently there are some BIG differences between the calculated total and
    # the reported total.

    # netgen_stacked = netgen_stacked[netgen_stacked["energy_source"] != "total"

    demand = raw_eia930__balance[
        qual_cols + list(raw_eia930__balance.filter(like="demand"))
    ].astype({"balancing_authority_code_eia": pd.CategoricalDtype()})

    return (
        Output(
            value=netgen_stacked,
            output_name="core_eia930__hourly_balancing_authority_net_generation",
        ),
        Output(
            value=demand,
            output_name="core_eia930__hourly_balancing_authority_demand",
        ),
    )


@multi_asset(
    outs={
        "core_eia930__hourly_subregion_demand": AssetOut(),
        "core_eia930__assn_balancing_authority_subregion": AssetOut(),
    },
    compute_kind="pandas",
)
def core_eia930__hourly_subregion_assets(raw_eia930__subregion: pd.DataFrame):
    """Produce a normalized table of hourly demand by subregion."""
    demand = (
        raw_eia930__subregion.assign(
            subregion_code_eia=lambda df: df["subregion_code_eia"].str.upper()
        )
        .astype(
            {
                "balancing_authority_code_eia": pd.CategoricalDtype(),
                "subregion_code_eia": pd.CategoricalDtype(),
            }
        )
        .loc[
            :,
            [
                "report_datetime_local",
                "report_datetime_utc",
                "balancing_authority_code_eia",
                "subregion_code_eia",
                "demand_mw",
            ],
        ]
    )
    assn = (
        demand.groupby("balancing_authority_code_eia")["subregion_code_eia"]
        .unique()
        .explode()
        .to_frame()
        .reset_index()
        .astype(
            {
                "balancing_authority_code_eia": pd.CategoricalDtype(),
                "subregion_code_eia": pd.CategoricalDtype(),
            }
        )
    )
    return (
        Output(value=demand, output_name="core_eia930__hourly_subregion_demand"),
        Output(
            value=assn, output_name="core_eia930__assn_balancing_authority_subregion"
        ),
    )


@multi_asset(
    outs={
        "core_eia930__hourly_balancing_authority_interchange": AssetOut(),
        "core_eia930__assn_balancing_authority_region": AssetOut(),
    },
    compute_kind="pandas",
)
def core_eia930__hourly_balancing_authority_interchange_assets(
    raw_eia930__interchange: pd.DataFrame,
):
    """Produce a normalized table of hourly interchange by balancing authority.

    * region_code_eia and adjacent_region_code_eia are from the same set of values, but
      adjacent_region_code_eia also includes "CAN" and "MEX" because foreign countries
      can be adjacent, but are not reporting directly.
    * similarly adjacent_balancing_authority_and balancing_authority_code come from the
      same pool of values, but the adjancent_balancing_authority_code_eia includes some
      codes that hail from Canada and Mexico.
    * There's an implied set of associations between the regions and the balancing
      authorities.
    * Need to check for consistency with BA codes mentioned here and elsewhere in
      PUDL (plants table, EIA861 BA table).

    """
    interchange = raw_eia930__interchange.astype(
        {
            "balancing_authority_code_eia": pd.CategoricalDtype(),
            "adjacent_balancing_authority_code_eia": pd.CategoricalDtype(),
            "region_code_eia": pd.CategoricalDtype(),
            "adjacent_region_code_eia": pd.CategoricalDtype(),
        }
    ).loc[
        :,
        [
            "report_datetime_local",
            "report_datetime_utc",
            "balancing_authority_code_eia",
            "adjacent_balancing_authority_code_eia",
            "region_code_eia",
            "adjacent_region_code_eia",
            "interchange_mw",
        ],
    ]
    assn = (
        interchange.groupby("adjacent_region_code_eia")
        .adjacent_balancing_authority_code_eia.unique()
        .explode()
        .to_frame()
        .reset_index()
        .astype(
            {
                "adjacent_region_code_eia": pd.CategoricalDtype(),
                "adjacent_balancing_authority_code_eia": pd.CategoricalDtype(),
            }
        )
        .rename(
            columns={
                "adjacent_region_code_eia": "region_code_eia",
                "adjacent_balancing_authority_code_eia": "balancing_authority_code_eia",
            }
        )
    )
    return (
        Output(
            value=interchange,
            output_name="core_eia930__hourly_balancing_authority_interchange",
        ),
        Output(
            value=assn,
            output_name="core_eia930__assn_balancing_authority_region",
        ),
    )
