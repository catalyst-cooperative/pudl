"""Module to perform data cleaning functions on EIA930 data tables."""

import pandas as pd
from dagster import AssetOut, Output, asset, multi_asset

import pudl
from pudl.metadata.enums import GENERATION_ENERGY_SOURCES_EIA930

logger = pudl.logging_helpers.get_logger(__name__)


@multi_asset(
    outs={
        "core_eia930__hourly_net_generation_by_energy_source": AssetOut(
            io_manager_key="parquet_io_manager"
        ),
        "core_eia930__hourly_operations": AssetOut(io_manager_key="parquet_io_manager"),
    },
    compute_kind="pandas",
)
def core_eia930__hourly_operations_assets(
    raw_eia930__balance: pd.DataFrame,
):
    """Separate raw_eia930__balance into net generation and demand tables.

    Energy source starts out in the column names, but is stacked into a categorical
    column. For structural purposes "interchange" is also treated as an "energy source"
    and stacked into the same column. For the moment "total" (sum of all energy sources)
    is also included, because the reported and calculated totals across all energy
    sources have significant differences which should be further explored.
    """
    nondata_cols = [
        "datetime_utc",
        "balancing_authority_code_eia",
    ]
    # Select all columns that aren't energy source specific
    operations = raw_eia930__balance[
        nondata_cols
        + list(
            raw_eia930__balance.filter(
                regex=r"(demand|interchange|net_generation_total)"
            )
        )
    ].rename(columns=lambda x: x.replace("net_generation_total_", "net_generation_"))
    # Select only the columns that pertain to individual energy sources. Note that for
    # the "unknown" energy source there are only "reported" values.
    netgen_by_source = (
        raw_eia930__balance[
            nondata_cols
            + [
                f"net_generation_{fuel}_{status}_mwh"
                for fuel in GENERATION_ENERGY_SOURCES_EIA930
                for status in ["reported", "adjusted", "imputed"]
            ]
        ]
        .rename(
            # Rename columns so that they contain only the energy source and the level
            # of processing with the pattern: energysource_levelofprocessing so the
            # column name can be rsplit on "_" to build a MultiIndex before stacking.
            lambda col: col.removeprefix("net_generation_").removesuffix("_mwh"),
            axis="columns",
        )
        .set_index(nondata_cols)
    )
    netgen_by_source.columns = pd.MultiIndex.from_tuples(
        # Some of our energy sources have multiple terms in them, so we rsplit on
        # a maximum of one underscore to ensure we get the exact two results we need:
        [x.rsplit("_", maxsplit=1) for x in netgen_by_source.columns],
        names=["generation_energy_source", None],
    )
    netgen_by_source = (
        netgen_by_source.stack(level="generation_energy_source", future_stack=True)
        .rename(columns=lambda x: f"net_generation_{x}_mwh")
        .reset_index()
    )

    netgen_by_source = netgen_by_source.rename(
        columns={"net_generation_imputed_mwh": "net_generation_imputed_eia_mwh"}
    )
    operations = operations.rename(
        columns={
            "net_generation_imputed_mwh": "net_generation_imputed_eia_mwh",
            "demand_imputed_mwh": "demand_imputed_eia_mwh",
            "interchange_imputed_mwh": "interchange_imputed_eia_mwh",
        }
    )

    # NOTE: currently there are some BIG differences between the calculated totals and
    # the reported for net generation.
    return (
        Output(
            value=netgen_by_source,
            output_name="core_eia930__hourly_net_generation_by_energy_source",
        ),
        Output(
            value=operations,
            output_name="core_eia930__hourly_operations",
        ),
    )


@asset(
    io_manager_key="parquet_io_manager",
    compute_kind="pandas",
)
def core_eia930__hourly_subregion_demand(
    raw_eia930__subregion: pd.DataFrame,
):
    """Produce a normalized table of hourly electricity demand by BA subregion."""
    return raw_eia930__subregion.assign(
        balancing_authority_subregion_code_eia=lambda df: df[
            "balancing_authority_subregion_code_eia"
        ].str.upper()
    ).loc[
        :,
        [
            "datetime_utc",
            "balancing_authority_code_eia",
            "balancing_authority_subregion_code_eia",
            "demand_reported_mwh",
        ],
    ]


@asset(
    io_manager_key="parquet_io_manager",
    compute_kind="pandas",
)
def core_eia930__hourly_interchange(
    raw_eia930__interchange: pd.DataFrame,
):
    """Produce a normalized table of hourly interchange by balancing authority."""
    return raw_eia930__interchange.loc[
        :,
        [
            "datetime_utc",
            "balancing_authority_code_eia",
            "balancing_authority_code_adjacent_eia",
            "interchange_reported_mwh",
        ],
    ]
