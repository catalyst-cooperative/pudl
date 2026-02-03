"""Module to perform data cleaning functions on EIA930 data tables."""

from pathlib import Path

import pandas as pd
import polars as pl
from dagster import AssetOut, Output, asset, multi_asset

import pudl
from pudl.helpers import ParquetData, persist_table_as_parquet, lf_from_parquet, duckdb_relation_from_parquet
from pudl.metadata.enums import GENERATION_ENERGY_SOURCES_EIA930

logger = pudl.logging_helpers.get_logger(__name__)


@multi_asset(
    outs={
        "core_eia930__hourly_net_generation_by_energy_source": AssetOut(
            io_manager_key="parquet_io_manager"
        ),
        "core_eia930__hourly_operations": AssetOut(),
    },
    compute_kind="pandas",
)
def core_eia930__hourly_operations_assets(
    raw_eia930__balance: ParquetData,
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
    data_cols = [
        (f"net_generation_{fuel}_{status}_mwh", f"{fuel}_{status}")
        for fuel in GENERATION_ENERGY_SOURCES_EIA930
        for status in ["reported", "adjusted", "imputed"]
    ]
    with duckdb_relation_from_parquet(raw_eia930__balance) as (tbl, conn,):
        conn.sql("SET memory_limit = '16GB';")
        _ = tbl.select(
            ", ".join(
                nondata_cols + [f"COALESCE(CAST({long} AS BIGINT), -1) AS {short}" for long, short in data_cols]
            )
        ).to_view("raw")
        _ = conn.query(
            "UNPIVOT raw "
            f"ON {', '.join([short for _, short in data_cols])} "
            "INTO NAME category VALUE net_generation_mwh",
        ).to_view("long")
        _ = conn.query(
            "SELECT * EXCLUDE(category), "
            "regexp_extract(category, '^(.*)_([^_]*)$', 1) AS generation_energy_source, "
            "regexp_extract(generation_energy_source, '^(.*)_([^_]*)$', 2) AS value_type "
            "FROM long",
        ).to_view("long_cols_split")
        table = conn.query(
            "PIVOT long_cols_split "
            "ON value_type "
            "USING first(net_generation_mwh)"
        )
        table.to_parquet("test.parquet")

    # Select all columns that aren't energy source specific
    """
    operations = raw_eia930__balance_lf.select(
        pl.col(nondata_cols),
        pl.selectors.contains("demand", "interchange", "net_generation_total"),
    ).rename(lambda x: x.replace("net_generation_total_", "net_generation_"))
    """
    # Select only the columns that pertain to individual energy sources. Note that for
    """
    operations = operations.rename(
        mapping={
            "net_generation_imputed_mwh": "net_generation_imputed_eia_mwh",
            "demand_imputed_mwh": "demand_imputed_eia_mwh",
            "interchange_imputed_mwh": "interchange_imputed_eia_mwh",
        }
    )
    """

    # NOTE: currently there are some BIG differences between the calculated totals and
    # the reported for net generation.
    return (
        Output(
            value=netgen_by_source,
            output_name="core_eia930__hourly_net_generation_by_energy_source",
        ),
        Output(
            value=None,
            output_name="core_eia930__hourly_operations",
        ),
    )


@asset(
    io_manager_key="parquet_io_manager",
    compute_kind="pandas",
)
def core_eia930__hourly_subregion_demand(
    raw_eia930__subregion: Path,
):
    """Produce a normalized table of hourly electricity demand by BA subregion."""
    raw_eia930__subregion_df = pd.read_parquet(raw_eia930__subregion)
    return raw_eia930__subregion_df.assign(
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
    raw_eia930__interchange: Path,
):
    """Produce a normalized table of hourly interchange by balancing authority."""
    raw_eia930__interchange_df = pd.read_parquet(raw_eia930__interchange)
    return raw_eia930__interchange_df.loc[
        :,
        [
            "datetime_utc",
            "balancing_authority_code_eia",
            "balancing_authority_code_adjacent_eia",
            "interchange_reported_mwh",
        ],
    ]
