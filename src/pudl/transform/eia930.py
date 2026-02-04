"""Module to perform data cleaning functions on EIA930 data tables."""

import duckdb
from dagster import AssetOut, Output, asset, multi_asset

import pudl
from pudl.helpers import (
    ParquetData,
    df_from_parquet,
    duckdb_relation_from_parquet,
    lf_from_parquet,
    persist_table_as_parquet,
)
from pudl.metadata.enums import GENERATION_ENERGY_SOURCES_EIA930

logger = pudl.logging_helpers.get_logger(__name__)


def _transform_netgen_by_source(
    table: duckdb.DuckDBPyRelation, conn: duckdb.DuckDBPyConnection
) -> ParquetData:
    """Transform the eia930 netgen by source table."""
    statuses = ["reported", "adjusted", "imputed"]
    nondata_cols = [
        "datetime_utc",
        "balancing_authority_code_eia",
    ]
    data_cols = [
        (f"net_generation_{fuel}_{status}_mwh", f"{fuel}_{status}")
        for fuel in GENERATION_ENERGY_SOURCES_EIA930
        for status in statuses
    ]

    # Select only the columns that pertain to individual energy sources. Note that for
    # the "unknown" energy source there are only "reported" values.
    table.select(
        ", ".join(
            nondata_cols
            + [
                # Set NULL vals to -1 to avoid rows being dropped in UNPIVOT
                f"COALESCE(CAST({long} AS FLOAT), -1) AS {short}"
                for long, short in data_cols
            ]
        )
    ).to_view("raw")
    conn.query(
        # Transform wide table to long
        # category column will contain values formatted like ``{energy_source}_{adjusted|imputed|reported}``
        "UNPIVOT raw "
        f"ON {', '.join([short for _, short in data_cols])} "
        "INTO NAME category VALUE net_generation_mwh",
    ).select(
        # Split category column to separate generation_energy_source and status columns
        "* EXCLUDE(category, net_generation_mwh), "
        "regexp_extract(category, '^(.*)_([^_]*)$', 1) AS generation_energy_source, "
        "regexp_extract(category, '^(.*)_([^_]*)$', 2) AS value_type, "
        "CASE WHEN net_generation_mwh = -1 THEN NULL ELSE net_generation_mwh END as net_generation_mwh"
    ).to_view("long")
    netgen_by_source = (
        # Widen table slightly contain one column per status
        conn.query("PIVOT long ON value_type USING first(net_generation_mwh)")
        .select(
            f"* EXCLUDE({', '.join(statuses)}), "
            "imputed as net_generation_imputed_eia_mwh, "
            "reported as net_generation_reported_mwh, "
            "adjusted as net_generation_adjusted_mwh"
        )
        .order("datetime_utc, balancing_authority_code_eia, generation_energy_source")
    )

    return persist_table_as_parquet(
        netgen_by_source, "core_eia930__hourly_net_generation_by_energy_source"
    )


def _transform_hourly_operations(
    table: duckdb.DuckDBPyRelation, conn: duckdb.DuckDBPyConnection
) -> ParquetData:
    """Transform the eia930 hourly operations table."""
    operations = table.select(
        duckdb.ColumnExpression("datetime_utc"),
        duckdb.ColumnExpression("balancing_authority_code_eia"),
        duckdb.ColumnExpression("net_generation_total_reported_mwh").alias(
            "net_generation_reported_mwh"
        ),
        duckdb.ColumnExpression("net_generation_total_adjusted_mwh").alias(
            "net_generation_adjusted_mwh"
        ),
        duckdb.ColumnExpression("net_generation_total_imputed_mwh").alias(
            "net_generation_imputed_eia_mwh"
        ),
        duckdb.ColumnExpression("interchange_reported_mwh"),
        duckdb.ColumnExpression("interchange_adjusted_mwh"),
        duckdb.ColumnExpression("interchange_imputed_mwh").alias(
            "interchange_imputed_eia_mwh"
        ),
        duckdb.ColumnExpression("demand_reported_mwh"),
        duckdb.ColumnExpression("demand_adjusted_mwh"),
        duckdb.ColumnExpression("demand_imputed_mwh").alias("demand_imputed_eia_mwh"),
        duckdb.ColumnExpression("demand_forecast_mwh"),
    )
    return persist_table_as_parquet(operations, "core_eia930__hourly_operations")


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
    raw_eia930__balance: ParquetData,
):
    """Separate raw_eia930__balance into net generation and demand tables.

    Energy source starts out in the column names, but is stacked into a categorical
    column. For structural purposes "interchange" is also treated as an "energy source"
    and stacked into the same column. For the moment "total" (sum of all energy sources)
    is also included, because the reported and calculated totals across all energy
    sources have significant differences which should be further explored.
    """
    with duckdb_relation_from_parquet(raw_eia930__balance) as (
        tbl,
        conn,
    ):
        conn.sql("SET memory_limit = '16GB';")
        netgen_parquet = _transform_netgen_by_source(tbl, conn)
        operations = _transform_hourly_operations(tbl, conn)

    # NOTE: currently there are some BIG differences between the calculated totals and
    # the reported for net generation.
    return (
        Output(
            value=lf_from_parquet(netgen_parquet),
            output_name="core_eia930__hourly_net_generation_by_energy_source",
        ),
        Output(
            value=lf_from_parquet(operations),
            output_name="core_eia930__hourly_operations",
        ),
    )


@asset(
    io_manager_key="parquet_io_manager",
    compute_kind="pandas",
)
def core_eia930__hourly_subregion_demand(
    raw_eia930__subregion: ParquetData,
):
    """Produce a normalized table of hourly electricity demand by BA subregion."""
    raw_eia930__subregion_df = df_from_parquet(raw_eia930__subregion)
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
    raw_eia930__interchange: ParquetData,
):
    """Produce a normalized table of hourly interchange by balancing authority."""
    raw_eia930__interchange_df = df_from_parquet(raw_eia930__interchange)
    return raw_eia930__interchange_df.loc[
        :,
        [
            "datetime_utc",
            "balancing_authority_code_eia",
            "balancing_authority_code_adjacent_eia",
            "interchange_reported_mwh",
        ],
    ]
