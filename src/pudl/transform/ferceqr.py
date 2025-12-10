"""Transform FERC EQR data."""

import dagster as dg
import polars as pl

from pudl.helpers import ParquetData, lf_from_parquet
from pudl.metadata.classes import Resource
from pudl.settings import ferceqr_year_quarters


def _apply_schema(table_name: str, lf: pl.LazyFrame) -> pl.LazyFrame:
    res = Resource.from_id(table_name)
    schema = res.to_polars_dtypes()

    return lf.cast(
        schema,
        strict=False,
    ).select(list(schema.keys()))


def _map_timezones(lf: pl.LazyFrame) -> pl.LazyFrame:
    tz_map = {
        "EP": "America/New_York",
        "PP": "America/Los_Angeles",
        "ES": "America/New_York",
        "ED": "America/New_York",
        "CP": "America/Chicago",
        "MS": "America/Denver",
        "MD": "America/Denver",
        "CD": "America/Chicago",
        "CS": "America/Chicago",
        "PS": "America/Los_Angeles",
        "PD": "America/Los_Angeles",
        "MP": "America/Denver",
        "EPT": "America/New_York",
        "CPT": "America/Chicago",
        "CST": "America/Chicago",
        "CDT": "America/Chicago",
        "PPT": "America/Los_Angeles",
        "EDT": "America/New_York",
        "AP": "America/New_York",
        "MPT": "America/Denver",
    }
    return (
        lf.with_columns(
            pl.when(pl.col("time_zone").str.to_uppercase().is_in(["N/A", "NA"]))
            .then(None)
            .otherwise(pl.col("time_zone").str.to_uppercase())
            .alias("timezone")
        )
        .drop("time_zone")
        .with_columns(pl.col("timezone").replace(tz_map))
    )


@dg.asset(io_manager_key="parquet_io_manager", partitions_def=ferceqr_year_quarters)
def core_ferceqr__identity(raw_ferceqr__ident: ParquetData):
    """Apply data types to EQR ident table."""
    return _apply_schema(
        "core_ferceqr__identity",
        lf_from_parquet(raw_ferceqr__ident, use_all_partitions=True).with_columns(
            pl.when(pl.col("transactions_reported_to_index_price_publishers") == "Y")
            .then(True)
            .when(pl.col("transactions_reported_to_index_price_publishers") == "N")
            .then(False)
            .otherwise(None)
            .alias("transactions_reported_to_index_price_publishers")
        ),
    )


@dg.asset(
    io_manager_key="parquet_io_manager",
    partitions_def=ferceqr_year_quarters,
    op_tags={"memory-use": "high"},
)
def core_ferceqr__transactions(raw_ferceqr__transactions: ParquetData):
    """Perform basic transforms on transactions table table."""
    return _apply_schema(
        "core_ferceqr__transactions",
        lf_from_parquet(raw_ferceqr__transactions, use_all_partitions=True)
        .with_columns(
            pl.col("transaction_begin_date").str.strptime(
                pl.Datetime, format="%Y%m%d%H%M"
            )
        )
        .with_columns(
            pl.col("contract_service_agreement").alias("contract_service_agreement_id")
        )
        .with_columns(
            pl.col("transaction_end_date").str.strptime(
                pl.Datetime, format="%Y%m%d%H%M"
            )
        )
        .with_columns(pl.col("trade_date").str.strptime(pl.Date, format="%Y%m%d"))
        .with_columns(
            pl.when(pl.col("exchange_brokerage_service").str.to_uppercase() == "N/A")
            .then(None)
            .otherwise(pl.col("exchange_brokerage_service").str.to_uppercase())
            .alias("exchange_brokerage_service")
        )
        .with_columns(
            pl.when(pl.col("type_of_rate").str.to_uppercase() == "N/A")
            .then(None)
            .otherwise(pl.col("type_of_rate").str.to_uppercase())
            .alias("type_of_rate")
        )
        .with_columns(
            pl.when(pl.col("class_name").str.to_uppercase() == "N/A")
            .then(None)
            .otherwise(pl.col("class_name").str.to_uppercase())
            .alias("class_name")
        )
        .with_columns(
            pl.when(pl.col("term_name").str.to_uppercase() == "N/A")
            .then(None)
            .otherwise(pl.col("term_name").str.to_uppercase())
            .alias("term_name")
        )
        .with_columns(
            pl.when(pl.col("increment_name").str.to_uppercase() == "N/A")
            .then(None)
            .otherwise(pl.col("increment_name").str.to_uppercase())
            .alias("increment_name")
        )
        .with_columns(
            pl.when(pl.col("increment_peaking_name").str.to_uppercase() == "N/A")
            .then(None)
            .otherwise(pl.col("increment_peaking_name").str.to_uppercase())
            .alias("increment_peaking_name")
        )
        .with_columns(
            pl.when(pl.col("product_name").str.to_uppercase() == "N/A")
            .then(None)
            .otherwise(pl.col("product_name").str.to_uppercase())
            .alias("product_name")
        )
        .with_columns(
            pl.when(pl.col("rate_units").str.to_uppercase() == "N/A")
            .then(None)
            .otherwise(pl.col("rate_units").str.to_uppercase())
            .alias("rate_units")
        )
        .with_columns(
            pl.col("point_of_delivery_balancing_authority").str.to_uppercase()
        )
        .with_columns(pl.col("point_of_delivery_specific_location").str.to_uppercase())
        .pipe(_map_timezones),
    )


@dg.asset(io_manager_key="parquet_io_manager", partitions_def=ferceqr_year_quarters)
def core_ferceqr__contracts(raw_ferceqr__contracts: ParquetData):
    """Perform basic transforms on contracts table table."""
    return _apply_schema(
        "core_ferceqr__contracts",
        lf_from_parquet(raw_ferceqr__contracts, use_all_partitions=True)
        .with_columns(
            pl.when(pl.col("contract_affiliate").str.to_uppercase() == "N/A")
            .then(None)
            .otherwise(pl.col("contract_affiliate").str.to_uppercase())
            .alias("contract_affiliate")
        )
        .with_columns(
            pl.col("contract_execution_date").str.strptime(pl.Datetime, format="%Y%m%d")
        )
        .with_columns(
            pl.col("commencement_date_of_contract_term").str.strptime(
                pl.Datetime, format="%Y%m%d"
            )
        )
        .with_columns(
            pl.col("contract_termination_date").str.strptime(pl.Date, format="%Y%m%d")
        )
        .with_columns(
            pl.col("actual_termination_date").str.strptime(pl.Date, format="%Y%m%d")
        )
        .with_columns(
            pl.when(pl.col("class_name").str.to_uppercase() == "N/A")
            .then(None)
            .otherwise(pl.col("class_name").str.to_uppercase())
            .alias("class_name")
        )
        .with_columns(
            pl.when(pl.col("term_name").str.to_uppercase() == "N/A")
            .then(None)
            .otherwise(pl.col("term_name").str.to_uppercase())
            .alias("term_name")
        )
        .with_columns(
            pl.when(pl.col("increment_name").str.to_uppercase() == "N/A")
            .then(None)
            .otherwise(pl.col("increment_name").str.to_uppercase())
            .alias("increment_name")
        )
        .with_columns(
            pl.when(pl.col("increment_peaking_name").str.to_uppercase() == "N/A")
            .then(None)
            .otherwise(pl.col("increment_peaking_name").str.to_uppercase())
            .alias("increment_peaking_name")
        )
        .with_columns(
            pl.when(pl.col("product_type_name").str.to_uppercase() == "N/A")
            .then(None)
            .otherwise(pl.col("product_type_name").str.to_uppercase())
            .alias("product_type_name")
        )
        .with_columns(
            pl.when(pl.col("product_name").str.to_uppercase() == "N/A")
            .then(None)
            .otherwise(pl.col("product_name").str.to_uppercase())
            .alias("product_name")
        )
        .with_columns(
            pl.when(pl.col("rate_units").str.to_uppercase() == "N/A")
            .then(None)
            .otherwise(pl.col("rate_units").str.to_uppercase())
            .alias("rate_units")
        )
        .with_columns(
            pl.col("begin_date").str.strptime(pl.Datetime, format="%Y%m%d%H%M")
        )
        .with_columns(pl.col("end_date").str.strptime(pl.Datetime, format="%Y%m%d%H%M"))
        .with_columns(
            pl.when(pl.col("contract_affiliate") == "Y")
            .then(True)
            .when(pl.col("contract_affiliate") == "N")
            .then(False)
            .otherwise(None)
            .alias("contract_affiliate")
        ),
    )
