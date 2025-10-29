"""EPA CEMS Hourly Emissions assets.

The :func:`core_epacems__hourly_emissions` asset defined in this module uses a dagster pattern
that is unique from other PUDL assets. The underlying architecture uses ops to create a
dynamic graph
which is wrapped by a special asset called a graph backed asset that creates an asset
from a graph of ops. The dynamic graph will allow dagster to dynamically generate an op
for processing each year of EPA CEMS data and execute these ops in parallel. For more information
see: https://docs.dagster.io/concepts/ops-jobs-graphs/dynamic-graphs and https://docs.dagster.io/concepts/assets/graph-backed-assets.
"""

from pathlib import Path

import pandas as pd
import polars as pl
from dagster import (
    AssetIn,
    DynamicOut,
    DynamicOutput,
    Out,
    asset,
    graph_asset,
    op,
)

import pudl
from pudl.extract.epacems import EpaCemsDatastore, EpaCemsPartition
from pudl.transform.epacems import transform_epacems
from pudl.workspace.setup import PudlPaths

logger = pudl.logging_helpers.get_logger(__name__)


def _partitioned_path() -> Path:
    partitioned_path = (
        PudlPaths().output_dir / "parquet" / "raw_epacems__hourly_emissions"
    )
    partitioned_path.mkdir(exist_ok=True)
    return partitioned_path


@op(
    out=DynamicOut(),
    required_resource_keys={"dataset_settings"},
)
def get_year_quarters_from_settings(context):
    """Return set of years in settings.

    These will be used to kick off worker processes to process each year of data in
    parallel.
    """
    epacems_settings = context.resources.dataset_settings.epacems
    for year_quarter in epacems_settings.year_quarters:
        yield DynamicOutput(year_quarter, mapping_key=str(year_quarter))


@op(
    required_resource_keys={"datastore", "dataset_settings"},
)
def extract_quarter(
    context,
    year_quarter: str,
) -> str:
    """Extract a single quarter of EPA CEMS data and write to parquet.

    Args:
        context: dagster keyword that provides access to resources and config.
        year_quarter: Year quarter to process, formatted like '1995q1'.
    """
    ds = context.resources.datastore

    partitioned_path = _partitioned_path()
    partition = EpaCemsPartition(year_quarter=year_quarter)

    ds = EpaCemsDatastore(context.resources.datastore)
    (
        ds.get_data_frame(partition=partition)
        .with_columns(year=partition.year)
        .sink_parquet(partitioned_path / f"{year_quarter}.parquet")
    )

    return year_quarter


@op(out={"core_epacems__hourly_emissions": Out(io_manager_key="parquet_io_manager")})
def transform_and_write_monolithic(
    partitions: list[str],
    core_epa__assn_eia_epacamd: pd.DataFrame,
    core_eia__entity_plants: pd.DataFrame,
) -> pl.LazyFrame:
    """Transform EQR data and write to a monolithic parquet file.

    Reads raw EQR data from partitioned dataset created by ``extract_quarter``, then
    uses Polars streaming engine to perform transforms and write to parquet in one
    step.

    Args:
        partitions: Year and state combinations in the output database.
    """
    partitioned_path = _partitioned_path()

    return pl.scan_parquet(partitioned_path).pipe(
        transform_epacems,
        core_epa__assn_eia_epacamd=core_epa__assn_eia_epacamd,
        core_eia__entity_plants=core_eia__entity_plants,
    )


@graph_asset
def core_epacems__hourly_emissions(
    _core_epa__assn_eia_epacamd_unique: pd.DataFrame,
    core_eia__entity_plants: pd.DataFrame,
) -> None:
    """Extract, transform and load CSVs for EPA CEMS.

    This asset creates a dynamic graph of ops to process EPA CEMS data in parallel. It
    will create both a partitioned and single monolithic parquet output. For more
    information see:
    https://docs.dagster.io/concepts/ops-jobs-graphs/dynamic-graphs.
    """
    year_quarters = get_year_quarters_from_settings()
    partitions = year_quarters.map(
        lambda year_quarter: extract_quarter(
            year_quarter,
        )
    )
    return transform_and_write_monolithic(
        partitions.collect(),
        _core_epa__assn_eia_epacamd_unique,
        core_eia__entity_plants,
    )


@asset(
    ins={
        "core_epacems__hourly_emissions": AssetIn(input_manager_key="pudl_io_manager"),
    },
)
def _core_epacems__emissions_unit_ids(
    core_epacems__hourly_emissions: pl.LazyFrame,
) -> pd.DataFrame:
    """Make unique annual plant_id_eia and emissions_unit_id_epa.

    Returns:
        dataframe with unique set of: "plant_id_eia", "year" and "emissions_unit_id_epa"
    """
    return (
        core_epacems__hourly_emissions.select(
            ["plant_id_eia", "year", "emissions_unit_id_epa"]
        )
        .unique()
        .collect(engine="streaming")
    ).to_pandas()
