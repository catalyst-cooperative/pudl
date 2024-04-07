"""EPA CEMS Hourly Emissions assets.

The :func:`core_epacems__hourly_emissions` asset defined in this module uses a dagster pattern
that is unique from other PUDL assets. The underlying architecture uses ops to create a
dynamic graph
which is wrapped by a special asset called a graph backed asset that creates an asset
from a graph of ops. The dynamic graph will allow dagster to dynamically generate an op
for processing each year of EPA CEMS data and execute these ops in parallel. For more information
see: https://docs.dagster.io/concepts/ops-jobs-graphs/dynamic-graphs and https://docs.dagster.io/concepts/assets/graph-backed-assets.
"""

from collections import namedtuple
from pathlib import Path

import dask.dataframe as dd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import (
    AssetIn,
    DynamicOut,
    DynamicOutput,
    asset,
    graph_asset,
    op,
)

import pudl
from pudl.extract.epacems import EpaCemsPartition
from pudl.metadata.classes import Resource
from pudl.metadata.enums import EPACEMS_STATES
from pudl.workspace.setup import PudlPaths

logger = pudl.logging_helpers.get_logger(__name__)


YearPartitions = namedtuple("YearPartitions", ["year_quarters"])


def _partitioned_path() -> Path:
    partitioned_path = (
        PudlPaths().output_dir / "parquet" / "core_epacems__hourly_emissions"
    )
    partitioned_path.mkdir(exist_ok=True)
    return partitioned_path


@op(
    out=DynamicOut(),
    required_resource_keys={"dataset_settings"},
)
def get_years_from_settings(context):
    """Return set of years in settings.

    These will be used to kick off worker processes to process each year of data in
    parallel.
    """
    epacems_settings = context.resources.dataset_settings.epacems
    years = {
        EpaCemsPartition(year_quarter=yq).year for yq in epacems_settings.year_quarters
    }
    for year in years:
        yield DynamicOutput(year, mapping_key=str(year))


@op(
    required_resource_keys={"datastore", "dataset_settings"},
    tags={"memory-use": "high"},
)
def process_single_year(
    context,
    year,
    core_epa__assn_eia_epacamd: pd.DataFrame,
    core_eia__entity_plants: pd.DataFrame,
) -> YearPartitions:
    """Process a single year of EPA CEMS data.

    Args:
        context: dagster keyword that provides access to resources and config.
        year: Year of data to process.
        core_epa__assn_eia_epacamd: The EPA EIA crosswalk table used for harmonizing the
            ORISPL code with EIA.
        core_eia__entity_plants: The EIA Plant entities used for aligning timezones.
    """
    ds = context.resources.datastore
    epacems_settings = context.resources.dataset_settings.epacems

    schema = Resource.from_id("core_epacems__hourly_emissions").to_pyarrow()
    partitioned_path = _partitioned_path()

    year_quarters_in_year = {
        yq
        for yq in epacems_settings.year_quarters
        if EpaCemsPartition(year_quarter=yq).year == year
    }

    for year_quarter in year_quarters_in_year:
        logger.info(f"Processing EPA CEMS hourly data for {year_quarter}")
        df = pudl.extract.epacems.extract(year_quarter=year_quarter, ds=ds)
        if not df.empty:  # If state-year combination has data
            df = pudl.transform.epacems.transform(
                df, core_epa__assn_eia_epacamd, core_eia__entity_plants
            )
        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)

        # Write to a directory of partitioned parquet files
        with pq.ParquetWriter(
            where=partitioned_path / f"epacems-{year_quarter}.parquet",
            schema=schema,
            compression="snappy",
            version="2.6",
        ) as partitioned_writer:
            partitioned_writer.write_table(table)

    return YearPartitions(year_quarters_in_year)


@op
def consolidate_partitions(context, partitions: list[YearPartitions]) -> None:
    """Read partitions into memory and write to a single monolithic output.

    Args:
        context: dagster keyword that provides access to resources and config.
        partitions: Year and state combinations in the output database.
    """
    partitioned_path = _partitioned_path()
    monolithic_path = (
        PudlPaths().output_dir / "parquet" / "core_epacems__hourly_emissions.parquet"
    )
    schema = Resource.from_id("core_epacems__hourly_emissions").to_pyarrow()

    with pq.ParquetWriter(
        where=monolithic_path, schema=schema, compression="snappy", version="2.6"
    ) as monolithic_writer:
        for year_partition in partitions:
            for state in EPACEMS_STATES:
                monolithic_writer.write_table(
                    # Concat a slice of each state's data from all quarters in a year
                    # and write to parquet to create year-state row groups
                    pa.concat_tables(
                        [
                            pq.read_table(
                                source=partitioned_path
                                / f"epacems-{year_quarter}.parquet",
                                filters=[[("state", "=", state.upper())]],
                                schema=schema,
                            )
                            for year_quarter in year_partition.year_quarters
                        ]
                    )
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
    years = get_years_from_settings()
    partitions = years.map(
        lambda year: process_single_year(
            year,
            _core_epa__assn_eia_epacamd_unique,
            core_eia__entity_plants,
        )
    )
    return consolidate_partitions(partitions.collect())


@asset(
    ins={
        "core_epacems__hourly_emissions": AssetIn(
            input_manager_key="epacems_io_manager"
        ),
    },
    compute_kind="Dask",
)
def _core_epacems__emissions_unit_ids(
    core_epacems__hourly_emissions: dd.DataFrame,
) -> pd.DataFrame:
    """Make unique annual plant_id_eia and emissions_unit_id_epa.

    Returns:
        dataframe with unique set of: "plant_id_eia", "year" and "emissions_unit_id_epa"
    """
    epacems_ids = (
        core_epacems__hourly_emissions[
            ["plant_id_eia", "year", "emissions_unit_id_epa"]
        ]
        .drop_duplicates()
        .compute()
    )

    return epacems_ids
