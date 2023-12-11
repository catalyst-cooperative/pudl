"""EPA CEMS Hourly Emissions assets.

The :func:`hourly_emissions_epacems` asset defined in this module uses a dagster pattern
that is unique from other PUDL assets. The underlying architecture uses ops to create a
dynamic graph
which is wrapped by a special asset called a graph backed asset that creates an asset
from a graph of ops. The dynamic graph will allow dagster to dynamically generate an op
for processing each year of EPA CEMS data and execute these ops in parallel. For more information
see: https://docs.dagster.io/concepts/ops-jobs-graphs/dynamic-graphs and https://docs.dagster.io/concepts/assets/graph-backed-assets.
"""
from collections import namedtuple

import dask.dataframe as dd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import AssetIn, DynamicOut, DynamicOutput, asset, graph_asset, op

import pudl
from pudl.extract.epacems import EpaCemsPartition
from pudl.metadata.classes import Resource
from pudl.metadata.enums import EPACEMS_STATES
from pudl.workspace.setup import PudlPaths

logger = pudl.logging_helpers.get_logger(__name__)


YearPartitions = namedtuple("YearPartitions", ["year_quarters"])


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
    tags={"datasource": "epacems"},
)
def process_single_year(
    context,
    year,
    epacamd_eia: pd.DataFrame,
    plants_entity_eia: pd.DataFrame,
) -> YearPartitions:
    """Process a single year of EPA CEMS data.

    Args:
        context: dagster keyword that provides access to resources and config.
        year: Year of data to process.
        epacamd_eia: The EPA EIA crosswalk table used for harmonizing the
            ORISPL code with EIA.
        plants_entity_eia: The EIA Plant entities used for aligning timezones.
    """
    ds = context.resources.datastore
    epacems_settings = context.resources.dataset_settings.epacems

    schema = Resource.from_id("hourly_emissions_epacems").to_pyarrow()
    partitioned_path = PudlPaths().output_dir / "hourly_emissions_epacems"
    partitioned_path.mkdir(exist_ok=True)

    year_quarters_in_year = {
        yq
        for yq in epacems_settings.year_quarters
        if EpaCemsPartition(year_quarter=yq).year == year
    }

    for year_quarter in year_quarters_in_year:
        logger.info(f"Processing EPA CEMS hourly data for {year_quarter}")
        df = pudl.extract.epacems.extract(year_quarter=year_quarter, ds=ds)
        if not df.empty:  # If state-year combination has data
            df = pudl.transform.epacems.transform(df, epacamd_eia, plants_entity_eia)
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
    partitioned_path = PudlPaths().output_dir / "hourly_emissions_epacems"
    monolithic_path = PudlPaths().output_dir / "hourly_emissions_epacems.parquet"
    schema = Resource.from_id("hourly_emissions_epacems").to_pyarrow()

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
def hourly_emissions_epacems(
    epacamd_eia_unique: pd.DataFrame, plants_entity_eia: pd.DataFrame
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
            epacamd_eia_unique,
            plants_entity_eia,
        )
    )
    return consolidate_partitions(partitions.collect())


@asset(
    ins={
        "hourly_emissions_epacems": AssetIn(input_manager_key="epacems_io_manager"),
    }
)
def emissions_unit_ids_epacems(
    hourly_emissions_epacems: dd.DataFrame,
) -> pd.DataFrame:
    """Make unique annual plant_id_eia and emissions_unit_id_epa.

    Returns:
        dataframe with unique set of: "plant_id_eia", "year" and "emissions_unit_id_epa"
    """
    epacems_ids = (
        hourly_emissions_epacems[["plant_id_eia", "year", "emissions_unit_id_epa"]]
        .drop_duplicates()
        .compute()
    )

    return epacems_ids
