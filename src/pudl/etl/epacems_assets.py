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
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import DynamicOut, DynamicOutput, Field, graph_asset, op

import pudl
from pudl.helpers import EnvVar
from pudl.metadata.classes import Resource

logger = pudl.logging_helpers.get_logger(__name__)


YearPartitions = namedtuple("YearPartitions", ["year", "states"])


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
    for year in epacems_settings.years:
        yield DynamicOutput(year, mapping_key=str(year))


@op(
    required_resource_keys={"datastore", "dataset_settings"},
    config_schema={
        "pudl_output_path": Field(
            EnvVar(
                env_var="PUDL_OUTPUT",
            ),
            description="Path of directory to store the database in.",
            default_value=None,
        ),
    },
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
    partitioned_path = (
        Path(context.op_config["pudl_output_path"]) / "hourly_emissions_epacems"
    )
    partitioned_path.mkdir(exist_ok=True)

    for state in epacems_settings.states:
        logger.info(f"Processing EPA CEMS hourly data for {year}-{state}")
        df = pudl.extract.epacems.extract(year=year, state=state, ds=ds)
        df = pudl.transform.epacems.transform(df, epacamd_eia, plants_entity_eia)
        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)

        # Write to a directory of partitioned parquet files
        with pq.ParquetWriter(
            where=partitioned_path / f"epacems-{year}-{state}.parquet",
            schema=schema,
            compression="snappy",
            version="2.6",
        ) as partitioned_writer:
            partitioned_writer.write_table(table)

    return YearPartitions(year, epacems_settings.states)


@op(
    config_schema={
        "pudl_output_path": Field(
            EnvVar(
                env_var="PUDL_OUTPUT",
            ),
            description="Path of directory to store the database in.",
            default_value=None,
        ),
    },
)
def consolidate_partitions(context, partitions: list[YearPartitions]) -> None:
    """Read partitions into memory and write to a single monolithic output.

    Args:
        context: dagster keyword that provides access to resources and config.
        partitions: Year and state combinations in the output database.
    """
    partitioned_path = (
        Path(context.op_config["pudl_output_path"]) / "hourly_emissions_epacems"
    )
    monolithic_path = (
        Path(context.op_config["pudl_output_path"]) / "hourly_emissions_epacems.parquet"
    )
    schema = Resource.from_id("hourly_emissions_epacems").to_pyarrow()

    with pq.ParquetWriter(
        where=monolithic_path, schema=schema, compression="snappy", version="2.6"
    ) as monolithic_writer:
        for year, states in partitions:
            for state in states:
                monolithic_writer.write_table(
                    pq.read_table(
                        source=partitioned_path / f"epacems-{year}-{state}.parquet",
                        schema=schema,
                    )
                )


@graph_asset
def hourly_emissions_epacems(
    epacamd_eia: pd.DataFrame, plants_entity_eia: pd.DataFrame
) -> None:
    """Extract, transform and load CSVs for EPA CEMS.

    This asset creates a dynamic graph of ops to process EPA CEMS data in parallel. It
    will create both a partitioned and single monolithic parquet output. For more
    information see: https://docs.dagster.io/concepts/ops-jobs-graphs/dynamic-graphs.
    """
    years = get_years_from_settings()
    partitions = years.map(
        lambda year: process_single_year(
            year,
            epacamd_eia,
            plants_entity_eia,
        )
    )
    return consolidate_partitions(partitions.collect())
