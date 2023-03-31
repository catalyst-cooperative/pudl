"""EPA CEMS Hourly Emissions assets."""
from pathlib import Path

import dask.dataframe as dd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import DynamicOut, DynamicOutput, Field, asset, graph_asset, op

import pudl
from pudl.helpers import EnvVar
from pudl.metadata.classes import Resource

logger = pudl.logging_helpers.get_logger(__name__)


@op(
    out=DynamicOut(),
    required_resource_keys={"dataset_settings"},
)
def get_year(context):
    """Return set of requested years."""
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
        "partition": Field(
            bool,
            description="Flag indicating whether the partitioned EPACEMS output should be created",
            default_value=False,
        ),
    },
)
def process_single_year(
    context,
    year,
    epacamd_eia: pd.DataFrame,
    plants_entity_eia: pd.DataFrame,
):
    """Process a single year of epacems data."""
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

        if context.op_config["partition"]:
            # Write to a directory of partitioned parquet files
            with pq.ParquetWriter(
                where=partitioned_path / f"epacems-{year}-{state}.parquet",
                schema=schema,
                compression="snappy",
                version="2.6",
            ) as partitioned_writer:
                partitioned_writer.write_table(table)


@graph_asset
def hourly_emissions_epacems(
    epacamd_eia: pd.DataFrame, plants_entity_eia: pd.DataFrame
) -> None:
    """Extract, transform and load CSVs for EPA CEMS.

    This asset loads the partitions to a single parquet file in the
    function instead of using an IO Manager. Use the epacems_io_manager
    IO Manager to read this asset for downstream dependencies.

    Args:
        context: dagster keyword that provides access to resources and config.
        epacamd_eia: The EPA EIA crosswalk table used for harmonizing the
                     ORISPL code with EIA.
        plants_entity_eia: The EIA Plant entities used for aligning timezones.
    """
    years = get_year()
    return years.map(
        lambda year: process_single_year(
            year,
            epacamd_eia,
            plants_entity_eia,
        )
    )


@asset(
    config_schema={
        "pudl_output_path": Field(
            EnvVar(
                env_var="PUDL_OUTPUT",
            ),
            description="Path of directory to store the database in.",
            default_value=None,
        ),
    },
    io_manager_key="epacems_io_manager",
    required_resource_keys={"dataset_settings"},
)
def hourly_emissions_epacems_monolithic(
    context, hourly_emissions_epacems: dd.DataFrame
):
    """Read partitioned output and create monolithic version."""
    epacems_settings = context.resources.dataset_settings.epacems

    schema = Resource.from_id("hourly_emissions_epacems").to_pyarrow()
    monolithic_path = (
        Path(context.op_config["pudl_output_path"]) / "hourly_emissions_epacems.parquet"
    )

    with pq.ParquetWriter(
        where=monolithic_path, schema=schema, compression="snappy", version="2.6"
    ) as monolithic_writer:
        for year in epacems_settings.years:
            df = hourly_emissions_epacems[hourly_emissions_epacems == year].compute()
            table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)

            # Write to one monolithic parquet file
            monolithic_writer.write_table(table)
