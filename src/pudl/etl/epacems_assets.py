"""EPA CEMS Hourley Emissions assets."""
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import Field, asset

import pudl
from pudl.helpers import EnvVar
from pudl.metadata.classes import Resource

logger = pudl.logging_helpers.get_logger(__name__)


@asset(
    required_resource_keys={"datastore", "dataset_settings"},
    config_schema={
        "pudl_output_path": Field(
            EnvVar(
                env_var="PUDL_OUTPUT",
            ),
            description="Path of directory to store the database in.",
            default_value=None,
        )
    },
)
def hourly_emissions_epacems(
    context, epacamd_eia: pd.DataFrame, plants_entity_eia: pd.DataFrame
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
    ds = context.resources.datastore
    epacems_settings = context.resources.dataset_settings.epacems

    timezones = pudl.transform.epacems.load_plant_utc_offset(plants_entity_eia)

    schema = Resource.from_id("hourly_emissions_epacems").to_pyarrow()
    epacems_path = (
        Path(context.op_config["pudl_output_path"]) / "hourly_emissions_epacems"
    )
    epacems_path.mkdir(exist_ok=True)

    for part in epacems_settings.partitions:
        year = part["year"]
        state = part["state"]
        logger.info(f"Processing EPA CEMS hourly data for {year}-{state}")
        df = pudl.extract.epacems.extract(year=year, state=state, ds=ds)
        df = pudl.transform.epacems.transform(df, epacamd_eia, timezones)
        with pq.ParquetWriter(
            where=epacems_path / f"epacems-{year}-{state}.parquet",
            schema=schema,
            compression="snappy",
            version="2.6",
        ) as pqwriter:
            pqwriter.write_table(
                pa.Table.from_pandas(df, schema=schema, preserve_index=False)
            )
