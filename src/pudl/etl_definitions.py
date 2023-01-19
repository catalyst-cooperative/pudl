"""Dagster repositories for PUDL."""
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import (
    Definitions,
    Field,
    asset,
    define_asset_job,
    load_assets_from_modules,
)

import pudl
from pudl.etl import create_glue_tables, fuel_receipts_costs_aggs_eia
from pudl.helpers import EnvVar
from pudl.io_managers import (
    epacems_io_manager,
    ferc1_dbf_sqlite_io_manager,
    ferc1_xbrl_sqlite_io_manager,
    pudl_sqlite_io_manager,
)
from pudl.metadata.classes import Resource
from pudl.settings import dataset_settings, ferc_to_sqlite_settings
from pudl.workspace.datastore import datastore

logger = pudl.logging_helpers.get_logger(__name__)


# TODO (bendnorman): Find a better place for the epacems asset
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
    group_name="epacems",
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


defs = Definitions(
    assets=[
        hourly_emissions_epacems,
        fuel_receipts_costs_aggs_eia,
        create_glue_tables,
        *load_assets_from_modules([pudl.extract.eia], group_name="eia_raw_assets"),
        *load_assets_from_modules(
            [pudl.transform.eia], group_name="eia_harvested_assets"
        ),
        *load_assets_from_modules([pudl.static_assets], group_name="static_assets"),
        *load_assets_from_modules(
            [pudl.output.output_assets], group_name="output_assets"
        ),
        *load_assets_from_modules([pudl.extract.ferc1], group_name="ferc_assets"),
        *load_assets_from_modules([pudl.transform.ferc1], group_name="ferc_assets"),
    ],
    resources={
        "datastore": datastore,
        "pudl_sqlite_io_manager": pudl_sqlite_io_manager,
        "ferc1_dbf_sqlite_io_manager": ferc1_dbf_sqlite_io_manager,
        "ferc1_xbrl_sqlite_io_manager": ferc1_xbrl_sqlite_io_manager,
        "dataset_settings": dataset_settings,
        "ferc_to_sqlite_settings": ferc_to_sqlite_settings,
        "epacems_io_manager": epacems_io_manager,
    },
    jobs=[
        define_asset_job(name="etl_full"),
        define_asset_job(
            name="etl_fast",
            config={
                "resources": {
                    "dataset_settings": {
                        "config": {
                            "eia": {
                                "eia860": {"years": [2020, 2021], "eia860m": True},
                                "eia923": {"years": [2020, 2021]},
                            },
                            "ferc1": {"years": [2020, 2021]},
                            "epacems": {
                                "states": ["ID", "ME"],
                                "years": [2019, 2020, 2021],
                            },
                        }
                    }
                }
            },
        ),
    ],
)
