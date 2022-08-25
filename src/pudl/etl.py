"""Run the PUDL ETL Pipeline.

The PUDL project integrates several different public datasets into a well
normalized relational database allowing easier access and interaction between all
datasets. This module coordinates the extract/transfrom/load process for
data from:

 - US Energy Information Agency (EIA):
   - Form 860 (eia860)
   - Form 923 (eia923)
 - US Federal Energy Regulatory Commission (FERC):
   - Form 1 (ferc1)
 - US Environmental Protection Agency (EPA):
   - Continuous Emissions Monitory System (epacems)

"""
import itertools
import logging
import time
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import sqlalchemy as sa
from dagster import Field, Nothing, job, op, resource
from sqlalchemy.pool import StaticPool

import pudl
from pudl.helpers import convert_cols_dtypes
from pudl.metadata.classes import Resource
from pudl.metadata.codes import CODE_METADATA
from pudl.metadata.dfs import FERC_ACCOUNTS, FERC_DEPRECIATION_LINES
from pudl.metadata.fields import apply_pudl_dtypes
from pudl.settings import (
    Eia860Settings,
    Eia923Settings,
    EiaSettings,
    EpaCemsSettings,
    EtlSettings,
    Ferc1Settings,
    GlueSettings,
)
from pudl.workspace.datastore import Datastore, datastore

logger = logging.getLogger(__name__)


###############################################################################
# EIA EXPORT FUNCTIONS
###############################################################################


def _read_static_tables_eia() -> dict[str, pd.DataFrame]:
    """Build dataframes of static EIA tables for use as foreign key constraints.

    There are many values specified within the data that are essentially
    constant, but which we need to store for data validation purposes, for use
    as foreign keys.  E.g. the list of valid EIA fuel type codes, or the
    possible state and country codes indicating a coal delivery's location of
    origin.

    """
    return {
        "energy_sources_eia": CODE_METADATA["energy_sources_eia"]["df"],
        "operational_status_eia": CODE_METADATA["operational_status_eia"]["df"],
        "fuel_types_aer_eia": CODE_METADATA["fuel_types_aer_eia"]["df"],
        "prime_movers_eia": CODE_METADATA["prime_movers_eia"]["df"],
        "sector_consolidated_eia": CODE_METADATA["sector_consolidated_eia"]["df"],
        "fuel_transportation_modes_eia": CODE_METADATA["fuel_transportation_modes_eia"][
            "df"
        ],
        "contract_types_eia": CODE_METADATA["contract_types_eia"]["df"],
        "coalmine_types_eia": CODE_METADATA["coalmine_types_eia"]["df"],
    }


@op(
    config_schema={
        "eia860_tables": Field(list, is_required=False),
        "eia860_years": Field(list, is_required=False),
        "eia860m": Field(bool, is_required=False),
        "eia923_tables": Field(list, is_required=False),
        "eia923_years": Field(list, is_required=False),
    },
    required_resource_keys={"datastore"},
)
def _etl_eia(context) -> dict[str, pd.DataFrame]:
    """Extract, transform and load CSVs for the EIA datasets.

    Args:
        context: dagster context keyword.

    Returns:
        A dictionary of EIA dataframes ready for loading into the PUDL DB.

    """
    eia860_settings_params = {}
    eia860_settings_params["tables"] = context.op_config.get("eia860_tables")
    eia860_settings_params["years"] = context.op_config.get("eia860_years")
    eia860_settings_params["eia860m"] = context.op_config.get("eia860m")
    eia860_settings_params = {k: v for k, v in eia860_settings_params.items() if v}
    logger.info(eia860_settings_params)

    eia923_settings_params = {}
    eia923_settings_params["tables"] = context.op_config.get("eia923_tables")
    eia923_settings_params["years"] = context.op_config.get("eia923_years")
    eia923_settings_params = {k: v for k, v in eia923_settings_params.items() if v}

    eia860_settings = Eia860Settings(**eia860_settings_params)
    eia923_settings = Eia923Settings(**eia923_settings_params)
    eia_settings = EiaSettings(eia860=eia860_settings, eia923=eia923_settings)

    if (not eia923_settings.tables or not eia923_settings.years) and (
        not eia923_settings.tables or not eia923_settings.years
    ):
        logger.info("Not loading EIA.")
        return []

    # generate dataframes for the static EIA tables
    out_dfs = _read_static_tables_eia()

    ds = context.resources.datastore
    # Extract EIA forms 923, 860
    eia923_raw_dfs = pudl.extract.eia923.Extractor(ds).extract(
        settings=eia_settings.eia923
    )
    eia860_raw_dfs = pudl.extract.eia860.Extractor(ds).extract(
        settings=eia_settings.eia860
    )
    # if we are trying to add the EIA 860M YTD data, then extract it and append
    if eia860_settings.eia860m:
        eia860m_raw_dfs = pudl.extract.eia860m.Extractor(ds).extract(
            settings=eia_settings.eia860
        )
        eia860_raw_dfs = pudl.extract.eia860m.append_eia860m(
            eia860_raw_dfs=eia860_raw_dfs, eia860m_raw_dfs=eia860m_raw_dfs
        )
    # Transform EIA forms 923, 860
    eia860_transformed_dfs = pudl.transform.eia860.transform(
        eia860_raw_dfs, eia860_settings=eia_settings.eia860
    )
    eia923_transformed_dfs = pudl.transform.eia923.transform(
        eia923_raw_dfs, eia923_settings=eia_settings.eia923
    )
    # create an eia transformed dfs dictionary
    eia_transformed_dfs = eia860_transformed_dfs.copy()
    eia_transformed_dfs.update(eia923_transformed_dfs.copy())

    # Do some final cleanup and assign appropriate types:
    eia_transformed_dfs = {
        name: convert_cols_dtypes(df, data_source="eia")
        for name, df in eia_transformed_dfs.items()
    }

    entities_dfs, eia_transformed_dfs = pudl.transform.eia.transform(
        eia_transformed_dfs,
        eia_settings=eia_settings,
    )
    # Assign appropriate types to new entity tables:
    entities_dfs = {
        name: apply_pudl_dtypes(df, group="eia") for name, df in entities_dfs.items()
    }

    for table in entities_dfs:
        entities_dfs[table] = (
            pudl.metadata.classes.Package.from_resource_ids()
            .get_resource(table)
            .encode(entities_dfs[table])
        )

    out_dfs.update(entities_dfs)
    out_dfs.update(eia_transformed_dfs)
    return out_dfs


###############################################################################
# FERC1 EXPORT FUNCTIONS
###############################################################################


def _read_static_tables_ferc1() -> dict[str, pd.DataFrame]:
    """Populate static PUDL tables with constants for use as foreign keys.

    There are many values specified within the data that are essentially
    constant, but which we need to store for data validation purposes, for use
    as foreign keys.  E.g. the list of valid EIA fuel type codes, or the
    possible state and country codes indicating a coal delivery's location of
    origin. For now these values are primarily stored in a large collection of
    lists, dictionaries, and dataframes which are specified in the
    pudl.metadata module.  This function uses those data structures to
    populate a bunch of small infrastructural tables within the PUDL DB.
    """
    return {
        "ferc_accounts": FERC_ACCOUNTS[["ferc_account_id", "ferc_account_description"]],
        "ferc_depreciation_lines": FERC_DEPRECIATION_LINES[
            ["line_id", "ferc_account_description"]
        ],
        "power_purchase_types_ferc1": CODE_METADATA["power_purchase_types_ferc1"]["df"],
    }


# TODO: update config to use dagster Field
@op(
    config_schema={
        "years": Field(list, is_required=False),
        "tables": Field(list, is_required=False),
    },
    required_resource_keys={"pudl_settings"},
)
def _etl_ferc1(context) -> dict[str, pd.DataFrame]:
    """Extract, transform and load CSVs for FERC Form 1.

    Returns:
        Dataframes containing PUDL database tables pertaining to the FERC Form 1
        data, keyed by table name.

    """
    # Validate settings
    # We need to filter setting parameters that aren't specified to let
    # the pydantic setting models handle the default values.
    settings_params = {}
    settings_params["years"] = context.op_config.get("years")
    settings_params["tables"] = context.op_config.get("tables")
    settings_kwargs = {k: v for k, v in settings_params.items() if v}
    ferc1_settings = Ferc1Settings(**settings_kwargs)

    # Compile static FERC 1 dataframes
    out_dfs = _read_static_tables_ferc1()

    # Extract FERC form 1
    ferc1_raw_dfs = pudl.extract.ferc1.extract(
        ferc1_settings=ferc1_settings, pudl_settings=context.resources.pudl_settings
    )
    # Transform FERC form 1
    ferc1_transformed_dfs = pudl.transform.ferc1.transform(
        ferc1_raw_dfs, ferc1_settings=ferc1_settings
    )

    out_dfs.update(ferc1_transformed_dfs)
    return out_dfs


###############################################################################
# EPA CEMS EXPORT FUNCTIONS
###############################################################################
def _etl_one_year_epacems(
    year: int,
    states: list[str],
    pudl_db: str,
    out_dir: str,
    ds: Datastore,
) -> None:
    """Process one year of EPA CEMS and output year-state paritioned Parquet files."""
    pudl_engine = sa.create_engine(pudl_db)
    schema = Resource.from_id("hourly_emissions_epacems").to_pyarrow()

    for state in states:
        with pq.ParquetWriter(
            where=Path(out_dir) / f"epacems-{year}-{state}.parquet",
            schema=schema,
            compression="snappy",
            version="2.6",
        ) as pqwriter:
            logger.info(f"Processing EPA CEMS hourly data for {year}-{state}")
            df = pudl.extract.epacems.extract(year=year, state=state, ds=ds)
            df = pudl.transform.epacems.transform(df, pudl_engine=pudl_engine)
            pqwriter.write_table(
                pa.Table.from_pandas(df, schema=schema, preserve_index=False)
            )


@op(
    config_schema={
        "years": Field(list, is_required=False),
        "states": Field(list, is_required=False),
        "clobber": Field(bool, default_value=False),
        "partition": Field(bool, default_value=False),
    },
    required_resource_keys={"pudl_settings", "datastore", "pudl_engine"},
)
def etl_epacems(context) -> Nothing:
    """Extract, transform and load CSVs for EPA CEMS.

    Args:
        context: dagster context keyword

    Returns:
        Unlike the other ETL functions, the EPACEMS writes its output to Parquet as it
        goes, since the dataset is too large to hold in memory.  So it doesn't return a
        dictionary of dataframes.

    """
    pudl_engine = context.resources.pudl_engine
    pudl_settings = context.resources.pudl_settings
    ds = context.resources.datastore
    clobber = context.op_config["clobber"]

    settings_params = {}
    settings_params["states"] = context.op_config.get("states")
    settings_params["years"] = context.op_config.get("years")
    settings_params["partition"] = context.op_config.get("partition")
    settings_kwargs = {k: v for k, v in settings_params.items() if v}
    epacems_settings = EpaCemsSettings(**settings_kwargs)

    # Verify that we have a PUDL DB with plant attributes:
    inspector = sa.inspect(pudl_engine)
    if "plants_eia860" not in inspector.get_table_names():
        raise RuntimeError(
            "No plants_eia860 available in the PUDL DB! Have you run the ETL? "
            f"Trying to access PUDL DB: {pudl_engine}"
        )

    eia_plant_years = pd.read_sql(
        """
        SELECT DISTINCT strftime('%Y', report_date)
        AS year
        FROM plants_eia860
        ORDER BY year ASC
        """,
        pudl_engine,
    ).year.astype(int)
    missing_years = list(set(epacems_settings.years) - set(eia_plant_years))
    if missing_years:
        logger.info(
            f"EPA CEMS years with no EIA plant data: {missing_years} "
            "Some timezones may be estimated based on plant state."
        )

    logger.info("Processing EPA CEMS data and writing it to Apache Parquet.")
    if logger.isEnabledFor(logging.INFO):
        start_time = time.monotonic()

    if epacems_settings.partition:
        epacems_dir = (
            Path(pudl_settings["parquet_dir"]) / "epacems" / "hourly_emissions_epacems"
        )
        _ = pudl.helpers.prep_dir(epacems_dir, clobber=clobber)
        do_one_year = partial(
            _etl_one_year_epacems,
            states=epacems_settings.states,
            pudl_db=pudl_settings["pudl_db"],
            out_dir=epacems_dir,
            ds=ds,
        )
        with ProcessPoolExecutor() as executor:
            # Convert results of map() to list to force execution
            _ = list(executor.map(do_one_year, epacems_settings.years))

    else:
        schema = Resource.from_id("hourly_emissions_epacems").to_pyarrow()
        epacems_path = Path(
            pudl_settings["parquet_dir"], "epacems/hourly_emissions_epacems.parquet"
        )
        if epacems_path.exists() and not clobber:
            raise SystemExit(
                "The EPA CEMS parquet file already exists, and we don't want to clobber it.\n"
                f"Move {epacems_path} aside or set clobber=True and try again."
            )

        with pq.ParquetWriter(
            where=str(epacems_path),
            schema=schema,
            compression="snappy",
            version="2.6",
        ) as pqwriter:
            for year, state in itertools.product(
                epacems_settings.years, epacems_settings.states
            ):
                logger.info(f"Processing EPA CEMS hourly data for {year}-{state}")
                df = pudl.extract.epacems.extract(year=year, state=state, ds=ds)
                df = pudl.transform.epacems.transform(df, pudl_engine=pudl_engine)
                pqwriter.write_table(
                    pa.Table.from_pandas(df, schema=schema, preserve_index=False)
                )

    if logger.isEnabledFor(logging.INFO):
        delta_t = time.strftime("%H:%M:%S", time.gmtime(time.monotonic() - start_time))
        time_message = f"Processing EPA CEMS took {delta_t}"
        logger.info(time_message)
        start_time = time.monotonic()


###############################################################################
# GLUE EXPORT FUNCTIONS
###############################################################################
@op(
    config_schema={
        "ferc1": Field(bool, is_required=False),
        "eia": Field(bool, is_required=False),
    },
    required_resource_keys={"pudl_settings"},
)
def _etl_glue(context) -> dict[str, pd.DataFrame]:
    """Extract, transform and load CSVs for the Glue tables.

    Args:
        glue_settings: Validated ETL parameters required by this data source.

    Returns:
        A dictionary of DataFrames whose keys are the names of the corresponding
        database table.

    """
    settings_params = {}
    settings_params["ferc1"] = context.op_config.get("ferc1")
    settings_params["eia"] = context.op_config.get("eia")
    settings_kwargs = {k: v for k, v in settings_params.items() if v}
    glue_settings = GlueSettings(**settings_kwargs)

    # grab the glue tables for ferc1 & eia
    glue_dfs = pudl.glue.ferc1_eia.glue(
        ferc1=glue_settings.ferc1,
        eia=glue_settings.eia,
    )

    # Add the EPA to EIA crosswalk, but only if the eia data is being processed.
    # Otherwise the foreign key references will have nothing to point at:
    if glue_settings.eia:
        glue_dfs.update(pudl.glue.eia_epacems.grab_clean_split())

    return glue_dfs


###############################################################################
# Coordinating functions
###############################################################################


def etl(  # noqa: C901
    etl_settings: EtlSettings,
    pudl_settings: dict,
    clobber: bool = False,
    use_local_cache: bool = True,
    gcs_cache_path: str = None,
    check_foreign_keys: bool = True,
    check_types: bool = True,
    check_values: bool = True,
):
    """Run the PUDL Extract, Transform, and Load data pipeline.

    First we validate the settings, and then process data destined for loading
    into SQLite, which includes The FERC Form 1 and the EIA Forms 860 and 923.
    Once those data have been output to SQLite we mvoe on to processing the
    long tables, which will be loaded into Apache Parquet files. Some of this
    processing depends on data that's already been loaded into the SQLite DB.

    Args:
        etl_settings: settings that describe datasets to be loaded.
        pudl_settings: a dictionary filled with settings that mostly
            describe paths to various resources and outputs.
        clobber: If True and there is already a pudl.sqlite database
            it will be deleted and a new one will be created.
        use_local_cache: controls whether datastore should be using local
            file cache.
        gcs_cache_path: controls whether datastore should be using Google
            Cloud Storage based cache.

    Returns:
        None

    """
    pudl_db_path = Path(pudl_settings["sqlite_dir"]) / "pudl.sqlite"
    if pudl_db_path.exists() and not clobber:
        raise SystemExit(
            "The PUDL DB already exists, and we don't want to clobber it.\n"
            f"Move {pudl_db_path} aside or set clobber=True and try again."
        )

    # Configure how we want to obtain raw input data:
    ds_kwargs = dict(
        gcs_cache_path=gcs_cache_path, sandbox=pudl_settings.get("sandbox", False)
    )
    if use_local_cache:
        ds_kwargs["local_cache_path"] = Path(pudl_settings["pudl_in"]) / "data"

    validated_etl_settings = etl_settings.datasets

    # Check for existing EPA CEMS outputs if we're going to process CEMS, and
    # do it before running the SQLite part of the ETL so we don't do a bunch of
    # work only to discover that we can't finish.
    datasets = validated_etl_settings.get_datasets()
    if "epacems" in datasets.keys():
        epacems_pq_path = Path(pudl_settings["parquet_dir"]) / "epacems"
        epacems_pq_path.mkdir(exist_ok=True)

    sqlite_dfs = {}
    # This could be cleaner if we simplified the settings file format:
    if datasets.get("ferc1", False):
        sqlite_dfs.update(_etl_ferc1(datasets["ferc1"], pudl_settings))
    if datasets.get("eia", False):
        sqlite_dfs.update(_etl_eia(datasets["eia"], ds_kwargs))
    if datasets.get("glue", False):
        sqlite_dfs.update(_etl_glue(datasets["glue"]))

    # Load the ferc1 + eia data directly into the SQLite DB:
    pudl_engine = sa.create_engine(pudl_settings["pudl_db"])
    pudl.load.dfs_to_sqlite(
        sqlite_dfs,
        engine=pudl_engine,
        check_foreign_keys=check_foreign_keys,
        check_types=check_types,
        check_values=check_values,
    )

    # Parquet Outputs:
    if datasets.get("epacems", False):
        etl_epacems(datasets["epacems"], pudl_settings, ds_kwargs, clobber=clobber)


# DAGSTER WIP
@resource
def pudl_settings(init_context):
    """Create a pudl engine Resource."""
    # TODO: figure out how to config the pudl workspace using dagster instead of just pulling the defaults.
    return pudl.workspace.setup.get_defaults()


@resource(required_resource_keys={"pudl_settings"})
def pudl_engine(init_context):
    """Create a pudl engine Resource."""
    return sa.create_engine(
        init_context.resources.pudl_settings["pudl_db"],
        poolclass=StaticPool,
    )


@job(
    resource_defs={
        "pudl_settings": pudl_settings,
        "datastore": datastore,
        "pudl_engine": pudl_engine,
    }
)
def pudl_etl():
    """Run pudl_etl job."""
    ferc1_dfs = _etl_ferc1()
    eia_dfs = _etl_eia()
    glue_dfs = _etl_glue()
    pudl.load.dfs_to_sqlite(ferc1_dfs=ferc1_dfs, eia_dfs=eia_dfs, glue_dfs=glue_dfs)


@job(
    resource_defs={
        "pudl_settings": pudl_settings,
        "datastore": datastore,
        "pudl_engine": pudl_engine,
    }
)
def etl_epacems_job():
    """Run etl_epacems_job."""
    etl_epacems()


if __name__ == "__main__":
    pudl_etl.execute_in_process()
