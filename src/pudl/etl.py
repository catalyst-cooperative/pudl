"""
Run the PUDL ETL Pipeline.

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
import logging
import time
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import sqlalchemy as sa

import pudl
from pudl.helpers import convert_cols_dtypes
from pudl.metadata.codes import CODE_METADATA
from pudl.metadata.dfs import FERC_ACCOUNTS, FERC_DEPRECIATION_LINES
from pudl.metadata.fields import apply_pudl_dtypes
from pudl.settings import (EiaSettings, EpaCemsSettings, EtlSettings,
                           Ferc1Settings, GlueSettings)
from pudl.workspace.datastore import Datastore

logger = logging.getLogger(__name__)


###############################################################################
# EIA EXPORT FUNCTIONS
###############################################################################

def _read_static_tables_eia() -> Dict[str, pd.DataFrame]:
    """Build dataframes of static EIA tables for use as foreign key constraints.

    There are many values specified within the data that are essentially
    constant, but which we need to store for data validation purposes, for use
    as foreign keys.  E.g. the list of valid EIA fuel type codes, or the
    possible state and country codes indicating a coal delivery's location of
    origin.

    """
    return {
        'energy_sources_eia': CODE_METADATA["energy_sources_eia"]["df"],
        'fuel_types_aer_eia': CODE_METADATA["fuel_types_aer_eia"]["df"],
        'prime_movers_eia': CODE_METADATA["prime_movers_eia"]["df"],
        'sector_consolidated_eia': CODE_METADATA["sector_consolidated_eia"]["df"],
        'fuel_transportation_modes_eia': CODE_METADATA["fuel_transportation_modes_eia"]["df"],
        'contract_types_eia': CODE_METADATA["contract_types_eia"]["df"],
        'coalmine_types_eia': CODE_METADATA["coalmine_types_eia"]["df"],
    }


def _etl_eia(
    eia_settings: EiaSettings,
    ds_kwargs: Dict[str, Any]
) -> Dict[str, pd.DataFrame]:
    """Extract, transform and load CSVs for the EIA datasets.

    Args:
        eia_settings: Validated ETL parameters required by this data source.
        ds_kwargs: Keyword arguments for instantiating a PUDL datastore,
            so that the ETL can access the raw input data.

    Returns:
        A dictionary of EIA dataframes ready for loading into the PUDL DB.

    """
    eia860_tables = eia_settings.eia860.tables
    eia860_years = eia_settings.eia860.years
    eia860m = eia_settings.eia860.eia860m
    eia923_tables = eia_settings.eia923.tables
    eia923_years = eia_settings.eia923.years

    if (
        (not eia923_tables or not eia923_years)
        and (not eia860_tables or not eia860_years)
    ):
        logger.info('Not loading EIA.')
        return []

    # generate dataframes for the static EIA tables
    out_dfs = _read_static_tables_eia()

    ds = Datastore(**ds_kwargs)
    # Extract EIA forms 923, 860
    eia923_raw_dfs = pudl.extract.eia923.Extractor(ds).extract(
        settings=eia_settings.eia923)
    eia860_raw_dfs = pudl.extract.eia860.Extractor(ds).extract(
        settings=eia_settings.eia860)
    # if we are trying to add the EIA 860M YTD data, then extract it and append
    if eia860m:
        eia860m_raw_dfs = pudl.extract.eia860m.Extractor(ds).extract(
            settings=eia_settings.eia860)
        eia860_raw_dfs = pudl.extract.eia860m.append_eia860m(
            eia860_raw_dfs=eia860_raw_dfs, eia860m_raw_dfs=eia860m_raw_dfs)

    # Transform EIA forms 923, 860
    eia860_transformed_dfs = pudl.transform.eia860.transform(
        eia860_raw_dfs, eia860_settings=eia_settings.eia860)
    eia923_transformed_dfs = pudl.transform.eia923.transform(
        eia923_raw_dfs, eia923_settings=eia_settings.eia923)
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
        name: apply_pudl_dtypes(df, group="eia")
        for name, df in entities_dfs.items()
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


def _read_static_tables_ferc1() -> Dict[str, pd.DataFrame]:
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
        'ferc_accounts': FERC_ACCOUNTS[[
            "ferc_account_id",
            "ferc_account_description",
        ]],
        'ferc_depreciation_lines': FERC_DEPRECIATION_LINES[[
            "line_id",
            "ferc_account_description",
        ]],
        'power_purchase_types_ferc1': CODE_METADATA["power_purchase_types_ferc1"]["df"],
    }


def _etl_ferc1(
    ferc1_settings: Ferc1Settings,
    pudl_settings: Dict[str, Any],
) -> Dict[str, pd.DataFrame]:
    """Extract, transform and load CSVs for FERC Form 1.

    Args:
        ferc1_settings: Validated ETL parameters required by this data source.
        pudl_settings: a dictionary filled with settings that mostly
            describe paths to various resources and outputs.

    Returns:
        Dataframes containing PUDL database tables pertaining to the FERC Form 1
        data, keyed by table name.

    """
    # Compile static FERC 1 dataframes
    out_dfs = _read_static_tables_ferc1()

    # Extract FERC form 1
    ferc1_raw_dfs = pudl.extract.ferc1.extract(
        ferc1_settings=ferc1_settings,
        pudl_settings=pudl_settings)
    # Transform FERC form 1
    ferc1_transformed_dfs = pudl.transform.ferc1.transform(
        ferc1_raw_dfs, ferc1_settings=ferc1_settings)

    out_dfs.update(ferc1_transformed_dfs)
    return out_dfs


###############################################################################
# EPA CEMS EXPORT FUNCTIONS
###############################################################################


def etl_epacems(
    epacems_settings: EpaCemsSettings,
    pudl_settings: Dict[str, Any],
    ds_kwargs: Dict[str, Any],
) -> None:
    """Extract, transform and load CSVs for EPA CEMS.

    Args:
        epacems_settings: Validated ETL parameters required by this data source.
        pudl_settings: a dictionary filled with settings that mostly describe paths to
            various resources and outputs.
        ds_kwargs: Keyword arguments for instantiating a PUDL datastore, so that the ETL
            can access the raw input data.

    Returns:
        Unlike the other ETL functions, the EPACEMS writes its output to Parquet as it
        goes, since the dataset is too large to hold in memory.  So it doesn't return a
        dictionary of dataframes.

    """
    pudl_engine = sa.create_engine(pudl_settings["pudl_db"])

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
        """, pudl_engine).year.astype(int)
    missing_years = list(set(epacems_settings.years) - set(eia_plant_years))
    if missing_years:
        logger.info(
            f"EPA CEMS years with no EIA plant data: {missing_years} "
            "Some timezones may be estimated based on plant state."
        )

    # NOTE: This is a generator for raw dataframes
    epacems_raw_dfs = pudl.extract.epacems.extract(
        epacems_settings, Datastore(**ds_kwargs))

    # NOTE: This is a generator for transformed dataframes
    epacems_transformed_dfs = pudl.transform.epacems.transform(
        epacems_raw_dfs=epacems_raw_dfs,
        pudl_engine=pudl_engine,
    )

    logger.info("Processing EPA CEMS data and writing it to Apache Parquet.")
    if logger.isEnabledFor(logging.INFO):
        start_time = time.monotonic()

    # run the cems generator dfs through the load step
    for df in epacems_transformed_dfs:
        pudl.load.df_to_parquet(
            df,
            resource_id="hourly_emissions_epacems",
            root_path=Path(pudl_settings["parquet_dir"]) / "epacems",
            partition_cols=["year", "state"]
        )

    if logger.isEnabledFor(logging.INFO):
        delta_t = time.strftime("%H:%M:%S", time.gmtime(
            time.monotonic() - start_time))
        time_message = f"Processing EPA CEMS took {delta_t}"
        logger.info(time_message)
        start_time = time.monotonic()


###############################################################################
# GLUE EXPORT FUNCTIONS
###############################################################################

def _etl_glue(glue_settings: GlueSettings) -> Dict[str, pd.DataFrame]:
    """Extract, transform and load CSVs for the Glue tables.

    Args:
        glue_settings (GlueSettings): Validated ETL parameters required by this data source.

    Returns:
        dict: A dictionary of :class:`pandas.Dataframe` whose keys are the names
        of the corresponding database table.

    """
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
    pudl_settings: Dict,
    clobber: bool = False,
    use_local_cache: bool = True,
    gcs_cache_path: str = None,
    check_foreign_keys: bool = True,
    check_types: bool = True,
    check_values: bool = True,
):
    """
    Run the PUDL Extract, Transform, and Load data pipeline.

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
        gcs_cache_path=gcs_cache_path,
        sandbox=pudl_settings.get("sandbox", False)
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
        _ = pudl.helpers.prep_dir(epacems_pq_path, clobber=clobber)

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
        etl_epacems(datasets["epacems"], pudl_settings, ds_kwargs)
