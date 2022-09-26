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
from typing import Any, Literal

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import sqlalchemy as sa

import pudl
from pudl.helpers import convert_cols_dtypes, get_logger
from pudl.metadata.classes import Package, Resource
from pudl.metadata.dfs import FERC_ACCOUNTS, FERC_DEPRECIATION_LINES
from pudl.metadata.fields import apply_pudl_dtypes
from pudl.settings import (
    EiaSettings,
    EpaCemsSettings,
    EtlSettings,
    Ferc1Settings,
    GlueSettings,
)
from pudl.workspace.datastore import Datastore

logger = get_logger(__name__)


###############################################################################
# EIA EXPORT FUNCTIONS
###############################################################################


def _etl_eia(
    eia_settings: EiaSettings, ds_kwargs: dict[str, Any]
) -> dict[str, pd.DataFrame]:
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

    if (not eia923_tables or not eia923_years) and (
        not eia860_tables or not eia860_years
    ):
        logger.info("Not loading EIA.")
        return []

    # generate dataframes for the static EIA tables
    out_dfs = _read_static_encoding_tables(etl_group="static_eia")

    ds = Datastore(**ds_kwargs)
    # Extract EIA forms 923, 860
    eia923_raw_dfs = pudl.extract.eia923.Extractor(ds).extract(
        settings=eia_settings.eia923
    )
    eia860_raw_dfs = pudl.extract.eia860.Extractor(ds).extract(
        settings=eia_settings.eia860
    )
    # if we are trying to add the EIA 860M YTD data, then extract it and append
    if eia860m:
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
            Package.from_resource_ids().get_resource(table).encode(entities_dfs[table])
        )

    out_dfs.update(entities_dfs)
    out_dfs.update(eia_transformed_dfs)
    return out_dfs


###############################################################################
# FERC1 EXPORT FUNCTIONS
###############################################################################


def _read_static_tables_ferc1():
    """Compile static tables for FERC1 for foriegn key constaints.

    This function grabs static encoded tables via :func:`_read_static_encoding_tables`
    as well as two static tables that are non-encoded tables (``ferc_accounts`` and
    ``ferc_depreciation_lines``).
    """
    static_table_dict = _read_static_encoding_tables("static_ferc1")
    static_table_dict.update(
        {
            "ferc_accounts": FERC_ACCOUNTS[
                ["ferc_account_id", "ferc_account_description"]
            ],
            "ferc_depreciation_lines": FERC_DEPRECIATION_LINES[
                ["line_id", "ferc_account_description"]
            ],
        }
    )
    return static_table_dict


def _etl_ferc1(
    ferc1_settings: Ferc1Settings,
    pudl_settings: dict[str, Any],
) -> dict[str, pd.DataFrame]:
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
    ferc1_dbf_raw_dfs = pudl.extract.ferc1.extract_dbf(
        ferc1_settings=ferc1_settings.ferc1_dbf_settings, pudl_settings=pudl_settings
    )
    # Transform FERC form 1
    ferc1_transformed_dfs = pudl.transform.ferc1.transform_dbf(
        ferc1_dbf_raw_dfs, ferc1_settings=ferc1_settings.ferc1_dbf_settings
    )

    # Extract FERC form 1 XBRL data
    ferc1_xbrl_raw_dfs = pudl.extract.ferc1.extract_xbrl(
        ferc1_settings=ferc1_settings.ferc1_xbrl_settings, pudl_settings=pudl_settings
    )
    # Transform FERC form 1 XBRL data
    _ = pudl.transform.ferc1.transform_xbrl(
        ferc1_xbrl_raw_dfs, ferc1_settings=ferc1_settings.ferc1_xbrl_settings
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
    ds_kwargs: dict[str, Any],
) -> None:
    """Process one year of EPA CEMS and output year-state paritioned Parquet files."""
    pudl_engine = sa.create_engine(pudl_db)
    ds = Datastore(**ds_kwargs)
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


def etl_epacems(
    epacems_settings: EpaCemsSettings,
    pudl_settings: dict[str, Any],
    ds_kwargs: dict[str, Any],
    clobber: str = False,
) -> None:
    """Extract, transform and load CSVs for EPA CEMS.

    Args:
        epacems_settings: Validated ETL parameters required by this data source.
        pudl_settings: a dictionary filled with settings that mostly describe paths to
            various resources and outputs.
        ds_kwargs: Keyword arguments for instantiating a PUDL datastore, so that the ETL
            can access the raw input data.
        clobber: If True and there is already a hourly_emissions_epacems parquer file
            or directory it will be deleted and a new one will be created.

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
    # Verify that we have a PUDL DB with crosswalk data
    if "epacamd_eia" not in inspector.get_table_names():
        raise RuntimeError(
            "No EPACAMD-EIA Crosswalk available in the PUDL DB! Have you run the ETL? "
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
            ds_kwargs=ds_kwargs,
        )
        with ProcessPoolExecutor() as executor:
            # Convert results of map() to list to force execution
            _ = list(executor.map(do_one_year, epacems_settings.years))

    else:
        ds = Datastore(**ds_kwargs)
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
# GLUE AND STATIC EXPORT FUNCTIONS
###############################################################################
def _etl_glue(
    glue_settings: GlueSettings,
    ds_kwargs: dict[str, Any],
    sqlite_dfs: dict[str, pd.DataFrame],
    eia_settings: EiaSettings,
) -> dict[str, pd.DataFrame]:
    """Extract, transform and load CSVs for the Glue tables.

    Args:
        glue_settings: Validated ETL parameters required by this data source.
        ds_kwargs: Keyword arguments for instantiating a PUDL datastore, so that the ETL
            can access the raw input data.
        sqlite_dfs: The dictionary of dataframes to be loaded into the pudl database.
            We pass the dictionary though because the EPACAMD-EIA crosswalk needs to
            know which EIA plants and generators are being loaded into the database
            (based on whether we run the full or fast etl). The tests will break if we
            pass the generators_entity_eia table as an argument because of the
            ferc1_solo test (where no eia tables are in the sqlite_dfs dict). Passing
            the whole dict avoids this because the crosswalk will only load if there
            are eia tables in the dict, but the dict will always be there.
        eia_settings: Validated ETL parameters required by this data source.

    Returns:
        A dictionary of DataFrames whose keys are the names of the corresponding
        database table.
    """
    # grab the glue tables for ferc1 & eia
    glue_dfs = pudl.glue.ferc1_eia.glue(
        ferc1=glue_settings.ferc1,
        eia=glue_settings.eia,
    )

    # Add the EPA to EIA crosswalk, but only if the eia data is being processed.
    # Otherwise the foreign key references will have nothing to point at:
    ds = Datastore(**ds_kwargs)
    if glue_settings.eia:
        # Check to see whether the settings file indicates the processing of all
        # available EIA years.
        processing_all_eia_years = (
            eia_settings.eia860.years
            == eia_settings.eia860.data_source.working_partitions["years"]
        )
        glue_raw_dfs = pudl.glue.epacamd_eia.extract(ds)
        glue_transformed_dfs = pudl.glue.epacamd_eia.transform(
            glue_raw_dfs,
            sqlite_dfs["generators_entity_eia"],
            sqlite_dfs["boilers_entity_eia"],
            processing_all_eia_years,
        )
        glue_dfs.update(glue_transformed_dfs)

    return glue_dfs


def _read_static_encoding_tables(
    etl_group: Literal["static_eia", "static_ferc1"]
) -> dict[str, pd.DataFrame]:
    """Build dataframes of static tables from a data source for use as foreign keys.

    There are many values specified within the data that are essentially constant, but
    which we need to store for data validation purposes, for use as foreign keys.  E.g.
    the list of valid EIA fuel type codes, or the possible state and country codes
    indicating a coal delivery's location of origin. For now these values are primarily
    stored in a large collection of lists, dictionaries, and dataframes which are
    specified in the :mod:`pudl.metadata` subpackage.  This function uses those data
    structures to populate a bunch of small infrastructural tables within the PUDL DB.

    Args:
        etl_group: name of static table etl group.

    Returns:
        a dictionary with table names as keys and dataframes as values for all tables
        labeled as static tables in their resource ``etl_group``
    """
    return {
        r.name: r.encoder.df
        for r in Package.from_resource_ids().resources
        if r.etl_group == etl_group and r.encoder
    }


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
        sqlite_dfs.update(
            _etl_glue(datasets["glue"], ds_kwargs, sqlite_dfs, datasets["eia"])
        )

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
