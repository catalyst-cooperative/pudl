"""Dagster assets of static data tables."""
from typing import Literal

import pandas as pd
from dagster import AssetOut, Output, multi_asset

import pudl
from pudl.metadata.classes import Package
from pudl.metadata.dfs import FERC_ACCOUNTS, POLITICAL_SUBDIVISIONS

logger = pudl.logging_helpers.get_logger(__name__)


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


@multi_asset(
    outs={
        table_name: AssetOut(io_manager_key="pudl_sqlite_io_manager")
        for table_name in Package.get_etl_group_tables("static_pudl")
    },
    required_resource_keys={"dataset_settings", "datastore"},
)
def static_pudl_tables(context):
    """Read static tables compiled as part of PUDL and not from any agency dataset."""
    ds = context.resources.datastore
    dataset_settings = context.resources.dataset_settings

    static_pudl_tables_dict = {"political_subdivisions": POLITICAL_SUBDIVISIONS}
    static_pudl_tables_dict["datasources"] = dataset_settings.make_datasources_table(ds)
    return (
        Output(output_name=table_name, value=df)
        for table_name, df in static_pudl_tables_dict.items()
    )


@multi_asset(
    outs={
        table_name: AssetOut(io_manager_key="pudl_sqlite_io_manager")
        for table_name in Package.get_etl_group_tables("static_eia")
    },
)
def static_eia_tables():
    """Create static EIA tables."""
    return (
        Output(output_name=table_name, value=df)
        for table_name, df in _read_static_encoding_tables("static_eia").items()
    )


@multi_asset(
    outs={
        table_name: AssetOut(io_manager_key="pudl_sqlite_io_manager")
        for table_name in Package.get_etl_group_tables("static_ferc1")
    },
)
def static_ferc1_tables():
    """Compile static tables for FERC1 for foriegn key constaints.

    This function grabs static encoded tables via :func:`_read_static_encoding_tables`
    as well as two static tables that are non-encoded tables (``ferc_accounts``).
    """
    static_table_dict = _read_static_encoding_tables("static_ferc1")
    static_table_dict.update(
        {
            "ferc_accounts": FERC_ACCOUNTS[
                ["ferc_account_id", "ferc_account_description"]
            ],
        }
    )
    return (
        Output(output_name=table_name, value=df)
        for table_name, df in static_table_dict.items()
    )
