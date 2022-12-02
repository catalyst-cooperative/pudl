"""Dagster assets of static data tables."""
from typing import Literal

import pandas as pd
from dagster import AssetOut, Output, multi_asset

import pudl
from pudl.metadata.classes import Package
from pudl.metadata.dfs import (
    FERC_ACCOUNTS,
    FERC_DEPRECIATION_LINES,
    POLITICAL_SUBDIVISIONS,
)

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


# TODO (bendnorman): Store the static table names in a better way.
# These tables are already in the metadata.
static_pudl_table_names = sorted(["political_subdivisions"])


@multi_asset(
    outs={
        table_name: AssetOut(io_manager_key="pudl_sqlite_io_manager")
        for table_name in static_pudl_table_names
    },
)
def static_pudl_tables():
    """Read static tables compiled as part of PUDL and not from any agency dataset."""
    static_pudl_tables_dict = {"political_subdivisions": POLITICAL_SUBDIVISIONS}
    return (
        Output(output_name=table_name, value=df)
        for table_name, df in static_pudl_tables_dict.items()
    )


@multi_asset(
    outs={
        table_name: AssetOut(io_manager_key="pudl_sqlite_io_manager")
        for table_name in _read_static_encoding_tables("static_eia")
    },
)
def static_eia_tables():
    """Create static EIA tables."""
    return (
        Output(output_name=table_name, value=df)
        for table_name, df in _read_static_encoding_tables("static_eia").items()
    )


static_ferc1_table_names = sorted(
    list(_read_static_encoding_tables("static_ferc1"))
    + ["ferc_accounts", "ferc_depreciation_lines"]
)


@multi_asset(
    outs={
        table_name: AssetOut(io_manager_key="pudl_sqlite_io_manager")
        for table_name in static_ferc1_table_names
    },
)
def static_ferc1_tables():
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
    return (
        Output(output_name=table_name, value=df)
        for table_name, df in static_table_dict.items()
    )
