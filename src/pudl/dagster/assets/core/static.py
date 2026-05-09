"""Dagster assets for static reference tables.

This module defines assets that materialize small, stable lookup tables and other
reference data that PUDL ships as part of the pipeline itself. Put asset definitions
here when the data comes from packaged metadata or code-maintained constants rather
than an external extract step, and keep source-specific extract logic elsewhere.
"""

from typing import Literal

import dagster as dg
import pandas as pd

import pudl
from pudl.metadata.classes import Package
from pudl.metadata.dfs import (
    FERC_ACCOUNTS,
    IMPUTATION_REASON_CODES,
    POLITICAL_SUBDIVISIONS,
)

logger = pudl.logging_helpers.get_logger(__name__)


def _read_static_encoding_tables(
    etl_group: Literal["static_eia", "static_ferc1", "static_rus"],
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


@dg.multi_asset(
    outs={
        table_name: dg.AssetOut(io_manager_key="pudl_io_manager", is_required=False)
        for table_name in Package.get_etl_group_tables("static_pudl")
    },
    can_subset=True,
    required_resource_keys={"etl_settings", "datastore"},
)
def static_pudl_tables(context):
    """Read static tables compiled as part of PUDL and not from any agency dataset."""
    ds = context.resources.datastore
    dataset_settings = context.resources.etl_settings.dataset_settings

    static_pudl_tables_dict = {
        "core_pudl__codes_subdivisions": POLITICAL_SUBDIVISIONS,
        "core_pudl__codes_imputation_reasons": IMPUTATION_REASON_CODES,
    }
    static_pudl_tables_dict["core_pudl__codes_datasources"] = (
        dataset_settings.make_datasources_table(ds)
    )

    selected_outputs = set(context.selected_output_names)
    return (
        dg.Output(output_name=table_name, value=df)
        for table_name, df in static_pudl_tables_dict.items()
        if table_name in selected_outputs
    )


@dg.multi_asset(
    outs={
        table_name: dg.AssetOut(io_manager_key="pudl_io_manager", is_required=False)
        for table_name in Package.get_etl_group_tables("static_eia")
    },
    can_subset=True,
)
def static_eia_tables(context):
    """Create static EIA tables."""
    static_tables = _read_static_encoding_tables("static_eia")
    static_tables.update(
        {
            "core_eia__codes_balancing_authority_subregions": pudl.metadata.dfs.BALANCING_AUTHORITY_SUBREGIONS_EIA
        }
    )

    selected_outputs = set(context.selected_output_names)
    return (
        dg.Output(output_name=table_name, value=df)
        for table_name, df in static_tables.items()
        if table_name in selected_outputs
    )


@dg.multi_asset(
    outs={
        table_name: dg.AssetOut(io_manager_key="pudl_io_manager", is_required=False)
        for table_name in Package.get_etl_group_tables("static_ferc1")
    },
    can_subset=True,
)
def static_ferc1_tables(context):
    """Compile static tables for FERC1 for foreign key constraints.

    This function grabs static encoded tables via :func:`_read_static_encoding_tables`
    as well as two static tables that are non-encoded tables (``ferc_accounts``).
    """
    static_table_dict = _read_static_encoding_tables("static_ferc1")
    static_table_dict.update(
        {
            "core_ferc__codes_accounts": FERC_ACCOUNTS[
                ["ferc_account_id", "ferc_account_description"]
            ],
        }
    )

    selected_outputs = set(context.selected_output_names)
    return (
        dg.Output(output_name=table_name, value=df)
        for table_name, df in static_table_dict.items()
        if table_name in selected_outputs
    )


@dg.multi_asset(
    outs={
        table_name: dg.AssetOut(io_manager_key="pudl_io_manager", is_required=False)
        for table_name in Package.get_etl_group_tables("static_rus")
    },
    can_subset=True,
)
def static_rus_tables(context):
    """Create static RUS tables."""
    static_tables = _read_static_encoding_tables("static_rus")

    selected_outputs = set(context.selected_output_names)
    return (
        dg.Output(output_name=table_name, value=df)
        for table_name, df in static_tables.items()
        if table_name in selected_outputs
    )
