"""Module to perform data cleaning functions on NREL ATB data tables."""

import logging

import pandas as pd
import pudl.constants as pc

logger = logging.getLogger(__name__)


def transform(nrelatb_raw_dfs, nrelatb_tables=pc.nrelatb_pudl_tables):
    """
    Transform NREL ATB DataFrames.

    Args:
        nrelatb_raw_dfs(dict): a dictionary of table names(keys) and
            DataFrames(values)
        nrelatb_tables(list): The list of NREL ATB tables that can be
            successfully pulled into PUDL

    Returns:
        dict: A dictionary of DataFrame objects in which tables from NREL
        ATB(keys) correspond to normalized DataFrames of values from
        that table(values)
    """
    nrelatb_transformed_dfs = pd.DataFrame()

    for tablename, table in nrelatb_raw_dfs.items():
        logger.info(f"Transforming raw NREL ATB DataFrames for {tablename}")
        src, mkt, financial = tablename.split("-")
        table["maturity"] = mkt
        table["cap_recovery_period"] = financial

        pd.concat([nrelatb_transformed_dfs, table])

    return nrelatb_transformed_dfs
