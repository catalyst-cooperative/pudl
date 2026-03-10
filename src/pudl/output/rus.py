"""Create output tables for RUS-7 and RUS-12."""

import pandas as pd
from dagster import AssetIn, AssetsDefinition, asset

from pudl.metadata.resource_helpers import (
    HARVESTED_CORE_TABLES_RUS7,
    HARVESTED_CORE_TABLES_RUS12,
)


def out_rus_asset_factory(
    core_table_name: str,
    borrower_table_name: str,
    io_manager_key: str | None = None,
) -> AssetsDefinition:
    """An asset factory for finished RUS output tables.

    Args:
        core_table_name: the name of the core table.
        borrower_table_name: the name of the borrower table which we
            want to merge onto the core table.
        io_manager_key: the name of the IO Manager of the final asset.

    Returns:
        A RUS output asset.
    """
    out_table_name = f"out_{core_table_name.removeprefix('core_')}"

    @asset(
        ins={core_table_name: AssetIn(), borrower_table_name: AssetIn()},
        name=out_table_name,
        io_manager_key=io_manager_key,
    )
    def out_rus_asset(**ins) -> pd.DataFrame:
        """Convert RUS core table to out - merge in the borrower info."""
        return pd.merge(
            ins[borrower_table_name], ins[core_table_name], on=["borrower_id_rus"]
        )

    return out_rus_asset


out_rus7_assets = [
    out_rus_asset_factory(
        core_table_name=core_table_name,
        borrower_table_name="core_rus7__entity_borrowers",
        io_manager_key="pudl_io_manager",
    )
    for core_table_name in HARVESTED_CORE_TABLES_RUS7
]

out_rus12_assets = [
    out_rus_asset_factory(
        core_table_name=core_table_name,
        borrower_table_name="core_rus12__entity_borrowers",
        io_manager_key="pudl_io_manager",
    )
    for core_table_name in HARVESTED_CORE_TABLES_RUS12
]
