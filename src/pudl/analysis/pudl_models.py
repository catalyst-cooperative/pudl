"""Implement utilities for working with data produced in the pudl modelling repo."""

import os

import pandas as pd
import pyarrow as pa
from dagster import AssetsDefinition, asset
from deltalake import DeltaTable


def get_model_tables() -> list[str]:
    pudl_models_tables = []
    if os.getenv("USE_PUDL_MODELS"):
        pudl_models_tables = [
            # "core_sec10k__company_information",
            # "core_sec10k__exhibit_21_company_ownership",
            # "core_sec10k__filings",
            "out_sec_10k__parents_and_subsidiaries",
        ]

    return pudl_models_tables


def _get_table_uri(table_name: str) -> str:
    return f"gs://model-outputs.catalyst.coop/sec10k/{table_name}"


def pudl_models_asset_factory(table_name: str) -> AssetsDefinition:
    """Factory function to create assets which will load pudl models tables."""

    @asset(
        name=table_name,
        io_manager_key="pudl_io_manager",
    )
    def _asset() -> pd.DataFrame:
        return DeltaTable(_get_table_uri(table_name)).to_pandas()

    return _asset


def get_pudl_models_assets() -> list[AssetsDefinition]:
    """Generate a collection of assets for all PUDL model tables."""
    return [pudl_models_asset_factory(table) for table in get_model_tables()]


def get_model_table_schemas() -> list[str, str, pa.Schema]:
    """Return pyarrow schemas for all PUDL models tables."""
    dts = [DeltaTable(_get_table_uri(table_name)) for table_name in get_model_tables()]

    return [
        (dt.metadata().name, dt.metadata().description, dt.schema().to_pyarrow())
        for dt in dts
    ]
