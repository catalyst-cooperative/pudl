"""Routines used for extracting the raw FERC 714 data."""

import json
from collections import OrderedDict
from itertools import chain
from pathlib import Path
from typing import Any

import pandas as pd
from dagster import AssetKey, AssetsDefinition, AssetSpec, asset

import pudl
from pudl.workspace.setup import PudlPaths

logger = pudl.logging_helpers.get_logger(__name__)

FERC714_CSV_ENCODING: OrderedDict[str, dict[str, str]] = OrderedDict(
    {
        "yearly_id_certification": {
            "name": "Part 1 Schedule 1 - Identification Certification.csv",
            "encoding": "iso-8859-1",
        },
        "yearly_balancing_authority_plants": {
            "name": "Part 2 Schedule 1 - Balancing Authority Generating Plants.csv",
            "encoding": "iso-8859-1",
        },
        "monthly_balancing_authority_demand": {
            "name": "Part 2 Schedule 2 - Balancing Authority Monthly Demand.csv",
            "encoding": "utf-8",
        },
        "yearly_balancing_authority_net_energy_load": {
            "name": "Part 2 Schedule 3 - Balancing Authority Net Energy for Load.csv",
            "encoding": "utf-8",
        },
        "yearly_balancing_authority_adjacency": {
            "name": "Part 2 Schedule 4 - Adjacent Balancing Authorities.csv",
            "encoding": "iso-8859-1",
        },
        "yearly_balancing_authority_interchange": {
            "name": "Part 2 Schedule 5 - Balancing Authority Interchange.csv",
            "encoding": "iso-8859-1",
        },
        "hourly_balancing_authority_lambda": {
            "name": "Part 2 Schedule 6 - Balancing Authority Hourly System Lambda.csv",
            "encoding": "utf-8",
        },
        "yearly_lambda_description": {
            "name": "Part 2 Schedule 6 - System Lambda Description.csv",
            "encoding": "iso-8859-1",
        },
        "yearly_planning_area_description": {
            "name": "Part 3 Schedule 1 - Planning Area Description.csv",
            "encoding": "iso-8859-1",
        },
        "yearly_planning_area_demand_forecast": {
            "name": "Part 3 Schedule 2 - Planning Area Forecast Demand.csv",
            "encoding": "utf-8",
        },
        "hourly_planning_area_demand": {
            "name": "Part 3 Schedule 2 - Planning Area Hourly Demand.csv",
            "encoding": "utf-8",
        },
        "respondent_id": {
            "name": "Respondent IDs.csv",
            "encoding": "utf-8",
        },
    }
)
"""Dictionary mapping PUDL tables to FERC-714 CSV filenames and character encodings."""

TABLE_NAME_MAP_FERC714: OrderedDict[str, dict[str, str]] = OrderedDict(
    {
        "core_ferc714__yearly_planning_area_demand_forecast": {
            "csv": "Part 3 Schedule 2 - Planning Area Forecast Demand.csv",
            "xbrl": "planning_area_hourly_demand_and_forecast_summer_and_winter_peak_demand_and_annual_net_energy_for_load_table_03_2",
        },
        "core_ferc714__hourly_planning_area_demand": {
            "csv": "Part 3 Schedule 2 - Planning Area Hourly Demand.csv",
            "xbrl": "planning_area_hourly_demand_and_forecast_summer_and_winter_peak_demand_and_annual_net_energy_for_load_03_2",
        },
        "core_ferc714__respondent_id": {
            "csv": "Respondent IDs.csv",
            "xbrl": "identification_and_certification_01_1",
        },
    }
)
"""A mapping of PUDL DB table names to their XBRL and CSV source table names."""


def raw_ferc714_csv_asset_factory(table_name: str) -> AssetsDefinition:
    """Generates an asset for building the raw CSV-based FERC 714 dataframe."""
    assert table_name in FERC714_CSV_ENCODING

    @asset(
        name=f"raw_ferc714_csv__{table_name}",
        required_resource_keys={"datastore", "dataset_settings"},
        compute_kind="pandas",
    )
    def _extract_raw_ferc714_csv(context):
        """Extract the raw FERC Form 714 dataframes from their original CSV files.

        Args:
            context: dagster keyword that provides access to resources and config.
        """
        ds = context.resources.datastore
        ferc714_settings = context.resources.dataset_settings.ferc714
        years = ", ".join(map(str, ferc714_settings.csv_years))

        logger.info(
            f"Extracting {table_name} from CSV into pandas DataFrame (years: {years})."
        )
        with (
            ds.get_zipfile_resource("ferc714", name="ferc714.zip") as zf,
            zf.open(FERC714_CSV_ENCODING[table_name]["name"]) as csv_file,
        ):
            df = pd.read_csv(
                csv_file,
                encoding=FERC714_CSV_ENCODING[table_name]["encoding"],
            )
        if table_name != "respondent_id":
            df = df.query("report_yr in @ferc714_settings.years")
        return df

    return _extract_raw_ferc714_csv


@asset
def raw_ferc714_xbrl__metadata_json(
    context,
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    """Extract the FERC 714 XBRL Taxonomy metadata we've stored as JSON.

    Returns:
        A dictionary keyed by PUDL table name, with an instant and a duration entry
        for each table, corresponding to the metadata for each of the respective instant
        or duration tables from XBRL if they exist. Table metadata is returned as a list
        of dictionaries, each of which can be interpreted as a row in a tabular
        structure, with each row annotating a separate XBRL concept from the FERC 714
        filings.
    """
    metadata_path = PudlPaths().output_dir / "ferc714_xbrl_taxonomy_metadata.json"
    with Path.open(metadata_path) as f:
        xbrl_meta_all = json.load(f)

    valid_tables = {
        table_name: xbrl_table
        for table_name in TABLE_NAME_MAP_FERC714
        if (xbrl_table := TABLE_NAME_MAP_FERC714.get(table_name, {}).get("xbrl"))
        is not None
    }

    def squash_period(xbrl_table: str | list[str], period, xbrl_meta_all):
        if isinstance(xbrl_table, str):
            xbrl_table = [xbrl_table]
        return [
            metadata
            for table in xbrl_table
            for metadata in xbrl_meta_all.get(f"{table}_{period}", [])
            if metadata
        ]

    xbrl_meta_out = {
        table_name: {
            "instant": squash_period(xbrl_table, "instant", xbrl_meta_all),
            "duration": squash_period(xbrl_table, "duration", xbrl_meta_all),
        }
        for table_name, xbrl_table in valid_tables.items()
    }

    return xbrl_meta_out


def create_raw_ferc714_xbrl_assets() -> list[AssetSpec]:
    """Create AssetSpecs for raw FERC 714 XBRL tables.

    AssetSpecs allow you to specify and access assets that are generated elsewhere.  In
    our case, the XBRL database contains the raw FERC 714 assets from 2021 onward. Prior
    to that, the assets are distributed as CSVs and are extracted with the
    ``raw_ferc714_csv_asset_factory`` function.

    Returns:
        A list of FERC 714 AssetSpecs.
    """
    # Create assets for the duration and instant tables
    xbrls = (v["xbrl"] for v in TABLE_NAME_MAP_FERC714.values())
    flattened_xbrls = chain.from_iterable(
        x if isinstance(x, list) else [x] for x in xbrls
    )
    xbrls_with_periods = chain.from_iterable(
        (f"{tn}_instant", f"{tn}_duration") for tn in flattened_xbrls
    )
    xbrl_table_names = tuple(set(xbrls_with_periods))
    raw_ferc714_xbrl_assets = [
        AssetSpec(key=AssetKey(f"raw_ferc714_xbrl__{table_name}")).with_io_manager_key(
            "ferc714_xbrl_sqlite_io_manager"
        )
        for table_name in xbrl_table_names
    ]
    return raw_ferc714_xbrl_assets


raw_ferc714_csv_assets = [
    raw_ferc714_csv_asset_factory(table_name) for table_name in FERC714_CSV_ENCODING
]

raw_ferc714_xbrl_assets = create_raw_ferc714_xbrl_assets()
