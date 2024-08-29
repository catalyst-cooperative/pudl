"""Routines used for extracting the raw FERC 714 data."""

import copy
import json
from collections import OrderedDict
from pathlib import Path
from typing import Any

import pandas as pd
from dagster import AssetKey, AssetsDefinition, SourceAsset, asset

import pudl
from pudl.workspace.setup import PudlPaths

logger = pudl.logging_helpers.get_logger(__name__)

FERC714_CSV_FILES: OrderedDict[str, dict[str, str]] = OrderedDict(
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
"""Dictionary mapping PUDL tables to FERC-714 filenames and character encodings."""

FERC714_XBRL_FILES: OrderedDict[str, dict[str, str]] = OrderedDict(
    {
        "core_identification_and_certification_01_1": {
            "duration": "identification_and_certification_01_1_duration"
        },
        "core_generating_plants_included_in_reporting_balancing_authority_area_02_1": {
            "duration": "generating_plants_included_in_reporting_balancing_authority_area_02_1_duration"
        },
        "core_generating_plants_included_in_reporting_balancing_authority_area_totals_02_1": {
            "duration": "generating_plants_included_in_reporting_balancing_authority_area_totals_02_1_duration"
        },
        "core_balancing_authority_area_monthly_capabilities_at_time_of_monthly_peak_demand_02_2": {
            "duration": "balancing_authority_area_monthly_capabilities_at_time_of_monthly_peak_demand_02_2_duration"
        },
        "core_balancing_authority_area_net_energy_for_load_and_peak_demand_sources_by_month_02_3": {
            "duration": "balancing_authority_area_net_energy_for_load_and_peak_demand_sources_by_month_02_3_duration"
        },
        "core_adjacent_balancing_authority_area_interconnections_02_4": {
            "duration": "adjacent_balancing_authority_area_interconnections_02_4_duration"
        },
        "core_balancing_authority_area_scheduled_and_actual_interchange_02_5": {
            "duration": "balancing_authority_area_scheduled_and_actual_interchange_02_5_duration"
        },
        "core_balancing_authority_area_scheduled_and_actual_interchange_totals_02_5": {
            "duration": "balancing_authority_area_scheduled_and_actual_interchange_totals_02_5_duration"
        },
        "core_balancing_authority_area_system_lambda_data_and_description_of_economic_dispatch_02_6": {
            "duration": "balancing_authority_area_system_lambda_data_and_description_of_economic_dispatch_02_6_duration",
            "instant": "balancing_authority_area_system_lambda_data_and_description_of_economic_dispatch_02_6_instant",
        },
        "core_electric_utilities_that_compose_the_planning_area_03_1": {
            "duration": "electric_utilities_that_compose_the_planning_area_03_1_duration"
        },
        "core_planning_area_hourly_demand_and_forecast_summer_and_winter_peak_demand_and_annual_net_energy_for_load_03_2": {
            "duration": "planning_area_hourly_demand_and_forecast_summer_and_winter_peak_demand_and_annual_net_energy_for_load_03_2_duration",
            "instant": "planning_area_hourly_demand_and_forecast_summer_and_winter_peak_demand_and_annual_net_energy_for_load_03_2_instant",
        },
        "core_planning_area_hourly_demand_and_forecast_summer_and_winter_peak_demand_and_annual_net_energy_for_load_table_03_2": {
            "duration": "planning_area_hourly_demand_and_forecast_summer_and_winter_peak_demand_and_annual_net_energy_for_load_table_03_2_duration"
        },
    }
)


def raw_ferc714_csv_asset_factory(table_name: str) -> AssetsDefinition:
    """Generates an asset for building the raw CSV-based FERC 714 dataframe."""
    assert table_name in FERC714_CSV_FILES

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
        years = ", ".join(map(str, ferc714_settings.years))

        logger.info(
            f"Extracting {table_name} from CSV into pandas DataFrame (years: {years})."
        )
        with (
            ds.get_zipfile_resource("ferc714", name="ferc714.zip") as zf,
            zf.open(FERC714_CSV_FILES[table_name]["name"]) as csv_file,
        ):
            df = pd.read_csv(
                csv_file,
                encoding=FERC714_CSV_FILES[table_name]["encoding"],
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

    def get_metadata(xbrl_table: str | list[str], xbrl_meta_all):
        if isinstance(xbrl_table, str):
            xbrl_table = [xbrl_table]
        return [
            metadata
            for table_name in xbrl_table
            for metadata in xbrl_meta_all.get(table_name, [])
            if metadata
        ]

    xbrl_meta_out = copy.deepcopy(FERC714_XBRL_FILES)
    for table_name, periods in FERC714_XBRL_FILES.items():
        for period, tt in periods.items():
            xbrl_meta_out[table_name][period] = get_metadata(tt, xbrl_meta_all)

    return xbrl_meta_out


def create_raw_ferc714_xbrl_assets() -> list[SourceAsset]:
    """Create SourceAssets for raw FERC 714 XBRL tables.

    SourceAssets allow you to access assets that are generated elsewhere.
    In our case, the XBRL database contains the raw FERC 714 assets from
    2021 onward. Prior to that, the assets are distributed as CSVs and
    are extracted with the ``raw_ferc714_csv_asset_factory`` function.

    Returns:
        A list of FERC 714 SourceAssets.
    """
    raw_table_names = [
        v for value in FERC714_XBRL_FILES.values() for v in value.values()
    ]
    return [
        SourceAsset(
            key=AssetKey(f"raw_ferc714_xbrl__{table_name}"),
            io_manager_key="ferc714_xbrl_sqlite_io_manager",
        )
        for table_name in raw_table_names
    ]


raw_ferc714_csv_assets = [
    raw_ferc714_csv_asset_factory(table_name) for table_name in FERC714_CSV_FILES
]

raw_ferc714_xbrl_assets = create_raw_ferc714_xbrl_assets()
