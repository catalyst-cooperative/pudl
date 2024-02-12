"""Routines used for extracting the raw FERC 714 data."""
from collections import OrderedDict

import pandas as pd
from dagster import AssetsDefinition, asset

import pudl

logger = pudl.logging_helpers.get_logger(__name__)

FERC714_FILES: OrderedDict[str, dict[str, str]] = OrderedDict(
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
        "yearly_planning_area_forecast_demand": {
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


def generate_raw_ferc714_asset(table_name: str) -> AssetsDefinition:
    """Generates an asset for building the raw FERC 714 dataframe."""
    assert table_name in FERC714_FILES

    @asset(
        name=f"raw_ferc714__{table_name}",
        required_resource_keys={"datastore", "dataset_settings"},
    )
    def _extract_raw_ferc714(context):
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
        with ds.get_zipfile_resource("ferc714", name="ferc714.zip") as zf, zf.open(
            FERC714_FILES[table_name]["name"]
        ) as csv_file:
            df = pd.read_csv(
                csv_file,
                encoding=FERC714_FILES[table_name]["encoding"],
            )
        if table_name != "respondent_id":
            df = df.query("report_yr in @ferc714_settings.years")
        return df

    return _extract_raw_ferc714


raw_ferc714_assets = [
    generate_raw_ferc714_asset(table_name) for table_name in FERC714_FILES
]
