"""Routines used for extracting the raw FERC 714 data."""
from collections import OrderedDict

import pandas as pd
from dagster import AssetOut, Output, multi_asset

import pudl

logger = pudl.logging_helpers.get_logger(__name__)

FERC714_FILES: OrderedDict[str, dict[str, str]] = OrderedDict(
    {
        "id_certification_ferc714": {
            "name": "Part 1 Schedule 1 - Identification Certification.csv",
            "encoding": "iso-8859-1",
        },
        "gen_plants_ba_ferc714": {
            "name": "Part 2 Schedule 1 - Balancing Authority Generating Plants.csv",
            "encoding": "iso-8859-1",
        },
        "demand_monthly_ba_ferc714": {
            "name": "Part 2 Schedule 2 - Balancing Authority Monthly Demand.csv",
            "encoding": "utf-8",
        },
        "net_energy_load_ba_ferc714": {
            "name": "Part 2 Schedule 3 - Balancing Authority Net Energy for Load.csv",
            "encoding": "utf-8",
        },
        "adjacency_ba_ferc714": {
            "name": "Part 2 Schedule 4 - Adjacent Balancing Authorities.csv",
            "encoding": "iso-8859-1",
        },
        "interchange_ba_ferc714": {
            "name": "Part 2 Schedule 5 - Balancing Authority Interchange.csv",
            "encoding": "iso-8859-1",
        },
        "lambda_hourly_ba_ferc714": {
            "name": "Part 2 Schedule 6 - Balancing Authority Hourly System Lambda.csv",
            "encoding": "utf-8",
        },
        "lambda_description_ferc714": {
            "name": "Part 2 Schedule 6 - System Lambda Description.csv",
            "encoding": "iso-8859-1",
        },
        "description_pa_ferc714": {
            "name": "Part 3 Schedule 1 - Planning Area Description.csv",
            "encoding": "iso-8859-1",
        },
        "demand_forecast_pa_ferc714": {
            "name": "Part 3 Schedule 2 - Planning Area Forecast Demand.csv",
            "encoding": "utf-8",
        },
        "demand_hourly_pa_ferc714": {
            "name": "Part 3 Schedule 2 - Planning Area Hourly Demand.csv",
            "encoding": "utf-8",
        },
        "respondent_id_ferc714": {
            "name": "Respondent IDs.csv",
            "encoding": "utf-8",
        },
    }
)
"""Dictionary mapping PUDL tables to FERC-714 filenames and character encodings."""


@multi_asset(
    outs={"raw_" + table_name: AssetOut() for table_name in FERC714_FILES},
    required_resource_keys={"datastore"},
)
def extract_ferc714(context):
    """Extract the raw FERC Form 714 dataframes from their original CSV files.

    Args:
        context: dagster keyword that provides access to resources and config.

    Returns:
        A tuple of extracted FERC-714 dataframes.
    """
    logger.warning(
        "Note that all years of FERC-714 data are lumped together and will be "
        "processed together regardless of what years are requested."
    )
    ds = context.resources.datastore
    raw_dfs: OrderedDict[str, pd.DataFrame] = OrderedDict({})
    for table_name in FERC714_FILES:
        logger.info(f"Extracting {table_name} from CSV into pandas DataFrame.")
        with ds.get_zipfile_resource("ferc714", name="ferc714.zip").open(
            FERC714_FILES[table_name]["name"]
        ) as f:
            raw_dfs[table_name] = pd.read_csv(
                f, encoding=FERC714_FILES[table_name]["encoding"]
            )
    return (
        Output(output_name="raw_" + table_name, value=df)
        for table_name, df in raw_dfs.items()
    )
