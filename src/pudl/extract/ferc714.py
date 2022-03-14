"""Routines used for extracting the raw FERC 714 data."""
import logging
import warnings

import pandas as pd

import pudl
from pudl.settings import Ferc714Settings

logger = logging.getLogger(__name__)

TABLE_FNAME = {
    "id_certification_ferc714": "Part 1 Schedule 1 - Identification Certification.csv",
    "gen_plants_ba_ferc714": "Part 2 Schedule 1 - Balancing Authority Generating Plants.csv",
    "demand_monthly_ba_ferc714": "Part 2 Schedule 2 - Balancing Authority Monthly Demand.csv",
    "net_energy_load_ba_ferc714": "Part 2 Schedule 3 - Balancing Authority Net Energy for Load.csv",
    "adjacency_ba_ferc714": "Part 2 Schedule 4 - Adjacent Balancing Authorities.csv",
    "interchange_ba_ferc714": "Part 2 Schedule 5 - Balancing Authority Interchange.csv",
    "lambda_hourly_ba_ferc714": "Part 2 Schedule 6 - Balancing Authority Hourly System Lambda.csv",
    "lambda_description_ferc714": "Part 2 Schedule 6 - System Lambda Description.csv",
    "description_pa_ferc714": "Part 3 Schedule 1 - Planning Area Description.csv",
    "demand_forecast_pa_ferc714": "Part 3 Schedule 2 - Planning Area Forecast Demand.csv",
    "demand_hourly_pa_ferc714": "Part 3 Schedule 2 - Planning Area Hourly Demand.csv",
    "respondent_id_ferc714": "Respondent IDs.csv",
}
"""Dictionary mapping PUDL tables to filenames within the FERC 714 zipfile."""

TABLE_ENCODING = {
    "id_certification_ferc714": "iso-8859-1",
    "gen_plants_ba_ferc714": "iso-8859-1",
    "demand_monthly_ba_ferc714": None,
    "net_energy_load_ba_ferc714": None,
    "adjacency_ba_ferc714": "iso-8859-1",
    "interchange_ba_ferc714": "iso-8859-1",
    "lambda_hourly_ba_ferc714": None,
    "lambda_description_ferc714": "iso-8859-1",
    "description_pa_ferc714": "iso-8859-1",
    "demand_forecast_pa_ferc714": None,
    "demand_hourly_pa_ferc714": None,
    "respondent_id_ferc714": None,
}
"""Dictionary describing the character encodings of the FERC 714 CSV files."""


def extract(
    ferc714_settings: Ferc714Settings = Ferc714Settings(),
    pudl_settings=None,
    ds=None
):
    """
    Extract the raw FERC Form 714 dataframes from their original CSV files.

    Args:
        ferc714_settings: Object containing validated settings relevant to
            FERC Form 714.
        pudl_settings (dict): A PUDL settings dictionary.
        ds (Datastore): instance of the datastore

    Returns:
        dict: A dictionary of dataframes, with raw FERC 714 table names as the
        keys, and minimally processed pandas.DataFrame instances as the values.

    """
    warnings.warn(
        "Integration of FERC 714 into PUDL is still experimental and incomplete.\n"
        "The data has not yet been validated, and the structure may change."
    )
    if pudl_settings is None:
        pudl_settings = pudl.workspace.setup.get_defaults()
    raw_dfs = {}
    for table in ferc714_settings.tables:
        logger.info(f"Extracting {table} from CSV into pandas DataFrame.")
        with ds.get_zipfile_resource("ferc714", name="ferc714.zip").open(TABLE_FNAME[table]) as f:
            raw_dfs[table] = pd.read_csv(f, encoding=TABLE_ENCODING[table])
    return raw_dfs
