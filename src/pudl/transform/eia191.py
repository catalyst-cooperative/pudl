"""Module to perform data cleaning functions on EIA191 data tables."""

import warnings
from enum import Enum

import pandas as pd
from dagster import ExperimentalWarning, asset

from pudl.helpers import add_fips_ids, convert_to_date
from pudl.logging_helpers import get_logger

logger = get_logger(__name__)

# Asset Checks are still Experimental, silence the warning since we use them
# everywhere.
warnings.filterwarnings("ignore", category=ExperimentalWarning)


class Eia191FieldType(Enum):
    """Field types for EIA 191 data."""

    DEPLETED_FIELD = 0
    SALT_DOME = 60
    AQUIFER = 396


class Eia191StatusType(Enum):
    """Status types for EIA 191 data."""

    ACTIVE = 0
    INACTIVE = 446


# Identify and create primary key
pk = ["id", "month", "year"]


@asset
def _core_eia176__data(raw_eia176__data: pd.DataFrame) -> pd.DataFrame:
    """Perform several transformations on the raw data.

    1. Construct a report_date column with monthly granularity based on the report_year and report_month columns
    2. Identify and create the natural primary key of this table.
    3. Define snake_case categorical values for field_type and status and enforce either with an ENUM constraint, or a FK relationship to a new coding table.
    4. Infer FIPS code for state and county using pudl.helpers.add_fips_ids()
    """
    return raw_eia176__data.pipe(
        convert_to_date, date_col="report_date", month_col="month"
    ).pipe(add_fips_ids, state_col="report_state", county_col="county_name")
