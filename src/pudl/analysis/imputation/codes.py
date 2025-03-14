"""Define codes which indicate why a value was imputed."""

from enum import Enum, unique

import pandas as pd
from dagster import asset


@unique
class ImputationReasonCodes(Enum):
    """Defines all reasons a value might be flagged for imputation."""

    ANOMALOUS_REGION = "Indicates that value is surrounded by flagged values."
    NEGATIVE_OR_ZERO = "Indicates value is negative or zero value."
    IDENTICAL_RUN = (
        "Indicates value is among last values in an identical run of values."
    )
    GLOBAL_OUTLIER = (
        "Indicates value is greater or less than n times the global median."
    )
    GLOBAL_OUTLIER_NEIGHBOR = "Indicates value is a neighbors global outliers."
    LOCAL_OUTLIER_HIGH = "Indicates value is a local outlier on the high end."
    LOCAL_OUTLIER_LOW = "Indicates value is a local outlier on the low end."
    DOUBLE_DELTA = "Indicates value is very different from neighbors on either side."
    SINGLE_DELTA = (
        "Indicates value is significantly different from nearest unflagged value."
    )


@asset(io_manager_key="pudl_io_manager")
def core_pudl__codes_imputation_reasons() -> pd.DataFrame:
    """Static table containing all ImputationReasonCodes and descriptions."""
    return pd.DataFrame(
        [
            {"code": code.name, "description": code.value}
            for code in ImputationReasonCodes
        ]
    )
