"""Extract EIA Form 176 data from CSVs.

The EIA Form 176 archive also contains CSVs for EIA Form 191 and EIA Form 757.
"""

import sqlalchemy as sa  # noqa: I001

from pudl.extract.csv import CsvExtractor
from pudl.workspace.setup import PudlPaths


class Eia176CsvExtractor(CsvExtractor):
    """Extractor for EIA Form 176 data."""

    DATASET = "eia176"
    DATABASE_NAME = "eia176.sqlite"
    COLUMN_TYPES = {
        "e176_company": {
            "COMPANY_ID": sa.String,
            "ACTIVITY_STATUS": sa.String,
            "NAME1": sa.String,
        },
    }


def extract_eia176(context):
    """Extract raw EIA data from excel sheets into dataframes.

    Args:
        # TODO: Add this context for dagster once you're past simple local execution
        context: dagster keyword that provides access to resources and config.
    """
    ds = context.resources.datastore
    # TODO: Should I use this?
    # eia_settings = context.resources.dataset_settings.eia

    csv_extractor = Eia176CsvExtractor(ds, PudlPaths().output_dir)
    csv_extractor.execute()
