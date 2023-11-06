"""Extract EIA Form 176 data from CSVs.

The EIA Form 176 archive also contains CSVs for EIA Form 191 and EIA Form 757.
"""

import sqlalchemy as sa  # noqa: I001

from pudl.extract.csv import CsvExtractor


class Eia176CsvExtractor(CsvExtractor):
    """Extractor for EIA Form 176 data."""

    DATABASE_NAME = "eia176.sqlite"
    DATASET = "eia176"
    COLUMN_TYPES = {
        "e176_company": {
            "COMPANY_ID": sa.String,
            "ACTIVITY_STATUS": sa.String,
            "NAME1": sa.String,
        },
    }
