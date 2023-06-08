"""Extract FERC Form 2 data from SQLite DBs derived from original DBF files.

The Form No. 2 is a compilation of financial and operational information from major
interstate natural gas pipelines subject to the jurisdiction of the FERC. The form
contains data for a calendar year. Among other things, the form contains a Comparative
Balance Sheet, Statement of Income, Statement of Retained Earnings, Statement of Cash
Flows, and Notes to Financial Statements.

Major is defined as having combined gas transported or stored for a fee that exceeds 50
million dekatherms.
"""

from typing import Any

import sqlalchemy as sa

import pudl
from pudl.extract.dbf import FercDbfExtractor
from pudl.extract.ferc import add_key_constraints
from pudl.settings import FercToSqliteSettings, GenericDatasetSettings

logger = pudl.logging_helpers.get_logger(__name__)


class Ferc2DbfExtractor(FercDbfExtractor):
    """Wrapper for running the foxpro to sqlite conversion of FERC1 dataset."""

    DATASET = "ferc2"
    DATABASE_NAME = "ferc2.sqlite"

    def get_settings(
        self, global_settings: FercToSqliteSettings
    ) -> GenericDatasetSettings:
        """Returns settings for FERC Form 1 DBF dataset."""
        return global_settings.ferc2_dbf_to_sqlite_settings

    def finalize_schema(self, meta: sa.MetaData) -> sa.MetaData:
        """Add primary and foreign keys for respondent_id."""
        return add_key_constraints(
            meta, pk_table="f2_s0_respondent_id", column="respondent_id"
        )

    @staticmethod
    def is_valid_partition(fl: dict[str, Any]):
        """Returns False if part key has value other than None.

        This eliminates partitions with part=1 or part=2.
        """
        return fl.get("part", None) is None
