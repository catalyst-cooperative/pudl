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

from pudl.extract.dbf import AbstractFercDbfReader, FercDbfExtractor, FercDbfReader
from pudl.extract.ferc import add_key_constraints
from pudl.workspace.datastore import Datastore


class Ferc2DbfExtractor(FercDbfExtractor):
    """Wrapper for running the foxpro to sqlite conversion of FERC1 dataset."""

    DATABASE_NAME = "ferc2.sqlite"

    def get_dbf_reader(self, base_datastore: Datastore) -> AbstractFercDbfReader:
        """Returns FERC Form 2 compatible dbf reader instance."""
        return FercDbfReader(base_datastore, dataset="ferc2")

    def finalize_schema(self, meta: sa.MetaData) -> sa.MetaData:
        """Add primary and foreign keys for respondent_id."""
        return add_key_constraints(
            meta, pk_table="f2_s0_respondent_id", column="respondent_id"
        )

    def select_partition_filters(
        self, fls: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Returns only those filters where part=None.

        Early years with part=1 or part=2 contain strange data that can't be processed
        by the FercDbfReader.
        """
        return [f for f in fls if f.get("part", None) is None]
