"""Extract FERC Form 2 data from SQLite DBs derived from original DBF files."""

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
        return add_key_constraints(meta, pk_table="f2_s0_respondent_id", column="respondent_id")
