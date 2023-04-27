"""Extract FERC Form 2 data from SQLite DBs derived from original DBF files."""

from pudl.extract.dbf import AbstractFercDbfReader, FercDbfExtractor, FercDbfReader
from pudl.workspace.datastore import Datastore


class Ferc2DbfExtractor(FercDbfExtractor):
    """Wrapper for running the foxpro to sqlite conversion of FERC1 dataset."""

    DATABASE_NAME = "ferc2.sqlite"

    def get_dbf_reader(self, base_datastore: Datastore) -> AbstractFercDbfReader:
        """Returns FERC Form 2 compatible dbf reader instance."""
        return FercDbfReader(base_datastore, dataset="ferc2")

    # finalize_schema() for constraints
    # postprocess() for respondents (if present)