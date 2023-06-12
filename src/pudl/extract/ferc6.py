"""Extract FERC Form 6 data from DBF archives."""

import sqlalchemy as sa

from pudl.extract.dbf import FercDbfExtractor, add_key_constraints
from pudl.settings import FercToSqliteSettings, GenericDatasetSettings


class Ferc6DbfExtractor(FercDbfExtractor):
    """Extracts FERC Form 2 data from the legacy DBF archives."""

    DATASET = "ferc6"
    DATABASE_NAME = "ferc6.sqlite"

    def get_settings(
        self, global_settings: FercToSqliteSettings
    ) -> GenericDatasetSettings:
        """Returns settings for FERC Form 1 DBF dataset."""
        return global_settings.ferc6_dbf_to_sqlite_settings

    def finalize_schema(self, meta: sa.MetaData) -> sa.MetaData:
        """Add primary and foreign keys for respondent_id."""
        return add_key_constraints(
            meta, pk_table="f6_s0_respondent_id", column="respondent_id"
        )
