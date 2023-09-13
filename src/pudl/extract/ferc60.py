"""Extract FERC Form 60 data from DBF archives."""

import pandas as pd
import sqlalchemy as sa

from pudl.extract.dbf import (
    FercDbfExtractor,
    PartitionedDataFrame,
    add_key_constraints,
    deduplicate_by_year,
)
from pudl.settings import FercToSqliteSettings, GenericDatasetSettings


class Ferc60DbfExtractor(FercDbfExtractor):
    """Extracts FERC Form 60 data from the legacy DBF archives."""

    DATASET = "ferc60"
    DATABASE_NAME = "ferc60.sqlite"

    def get_settings(
        self, global_settings: FercToSqliteSettings
    ) -> GenericDatasetSettings:
        """Returns settings for FERC Form 60 DBF dataset."""
        return global_settings.ferc60_dbf_to_sqlite_settings

    def finalize_schema(self, meta: sa.MetaData) -> sa.MetaData:
        """Add primary and foreign keys for respondent_id."""
        return add_key_constraints(
            meta, pk_table="f60_s0_respondent_id", column="respondent_id"
        )

    def aggregate_table_frames(
        self, table_name: str, dfs: list[PartitionedDataFrame]
    ) -> pd.DataFrame | None:
        """Runs the deduplication on f60_s0_respondent_id table.

        Other tables are aggregated as usual, meaning that the partial frames are simply
        concatenated.
        """
        if table_name == "f60_s0_respondent_id":
            return deduplicate_by_year(dfs, "respondent_id")
        return super().aggregate_table_frames(table_name, dfs)
