"""Extract FERC Form 6 data from DBF archives."""

import pandas as pd
import sqlalchemy as sa

from pudl.extract.dbf import (
    FercDbfExtractor,
    PartitionedDataFrame,
    add_key_constraints,
    deduplicate_by_year,
)
from pudl.settings import FercDbfToSqliteDataConfig, FercToSqliteDataConfig


class Ferc6DbfExtractor(FercDbfExtractor):
    """Extracts FERC Form 6 data from the legacy DBF archives."""

    DATASET = "ferc6"
    DATABASE_NAME = "ferc6_dbf.sqlite"

    def get_data_config(
        self, ferc_to_sqlite_data_config: FercToSqliteDataConfig
    ) -> FercDbfToSqliteDataConfig:
        """Returns data config for FERC Form 6 DBF dataset."""
        return ferc_to_sqlite_data_config.ferc6_dbf

    def finalize_schema(self, meta: sa.MetaData) -> sa.MetaData:
        """Add primary and foreign keys for respondent_id."""
        return add_key_constraints(
            meta, pk_table="f6_s0_respondent_id", column="respondent_id"
        )

    def aggregate_table_frames(
        self, table_name: str, dfs: list[PartitionedDataFrame]
    ) -> pd.DataFrame | None:
        """Runs the deduplication on f6_s0_respondent_id table.

        Other tables are aggregated as usual, meaning that the partial frames are simply
        concatenated.
        """
        if table_name == "f6_s0_respondent_id":
            return deduplicate_by_year(dfs, "respondent_id")
        return super().aggregate_table_frames(table_name, dfs)
