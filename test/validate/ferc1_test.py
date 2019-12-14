"""
Validate post-ETL FERC Form 1 data and the associated derived outputs.

These tests depend on a FERC Form 1 specific PudlTabl output object, which is
a parameterized fixture that has session scope.
"""

import logging

import pandas as pd
import pytest

import pudl.constants as pc

logger = logging.getLogger(__name__)

# These are tables for which individual records have been sliced up and
# turned into columns -- so there's no universally unique record ID:
row_mapped_tables = [
    "plant_in_service_ferc1",
]
unique_record_tables = [
    t for t in pc.pudl_tables["ferc1"] if t not in row_mapped_tables
]


@pytest.mark.parametrize("table_name", unique_record_tables)
def test_record_id_dupes(pudl_engine, table_name):
    """Verify that the generated ferc1 record_ids are unique."""
    table = pd.read_sql(table_name, pudl_engine)
    n_dupes = table.record_id.duplicated().values.sum()
    logger.info(f"{n_dupes} duplicate record_ids found in {table_name}")

    if n_dupes:
        dupe_ids = (table.record_id[table.record_id.duplicated()].values)
        raise AssertionError(
            f"{n_dupes} duplicate record_ids found in "
            f"{table_name}: {dupe_ids}."
        )


def test_pu_ferc1(pudl_out_ferc1):
    """Test output routines for tables from FERC Form 1."""
    logger.info("Compiling FERC Form 1 plants & utilities table...")
    logger.info(f"{len(pudl_out_ferc1.pu_ferc1())} plant & utility "
                f"records found.")
