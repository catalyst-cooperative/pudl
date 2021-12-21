"""
Load PUDL data into an Apache Parquet dataset.

Currently this module is only used for the EPA CEMS hourly dataset, but it will
also be used for other long tables that are too big for SQLite to handle
gracefully.

"""

import logging
from pathlib import Path
from typing import List, Literal, Union

import pandas as pd
import pyarrow as pa
from pyarrow import parquet as pq

from pudl.metadata.classes import Resource

logger = logging.getLogger(__name__)


def to_parquet(
    df: pd.DataFrame,
    resource_id: str,
    root_path: Union[str, Path],
    partition_cols: Union[List[str], Literal[None]] = None,
) -> None:
    """
    Write a PUDL table out to a partitioned Parquet dataset.

    Uses the name of the table to look up appropriate metadata and construct
    a PyArrow schema.

    Args:
        df: The tabular data to be written to a Parquet dataset.
        resource_id: Name of the table that's being written to Parquet.
        root_path: Top level directory for the partitioned dataset.
        partition_cols: Columns to use to partition the Parquet dataset. For
            EPA CEMS we use ["year", "state"].

    """
    pq.write_to_dataset(
        pa.Table.from_pandas(
            df,
            schema=Resource.from_id(resource_id).to_pyarrow(),
            preserve_index=False,
        ),
        root_path=root_path,
        partition_cols=partition_cols,
        compression="snappy",
    )
