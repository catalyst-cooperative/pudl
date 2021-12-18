"""
Load PUDL data into an Apache Parquet dataset.

Currently this module is only used for the EPA CEMS hourly dataset, but it will
also be used for other long tables that are too big for SQLite to handle
gracefully.

"""

import logging

import pyarrow as pa
from pyarrow import parquet as pq

import pudl

logger = logging.getLogger(__name__)


def epacems_to_parquet(df, root_path):
    """
    Write an EPA CEMS dataframe out to a partitioned Parquet dataset.

    Args:
        df (pandas.DataFrame): Dataframe containing the data to be output.
        root_path (path-like): The top level directory for the partitioned
            dataset.

    Returns:
        None

    """
    epacems_pyarrow_schema = (
        pudl.metadata.classes.Package.from_resource_ids(
            pudl.metadata.RESOURCE_METADATA
        )
        .get_resource("hourly_emissions_epacems")
        .to_pyarrow()
    )

    pq.write_to_dataset(
        pa.Table.from_pandas(
            df,
            preserve_index=False,
            schema=epacems_pyarrow_schema,
        ),
        root_path=str(root_path),
        partition_cols=["year", "state"],  # Hard-coded b/c we assume this
        compression="snappy",  # Hard-coded b/c it's like 50x faster than gzip
    )
