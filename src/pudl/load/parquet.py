"""
Load PUDL data into an Apache Parquet dataset.

Currently this module is only used for the EPA CEMS hourly dataset, but it will
also be used for other long tables that are too big for SQLite to handle
gracefully.

"""

import logging
from functools import partial

import pyarrow as pa
from pyarrow import parquet as pq

logger = logging.getLogger(__name__)

# We re-use the same data types with the same options several times in the
# definition of the PyArrow schema below. These partial() functions simply
# define convenient aliases so we don't have to define the same thing multiple
# times. These definitions will be migrated into the metadata classes. See:
# https://github.com/catalyst-cooperative/pudl/issues/1200
INT_NULLABLE = partial(pa.field, type=pa.int32(), nullable=True)
INT_NOT_NULL = partial(pa.field, type=pa.int32(), nullable=False)
STR_NOT_NULL = partial(pa.field, type=pa.string(), nullable=False)
# Timestamp resolution is hourly, but second is the largest allowed.
TIMESTAMP = partial(pa.field, type=pa.timestamp("s", tz="UTC"), nullable=False)
# float32 can hold integers up to 16,777,216 so no need for float64.
FLOAT_NULLABLE = partial(pa.field, type=pa.float32(), nullable=True)
FLOAT_NOT_NULL = partial(pa.field, type=pa.float32(), nullable=False)
DICT_NULLABLE = partial(
    pa.field,
    type=pa.dictionary(pa.int8(), pa.string(), ordered=False),
    nullable=True
)

EPACEMS_ARROW_SCHEMA = pa.schema([
    DICT_NULLABLE("state"),
    INT_NOT_NULL("plant_id_eia"),
    STR_NOT_NULL("unitid"),
    TIMESTAMP("operating_datetime_utc"),
    FLOAT_NULLABLE("operating_time_hours"),
    FLOAT_NOT_NULL("gross_load_mw"),
    FLOAT_NULLABLE("steam_load_1000_lbs"),
    FLOAT_NULLABLE("so2_mass_lbs"),
    DICT_NULLABLE("so2_mass_measurement_code"),
    FLOAT_NULLABLE("nox_rate_lbs_mmbtu"),
    DICT_NULLABLE("nox_rate_measurement_code"),
    FLOAT_NULLABLE("nox_mass_lbs"),
    DICT_NULLABLE("nox_mass_measurement_code"),
    FLOAT_NULLABLE("co2_mass_tons"),
    DICT_NULLABLE("co2_mass_measurement_code"),
    FLOAT_NOT_NULL("heat_content_mmbtu"),
    INT_NULLABLE("facility_id"),
    INT_NULLABLE("unit_id_epa"),
    INT_NOT_NULL("year"),
])
"""Schema defining efficient data types for EPA CEMS Parquet outputs."""


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
    pq.write_to_dataset(
        pa.Table.from_pandas(
            df,
            preserve_index=False,
            schema=EPACEMS_ARROW_SCHEMA,
        ),
        root_path=str(root_path),
        partition_cols=["year", "state"],  # Hard-coded b/c we assume this
        compression="snappy",  # Hard-coded b/c it's like 50x faster than gzip
    )
