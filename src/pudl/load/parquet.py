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


def epacems_to_parquet(
    df,
    root_dir,
    partition_cols=('year', 'state'),
    compression="snappy",
):
    """Write an EPA CEMS dataframe out to a partitioned Parquet dataset."""
    pq.write_to_dataset(
        pa.Table.from_pandas(
            df,
            preserve_index=False,
            schema=EPACEMS_ARROW_SCHEMA,
        ),
        root_path=root_dir,
        partition_cols=list(partition_cols),
        compression=compression,
    )
