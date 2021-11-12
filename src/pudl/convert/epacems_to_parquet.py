"""A script for converting the EPA CEMS dataset from gzip to Apache Parquet.

The original EPA CEMS data is available as ~12,000 gzipped CSV files, one for
each month for each state, from 1995 to the present. On disk they take up
about 7.3 GB of space, compressed. Uncompressed it is closer to 100 GB. That's
too much data to work with in memory.

Apache Parquet is a compressed, columnar datastore format, widely used in Big
Data applications. It's an open standard, and is very fast to read from disk.
It works especially well with both `Dask dataframes <https://dask.org/>`__ (a
parallel / distributed computing extension of pandas) and Apache Spark (a cloud
based Big Data processing pipeline system.)

Since pulling 100 GB of data into SQLite takes a long time, and working with
that data en masse isn't particularly pleasant on a laptop, this script can be
used to convert the original EPA CEMS data to the more widely usable Apache
Parquet format for use with Dask, either on a multi-core workstation or in an
interactive cloud computing environment like `Pangeo <https://pangeo.io>`__.

"""
import argparse
import logging
import sys
from functools import partial

import coloredlogs
import pandas as pd
import prefect
import pyarrow as pa

import pudl
from pudl import constants as pc
from pudl.settings import EpaCemsSettings
from pudl.workflow.epacems import EpaCemsPipeline

logger = logging.getLogger(__name__)


def create_in_dtypes():
    """
    Create a dictionary of input data types.

    This specifies the dtypes of the input columns, which is necessary for some
    cases where, e.g., a column is always NaN.

    Returns:
        dict: mapping columns names to :mod:`pandas` data types.

    """
    # These measurement codes are used by all four of our measurement variables
    common_codes = (
        "LME",
        "Measured",
        "Measured and Substitute",
        "Other",
        "Substitute",
        "Undetermined",
        "Unknown Code",
        "",
    )
    co2_so2_cats = pd.CategoricalDtype(categories=common_codes, ordered=False)
    nox_cats = pd.CategoricalDtype(
        categories=common_codes + ("Calculated",), ordered=False
    )
    state_cats = pd.CategoricalDtype(
        categories=pc.cems_states.keys(), ordered=False)
    in_dtypes = {
        "state": state_cats,
        "plant_id_eia": "int32",
        "unitid": pd.StringDtype(),
        "operating_time_hours": "float32",
        "gross_load_mw": "float32",
        "steam_load_1000_lbs": "float32",
        "so2_mass_lbs": "float32",
        "so2_mass_measurement_code": co2_so2_cats,
        "nox_rate_lbs_mmbtu": "float32",
        "nox_rate_measurement_code": nox_cats,
        "nox_mass_lbs": "float32",
        "nox_mass_measurement_code": nox_cats,
        "co2_mass_tons": "float32",
        "co2_mass_measurement_code": co2_so2_cats,
        "heat_content_mmbtu": "float32",
        "facility_id": pd.Int32Dtype(),
        "unit_id_epa": pd.Int32Dtype(),
    }
    return in_dtypes


def create_cems_schema():
    """Make an explicit Arrow schema for the EPA CEMS data.

    Make changes in the types of the generated parquet files by editing this
    function.

    Note that parquet's internal representation doesn't use unsigned numbers or
    16-bit ints, so just keep things simple here and always use int32 and
    float32.

    Returns:
        pyarrow.schema: An Arrow schema for the EPA CEMS data.

    """
    int_nullable = partial(pa.field, type=pa.int32(), nullable=True)
    int_not_null = partial(pa.field, type=pa.int32(), nullable=False)
    str_not_null = partial(pa.field, type=pa.string(), nullable=False)
    # Timestamp resolution is hourly, but second is the largest allowed.
    timestamp = partial(pa.field, type=pa.timestamp("s", tz="UTC"), nullable=False)
    float_nullable = partial(pa.field, type=pa.float32(), nullable=True)
    float_not_null = partial(pa.field, type=pa.float32(), nullable=False)
    # (float32 can accurately hold integers up to 16,777,216 so no need for
    # float64)
    dict_nullable = partial(
        pa.field,
        type=pa.dictionary(pa.int8(), pa.string(), ordered=False),
        nullable=True
    )
    return pa.schema([
        dict_nullable("state"),
        int_not_null("plant_id_eia"),
        str_not_null("unitid"),
        timestamp("operating_datetime_utc"),
        float_nullable("operating_time_hours"),
        float_not_null("gross_load_mw"),
        float_nullable("steam_load_1000_lbs"),
        float_nullable("so2_mass_lbs"),
        dict_nullable("so2_mass_measurement_code"),
        float_nullable("nox_rate_lbs_mmbtu"),
        dict_nullable("nox_rate_measurement_code"),
        float_nullable("nox_mass_lbs"),
        dict_nullable("nox_mass_measurement_code"),
        float_nullable("co2_mass_tons"),
        dict_nullable("co2_mass_measurement_code"),
        float_not_null("heat_content_mmbtu"),
        int_nullable("facility_id"),
        int_nullable("unit_id_epa"),
        int_not_null("year"),
    ])


def parse_command_line(argv):
    """
    Parse command line arguments. See the -h option.

    Args:
        argv (str): Command line arguments, including caller filename.

    Returns:
        dict: Dictionary of command line arguments and their parsed values.

    """
    parser = argparse.ArgumentParser(description=__doc__)
    defaults = pudl.workspace.setup.get_defaults()
    parser.add_argument(
        'datapkg',
        type=str,
        help="""Path to the datapackage.json file describing the datapackage
        that contains the CEMS data to be converted.""",
    )
    parser.add_argument(
        '-z',
        '--compression',
        type=str,
        choices=["gzip", "snappy"],
        help="""Compression algorithm to use for Parquet files. Can be either
        'snappy' (much faster but larger files) or 'gzip' (slower but better
        compression). (default: %(default)s).""",
        default='snappy'
    )
    parser.add_argument(
        '-i',
        '--pudl_in',
        type=str,
        help="""Path to the top level datastore directory. (default:
        %(default)s).""",
        default=defaults["pudl_in"],
    )
    parser.add_argument(
        '-o',
        '--pudl_out',
        type=str,
        help="""Path to the pudl output directory. (default: %(default)s).""",
        default=str(defaults["pudl_out"])
    )
    parser.add_argument(
        '-y',
        '--years',
        nargs='+',
        type=int,
        help="""Which years of EPA CEMS data should be converted to Apache
        Parquet format. Default is all available years, ranging from 1995 to
        the present. Note that data is typically incomplete before ~2000.""",
        default=pc.WORKING_PARTITIONS['epacems']['years']
    )
    parser.add_argument(
        '-s',
        '--states',
        nargs='+',
        type=str.upper,
        help="""Which states EPA CEMS data should be converted to Apache
        Parquet format, as a list of two letter US state abbreviations. Default
        is everything: all 48 continental US states plus Washington DC.""",
        default=pc.WORKING_PARTITIONS["epacems"]["states"]
    )
    parser.add_argument(
        '-c',
        '--clobber',
        action='store_true',
        help="""Clobber existing parquet files if they exist. If clobber is not
        included but the parquet directory already exists the _build will
        fail.""",
        default=False)
    arguments = parser.parse_args(argv[1:])
    return arguments


def main():
    """Convert zipped EPA CEMS Hourly data to Apache Parquet format."""
    # Display logged output from the PUDL package:
    logger = logging.getLogger(pudl.__name__)
    log_format = '%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s'
    coloredlogs.install(fmt=log_format, level='INFO', logger=logger)

    args = parse_command_line(sys.argv)

    pudl_settings = pudl.workspace.setup.get_defaults()
    settings = EpaCemsSettings(years=args.years, states=args.states)
    # flow = prefect.Flow("EPACems Parquet", result=FSSpecResult(root_dir=result_cache))
    flow = prefect.Flow("EPACems Parquet")

    _ = EpaCemsPipeline(flow, pudl_settings, settings)


if __name__ == '__main__':
    sys.exit(main())
