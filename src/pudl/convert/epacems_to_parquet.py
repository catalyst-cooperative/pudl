"""A script for converting the EPA CEMS dataset from gzip to Apache Parquet.

The original EPA CEMS data is available as ~12,000 gzipped CSV files, one for
each month for each state, from 1995 to the present. On disk they take up
about 7.3 GB of space, compressed. Uncompressed it is closer to 100 GB. That's
too much data to work with in memory.

Apache Parquet is a compressed, columnar datastore format, widely used in Big
Data applications. It's an open standard, and is very fast to read from disk.
It works especially well with both Dask dataframes (a parallel / distributed
computing extension of Pandas) and Apache Spark (a cloud based Big Data
processing pipeline system.)

Since pulling 100 GB of data into postgres takes a long time, and working with
that data en masse isn't particularly pleasant on a laptop, this script can be
used to convert the original EPA CEMS data to the more widely usable Apache
Parquet format for use with Dask, either on a multi-core workstation or in an
interactive cloud computing environment like Pangeo.

For more information on working with these systems check out:
 * https://tomaugspurger.github.io/modern-1-intro
 * https://dask.pydata.org
 * https://pangio.io
"""

import argparse
import logging
import pathlib
import sys
from functools import partial

import coloredlogs
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import pudl
import pudl.constants as pc

# Because this is an entry point module for a script, we want to gather all the
# pudl output, not just from this module, hence the pudl.__name__
logger = logging.getLogger(__name__)

IN_DTYPES = {
    "co2_mass_measurement_code": "category",
    "nox_mass_measurement_code": "category",
    "nox_rate_measurement_code": "category",
    "so2_mass_measurement_code": "category",
    "state": "category",
    "unitid": "str",
    # Note: it'd be better to use pandas' nullable integers once this issue is
    # resolved: https://issues.apache.org/jira/browse/ARROW-5379
    # 'facility_id': "Int32",
    # 'unit_id_epa': "Int32",
    'facility_id': "float32",
    'unit_id_epa': "float32",
}


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
    # Timestamp resolution is hourly, but millisecond is the largest allowed.
    timestamp = partial(pa.field, type=pa.timestamp(
        "ms", tz="utc"), nullable=False)
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
        int_not_null("year"),
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
    ])


def year_from_operating_datetime(df):
    """Add a 'year' column based on the year in the operating_datetime.

    Args:
        df (pandas.DataFrame): A DataFrame containing EPA CEMS data.

    Returns:
        pandas.DataFrame: A DataFrame containing EPA CEMS data with a 'year'
        column.

    """
    df['year'] = df.operating_datetime_utc.dt.year
    return df


def _verify_cems_args(data_path, epacems_years, epacems_states):
    """Check that the data packaage has all years and states you want."""
    years = set()
    states = set()
    for file in data_path.iterdir():
        if "epacems" in file.name:
            df_name = file.name[:file.name.find(".")]
            years.add(int(df_name[25:29]))
            states.add(df_name[30:])
    logger.info(f' CEMS package has {years} and {states}')
    for arg_year in epacems_years:
        if arg_year not in years:
            raise AssertionError(
                f'The data packages do not include the requested year: {arg_year}'
            )
    for arg_state in epacems_states:
        if arg_state.lower() not in states:
            raise AssertionError(
                f'The data packages do not include the requested state: {arg_state}'
            )


def epacems_to_parquet(pkg_dir,
                       epacems_years,
                       epacems_states,
                       out_dir,
                       compression='snappy',
                       partition_cols=('year', 'state')):
    """Take transformed EPA CEMS dataframes and output them as Parquet files.

    We need to do a few additional manipulations of the dataframes after they
    have been transformed by PUDL to get them ready for output to the Apache
    Parquet format. Mostly this has to do with ensuring homogeneous data types
    across all of the dataframes, and downcasting to the most efficient data
    type possible for each of them. We also add a 'year' column so that we can
    partition the datset on disk by year as well as state.

    Args:
        pkg_dir (path-like): Path to the directory of the data package which
            contains EPA CEMS. This directory should have a json metadata file
            and a `data` subdirectory containing CSVs.
        epacems_years (list): list of years from which we are trying to read
            CEMs data
        epacems_states (list): list of years from which we are trying to read
            CEMs data
        out_dir (path-like): The directory in which to output the Parquet files
        compression (type?):
        partition_cols (tuple):

    Raises:
        AssertionError: Raised if an output directory is not specified.

    Todo:
        Return to

    """
    if not out_dir:
        raise AssertionError("Required output directory not specified.")

    schema = create_cems_schema()
    data_path = pathlib.Path(pkg_dir, 'data')
    # double check that all of the years you are asking for are actually in
    _verify_cems_args(data_path, epacems_years, epacems_states)
    for file in data_path.iterdir():
        if "epacems" in file.name:
            df_name = file.name[:file.name.find(".")]
            year = df_name[25:29]
            state = df_name[30:]
            # only convert the years and states that you actually want
            if int(year) in epacems_years and state in epacems_states:
                df = pd.read_csv(file, parse_dates=['operating_datetime_utc'])
                logger.info(
                    f"Converted {len(df)} records for {df_name}."
                )
                df = year_from_operating_datetime(df).astype(IN_DTYPES)
                pq.write_to_dataset(
                    pa.Table.from_pandas(
                        df, preserve_index=False, schema=schema),
                    root_path=str(out_dir), partition_cols=list(partition_cols),
                    compression=compression)


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
        '--pkg_dir',
        type=str,
        help="""Path to the directory of the data package which contains EPA
            CEMS. This directory should have a json metadata file and a `data`
            subdirectory containing CSVs.""",
    )
    parser.add_argument(
        '-c',
        '--compression',
        type=str,
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
        default=pc.data_years['epacems']
    )
    parser.add_argument(
        '-s',
        '--states',
        nargs='+',
        type=str.upper,
        help="""Which states EPA CEMS data should be converted to Apache
        Parquet format, as a list of two letter US state abbreviations. Default
        is everything: all 48 continental US states plus Washington DC.""",
        default=pc.cems_states.keys()
    )
    arguments = parser.parse_args(argv[1:])
    return arguments


def main():
    """Convert zipped EPA CEMS Hourly data to Apache Parquet format."""
    # Display logged output from the PUDL package:
    logger = logging.getLogger(pudl.__name__)
    log_format = '%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s'
    coloredlogs.install(fmt=log_format, level='INFO', logger=logger)

    args = parse_command_line(sys.argv)

    pudl_settings = pudl.workspace.setup.derive_paths(
        pudl_in=args.pudl_in, pudl_out=args.pudl_out)

    # Make sure the required input files are available before we go doing a
    # bunch of work cloning the database...
    logger.info("Checking for required EPA CEMS input files...")
    pudl.helpers.verify_input_files(
        ferc1_years=[],
        eia860_years=[],
        eia923_years=[],
        epacems_years=args.years,
        epacems_states=args.states,
        pudl_settings=pudl_settings,
    )

    epacems_to_parquet(pkg_dir=args.pkg_dir,
                       epacems_years=args.years,
                       epacems_states=args.states,
                       out_dir=pathlib.Path(
                           pudl_settings['parquet_dir'], "epacems"),
                       compression=args.compression,
                       partition_cols=('year', 'state'))


if __name__ == '__main__':
    sys.exit(main())
