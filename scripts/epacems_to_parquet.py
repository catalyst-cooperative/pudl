#!/usr/bin/env python
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

import os
import sys
import argparse
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

assert sys.version_info >= (3, 6)  # require modern python

# This is a hack to make the pudl package importable from within this script,
# even though it isn't in one of the normal site-packages directories where
# Python typically searches.  When we have some real installation/packaging
# happening, this will no longer be necessary.
sys.path.append(os.path.abspath('..'))

IN_DTYPES = {
    'unitid': str,
    'facility_id': str,
    'unit_id_epa': str,
}

OUT_DTYPES = {
    'year': 'uint16',
    'state': 'category',
    'plant_name': 'category',
    'plant_id_eia': 'uint16',
    'unitid': 'category',
    'gross_load_mw': 'float32',
    'steam_load_1000_lbs': 'float32',
    'so2_mass_lbs': 'float32',
    'so2_mass_measurement_code': 'category',
    'nox_rate_lbs_mmbtu': 'float32',
    'nox_rate_measurement_code': 'category',
    'nox_mass_lbs': 'float32',
    'nox_mass_measurement_code': 'category',
    'co2_mass_tons': 'float32',
    'co2_mass_measurement_code': 'category',
    'heat_content_mmbtu': 'float32',
    'facility_id': 'category',
    'unit_id_epa': 'category',
    'operating_datetime': 'datetime64',
    'operating_time_hours': 'float32'
}


def parse_command_line(argv):
    """
    Parse command line arguments. See the -h option.

    :param argv: arguments on the command line must include caller file name.
    """
    from pudl.settings import SETTINGS
    import pudl.constants as pc
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-q',
        '--quiet',
        dest='verbose',
        action='store_false',
        help="Quiet mode. Suppress download progress indicators and warnings.",
        default=True
    )
    parser.add_argument(
        '-d',
        '--datadir',
        type=str,
        help="""Path to the top level datastore directory. (default:
        %(default)s).""",
        default=SETTINGS['data_dir']
    )
    parser.add_argument(
        '-o',
        '--outdir',
        type=str,
        help="""Path to the output directory. (default: %(default)s).""",
        default=os.path.join(SETTINGS['pudl_dir'], 'results',
                             'parquet', 'epacems')
    )
    parser.add_argument(
        '-y',
        '--years',
        nargs='+',
        type=int,
        help="""Which years of EPA CEMS data should be converted to Apache
        Parquet format. Default is all years from 1995 to the present.""",
        default=pc.data_years['epacems']
    )
    parser.add_argument(
        '-s',
        '--states',
        nargs='+',
        help="""Which states EPA CEMS data should be converted to Apache
        Parquet format, as a list of two letter US state abbreviations. Default
        is all 50 US states.""",
        default=pc.cems_states.keys()
    )
    parser.add_argument(
        '-p',
        '--partition_cols',
        nargs='+',
        choices=['year', 'state'],
        help="""Which columns should be used to partition the Apache Parquet
        dataset? (default: %(default)s)""",
        default=['year', 'state']
    )
    parser.add_argument(
        '-c',
        '--compression',
        type=str,
        choices=['gzip', 'snappy'],
        help="""What compression algorithm should be used in the Apache Parquet
        file output? gzip is slower but will result in smaller files. snappy
        is faster but results in larger files.
        dataset? (default: %(default)s)""",
        default='snappy'
    )

    arguments = parser.parse_args(argv[1:])
    return arguments


def downcast_numeric(df, from_dtype, to_dtype):
    """Downcast columns from_dtype to_dtype to save space."""
    to_downcast = df.select_dtypes(include=[from_dtype])
    for col in to_downcast.columns:
        df[col] = pd.to_numeric(to_downcast[col], downcast=to_dtype)
    return df


def year_from_operating_datetime(df):
    """Add a 'year' column based on the year in the operating_datetime."""
    df['year'] = df.operating_datetime.dt.year
    return df


def cems_to_parquet(transformed_df_dicts, outdir=None, schema=None,
                    compression='snappy', partition_cols=('year', 'state')):
    """
    Take transformed EPA CEMS dataframes and output them as Parquet files.

    We need to do a few additional manipulations of the dataframes after they
    have been transformed by PUDL to get them ready for output to the Apache
    Parquet format. Mostly this has to do with ensuring homogeneous data types
    across all of the dataframes, and downcasting to the most efficient data
    type possible for each of them. We also add a 'year' column so that we can
    partition the datset on disk by year as well as state.
    """
    if not schema:
        raise AssertionError("Required Parquet table schema not specified.")
    if not outdir:
        raise AssertionError("Required output directory not specified.")

    for df_dict in transformed_df_dicts:
        for yr_st in df_dict:
            df = df_dict[yr_st]
            print(f'            {yr_st}: {len(df)} records')
            if not df.empty:
                df = (
                    df.astype(IN_DTYPES)
                    .pipe(downcast_numeric,
                          from_dtype='float',
                          to_dtype='float')
                    .pipe(downcast_numeric,
                          from_dtype='int',
                          to_dtype='unsigned')
                    .pipe(year_from_operating_datetime)
                    .astype(OUT_DTYPES)
                )
                pq.write_to_dataset(
                    pa.Table.from_pandas(
                        df, preserve_index=False, schema=schema),
                    root_path=outdir, partition_cols=partition_cols,
                    compression=compression)


def main():
    """Main function controlling flow of the script."""
    import pudl

    args = parse_command_line(sys.argv)

    # Create a pandas dataframe that has the appropriate column types for our
    # Parquet output, so we can use it as a template / schema.
    cems_df_template = pd.DataFrame(columns=OUT_DTYPES.keys())
    cems_df_template = cems_df_template.astype(OUT_DTYPES)
    cems_table = pa.Table.from_pandas(cems_df_template)

    # Use the PUDL EPA CEMS Extract / Transform pipelines to process the
    # original raw data from EPA as needed.
    raw_dfs = pudl.extract.epacems.extract(
        epacems_years=args.years,
        states=args.states,
        verbose=args.verbose
    )
    transformed_dfs = pudl.transform.epacems.transform(
        raw_dfs, verbose=args.verbose)

    # Do a few additional manipulations specific to the Apache Parquet output,
    # and write the resulting files to disk.
    cems_to_parquet(transformed_dfs,
                    outdir=args.outdir,
                    schema=cems_table.schema,
                    compression=args.compression,
                    partition_cols=args.partition_cols)


if __name__ == '__main__':
    sys.exit(main())
