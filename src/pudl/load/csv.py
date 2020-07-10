"""Functions for loading processed PUDL data tables into CSV files.

Once each set of tables pertaining to a data source have been transformed, we
need to output them into CSV files which will become the data underlying
tabular data resources. Most of these resources contain an entire table. In the
case of larger tables (like EPA CEMS) the data may be partitioned into a
collection of gzipped CSV files which are all part of a single resource group.

These functions are designed to pick up where the transform step leaves off,
taking a dictionary of dataframes and applying a few last alterations that are
necessary only in the context of outputting the data as text based files. These
include converting floatified integer columns into strings with null values,
and appropriately indexing the dataframes as needed.

"""
import logging
import pathlib

import pudl
from pudl import constants as pc

logger = logging.getLogger(__name__)


def dict_dump(transformed_dfs, data_source, datapkg_dir):
    """
    Wrapper for clean_columns_dump that takes a dictionary of DataFrames.

    Args:
        transformed_dfs (dict): A dictionary of DataFrame objects in which
            tables from datasets (keys) correspond to normalized DataFrames of
            values from that table (values)
        data_source (str): The name of the data source we are working with
            (eia923, ferc1, etc.)
        datapkg_dir (path-like): Path to the top level directory for the
            datapackage these CSV files are part of. Will contain a "data"
            directory and a datapackage.json file.

    Returns:
        None

    """
    for resource_name, df in transformed_dfs.items():
        logger.info(
            f"Loading {data_source} {resource_name} dataframe into CSV")
        clean_columns_dump(df, resource_name, datapkg_dir)


def clean_columns_dump(df, resource_name, datapkg_dir):
    """
    Output cleaned data columns to a CSV file.

    Ensures that the id column is set appropriately depending on whether the
    table has a natural primary key or an autoincremnted pseudo-key. Ensures
    that the set of columns in the dataframe to be output are identical to
    those in the corresponding metadata definition. Transforms integer columns
    with NA values into strings for dumping, as appropriate.

    Args:
        resource_name (str): The exact name of the tabular resource which the
            DataFrame df is going to be used to populate. This will be used
            to name the output CSV file, and must match the corresponding
            stored metadata template.
        datapkg_dir (path-like): Path to the datapackage directory that the
            CSV will be part of. Assumes CSV files get put in a "data"
            directory within this directory.
        df (pandas.DataFrame): The dataframe containing the data to be written
            out into CSV for inclusion in a tabular datapackage.

    Returns:
        None

    """
    # fix the order of the columns based on the metadata..
    # grab the table metadata from the mega metadata
    resource = pudl.load.metadata.pull_resource_from_megadata(resource_name)
    # pull the columns from the table schema
    columns = [x['name'] for x in resource['schema']['fields']]
    # there are two types of tables when it comes to indexes and ids...
    # first there are tons of tables that don't use the index as a column
    # these tables don't have an `id` column and we don't want to keep the
    # index when doing df.to_csv()
    if 'id' not in columns:
        keep_index = False
    # there are also a ton of tables that use the `id` column as an auto-
    # autoincrement id/primary key. For these tables, the index will end up
    # as the id column so we want to remove the `id` in the list of columns
    # because while the id column exists in the metadata it isn't in the df
    else:
        columns.remove('id')
        df = df.reset_index(drop=True)
        keep_index = True

    # Now we want to check and make sure the set of columns in the dataframe
    # is the same as the set of columns described in the metadata. If not, we
    # have a problem, and need to report it to the user.
    if set(columns) != set(df.columns):
        set_diff = set(columns).symmetric_difference(set(df.columns))
        raise ValueError(
            f"Columns {set_diff} are not shared between metadata JSON"
            f"descriptor and dataframe for {resource_name}!"
        )

    # Reindex to ensure the index is clean
    df = df.reindex(columns=columns)
    if resource_name in pc.need_fix_inting:
        df = pudl.helpers.fix_int_na(
            df, columns=pc.need_fix_inting[resource_name])
    csv_dump(df, resource_name, keep_index=keep_index, datapkg_dir=datapkg_dir)


def csv_dump(df, resource_name, keep_index, datapkg_dir):
    """Write a dataframe to CSV.

    Set :func:`pandas.DataFrame.to_csv` arguments appropriately depending on
    what data source we're writing out, and then write it out. In practice
    this means adding a .csv to the end of the resource name, and then, if it's
    part of epacems, adding a .gz after that.

    Args:
        df (pandas.DataFrame): The DataFrame to be dumped to CSV.
        resource_name (str): The exact name of the tabular resource which the
            DataFrame df is going to be used to populate. This will be used
            to name the output CSV file, and must match the corresponding
            stored metadata template.
        keep_index (bool): if True, use the "id" column of df as the index
            and output it.
        datapkg_dir (path-like): Path to the top level datapackage directory.

    Returns:
        None

    """
    args = {
        "path_or_buf": pathlib.Path(datapkg_dir, "data", resource_name + ".csv"),
        "index": keep_index,
    }

    if "hourly_emissions_epacems" in resource_name:
        args["path_or_buf"] = pathlib.Path(
            args["path_or_buf"].parent,
            args["path_or_buf"].name + ".gz")
        args["mode"] = "a"
        args["date_format"] = '%Y-%m-%dT%H:%M:%SZ'

    if keep_index:
        args["index_label"] = "id"

    df.to_csv(**args)
