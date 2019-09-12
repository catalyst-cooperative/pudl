"""A module with functions for loading the pudl database tables."""

import contextlib
import io
import logging
import os

import pandas as pd

import pudl
import pudl.constants as pc
import pudl.models.entities

logger = logging.getLogger(__name__)


def _csv_dump_load(df, table_name, engine, csvdir='', keep_csv=False):
    """
    Write a dataframe to CSV and load it into postgresql using COPY FROM.

    The fastest way to load a bunch of records is using the database's native
    text file copy function.  This function dumps a given dataframe out to a
    CSV file, and then loads it into the specified table using a sqlalchemy
    wrapper around the postgresql COPY FROM command, called postgres_copy.

    Note that this creates an additional in-memory representation of the data,
    which takes slightly less memory than the DataFrame itself.

    Args:
        df (pandas.DataFrame): The DataFrame which is to be dumped to CSV and
            loaded into the database. All DataFrame columns must have exactly
            the same names as the database fields they are meant to populate,
            and all column data types must be directly compatible with the
            database fields they are meant to populate. Do any cleanup before
            you call this function.
        table_name (str): The exact name of the database table which the
            DataFrame df is going to be used to populate. It will be used both
            to look up an SQLAlchemy table object in the PUDLBase metadata
            object, and to name the CSV file.
        engine (sqlalchemy.engine): SQLAlchemy database engine, which will be
            used to pull the CSV output into the database.
        csvdir (path-like): Path to the directory into which the CSV file should
            be saved, if it's being kept.
        keep_csv (bool): True if the CSV output should be saved after the data
            has been loaded into the database. False if they should be deleted.
            NOTE: If multiple COPYs are done for the same table_name, only
            the last will be retained by keep_csv, which may be unsatisfying.

    Returns:
        None

    """
    with io.StringIO() as f:
        df.to_csv(f, index=False)
        f.seek(0)
        sql_tbl = f'"{table_name}"'
        sql_cols = f"({','.join(df.columns)})"
        sql_flags = "(FORMAT 'csv', HEADER TRUE, DELIMITER ',')"
        copy = f"COPY {sql_tbl} {sql_cols} FROM STDIN {sql_flags}"

        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.copy_expert(copy, f)
        conn.commit()
        cursor.close()


class BulkCopy(contextlib.AbstractContextManager):
    """
    Accumulate several DataFrames, then COPY FROM python to postgresql.

    NOTE: You should use this class to load one table at a time. To load
    different tables, use different instances of BulkCopy.

    Args:
        table_name (str): The exact name of the database table which the
            DataFrame df is going to be used to populate. It will be used both
            to look up an SQLAlchemy table object in the PUDLBase metadata
            object, and to name the CSV file.
        engine (sqlalchemy.engine): SQLAlchemy database engine, which will be
            used to pull the CSV output into the database.
        buffer (int): Size of data to accumulate (in bytes) before actually
            writing the data into postgresql. (Approximate, because we don't
            introspect memory usage 'deeply'). Default 1 GB.
        csvdir (str): Path to the directory into which the CSV file should be
            saved, if it's being kept.
        keep_csv (bool): True if the CSV output should be saved after the data
            has been loaded into the database. False if they should be deleted.
            NOTE: If multiple COPYs are done for the same table_name, only
            the last will be retained by keep_csv, which may be unsatisfying.

    Example:
        >>> with BulkCopy(my_table, my_engine) as p:
                for df in df_generator:
                    p.add(df)

    """

    def __init__(self, table_name, engine, buffer=1024**3,
                 csvdir='', keep_csv=False):
        """Initialize the BulkCopy context manager."""
        self.table_name = table_name
        self.engine = engine
        self.buffer = buffer
        self.keep_csv = keep_csv
        self.csvdir = csvdir
        # Initialize a list to keep the dataframes
        self.accumulated_dfs = []
        self.accumulated_size = 0

    def add(self, df):
        """
        Add a DataFrame to the accumulated list.

        Args:
            df (:mod:`pandas.DataFrame`): The DataFrame to add to the
                accumulated list.

        Returns:
            None

        """
        if not isinstance(df, pd.DataFrame):
            raise AssertionError(
                "Expected dataframe as input."
            )
        df = pudl.helpers.fix_int_na(
            df, columns=pc.need_fix_inting[self.table_name]
        )
        # Note: append to a list here, then do a concat when we spill
        self.accumulated_dfs.append(df)
        self.accumulated_size += sum(df.memory_usage())
        if self.accumulated_size > self.buffer:
            logger.debug(
                f"Copying {len(self.accumulated_dfs)} accumulated dataframes, "
                f"totalling {round(self.accumulated_size / 1024**2)} MB")
            self.spill()

    def _check_names(self):
        expected_colnames = set(self.accumulated_dfs[0].columns.values)
        for df in self.accumulated_dfs:
            colnames = set(df.columns.values)
            if colnames != expected_colnames:
                raise AssertionError(f"""
Column names changed between dataframes. BulkCopy should only be used
with one table at a time, and all columns must be present in all dataframes.
making up the table to be loaded. Symmetric difference between actual and
expected:
{str(colnames.symmetric_difference(expected_colnames))}
            """)

    def spill(self):
        """Spills the accumulated dataframes into postgresql."""
        if self.accumulated_dfs:
            self._check_names()
            if len(self.accumulated_dfs) > 1:
                # Work around https://github.com/pandas-dev/pandas/issues/25257
                all_dfs = pd.concat(self.accumulated_dfs,
                                    copy=False, ignore_index=True, sort=False)
            else:
                all_dfs = self.accumulated_dfs[0]
            logger.info(
                "===================== Dramatic Pause ====================")
            logger.info(
                f"    Loading {len(all_dfs):,} records "
                f"({round(self.accumulated_size/1024**2)} MB) into PUDL.")
            _csv_dump_load(all_dfs, table_name=self.table_name,
                           engine=self.engine, csvdir=self.csvdir,
                           keep_csv=self.keep_csv)
            logger.info(
                "================ Resume Number Crunching ================")
        self.accumulated_dfs = []
        self.accumulated_size = 0

    def close(self):
        """
        Output the accumulated tabular data to disk.

        Todo:
            Incomplete docstring.

        """
        self.spill()

    def __exit__(self, exception_type, exception_value, traceback):
        """Attempt to exit cleanly, writing any accumulated data to disk."""
        self.close()


class BulkCopyPkg(contextlib.AbstractContextManager):
    """
    Accumulate several DataFrames, then COPY FROM pandas to a CSV.

    NOTE: You shoud use this class to load one table at a time. To load
    different tables, use different instances of BulkCopy.

    Args:
        table_name (str): The exact name of the database table which the
            DataFrame df is going to be used to populate. It will be used both
            to look up an SQLAlchemy table object in the PUDLBase metadata
            object, and to name the CSV file.
        engine (sqlalchemy.engine): SQLAlchemy database engine, which will be
            used to pull the CSV output into the database.
        buffer (int): Size of data to accumulate (in bytes) before actually
            writing the data into postgresql. (Approximate, because we don't
            introspect memory usage 'deeply'). Default 1 GB.

    Example:
        >>> with BulkCopy(my_table, my_engine) as p:
                for df in df_generator:
                    p.add(df)

    """

    def __init__(self, table_name, pkg_dir, buffer=1024**3):
        """Initialize the BulkCopy context manager."""
        self.table_name = table_name
        self.buffer = buffer
        self.pkg_dir = pkg_dir
        # Initialize a list to keep the dataframes
        self.accumulated_dfs = []
        self.accumulated_size = 0

    def add(self, df):
        """Adds a DataFrame to the accumulated list.

        Args:
            df (pandas.DataFrame): The DataFrame to add to the accumulated list.

        Returns:
            None
        """
        if not isinstance(df, pd.DataFrame):
            raise AssertionError(
                "Expected dataframe as input."
            )
        df = pudl.helpers.fix_int_na(
            df, columns=pc.need_fix_inting[self.table_name]
        )
        # Note: append to a list here, then do a concat when we spill
        self.accumulated_dfs.append(df)
        self.accumulated_size += sum(df.memory_usage())
        if self.accumulated_size > self.buffer:
            logger.debug(
                f"Copying {len(self.accumulated_dfs)} accumulated dataframes, "
                f"totalling {round(self.accumulated_size / 1024**2)} MB")
            self.spill()

    def _check_names(self):
        expected_colnames = set(self.accumulated_dfs[0].columns.values)
        for df in self.accumulated_dfs:
            colnames = set(df.columns.values)
            if colnames != expected_colnames:
                raise AssertionError(f"""
Column names changed between dataframes. BulkCopy should only be used
with one table at a time, and all columns must be present in all dataframes.
making up the table to be loaded. Symmetric difference between actual and
expected:
{str(colnames.symmetric_difference(expected_colnames))}
            """)

    def spill(self):
        """Spill the accumulated dataframes into the datapackage."""
        if self.accumulated_dfs:
            self._check_names()
            if len(self.accumulated_dfs) > 1:
                # Work around https://github.com/pandas-dev/pandas/issues/25257
                all_dfs = pd.concat(self.accumulated_dfs,
                                    copy=False, ignore_index=True, sort=False)
            else:
                all_dfs = self.accumulated_dfs[0]
            logger.info(
                "===================== Dramatic Pause ====================")
            logger.info(
                f"    Loading {len(all_dfs):,} records "
                f"({round(self.accumulated_size/1024**2)} MB) into PUDL."
            )

            # csv_dump(all_dfs, self.table_name, True, self.pkg_dir)
            clean_columns_dump(self.table_name, self.pkg_dir, all_dfs)
            logger.info(
                "================ Resume Number Crunching ================")
        self.accumulated_dfs = []
        self.accumulated_size = 0

    def close(self):
        """
        Output the accumulated tabular data to disk.

        Todo:
            Return to

        """
        self.spill()

    def __exit__(self, exception_type, exception_value, traceback):
        """Attempt to exit cleanly, writing any accumulated data to disk."""
        self.close()


def dict_dump_load(transformed_dfs,
                   data_source,
                   pudl_engine,
                   need_fix_inting=pc.need_fix_inting,
                   csvdir='',
                   keep_csv=False):
    """Wrapper for _csv_dump_load for each data source.

    Args:
        transformed_dfs (dict): A dictionary of DataFrame objects in which
            tables from datasets (keys) correspond to normalized DataFrames of
            values from that table (values)
        data_source (str): The name of the datasource we are working with.
        pudl_engine (sqlalchemy.engine): SQLAlchemy database engine, which will
            be used to pull the CSV output into the database.
        need_fix_inting (dict): A dictionary containing table names (keys) and
            column names for each table that need their null values cleaned up
            (values).
        csvdir (path-like): Path to the directory into which the CSV file should
            be saved, if it's being kept.
        keep_csv (bool): True if the CSV output should be saved after the data
            has been loaded into the database. False if they should be deleted.
            NOTE: If multiple COPYs are done for the same table_name, only
            the last will be retained by keep_csv, which may be unsatisfying.

    Returns:
        None
    """
    for table_name, df in transformed_dfs.items():
        if table_name != "hourly_emissions_epacems":
            logger.info(
                f"Loading {data_source} {table_name} dataframe into PUDL DB")
        if table_name in list(need_fix_inting.keys()):
            df = pudl.helpers.fix_int_na(
                df, columns=pc.need_fix_inting[table_name])

        _csv_dump_load(df,
                       table_name,
                       pudl_engine,
                       csvdir=csvdir,
                       keep_csv=keep_csv)


###############################################################################
# Packaging specific load step
###############################################################################

def csv_dump(df, table_name, keep_index, pkg_dir):
    """Writes a dataframe to CSV and loads it into postgresql using COPY FROM.

    The fastest way to load a bunch of records is using the database's native
    text file copy function.  This function dumps a given dataframe out to a
    CSV file, and then loads it into the specified table using a sqlalchemy
    wrapper around the postgresql COPY FROM command, called postgres_copy.

    Note that this creates an additional in-memory representation of the data,
    which takes slightly less memory than the DataFrame itself.

    Args:
        df (pandas.DataFrame): The DataFrame which is to be dumped to CSV and
            loaded into the database. All DataFrame columns must have exactly
            the same names as the database fields they are meant to populate,
            and all column data types must be directly compatible with the
            database fields they are meant to populate. Do any cleanup before
            you call this function.
        table_name (str): The exact name of the database table which the
            DataFrame df is going to be used to populate. It will be used both
            to look up an SQLAlchemy table object in the PUDLBase metadata
            object, and to name the CSV file.
        keep_index (bool): Should the output CSV file contain an index?
        pkg_dir (path-like): Path to the directory into which the CSV file
            should be saved, if it's being kept.

    Returns:
        None

    """
    outfile = os.path.join(pkg_dir, 'data', table_name + '.csv')
    if table_name == 'hourly_emissions_epacems':
        df.to_csv(path_or_buf=outfile, index=True, index_label='id',
                  date_format='%Y-%m-%dT%H:%M:%S')
        return
    if keep_index:
        df.to_csv(path_or_buf=outfile, index=True, index_label='id')
    else:
        df.to_csv(path_or_buf=outfile, index=False)


def clean_columns_dump(table_name, pkg_dir, df):
    """
    Output the cleaned columns to a CSV file.

    Args:
        table_name (str):
        pkg_dir (path-like):
        df (pandas.DataFrame):

    Returns:
        None

    Todo:
        Incomplete Docstring

    """
    # fix the order of the columns based on the metadata..
    # grab the table metadata from the mega metadata
    resource = pudl.helpers.pull_resource_from_megadata(table_name)
    # pull the columns from the table schema
    columns = [x['name'] for x in resource['schema']['fields']]
    # there are two types of tables when it comes to indexes and ids...
    # first there are tons of tables that don't use the index as a column
    # these tables don't have an `id` column and we don't want to keept the
    # index when doing df.to_csv()
    if 'id' not in columns:
        df = df.reindex(columns=columns)
        csv_dump(df,
                 table_name,
                 keep_index=False,
                 pkg_dir=pkg_dir)
    # there are also a ton of tables that use the `id` column as an auto-
    # autoincrement id/primary key. For these tables, the index will end up
    # as the id column so we want to remove the `id` in the list of columns
    # because while the id column exists in the metadata it isn't in the df
    # We also want to reindex in order to ensure the index is clean
    else:
        columns.remove('id')
        df = df.reset_index(drop=True)
        df = df.reindex(columns=columns)
        csv_dump(df,
                 table_name,
                 keep_index=True,
                 pkg_dir=pkg_dir)


def dict_dump(transformed_dfs,
              data_source,
              need_fix_inting=pc.need_fix_inting,
              pkg_dir=''):
    """
    Wrapper for _csv_dump for each data source.

    Args:
        transformed_dfs (dict): A dictionary of DataFrame objects in which
            tables from datasets (keys) correspond to normalized DataFrames of
            values from that table (values)
        datasource (str): The name of the datasource we are working with.
        need_fix_inting (dict): A dictionary containing table names (keys) and
            column names for each table that need their null values cleaned up
            (values).
        pkg_dir (path-like): Path to the directory into which the CSV file
            should be saved, if it's being kept.

    Returns:
        None

    """
    for table_name, df in transformed_dfs.items():
        if table_name != "hourly_emissions_epacems":
            logger.info(
                f"Loading {data_source} {table_name} dataframe into CSV")
        if table_name in list(need_fix_inting.keys()):
            df = pudl.helpers.fix_int_na(
                df, columns=pc.need_fix_inting[table_name])
        clean_columns_dump(table_name, pkg_dir, df)
