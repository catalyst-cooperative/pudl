"""A module with functions for loading the pudl database tables."""

import pudl.models.entities
import pudl.transform.pudl
import pudl.constants as pc
import numpy as np


def _csv_dump_load(df, table_name, engine, csvdir='', keep_csv=True):
    """
    Write a dataframe to CSV and load it into postgresql using COPY FROM.

    The fastest way to load a bunch of records is using the database's native
    text file copy function.  This function dumps a given dataframe out to a
    CSV file, and then loads it into the specified table using a sqlalchemy
    wrapper around the postgresql COPY FROM command, called postgres_copy.

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
        csvdir (str): Path to the directory into which the CSV files should be
            output (and saved, if they are being kept).
        keep_csv (bool): True if the CSV output should be saved after the data
            has been loaded into the database. False if they should be deleted.

    Returns: Nothing.
    """
    import postgres_copy
    import os

    csvfile = os.path.join(csvdir, table_name + '.csv')
    df.to_csv(csvfile, index=False)
    tbl = pudl.models.entities.PUDLBase.metadata.tables[table_name]
    with open(csvfile, 'r', encoding='utf8') as f:
        postgres_copy.copy_from(f, tbl, engine, columns=tuple(df.columns),
                                format='csv', header=True, delimiter=',')
    # TODO: For the CEMS, this function is called many times, but the CSV
    # filename is the same. If you want to save all, that will be unsatisfying.
    if not keep_csv:
        os.remove(csvfile)


def _fix_int_cols(table_to_fix,
                  transformed_dct,
                  need_fix_inting=pc.need_fix_inting,
                  verbose=True):
    """
    Run fix_int_na on multiple columns per table.

    There are some tables that have one table that needs fix_int_naing, while
    some tables have a few columns.

    Args:
        table_to_fix: the name of the table that needs fixing.
        transformed_dct: dictionary of tables with transformed dfs.
        need_fix_inting: dictionary of tables with columns that need fixing.
    """
    for column in need_fix_inting[table_to_fix]:
        if verbose:
            print("        fixing {} column".format(column))
        transformed_dct[table_to_fix][column] = \
            pudl.transform.pudl.fix_int_na(
                transformed_dct[table_to_fix][column])

def dump_load_accum(df, table_name, engine, buffer = 1e6):
    """UNTESTED:
    Accumulate the tables in a list, then COPY them when they've hit
    buffer count.

    Uses function's lst attribute (see https://www.python.org/dev/peps/pep-0232/)

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
        csvdir (str): Path to the directory into which the CSV files should be
            output (and saved, if they are being kept).
        keep_csv (bool): True if the CSV output should be saved after the data
            has been loaded into the database. False if they should be deleted.
    """
    row_count = sum([x.shape[0] for x in _csv_dump_load_accum.lst])
    if row_count < buffer:
        dump_load_accum.lst.append(df)
    else:
        all_dfs = pd.concat(_csv_dump_load_accum.lst, copy=False, ignore_index=True)
        _csv_dump_load(all_dfs, table_name=table_name, engine=engine, keep_csv=False)
        dump_load_accum.lst = []
# Initialize lst attributed
dump_load_accum.lst = []


def dict_dump_load(transformed_dfs,
                   data_source,
                   pudl_engine,
                   need_fix_inting=pc.need_fix_inting,
                   verbose=True,
                   csvdir='',
                   keep_csv=True):
    """
    Wrapper for _csv_dump_load for each data source.
    """
    if verbose:
        print("Loading tables from {} into PUDL:".format(data_source))
    for table_name, df in transformed_dfs.items():
        if verbose and table_name != "hourly_emissions_epacems":
            print("    {}...".format(table_name))
        if table_name in list(need_fix_inting.keys()):
            _fix_int_cols(table_name,
                          transformed_dfs,
                          need_fix_inting=pc.need_fix_inting,
                          verbose=verbose)
        _csv_dump_load(df,
                       table_name,
                       pudl_engine,
                       csvdir=csvdir,
                       keep_csv=keep_csv)
