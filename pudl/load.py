"""A module with functions for loading the pudl database tables."""

import pudl.models.glue


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
    tbl = pudl.models.glue.PUDLBase.metadata.tables[table_name]
    with open(csvfile, 'r', encoding = 'utf8') as f:
        postgres_copy.copy_from(f, tbl, engine, columns=tuple(df.columns),
                                format='csv', header=True, delimiter=',')
    if not keep_csv:
        os.remove(csvfile)


def eia860(eia860_transformed_dfs,
           pudl_engine,
           csvdir='',
           keep_csv=True):
    for key, value in eia860_transformed_dfs.items():
        _csv_dump_load(value,
                       key,
                       pudl_engine,
                       csvdir=csvdir,
                       keep_csv=keep_csv)
