"""
Tools for extracting data from the FERC Form 1 FoxPro database for use in PUDL.

FERC distributes the annual responses to Form 1 as binary FoxPro database
files. This format is no longer widely supported, and so our first challenge in
accessing the Form 1 data is to convert it into a modern format. In addition,
FERC distributes one database for each year, and these databases are not
explicitly linked together. Over time the structure has changed as new tables
and fields have been added. In order to be able to use the data to do analyses
across many years, we need to bring all of it into a unified structure. However
it appears that these changes are only entirely additive -- the most recent
versions of the DB contain all the tables and fields that existed in earlier
versions.

PUDL uses the most recently released year of data as a template, and infers the
structure of the FERC Form 1 database based on the strings embedded within the
binary files, pulling out the names of tables and their constituent columns.
The structure of the database is also informed by information we found on the
FERC website, including a mapping between the table names, DBF file names,
and the pages of the Form 1 (add link to file, which should distributed with
the docs) that the data was gathered from, as well as a diagram of the
structure of the database as it existed in 2015 (add link/embed image).

Using this inferred structure PUDL creates an SQLite database mirroring the
FERC database using :mod:`sqlalchemy`. Then we use a python package called
`dbfread <https://dbfread.readthedocs.io/en/latest/>` to extract the data from
the DBF tables, and insert it virtually unchanged into the SQLite database.
However, we do compile a master table of the all the respondent IDs and
respondent names, which all the other tables refer to. Unlike the other tables,
this table has no ``report_year`` and so it represents a merge of all the years
of data. In the event that the name associated with a given respondent ID has
changed over time, we retain the most recently reported name.

Ths SQLite based compilation of the original FERC Form 1 databases can
accommodate all 100+ tables from all the published years of data (beginning in
1994). Including all the data through 2017, the database takes up more than
7GB of disk space. However, almost 90% of that "data" is embeded binary files
in two tables. If those tables are excluded, the database is less than 800MB
in size.

The process of cloning the FERC Form 1 database(s) is coordinated by a script
called ``ferc1_to_sqlite`` implemented in :mod:`pudl.convert.ferc1_to_sqlite`
which is controlled by a YAML file. See the example file distributed with the
package **here** (link!).

Once the cloned SQLite database has been created, we use it as an input into
the PUDL ETL pipeline, and we extract a small subset of the available tables
for further processing and integration with other data sources like the EIA 860
and EIA 923.

"""
import logging
import os.path
import re
import string

import dbfread
import pandas as pd
import sqlalchemy as sa

import pudl
import pudl.constants as pc
import pudl.workspace.datastore as datastore

logger = logging.getLogger(__name__)


def drop_tables(engine):
    """Drop all FERC Form 1 tables from the SQLite database.

    Creates an sa.schema.MetaData object reflecting the structure of the
    database that the passed in ``engine`` refers to, and uses that schema to
    drop all existing tables.

    Todo:
        Treat DB connection as a context manager (with/as).

    Args:
        engine (sa.engine.Engine): An SQL Alchemy SQLite database Engine
            pointing at an exising SQLite database to be deleted.

    Returns:
        None

    """
    md = sa.MetaData(bind=engine)
    md.reflect(engine)
    md.drop_all(engine)
    conn = engine.connect()
    conn.execute("VACUUM")
    conn.close()


def add_sqlite_table(table_name, sqlite_meta, dbc_map, data_dir,
                     refyear=max(pc.working_years['ferc1']),
                     bad_cols=()):
    """Adds a new Table to the FERC Form 1 database schema.

    Creates a new sa.Table object named ``table_name`` and add it to the
    database schema contained in ``sqlite_meta``. Use the information in the
    dictionary ``dbc_map`` to translate between the DBF filenames in the
    datastore (e.g. ``F1_31.DBF``), and the full name of the table in the
    FoxPro database (e.g. ``f1_fuel``) and also between truncated column
    names extracted from that DBF file, and the full column names extracted
    from the DBC file. Read the column datatypes out of each DBF file and use
    them to define the columns in the new Table object.

    Args:
        table_name (str): The name of the new table to be added to the
            database schema.
        sqlite_meta (sa.schema.MetaData): The database schema to which the
            newly defined Table will be added.
        dbc_map (dict): A dictionary of dictionaries
        bad_cols (iterable of 2-tuples): A list or other iterable containing
            pairs of strings of the form (table_name, column_name), indicating
            columns (and their parent tables) which should *not* be cloned
            into the SQLite database for some reason.
    """
    # Create the new table object
    new_table = sa.Table(table_name, sqlite_meta)
    ferc1_dbf = dbfread.DBF(
        get_dbf_path(table_name, refyear, data_dir=data_dir))

    # Add Columns to the table
    for field in ferc1_dbf.fields:
        if field.name == '_NullFlags':
            continue
        col_name = dbc_map[table_name][field.name]
        if (table_name, col_name) in bad_cols:
            continue
        col_type = pc.dbf_typemap[field.type]
        if col_type == sa.String:
            col_type = sa.String(length=field.length)
        new_table.append_column(sa.Column(col_name, col_type))

    col_names = [c.name for c in new_table.columns]

    if table_name == 'f1_respondent_id':
        new_table.append_constraint(
            sa.PrimaryKeyConstraint(
                'respondent_id', sqlite_on_conflict='REPLACE'
            )
        )

    if (('respondent_id' in col_names) and (table_name != 'f1_respondent_id')):
        new_table.append_constraint(
            sa.ForeignKeyConstraint(
                columns=['respondent_id', ],
                refcolumns=['f1_respondent_id.respondent_id']
            )
        )


def dbc_filename(year, data_dir):
    """Given a year, returns the path to the master FERC Form 1 .DBC file.

    Args:
        year (int): The year that we're trying to read data for

    Returns:
        str: the file path to the master FERC Form 1 .DBC file for the year
    """
    ferc1_path = datastore.path('ferc1', data_dir=data_dir,
                                year=year, file=False)
    return os.path.join(ferc1_path, 'F1_PUB.DBC')


def get_strings(filename, min_length=4):
    """
    Extract printable strings from a binary and return them as a generator.

    This is meant to emulate the Unix "strings" command, for the purposes of
    grabbing database table and column names from the F1_PUB.DBC file that is
    distributed with the FERC Form 1 data.

    Args:
        filename (str): the name of the DBC file from which to extract strings
        min_length (int): the minimum number of consecutive printable
            characters that should be considered a meaningful string and
            extracted.

    Yields:
        str: result

    Todo:
        Zane revisit

    """
    with open(filename, errors="ignore") as f:
        result = ""
        for c in f.read():
            if c in string.printable:
                result += c
                continue
            if len(result) >= min_length:
                yield result
            result = ""
        if len(result) >= min_length:  # catch result at EOF
            yield result


def get_dbc_map(year, data_dir, min_length=4):
    """
    Extract names of all tables and fields from a FERC Form 1 DBC file.

    Read the DBC file associated with the FERC Form 1 database for the given
    ``year``, and extract all printable strings longer than ``min_lengh``.
    Select those strings that appear to be database table names, and their
    associated field for use in re-naming the truncated column names extracted
    from the corresponding DBF files (those names are limited to having only 10
    characters in their names.)

    For more info see: https://github.com/catalyst-cooperative/pudl/issues/288

    Todo:
        Ideally this routine shouldn't refer to any particular year of data,
        but right now it depends on the ferc1_dbf2tbl dictionary, which was
        generated from the 2015 Form 1 database.

    Args:
        year (int): The year of data from which the database table and column
            names are to be extracted. Typically this is expected to be the
            most recently available year of FERC Form 1 data.
        data_dir (str): A string representing the full path to the top level of
            the PUDL datastore containing the FERC Form 1 data to be used.
        min_length (int): The minimum number of consecutive printable
            characters that should be considered a meaningful string and
            extracted.

    Returns:
        dict: a dictionary whose keys are the long table names extracted
        from the DBC file, and whose values are lists of pairs of values,
        the first of which is the full name of each field in the table with
        the same name as the key, and the second of which is the truncated
        (<=10 character) long name of that field as found in the DBF file.


    """
    # Extract all the strings longer than "min" from the DBC file
    dbc_strings = list(
        get_strings(dbc_filename(year, data_dir), min_length=min_length)
    )

    # Get rid of leading & trailing whitespace in the strings:
    dbc_strings = [s.strip() for s in dbc_strings]

    # Get rid of all the empty strings:
    dbc_strings = [s for s in dbc_strings if s != '']

    # Collapse all whitespace to a single space:
    dbc_strings = [re.sub(r'\s+', ' ', s) for s in dbc_strings]

    # Pull out only strings that begin with Table or Field
    dbc_strings = [s for s in dbc_strings if re.match('(^Table|^Field)', s)]

    # Split strings by whitespace, and retain only the first two elements.
    # This eliminates some weird dangling junk characters
    dbc_strings = [' '.join(s.split()[:2]) for s in dbc_strings]

    # Remove all of the leading Field keywords
    dbc_strings = [re.sub('Field ', '', s) for s in dbc_strings]

    # Join all the strings together (separated by spaces) and then split the
    # big string on Table, so each string is now a table name followed by the
    # associated field names, separated by spaces
    dbc_table_strings = ' '.join(dbc_strings).split('Table ')

    # strip leading & trailing whitespace from the lists
    # and get rid of empty strings:
    dbc_table_strings = [s.strip() for s in dbc_table_strings if s != '']

    # Create a dictionary using the first element of these strings (the table
    # name) as the key, and the list of field names as the values, and return
    # it:
    tf_dict = {}
    for table_string in dbc_table_strings:
        table_and_fields = table_string.split()
        tf_dict[table_and_fields[0]] = table_and_fields[1:]

    dbc_map = {}
    for table in pc.ferc1_tbl2dbf:
        dbf_path = get_dbf_path(table, year, data_dir=data_dir)
        if os.path.isfile(dbf_path):
            dbf_fields = dbfread.DBF(dbf_path).field_names
            dbf_fields = [f for f in dbf_fields if f != '_NullFlags']
            dbc_map[table] = \
                {k: v for k, v in zip(dbf_fields, tf_dict[table])}
            assert len(tf_dict[table]) == len(dbf_fields)

    # Insofar as we are able, make sure that the fields match each other
    for k in dbc_map:
        for sn, ln in zip(dbc_map[k].keys(), dbc_map[k].values()):
            assert ln[:8] == sn.lower()[:8]

    return dbc_map


def define_sqlite_db(sqlite_meta, dbc_map, data_dir,
                     tables=pc.ferc1_tbl2dbf,
                     refyear=max(pc.working_years['ferc1']),
                     bad_cols=()):
    """Defines a FERC Form 1 DB structure in a given SQLAlchemy MetaData object.

    Given a template from an existing year of FERC data, and a list of target
    tables to be cloned, convert that information into table and column names,
    and data types, stored within a SQLAlchemy MetaData object. Use that
    MetaData object (which is bound to the SQLite database) to create all the
    tables to be populated later.

    Args:
        sqlite_meta (sa.MetaData): A SQLAlchemy MetaData object which is bound
            to the FERC Form 1 SQLite database.
        dbc_map (dict of dicts): A dictionary of dictionaries, of the kind
            returned by get_dbc_map(), describing the table and column names
            stored within the FERC Form 1 FoxPro database files.
        data_dir (str): A string representing the full path to the top level of
            the PUDL datastore containing the FERC Form 1 data to be used.
        tables (iterable of strings): List or other iterable of FERC database
            table names that should be included in the database being defined.
            e.g. 'f1_fuel' and 'f1_steam'
        refyear (integer): The year of the FERC Form 1 DB to use as a template
            for creating the overall multi-year database schema.
        bad_cols (iterable of 2-tuples): A list or other iterable containing
            pairs of strings of the form (table_name, column_name), indicating
            columns (and their parent tables) which should *not* be cloned
            into the SQLite database for some reason.

    Returns:
        None: the effects of the function are stored inside sqlite_meta

    """
    for table in tables:
        add_sqlite_table(table, sqlite_meta, dbc_map,
                         refyear=refyear,
                         bad_cols=bad_cols,
                         data_dir=data_dir)

    sqlite_meta.create_all()


def get_dbf_path(table, year, data_dir):
    """Given a year and table name, returns the path to its datastore DBF file.

    Args:
        table (string): The name of one of the FERC Form 1 data tables. For
            example 'f1_fuel' or 'f1_steam'
        year (int): The year whose data you wish to find.
        data_dir (str): A string representing the full path to the top level of
            the PUDL datastore containing the FERC Form 1 data to be used.

    Returns:
        str: dbf_path, a (hopefully) OS independent path including the
        filename of the DBF file corresponding to the requested year and
        table name.
    """
    dbf_name = pc.ferc1_tbl2dbf[table]
    ferc1_dir = datastore.path(
        'ferc1', year=year, file=False, data_dir=data_dir)
    dbf_path = os.path.join(ferc1_dir, f"{dbf_name}.DBF")
    return dbf_path


class FERC1FieldParser(dbfread.FieldParser):
    """A custom DBF parser to deal with bad FERC Form 1 data types."""

    def parseN(self, field, data):  # noqa: N802
        """
        Augments the Numeric DBF parser to account for bad FERC data.

        There are a small number of bad entries in the backlog of FERC Form 1
        data. They take the form of leading/trailing zeroes or null characters
        in supposedly numeric fields, and occasionally a naked '.'

        Accordingly, this custom parser strips leading and trailing zeros and
        null characters, and replaces a bare '.' character with zero, allowing
        all these fields to be cast to numeric values.

        Args:
            self ():
            field ():
            data ():

        Todo:
            Zane revisit
        """
        # Strip whitespace, null characters, and zeroes
        data = data.strip().strip(b'*\x00').lstrip(b'0')
        # Replace bare periods (which are non-numeric) with zero.
        if data == b'.':
            data = b'0'
        return super(FERC1FieldParser, self).parseN(field, data)


def get_raw_df(table, dbc_map, data_dir,
               years=pc.data_years['ferc1']):
    """Combines several years of a given FERC Form 1 DBF table into a dataframe.

    Args:
        table (string): The name of the FERC Form 1 table from which data is
            read.
        dbc_map (dict of dicts): A dictionary of dictionaries, of the kind
            returned by get_dbc_map(), describing the table and column names
            stored within the FERC Form 1 FoxPro database files.
        data_dir (str): A string representing the full path to the top level of
            the PUDL datastore containing the FERC Form 1 data to be used.
        min_length (int): The minimum number of consecutive printable
        years (list): The range of years to be combined into a single DataFrame.

    Returns:
        pandas.DataFrame: A DataFrame containing several years of FERC Form 1
        data for the given table.
    """
    dbf_name = pc.ferc1_tbl2dbf[table]

    raw_dfs = []
    for yr in years:
        ferc1_dir = datastore.path(
            'ferc1', year=yr, file=False, data_dir=data_dir)
        dbf_path = os.path.join(ferc1_dir, f"{dbf_name}.DBF")

        if os.path.exists(dbf_path):
            new_df = pd.DataFrame(
                iter(dbfread.DBF(dbf_path,
                                 encoding='latin1',
                                 parserclass=FERC1FieldParser)))
            raw_dfs = raw_dfs + [new_df, ]

    if raw_dfs:
        return (
            pd.concat(raw_dfs, sort=True).
            drop('_NullFlags', axis=1, errors='ignore').
            rename(dbc_map[table], axis=1)
        )


def dbf2sqlite(tables, years, refyear, pudl_settings, bad_cols=()):
    """Clone the FERC Form 1 Databsae to SQLite.

    Args:
        tables (iterable): What tables should be cloned?
        years (iterable): Which years of data should be cloned?
        refyear (int): Which database year to use as a template.
        pudl_settings (dict): Dictionary containing paths and database URLs
            used by PUDL.
        bad_cols (iterable of tuples): A list of (table, column) pairs
            indicating columns that should be skipped during the cloning
            process. Both table and column are strings in this case, the
            names of their respective entities within the database metadata.

    Returns:
        None

    """
    # Read in the structure of the DB, if it exists
    logger.info("Dropping the old FERC Form 1 SQLite DB if it exists.")
    sqlite_engine = sa.create_engine(pudl_settings["ferc1_db"])
    try:
        # So that we can wipe it out
        pudl.helpers.drop_tables(sqlite_engine)
    except sa.exc.OperationalError:
        pass

    # And start anew
    sqlite_engine = sa.create_engine(pudl_settings["ferc1_db"])
    sqlite_meta = sa.MetaData(bind=sqlite_engine)

    # Get the mapping of filenames to table names and fields
    logger.info(f"Creating a new database schema based on {refyear}.")
    dbc_map = get_dbc_map(refyear, data_dir=pudl_settings['data_dir'])
    define_sqlite_db(sqlite_meta, dbc_map, tables=tables,
                     refyear=refyear, bad_cols=bad_cols,
                     data_dir=pudl_settings['data_dir'])

    for table in tables:
        logger.info(f"Pandas: reading {table} into a DataFrame.")
        new_df = get_raw_df(table, dbc_map, years=years,
                            data_dir=pudl_settings['data_dir'])
        # Because this table has no year in it, there would be multiple
        # definitions of respondents if we didn't drop duplicates.
        if table == 'f1_respondent_id':
            new_df = new_df.drop_duplicates(
                subset='respondent_id', keep='last')
        n_recs = len(new_df)
        logger.debug(f"    {table}: N = {n_recs}")
        # Only try and load the table if there are some actual records:
        if n_recs <= 0:
            continue

        # Write the records out to the SQLite database, and make sure that
        # the inferred data types are being enforced during loading.
        # if_exists='append' is being used because we defined the tables
        # above, but left them empty. Becaue the DB is reset at the beginning
        # of the function, this shouldn't ever result in duplicate records.
        coltypes = {col.name: col.type for col in sqlite_meta.tables[table].c}
        logger.info(f"SQLite: loading {n_recs} rows into {table}.")
        new_df.to_sql(table, sqlite_engine,
                      if_exists='append', chunksize=100000,
                      dtype=coltypes, index=False)


###########################################################################
# Functions for extracting ferc1 tables from SQLite to PUDL
###########################################################################

def get_ferc1_meta(pudl_settings):
    """Grab the FERC1 db metadata and check for tables."""
    # Connect to the local SQLite DB and read its structure.
    ferc1_engine = sa.create_engine(pudl_settings["ferc1_db"])
    ferc1_meta = sa.MetaData(bind=ferc1_engine)
    ferc1_meta.reflect()
    if not ferc1_meta.tables:
        raise AssertionError(
            f"No FERC Form 1 tables found. Is the SQLite DB initialized?"
        )
    return ferc1_meta


def extract(ferc1_tables=pc.ferc1_pudl_tables,
            ferc1_years=pc.working_years['ferc1'],
            pudl_settings=None):
    """Coordinates the extraction of all FERC Form 1 tables into PUDL.

    Args:
        ferc1_tables (iterable of strings): List of the FERC 1 database tables
            to be loaded into PUDL. These are the names of the tables in the
            PUDL database, not the FERC Form 1 database.
        ferc1_years (iterable of ints): List of years for which FERC Form 1
            data should be loaded into PUDL. Note that not all years for which
            FERC data is available may have been integrated into PUDL yet.

    Returns:
        dict: A dictionary of pandas DataFrames, with the names of PUDL
        database tables as the keys. These are the raw unprocessed dataframes,
        reflecting the data as it is in the FERC Form 1 DB, for passing off to
        the data tidying and cleaning fuctions found in the
        pudl.transform.ferc1 module.

    Raises:
        ValueError: If the year is not in the list of years for which FERC data
            is available
        ValueError: If the year is not in the list of working FERC years
        ValueError: If the FERC table requested is not integrated into PUDL
        AssertionError: If no ferc1_meta tables are found

    """
    if (not ferc1_tables) or (not ferc1_years):
        return {}

    for year in ferc1_years:
        if year not in pc.data_years['ferc1']:
            raise ValueError(
                f"FERC Form 1 data from the year {year} was requested but is "
                f"not available. The years for which data is available are: "
                f"{' '.join(pc.data_years['ferc1'])}."
            )
        if year not in pc.working_years['ferc1']:
            raise ValueError(
                f"FERC Form 1 data from the year {year} was requested but it "
                f"has not yet been integrated into PUDL. "
                f"If you'd like to contribute the necessary cleaning "
                f"functions, come find us on GitHub: "
                f"{pudl.__downloadurl__}"
                f"For now, the years which PUDL has integrated are: "
                f"{' '.join(pc.working_years['ferc1'])}."
            )
    for table in ferc1_tables:
        if table not in pc.ferc1_pudl_tables:
            raise ValueError(
                f"FERC Form 1 table {table} was requested but it has not yet "
                f"been integreated into PUDL. Heck, it might not even exist! "
                f"If you'd like to contribute the necessary cleaning "
                f"functions, come find us on GitHub: "
                f"{pudl.__downloadurl__}"
                f"For now, the tables which PUDL has integrated are: "
                f"{' '.join(pc.ferc1_pudl_tables)}"
            )

    ferc1_meta = get_ferc1_meta(pudl_settings)

    ferc1_extract_functions = {
        "fuel_ferc1": fuel,
        "plants_steam_ferc1": plants_steam,
        "plants_small_ferc1": plants_small,
        "plants_hydro_ferc1": plants_hydro,
        "plants_pumped_storage_ferc1": plants_pumped_storage,
        "plant_in_service_ferc1": plant_in_service,
        "purchased_power_ferc1": purchased_power,
        "accumulated_depreciation_ferc1": accumulated_depreciation}

    ferc1_raw_dfs = {}
    for pudl_table in ferc1_tables:
        if pudl_table not in ferc1_extract_functions:
            raise ValueError(
                f"No extract function found for requested FERC Form 1 data "
                f"table {pudl_table}!"
            )
        ferc1_sqlite_table = pc.table_map_ferc1_pudl[pudl_table]
        logger.info(
            f"Converting extracted FERC Form 1 table {pudl_table} into a "
            f"pandas DataFrame.")
        ferc1_raw_dfs[pudl_table] = ferc1_extract_functions[pudl_table](
            ferc1_meta, ferc1_sqlite_table, ferc1_years)

    return ferc1_raw_dfs


def fuel(ferc1_meta, ferc1_table, ferc1_years):
    """Creates a DataFrame of f1_fuel table records with plant names, >0 fuel.

    Args:
        ferc1_meta (sa.MetaData): a MetaData object describing the cloned FERC
            Form 1 database
        ferc1_table (str): The name of the FERC 1 database table to read, in
            this case, the f1_fuel table.
        ferc1_years (list): The range of years from which to read data.

    Returns:
        pandas.DataFrame: A DataFrame containing f1_fuel records that have
        plant_names and non-zero fuel amounts.
    """
    # Grab the f1_fuel SQLAlchemy Table object from the metadata object.
    f1_fuel = ferc1_meta.tables[ferc1_table]
    # Generate a SELECT statement that pulls all fields of the f1_fuel table,
    # but only gets records with plant names and non-zero fuel amounts:
    f1_fuel_select = (
        sa.sql.select([f1_fuel]).
        where(f1_fuel.c.fuel != '').
        where(f1_fuel.c.fuel_quantity > 0).
        where(f1_fuel.c.plant_name != '').
        where(f1_fuel.c.report_year.in_(ferc1_years)).
        where(f1_fuel.c.respondent_id.notin_(pc.missing_respondents_ferc1))
    )
    # Use the above SELECT to pull those records into a DataFrame:
    return pd.read_sql(f1_fuel_select, ferc1_meta.bind)


def plants_steam(ferc1_meta, ferc1_table, ferc1_years):
    """Creates a DataFrame of f1_steam records with plant names, capacities > 0.

    Args:
        ferc1_meta (sa.MetaData): a MetaData object describing the cloned FERC
            Form 1 database
        ferc1_table (str): The name of the FERC 1 database table to read, in
            this case, the f1_steam table.
        ferc1_years (list): The range of years from which to read data.

    Returns:
        pandas.DataFrame: A DataFrame containing f1_steam records that have
        plant names and non-zero capacities.
    """
    f1_steam = ferc1_meta.tables[ferc1_table]
    f1_steam_select = (
        sa.sql.select([f1_steam]).
        where(f1_steam.c.tot_capacity > 0).
        where(f1_steam.c.plant_name != '').
        where(f1_steam.c.report_year.in_(ferc1_years)).
        where(f1_steam.c.respondent_id.notin_(pc.missing_respondents_ferc1))
    )

    return pd.read_sql(f1_steam_select, ferc1_meta.bind)


def plants_small(ferc1_meta, ferc1_table, ferc1_years):
    """Creates a DataFrame of f1_small for records with minimum data criteria.

    Args:
        ferc1_meta (sa.MetaData): a MetaData object describing the cloned FERC
            Form 1 database
        ferc1_table (str): The name of the FERC 1 database table to read, in
            this case, the f1_small table.
        ferc1_years (list): The range of years from which to read data.

    Returns:
        pandas.DataFrame: A DataFrame containing f1_small records that have
        plant names and non zero demand, generation, operations,
        maintenance, and fuel costs.
    """
    from sqlalchemy import or_

    f1_small = ferc1_meta.tables[ferc1_table]
    f1_small_select = (
        sa.sql.select([f1_small, ]).
        where(f1_small.c.report_year.in_(ferc1_years)).
        where(f1_small.c.plant_name != '').
        where(f1_small.c.respondent_id.notin_(pc.missing_respondents_ferc1)).
        where(or_((f1_small.c.capacity_rating != 0),
                  (f1_small.c.net_demand != 0),
                  (f1_small.c.net_generation != 0),
                  (f1_small.c.plant_cost != 0),
                  (f1_small.c.plant_cost_mw != 0),
                  (f1_small.c.operation != 0),
                  (f1_small.c.expns_fuel != 0),
                  (f1_small.c.expns_maint != 0),
                  (f1_small.c.fuel_cost != 0)))
    )

    return pd.read_sql(f1_small_select, ferc1_meta.bind)


def plants_hydro(ferc1_meta, ferc1_table, ferc1_years):
    """Creates a DataFrame of f1_hydro for records that have plant names.

    Args:
        ferc1_meta (sa.MetaData): a MetaData object describing the cloned FERC
            Form 1 database
        ferc1_table (str): The name of the FERC 1 database table to read, in
            this case, the f1_hydro table.
        ferc1_years (list): The range of years from which to read data.

    Returns:
        pandas.DataFrame: A DataFrame containing f1_hydro records that have
        plant names.
    """
    f1_hydro = ferc1_meta.tables[ferc1_table]

    f1_hydro_select = (
        sa.sql.select([f1_hydro]).
        where(f1_hydro.c.plant_name != '').
        where(f1_hydro.c.report_year.in_(ferc1_years)).
        where(f1_hydro.c.respondent_id.notin_(pc.missing_respondents_ferc1))
    )

    return pd.read_sql(f1_hydro_select, ferc1_meta.bind)


def plants_pumped_storage(ferc1_meta, ferc1_table, ferc1_years):
    """Creates a DataFrame of f1_plants_pumped_storage records with plant names.

    Args:
        ferc1_meta (sa.MetaData): a MetaData object describing the cloned FERC
            Form 1 database
        ferc1_table (str): The name of the FERC 1 database table to read, in
            this case, the f1_plants_pumped_storage table.
        ferc1_years (list): The range of years from which to read data.

    Returns:
        pandas.DataFrame: A DataFrame containing f1_plants_pumped_storage
        records that have plant names.
    """
    f1_pumped_storage = ferc1_meta.tables[ferc1_table]

    # Removing the empty records.
    # This reduces the entries for 2015 from 272 records to 27.
    f1_pumped_storage_select = (
        sa.sql.select([f1_pumped_storage]).
        where(f1_pumped_storage.c.plant_name != '').
        where(f1_pumped_storage.c.report_year.in_(ferc1_years)).
        where(f1_pumped_storage.c.respondent_id.
              notin_(pc.missing_respondents_ferc1))
    )

    return pd.read_sql(f1_pumped_storage_select, ferc1_meta.bind)


def plant_in_service(ferc1_meta, ferc1_table, ferc1_years):
    """Creates a DataFrame of the fields of plant_in_service_ferc1.

    Args:
        ferc1_meta (sa.MetaData): a MetaData object describing the cloned FERC
            Form 1 database
        ferc1_table (str): The name of the FERC 1 database table to read, in
            this case, the plant_in_service_ferc1 table.
        ferc1_years (list): The range of years from which to read data.

    Returns:
        pandas.DataFrame: A DataFrame containing all plant_in_service_ferc1
        records.
    """
    f1_plant_in_srvce = ferc1_meta.tables[ferc1_table]
    f1_plant_in_srvce_select = (
        sa.sql.select([f1_plant_in_srvce]).
        where(f1_plant_in_srvce.c.report_year.in_(ferc1_years)).
        # line_no mapping is invalid before 2007
        where(f1_plant_in_srvce.c.report_year >= 2007).
        where(f1_plant_in_srvce.c.respondent_id.
              notin_(pc.missing_respondents_ferc1))
    )

    return pd.read_sql(f1_plant_in_srvce_select, ferc1_meta.bind)


def purchased_power(ferc1_meta, ferc1_table, ferc1_years):
    """Creates a DataFrame the fields of purchased_power_ferc1.

    Args:
        ferc1_meta (sa.MetaData): a MetaData object describing the cloned FERC
            Form 1 database
        ferc1_table (str): The name of the FERC 1 database table to read, in
            this case, the purchased_power_ferc1 table.
        ferc1_years (list): The range of years from which to read data.

    Returns:
        pandas.DataFrame: A DataFrame containing all purchased_power_ferc1
        records.
    """
    f1_purchased_pwr = ferc1_meta.tables[ferc1_table]
    f1_purchased_pwr_select = (
        sa.sql.select([f1_purchased_pwr]).
        where(f1_purchased_pwr.c.report_year.in_(ferc1_years)).
        where(f1_purchased_pwr.c.respondent_id.
              notin_(pc.missing_respondents_ferc1))
    )

    return pd.read_sql(f1_purchased_pwr_select, ferc1_meta.bind)


def accumulated_depreciation(ferc1_meta, ferc1_table, ferc1_years):
    """Creates a DataFrame of the fields of accumulated_depreciation_ferc1.

    Args:
        ferc1_meta (sa.MetaData): a MetaData object describing the cloned FERC
            Form 1 database
        ferc1_table (str): The name of the FERC 1 database table to read, in
            this case, the accumulated_depreciation_ferc1.
        ferc1_years (list): The range of years from which to read data.

    Returns:
        pandas.DataFrame: A DataFrame containing all
        accumulated_depreciation_ferc1 records.
    """
    f1_accumdepr_prvsn = ferc1_meta.tables[ferc1_table]
    f1_accumdepr_prvsn_select = (
        sa.sql.select([f1_accumdepr_prvsn]).
        where(f1_accumdepr_prvsn.c.report_year.in_(ferc1_years)).
        where(f1_accumdepr_prvsn.c.respondent_id.
              notin_(pc.missing_respondents_ferc1))
    )

    return pd.read_sql(f1_accumdepr_prvsn_select, ferc1_meta.bind)


###########################################################################
# Helper functions for debugging the extract process and facilitating the
# manual portions of the FERC to EIA plant and utility mapping process.
###########################################################################

def get_raw_db_plants(pudl_settings, years):
    """
    Pull a dataframe of all plants in the FERC Form 1 DB for the given years.

    This function looks in the f1_steam, f1_gnrt_plant, f1_hydro and
    f1_pumped_storage tables, and generates a dataframe containing every unique
    combination of respondent_id (utility_id_ferc1) and plant_name is finds.
    Also included is the capacity of the plant in MW (as reported in the
    raw FERC Form 1 DB), the respondent_name (utility_name_ferc1) and a column
    indicating which of the plant tables the record came from.  Plant and
    utility names are translated to lowercase, with leading and trailing
    whitespace stripped and repeating internal whitespace compacted to a single
    space.

    This function is primarily meant for use generating inputs into the manual
    mapping of FERC to EIA plants with PUDL IDs.

    Args:
        pudl_settings (dict): Dictionary containing various paths and database
            URLs used by PUDL.
        years (iterable): Years for which plants should be compiled.

    Returns:
        :class:`pandas.DataFrame`: A dataframe containing five columns:
            utility_id_ferc1, utility_name_ferc1, plant_name, capacity_mw, and
            plant_table. Each row is a unique combination of utility_id_ferc1
            and plant_name.

    """
    # Need to be able to use years outside the "valid" range if we're trying
    # to get new plant ID info...
    for yr in years:
        if yr not in pc.data_years['ferc1']:
            raise ValueError(
                f"Input year {yr} is not available in the FERC data.")

    # Grab the FERC 1 DB metadata so we can query against the DB w/ SQLAlchemy:
    ferc1_engine = sa.create_engine(pudl_settings["ferc1_db"])
    ferc1_meta = sa.MetaData(bind=ferc1_engine)
    ferc1_meta.reflect()
    ferc1_tables = ferc1_meta.tables

    # This table contains the utility names and IDs:
    respondent_table = ferc1_tables['f1_respondent_id']
    # These are all the tables we're gathering "plants" from:
    plant_tables = ['f1_steam', 'f1_gnrt_plant',
                    'f1_hydro', 'f1_pumped_storage']
    # FERC doesn't use the sme column names for the same values across all of
    # Their tables... but all of these are cpacity in MW.
    capacity_cols = {'f1_steam': 'tot_capacity',
                     'f1_gnrt_plant': 'capacity_rating',
                     'f1_hydro': 'tot_capacity',
                     'f1_pumped_storage': 'tot_capacity'}

    # Generate a list of all combinations of utility ID, utility name, and
    # plant name that currently exist inside the raw FERC Form 1 Database, by
    # iterating over the tables that contain "plants" and grabbing those
    # columns (along with their capacity, since that's useful for matching
    # purposes)
    all_plants = pd.DataFrame()
    for tbl in plant_tables:
        plant_select = sa.sql.select([
            ferc1_tables[tbl].c.respondent_id,
            ferc1_tables[tbl].c.plant_name,
            ferc1_tables[tbl].columns[capacity_cols[tbl]],
            respondent_table.c.respondent_name
        ]).distinct().where(
            sa.and_(
                ferc1_tables[tbl].c.respondent_id == respondent_table.c.respondent_id,
                ferc1_tables[tbl].c.plant_name != '',
                ferc1_tables[tbl].c.report_year.in_(years)
            )
        )
        # Add all the plants from the current table to our bigger list:
        all_plants = all_plants.append(
            pd.read_sql(plant_select, ferc1_engine).
            rename(columns={'respondent_id': 'utility_id_ferc1',
                            'respondent_name': 'utility_name_ferc1',
                            capacity_cols[tbl]: "capacity_mw"}).
            pipe(pudl.helpers.strip_lower, columns=['plant_name',
                                                    'utility_name_ferc1']).
            assign(plant_table=tbl).
            loc[:, ['utility_id_ferc1',
                    'utility_name_ferc1',
                    'plant_name',
                    'capacity_mw',
                    'plant_table']]
        )

    # We don't want dupes, and sorting makes the whole thing more readable:
    all_plants = (
        all_plants.drop_duplicates(["utility_id_ferc1", "plant_name"]).
        sort_values(["utility_id_ferc1", "plant_name"])
    )
    return all_plants


def get_mapped_plants():
    """
    Generate a dataframe containing all previously mapped FERC 1 plants.

    Many plants are reported in FERC Form 1 with different versions of the same
    name in different years. Because FERC provides no unique ID for plants,
    these names must be used as part of their identifier. We manually curate a
    list of all the versions of plant names which map to the same actual plant.
    In order to identify new plants each year, we have to compare the new plant
    names and respondent IDs against this raw mapping, not the contents of the
    PUDL data, since within PUDL we use one canonical name for the plant. This
    function pulls that list of various plant names and their corresponding
    utilities (both name and ID) for use in identifying which plants have yet
    to be mapped when we are integrating new data.

    Args:
        None

    Returns:
        :class:`pandas.DataFrame`: A DataFrame with three columns: plant_name,
        utility_id_ferc1, and utility_name_ferc1. Each row represents a unique
        combination of utility_id_ferc1 and plant_name.

    """
    # If we're only trying to get the NEW plants, then we need to see which
    # ones we have already integrated into the PUDL database. However, because
    # FERC doesn't use the same plant names from year to year, we have to rely
    # on the full mapping of FERC plant names to PUDL IDs, which only exists
    # in the ID mapping spreadhseet (the FERC Plant names in the PUDL DB are
    # canonincal names we've chosen to represent all the varied plant names
    # that exist in the raw FERC DB.
    ferc1_mapped_plants = (
        pudl.glue.ferc1_eia.get_plant_map().
        loc[:, ["utility_id_ferc1", "utility_name_ferc1", "plant_name_ferc"]].
        dropna(subset=["utility_id_ferc1"]).
        pipe(pudl.helpers.strip_lower,
             columns=["utility_id_ferc1",
                      "utility_name_ferc1",
                      "plant_name_ferc"]).
        drop_duplicates(["utility_id_ferc1", "plant_name_ferc"]).
        astype({"utility_id_ferc1": int}).
        sort_values(["utility_id_ferc1", "plant_name_ferc"]).
        rename(columns={"plant_name_ferc": "plant_name"})
    )
    return ferc1_mapped_plants


def get_mapped_utilities():
    """
    Read in the list of manually mapped utilities for FERC Form 1.

    Unless a new utility has appeared in the database, this should be identical
    to the full list of utilities available in the FERC Form 1 database.
    """
    ferc1_mapped_utils = (
        pudl.glue.ferc1_eia.get_utility_map().
        loc[:, ["utility_id_ferc1", "utility_name_ferc1"]].
        dropna(subset=["utility_id_ferc1"]).
        pipe(pudl.helpers.strip_lower,
             columns=["utility_id_ferc1", "utility_name_ferc1"]).
        drop_duplicates("utility_id_ferc1").
        astype({"utility_id_ferc1": int}).
        sort_values(["utility_id_ferc1"])
    )
    return ferc1_mapped_utils


def get_unmapped_plants(pudl_settings, years):
    """
    Generate a DataFrame of all unmapped FERC plants in the given years.

    Pulls all plants from the FERC Form 1 DB for the given years, and compares
    that list against the already mapped plants. Any plants found in the
    database but not in the list of mapped plants are returned.

    Args:
        pudl_settings (dict): Dictionary containing various paths and database
            URLs used by PUDL.
        years (iterable): Years for which plants should be compiled from the
            raw FERC Form 1 DB.

    Returns:
        :class:`pandas.DataFrame`: A dataframe containing five columns:
            utility_id_ferc1, utility_name_ferc1, plant_name, capacity_mw, and
            plant_table. Each row is a unique combination of utility_id_ferc1
            and plant_name, which appears in the FERC Form 1 DB, but not in
            the list of manually mapped plants.

    """
    db_plants = (
        get_raw_db_plants(pudl_settings, years).
        set_index(["utility_id_ferc1", "plant_name"])
    )
    mapped_plants = (
        get_mapped_plants().
        set_index(["utility_id_ferc1", "plant_name"])
    )
    new_plants_index = db_plants.index.difference(mapped_plants.index)
    unmapped_plants = db_plants.loc[new_plants_index].reset_index()
    return unmapped_plants


def get_unmapped_utilities(pudl_settings, years):
    """
    Generate a list of as-of-yet unmapped utilities from the FERC Form 1 DB.

    Find any utilities which exist in the FERC Form 1 database for the years
    requested, but which do not show up in the mapped plants.  Note that there
    are many more utilities in FERC Form 1 that simply have no plants
    associated with them that will not show up here.

    Args:
        pudl_settings (dict): Dictionary containing various paths and database
            URLs used by PUDL.
        years (iterable): Years for which plants should be compiled from the
            raw FERC Form 1 DB.
    Returns:
        :class:`pandas.DataFrame`:

    """
    # Note: we only map the utlities that have plants associated with them.
    # Grab the list of all utilities listed in the mapped plants:
    mapped_utilities = get_mapped_utilities().set_index("utility_id_ferc1")
    # Generate a list of all utilities which have unmapped plants:
    # (Since any unmapped utility *must* have unmapped plants)
    utils_with_unmapped_plants = (
        get_unmapped_plants(pudl_settings, years).
        loc[:, ["utility_id_ferc1", "utility_name_ferc1"]].
        drop_duplicates("utility_id_ferc1").
        set_index("utility_id_ferc1")
    )
    # Find the indices of all utilities with unmapped plants that do not appear
    # in the list of mapped utilities at all:
    new_utilities_index = (
        utils_with_unmapped_plants.index.
        difference(mapped_utilities.index)
    )
    # Use that index to select only the previously unmapped utilities:
    unmapped_utilities = (
        utils_with_unmapped_plants.
        loc[new_utilities_index].
        reset_index()
    )
    return unmapped_utilities


def check_ferc1_tables(refyear=2017):
    """
    Test each FERC 1 data year for compatibility with reference year schema.

    Args:
        refyear (int): The reference year for testing compatibility of the
            database schema with a FERC Form 1 table and year.

    Returns:
        dict: A dictionary having database table names as keys, and lists of
            which years that table was compatible with the reference year as
            values.

    """
    good_table_years = {}
    tables = list(pc.ferc1_dbf2tbl.values())
    # This is a special table, to which every other table refers, it will be
    # loaded alongside every table we test.
    tables.remove('f1_respondent_id')
    for table in tables:
        good_years = []
        print(f"'{table}': [", end="", flush=True)
        for yr in pc.data_years['ferc1']:
            try:
                pudl.extract.ferc1.init_db(
                    ferc1_tables=['f1_respondent_id', table],
                    refyear=refyear,
                    years=[yr, ],
                    def_db=True,
                    testing=True,
                    force_tables=True)
                good_years = good_years + [yr, ]
                print(f"{yr},", end=" ", flush=True)
            # generally bare except: statements are bad, but here we're really
            # just trying to test whether the ferc1 extraction fails for *any*
            # reason, and if not, mark that year as good, thus the # nosec
            except:  # noqa: E722  # nosec
                continue
            ferc1_engine = pudl.extract.ferc1.connect_db(testing=True)
            pudl.extract.ferc1.drop_tables(ferc1_engine)
        good_table_years[table] = good_years
        print("],", flush=True)

    return good_table_years


def show_dupes(table, dbc_map, years=pc.data_years['ferc1'],
               pk=['respondent_id', 'report_year', 'report_prd',
                   'row_number', 'spplmnt_num']):
    """
    Identify duplicate primary keys by year within a given FERC Form 1 table.

    Args:
        table (str): Name of the original FERC Form 1 table to identify
            duplicate records in.
        years (iterable): a list or other iterable containing the years that
            should be searched for duplicate records. By default it is all
            available years of FERC Form 1 data.
        pk (list): A list of strings identifying the columns in the FERC Form 1
            table that should be treated as a composite primary key. By default
            this includes: respondent_id, report_year, report_prd, row_number,
            and spplmnt_num.

    Returns:
        None

    """
    logger.info(f"{table}:")
    for yr in years:
        raw_df = get_raw_df(table, dbc_map, years=[yr, ])
        if not set(pk).difference(set(raw_df.columns)):
            n_dupes = len(raw_df) - len(raw_df.drop_duplicates(subset=pk))
            if n_dupes > 0:
                logger.info(f"    {yr}: {n_dupes}")
    # return raw_df[raw_df.duplicated(subset=pk, keep=False)]
