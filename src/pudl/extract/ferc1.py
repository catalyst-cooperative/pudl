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
`dbfread <https://dbfread.readthedocs.io/en/latest/>`__ to extract the data from
the DBF tables, and insert it virtually unchanged into the SQLite database.
However, we do compile a master table of the all the respondent IDs and
respondent names, which all the other tables refer to. Unlike the other tables,
this table has no ``report_year`` and so it represents a merge of all the years
of data. In the event that the name associated with a given respondent ID has
changed over time, we retain the most recently reported name.

Ths SQLite based compilation of the original FERC Form 1 databases can
accommodate all 116 tables from all the published years of data (beginning in
1994). Including all the data through 2018, the database takes up more than
7GB of disk space. However, almost 90% of that "data" is embeded binary files
in two tables. If those tables are excluded, the database is less than 800MB
in size.

The process of cloning the FERC Form 1 database(s) is coordinated by a script
called ``ferc1_to_sqlite`` implemented in :mod:`pudl.convert.ferc1_to_sqlite`
which is controlled by a YAML file. See the example file distributed with the
package.

Once the cloned SQLite database has been created, we use it as an input into
the PUDL ETL pipeline, and we extract a small subset of the available tables
for further processing and integration with other data sources like the EIA 860
and EIA 923.

"""
import csv
import importlib
import logging
import zipfile
from pathlib import Path

import dbfread
import pandas as pd
import sqlalchemy as sa
from dbfread import DBF

import pudl
from pudl import constants as pc
from pudl.workspace import datastore as datastore

logger = logging.getLogger(__name__)


class Ferc1Datastore(datastore.Datastore):
    """Provide a thin interface for pulling files from the Datastore."""

    def get_folder(self, year):
        """
        Retrieve the DBC path (within a zip file) for a given year.

        Args:
            year (int): Year for the form.
        Returns:
            str: Path of ferc data within the zip file
        """
        pkg = "pudl.package_data.meta.ferc1_row_maps"
        dbc_path = None

        with importlib.resources.open_text(pkg, "file_map.csv") as f:
            reader = csv.DictReader(f)

            for row in reader:
                if int(row["year"]) == year:
                    dbc_path = row["path"]

        if dbc_path is None:
            raise ValueError("No ferc1 data for year %d" % year)

        return dbc_path

    def get_file(self, year, filename):
        """
        Retrieve the specified file from the ferc1 archive.

        Args:
            year (int): Year for the form.
        Returns:
            bytes object of the requested file, if available.
        """
        dbc_path = str(Path(self.get_folder(year)) / filename)
        resource = next(self.get_resources("ferc1", year=year))
        z = zipfile.ZipFile(resource["path"])

        try:
            f = z.open(dbc_path)
        except KeyError:
            raise KeyError(f"{dbc_path} is not available in {year} archive.")

        return f


def drop_tables(engine):
    """Drop all FERC Form 1 tables from the SQLite database.

    Creates an sa.schema.MetaData object reflecting the structure of the
    database that the passed in ``engine`` refers to, and uses that schema to
    drop all existing tables.

    Todo:
        Treat DB connection as a context manager (with/as).

    Args:
        engine (:class:`sqlalchemy.engine.Engine`): A DB Engine pointing at an
            exising SQLite database to be deleted.

    Returns:
        None

    """
    md = sa.MetaData(bind=engine)
    md.reflect(engine)
    md.drop_all(engine)
    conn = engine.connect()
    conn.execute("VACUUM")
    conn.close()


def add_sqlite_table(table_name, sqlite_meta, dbc_map, ds,
                     refyear=max(pc.working_years['ferc1']),
                     testing=False,
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
        sqlite_meta (:class:`sqlalchemy.schema.MetaData`): The database schema
            to which the newly defined :class:`sqlalchemy.Table` will be added.
        dbc_map (dict): A dictionary of dictionaries
        ds (:class:`Ferc1Datastore`): Initialized datastore
        testing (bool): Assume this is a test run, use sandboxes
        bad_cols (iterable of 2-tuples): A list or other iterable containing
            pairs of strings of the form (table_name, column_name), indicating
            columns (and their parent tables) which should *not* be cloned
            into the SQLite database for some reason.

    Returns:
        None

    """
    # Create the new table object
    new_table = sa.Table(table_name, sqlite_meta)

    fn = pc.ferc1_tbl2dbf[table_name] + ".DBF"
    filedata = ds.get_file(refyear, fn)

    ferc1_dbf = dbfread.DBF(fn, ignore_missing_memofile=True,
                            filedata=filedata)

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


def get_fields(filedata):
    """
    Produce the expected table names and fields from a DBC file.

    Args:
        filedata: Contents of the DBC file from which to extract.
    Returns:
        dict of table_name: [fields]
    """
    dbf = DBF("", ignore_missing_memofile=True, filedata=filedata)
    table_ids = {}
    table_cols = {}

    for r in dbf:

        if r.get("OBJECTTYPE", None) == "Table":
            tname = r["OBJECTNAME"]
            tid = r["OBJECTID"]

            if tid not in table_ids:
                table_ids[tid] = tname

        elif r.get("OBJECTTYPE", None) == "Field":
            tid = r["PARENTID"]
            colname = r["OBJECTNAME"]

            if tid in table_cols:
                table_cols[tid].append(colname)
            else:
                table_cols[tid] = [colname]

    tables = {}

    for tid, tname in table_ids.items():
        if tid in table_cols:
            tables[tname] = table_cols[tid]
        else:
            logger.warning("Missing cols on %s", tname)

    return tables


def get_dbc_map(ds, year, min_length=4):
    """
    Extract names of all tables and fields from a FERC Form 1 DBC file.

    Read the DBC file associated with the FERC Form 1 database for the given
    ``year``, and extract all printable strings longer than ``min_lengh``.
    Select those strings that appear to be database table names, and their
    associated field for use in re-naming the truncated column names extracted
    from the corresponding DBF files (those names are limited to having only 10
    characters in their names.)

    Args:
        ds (:class:`Ferc1Datastore`): Initialized datastore
        year (int): The year of data from which the database table and column
            names are to be extracted. Typically this is expected to be the
            most recently available year of FERC Form 1 data.
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
    dbc = ds.get_file(year, "F1_PUB.DBC")
    tf_dict = get_fields(dbc)

    dbc_map = {}
    for table, fn in pc.ferc1_tbl2dbf.items():

        try:
            dbc = ds.get_file(year, f"{fn}.DBF")
        except KeyError:
            # Not all tables exist in all years, so this is acceptable
            dbc = None

        if dbc is None:
            continue

        dbf_fields = dbfread.DBF(
            "", filedata=dbc, ignore_missing_memofile=True).field_names
        dbf_fields = [f for f in dbf_fields if f != '_NullFlags']
        dbc_map[table] = \
            {k: v for k, v in zip(dbf_fields, tf_dict[table])}
        if len(tf_dict[table]) != len(dbf_fields):
            raise ValueError(
                f"Number of DBF fields in {table} does not match what was "
                f"found in the FERC Form 1 DBC index file for {year}."
            )

    # Insofar as we are able, make sure that the fields match each other
    for k in dbc_map:
        for sn, ln in zip(dbc_map[k].keys(), dbc_map[k].values()):
            if ln[:8] != sn.lower()[:8]:
                raise ValueError(
                    f"DBF field name mismatch: {ln[:8]} != {sn.lower()[:8]}"
                )

    return dbc_map


def define_sqlite_db(sqlite_meta, dbc_map, ds,
                     tables=pc.ferc1_tbl2dbf,
                     refyear=max(pc.working_years['ferc1']),
                     bad_cols=()):
    """
    Defines a FERC Form 1 DB structure in a given SQLAlchemy MetaData object.

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
        ds (:class:`Ferc1Datastore`): Initialized Ferc1Datastore
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
        add_sqlite_table(table, sqlite_meta, dbc_map, ds,
                         refyear=refyear, bad_cols=bad_cols)

    sqlite_meta.create_all()


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

        """
        # Strip whitespace, null characters, and zeroes
        data = data.strip().strip(b'*\x00').lstrip(b'0')
        # Replace bare periods (which are non-numeric) with zero.
        if data == b'.':
            data = b'0'
        return super(FERC1FieldParser, self).parseN(field, data)


def get_raw_df(ds, table, dbc_map, years=pc.data_years['ferc1']):
    """Combine several years of a given FERC Form 1 DBF table into a dataframe.

    Args:
        ds (:class:`Ferc1Datastore`): Initialized datastore
        table (string): The name of the FERC Form 1 table from which data is
            read.
        dbc_map (dict of dicts): A dictionary of dictionaries, of the kind
            returned by get_dbc_map(), describing the table and column names
            stored within the FERC Form 1 FoxPro database files.
        min_length (int): The minimum number of consecutive printable
        years (list): Range of years to be combined into a single DataFrame.

    Returns:
        :class:`pandas.DataFrame`: A DataFrame containing several years of FERC
        Form 1 data for the given table.

    """
    dbf_name = pc.ferc1_tbl2dbf[table]

    raw_dfs = []
    for yr in years:
        try:
            filedata = ds.get_file(yr, f"{dbf_name}.DBF")
        except KeyError:
            continue

        new_df = pd.DataFrame(
            iter(dbfread.DBF(dbf_name,
                             encoding='latin1',
                             parserclass=FERC1FieldParser,
                             ignore_missing_memofile=True,
                             filedata=filedata)))
        raw_dfs = raw_dfs + [new_df, ]

    if raw_dfs:
        return (
            pd.concat(raw_dfs, sort=True).
            drop('_NullFlags', axis=1, errors='ignore').
            rename(dbc_map[table], axis=1)
        )


def dbf2sqlite(tables, years, refyear, pudl_settings,
               bad_cols=(), clobber=False):
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
        pudl.helpers.drop_tables(sqlite_engine, clobber=clobber)
    except sa.exc.OperationalError:
        pass

    # And start anew
    sqlite_engine = sa.create_engine(pudl_settings["ferc1_db"])
    sqlite_meta = sa.MetaData(bind=sqlite_engine)

    # Get the mapping of filenames to table names and fields
    logger.info(f"Creating a new database schema based on {refyear}.")
    sandbox = pudl_settings.get("sandbox", False)
    ds = Ferc1Datastore(
        Path(pudl_settings["pudl_in"]),
        sandbox=sandbox)

    dbc_map = get_dbc_map(ds, refyear)
    define_sqlite_db(sqlite_meta, dbc_map, ds, tables=tables,
                     refyear=refyear, bad_cols=bad_cols)

    for table in tables:
        logger.info(f"Pandas: reading {table} into a DataFrame.")
        new_df = get_raw_df(ds, table, dbc_map, years=years)
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
        # add the missing respondents into the respondent_id table.
        if table == 'f1_respondent_id':
            logger.debug(f'inserting missing respondents into {table}')
            sa.insert(sqlite_meta.tables['f1_respondent_id'],
                      # we can insert info info into any of the columns for this
                      # table through the following dictionary, but each of the
                      # records need to have all of the same columns (you can't
                      # add a column for one respondent without adding it to all).
                      values=[
                      {'respondent_id': 514,
                       'respondent_name': 'AEP, Texas (PUDL determined)'},
                      {'respondent_id': 515,
                       'respondent_name': 'respondent_515'},
                      {'respondent_id': 516,
                       'respondent_name': 'respondent_516'},
                      {'respondent_id': 517,
                       'respondent_name': 'respondent_517'},
                      {'respondent_id': 518,
                       'respondent_name': 'respondent_518'},
                      {'respondent_id': 519,
                       'respondent_name': 'respondent_519'},
                      {'respondent_id': 522,
                       'respondent_name':
                       'Luning Energy Holdings LLC, Invenergy Investments (PUDL determined)'},
            ]).execute()


###########################################################################
# Functions for extracting ferc1 tables from SQLite to PUDL
###########################################################################
def get_ferc1_meta(ferc1_engine):
    """Grab the FERC Form 1 DB metadata and check that tables exist.

    Connects to the FERC Form 1 SQLite database and reads in its metadata
    (table schemas, types, etc.) by reflecting the database. Checks to make
    sure the DB is not empty, and returns the metadata object.

    Args:
        ferc1_engine (:mod:`sqlalchemy.engine.Engine`): SQL Alchemy database
            connection engine for the PUDL FERC 1 DB.

    Returns:
        :class:`sqlalchemy.Metadata`: A SQL Alchemy metadata object, containing
        the definition of the DB structure.

    Raises:
        ValueError: If there are no tables in the SQLite Database.

    """
    # Connect to the local SQLite DB and read its structure.
    ferc1_meta = sa.MetaData(bind=ferc1_engine)
    ferc1_meta.reflect()
    if not ferc1_meta.tables:
        raise ValueError(
            "No FERC Form 1 tables found. Is the SQLite DB initialized?"
        )
    return ferc1_meta


def extract(ferc1_tables=pc.pudl_tables['ferc1'],
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
        :mod:`pudl.transform.ferc1` module.

    Raises:
        ValueError: If the year is not in the list of years for which FERC data
            is available
        ValueError: If the year is not in the list of working FERC years
        ValueError: If the FERC table requested is not integrated into PUDL

    """
    if (not ferc1_tables) or (not ferc1_years):
        return {}

    for year in ferc1_years:
        if year not in pc.data_years["ferc1"]:
            raise ValueError(
                f"FERC Form 1 data from the year {year} was requested but is "
                f"not available. The years for which data is available are: "
                f"{' '.join(str(year) for item in pc.data_years['ferc1'])}."
            )
        if year not in pc.working_years["ferc1"]:
            raise ValueError(
                f"FERC Form 1 data from the year {year} was requested but it "
                f"has not yet been integrated into PUDL. "
                f"If you'd like to contribute the necessary cleaning "
                f"functions, come find us on GitHub: "
                f"{pudl.__downloadurl__}"
                f"For now, the years which PUDL has integrated are: "
                f"{' '.join(str(year) for item in pc.working_years['ferc1'])}."
            )
    for table in ferc1_tables:
        if table not in pc.pudl_tables["ferc1"]:
            raise ValueError(
                f"FERC Form 1 table {table} was requested but it has not yet "
                f"been integreated into PUDL. Heck, it might not even exist! "
                f"If you'd like to contribute the necessary cleaning "
                f"functions, come find us on GitHub: "
                f"{pudl.__downloadurl__}"
                f"For now, the tables which PUDL has integrated are: "
                f"{' '.join(str(year) for item in pc.pudl_tables['ferc1'])}"
            )

    ferc1_meta = get_ferc1_meta(sa.create_engine(pudl_settings["ferc1_db"]))

    ferc1_extract_functions = {
        "fuel_ferc1": fuel,
        "plants_steam_ferc1": plants_steam,
        "plants_small_ferc1": plants_small,
        "plants_hydro_ferc1": plants_hydro,
        "plants_pumped_storage_ferc1": plants_pumped_storage,
        "plant_in_service_ferc1": plant_in_service,
        "purchased_power_ferc1": purchased_power,
        "accumulated_depreciation_ferc1": accumulated_depreciation
    }

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
        :class:`pandas.DataFrame`: A DataFrame containing f1_fuel records that
        have plant_names and non-zero fuel amounts.

    """
    # Grab the f1_fuel SQLAlchemy Table object from the metadata object.
    f1_fuel = ferc1_meta.tables[ferc1_table]
    # Generate a SELECT statement that pulls all fields of the f1_fuel table,
    # but only gets records with plant names and non-zero fuel amounts:
    f1_fuel_select = (
        sa.sql.select([f1_fuel])
        .where(f1_fuel.c.fuel != '')
        .where(f1_fuel.c.fuel_quantity > 0)
        .where(f1_fuel.c.plant_name != '')
        .where(f1_fuel.c.report_year.in_(ferc1_years))
    )
    # Use the above SELECT to pull those records into a DataFrame:
    return pd.read_sql(f1_fuel_select, ferc1_meta.bind)


def plants_steam(ferc1_meta, ferc1_table, ferc1_years):
    """
    Create a :class:`pandas.DataFrame` containing valid raw f1_steam records.

    Selected records must indicate a plant capacity greater than 0, and include
    a non-null plant name.

    Args:
        ferc1_meta (:class:`sqlalchemy.MetaData`): a MetaData object describing
            the cloned FERC Form 1 database
        ferc1_table (str): The name of the FERC 1 database table to read, in
            this case, the f1_steam table.
        ferc1_years (list): The range of years from which to read data.

    Returns:
        pandas.DataFrame: A DataFrame containing f1_steam records that have
        plant names and non-zero capacities.

    """
    f1_steam = ferc1_meta.tables[ferc1_table]
    f1_steam_select = (
        sa.sql.select([f1_steam])
        .where(f1_steam.c.report_year.in_(ferc1_years))
        .where(f1_steam.c.plant_name != '')
        .where(f1_steam.c.tot_capacity > 0.0)
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
        sa.sql.select([f1_small, ])
        .where(f1_small.c.report_year.in_(ferc1_years))
        .where(f1_small.c.plant_name != '')
        .where(or_((f1_small.c.capacity_rating != 0),
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
        sa.sql.select([f1_hydro])
        .where(f1_hydro.c.plant_name != '')
        .where(f1_hydro.c.report_year.in_(ferc1_years))
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
        sa.sql.select([f1_pumped_storage])
        .where(f1_pumped_storage.c.plant_name != '')
        .where(f1_pumped_storage.c.report_year.in_(ferc1_years))
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
        sa.sql.select([f1_plant_in_srvce])
        .where(f1_plant_in_srvce.c.report_year.in_(ferc1_years))
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
        sa.sql.select([f1_purchased_pwr])
        .where(f1_purchased_pwr.c.report_year.in_(ferc1_years))
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
        :class:`pandas.DataFrame`: A DataFrame containing all
        accumulated_depreciation_ferc1 records.

    """
    f1_accumdepr_prvsn = ferc1_meta.tables[ferc1_table]
    f1_accumdepr_prvsn_select = (
        sa.sql.select([f1_accumdepr_prvsn])
        .where(f1_accumdepr_prvsn.c.report_year.in_(ferc1_years))
    )

    return pd.read_sql(f1_accumdepr_prvsn_select, ferc1_meta.bind)


###########################################################################
# Helper functions for debugging the extract process and facilitating the
# manual portions of the FERC to EIA plant and utility mapping process.
###########################################################################
def check_ferc1_tables(refyear):
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


def show_dupes(table, dbc_map, data_dir, years=pc.data_years['ferc1'],
               pk=('respondent_id', 'report_year', 'report_prd',
                   'row_number', 'spplmnt_num')):
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
        raw_df = get_raw_df(table, dbc_map, data_dir=data_dir, years=[yr, ])
        if not set(pk).difference(set(raw_df.columns)):
            n_dupes = len(raw_df) - len(raw_df.drop_duplicates(subset=pk))
            if n_dupes > 0:
                logger.info(f"    {yr}: {n_dupes}")
    # return raw_df[raw_df.duplicated(subset=pk, keep=False)]
