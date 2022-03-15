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
import io
import logging
from pathlib import Path
from typing import Dict, Set

import dbfread
import pandas as pd
import sqlalchemy as sa
from dbfread import DBF
from sqlalchemy import or_

import pudl
from pudl.metadata.classes import DataSource
from pudl.metadata.constants import DBF_TABLES_FILENAMES
from pudl.settings import Ferc1Settings, Ferc1ToSqliteSettings
from pudl.workspace.datastore import Datastore

logger = logging.getLogger(__name__)

DBF_TYPES = {
    'C': sa.String,
    'D': sa.Date,
    'F': sa.Float,
    'I': sa.Integer,
    'L': sa.Boolean,
    'M': sa.Text,  # 10 digit .DBT block number, stored as a string...
    'N': sa.Float,
    'T': sa.DateTime,
    '0': sa.Integer,  # based on dbf2sqlite mapping
    'B': 'XXX',  # .DBT block number, binary string
    '@': 'XXX',  # Timestamp... Date = Julian Day, Time is in milliseconds?
    '+': 'XXX',  # Autoincrement (e.g. for IDs)
    'O': 'XXX',  # Double, 8 bytes
    'G': 'XXX',  # OLE 10 digit/byte number of a .DBT block, stored as string
}
"""dict: A mapping of DBF field types to SQLAlchemy Column types.

This dictionary maps the strings which are used to denote field types in the
DBF objects to the corresponding generic SQLAlchemy Column types:
These definitions come from a combination of the dbfread example program
dbf2sqlite and this DBF file format documentation page:
http://www.dbase.com/KnowledgeBase/int/db7_file_fmt.htm
Un-mapped types left as 'XXX' which should obviously make an error.
"""

PUDL_RIDS = {
    514: "AEP Texas",
    519: "Upper Michigan Energy Resources Company",
    522: "Luning Energy Holdings LLC, Invenergy Investments",
    529: "Tri-State Generation and Transmission Association",
    531: "Basin Electric Power Cooperative",
}
"""Missing FERC 1 Respondent IDs for which we have identified the respondent."""


def missing_respondents(reported, observed, identified):
    """
    Fill in missing respondents for the f1_respondent_id table.

    Args:
        reported (iterable): Respondent IDs appearing in f1_respondent_id.
        observed (iterable): Respondent IDs appearing anywhere in the ferc1 DB.
        identified (dict): A {respondent_id: respondent_name} mapping for those
            observed but not reported respondent IDs which we have been able to
            identify based on circumstantial evidence. See also:
            `pudl.extract.ferc1.PUDL_RIDS`

    Returns:
        list: A list of dictionaries representing minimal f1_respondent_id table
        records, of the form {"respondent_id": ID, "respondent_name": NAME}. These
        records are generated only for unreported respondents. Identified respondents
        get the values passed in through ``identified`` and the other observed but
        unidentified respondents are named "Missing Respondent ID"

    """
    records = []
    for rid in observed:
        if rid in reported:
            continue
        elif rid in identified:
            records.append(
                {
                    "respondent_id": rid,
                    "respondent_name": f"{identified[rid]} (PUDL determined)"
                },
            )
        else:
            records.append(
                {
                    "respondent_id": rid,
                    "respondent_name": f"Missing Respondent {rid}"
                },
            )
    return records


def observed_respondents(ferc1_engine: sa.engine.Engine) -> Set[int]:
    """
    Compile the set of all observed respondent IDs found in the FERC 1 database.

    A significant number of FERC 1 respondent IDs appear in the data tables, but not
    in the f1_respondent_id table. In order to construct a self-consistent database with
    we need to find all of those missing respondent IDs and inject them into the table
    when we clone the database.

    Args:
        ferc1_engine: An engine for connecting to the FERC 1 database.

    Returns:
        Every respondent ID reported in any of the FERC 1 DB tables.

    """
    f1_table_meta = pudl.output.pudltabl.get_table_meta(ferc1_engine)
    observed = set([])
    for table in f1_table_meta.values():
        if "respondent_id" in table.columns:
            observed = observed.union(set(pd.read_sql_table(
                table.name, ferc1_engine, columns=["respondent_id"]).respondent_id))
    return observed


class Ferc1Datastore:
    """Simple datastore wrapper for accessing ferc1 resources."""

    PACKAGE_PATH = "pudl.package_data.ferc1"

    def __init__(self, datastore: Datastore):
        """Instantiate datastore wrapper for ferc1 resources."""
        self.datastore = datastore
        self._cache = {}  # type: Dict[int, io.BytesIO]
        self.dbc_path = {}  # type: Dict[int, Path]

        with importlib.resources.open_text(self.PACKAGE_PATH, "file_map.csv") as f:
            for row in csv.DictReader(f):
                year = int(row["year"])
                path = Path(row["path"])
                self.dbc_path[year] = path

    def get_dir(self, year: int) -> Path:
        """Returns the path where individual ferc1 files are stored inside the yearly archive."""
        if year not in self.dbc_path:
            raise ValueError(f"No ferc1 data for year {year}")
        return self.dbc_path[year]

    def get_file(self, year: int, filename: str):
        """Opens given ferc1 file from the corresponding archive."""
        if year not in self._cache:
            self._cache[year] = self.datastore.get_zipfile_resource("ferc1", year=year)
        archive = self._cache[year]
        try:
            return archive.open((self.get_dir(year) / filename).as_posix())
        except KeyError:
            raise KeyError(f"{filename} not availabe for year {year} in ferc1.")


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
    md = sa.MetaData()
    md.reflect(engine)
    md.drop_all(engine)
    conn = engine.connect()
    conn.execute("VACUUM")
    conn.close()


def add_sqlite_table(
    table_name,
    sqlite_meta,
    dbc_map,
    ds,
    refyear=max(DataSource.from_id("ferc1").working_partitions["years"]),
    bad_cols=()
):
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
        bad_cols (iterable of 2-tuples): A list or other iterable containing
            pairs of strings of the form (table_name, column_name), indicating
            columns (and their parent tables) which should *not* be cloned
            into the SQLite database for some reason.

    Returns:
        None

    """
    # Create the new table object
    new_table = sa.Table(table_name, sqlite_meta)

    dbf_filename = DBF_TABLES_FILENAMES[table_name]
    filedata = ds.get_file(refyear, dbf_filename)

    ferc1_dbf = dbfread.DBF(
        dbf_filename,
        ignore_missing_memofile=True,
        filedata=filedata
    )

    # Add Columns to the table
    for field in ferc1_dbf.fields:
        if field.name == '_NullFlags':
            continue
        col_name = dbc_map[table_name][field.name]
        if (table_name, col_name) in bad_cols:
            continue
        col_type = DBF_TYPES[field.type]
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
            logger.warning(f"Missing cols on {tname}")

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
    for table, dbf_filename in DBF_TABLES_FILENAMES.items():
        try:
            dbc = ds.get_file(year, dbf_filename)
        except KeyError:
            # Not all tables exist in all years, so this is acceptable
            dbc = None

        if dbc is None:
            continue

        dbf_fields = dbfread.DBF(
            "", filedata=dbc, ignore_missing_memofile=True).field_names
        dbf_fields = [f for f in dbf_fields if f != '_NullFlags']
        dbc_map[table] = dict(zip(dbf_fields, tf_dict[table]))
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


def define_sqlite_db(
    sqlite_engine,
    sqlite_meta,
    dbc_map,
    ds,
    ferc1_to_sqlite_settings: Ferc1ToSqliteSettings = Ferc1ToSqliteSettings()
):
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
        ferc1_to_sqlite_settings: Object containing Ferc1 to SQLite validated
            settings.

    Returns:
        None: the effects of the function are stored inside sqlite_meta

    """
    for table in ferc1_to_sqlite_settings.tables:
        add_sqlite_table(
            table_name=table,
            sqlite_meta=sqlite_meta,
            dbc_map=dbc_map,
            ds=ds,
            refyear=ferc1_to_sqlite_settings.refyear,
            bad_cols=ferc1_to_sqlite_settings.bad_cols
        )

    sqlite_meta.create_all(sqlite_engine)


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


def get_raw_df(
    ds,
    table,
    dbc_map,
    years=DataSource.from_id("ferc1").working_partitions["years"]
):
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
    dbf_filename = DBF_TABLES_FILENAMES[table]

    raw_dfs = []
    for yr in years:
        try:
            filedata = ds.get_file(yr, dbf_filename)
        except KeyError:
            continue

        new_df = pd.DataFrame(
            iter(dbfread.DBF(dbf_filename,
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


def dbf2sqlite(
    ferc1_to_sqlite_settings: Ferc1ToSqliteSettings = Ferc1ToSqliteSettings(),
    pudl_settings=None,
    clobber=False,
    datastore=None
):
    """Clone the FERC Form 1 Databsae to SQLite.

    Args:
        ferc1_to_sqlite_settings: Object containing Ferc1 to SQLite validated
            settings.
        pudl_settings (dict): Dictionary containing paths and database URLs
            used by PUDL.
        bad_cols (iterable of tuples): A list of (table, column) pairs
            indicating columns that should be skipped during the cloning
            process. Both table and column are strings in this case, the
            names of their respective entities within the database metadata.
        datastore (Datastore): instance of a datastore to access the resources.

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
    sqlite_meta = sa.MetaData()
    sqlite_meta.reflect(sqlite_engine)

    # Get the mapping of filenames to table names and fields
    logger.info(
        f"Creating a new database schema based on {ferc1_to_sqlite_settings.refyear}.")
    datastore = Ferc1Datastore(datastore)
    dbc_map = get_dbc_map(datastore, ferc1_to_sqlite_settings.refyear)
    define_sqlite_db(
        sqlite_engine=sqlite_engine,
        sqlite_meta=sqlite_meta,
        dbc_map=dbc_map,
        ds=datastore,
        ferc1_to_sqlite_settings=ferc1_to_sqlite_settings
    )

    for table in ferc1_to_sqlite_settings.tables:
        logger.info(f"Pandas: reading {table} into a DataFrame.")
        new_df = get_raw_df(datastore, table, dbc_map,
                            years=ferc1_to_sqlite_settings.years)
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
        new_df.to_sql(
            table,
            sqlite_engine,
            if_exists='append',
            chunksize=100000,
            dtype=coltypes,
            index=False
        )

    # add the missing respondents into the respondent_id table.
    reported_ids = (
        pd.read_sql_table("f1_respondent_id", sqlite_engine)
        .respondent_id
        .unique()
    )
    observed_ids = observed_respondents(sqlite_engine)
    missing = missing_respondents(
        reported=reported_ids,
        observed=observed_ids,
        identified=PUDL_RIDS,
    )
    logger.info(
        f"Inserting {len(missing)} missing IDs into f1_respondent_id table."
    )
    with sqlite_engine.begin() as conn:
        conn.execute(
            sqlite_meta.tables['f1_respondent_id']
            .insert()
            .values(missing)
        )


###########################################################################
# Functions for extracting ferc1 tables from SQLite to PUDL
###########################################################################
def get_ferc1_meta(ferc1_engine):
    """Grab the FERC Form 1 DB metadata and check that tables exist.

    Connects to the FERC Form 1 SQLite database and reads in its metadata
    (table schemas, types, etc.) by reflecting the database. Checks to make
    sure the DB is not empty, and returns the metadata object.

    Args:
        ferc1_engine (sqlalchemy.engine.Engine): SQL Alchemy database
            connection engine for the PUDL FERC 1 DB.

    Returns:
        sqlalchemy.MetaData A SQL Alchemy metadata object, containing
        the definition of the DB structure.

    Raises:
        ValueError: If there are no tables in the SQLite Database.

    """
    # Connect to the local SQLite DB and read its structure.
    ferc1_meta = sa.MetaData()
    ferc1_meta.reflect(ferc1_engine)
    if not ferc1_meta.tables:
        raise ValueError(
            "No FERC Form 1 tables found. Is the SQLite DB initialized?"
        )
    return ferc1_meta


def extract(
    ferc1_settings: Ferc1Settings = Ferc1Settings(),
    pudl_settings=None,
):
    """Coordinates the extraction of all FERC Form 1 tables into PUDL.

    Args:
        ferc1_settings: Object containing validated settings
            relevant to FERC Form 1. Contains the tables and years to be loaded
            into PUDL.

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
    if pudl_settings is None:
        pudl_settings = pudl.workspace.setup.get_defaults()

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
    for pudl_table in ferc1_settings.tables:
        if pudl_table not in ferc1_extract_functions:
            raise ValueError(
                f"No extract function found for requested FERC Form 1 data "
                f"table {pudl_table}!"
            )
        logger.info(
            f"Converting extracted FERC Form 1 table {pudl_table} into a "
            f"pandas DataFrame.")
        ferc1_raw_dfs[pudl_table] = ferc1_extract_functions[pudl_table](
            ferc1_engine=sa.create_engine(pudl_settings["ferc1_db"]),
            ferc1_settings=ferc1_settings,
        )

    return ferc1_raw_dfs


def fuel(ferc1_engine, ferc1_settings):
    """Creates a DataFrame of f1_fuel table records with plant names, >0 fuel.

    Args:
        ferc1_engine (sqlalchemy.engine.Engine): An SQL Alchemy connection
            engine for the FERC Form 1 database.
        ferc1_settings (Ferc1Settings): Object containing validated settings
            relevant to FERC Form 1.

    Returns:
        :class:`pandas.DataFrame`: A DataFrame containing f1_fuel records that
        have plant_names and non-zero fuel amounts.

    """
    ferc1_meta = get_ferc1_meta(ferc1_engine)
    f1_fuel = ferc1_meta.tables["f1_fuel"]

    # Generate a SELECT statement that pulls all fields of the f1_fuel table,
    # but only gets records with plant names and non-zero fuel amounts:
    f1_fuel_select = (
        sa.sql.select(f1_fuel)
        .where(f1_fuel.c.fuel != '')
        .where(f1_fuel.c.fuel_quantity > 0)
        .where(f1_fuel.c.plant_name != '')
        .where(f1_fuel.c.report_year.in_(ferc1_settings.years))
    )
    # Use the above SELECT to pull those records into a DataFrame:
    return pd.read_sql(f1_fuel_select, ferc1_engine)


def plants_steam(ferc1_engine, ferc1_settings):
    """
    Create a :class:`pandas.DataFrame` containing valid raw f1_steam records.

    Selected records must indicate a plant capacity greater than 0, and include
    a non-null plant name.

    Args:
        ferc1_engine (sqlalchemy.engine.Engine): An SQL Alchemy connection
            engine for the FERC Form 1 database.
        ferc1_settings (Ferc1Settings): Object containing validated settings
            relevant to FERC Form 1.

    Returns:
        pandas.DataFrame: A DataFrame containing f1_steam records that have
        plant names and non-zero capacities.

    """
    ferc1_meta = get_ferc1_meta(ferc1_engine)
    f1_steam = ferc1_meta.tables["f1_steam"]
    f1_steam_select = (
        sa.sql.select(f1_steam)
        .where(f1_steam.c.report_year.in_(ferc1_settings.years))
        .where(f1_steam.c.plant_name != '')
        .where(f1_steam.c.tot_capacity > 0.0)
    )

    return pd.read_sql(f1_steam_select, ferc1_engine)


def plants_small(ferc1_engine, ferc1_settings):
    """Creates a DataFrame of f1_small for records with minimum data criteria.

    Args:
        ferc1_engine (sqlalchemy.engine.Engine): An SQL Alchemy connection
            engine for the FERC Form 1 database.
        ferc1_settings (Ferc1Settings): Object containing validated settings
            relevant to FERC Form 1.

    Returns:
        pandas.DataFrame: A DataFrame containing f1_small records that have
        plant names and non zero demand, generation, operations,
        maintenance, and fuel costs.
    """
    ferc1_meta = get_ferc1_meta(ferc1_engine)
    f1_small = ferc1_meta.tables["f1_gnrt_plant"]
    f1_small_select = (
        sa.sql.select(f1_small)
        .where(f1_small.c.report_year.in_(ferc1_settings.years))
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

    return pd.read_sql(f1_small_select, ferc1_engine)


def plants_hydro(ferc1_engine, ferc1_settings):
    """Creates a DataFrame of f1_hydro for records that have plant names.

    Args:
        ferc1_engine (sqlalchemy.engine.Engine): An SQL Alchemy connection
            engine for the FERC Form 1 database.
        ferc1_settings (Ferc1Settings): Object containing validated settings
            relevant to FERC Form 1.

    Returns:
        pandas.DataFrame: A DataFrame containing f1_hydro records that have
        plant names.

    """
    ferc1_meta = get_ferc1_meta(ferc1_engine)
    f1_hydro = ferc1_meta.tables["f1_hydro"]

    f1_hydro_select = (
        sa.sql.select(f1_hydro)
        .where(f1_hydro.c.plant_name != '')
        .where(f1_hydro.c.report_year.in_(ferc1_settings.years))
    )

    return pd.read_sql(f1_hydro_select, ferc1_engine)


def plants_pumped_storage(ferc1_engine, ferc1_settings):
    """Creates a DataFrame of f1_plants_pumped_storage records with plant names.

    Args:
        ferc1_engine (sqlalchemy.engine.Engine): An SQL Alchemy connection
            engine for the FERC Form 1 database.
        ferc1_settings (Ferc1Settings): Object containing validated settings
            relevant to FERC Form 1.

    Returns:
        pandas.DataFrame: A DataFrame containing f1_plants_pumped_storage
        records that have plant names.

    """
    ferc1_meta = get_ferc1_meta(ferc1_engine)
    f1_pumped_storage = ferc1_meta.tables["f1_pumped_storage"]

    # Removing the empty records.
    # This reduces the entries for 2015 from 272 records to 27.
    f1_pumped_storage_select = (
        sa.sql.select(f1_pumped_storage)
        .where(f1_pumped_storage.c.plant_name != '')
        .where(f1_pumped_storage.c.report_year.in_(ferc1_settings.years))
    )

    return pd.read_sql(f1_pumped_storage_select, ferc1_engine)


def plant_in_service(ferc1_engine, ferc1_settings):
    """Creates a DataFrame of the fields of plant_in_service_ferc1.

    Args:
        ferc1_engine (sqlalchemy.engine.Engine): An SQL Alchemy connection
            engine for the FERC Form 1 database.
        ferc1_settings (Ferc1Settings): Object containing validated settings
            relevant to FERC Form 1.

    Returns:
        pandas.DataFrame: A DataFrame containing all plant_in_service_ferc1
        records.

    """
    ferc1_meta = get_ferc1_meta(ferc1_engine)
    f1_plant_in_srvce = ferc1_meta.tables["f1_plant_in_srvce"]
    f1_plant_in_srvce_select = (
        sa.sql.select(f1_plant_in_srvce)
        .where(f1_plant_in_srvce.c.report_year.in_(ferc1_settings.years))
    )

    return pd.read_sql(f1_plant_in_srvce_select, ferc1_engine)


def purchased_power(ferc1_engine, ferc1_settings):
    """Creates a DataFrame the fields of purchased_power_ferc1.

    Args:
        ferc1_engine (sqlalchemy.engine.Engine): An SQL Alchemy connection
            engine for the FERC Form 1 database.
        ferc1_settings (Ferc1Settings): Object containing validated settings
            relevant to FERC Form 1.

    Returns:
        pandas.DataFrame: A DataFrame containing all purchased_power_ferc1
        records.

    """
    ferc1_meta = get_ferc1_meta(ferc1_engine)
    f1_purchased_pwr = ferc1_meta.tables["f1_purchased_pwr"]
    f1_purchased_pwr_select = (
        sa.sql.select(f1_purchased_pwr)
        .where(f1_purchased_pwr.c.report_year.in_(ferc1_settings.years))
    )

    return pd.read_sql(f1_purchased_pwr_select, ferc1_engine)


def accumulated_depreciation(ferc1_engine, ferc1_settings):
    """Creates a DataFrame of the fields of accumulated_depreciation_ferc1.

    Args:
        ferc1_engine (sqlalchemy.engine.Engine): An SQL Alchemy connection
            engine for the FERC Form 1 database.
        ferc1_settings (Ferc1Settings): Object containing validated settings
            relevant to FERC Form 1.

    Returns:
        :class:`pandas.DataFrame`: A DataFrame containing all
        accumulated_depreciation_ferc1 records.

    """
    ferc1_meta = get_ferc1_meta(ferc1_engine)
    f1_accumdepr_prvsn = ferc1_meta.tables["f1_accumdepr_prvsn"]
    f1_accumdepr_prvsn_select = (
        sa.sql.select(f1_accumdepr_prvsn)
        .where(f1_accumdepr_prvsn.c.report_year.in_(ferc1_settings.years))
    )

    return pd.read_sql(f1_accumdepr_prvsn_select, ferc1_engine)
