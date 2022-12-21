"""Extract FERC Form 1 data from SQLite DBs derived from original DBF or XBRL files.

The FERC Form 1 data is available in two primary formats, spanning different years. The
early digital data (1994-2020) was distributed using annual Visual FoxPro databases.
Starting in 2021, the agency moved to using XBRL (a dialect of XML) published via an
RSS feed one filing at a time. First we convert both of those difficult to use original
formats into relational databases (currently stored in SQLite). We use those
databases as the starting point for our extensive cleaning and reorganization of a small
portion of the available tables into a well normalized database that covers all the
years of available data. The complete input databases are published separately to
provide users access to all of the original tables, since we've only been able to
clean up a small subset of them.

The conversion from both DBF and XBRL to SQLite is coordinated by the
:mod:`pudl.convert.ferc_to_sqlite` script. The code for the XBRL to SQLite conversion
is used across all the modern FERC forms, and is contained in a standalone package:

https://github.com/catalyst-cooperative/ferc-xbrl-extractor

The code for converting the older FERC 1 DBF files into an SQLite DB is contained in
this module.

One challenge with both of these data sources is that each year of data is treated as a
standalone resource by FERC. The databases are not explicitly linked together across
years. Over time the structure of the Visual FoxPro DB has changed as new tables and
fields have been added. In order to be able to use the data to do analyses across many
years, we need to bring all of it into a unified structure. These structural changes
have only ever been additive -- more recent versions of the DBF databases contain all
the tables and fields that existed in earlier versions.

PUDL uses the most recently released year of DBF data (2020) as a template for the
database schema, since it is capable of containing all the fields and tables found in
the other years.  The structure of the database is also informed by other documentation
we have been able to compile over the years from the FERC website and other sources.
Copies of these resoruces are included in the :doc:`FERC Form 1 data source
documentation </data_sources/ferc1>`

Using this inferred structure PUDL creates an SQLite database mirroring the FERC
database using :mod:`sqlalchemy`. Then we use a python package called `dbfread
<https://dbfread.readthedocs.io/en/latest/>`__ to extract the data from the DBF tables,
and insert it virtually unchanged into the SQLite database.

Note that many quantities in the Visual FoxPro databases are tied not just to a
particular table and column, but to a row number within an individual filing, and
those row numbers have changed slowly over the years for some tables as rows have been
added or removed from the form. The ``f1_row_lit_tbl`` table contains a record of these
changes, and can be used to align reported quantities across time.

The one significant change we make to the raw input data is to ensure that there's a
master table of the all the respondent IDs and respondent names. All the other tables
refer to this table. Unlike the other tables the ``f1_respondent_id`` table has no
``report_year`` and so it represents a merge of all the years of data. In the event that
the name associated with a given respondent ID has changed over time, we retain the most
recently reported name.

Note that there are a small number of respondent IDs that **do not** appear in any year
of the ``f1_respondent_id`` table, but that **do** appear in the data tables. We add
these observed but not directly reported IDs to the ``f1_respondent_id`` table and have
done our best to identify what utility they correspond to based on the assets associated
with those respondent IDs.

This SQLite compilation of the original FERC Form 1 databases accommodates all
116 tables from all the published years of DBF data (1994-2020) and takes up about 1GB
of space on disk. You can interact with the most recent development version of this
database online at:

https://data.catalyst.coop/ferc1
"""
import csv
import importlib
import io
import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any, Literal

import pandas as pd
import sqlalchemy as sa
from dbfread import DBF, FieldParser

import pudl
from pudl.metadata.classes import DataSource
from pudl.metadata.constants import DBF_TABLES_FILENAMES
from pudl.settings import Ferc1DbfToSqliteSettings, Ferc1Settings
from pudl.workspace.datastore import Datastore

logger = pudl.logging_helpers.get_logger(__name__)

DBF_TYPES = {
    "C": sa.String,
    "D": sa.Date,
    "F": sa.Float,
    "I": sa.Integer,
    "L": sa.Boolean,
    "M": sa.Text,  # 10 digit .DBT block number, stored as a string...
    "N": sa.Float,
    "T": sa.DateTime,
    "0": sa.Integer,  # based on dbf2sqlite mapping
    "B": "XXX",  # .DBT block number, binary string
    "@": "XXX",  # Timestamp... Date = Julian Day, Time is in milliseconds?
    "+": "XXX",  # Autoincrement (e.g. for IDs)
    "O": "XXX",  # Double, 8 bytes
    "G": "XXX",  # OLE 10 digit/byte number of a .DBT block, stored as string
}
"""dict: A mapping of DBF field types to SQLAlchemy Column types.

This dictionary maps the strings which are used to denote field types in the DBF objects
to the corresponding generic SQLAlchemy Column types: These definitions come from a
combination of the dbfread example program dbf2sqlite and this DBF file format
documentation page: http://www.dbase.com/KnowledgeBase/int/db7_file_fmt.htm

Unmapped types left as 'XXX' which should result in an error if encountered.
"""

TABLE_NAME_MAP: dict[str, dict[str, str]] = {
    "fuel_ferc1": {
        "dbf": "f1_fuel",
        "xbrl": "steam_electric_generating_plant_statistics_large_plants_fuel_statistics_402",
    },
    "plants_steam_ferc1": {
        "dbf": "f1_steam",
        "xbrl": "steam_electric_generating_plant_statistics_large_plants_402",
    },
    "plants_small_ferc1": {
        "dbf": "f1_gnrt_plant",
        "xbrl": "generating_plant_statistics_410",
    },
    "plants_hydro_ferc1": {
        "dbf": "f1_hydro",
        "xbrl": "hydroelectric_generating_plant_statistics_large_plants_406",
    },
    "plants_pumped_storage_ferc1": {
        "dbf": "f1_pumped_storage",
        "xbrl": "pumped_storage_generating_plant_statistics_large_plants_408",
    },
    "plant_in_service_ferc1": {
        "dbf": "f1_plant_in_srvce",
        "xbrl": "electric_plant_in_service_204",
    },
    "purchased_power_ferc1": {
        "dbf": "f1_purchased_pwr",
        "xbrl": "purchased_power_326",
    },
    "electric_oandm_ferc1": {
        "dbf": "f1_324_elc_expns",
        "xbrl": "electric_operations_and_maintenance_expenses_320",
    },
    "electric_energy_sources_ferc1": {
        "dbf": "f1_elctrc_erg_acct",
        "xbrl": "electric_energy_account_401a",
    },
    "electric_energy_dispositions_ferc1": {
        "dbf": "f1_elctrc_erg_acct",
        "xbrl": "electric_energy_account_401a",
    },
    "utility_plant_summary_ferc1": {
        "dbf": "f1_utltyplnt_smmry",
        "xbrl": "summary_of_utility_plant_and_accumulated_provisions_for_depreciation_amortization_and_depletion_200",
    },
    "transmission_ferc1": {
        "dbf": "f1_xmssn_line",
        "xbrl": "transmission_line_statistics_422",
    },
    "balance_sheet_assets_ferc1": {
        "dbf": "f1_comp_balance_db",
        "xbrl": "comparative_balance_sheet_assets_and_other_debits_110",
    },
    "depreciation_amortization_summary_ferc1": {
        "dbf": "f1_dacs_epda",
        "xbrl": "summary_of_depreciation_and_amortization_charges_section_a_336",
    },
}
"""A mapping of PUDL DB table names to their XBRL and DBF source table names."""

PUDL_RIDS: dict[int, str] = {
    514: "AEP Texas",
    519: "Upper Michigan Energy Resources Company",
    522: "Luning Energy Holdings LLC, Invenergy Investments",
    529: "Tri-State Generation and Transmission Association",
    531: "Basin Electric Power Cooperative",
}
"""Missing FERC 1 Respondent IDs for which we have identified the respondent."""


def missing_respondents(
    reported: Iterable[int],
    observed: Iterable[int],
    identified: dict[int, str],
) -> list[dict[str, int | str]]:
    """Fill in missing respondents for the f1_respondent_id table.

    Args:
        reported: Respondent IDs appearing in the f1_respondent_id table.
        observed: Respondent IDs appearing anywhere in the FERC 1 DB.
        identified: A dictionary mapping respondent_id: to respondent_name for those
            observed but unreported respondent IDs we've been able to identify based on
            circumstantial evidence. See :py:const:`pudl.extract.ferc1.PUDL_RIDS`.

    Returns:
        A list of dictionaries representing minimal f1_respondent_id table
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
                    "respondent_name": f"{identified[rid]} (PUDL determined)",
                },
            )
        else:
            records.append(
                {
                    "respondent_id": rid,
                    "respondent_name": f"Missing Respondent {rid}",
                },
            )
    return records


def observed_respondents(ferc1_engine: sa.engine.Engine) -> set[int]:
    """Compile the set of all observed respondent IDs found in the FERC 1 database.

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
    observed = set()
    for table in f1_table_meta.values():
        if "respondent_id" in table.columns:
            observed = observed.union(
                set(
                    pd.read_sql_table(
                        table.name, ferc1_engine, columns=["respondent_id"]
                    ).respondent_id
                )
            )
    return observed


class Ferc1DbfDatastore:
    """A wrapper to standardize access to FERC 1 resources by year and filename.

    The internal directory structure of the published zipfiles containing FERC Form 1
    data changes from year to year unpredictably, but the names of the individual
    database files which we parse is consistent. This wrapper encapsulates the annual
    directory structure variation and lets us request a particular filename by year
    without needing to understand the directory structure.
    """

    PACKAGE_PATH = "pudl.package_data.ferc1"

    def __init__(self, datastore: Datastore):
        """Instantiate datastore wrapper for ferc1 resources."""
        self.datastore = datastore
        self._cache: dict[int, io.BytesIO] = {}
        self.dbc_path: dict[int, Path] = {}

        with importlib.resources.open_text(self.PACKAGE_PATH, "file_map.csv") as f:
            for row in csv.DictReader(f):
                year = int(row["year"])
                path = Path(row["path"])
                self.dbc_path[year] = path

    def get_dir(self, year: int) -> Path:
        """Get path to directory containing DBF files for an annual archive."""
        if year not in self.dbc_path:
            raise ValueError(f"No ferc1 data for year {year}")
        return self.dbc_path[year]

    def get_file(self, year: int, filename: str):
        """Opens given ferc1 file from the corresponding archive."""
        if year not in self._cache:
            self._cache[year] = self.datastore.get_zipfile_resource(
                "ferc1", year=year, data_format="dbf"
            )
        archive = self._cache[year]
        try:
            return archive.open((self.get_dir(year) / filename).as_posix())
        except KeyError:
            raise KeyError(f"{filename} not available for year {year} in ferc1.")


def add_sqlite_table(
    table_name: str,
    sqlite_meta: sa.schema.MetaData,
    dbc_map: dict[str, dict[str, str]],
    ferc1_dbf_ds: Ferc1DbfDatastore,
    refyear: int | None = None,
) -> None:
    """Add a new Table to the FERC Form 1 database schema.

    Creates a new sa.Table object named ``table_name`` and add it to the database schema
    contained in ``sqlite_meta``. Use the information in the dictionary ``dbc_map`` to
    translate between the DBF filenames in the datastore (e.g. ``F1_31.DBF``), and the
    full name of the table in the FoxPro database (e.g. ``f1_fuel``) and also between
    truncated column names extracted from that DBF file, and the full column names
    extracted from the DBC file. Read the column datatypes out of each DBF file and use
    them to define the columns in the new Table object.

    Args:
        table_name: The name of the new table to be added to the database schema.
        sqlite_meta: The database schema to which the newly defined
            :class:`sqlalchemy.Table` will be added.
        dbc_map: A dictionary of dictionaries
        ferc1_dbf_ds: Initialized FERC1 DBF datastore.
        refyear: Reference year to use as a template for the database schema.
    """
    if refyear is None:
        refyear = max(DataSource.from_id("ferc1").working_partitions["years"])

    new_table = sa.Table(table_name, sqlite_meta)

    dbf_filename = DBF_TABLES_FILENAMES[table_name]
    filedata = ferc1_dbf_ds.get_file(refyear, dbf_filename)

    ferc1_dbf = DBF(dbf_filename, ignore_missing_memofile=True, filedata=filedata)

    # Add Columns to the table
    for field in ferc1_dbf.fields:
        if field.name == "_NullFlags":
            continue
        col_name = dbc_map[table_name][field.name]
        col_type = DBF_TYPES[field.type]
        if col_type == sa.String:
            col_type = sa.String(length=field.length)
        new_table.append_column(sa.Column(col_name, col_type))

    col_names = [c.name for c in new_table.columns]

    if table_name == "f1_respondent_id":
        new_table.append_constraint(
            sa.PrimaryKeyConstraint("respondent_id", sqlite_on_conflict="REPLACE")
        )

    if ("respondent_id" in col_names) and (table_name != "f1_respondent_id"):
        new_table.append_constraint(
            sa.ForeignKeyConstraint(
                columns=[
                    "respondent_id",
                ],
                refcolumns=["f1_respondent_id.respondent_id"],
            )
        )


def get_fields(filedata) -> dict[str, list[str]]:
    """Produce the expected table names and fields from a DBC file.

    Args:
        filedata: Contents of the DBC file from which to extract.

    Returns:
        Dictionary mapping table names to the list of fields contained in that table.
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


def get_dbc_map(
    ferc1_dbf_ds: Ferc1DbfDatastore,
    year: int,
) -> dict[str, dict[str, str]]:
    """Extract names of all tables and fields from a FERC Form 1 DBC file.

    Read the DBC file associated with the FERC Form 1 database for the given ``year``,
    and extract all embedded table and column names.

    Args:
        ferc1_dbf_ds: Initialized FERC 1 datastore.
        year: The year of data from which the database table and column names are to be
            extracted. Typically this is expected to be the most recently available year
            of FERC Form 1 DBF data.

    Returns:
        A dictionary whose keys are the long table names extracted from the DBC file,
        and whose values are dictionaries mapping the first of which is the full name of
        each field in the table with the same name as the key, and the second of which
        is the truncated (<=10 character) long name of that field as found in the DBF
        file.
    """
    dbc = ferc1_dbf_ds.get_file(year, "F1_PUB.DBC")
    tf_dict = get_fields(dbc)

    dbc_map = {}
    for table, dbf_filename in DBF_TABLES_FILENAMES.items():
        try:
            dbc = ferc1_dbf_ds.get_file(year, dbf_filename)
        except KeyError:
            # Not all tables exist in all years, so this is acceptable
            dbc = None

        if dbc is None:
            continue

        dbf_fields = DBF("", filedata=dbc, ignore_missing_memofile=True).field_names
        dbf_fields = [f for f in dbf_fields if f != "_NullFlags"]
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
    sqlite_engine: sa.engine.Engine,
    sqlite_meta: sa.MetaData,
    dbc_map: dict[str, dict[str, str]],
    ferc1_dbf_ds: Ferc1DbfDatastore,
    ferc1_to_sqlite_settings: Ferc1DbfToSqliteSettings = Ferc1DbfToSqliteSettings(),
):
    """Defines a FERC Form 1 DB structure in a given SQLAlchemy MetaData object.

    Given a template from an existing year of FERC data, and a list of target
    tables to be cloned, convert that information into table and column names,
    and data types, stored within a SQLAlchemy MetaData object. Use that
    MetaData object (which is bound to the SQLite database) to create all the
    tables to be populated later.

    Args:
        sqlite_engine: A connection engine for an existing FERC 1 DB.
        sqlite_meta: A SQLAlchemy MetaData object which is bound to the FERC Form 1
            SQLite database.
        dbc_map: A dictionary of dictionaries, from :func:`get_dbc_map`, describing the
            table and column names stored within the FERC Form 1 FoxPro database files.
        ferc1_dbf_ds: Initialized FERC 1 Datastore.
        ferc1_to_sqlite_settings: Object containing Ferc1 to SQLite validated settings.

    Returns:
        None: the effects of the function are stored inside sqlite_meta
    """
    for table in ferc1_to_sqlite_settings.tables:
        add_sqlite_table(
            table_name=table,
            sqlite_meta=sqlite_meta,
            dbc_map=dbc_map,
            ferc1_dbf_ds=ferc1_dbf_ds,
            refyear=ferc1_to_sqlite_settings.refyear,
        )

    sqlite_meta.create_all(sqlite_engine)


class FERC1FieldParser(FieldParser):
    """A custom DBF parser to deal with bad FERC Form 1 data types."""

    def parseN(self, field, data: bytes) -> int | float | None:  # noqa: N802
        """Augments the Numeric DBF parser to account for bad FERC data.

        There are a small number of bad entries in the backlog of FERC Form 1
        data. They take the form of leading/trailing zeroes or null characters
        in supposedly numeric fields, and occasionally a naked '.'

        Accordingly, this custom parser strips leading and trailing zeros and
        null characters, and replaces a bare '.' character with zero, allowing
        all these fields to be cast to numeric values.

        Args:
            field: The DBF field being parsed.
            data: Binary data (bytes) read from the DBF file.
        """  # noqa: D417
        # Strip whitespace, null characters, and zeroes
        data = data.strip().strip(b"*\x00").lstrip(b"0")
        # Replace bare periods (which are non-numeric) with zero.
        if data == b".":
            data = b"0"
        return super().parseN(field, data)


def get_raw_df(
    ferc1_dbf_ds: Ferc1DbfDatastore,
    table: str,
    dbc_map: dict[str, dict[str, str]],
    years: list[int] = DataSource.from_id("ferc1").working_partitions["years"],
) -> pd.DataFrame:
    """Combine several years of a given FERC Form 1 DBF table into a dataframe.

    Args:
        ferc1_dbf_ds: Initialized FERC 1 DBF datastore
        table: The name of the FERC Form 1 table from which data is read.
        dbc_map: A dictionary returned by :func:`get_dbc_map`, describing the table and
            column names stored within the FERC Form 1 FoxPro database files.
        years: List of years to be combined into a single DataFrame.

    Returns:
        A DataFrame containing multiple years of FERC Form 1 data for the requested
        table.
    """
    dbf_filename = DBF_TABLES_FILENAMES[table]

    raw_dfs = []
    for yr in years:
        try:
            filedata = ferc1_dbf_ds.get_file(yr, dbf_filename)
        except KeyError:
            continue

        new_df = pd.DataFrame(
            iter(
                DBF(
                    dbf_filename,
                    encoding="latin1",
                    parserclass=FERC1FieldParser,
                    ignore_missing_memofile=True,
                    filedata=filedata,
                )
            )
        )
        raw_dfs = raw_dfs + [
            new_df,
        ]

    if raw_dfs:
        return (
            pd.concat(raw_dfs, sort=True)
            .drop("_NullFlags", axis=1, errors="ignore")
            .rename(dbc_map[table], axis=1)
        )


def dbf2sqlite(
    ferc1_to_sqlite_settings: Ferc1DbfToSqliteSettings | None = None,
    pudl_settings: dict[str, Any] | None = None,
    clobber: bool = False,
    datastore: Datastore | None = None,
) -> None:
    """Clone the FERC Form 1 Visual FoxPro databases into SQLite.

    Args:
        ferc1_to_sqlite_settings: Object containing Ferc1 to SQLite validated settings.
            If None (the default) then a default :class:`Ferc1DbfToSqliteSettings`
            object will be used.
        pudl_settings: Dictionary containing paths and database URLs used by PUDL.
        clobber: Whether to clobber an existing FERC 1 database.
        datastore: instance of a datastore providing access to raw resources.
    """
    if not ferc1_to_sqlite_settings:
        ferc1_to_sqlite_settings = Ferc1DbfToSqliteSettings()
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
        f"Creating a new database schema based on {ferc1_to_sqlite_settings.refyear}."
    )
    ferc1_dbf_ds = Ferc1DbfDatastore(datastore)
    dbc_map = get_dbc_map(ferc1_dbf_ds, ferc1_to_sqlite_settings.refyear)
    define_sqlite_db(
        sqlite_engine=sqlite_engine,
        sqlite_meta=sqlite_meta,
        dbc_map=dbc_map,
        ferc1_dbf_ds=ferc1_dbf_ds,
        ferc1_to_sqlite_settings=ferc1_to_sqlite_settings,
    )

    for table in ferc1_to_sqlite_settings.tables:
        logger.info(f"Pandas: reading {table} into a DataFrame.")
        new_df = get_raw_df(
            ferc1_dbf_ds, table, dbc_map, years=ferc1_to_sqlite_settings.years
        )
        # Because this table has no year in it, there would be multiple
        # definitions of respondents if we didn't drop duplicates.
        if table == "f1_respondent_id":
            new_df = new_df.drop_duplicates(subset="respondent_id", keep="last")
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
            if_exists="append",
            chunksize=100000,
            dtype=coltypes,
            index=False,
        )

    # add the missing respondents into the respondent_id table.
    reported_ids = pd.read_sql_table(
        "f1_respondent_id", sqlite_engine
    ).respondent_id.unique()
    observed_ids = observed_respondents(sqlite_engine)
    missing = missing_respondents(
        reported=reported_ids,
        observed=observed_ids,
        identified=PUDL_RIDS,
    )
    logger.info(f"Inserting {len(missing)} missing IDs into f1_respondent_id table.")
    with sqlite_engine.begin() as conn:
        conn.execute(sqlite_meta.tables["f1_respondent_id"].insert().values(missing))


###########################################################################
# Functions for extracting ferc1 tables from SQLite to PUDL
###########################################################################
def get_ferc1_meta(ferc1_engine: sa.engine.Engine) -> sa.MetaData:
    """Grab the FERC Form 1 DB metadata and check that tables exist.

    Connects to the FERC Form 1 SQLite database and reads in its metadata
    (table schemas, types, etc.) by reflecting the database. Checks to make
    sure the DB is not empty, and returns the metadata object.

    Args:
        ferc1_engine: SQL Alchemy database connection engine for the PUDL FERC 1 DB.

    Returns:
        A SQL Alchemy metadata object, containing the definition of the DB structure.

    Raises:
        ValueError: If there are no tables in the SQLite Database.
    """
    # Connect to the local SQLite DB and read its structure.
    ferc1_meta = sa.MetaData()
    ferc1_meta.reflect(ferc1_engine)
    if not ferc1_meta.tables:
        raise ValueError("No FERC Form 1 tables found. Is the SQLite DB initialized?")
    return ferc1_meta


def extract_dbf(
    ferc1_settings: Ferc1Settings | None = None,
    pudl_settings: dict[str, Any] | None = None,
) -> dict[str, pd.DataFrame]:
    """Coordinates the extraction of all FERC Form 1 tables into PUDL.

    Args:
        ferc1_settings: Object containing validated settings relevant to FERC Form 1.
            Contains the tables and years to be loaded into PUDL.
        pudl_settings: A PUDL settings dictionary.

    Returns:
        A dictionary of DataFrames, with the names of PUDL database tables as the keys.
        These are the raw unprocessed dataframes, reflecting the data as it is in the
        FERC Form 1 DB, for passing off to the data tidying and cleaning fuctions found
        in the :mod:`pudl.transform.ferc1` module.

    Raises:
        ValueError: If the FERC table requested is not integrated into PUDL
    """
    if pudl_settings is None:
        pudl_settings = pudl.workspace.setup.get_defaults()

    if ferc1_settings is None:
        ferc1_settings = Ferc1Settings()

    ferc1_raw_dfs = {}
    for pudl_table in ferc1_settings.tables:
        if pudl_table not in TABLE_NAME_MAP:
            raise ValueError(
                f"No extract function found for requested FERC Form 1 data "
                f"table {pudl_table}!"
            )
        logger.info(
            f"Converting extracted FERC Form 1 table {pudl_table} into a "
            f"pandas DataFrame from DBF table."
        )
        ferc1_raw_dfs[pudl_table] = extract_dbf_generic(
            ferc1_engine=sa.create_engine(pudl_settings["ferc1_db"]),
            ferc1_settings=ferc1_settings,
            table_name=TABLE_NAME_MAP[pudl_table]["dbf"],
        )

    return ferc1_raw_dfs


def extract_xbrl(
    ferc1_settings: Ferc1Settings | None = None,
    pudl_settings: dict[str, Any] | None = None,
) -> dict[str, dict[Literal["duration", "instant"], pd.DataFrame]]:
    """Coordinates the extraction of all FERC Form 1 tables into PUDL from XBRL data.

    Args:
        ferc1_settings: Object containing validated settings relevant to FERC Form 1.
            Contains the tables and years to be loaded into PUDL.
        pudl_settings: A PUDL settings dictionary.

    Returns:
        A dictionary where keys are the names of the PUDL database tables, values are
        dictionaries of DataFrames coresponding to the instant and duration tables from
        the XBRL derived FERC 1 database.

    Raises:
        ValueError: If the FERC table requested is not yet integrated into PUDL.
    """
    if pudl_settings is None:
        pudl_settings = pudl.workspace.setup.get_defaults()

    if ferc1_settings is None:
        ferc1_settings = Ferc1Settings()

    ferc1_raw_dfs = {}
    if not ferc1_settings.xbrl_years:
        return ferc1_raw_dfs

    for pudl_table in ferc1_settings.tables:
        if pudl_table not in TABLE_NAME_MAP:
            raise ValueError(f"{pudl_table} not found in the list of known tables.")
        if "xbrl" not in TABLE_NAME_MAP[pudl_table]:
            raise ValueError(f"No XBRL tables have been associated with {pudl_table}.")

        logger.info(
            f"Converting extracted FERC Form 1 table {pudl_table} into a "
            f"pandas DataFrame from XBRL table."
        )

        # Attempt to extract both duration and instant tables
        xbrl_table = TABLE_NAME_MAP[pudl_table]["xbrl"]
        ferc1_raw_dfs[pudl_table] = {}
        for period_type in ["duration", "instant"]:
            ferc1_raw_dfs[pudl_table][period_type] = extract_xbrl_generic(
                ferc1_engine=sa.create_engine(pudl_settings["ferc1_xbrl_db"]),
                ferc1_settings=ferc1_settings,
                table_name=f"{xbrl_table}_{period_type}",
            )

    return ferc1_raw_dfs


def extract_xbrl_generic(
    ferc1_engine: sa.engine.Engine,
    ferc1_settings: Ferc1Settings,
    table_name: str,
) -> pd.DataFrame:
    """Extract a single FERC Form 1 XBRL table by name.

    Args:
        ferc1_engine: An SQL Alchemy connection engine for the FERC Form 1 database.
        ferc1_settings: Object containing validated settings relevant to FERC Form 1.
        table_name: Name of the XBRL table to extract, as it appears in the original
            XBRL derived SQLite database.
    """
    # Get XBRL DB metadata
    ferc1_meta = get_ferc1_meta(ferc1_engine)

    # Not every table contains both instant and duration
    # Return empty dataframe if table doesn't exist
    if table_name not in ferc1_meta.tables:
        return pd.DataFrame()

    # Identification table used to get the filing year
    id_table = "identification_001_duration"

    return pd.read_sql(
        f"""
        SELECT {table_name}.*, {id_table}.report_year FROM {table_name}
        JOIN {id_table} ON {id_table}.filing_name = {table_name}.filing_name
        WHERE {id_table}.report_year BETWEEN :min_year AND :max_year;
        """,
        con=ferc1_engine,
        params={
            "min_year": min(ferc1_settings.xbrl_years),
            "max_year": max(ferc1_settings.xbrl_years),
        },
    )


def extract_dbf_generic(
    ferc1_engine: sa.engine.Engine,
    ferc1_settings: Ferc1Settings,
    table_name: str,
) -> pd.DataFrame:
    """Extract a single FERC Form 1 DBF table by name.

    Args:
        ferc1_engine: An SQL Alchemy connection engine for the FERC Form 1 database.
        ferc1_settings: Object containing validated settings relevant to FERC Form 1.
        table_name: Name of desired output table to produce.
    """
    return pd.read_sql_query(
        f"SELECT * FROM {table_name} "  # nosec: B608
        "WHERE report_year BETWEEN :min_year AND :max_year;",
        con=ferc1_engine,
        params={
            "min_year": min(ferc1_settings.dbf_years),
            "max_year": max(ferc1_settings.dbf_years),
        },
    )


def extract_xbrl_metadata(
    ferc1_settings: Ferc1Settings | None = None,
    pudl_settings: dict[Any] | None = None,
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    """Extract the FERC 1 XBRL Taxonomy metadata we've stored as JSON.

    Args:
        ferc1_settings: Settings object used to identify which tables metadata should
            be extracted for.
        pudl_settings: PUDL settings dictionary used to look up the location of the
            XBRL metadata.

    Returns:
        A dictionary keyed by PUDL table name, with an instant and a duration entry
        for each table, corresponding to the metadata for each of the respective instant
        or duration tables from XBRL if they exist. Table metadata is returned as a list
        of dictionaries, each of which can be interpreted as a row in a tabular
        structure, with each row annotating a separate XBRL concept from the FERC 1
        filings. If there is no instant/duration table, an empty list is returned
        instead.
    """
    if pudl_settings is None:
        pudl_settings = pudl.workspace.setup.get_defaults()

    if ferc1_settings is None:
        ferc1_settings = Ferc1Settings()

    with open(pudl_settings["ferc1_xbrl_taxonomy_metadata"]) as f:
        xbrl_meta_all = json.load(f)

    xbrl_meta_out = {}
    for pudl_table in ferc1_settings.tables:
        if pudl_table not in TABLE_NAME_MAP:
            raise ValueError(f"{pudl_table} not found in the list of known tables.")
        if "xbrl" not in TABLE_NAME_MAP[pudl_table]:
            raise ValueError(f"No XBRL tables have been associated with {pudl_table}.")

        logger.info(
            f"Reading XBRL Taxonomy metadata for FERC Form 1 table {pudl_table}"
        )
        # Attempt to extract both duration and instant tables
        xbrl_table = TABLE_NAME_MAP[pudl_table]["xbrl"]
        xbrl_meta_out[pudl_table] = {}

        for period in ["instant", "duration"]:
            try:
                xbrl_meta_out[pudl_table][period] = xbrl_meta_all[
                    f"{xbrl_table}_{period}"
                ]
            except KeyError:
                xbrl_meta_out[pudl_table][period] = []

    return xbrl_meta_out
