"""Routines for exporting data from PUDL for use elsewhere.

Function names should be indicative of the format of the thing that's being
exported (e.g. CSV, Excel spreadsheets, parquet files, HDF5).
"""

import datetime
import hashlib
import logging
import os
import pathlib
import re
import uuid

import datapackage
import goodtables
import pandas as pd
import sqlalchemy as sa
import tableschema

import pudl
from pudl import constants as pc

logger = logging.getLogger(__name__)


def simplify_sql_type(sql_type, field_name=""):
    """
    Convert an SQL Alchemy Type into a string type for use in a Table Schema.

    See: https://frictionlessdata.io/specs/table-schema/

    Args:
        sql_type (sqlalchemy.sql.sqltypes.type instance): The type associated
            with the column being processed, as extracted from a MetaData
            object reflecting the database being packaged. Should be taken
            from the list of Column objects associated with a Table object.
        field_name (string, optional): The name of the field, which may offer
            more context as to the nature of the field (e.g. an integer field
            whose name ends in _year is a year).
    Returns:
        simple_type (string): A string representing a simple data type, allowed
            in the Table Schema standard.
    """

    type_map = {
        'integer': (sa.Integer,),
        'number': (sa.Float, sa.Numeric),
        'boolean': (sa.Boolean,),
        'string': (sa.String, sa.Enum),
        'date': (sa.Date,),
        'time': (sa.Time,),
        'datetime': (sa.DateTime,),
        'duration': (sa.Interval,),
        'object': (sa.JSON,),
        'array': (sa.ARRAY,),
    }

    for dtype in type_map:
        if isinstance(sql_type, type_map[dtype]):
            simple_type = dtype

    if (simple_type == 'integer' and re.match('.*_year$', field_name)):
        simple_type = 'year'

    return simple_type


def get_fields(table):
    """
    Generate table schema compatible list of fields from database table.

    See: https://frictionlessdata.io/specs/table-schema/

    Field attributes which are currently set by the function:
      * name (same as the database column)
      * description (taken from the database column 'comment' field.)
      * type (simplified from the SQL Alchemy Column data type)
      * constraints (only for Enum types)

    Still to be implemented:
      * constraints other than Enum

    Args:
        table (SQL Alchemy Table): The Table object to generate fields from.
    Returns:
        fields (list): A list of 'field' JSON objects, conforming to the
            Frictionless Data Table Schema standard.
    """

    fields = []
    for col in table.columns.keys():
        newfield = {}
        newfield['name'] = col
        newfield['type'] = simplify_sql_type(table.c[col].type, field_name=col)
        if isinstance(table.c[col].type, sa.sql.sqltypes.Enum):
            newfield['constraints'] = {'enum': table.c[col].type.enums}
        if table.c[col].comment:
            newfield['description'] = table.c[col].comment
        fields.append(newfield)
    return fields


def get_primary_key(table):
    """Create a primaryKey object based on an SQLAlchemy Table"""
    return table.primary_key.columns.keys()


def get_foreign_keys(table):
    """Get a list of foreignKey objects from an SQLAlchemy Table"""
    fkeys = []
    for col in table.columns:
        if col.foreign_keys:
            for k in col.foreign_keys:
                fkey = {}
                fkey["fields"] = col.name
                fkey["reference"] = {"resource": k.column.table.name,
                                     "fields": k.column.name}
                fkeys.append(fkey)
    return fkeys


def get_missing_values(table):
    """
    Get a list of missing values from an SQLAlchemy Table.

    We'll only really be able to see how this works with some data. For now it
    just returns the default value: [""].
    """
    return [""]


def get_table_schema(table):
    """
    Create a Table Schema descriptor from an SQL Alchemy table.

    See: https://frictionlessdata.io/specs/table-schema/

    There are four possible elements in the Table Schema:
      * fields (an array of field descriptors)
      * primaryKey
      * foreignKeys (an array of foreignKey objects)
      * missingValues (an array of strings to be interpreted as null)

    """
    descriptor = {}
    descriptor['fields'] = get_fields(table)
    descriptor['primaryKey'] = get_primary_key(table)
    fkeys = get_foreign_keys(table)
    if fkeys:
        descriptor['foreignKeys'] = fkeys
    descriptor['missingValues'] = get_missing_values(table)

    schema = tableschema.Schema(descriptor)
    if not schema.valid:
        raise AssertionError(
            f"""
            Invalid table schema for {table}

            Errors:
            {schema.errors}
            """
        )
    return descriptor


def get_table(tablename, testing=False):
    """
    Retrieve SQLAlchemy Table object corresponding to a PUDL DB table name.
    """
    md = sa.MetaData(bind=pudl.init.connect_db(testing=testing))
    md.reflect()
    return md.tables[tablename]


def get_tabular_data_resource(tablename, pkg_dir, testing=False):
    """
    Create a Tabular Data Resource descriptor for a PUDL DB table.

    Based on the information in the database, and some additional metadata,
    stored elsewhere (Where?!?!) this function will generate a valid Tabular
    Data Resource descriptor, according to the Frictionless Data specification,
    which can be found here:

    https://frictionlessdata.io/specs/tabular-data-resource/
    """
    table = get_table(tablename, testing=testing)

    # Where the CSV file holding the data is, relative to datapackage.json
    # This is the value that has to be embedded in the data package.
    csv_relpath = os.path.join('data', f'{tablename}.csv')
    # We need to access the file to calculate hash and size too:
    csv_abspath = os.path.join(os.path.abspath(pkg_dir), csv_relpath)

    descriptor = {}
    descriptor['profile'] = "tabular-data-resource"
    descriptor['name'] = tablename
    descriptor['path'] = csv_relpath
    descriptor['title'] = tablename  # maybe we should make this pretty...
    if table.comment:
        descriptor['description'] = table.comment
    descriptor['encoding'] = "utf-8"
    descriptor['mediatype'] = "text/csv"
    descriptor['format'] = "csv"
    descriptor['dialect'] = {
        "delimiter": ",",
        "header": True,
        "quoteChar": "\"",
        "doubleQuote": True,
        "lineTerminator": "\r\n",
        "skipInitialSpace": True,
    }
    descriptor['schema'] = get_table_schema(table)
    descriptor['bytes'] = os.path.getsize(csv_abspath)
    descriptor['hash'] = hash_csv(csv_abspath)

    # If omitted, icenses are inherited from the containing data package.
    descriptor["licenses"] = [pudl.constants.licenses['cc-by-4.0'], ]

    data_sources = \
        pudl.helpers.data_sources_from_tables([table.name, ])
    # descriptor["sources"] = \
    #    [pudl.constants.data_sources[src] for src in data_sources]
    descriptor["sources"] = []
    for src in data_sources:
        if src in pudl.constants.data_sources:
            descriptor["sources"].append({"title": src,
                                          "path": pc.base_data_urls[src]})

    resource = datapackage.Resource(descriptor)
    if not resource.valid:
        raise AssertionError(
            f"""
            Invalid tabular data resource: {resource.name}

            Errors:
            {resource.errors}
            """
        )

    return descriptor


def hash_csv(csv_path):
    """
    Calculate a SHA-i256 hash of the CSV file for data integrity checking.

    Args:
        csv_path (path-like) : Path the CSV file to hash.

    Returns:
        str: the hexdigest of the hash, with a 'sha256:' prefix.

    """
    # how big of a bit should I take?
    blocksize = 65536
    # sha256 is the fastest relatively secure hashing algorith.
    hasher = hashlib.sha256()
    # opening the file and eat it for lunch
    with open(csv_path, 'rb') as afile:
        buf = afile.read(blocksize)
        while len(buf) > 0:
            hasher.update(buf)
            buf = afile.read(blocksize)

    # returns the hash
    return f"sha256:{hasher.hexdigest()}"


def data_package(pkg_tables, pkg_skeleton, pudl_settings=None,
                 testing=False, dry_run=False):
    """
    Create a data package of requested tables and their dependencies.

    See Frictionless Data for the tabular data package specification:

    http://frictionlessdata.io/specs/tabular-data-package/

    Args:
        pkg_skeleton (dict): A python dictionary containing several
            top level elements of the data package JSON descriptor
            specific to the data package, including:
              * name: pudl-<datasource> e.g. pudl-eia923, pudl-ferc1
              * title: One line human readable description.
              * description: A paragraph long description.
              * keywords: For search purposes.
        pkg_tables (iterable): The names of database tables to include.
            Each one will be converted into a tabular data resource.
            Dependent tables will also be added to the data package.
        out_dir (path-like): The location of the packaging directory.
            The data package will be created in a subdirectory in
            this directory, according to the name of the package.

    Returns:
        data_pkg (Package): an object representing the data package,
            as defined by the datapackage library.

    """
    if pudl_settings is None:
        pudl_settings = pudl.settings.init()
    # A few paths we are going to need repeatedly:
    # out_dir is the packaging directory -- the place where packages end up
    # pkg_dir is the top level directory of this package:
    pkg_dir = os.path.abspath(os.path.join(
        pudl_settings['datapackage_dir'], pkg_skeleton["name"]))
    # data_dir is the data directory within the package directory:
    data_dir = os.path.join(pkg_dir, "data")
    # pkg_json is the datapackage.json that we ultimately output:
    pkg_json = os.path.join(pkg_dir, "datapackage.json")

    # Given the list of target tables, find all dependent tables.
    all_tables = pudl.helpers.get_dependent_tables_from_list(
        pkg_tables, testing=testing)

    # Extract the target tables and save them as CSV files.
    # We have to do this before creating the data resources
    # because the files are necessary in order to calculate
    # the file sizes and hashes.
    for t in all_tables:
        csv_out = os.path.join(data_dir, f"{t}.csv")
        os.makedirs(os.path.dirname(csv_out), exist_ok=True)
        if dry_run is True:
            logger.info(f"Skipping export of {t} to {csv_out}")
            pathlib.Path(csv_out).touch()
        else:
            df = pd.read_sql_table(t, pudl.init.connect_db(testing=testing))
            if t in pudl.constants.need_fix_inting:
                df = pudl.helpers.fix_int_na(
                    df, pudl.constants.need_fix_inting[t])
                logger.info(f"Exporting {t} to {csv_out}")
                df.to_csv(csv_out, index=False)

    # Create a tabular data resource for each of the tables.
    resources = []
    for t in all_tables:
        resources.append(
            pudl.output.export.get_tabular_data_resource(t, pkg_dir=pkg_dir))

    # resource.iter(relations=True)
    # resource.check_relations()

    data_sources = pudl.helpers.data_sources_from_tables(
        all_tables, testing=testing)
    sources = []
    for src in data_sources:
        if src in pudl.constants.data_sources:
            sources.append({"title": src,
                            "path": "idfk"})

    contributors = set()
    for src in data_sources:
        for c in pudl.constants.contributors_by_source[src]:
            contributors.add(c)

    pkg_descriptor = {
        "name": pkg_skeleton["name"],
        "profile": "tabular-data-package",
        "title": pkg_skeleton["title"],
        "description": pkg_skeleton["description"],
        "keywords": pkg_skeleton["keywords"],
        "homepage": "https://catalyst.coop/pudl/",
        "created": (datetime.datetime.utcnow().
                    replace(microsecond=0).isoformat() + 'Z'),
        "contributors": [pudl.constants.contributors[c] for c in contributors],
        "sources": sources,
        "licenses": [pudl.constants.licenses["cc-by-4.0"]],
        "resources": resources,
    }

    # Use that descriptor to instantiate a Package object
    data_pkg = datapackage.Package(pkg_descriptor)

    # Validate the data package descriptor before we go to
    if not data_pkg.valid:
        logger.warning(f"""
            Invalid tabular data package: {data_pkg.descriptor["name"]}
            Errors: {data_pkg.errors}""")

    data_pkg.save(pkg_json)

    if not dry_run:
        # Validate the data within the package using goodtables:
        report = goodtables.validate(pkg_json, row_limit=100_000)
        if not report['valid']:
            logger.warning("Data package data validation failed.")

    return data_pkg


def annotated_xlsx(df, notes_dict, tags_dict, first_cols, sheet_name,
                   xlsx_writer):
    """Output an annotated spreadsheet workbook based on compiled dataframes.

    Create annotation tab and header rows for EIA 860, EIA 923, and FERC 1
    fields in a dataframe. This is done using an Excel Writer object, which
    must be created and saved outside the function, thereby allowing multiple
    sheets and associated annotations to be compiled in the same Excel file

    Args:
    -----
        df: The dataframe for which annotations are being created
        notes_dict: dictionary with column names as keys and long annotations
            as values
        tags_dict: dictionary of dictionaries with tag categories as keys for
            outer dictionary and values are dictionaries with column names as
            keys and values are tag within the tag category
        first_cols: ordered list of columns that should come first in outfile
        sheet_name: name of data sheet in output spreadsheet
        xlsx_writer: this is an ExcelWriter object used to accumulate multiple
            tabs, which must be created outside of function, before calling the
            first time e.g. "xlsx_writer = pd.ExcelWriter('outfile.xlsx')"

    Returns:
    --------
        xlsx_writer : which must be called outside the function, after final
        use of function, for writing out to excel: "xlsx_writer.save()"

    """
    first_cols = [c for c in first_cols if c in df.columns]
    df = pudl.helpers.organize_cols(df, first_cols)

    # Transpose the original dataframe to easily add and map tags as columns
    dfnew = df.transpose()

    # For loop where tag is metadata field (e.g. data_source or data_origin) &
    # column is a nested dictionary of column name & value; maps tags_dict to
    # columns in df and creates a new column for each tag category
    for tag, column_dict in tags_dict.items():
        dfnew[tag] = dfnew.index.to_series().map(column_dict)
    # Take the new columns that were created for each tag category and add them
    # to the index
    for tag, column_dict in tags_dict.items():
        dfnew = dfnew.set_index([tag], append=True)
    # Transpose to return data fields to columns
    dfnew = dfnew.transpose()
    # Create an excel sheet for the data frame
    dfnew.to_excel(xlsx_writer, sheet_name=str(sheet_name), na_rep='NA')
    # Convert notes dictionary into a pandas series
    notes = pd.Series(notes_dict, name='notes')
    notes.index.name = 'field_name'
    # Create an excel sheet of the notes_dict
    notes.to_excel(xlsx_writer, sheet_name=str(sheet_name) + '_notes',
                   na_rep='NA')

    # ZS: Don't think we *need* to return the xlsx_writer object here, because
    # any alternations we've made are stored within the object -- and its scope
    # exists beyond this function (since it was passed in).
    # If we *are* going to return the xlsx_writer, then we should probably be
    # making copy of it up front and not alter the one that's passed in. Either
    # idiom is common, but the mix of both might be confusing.

    # Return the xlsx_writer object, which can be written out, outside of
    # function, with 'xlsx_writer.save()'
    return xlsx_writer

###############################################################################
# CREATING PACKAGES AND METADATA
###############################################################################


def test_file_consistency(pkg_name, tables, out_dir):
    """
    Testing the consistency of tables for packaging

    The purpose of this function is to test that we have the correct list of
    tables.There are three different ways we could determine which tables are
    being dumped into packages: a list of the tabels being generated through
    the ETL functions, the list of dependent tables and the list of CSVs in
    package directory.
    """
    file_tbls = [x.replace(".csv", "") for x in os.listdir(
        os.path.join(out_dir, pkg_name, 'data'))]
    dependent_tbls = list(
        pudl.helpers.get_dependent_tables_from_list_pkg(tables)
    )
    etl_tbls = tables

    dependent_tbls.sort()
    file_tbls.sort()
    etl_tbls.sort()
    # TODO: determine what to do about the dependent_tbls... right now the
    # dependent tables include some glue tables for FERC in particular, but
    # we are imagining the glue tables will be
    if ((file_tbls == etl_tbls)):  # & (dependent_tbls == etl_tbls)):
        logger.info(f"Tables are consistent for {pkg_name} package")
    else:
        inconsistent_tbls = []
        for tbl in file_tbls:
            if tbl not in etl_tbls:
                inconsistent_tbls.extend(tbl)
                raise AssertionError(f"{tbl} from CSVs not in ETL tables")

        # for tbl in dependent_tbls:
        #    if tbl not in etl_tbls:
        #        inconsistent_tbls.extend(tbl)
        #        raise AssertionError(
        #            f"{tbl} from forgien key relationships not in ETL tables")
        # this is here for now just in case the previous two asserts don't work..
        # we should probably just stick to one.
        raise AssertionError(
            f"Tables are inconsistent. "
            f"Missing tables include: {inconsistent_tbls}")


def get_tabular_data_resource_2(table_name, pkg_dir, testing=False):
    """
    Create a Tabular Data Resource descriptor for a PUDL table.

    Based on the information in the database, and some additional metadata,
    stored elsewhere (Where?!?!) this function will generate a valid Tabular
    Data Resource descriptor, according to the Frictionless Data specification,
    which can be found here:

    https://frictionlessdata.io/specs/tabular-data-resource/
    """
    # Where the CSV file holding the data is, relative to datapackage.json
    # This is the value that has to be embedded in the data package.
    csv_relpath = os.path.join('data', f'{table_name}.csv')
    # We need to access the file to calculate hash and size too:
    csv_abspath = os.path.join(os.path.abspath(pkg_dir), csv_relpath)

    # pull the skeleton of the descriptor from the megadata file
    descriptor = pudl.helpers.pull_resource_from_megadata(table_name)
    descriptor['path'] = csv_relpath
    descriptor['bytes'] = os.path.getsize(csv_abspath)
    descriptor['hash'] = pudl.output.export.hash_csv(csv_abspath)
    descriptor['created'] = (datetime.datetime.utcnow().
                             replace(microsecond=0).isoformat() + 'Z'),

    resource = datapackage.Resource(descriptor)
    if resource.valid:
        logger.debug(f"{table_name} is a valid resource")
    if not resource.valid:
        raise AssertionError(
            f"""
            Invalid tabular data resource: {resource.name}

            Errors:
            {resource.errors}
            """
        )

    return descriptor


def generate_metadata(pkg_settings, tables, pkg_dir,
                      uuid_pkgs=uuid.uuid4()):
    # pkg_json is the datapackage.json that we ultimately output:
    pkg_json = os.path.join(pkg_dir, "datapackage.json")
    # Create a tabular data resource for each of the tables.
    resources = []
    for t in tables:
        resources.append(
            get_tabular_data_resource_2(t, pkg_dir=pkg_dir))

    data_sources = pudl.helpers.data_sources_from_tables_pkg(
        tables)
    sources = []
    for src in data_sources:
        if src in pudl.constants.data_sources:
            sources.append({"title": src,
                            "path": pc.base_data_urls[src]})

    contributors = set()
    for src in data_sources:
        for c in pudl.constants.contributors_by_source[src]:
            contributors.add(c)

    pkg_descriptor = {
        "name": pkg_settings["name"],
        "profile": "tabular-data-package",
        "title": pkg_settings["title"],
        "id": uuid_pkgs,
        "description": pkg_settings["description"],
        # "keywords": pkg_settings["keywords"],
        "homepage": "https://catalyst.coop/pudl/",
        "created": (datetime.datetime.utcnow().
                    replace(microsecond=0).isoformat() + 'Z'),
        "contributors": [pudl.constants.contributors[c] for c in contributors],
        "sources": sources,
        "licenses": [pudl.constants.licenses["cc-by-4.0"]],
        "resources": resources,
    }

    # Use that descriptor to instantiate a Package object
    data_pkg = datapackage.Package(pkg_descriptor)

    # Validate the data package descriptor before we go to
    if not data_pkg.valid:
        logger.warning(f"""
            Invalid tabular data package: {data_pkg.descriptor["name"]}
            Errors: {data_pkg.errors}""")

    data_pkg.save(pkg_json)
    # Validate the data within the package using goodtables:
    report = goodtables.validate(pkg_json, row_limit=1000)
    if not report['valid']:
        logger.warning("Data package data validation failed.")

    return data_pkg, report


def generate_data_packages(settings, pudl_settings, debug=False):
    """
    Cordinate the generation of data packages.

    Args:
        settings (iterable) : a list of dictionaries. Each item in the list
            corresponds to a data package. Each data package's dictionary
            contains the arguements for its ETL function.
        out_dir (path-like): The location of the packaging directory.
            The data package will be created in a subdirectory in
            this directory, according to the name of the package.
        debug (boolean)
    """
    # validate the settings from the settings file.
    validated_settings = pudl.etl_pkg.validate_input(settings)
    uuid_pkgs = str(uuid.uuid4())
    metas = {}
    for pkg in validated_settings:
        # run the ETL functions for this pkg and return the list of tables
        # dumped to CSV
        pkg_tbls = pudl.etl_pkg.etl_pkg(pkg, pudl_settings)
        # assure that the list of tables from ETL match up with the CVSs and
        # dependent tables
        test_file_consistency(
            pkg['name'],
            pkg_tbls,
            out_dir=os.path.join(pudl_settings['pudl_out'], 'datapackage'))
        if pkg_tbls:
            # generate the metadata for the package and validate
            # TODO: we'll probably want to remove this double return... but having
            # the report and the metadata while debugging is very useful.
            meta, report = generate_metadata(
                pkg,
                pkg_tbls,
                os.path.join(pudl_settings['datapackage_dir'], pkg['name']),
                uuid_pkgs=uuid_pkgs)
            metas[pkg['name']] = [meta, report]
        else:
            logger.info(f"Not generating metadata for {pkg['name']}")
    if debug:
        return (metas)
