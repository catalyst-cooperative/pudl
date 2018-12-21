"""Functions to help with the creation of frictionless datapackages."""

import os
import re
import hashlib
import sqlalchemy as sa
import tableschema
import datapackage
import pudl


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
    descriptor['foreignKeys'] = get_foreign_keys(table)
    descriptor['missingValues'] = get_missing_values(table)

    schema = tableschema.Schema(descriptor)
    if not schema.valid:
        raise AssertionError(
            f"""
            Invalid table schema.

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
    descriptor['description'] = table.comment
    descriptor['encoding'] = "utf-8"
    descriptor['mediatype'] = "text/csv"
    descriptor['format'] = "csv"
    descriptor['dialect'] = {
        "delimiter": ",",
        "header": True,
        "quoteChar": "\"",
        # "doubleQuote": true,
        # "lineTerminator": "\r\n",
        # "skipInitialSpace": true,
    }
    descriptor['schema'] = get_table_schema(table)
    descriptor['bytes'] = os.path.getsize(csv_abspath)
    descriptor['hash'] = hash_csv(csv_abspath)

    # If omitted, icenses are inherited from the containing data package.
    descriptor["licenses"] = [{
        "name": "CC-BY-4.0",
        "title": "Creative Commons Attribution 4.0",
        "path": "https://creativecommons.org/licenses/by/4.0/"
    }]

    # This should also include the table specific data sources.
    descriptor["sources"] = [{
        "title": "Public Utility Data Liberation Project (PUDL)",
        "path": "https://catalyst.coop/public-utility-data-liberation/",
        "email": "pudl@catalyst.coop",
    }]

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
    Calculate a SHA-1 hash of the CSV file for integrity checking.

    Returns the hexdigest of the hash, with a sha1: prefix as a string.
    """
    blocksize = 65536
    hasher = hashlib.sha1()
    with open(csv_path, 'rb') as afile:
        buf = afile.read(blocksize)
        while len(buf) > 0:
            hasher.update(buf)
            buf = afile.read(blocksize)

    return f"sha1:{hasher.hexdigest()}"
