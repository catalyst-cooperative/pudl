"""Functions to help with the creation of frictionless datapackages."""

import re
import sqlalchemy as sa


def simplify_sa_type(sa_type, field_name=""):
    """Convert a SQL Alchemy type to a simplified datapackage type."""

    type_dict = {
        'integer': (sa.sql.sqltypes.Integer,),
        'number': (sa.sql.sqltypes.Float, sa.sql.sqltypes.Numeric),
        'boolean': (sa.sql.sqltypes.Boolean,),
        'string': (sa.sql.sqltypes.String, sa.sql.sqltypes.Enum),
        'date': (sa.sql.sqltypes.Date,),
        'time': (sa.sql.sqltypes.Time,),
        'datetime': (sa.sql.sqltypes.DateTime,),
        'duration': (sa.sql.sqltypes.Interval,),
        'object': (sa.sql.sqltypes.JSON,),
        'array': (sa.sql.sqltypes.ARRAY,),
    }

    for dtype in type_dict:
        if isinstance(sa_type, type_dict[dtype]):
            simple_type = dtype

    if (simple_type == 'integer' and re.match('.*_year$', field_name)):
        simple_type = 'year'

    return simple_type


def table_to_resource(tablename, engine):
    """Create a tabular data resource representing a PUDL database table."""

    md = sa.MetaData(bind=engine)
    md.reflect()
    table = md.tables[tablename]
    fields = []
    for col in table.columns.keys():
        newfield = {}
        newfield['name'] = col
        newfield['type'] = simplify_sa_type(table.c[col].type, field_name=col)
        if isinstance(table.c[col].type, sa.sql.sqltypes.Enum):
            newfield['constraints'] = {'enum': table.c[col].type.enums}
        newfield['description'] = table.c[col].comment

        fields.append(newfield)
    return fields
