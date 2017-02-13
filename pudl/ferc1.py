import os.path
from pudl import settings
from pudl import constants
from sqlalchemy import MetaData, create_engine

###########################################################################
# Functions related to ingest & processing of FERC Form 1 data.
###########################################################################

def db_connect_ferc1(testing=False):
    """
    Performs database connection using database settings from settings.py.
    Returns sqlalchemy engine instance
    """
    from sqlalchemy.engine.url import URL
    if(testing):
        return create_engine(URL(**settings.DB_FERC1_TEST))
    else:
        return create_engine(URL(**settings.DB_FERC1))

def datadir(year):
    """Given a year, return path to appropriate FERC Form 1 data directory."""
    return os.path.join(settings.DATA_DIR,'ferc','form1','f1_{}'.format(year))

def dbc_filename(year):
    """Given a year, return path to the master FERC Form 1 .DBC file."""
    return os.path.join(datadir(year),'F1_PUB.DBC')

def get_strings(filename, min=4):
    """Extract printable strings from a binary and return them as a generator.

    This is meant to emulate the Unix "strings" command, for the purposes of
    grabbing database table and column names from the F1_PUB.DBC file that is
    distributed with the FERC Form 1 data.
    """
    import string
    with open(filename, errors="ignore") as f:
        result = ""
        for c in f.read():
            if c in string.printable:
                result += c
                continue
            if len(result) >= min:
                yield result
            result = ""
        if len(result) >= min:  # catch result at EOF
            yield result

def cleanstrings(field, stringmap, unmapped=None):
    """Clean up a field of string data in one of the Form 1 data frames.

    This function maps many different strings meant to represent the same value
    or category to a single value. In addition, white space is stripped and
    values are translated to lower case.  Optionally replace all unmapped
    values in the original field with a value (like NaN) to indicate data which
    is uncategorized or confusing.

    field is a pandas dataframe column (e.g. f1_fuel["FUEL"])

    stringmap is a dictionary whose keys are the strings we're mapping to, and
    whose values are the strings that get mapped.

    unmapped is the value which strings not found in the stringmap dictionary
    should be replaced by.

    The function returns a new pandas series/column that can be used to set the
    values of the original data.
    """
    from numpy import setdiff1d

    # Simplify the strings we're working with, to reduce the number of strings
    # we need to enumerate in the maps

    # Transform the strings to lower case
    field = field.apply(lambda x: x.lower())
    # remove leading & trailing whitespace
    field = field.apply(lambda x: x.strip())
    # remove duplicate internal whitespace
    field = field.replace('[\s+]', ' ', regex=True)

    for k in stringmap.keys():
        field = field.replace(stringmap[k],k)

    if unmapped is not None:
        badstrings = setdiff1d(field.unique(),list(stringmap.keys()))
        field = field.replace(badstrings,unmapped)

    return field

def define_db(refyear, dbfs, ferc1_meta, db_engine):
    """
    Given a list of FERC Form 1 DBF files, create analogous database tables.

    Based on strings extracted from the master F1_PUB.DBC file corresponding to
    the year indicated by refyear, and the list of DBF files specified in dbfs
    recreate a subset of the FERC Form 1 database as a Postgres database using
    SQLAlchemy.

    refyear:    year of FERC Form 1 data to use as the database template.
    dbfs:       list of DBF file prefixes (e.g. F1_77) to ingest.
    ferc1_meta: SQLAlchemy MetaData object to store the schema in.
    db_engine:  SQLAlchemy database engine to use to create the tables.

    """
    from sqlalchemy import Table, Column, Integer, String, Float, DateTime
    from sqlalchemy import Boolean, Date, MetaData, Text, ForeignKeyConstraint
    from sqlalchemy import PrimaryKeyConstraint
    import dbfread
    import re
    from pudl.constants import ferc1_dbf2tbl, ferc1_data_tables, dbf_typemap

    ferc1_tblmap = extract_dbc_tables(refyear)

    for dbf in dbfs:
        dbf_filename = os.path.join(datadir(refyear),'{}.DBF'.format(dbf))
        ferc1_dbf = dbfread.DBF(dbf_filename)

        # And the corresponding SQLAlchemy Table object:
        table_name = ferc1_dbf2tbl[dbf]
        ferc1_sql = Table(table_name, ferc1_meta)

        # _NullFlags isn't a "real" data field... remove it.
        fields = [ f for f in ferc1_dbf.fields if not re.match('_NullFlags', f.name)]

        for field in fields:
            col_name = ferc1_tblmap[ferc1_dbf2tbl[dbf]][field.name]
            col_type = dbf_typemap[field.type]

            # String/VarChar is the only type that really NEEDS a length
            if(col_type == String):
                col_type = col_type(length=field.length)

            # This eliminates the "footnote" fields which all mirror database
            # fields, but end with _f. We have not yet integrated the footnotes
            # into the rest of the DB, and so why clutter it up?
            if(not re.match('(.*_f$)', col_name)):
                ferc1_sql.append_column(Column(col_name, col_type))

        # Append primary key constraints to the table:

        if (table_name in ferc1_data_tables):
            # All the "real" data tables use the same 5 fields as a composite
            # primary key: [ respondent_id, report_year, report_prd,
            # row_number, spplmnt_num ]
            ferc1_sql.append_constraint(PrimaryKeyConstraint(
                'respondent_id',
                'report_year',
                'report_prd',
                'row_number',
                'spplmnt_num')
            )

            # They also all have respondent_id as their foreign key:
            ferc1_sql.append_constraint(ForeignKeyConstraint(
                columns=['respondent_id',],
                refcolumns=['f1_respondent_id.respondent_id'])
            )

        if (table_name == 'f1_respondent_id'):
            ferc1_sql.append_constraint(PrimaryKeyConstraint('respondent_id'))

        # Sadly the primary key definitions here don't seem to be right...
        if (table_name == 'f1_s0_filing_log'):
            ferc1_sql.append_constraint(PrimaryKeyConstraint(
                'respondent_id',
                'report_yr',
                'report_prd',
                'filing_num')
            )
            ferc1_sql.append_constraint(ForeignKeyConstraint(
                columns=['respondent_id',],
                refcolumns=['f1_respondent_id.respondent_id'])
            )

        # Sadly the primary key definitions here don't seem to be right...
        if (table_name == 'f1_row_lit_tbl'):
            ferc1_sql.append_constraint(PrimaryKeyConstraint(
                'sched_table_name',
                'report_year',
                'row_number')
            )
        # Other tables we have not yet attempted to deal with...
        #'f1_email'
        #  primary_key = respondent_id
        #  foreign_key = f1_respondent_id.respondent_id
        #'f1_ident_attsttn',
        #  primary_key = respondent_id
        #  primary_key = report_year
        #  primary_key = report_period
        #  foreign_key = f1_responded_id.respondent_id
        #'f1_footnote_data', #NOT USING NOW/NOT COMPLETE
        #  primary_key = fn_id
        #  primary_key = respondent_id
        #  foreign_key = f1_respondent_id.respondent_id
        #  foreign_key = f1_s0_filing_log.report_prd
        #'f1_pins',
        #  primary_key = f1_respondent_id.respondent_id
        #  foreign_key = f1_respondent_id.respondent_id
        #'f1_freeze',
        #'f1_security'
        #'f1_load_file_names'
        #'f1_unique_num_val',
        #'f1_sched_lit_tbl',
        #'f1_sys_error_log',
        #'f1_col_lit_tbl',    # GET THIS ONE
        #'f1_codes_val',
        #'f1_s0_checks',

def extract_dbc_tables(year, minstring=4):
    """Extract the names of all the tables and fields from FERC Form 1 DB

    This function reads all the strings in the given DBC database file for the
    and picks out the ones that appear to be database table names, and their
    subsequent table field names, for use in re-naming the truncated columns
    extracted from the corresponding DBF files (which are limited to having only
    10 characters in their names.) Strings must have at least min printable
    characters.

    Returns a dictionary whose keys are the long table names extracted from the
    DBC file, and whose values are lists of pairs of values, the first of which
    is the full name of each field in the table with the same name as the key,
    and the second of which is the truncated (<=10 character) long name of that
    field as found in the DBF file.

    TODO: This routine shouldn't refer to any particular year of data, but right
    now it depends on the ferc1_dbf2tbl dictionary, which was generated from
    the 2015 Form 1 database.
    """
    import re
    import dbfread
    from pudl.constants import ferc1_dbf2tbl

    # Extract all the strings longer than "min" from the DBC file
    dbc_strs = list(get_strings(dbc_filename(year), min=minstring))

    # Get rid of leading & trailing whitespace in the strings:
    dbc_strs = [ s.strip() for s in dbc_strs ]

    # Get rid of all the empty strings:
    dbc_strs = [ s for s in dbc_strs if s is not '' ]

    # Collapse all whitespace to a single space:
    dbc_strs = [ re.sub('\s+',' ',s) for s in dbc_strs ]

    # Pull out only strings that begin with Table or Field
    dbc_strs = [ s for s in dbc_strs if re.match('(^Table|^Field)',s) ]

    # Split each string by whitespace, and retain only the first two elements.
    # This eliminates some weird dangling junk characters
    dbc_strs = [ ' '.join(s.split()[:2]) for s in dbc_strs ]

    # Remove all of the leading Field keywords
    dbc_strs = [ re.sub('Field ','',s) for s in dbc_strs ]

    # Join all the strings together (separated by spaces) and then split the
    # big string on Table, so each string is now a table name followed by the
    # associated field names, separated by spaces
    dbc_list = ' '.join(dbc_strs).split('Table ')

    # strip leading & trailing whitespace from the lists, and get rid of empty
    # strings:
    dbc_list = [ s.strip() for s in dbc_list if s is not '' ]

    # Create a dictionary using the first element of these strings (the table
    # name) as the key, and the list of field names as the values, and return
    # it:
    tf_dict = {}
    for tbl in dbc_list:
        x = tbl.split()
        tf_dict[x[0]]=x[1:]

    tf_doubledict = {}
    for dbf in ferc1_dbf2tbl.keys():
        filename = os.path.join(datadir(year),'{}.DBF'.format(dbf))
        if os.path.isfile(filename):
            dbf_fields = dbfread.DBF(filename).field_names
            dbf_fields = [ f for f in dbf_fields if f != '_NullFlags' ]
            tf_doubledict[ferc1_dbf2tbl[dbf]]={ k:v for k,v in zip(dbf_fields,tf_dict[ferc1_dbf2tbl[dbf]]) }
            assert(len(tf_dict[ferc1_dbf2tbl[dbf]])==len(dbf_fields))

    # Insofar as we are able, make sure that the fields match each other
    for k in tf_doubledict.keys():
        for sn,ln in zip(tf_doubledict[k].keys(),tf_doubledict[k].values()):
            assert(ln[:8]==sn.lower()[:8])

    return(tf_doubledict)

def init_db(ferc1_tables=constants.ferc1_default_tables,
            refyear=2015,
            years=[2015,],
            testing=False):
    """Assuming an empty FERC Form 1 DB, create tables and insert data.

    This function uses dbfread and SQLAlchemy to migrate a set of FERC Form 1
    database tables from the provided DBF format into a postgres database.
    """
    from sqlalchemy.engine.url import URL
    import datetime
    import dbfread
    from pudl.constants import ferc1_tbl2dbf, ferc1_dbf2tbl

    ferc1_engine = db_connect_ferc1(testing=testing)

    # MetaData object will contain the database schema.
    ferc1_meta = MetaData()

    # Translate the list of FERC Form 1 database tables that has
    # been passed in into a list of DBF files prefixes:
    dbfs = [ constants.ferc1_tbl2dbf[table] for table in ferc1_tables ]

    # This function (see below) uses metadata from the DBF files to define a
    # postgres database structure suitable for accepting the FERC Form 1 data
    define_db(refyear, dbfs, ferc1_meta, ferc1_engine)

    # Wipe the DB and start over...
    ferc1_meta.drop_all(ferc1_engine)
    ferc1_meta.create_all(ferc1_engine)

    # Create a DB connection to use for the record insertions below:
    conn=ferc1_engine.connect()

    # This awkward dictionary of dictionaries lets us map from a DBF file
    # to a couple of lists -- one of the short field names from the DBF file,
    # and the other the full names that we want to have the SQL database...
    ferc1_tblmap = extract_dbc_tables(refyear)

    for year in years:
        for dbf in dbfs:
            dbf_filename = os.path.join(datadir(year), '{}.DBF'.format(dbf))
            dbf_table = dbfread.DBF(dbf_filename, load=True)

            # ferc1_dbf2tbl is a dictionary mapping DBF files to SQL table names
            sql_table_name = ferc1_dbf2tbl[dbf]
            sql_table = ferc1_meta.tables[sql_table_name]

            # Build up a list of dictionaries to INSERT into the postgres database.
            # Each dictionary is one record. Within each dictionary the keys are
            # the field names, and the values are the values for that field.
            sql_records = []
            for dbf_rec in dbf_table.records:
                sql_rec = {}
                for dbf_field, sql_field in ferc1_tblmap[sql_table_name].items():
                    sql_rec[sql_field] = dbf_rec[dbf_field]
                sql_records.append(sql_rec)

            # insert the new records!
            conn.execute(sql_table.insert(), sql_records)

    conn.close()
