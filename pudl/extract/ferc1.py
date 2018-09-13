"""A module to clone the FERC Form 1 Database from FoxPro to Postgres."""
import os.path
import string
import re
import datetime
import pandas as pd
import sqlalchemy as sa
import dbfread
from config import SETTINGS
import pudl.constants as pc

# MetaData object will contain the ferc1 database schema.
ferc1_meta = sa.MetaData()

###########################################################################
# Functions related to ingest & processing of FERC Form 1 data.
###########################################################################


def connect_db(testing=False):
    """
    Connect to the FERC Form 1 DB using global settings from config.py.

    Returns sqlalchemy engine instance.
    """
    if testing:
        return sa.create_engine(sa.engine.url.URL(**SETTINGS['db_ferc1_test']))

    return sa.create_engine(sa.engine.url.URL(**SETTINGS['db_ferc1']))


def _create_tables(engine):
    """Create the FERC Form 1 DB tables."""
    ferc1_meta.create_all(engine)


def drop_tables(engine):
    """Drop the FERC Form 1 DB tables."""
    ferc1_meta.drop_all(engine)


def datadir(year, basedir=SETTINGS['ferc1_data_dir']):
    """Given a year, return path to appropriate FERC Form 1 data directory."""
    assert year in pc.data_years['ferc1']
    return os.path.join(basedir, 'f1_{}'.format(year))


def dbc_filename(year, basedir=SETTINGS['ferc1_data_dir']):
    """Given a year, return path to the master FERC Form 1 .DBC file."""
    return os.path.join(datadir(year, basedir), 'F1_PUB.DBC')


def get_strings(filename, min=4):
    """
    Extract printable strings from a binary and return them as a generator.

    This is meant to emulate the Unix "strings" command, for the purposes of
    grabbing database table and column names from the F1_PUB.DBC file that is
    distributed with the FERC Form 1 data.
    """
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


def extract_dbc_tables(year, minstring=4, basedir=SETTINGS['ferc1_data_dir']):
    """Extract the names of all the tables and fields from FERC Form 1 DB.

    This function reads all the strings in the given DBC database file for the
    and picks out the ones that appear to be database table names, and their
    subsequent table field names, for use in re-naming the truncated columns
    extracted from the corresponding DBF files (which are limited to having
    only 10 characters in their names.) Strings must have at least min
    printable characters.

    Returns:

        dict: A dictionary whose keys are the long table names extracted from
            the DBC file, and whose values are lists of pairs of values, the
            first of which is the full name of each field in the table with the
            same name as the key, and the second of which is the truncated
            (<=10 character) long name of that field as found in the DBF file.

    TODO: This routine shouldn't refer to any particular year of data, but
    right now it depends on the ferc1_dbf2tbl dictionary, which was generated
    from the 2015 Form 1 database.
    """
    # Extract all the strings longer than "min" from the DBC file
    dbc_strs = list(get_strings(dbc_filename(year), min=minstring))

    # Get rid of leading & trailing whitespace in the strings:
    dbc_strs = [s.strip() for s in dbc_strs]

    # Get rid of all the empty strings:
    dbc_strs = [s for s in dbc_strs if s is not '']

    # Collapse all whitespace to a single space:
    dbc_strs = [re.sub('\s+', ' ', s) for s in dbc_strs]

    # Pull out only strings that begin with Table or Field
    dbc_strs = [s for s in dbc_strs if re.match('(^Table|^Field)', s)]

    # Split each string by whitespace, and retain only the first two elements.
    # This eliminates some weird dangling junk characters
    dbc_strs = [' '.join(s.split()[:2]) for s in dbc_strs]

    # Remove all of the leading Field keywords
    dbc_strs = [re.sub('Field ', '', s) for s in dbc_strs]

    # Join all the strings together (separated by spaces) and then split the
    # big string on Table, so each string is now a table name followed by the
    # associated field names, separated by spaces
    dbc_list = ' '.join(dbc_strs).split('Table ')

    # strip leading & trailing whitespace from the lists, and get rid of empty
    # strings:
    dbc_list = [s.strip() for s in dbc_list if s is not '']

    # Create a dictionary using the first element of these strings (the table
    # name) as the key, and the list of field names as the values, and return
    # it:
    tf_dict = {}
    for tbl in dbc_list:
        x = tbl.split()
        tf_dict[x[0]] = x[1:]

    tf_doubledict = {}
    for dbf in pc.ferc1_dbf2tbl.keys():
        filename = os.path.join(datadir(year, basedir), '{}.DBF'.format(dbf))
        if os.path.isfile(filename):
            dbf_fields = dbfread.DBF(filename).field_names
            dbf_fields = [f for f in dbf_fields if f != '_NullFlags']
            tf_doubledict[pc.ferc1_dbf2tbl[dbf]] = \
                {k: v for k, v in
                    zip(dbf_fields, tf_dict[pc.ferc1_dbf2tbl[dbf]])}
            assert(len(tf_dict[pc.ferc1_dbf2tbl[dbf]]) == len(dbf_fields))

    # Insofar as we are able, make sure that the fields match each other
    for k in tf_doubledict.keys():
        for sn, ln in zip(tf_doubledict[k].keys(), tf_doubledict[k].values()):
            assert(ln[:8] == sn.lower()[:8])

    return tf_doubledict


def define_db(refyear, ferc1_tables, ferc1_meta,
              basedir=SETTINGS['ferc1_data_dir'],
              verbose=True):
    """
    Given a list of FERC Form 1 DBF files, create analogous database tables.

    Based on strings extracted from the master F1_PUB.DBC file corresponding to
    the year indicated by refyear, and the list of DBF files specified in dbfs
    recreate a subset of the FERC Form 1 database as a Postgres database using
    SQLAlchemy.

    Args:

        refyear (int): Year of FERC Form 1 data to use as the database
            template.
        ferc1_tables (list): List of FERC Form 1 tables to ingest.
        ferc1_meta (SQLAlchemy MetaData): SQLAlchemy MetaData object
            to store the schema in.
    """
    ferc1_tblmap = extract_dbc_tables(refyear)
    # Translate the list of FERC Form 1 database tables that has
    # been passed in into a list of DBF files prefixes:
    dbfs = [pc.ferc1_tbl2dbf[table] for table in ferc1_tables]

    if verbose:
        print("Defining new FERC Form 1 DB based on {}...".format(refyear))

    if verbose:
        print("Clearing any existing FERC Form 1 database MetaData...")
    # This keeps us from having collisions when re-initializing the DB.
    ferc1_meta.clear()

    for dbf in dbfs:
        dbf_filename = os.path.join(datadir(refyear, basedir),
                                    '{}.DBF'.format(dbf))
        ferc1_dbf = dbfread.DBF(dbf_filename)

        # And the corresponding SQLAlchemy Table object:
        table_name = pc.ferc1_dbf2tbl[dbf]
        ferc1_sql = sa.Table(table_name, ferc1_meta)

        # _NullFlags isn't a "real" data field... remove it.
        fields = [f for f in ferc1_dbf.fields if not re.match(
            '_NullFlags', f.name)]

        for field in fields:
            col_name = ferc1_tblmap[pc.ferc1_dbf2tbl[dbf]][field.name]
            col_type = pc.dbf_typemap[field.type]

            # String/VarChar is the only type that really NEEDS a length
            if col_type == sa.String:
                col_type = col_type(length=field.length)

            # This eliminates the "footnote" fields which all mirror database
            # fields, but end with _f. We have not yet integrated the footnotes
            # into the rest of the DB, and so why clutter it up?
            if not re.match('(.*_f$)', col_name):
                ferc1_sql.append_column(sa.Column(col_name, col_type))

        # Append primary key constraints to the table:
        if table_name == 'f1_respondent_id':
            ferc1_sql.append_constraint(
                sa.PrimaryKeyConstraint('respondent_id'))

        if table_name in pc.ferc1_data_tables:
            # All the "real" data tables use the same 5 fields as a composite
            # primary key: [ respondent_id, report_year, report_prd,
            # row_number, spplmnt_num ]
            ferc1_sql.append_constraint(sa.PrimaryKeyConstraint(
                'respondent_id',
                'report_year',
                'report_prd',
                'row_number',
                'spplmnt_num')
            )

            # They also all have respondent_id as their foreign key:
            ferc1_sql.append_constraint(sa.ForeignKeyConstraint(
                columns=['respondent_id', ],
                refcolumns=['f1_respondent_id.respondent_id'])
            )


def init_db(ferc1_tables=pc.ferc1_default_tables,
            refyear=max(pc.working_years['ferc1']),
            years=pc.working_years['ferc1'],
            basedir=SETTINGS['ferc1_data_dir'],
            def_db=True,
            verbose=True,
            testing=False):
    """Assuming an empty FERC Form 1 DB, create tables and insert data.

    This function uses dbfread and SQLAlchemy to migrate a set of FERC Form 1
    database tables from the provided DBF format into a postgres database.

    Args:

        ferc1_tables (list): The set of tables to read from the FERC Form 1 dbf
            database into the FERC Form 1 DB.
        refyear (int): Year of FERC Form 1 data to use as the database
            template.
        years (list): The set of years to read from FERC Form 1 dbf database
            into the FERC Form 1 DB.
    """
    if verbose:
        print("Start ferc mirror db ingest at {}".format(datetime.datetime.now().
                                                         strftime("%A, %d. %B %Y %I:%M%p")))

    if not ferc1_tables or not years:
        if verbose:
            print("Not ingesting mirrored ferc db.")
        return None

    # if the refyear (particular if it came directly from settings.yml) is set
    # to none, but there
    if not refyear:
        refyear = max(SETTINGS['ferc1_years'])
        if verbose:
            print("Changed ferc1 refyear to {}".format(refyear))

    ferc1_engine = connect_db(testing=testing)

    # This function (see below) uses metadata from the DBF files to define a
    # postgres database structure suitable for accepting the FERC Form 1 data
    if def_db:
        define_db(refyear, ferc1_tables, ferc1_meta)

    # Wipe the DB and start over...
    drop_tables(ferc1_engine)
    _create_tables(ferc1_engine)

    # Create a DB connection to use for the record insertions below:
    conn = ferc1_engine.connect()

    # This awkward dictionary of dictionaries lets us map from a DBF file
    # to a couple of lists -- one of the short field names from the DBF file,
    # and the other the full names that we want to have the SQL database...
    ferc1_tblmap = extract_dbc_tables(refyear)

    # Translate the list of FERC Form 1 database tables that has
    # been passed in into a list of DBF files prefixes:
    dbfs = [pc.ferc1_tbl2dbf[table] for table in ferc1_tables]

    for year in years:
        if verbose:
            print("Ingesting FERC Form 1 Data from {}...".format(year))
        for dbf in dbfs:
            dbf_filename = os.path.join(datadir(year, basedir),
                                        '{}.DBF'.format(dbf))
            dbf_table = dbfread.DBF(dbf_filename, load=True)

            # pc.ferc1_dbf2tbl is a dictionary mapping DBF files to SQL table
            # names
            sql_table_name = pc.ferc1_dbf2tbl[dbf]
            sql_stmt = sa.dialects.postgresql.insert(
                ferc1_meta.tables[sql_table_name])

            # Build up a list of dictionaries to INSERT into the postgres
            # database. Each dictionary is one record. Within each dictionary
            # the keys are the field names, and the values are the values for
            # that field.
            sql_records = []
            bad_respondents = [515, ]
            for dbf_rec in dbf_table.records:
                sql_rec = {}
                for d, s in ferc1_tblmap[sql_table_name].items():
                    sql_rec[s] = dbf_rec[d]
                if sql_rec['respondent_id'] not in bad_respondents:
                    sql_records.append(sql_rec)

            # If we're reading in multiple years of FERC Form 1 data, we
            # need to avoid collisions in the f1_respondent_id table, which
            # does not have a year field... F1_1 is the DBF file that stores
            # this table:
            if dbf == 'F1_1':
                sql_stmt = sql_stmt.on_conflict_do_nothing()

            # insert the new records!
            conn.execute(sql_stmt, sql_records)

    conn.close()


###########################################################################
# Functions related to extracting ferc1 tables for pudl.
###########################################################################

def fuel(ferc1_raw_dfs,
         ferc1_engine,
         ferc1_table='f1_fuel',
         pudl_table='fuel_ferc1',
         ferc1_years=pc.working_years['ferc1']):
    """Pull the f1_fuel table from the ferc1 db."""
    # Grab the f1_fuel SQLAlchemy Table object from the metadata object.
    f1_fuel = ferc1_meta.tables[ferc1_table]
    # Generate a SELECT statement that pulls all fields of the f1_fuel table,
    # but only gets records with plant names, and non-zero fuel amounts:
    f1_fuel_select = sa.sql.select([f1_fuel]).\
        where(f1_fuel.c.fuel != '').\
        where(f1_fuel.c.fuel_quantity > 0).\
        where(f1_fuel.c.plant_name != '').\
        where(f1_fuel.c.report_year.in_(ferc1_years))
    # Use the above SELECT to pull those records into a DataFrame:
    fuel_ferc1_df = pd.read_sql(f1_fuel_select, ferc1_engine)

    ferc1_raw_dfs[pudl_table] = fuel_ferc1_df

    return ferc1_raw_dfs


def plants_steam(ferc1_raw_dfs,
                 ferc1_engine,
                 ferc1_table='f1_steam',
                 pudl_table='plants_steam_ferc1',
                 ferc1_years=pc.working_years['ferc1']):
    """ """
    f1_steam = ferc1_meta.tables[ferc1_table]
    f1_steam_select = sa.sql.select([f1_steam]).\
        where(f1_steam.c.net_generation > 0).\
        where(f1_steam.c.plant_name != '').\
        where(f1_steam.c.report_year.in_(ferc1_years))

    ferc1_steam_df = pd.read_sql(f1_steam_select, ferc1_engine)

    # populate the unlatered dictionary of dataframes
    ferc1_raw_dfs[pudl_table] = ferc1_steam_df

    return ferc1_raw_dfs


def plants_small(ferc1_raw_dfs,
                 ferc1_engine,
                 ferc1_table='f1_gnrt_plant',
                 pudl_table='plants_small_ferc1',
                 ferc1_years=pc.working_years['ferc1']):
    """
    """
    from sqlalchemy import or_

    assert min(ferc1_years) >= min(pc.working_years['ferc1']),\
        """Year {} is too early. Small plant data has not been categorized for
         before 2004.""".format(min(ferc1_years))
    assert max(ferc1_years) <= max(pc.working_years['ferc1']),\
        """Year {} is too recent. Small plant data has not been categorized for
         any year after 2015.""".format(max(ferc1_years))
    f1_small = ferc1_meta.tables[ferc1_table]
    f1_small_select = sa.sql.select([f1_small, ]).\
        where(f1_small.c.report_year.in_(ferc1_years)).\
        where(f1_small.c.plant_name != '').\
        where(or_((f1_small.c.capacity_rating != 0),
                  (f1_small.c.net_demand != 0),
                  (f1_small.c.net_generation != 0),
                  (f1_small.c.plant_cost != 0),
                  (f1_small.c.plant_cost_mw != 0),
                  (f1_small.c.operation != 0),
                  (f1_small.c.expns_fuel != 0),
                  (f1_small.c.expns_maint != 0),
                  (f1_small.c.fuel_cost != 0)))

    ferc1_small_df = pd.read_sql(f1_small_select, ferc1_engine)

    # populate the unlatered dictionary of dataframes
    ferc1_raw_dfs[pudl_table] = ferc1_small_df

    return ferc1_raw_dfs


def plants_hydro(ferc1_raw_dfs,
                 ferc1_engine,
                 ferc1_table='f1_hydro',
                 pudl_table='plants_hydro_ferc1',
                 ferc1_years=pc.working_years['ferc1']):
    f1_hydro = ferc1_meta.tables[ferc1_table]

    f1_hydro_select = sa.sql.select([f1_hydro]).\
        where(f1_hydro.c.plant_name != '').\
        where(f1_hydro.c.report_year.in_(ferc1_years))

    ferc1_hydro_df = pd.read_sql(f1_hydro_select, ferc1_engine)

    ferc1_raw_dfs[pudl_table] = ferc1_hydro_df

    return ferc1_raw_dfs


def plants_pumped_storage(ferc1_raw_dfs,
                          ferc1_engine,
                          ferc1_table='f1_pumped_storage',
                          pudl_table='plants_pumped_storage_ferc1',
                          ferc1_years=pc.working_years['ferc1']):
    f1_pumped_storage = \
        ferc1_meta.tables[ferc1_table]

    # Removing the empty records.
    # This reduces the entries for 2015 from 272 records to 27.
    f1_pumped_storage_select = sa.sql.select([f1_pumped_storage]).\
        where(f1_pumped_storage.c.plant_name != '').\
        where(f1_pumped_storage.c.report_year.in_(ferc1_years))

    ferc1_pumped_storage_df = pd.read_sql(
        f1_pumped_storage_select, ferc1_engine)

    ferc1_raw_dfs[pudl_table] = ferc1_pumped_storage_df

    return ferc1_raw_dfs


def plant_in_service(ferc1_raw_dfs,
                     ferc1_engine,
                     ferc1_table='f1_plant_in_srvce',
                     pudl_table='plant_in_service_ferc1',
                     ferc1_years=pc.working_years['ferc1']):
    f1_plant_in_srvce = \
        ferc1_meta.tables[ferc1_table]
    f1_plant_in_srvce_select = sa.sql.select([f1_plant_in_srvce]).\
        where(
            sa.sql.and_(
                f1_plant_in_srvce.c.report_year.in_(ferc1_years),
                # line_no mapping is invalid before 2007
                f1_plant_in_srvce.c.report_year >= 2007))

    ferc1_pis_df = pd.read_sql(f1_plant_in_srvce_select, ferc1_engine)

    ferc1_raw_dfs[pudl_table] = ferc1_pis_df

    return ferc1_raw_dfs


def purchased_power(ferc1_raw_dfs,
                    ferc1_engine,
                    ferc1_table='f1_purchased_pwr',
                    pudl_table='purchased_power_ferc1',
                    ferc1_years=pc.working_years['ferc1']):
    f1_purchased_pwr = ferc1_meta.tables[ferc1_table]
    f1_purchased_pwr_select = sa.sql.select([f1_purchased_pwr]).\
        where(f1_purchased_pwr.c.report_year.in_(ferc1_years))

    ferc1_purchased_pwr_df = pd.read_sql(f1_purchased_pwr_select, ferc1_engine)

    ferc1_raw_dfs[pudl_table] = ferc1_purchased_pwr_df

    return ferc1_raw_dfs


def accumulated_depreciation(ferc1_raw_dfs,
                             ferc1_engine,
                             ferc1_table='f1_accumdepr_prvsn',
                             pudl_table='accumulated_depreciation_ferc1',
                             ferc1_years=pc.working_years['ferc1']):
    f1_accumdepr_prvsn = ferc1_meta.tables[ferc1_table]
    f1_accumdepr_prvsn_select = sa.sql.select([f1_accumdepr_prvsn]).\
        where(f1_accumdepr_prvsn.c.report_year.in_(ferc1_years))

    ferc1_apd_df = pd.read_sql(f1_accumdepr_prvsn_select, ferc1_engine)

    ferc1_raw_dfs[pudl_table] = ferc1_apd_df

    return ferc1_raw_dfs


def extract(ferc1_tables=pc.ferc1_pudl_tables,
            ferc1_years=pc.working_years['ferc1'],
            testing=False,
            verbose=True):
    """Extract FERC 1."""
    # BEGIN INGESTING FERC FORM 1 DATA:
    ferc1_engine = connect_db(testing=testing)

    ferc1_raw_dfs = {}
    ferc1_extract_functions = {
        'fuel_ferc1': fuel,
        'plants_steam_ferc1': plants_steam,
        'plants_small_ferc1': plants_small,
        'plants_hydro_ferc1': plants_hydro,
        'plants_pumped_storage_ferc1': plants_pumped_storage,
        'plant_in_service_ferc1': plant_in_service,
        'purchased_power_ferc1': purchased_power,
        'accumulated_depreciation_ferc1': accumulated_depreciation}

    # define the ferc 1 metadata object
    # this is here because if ferc wasn't ingested in the same session, there
    # will not be a defined metadata object to use to find and grab the tables
    # from the ferc1 mirror db
    if len(ferc1_meta.tables) == 0:
        define_db(max(pc.working_years['ferc1']),
                  pc.ferc1_default_tables,
                  ferc1_meta,
                  basedir=SETTINGS['ferc1_data_dir'],
                  verbose=verbose)

    if verbose:
        print("Extracting tables from FERC 1:")
    for table in ferc1_extract_functions:
        if ferc1_tables:
            if table in ferc1_tables:
                if verbose:
                    print("    {}...".format(table))
                ferc1_extract_functions[table](
                    ferc1_raw_dfs,
                    ferc1_engine,
                    ferc1_table=pc.table_map_ferc1_pudl[table],
                    pudl_table=table,
                    ferc1_years=ferc1_years)

    return ferc1_raw_dfs
