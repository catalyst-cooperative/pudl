"""
The Public Utility Data Liberation (PUDL) project core module.

The PUDL project integrates several different public data sets into one well
normalized database allowing easier access and interaction between all of them.
This module defines database tables and initializes them with data from:

 - US Energy Information Agency (EIA):
   - Form 860 (eia860)
   - Form 923 (eia923)
 - US Federal Energy Regulatory Commission (FERC):
   - Form 1 (ferc1)
 - US Environmental Protection Agency (EPA):
   - Continuous Emissions Monitory System (epacems)
"""

import logging
import os.path
import datetime
import time
import pandas as pd
import sqlalchemy as sa

import pudl
import pudl.models.entities
import pudl.models.glue
import pudl.models.eia923
import pudl.models.eia860
import pudl.models.ferc1
import pudl.models.epacems
import pudl.models.epaipm

import pudl.constants as pc
from pudl.settings import SETTINGS

logger = logging.getLogger(__name__)

###############################################################################
###############################################################################
# DATABASE CONNECTION & HELPER FUNCTIONS
###############################################################################
###############################################################################


def connect_db(testing=False):
    """Connect to the PUDL database using global settings from settings.py."""
    if testing:
        return sa.create_engine(sa.engine.url.URL(**SETTINGS['db_pudl_test']))
    return sa.create_engine(sa.engine.url.URL(**SETTINGS['db_pudl']))


def _create_tables(engine):
    """Create the tables and views associated with the PUDL Database."""
    pudl.models.entities.PUDLBase.metadata.create_all(engine)
    _create_views(engine)


def drop_tables(engine):
    """Drop all the tables and views associated with the PUDL Database."""
    # Drop the views first because they depend on the underlying tables.
    # can't easily cascade because SQLAlchemy doesn't know about the views
    try:
        _drop_views(engine)
        pudl.models.entities.PUDLBase.metadata.drop_all(engine)
    except sa.exc.DBAPIError as e:
        logger.error(
            """Failed to drop and re-create the existing tables. This sometimes
happens when the database organization has changed. The easiest fix
is to reset the databases. Instructions here:
https://github.com/catalyst-cooperative/pudl/blob/master/docs/reset_instructions.md""")
        raise e


def _create_views(engine):
    """Create views on the PUDL tables

    stackoverflow doesn't know how to create views with declarative_base, so I
    don't either.
    https://stackoverflow.com/questions/40083753/sqlalchemy-creating-view-with-orm
    """
    views_sql_list = pudl.models.epacems.CREATE_VIEWS
    for s in views_sql_list:
        engine.execute(s)


def _drop_views(engine):
    """Drop the views associated with the PUDL database"""
    views_sql_commands = pudl.models.epacems.DROP_VIEWS
    for s in views_sql_commands:
        engine.execute(s)


def verify_input_files(ferc1_years,
                       eia923_years,
                       eia860_years,
                       epacems_years,
                       epacems_states):
    """Verify that all the files exist before starting the ingest

    :param ferc1_years: Years of FERC1 data we're going to import (iterable)
    :param eia923_years: Years of EIA923 data we're going to import (iterable)
    :param eia860_years: Years of EIA860 data we're going to import (iterable)
    :param epacems_years: Years of CEMS data we're going to import (iterable)
    :param epacems_states: States of CEMS data we're going to import (iterable)
    """

    # NOTE that these filename functions take other arguments, like BASEDIR.
    # Here, we're assuming that the default arguments (as defined in SETTINGS)
    # are what we want.
    missing_ferc1_years = {str(y) for y in ferc1_years
                           if not os.path.isfile(pudl.extract.ferc1.dbc_filename(y))}

    missing_eia860_years = set()
    for y in eia860_years:
        for pattern in pc.files_eia860:
            f = pc.files_dict_eia860[pattern]
            try:
                # This function already looks for the file, and raises an
                # IndexError if missi
                pudl.extract.eia860.get_eia860_file(y, f)
            except IndexError:
                missing_eia860_years.add(str(y))

    missing_eia923_years = set()
    for y in eia923_years:
        try:
            f = pudl.extract.eia923.get_eia923_file(y)
        except AssertionError:
            missing_eia923_years.add(str(y))
        if not os.path.isfile(f):
            missing_eia923_years.add(str(y))

    if epacems_states and epacems_states[0].lower() == 'all':
        epacems_states = list(pc.cems_states.keys())
    missing_epacems_year_states = set()
    for y in epacems_years:
        for s in epacems_states:
            for m in range(1, 13):
                try:
                    f = pudl.extract.epacems.get_epacems_file(y, m, s)
                except AssertionError:
                    missing_epacems_year_states.add((str(y), s))
                    continue
                if not os.path.isfile(f):
                    missing_epacems_year_states.add((str(y), s))

    any_missing = (missing_eia860_years or missing_eia923_years
                   or missing_ferc1_years or missing_epacems_year_states)
    if any_missing:
        err_msg = ["Missing data files for the following sources and years:"]
        if missing_ferc1_years:
            err_msg += ["  FERC 1:  " + ", ".join(missing_ferc1_years)]
        if missing_eia860_years:
            err_msg += ["  EIA 860: " + ", ".join(missing_eia860_years)]
        if missing_eia923_years:
            err_msg += ["  EIA 923: " + ", ".join(missing_eia923_years)]
        if missing_epacems_year_states:
            missing_yr_str = ", ".join(
                {yr_st[0] for yr_st in missing_epacems_year_states})
            missing_st_str = ", ".join(
                {yr_st[1] for yr_st in missing_epacems_year_states})
            err_msg += ["  EPA CEMS:"]
            err_msg += ["    Years:  " + missing_yr_str]
            err_msg += ["    States: " + missing_st_str]
        raise FileNotFoundError("\n".join(err_msg))

###############################################################################
###############################################################################
#   BEGIN INGESTING STATIC & INFRASTRUCTURE TABLES
###############################################################################
###############################################################################


def _ingest_datasets_table(ferc1_years,
                           eia860_years,
                           eia923_years,
                           epacems_years,
                           engine):
    """
    Create and populate datasets table.

    This table will be used to determine which sources have been ingested into
    the database in later output or anaylsis.
    """
    datasets = pd.DataFrame.from_records([('ferc1', bool(ferc1_years)),
                                          ('eia860', bool(eia860_years)),
                                          ('eia923', bool(eia923_years)),
                                          ('epacems', bool(epacems_years)), ],
                                         columns=['datasource', 'active'])

    datasets.to_sql(name='datasets',
                    con=engine, index=False, if_exists='append',
                    dtype={'datasource': sa.String, 'active': sa.Boolean})


def ingest_static_tables(engine):
    """
    Populate static PUDL tables with constants for use as foreign keys.

    There are many values specified within the data that are essentially
    constant, but which we need to store for data validation purposes, for use
    as foreign keys.  E.g. the list of valid EIA fuel type codes, or the
    possible state and country codes indicating a coal delivery's location of
    origin. For now these values are primarily stored in a large collection of
    lists, dictionaries, and dataframes which are specified in the
    pudl.constants module.  This function uses those data structures to
    populate a bunch of small infrastructural tables within the PUDL DB.

    Args:
        engine (sqlalchemy.engine): A database engine with which to connect to
            to the PUDL DB.

    Returns: Nothing.

    """
    # Populate tables with static data from above.
    (pd.DataFrame({'abbr': list(pc.fuel_type_eia923.keys()),
                   'fuel_type': list(pc.fuel_type_eia923.values())})
     .to_sql('fuel_type_eia923',
             con=engine, index=False, if_exists='append'))

    (pd.DataFrame({'abbr': list(pc.prime_movers_eia923.keys()),
                   'prime_mover': list(pc.prime_movers_eia923.values())})
     .to_sql('prime_movers_eia923',
             con=engine, index=False, if_exists='append'))

    (pd.DataFrame({'abbr': list(pc.fuel_type_aer_eia923.keys()),
                   'fuel_type': list(pc.fuel_type_aer_eia923.values())})
     .to_sql('fuel_type_aer_eia923',
             con=engine, index=False, if_exists='append'))

    (pd.DataFrame({'abbr': list(pc.energy_source_eia923.keys()),
                   'source': list(pc.energy_source_eia923.values())})
     .to_sql('energy_source_eia923',
             con=engine, index=False, if_exists='append'))

    (pd.DataFrame({'abbr': list(pc.transport_modes_eia923.keys()),
                   'mode': list(pc.transport_modes_eia923.values())})
     .to_sql('transport_modes_eia923',
             con=engine, index=False, if_exists='append'))

    (pc.ferc_electric_plant_accounts
     .drop('row_number', axis=1)
     .replace({'ferc_account_description': r'\s+'}, ' ', regex=True)
     .rename(columns={'ferc_account_id': 'id',
                      'ferc_account_description': 'description'})
     .to_sql('ferc_accounts',
             con=engine, index=False, if_exists='append',
             dtype={'id': sa.String,
                    'description': sa.String}))

    (pc.ferc_accumulated_depreciation
     .drop('row_number', axis=1)
     .rename(columns={'line_id': 'id',
                      'ferc_account_description': 'description'})
     .to_sql('ferc_depreciation_lines',
             con=engine, index=False, if_exists='append',
             dtype={'id': sa.String,
                    'description': sa.String}))

    (
        pd.DataFrame(
            {'region_id_ipm': pc.epaipm_region_names}
        ).to_sql(
            'regions_entity_ipm',
            con=engine,
            index=False,
            if_exists='append'
        )
    )


def _ingest_glue(engine,
                 eia923_years,
                 eia860_years,
                 ferc1_years):
    """
    Populate glue tables depending on which datasources are being ingested.
    """
    # currently we only have on set of glue between datasets...
    _ingest_glue_eia_ferc1(engine,
                           eia923_years,
                           eia860_years,
                           ferc1_years)


def _ingest_glue_eia_ferc1(engine,
                           eia923_years,
                           eia860_years,
                           ferc1_years):
    """
    Populate the tables which relate the EIA, EPA, and FERC datasets.

    We have compiled a bunch of information which can be used to map individual
    utilities and plants listed in the EIA, EPA, and FERC data sets to each
    other, allowing disparate data reported in the two sources to be related
    to each other. That data is primarily stored in the plant_output and
    utility_output tabs of results/id_mapping/mapping_eia923_ferc1.xlsx in the
    repository. There are a total of seven relations described in this data:

        - utilities: Unique id and name for each utility for use across the
          PUDL DB.
        - plants: Unique id and name for each plant for use across the PUDL DB.
        - utilities_eia: EIA operator ids and names attached to a PUDL
          utility id.
        - plants_eia: EIA plant ids and names attached to a PUDL plant id.
        - utilities_ferc: FERC respondent ids & names attached to a PUDL
          utility id.
        - plants_ferc: A combination of FERC plant names and respondent ids,
          associated with a PUDL plant ID. This is necessary because FERC does
          not provide plant ids, so the unique plant identifier is a
          combination of the respondent id and plant name.
        - utility_plant_assn: An association table which describes which plants
          have relationships with what utilities. If a record exists in this
          table then combination of PUDL utility id & PUDL plant id does have
          an association of some kind. The nature of that association is
          somewhat fluid, and more scrutiny will likely be required for use in
          analysis.

    Presently, the 'glue' tables are a very basic piece of infrastructure for
    the PUDL DB, because they contain the primary key fields for utilities and
    plants. It may make sense to revise this going forward, as the
    relationships between data from different sources are looser than we had
    originally anticipated.
    """
    # ferc glue tables are structurally entity tables w/ forigen key
    # relationships to ferc datatables, so we need some of the eia/ferc 'glue'
    # when only ferc is ingested into the database.
    if not ferc1_years:
        return
    map_eia_ferc_file = os.path.join(SETTINGS['pudl_dir'],
                                     'results',
                                     'id_mapping',
                                     'mapping_eia923_ferc1.xlsx')

    plant_map = pd.read_excel(map_eia_ferc_file, 'plants_output',
                              na_values='', keep_default_na=False,
                              converters={'plant_id': int,
                                          'plant_name': str,
                                          'respondent_id_ferc': int,
                                          'respondent_name_ferc': str,
                                          'plant_name_ferc': str,
                                          'plant_id_eia': int,
                                          'plant_name_eia': str,
                                          'operator_name_eia': str,
                                          'operator_id_eia': int})

    utility_map = pd.read_excel(map_eia_ferc_file, 'utilities_output',
                                na_values='', keep_default_na=False,
                                converters={'utility_id': int,
                                            'utility_name': str,
                                            'respondent_id_ferc': int,
                                            'respondent_name_ferc': str,
                                            'operator_id_eia': int,
                                            'operator_name_eia': str})

    # We need to standardize plant names -- same capitalization and no leading
    # or trailing white space... since this field is being used as a key in
    # many cases. This also needs to be done any time plant_name is pulled in
    # from other tables.
    plant_map = pudl.helpers.strip_lower(plant_map, ['plant_name_ferc'])

    plants = plant_map[['plant_id', 'plant_name']]
    plants = plants.drop_duplicates('plant_id')

    plants_eia = plant_map[['plant_id_eia',
                            'plant_name_eia',
                            'plant_id']]
    plants_eia = plants_eia.drop_duplicates('plant_id_eia')
    plants_ferc = plant_map[['plant_name_ferc',
                             'respondent_id_ferc',
                             'plant_id']]
    plants_ferc = plants_ferc.drop_duplicates(['plant_name_ferc',
                                               'respondent_id_ferc'])
    utilities = utility_map[['utility_id', 'utility_name']]
    utilities = utilities.drop_duplicates('utility_id')
    utilities_eia = utility_map[['operator_id_eia',
                                 'operator_name_eia',
                                 'utility_id']]
    utilities_eia = utilities_eia.drop_duplicates('operator_id_eia')
    utilities_eia = utilities_eia.dropna(subset=['operator_id_eia'])

    utilities_ferc = utility_map[['respondent_id_ferc',
                                  'respondent_name_ferc',
                                  'utility_id']]
    utilities_ferc = utilities_ferc.drop_duplicates('respondent_id_ferc')
    utilities_ferc = utilities_ferc.dropna(subset=['respondent_id_ferc'])

    # Now we need to create a table that indicates which plants are associated
    # with every utility.

    # These dataframes map our plant_id to FERC respondents and EIA
    # operators -- the equivalents of our "utilities"
    plants_respondents = plant_map[['plant_id', 'respondent_id_ferc']]
    plants_respondents = plants_respondents.dropna(
        subset=['respondent_id_ferc'])
    plants_operators = plant_map[['plant_id', 'operator_id_eia']]
    plants_operators = plants_operators.dropna(subset=['operator_id_eia'])

    # Here we treat the dataframes like database tables, and join on the
    # FERC respondent_id and EIA operator_id, respectively.
    utility_plant_ferc1 = pd.merge(utilities_ferc,
                                   plants_respondents,
                                   on='respondent_id_ferc')
    utility_plant_eia923 = pd.merge(utilities_eia,
                                    plants_operators,
                                    on='operator_id_eia')
    # Now we can concatenate the two dataframes, and get rid of all the columns
    # except for plant_id and utility_id (which determine the  utility to plant
    # association), and get rid of any duplicates or lingering NaN values...
    utility_plant_assn = pd.concat([utility_plant_eia923, utility_plant_ferc1],
                                   sort=True)
    utility_plant_assn = utility_plant_assn[['plant_id', 'utility_id']].\
        dropna().drop_duplicates()

    # At this point there should be at most one row in each of these data
    # frames with NaN values after we drop_duplicates in each. This is because
    # there will be some plants and utilities that only exist in FERC, or only
    # exist in EIA, and while they will have PUDL IDs, they may not have
    # FERC/EIA info (and it'll get pulled in as NaN)

    for df, df_n in zip([plants_eia, plants_ferc,
                         utilities_eia, utilities_ferc],
                        ['plants_eia', 'plants_ferc',
                         'utilities_eia', 'utilities_ferc']):
        if df[pd.isnull(df).any(axis=1)].shape[0] > 1:
            raise AssertionError(f"FERC to EIA glue breaking in {df_n}")
        df.dropna(inplace=True)

    # Before we start inserting records into the database, let's do some basic
    # sanity checks to ensure that it's (at least kind of) clean.
    # INSERT SANITY HERE

    # Any FERC respondent_id that appears in plants_ferc must also exist in
    # utilities_ferc:
    # INSERT MORE SANITY HERE

    (plants.
     rename(columns={'plant_id': 'id', 'plant_name': 'name'}).
     to_sql(name='plants', con=engine, index=False, if_exists='append',
            dtype={'id': sa.Integer, 'name': sa.String}))

    (utilities.
     rename(columns={'utility_id': 'id', 'utility_name': 'name'}).
     to_sql(name='utilities', con=engine, index=False, if_exists='append',
            dtype={'id': sa.Integer, 'name': sa.String}))

    (utilities_ferc.
     rename(columns={'respondent_id_ferc': 'utility_id_ferc1',
                     'respondent_name_ferc': 'utility_name_ferc1',
                     'utility_id': 'utility_id_pudl'}).
     to_sql(name='utilities_ferc', con=engine, index=False, if_exists='append',
            dtype={'utility_id_ferc1': sa.Integer,
                   'utility_name_ferc1': sa.String,
                   'utility_id_pudl': sa.Integer}))

    (plants_ferc.
     rename(columns={'respondent_id_ferc': 'utility_id_ferc1',
                     'plant_name_ferc': 'plant_name',
                     'plant_id': 'plant_id_pudl'}).
     to_sql(name='plants_ferc', con=engine, index=False, if_exists='append',
            dtype={'utility_id_ferc1': sa.Integer,
                   'plant_name': sa.String,
                   'plant_id_pudl': sa.Integer}))

    (utility_plant_assn.
     to_sql(name='utility_plant_assn', con=engine,
            index=False, if_exists='append',
            dtype={'plant_id': sa.Integer,
                   'utility_id': sa.Integer}))

    # when either eia form is being ingested, include the eia tables as well.
    if eia860_years or eia923_years:
        (utilities_eia.
            rename(columns={'operator_id_eia': 'utility_id_eia',
                            'operator_name_eia': 'utility_name',
                            'utility_id': 'utility_id_pudl'}).
            to_sql(name='utilities_eia', con=engine,
                   index=False, if_exists='append',
                   dtype={'utility_id_eia': sa.Integer,
                          'utility_name': sa.String,
                          'utility_id_pudl': sa.Integer}))

        (plants_eia.
            rename(columns={'plant_name_eia': 'plant_name',
                            'plant_id': 'plant_id_pudl'}).
            to_sql(name='plants_eia', con=engine,
                   index=False, if_exists='append',
                   dtype={'plant_id_eia': sa.Integer,
                          'plant_name': sa.String,
                          'plant_id_pudl': sa.Integer}))


###############################################################################
###############################################################################
# BEGIN DATABASE INITIALIZATION
###############################################################################
###############################################################################


def _ETL_ferc1(pudl_engine, ferc1_tables, ferc1_years, ferc1_testing,
               csvdir, keep_csv):
    if not ferc1_years or not ferc1_tables:
        logger.info('Not ingesting FERC1')
        return None

    # Extract FERC form 1
    ferc1_raw_dfs = pudl.extract.ferc1.extract(ferc1_tables=ferc1_tables,
                                               ferc1_years=ferc1_years,
                                               testing=ferc1_testing)
    # Transform FERC form 1
    ferc1_transformed_dfs = pudl.transform.ferc1.transform(
        ferc1_raw_dfs, ferc1_tables=ferc1_tables)
    # Load FERC form 1
    pudl.load.dict_dump_load(ferc1_transformed_dfs,
                             "FERC 1",
                             pudl_engine,
                             need_fix_inting=pc.need_fix_inting,
                             csvdir=csvdir,
                             keep_csv=keep_csv)


def _ETL_eia(pudl_engine, eia923_tables, eia923_years, eia860_tables,
             eia860_years, csvdir, keep_csv):
    if (not eia923_tables or not eia923_years) and (not eia860_tables or not eia860_years):
        logger.info('Not ingesting EIA.')
        return None

    # Extract EIA forms 923, 860
    eia923_raw_dfs = pudl.extract.eia923.extract(eia923_years=eia923_years)
    eia860_raw_dfs = pudl.extract.eia860.extract(eia860_years=eia860_years)
    # Transform EIA forms 923, 860
    eia923_transformed_dfs = \
        pudl.transform.eia923.transform(eia923_raw_dfs,
                                        eia923_tables=eia923_tables)
    eia860_transformed_dfs = \
        pudl.transform.eia860.transform(eia860_raw_dfs,
                                        eia860_tables=eia860_tables)
    # create an eia transformed dfs dictionary
    eia_transformed_dfs = eia860_transformed_dfs.copy()
    eia_transformed_dfs.update(eia923_transformed_dfs.copy())

    entities_dfs, eia_transformed_dfs = \
        pudl.transform.eia.transform(eia_transformed_dfs,
                                     eia923_years=eia923_years,
                                     eia860_years=eia860_years)
    # Compile transformed dfs for loading...
    transformed_dfs = {"Entities": entities_dfs, "EIA": eia_transformed_dfs}
    # Load step
    for data_source, transformed_df in transformed_dfs.items():
        pudl.load.dict_dump_load(transformed_df,
                                 data_source,
                                 pudl_engine,
                                 need_fix_inting=pc.need_fix_inting,
                                 csvdir=csvdir,
                                 keep_csv=keep_csv)


def _ETL_cems(pudl_engine, epacems_years, csvdir, keep_csv, states):
    """"""
    # If we're not doing CEMS, just stop here to avoid printing messages like
    # "Reading EPA CEMS data...", which could be confusing.
    if not states or not epacems_years:
        logger.info('Not ingesting EPA CEMS.')
        return None
    if states[0].lower() == 'all':
        states = list(pc.cems_states.keys())

    # NOTE: This a generator for raw dataframes
    epacems_raw_dfs = pudl.extract.epacems.extract(
        epacems_years=epacems_years, states=states)
    # NOTE: This is a generator for transformed dataframes
    epacems_transformed_dfs = pudl.transform.epacems.transform(
        pudl_engine=pudl_engine, epacems_raw_dfs=epacems_raw_dfs)
    logger.info("Loading tables from EPA CEMS into PUDL:")
    if logger.isEnabledFor(logging.INFO):
        start_time = time.monotonic()
    with pudl.load.BulkCopy(
            table_name="hourly_emissions_epacems",
            engine=pudl_engine,
            csvdir=csvdir,
            keep_csv=keep_csv) as loader:

        for transformed_df_dict in epacems_transformed_dfs:
            # There's currently only one dataframe in this dict at a time,
            # but that could be changed if useful.
            # The keys to the dict are a tuple (year, month, state)
            for transformed_df in transformed_df_dict.values():
                loader.add(transformed_df)
    if logger.isEnabledFor(logging.INFO):
        time_message = "    Loading    EPA CEMS took {}".format(
            time.strftime("%H:%M:%S",
                          time.gmtime(time.monotonic() - start_time)))
        logger.info(time_message)
        start_time = time.monotonic()
    pudl.models.epacems.finalize(pudl_engine)
    if logger.isEnabledFor(logging.INFO):
        time_message = "    Finalizing EPA CEMS took {}".format(
            time.strftime("%H:%M:%S", time.gmtime(
                time.monotonic() - start_time))
        )
        logger.info(time_message)


def _ETL_ipm(pudl_engine, epaipm_tables, csvdir, keep_csv):
    """
    Extract, transform, and load tables from EPA IPM.

    Args:
        pudl_engine (sqlalchemy.engine): SQLAlchemy database engine, which will be
            used to pull the CSV output into the database.
        epaipm_tables (list): Names of tables to process.
        csvdir (str): Path to the directory into which the CSV file should be
            saved, if it's being kept.
        keep_csv (bool): True if the CSV output should be saved after the data
            has been loaded into the database. False if they should be deleted.
            NOTE: If multiple COPYs are done for the same table_name, only
            the last will be retained by keep_csv, which may be unsatisfying.

    Returns:
        None
    """

    # Extract IPM tables
    epaipm_raw_dfs = pudl.extract.epaipm.extract(epaipm_tables)

    epaipm_transformed_dfs = pudl.transform.epaipm.transform(
        epaipm_raw_dfs, epaipm_tables
    )

    pudl.load.dict_dump_load(
        epaipm_transformed_dfs,
        "EPA IPM",
        pudl_engine,
        need_fix_inting=pc.need_fix_inting,
        csvdir=csvdir,
        keep_csv=keep_csv
    )


def init_db(ferc1_tables=None,
            ferc1_years=None,
            eia923_tables=None,
            eia923_years=None,
            eia860_tables=None,
            eia860_years=None,
            epacems_years=None,
            epacems_states=None,
            epaipm_tables=None,
            pudl_testing=None,
            ferc1_testing=None,
            debug=None,
            csvdir=None,
            keep_csv=None):
    """
    Create the PUDL database and fill it up with data.

    Args:
        ferc1_tables (list): The list of tables that will be created and
            ingested. By default only known to be working tables are ingested.
            That list of tables is defined in pudl.constants.
        ferc1_years (list): The list of years from which to pull FERC Form 1
            data.
        eia923_tables (list): The list of tables that will be created and
            ingested. By default only known to be working tables are ingested.
            That list of tables is defined in pudl.constants.
        eia923_years (iterable): The list of years from which to pull EIA 923
            data.
        eia860_tables (list): The list of tables that will be created and
            ingested. By default only known to be working tables are ingested.
            That list of tables is defined in pudl.constants.
        eia860_years (iterable): The list of years from which to pull EIA 860
            data.
        epacems_years (iterable): The list of years from which to pull EPA CEMS
            data. Note that there's only one EPA CEMS table.
        epacems_states (iterable): The list of states for which we are to pull
            EPA CEMS data. With all states, ETL takes ~8 hours.
        epaipm_tables (list): The list of tables that will be created and
            ingested. By default only known to be working tables are ingested.
            That list of tables is defined in pudl.constants.
        debug (bool): You can tell init_db to ingest whatever list of tables
            you want, but if your desired table is not in the list of known to
            be working tables, you need to set debug=True (otherwise init_db
            won't let you).
    """
    # Make sure that the tables we're being asked to ingest can actually be
    # pulled into both the FERC Form 1 DB, and the PUDL DB...
    logger.info("Beginning PUDL DB ETL process.")

    if (not debug) and (ferc1_tables):
        for table in ferc1_tables:
            if table not in pc.ferc1_pudl_tables:
                raise AssertionError(
                    f"Unrecognized FERC table: {table}."
                )

    if (not debug) and (eia860_tables):
        for table in eia860_tables:
            if table not in pc.eia860_pudl_tables:
                raise AssertionError(
                    f"Unrecognized EIA 860 table: {table}"
                )

    if (not debug) and (eia923_tables):
        for table in eia923_tables:
            if table not in pc.eia923_pudl_tables:
                raise AssertionError(
                    f"Unrecogized EIA 923 table: {table}"
                )

    if (not debug) and (epaipm_tables):
        for table in epaipm_tables:
            if table not in pc.epaipm_pudl_tables:
                raise AssertionError(
                    f"Unrecogized EPA IPM table: {table}"
                )

    # Connect to the PUDL DB, wipe out & re-create tables:
    pudl_engine = connect_db(testing=pudl_testing)
    drop_tables(pudl_engine)
    _create_tables(pudl_engine)

    _ingest_datasets_table(ferc1_years=ferc1_years,
                           eia860_years=eia860_years,
                           eia923_years=eia923_years,
                           epacems_years=epacems_years,
                           engine=pudl_engine)

    # Populate all the static tables:
    logger.info("Ingesting static PUDL tables...")
    ingest_static_tables(pudl_engine)
    # Populate tables that relate FERC1 & EIA923 data to each other.
    logger.info("Sniffing EIA923/FERC1 glue tables...")
    _ingest_glue(engine=pudl_engine,
                 eia923_years=eia923_years,
                 eia860_years=eia860_years,
                 ferc1_years=ferc1_years)

    # Separate the extract/transform/load for the different datsets because
    # they don't depend on each other and it's nice to not have so much stuff
    # in memory at the same time.

    # ETL for FERC form 1
    _ETL_ferc1(pudl_engine=pudl_engine,
               ferc1_tables=ferc1_tables,
               ferc1_years=ferc1_years,
               ferc1_testing=ferc1_testing,
               csvdir=csvdir,
               keep_csv=keep_csv)
    # ETL for EIA forms 860, 923
    _ETL_eia(pudl_engine=pudl_engine,
             eia923_tables=eia923_tables,
             eia923_years=eia923_years,
             eia860_tables=eia860_tables,
             eia860_years=eia860_years,
             csvdir=csvdir,
             keep_csv=keep_csv)
    # ETL for EPA CEMS
    _ETL_cems(pudl_engine=pudl_engine,
              epacems_years=epacems_years,
              states=epacems_states,
              csvdir=csvdir,
              keep_csv=keep_csv)

    _ETL_ipm(
        pudl_engine=pudl_engine,
        epaipm_tables=epaipm_tables,
        csvdir=csvdir,
        keep_csv=keep_csv
    )

    pudl_engine.execute("ANALYZE")
