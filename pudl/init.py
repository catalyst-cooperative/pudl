"""
The Public Utility Data Liberation (PUDL) project core module.

The PUDL project integrates several different public data sets into one well
normalized database allowing easier access and interaction between all of them.
This module defines database tables using the SQLAlchemy Object Relational
Mapper (ORM) and initializes the database from several sources:

 - US Energy Information Agency (EIA):
   - Form 860 (eia860)
   - Form 861 (eia861)
   - Form 923 (eia923)
 - US Federal Energy Regulatory Commission (FERC):
   - Form 1 (ferc1)
   - Form 714 (ferc714)
 - US Environmental Protection Agency (EPA):
   - Air Market Program Data (epaampd)
   - Greenhouse Gas Reporting Program (epaghgrp)
"""

import os.path
import datetime
import time
import pandas as pd
import sqlalchemy as sa

from pudl import settings
import pudl.models.entities
import pudl.models.glue
import pudl.models.eia923
import pudl.models.eia860
import pudl.models.ferc1
import pudl.models.eia
import pudl.models.epacems
import pudl.extract.eia860
import pudl.extract.eia923
import pudl.extract.ferc1
import pudl.extract.epacems
import pudl.transform.ferc1
import pudl.transform.eia923
import pudl.transform.eia860
import pudl.transform.epacems
import pudl.transform.eia
import pudl.transform.pudl
import pudl.load

import pudl.constants as pc

###############################################################################
###############################################################################
# DATABASE CONNECTION & HELPER FUNCTIONS
###############################################################################
###############################################################################


def connect_db(testing=False):
    """Connect to the PUDL database using global settings from settings.py."""
    if testing:
        return sa.create_engine(sa.engine.url.URL(**settings.DB_PUDL_TEST))
    return sa.create_engine(sa.engine.url.URL(**settings.DB_PUDL))


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
        print("""Error dropping and the existing tables. This sometimes
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

###############################################################################
###############################################################################
#   BEGIN INGESTING STATIC & INFRASTRUCTURE TABLES
###############################################################################
###############################################################################


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
    PUDL_Session = sa.orm.sessionmaker(bind=engine)
    pudl_session = PUDL_Session()

    # Populate tables with static data from above.
    pudl_session.add_all(
        [pudl.models.glue.FuelUnit(unit=u)for u in pc.ferc1_fuel_unit_strings])
    pudl_session.add_all([pudl.models.glue.Month(month=i + 1)
                          for i in range(12)])
    pudl_session.add_all(
        [pudl.models.glue.Quarter(
            q=i + 1, end_month=3 * (i + 1)) for i in range(4)])
    pudl_session.add_all(
        [pudl.models.glue.PrimeMover(
            prime_mover=pm) for pm in pc.prime_movers])
    pudl_session.add_all(
        [pudl.models.glue.RTOISO(
            abbr=k, name=v) for k, v in pc.rto_iso.items()])
    pudl_session.add_all([pudl.models.glue.CensusRegion(abbr=k, name=v)
                          for k, v in pc.census_region.items()])
    pudl_session.add_all(
        [pudl.models.glue.NERCRegion(
            abbr=k, name=v) for k, v in pc.nerc_region.items()])
    pudl_session.add_all(
        [pudl.models.eia923.RespondentFrequencyEIA923(abbr=k, unit=v)
         for k, v in pc.respondent_frequency_eia923.items()])
    pudl_session.add_all(
        [pudl.models.eia923.SectorEIA(id=k, name=v)
         for k, v in pc.sector_eia.items()])
    pudl_session.add_all(
        [pudl.models.eia923.ContractTypeEIA923(abbr=k, contract_type=v)
         for k, v in pc.contract_type_eia923.items()])
    pudl_session.add_all(
        [pudl.models.eia923.FuelTypeEIA923(abbr=k, fuel_type=v)
         for k, v in pc.fuel_type_eia923.items()])
    pudl_session.add_all(
        [pudl.models.eia923.PrimeMoverEIA923(abbr=k, prime_mover=v)
         for k, v in pc.prime_movers_eia923.items()])
    pudl_session.add_all(
        [pudl.models.eia923.FuelUnitEIA923(abbr=k, unit=v)
         for k, v in pc.fuel_units_eia923.items()])
    pudl_session.add_all(
        [pudl.models.eia923.FuelTypeAER(abbr=k, fuel_type=v)
         for k, v in pc.fuel_type_aer_eia923.items()])
    pudl_session.add_all(
        [pudl.models.eia923.FuelGroupEIA923(group=gr)
         for gr in pc.fuel_group_eia923])
    pudl_session.add_all(
        [pudl.models.eia923.EnergySourceEIA923(abbr=k, source=v)
         for k, v in pc.energy_source_eia923.items()])
    pudl_session.add_all(
        [pudl.models.eia923.CoalMineTypeEIA923(abbr=k, name=v)
         for k, v in pc.coalmine_type_eia923.items()])
    pudl_session.add_all(
        [pudl.models.eia923.CoalMineStateEIA923(abbr=k, state=v)
         for k, v in pc.coalmine_state_eia923.items()])
    pudl_session.add_all(
        [pudl.models.eia923.CoalMineStateEIA923(abbr=k, state=v)
         for k, v in pc.us_states.items()])  # is this right way to add these?
    pudl_session.add_all(
        [pudl.models.eia923.TransportModeEIA923(abbr=k, mode=v)
         for k, v in pc.transport_modes_eia923.items()])
    pudl_session.add_all(
        [pudl.models.eia923.NaturalGasTransportEIA923(abbr=k, status=v)
         for k, v in pc.natural_gas_transport_eia923.items()])
    pudl_session.add_all([pudl.models.glue.State(abbr=k, name=v)
                          for k, v in pc.us_states.items()])

    # Commit the changes to the DB and close down the session.
    pudl_session.commit()
    pudl_session.close_all()

    # We aren't bringing row_number in to the PUDL DB:
    ferc_accts_df = pc.ferc_electric_plant_accounts.drop('row_number', axis=1)
    # Get rid of excessive whitespace introduced to break long lines (ugh)
    ferc_accts_df.ferc_account_description = \
        ferc_accts_df.ferc_account_description.str.replace('\s+', ' ')

    ferc_accts_df.rename(columns={'ferc_account_id': 'id',
                                  'ferc_account_description': 'description'},
                         inplace=True)

    ferc_accts_df.to_sql('ferc_accounts',
                         con=engine, index=False, if_exists='append',
                         dtype={'id': sa.String,
                                'description': sa.String})

    ferc_depreciation_lines_df = \
        pc.ferc_accumulated_depreciation.drop('row_number', axis=1)

    ferc_depreciation_lines_df.\
        rename(columns={'line_id': 'id',
                        'ferc_account_description': 'description'},
               inplace=True)

    ferc_depreciation_lines_df.\
        to_sql('ferc_depreciation_lines',
               con=engine, index=False, if_exists='append',
               dtype={'id': sa.String,
                      'description': sa.String})


def ingest_glue_tables(engine):
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
        - util_plant_assn: An association table which describes which plants
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
    map_eia_ferc_file = os.path.join(settings.PUDL_DIR,
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
    plant_map['plant_name_ferc'] = plant_map['plant_name_ferc'].str.strip()
    plant_map['plant_name_ferc'] = plant_map['plant_name_ferc'].str.title()

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

    for df, df_n in zip([plants_eia,
                         plants_ferc,
                         utilities_eia,
                         utilities_ferc],
                        ['plants_eia',
                         'plants_ferc',
                         'utilities_eia',
                         'utilities_ferc']):
        assert df[pd.isnull(df).any(axis=1)].shape[0] <= 1,\
            print("breaks on {}".format(df_n))
        df.dropna(inplace=True)

    # Before we start inserting records into the database, let's do some basic
    # sanity checks to ensure that it's (at least kind of) clean.
    # INSERT SANITY HERE

    # Any FERC respondent_id that appears in plants_ferc must also exist in
    # utilities_ferc:
    # INSERT MORE SANITY HERE

    plants.rename(columns={'plant_id': 'id', 'plant_name': 'name'},
                  inplace=True)
    plants.to_sql(name='plants',
                  con=engine, index=False, if_exists='append',
                  dtype={'id': sa.Integer, 'name': sa.String})

    utilities.rename(columns={'utility_id': 'id', 'utility_name': 'name'},
                     inplace=True)
    utilities.to_sql(name='utilities',
                     con=engine, index=False, if_exists='append',
                     dtype={'id': sa.Integer, 'name': sa.String})

    utilities_eia.rename(columns={'operator_id_eia': 'operator_id',
                                  'operator_name_eia': 'operator_name',
                                  'utility_id': 'util_id_pudl'},
                         inplace=True)
    utilities_eia.to_sql(name='utilities_eia',
                         con=engine, index=False, if_exists='append',
                         dtype={'operator_id': sa.Integer,
                                'operator_name': sa.String,
                                'util_id_pudl': sa.Integer})

    utilities_ferc.rename(columns={'respondent_id_ferc': 'respondent_id',
                                   'respondent_name_ferc': 'respondent_name',
                                   'utility_id': 'util_id_pudl'},
                          inplace=True)
    utilities_ferc.to_sql(name='utilities_ferc',
                          con=engine, index=False, if_exists='append',
                          dtype={'respondent_id': sa.Integer,
                                 'respondent_name': sa.String,
                                 'util_id_pudl': sa.Integer})

    plants_eia.rename(columns={'plant_name_eia': 'plant_name',
                               'plant_id': 'plant_id_pudl'},
                      inplace=True)
    plants_eia.to_sql(name='plants_eia',
                      con=engine, index=False, if_exists='append',
                      dtype={'plant_id_eia': sa.Integer,
                             'plant_name': sa.String,
                             'plant_id_pudl': sa.Integer})

    plants_ferc.rename(columns={'respondent_id_ferc': 'respondent_id',
                                'plant_name_ferc': 'plant_name',
                                'plant_id': 'plant_id_pudl'},
                       inplace=True)
    plants_ferc.to_sql(name='plants_ferc',
                       con=engine, index=False, if_exists='append',
                       dtype={'respondent_id': sa.Integer,
                              'plant_name': sa.String,
                              'plant_id_pudl': sa.Integer})

    utility_plant_assn.to_sql(name='util_plant_assn',
                              con=engine, index=False, if_exists='append',
                              dtype={'plant_id': sa.Integer,
                                     'utility_id': sa.Integer})


###############################################################################
###############################################################################
# BEGIN DATABASE INITIALIZATION
###############################################################################
###############################################################################


def extract_ferc1(ferc1_tables=pc.ferc1_pudl_tables,
                  ferc1_years=pc.working_years['ferc1'],
                  testing=False,
                  verbose=True):
    """Extract FERC 1."""
    # BEGIN INGESTING FERC FORM 1 DATA:
    ferc1_engine = pudl.extract.ferc1.connect_db(testing=testing)

    ferc1_raw_dfs = {}
    ferc1_extract_functions = {
        'fuel_ferc1': pudl.extract.ferc1.fuel,
        'plants_steam_ferc1': pudl.extract.ferc1.plants_steam,
        'plants_small_ferc1': pudl.extract.ferc1.plants_small,
        'plants_hydro_ferc1': pudl.extract.ferc1.plants_hydro,
        'plants_pumped_storage_ferc1':
            pudl.extract.ferc1.plants_pumped_storage,
        'plant_in_service_ferc1': pudl.extract.ferc1.plant_in_service,
        'purchased_power_ferc1': pudl.extract.ferc1.purchased_power,
        'accumulated_depreciation_ferc1':
            pudl.extract.ferc1.accumulated_depreciation}

    # define the ferc 1 metadata object
    # this is here because if ferc wasn't ingested in the same session, there
    # will not be a defined metadata object to use to find and grab the tables
    # from the ferc1 mirror db
    if len(pudl.extract.ferc1.ferc1_meta.tables) == 0:
        pudl.extract.ferc1.define_db(max(pc.working_years['ferc1']),
                                     pc.ferc1_default_tables,
                                     pudl.extract.ferc1.ferc1_meta,
                                     basedir=settings.FERC1_DATA_DIR,
                                     verbose=verbose)

    if verbose:
        print("Extracting tables from FERC 1:")
    for table in ferc1_extract_functions:
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


def transform_ferc1(ferc1_raw_dfs,
                    ferc1_tables=pc.ferc1_pudl_tables,
                    verbose=True):
    """Transform FERC 1."""
    ferc1_transform_functions = {
        'fuel_ferc1': pudl.transform.ferc1.fuel,
        'plants_steam_ferc1': pudl.transform.ferc1.plants_steam,
        'plants_small_ferc1': pudl.transform.ferc1.plants_small,
        'plants_hydro_ferc1': pudl.transform.ferc1.plants_hydro,
        'plants_pumped_storage_ferc1':
            pudl.transform.ferc1.plants_pumped_storage,
        'plant_in_service_ferc1': pudl.transform.ferc1.plant_in_service,
        'purchased_power_ferc1': pudl.transform.ferc1.purchased_power,
        'accumulated_depreciation_ferc1':
            pudl.transform.ferc1.accumulated_depreciation
    }
    # create an empty ditctionary to fill up through the transform fuctions
    ferc1_transformed_dfs = {}

    # for each ferc table,
    if verbose:
        print("Transforming dataframes from FERC 1:")
    for table in ferc1_transform_functions:
        if table in ferc1_tables:
            if verbose:
                print("    {}...".format(table))
            ferc1_transform_functions[table](ferc1_raw_dfs,
                                             ferc1_transformed_dfs)

    return ferc1_transformed_dfs


def _ETL_ferc1(pudl_engine, ferc1_tables, ferc1_years, verbose, ferc1_testing,
               csvdir, keep_csv):
    # Extract FERC form 1
    ferc1_raw_dfs = extract_ferc1(ferc1_tables=ferc1_tables,
                                  ferc1_years=ferc1_years,
                                  testing=ferc1_testing,
                                  verbose=verbose)
    # Transform FERC form 1
    ferc1_transformed_dfs = transform_ferc1(ferc1_raw_dfs,
                                            ferc1_tables=ferc1_tables,
                                            verbose=verbose)
    # Load FERC form 1
    pudl.load.dict_dump_load(ferc1_transformed_dfs,
                             "FERC 1",
                             pudl_engine,
                             need_fix_inting=pc.need_fix_inting,
                             verbose=verbose,
                             csvdir=csvdir,
                             keep_csv=keep_csv)


def _ETL_eia(pudl_engine, eia923_tables, eia923_years, eia860_tables,
             eia860_years, verbose, csvdir, keep_csv):
    # Extract EIA forms 923, 860
    eia923_raw_dfs = pudl.extract.eia923.extract(eia923_years=eia923_years,
                                                 verbose=verbose)
    eia860_raw_dfs = pudl.extract.eia860.extract(eia860_years=eia860_years,
                                                 verbose=verbose)
    # Transform EIA forms 923, 860
    eia923_transformed_dfs = \
        pudl.transform.eia923.transform(eia923_raw_dfs,
                                        eia923_tables=eia923_tables,
                                        verbose=verbose)
    eia860_transformed_dfs = \
        pudl.transform.eia860.transform(eia860_raw_dfs,
                                        eia860_tables=eia860_tables,
                                        verbose=verbose)
    # create an eia transformed dfs dictionary
    eia_transformed_dfs = eia860_transformed_dfs.copy()
    eia_transformed_dfs.update(eia923_transformed_dfs.copy())

    entities_dfs, eia_transformed_dfs = \
        pudl.transform.eia.transform(eia_transformed_dfs,
                                     eia923_years=eia923_years,
                                     eia860_years=eia860_years,
                                     verbose=verbose)
    # Compile transformed dfs for loading...
    transformed_dfs = {"Entities": entities_dfs, "EIA": eia_transformed_dfs}
    # Load step
    for data_source, transformed_df in transformed_dfs.items():
        pudl.load.dict_dump_load(transformed_df,
                                 data_source,
                                 pudl_engine,
                                 need_fix_inting=pc.need_fix_inting,
                                 verbose=verbose,
                                 csvdir=csvdir,
                                 keep_csv=keep_csv)


def _ETL_cems(pudl_engine, epacems_years, verbose, csvdir, keep_csv, states):
    """"""
    # If we're not doing CEMS, just stop here to avoid printing messages like
    # "Reading EPA CEMS data...", which could be confusing.
    if states[0].lower() == 'none':
        return None
    if states[0].lower() == 'all':
        states = list(pc.cems_states.keys())

    # NOTE: This a generator for raw dataframes
    epacems_raw_dfs = pudl.extract.epacems.extract(
        epacems_years=epacems_years, states=states, verbose=verbose)
    # NOTE: This is a generator for transformed dataframes
    epacems_transformed_dfs = pudl.transform.epacems.transform(
        epacems_raw_dfs, verbose=verbose
    )
    if verbose:
        print("Loading tables from EPA CEMS into PUDL:")
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
    if verbose:
        time_message = "    Loading    EPA CEMS took {}".format(
            time.strftime("%H:%M:%S",
                          time.gmtime(time.monotonic() - start_time)))
        print(time_message)
        start_time = time.monotonic()
    pudl.models.epacems.finalize(pudl_engine)
    if verbose:
        time_message = "    Finalizing EPA CEMS took {}".format(
            time.strftime("%H:%M:%S", time.gmtime(
                time.monotonic() - start_time))
        )
        print(time_message)


def init_db(ferc1_tables=pc.ferc1_pudl_tables,
            ferc1_years=pc.working_years['ferc1'],
            eia923_tables=pc.eia923_pudl_tables,
            eia923_years=pc.working_years['eia923'],
            eia860_tables=pc.eia860_pudl_tables,
            eia860_years=pc.working_years['eia860'],
            epacems_years=pc.working_years['epacems'],
            epacems_states=list(pc.cems_states.keys()),
            verbose=True, debug=False,
            pudl_testing=False,
            ferc1_testing=False,
            csvdir=os.path.join(settings.PUDL_DIR, 'results', 'csvdump'),
            keep_csv=False):
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
        debug (bool): You can tell init_db to ingest whatever list of tables
            you want, but if your desired table is not in the list of known to
            be working tables, you need to set debug=True (otherwise init_db
            won't let you).
    """
    # Make sure that the tables we're being asked to ingest can actually be
    # pulled into both the FERC Form 1 DB, and the PUDL DB...
    if verbose:
        print("Start ingest at {}".format(datetime.datetime.now().
                                          strftime("%A, %d. %B %Y %I:%M%p")))
    if not debug:
        for table in ferc1_tables:
            # assert(table in pc.ferc1_working_tables)
            assert table in pc.ferc1_pudl_tables

    if not debug:
        for table in eia923_tables:
            assert table in pc.eia923_pudl_tables

    # Connect to the PUDL DB, wipe out & re-create tables:
    pudl_engine = connect_db(testing=pudl_testing)
    drop_tables(pudl_engine)
    _create_tables(pudl_engine)
    # Populate all the static tables:
    if verbose:
        print("Ingesting static PUDL tables...")
    ingest_static_tables(pudl_engine)
    # Populate tables that relate FERC1 & EIA923 data to each other.
    if verbose:
        print("Sniffing EIA923/FERC1 glue tables...")
    ingest_glue_tables(pudl_engine)

    # Separate the extract/transform/load for the different datsets because
    # they don't depend on each other and it's nice to not have so much stuff
    # in memory at the same time.

    # ETL for FERC form 1
    _ETL_ferc1(pudl_engine=pudl_engine,
               ferc1_tables=ferc1_tables,
               ferc1_years=ferc1_years,
               verbose=verbose,
               ferc1_testing=ferc1_testing,
               csvdir=csvdir,
               keep_csv=keep_csv)
    # ETL for EIA forms 860, 923
    _ETL_eia(pudl_engine=pudl_engine,
             eia923_tables=eia923_tables,
             eia923_years=eia923_years,
             eia860_tables=eia860_tables,
             eia860_years=eia860_years,
             verbose=verbose,
             csvdir=csvdir,
             keep_csv=keep_csv)
    # ETL for EPA CEMS
    _ETL_cems(pudl_engine=pudl_engine,
              epacems_years=epacems_years,
              states=epacems_states,
              verbose=verbose,
              csvdir=csvdir,
              keep_csv=keep_csv)

    pudl_engine.execute("ANALYZE")
