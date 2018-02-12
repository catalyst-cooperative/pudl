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

import pandas as pd
import numpy as np
import sqlalchemy as sa
import postgres_copy
import os.path
import re
import datetime

from pudl import settings
from pudl import load
import pudl.models.glue
import pudl.models.eia923
import pudl.models.eia860
import pudl.models.ferc1
import pudl.extract.eia860
import pudl.extract.eia923
import pudl.extract.ferc1
import pudl.transform.ferc1
import pudl.transform.eia923
import pudl.transform.eia860
import pudl.transform.pudl

import pudl.constants as pc

###############################################################################
###############################################################################
# DATABASE CONNECTION & HELPER FUNCTIONS
###############################################################################
###############################################################################


def connect_db(testing=False):
    """Connect to the PUDL database using global settings from settings.py."""
    if(testing):
        return sa.create_engine(sa.engine.url.URL(**settings.DB_PUDL_TEST))
    else:
        return sa.create_engine(sa.engine.url.URL(**settings.DB_PUDL))


def _create_tables(engine):
    """Create the tables associated with the PUDL Database."""
    pudl.models.glue.PUDLBase.metadata.create_all(engine)


def drop_tables(engine):
    """Drop all the tables associated with the PUDL Database and start over."""
    pudl.models.glue.PUDLBase.metadata.drop_all(engine)


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
        [pudl.models.glue.FuelUnit(unit=u) for u in pc.ferc1_fuel_unit_strings.keys()])
    pudl_session.add_all([pudl.models.glue.Month(month=i + 1)
                          for i in range(12)])
    pudl_session.add_all(
        [pudl.models.glue.Quarter(q=i + 1, end_month=3 * (i + 1)) for i in range(4)])
    pudl_session.add_all(
        [pudl.models.glue.PrimeMover(prime_mover=pm) for pm in pc.prime_movers])
    pudl_session.add_all(
        [pudl.models.glue.RTOISO(abbr=k, name=v) for k, v in pc.rto_iso.items()])
    pudl_session.add_all([pudl.models.glue.CensusRegion(abbr=k, name=v)
                          for k, v in pc.census_region.items()])
    pudl_session.add_all(
        [pudl.models.glue.NERCRegion(abbr=k, name=v) for k, v in pc.nerc_region.items()])
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
    Populate the tables which relate the EIA & FERC datasets to each other.

    We have compiled a bunch of information which can be used to map individual
    utilities and plants listed in both the EIA and FERC data sets to each
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

    utilities_ferc = utility_map[['respondent_id_ferc',
                                  'respondent_name_ferc',
                                  'utility_id']]
    utilities_ferc = utilities_ferc.drop_duplicates('respondent_id_ferc')

    # Now we need to create a table that indicates which plants are associated
    # with every utility.

    # These dataframes map our plant_id to FERC respondents and EIA
    # operators -- the equivalents of our "utilities"
    plants_respondents = plant_map[['plant_id', 'respondent_id_ferc']]
    plants_operators = plant_map[['plant_id', 'operator_id_eia']]

    # Here we treat the dataframes like database tables, and join on the
    # FERC respondent_id and EIA operator_id, respectively.
    utility_plant_ferc1 = utilities_ferc.\
        join(plants_respondents.
             set_index('respondent_id_ferc'),
             on='respondent_id_ferc')

    utility_plant_eia923 = utilities_eia.join(
        plants_operators.set_index('operator_id_eia'),
        on='operator_id_eia')

    # Now we can concatenate the two dataframes, and get rid of all the columns
    # except for plant_id and utility_id (which determine the  utility to plant
    # association), and get rid of any duplicates or lingering NaN values...
    utility_plant_assn = pd.concat([utility_plant_eia923, utility_plant_ferc1])
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
# BEGIN FERC 1 INGEST FUNCTIONS
###############################################################################
###############################################################################


def ingest_fuel_ferc1(pudl_engine, ferc1_engine, ferc1_years):
    """
    Clean & ingest f1_fuel table from FERC Form 1 DB into the PUDL DB.

    Read data from the f1_fuel table of our cloned FERC Form 1 database for
    the years specified by ferc1_years, clean it up and re-organize it for
    loading into the PUDL DB. This process includes converting some columns
    to be in terms of our preferred units, like MWh and mmbtu instead of kWh
    and btu. Plant names are also standardized (stripped & Title Case). Fuel
    and fuel unit strings are also standardized using our cleanstrings()
    function and string cleaning dictionaries found in pudl.constants.

    Args:
        pudl_engine (sqlalchemy.engine): Engine for connecting to the PUDL DB.
        ferc1_engine (sqlalchemy.engine): Engine for connecting to the FERC DB.
        ferc1_years (sequence of integers): Years for which we should extract
            fuel data from the FERC1 Database.

    Returns: Nothing.
    """
    # Grab the f1_fuel SQLAlchemy Table object from the metadata object.
    f1_fuel = pudl.extract.ferc1.ferc1_meta.tables['f1_fuel']
    # Generate a SELECT statement that pulls all fields of the f1_fuel table,
    # but only gets records with plant names, and non-zero fuel amounts:
    f1_fuel_select = sa.sql.select([f1_fuel]).\
        where(f1_fuel.c.fuel != '').\
        where(f1_fuel.c.fuel_quantity > 0).\
        where(f1_fuel.c.plant_name != '').\
        where(f1_fuel.c.report_year.in_(ferc1_years))
    # Use the above SELECT to pull those records into a DataFrame:
    fuel_ferc1_df = pd.read_sql(f1_fuel_select, ferc1_engine)

    fuel_ferc1_df = pudl.transform.ferc1.clean_fuel_ferc1(fuel_ferc1_df)

    fuel_ferc1_df.to_sql(name='fuel_ferc1',
                         con=pudl_engine, index=False, if_exists='append',
                         dtype={'respondent_id': sa.Integer,
                                'report_year': sa.Integer})


def ingest_plants_steam_ferc1(pudl_engine, ferc1_engine, ferc1_years):
    """
    Clean f1_steam table of the FERC Form 1 DB and pull into the PUDL DB.

    Load data about large thermal plants from our cloned FERC Form 1 DB into
    the PUDL DB. Clean up and reorganize some of the information along the way.
    This includes converting to our preferred units of MWh and MW, as well as
    standardizing the strings describing the kind of plant and construction.

    Args:
        pudl_engine (sqlalchemy.engine): Engine for connecting to the PUDL DB.
        ferc1_engine (sqlalchemy.engine): Engine for connecting to the FERC DB.
        ferc1_years (sequence of integers): Years for which we should extract
            fuel data from the FERC1 Database.

    Returns: Nothing.
    """
    f1_steam = pudl.extract.ferc1.ferc1_meta.tables['f1_steam']
    f1_steam_select = sa.sql.select([f1_steam]).\
        where(f1_steam.c.net_generation > 0).\
        where(f1_steam.c.plant_name != '').\
        where(f1_steam.c.report_year.in_(ferc1_years))

    ferc1_steam_df = pd.read_sql(f1_steam_select, ferc1_engine)
    # Discard DataFrame columns that we aren't pulling into PUDL:
    ferc1_steam_df.drop(['spplmnt_num', 'row_number', 'row_seq', 'row_prvlg',
                         'report_prd'], axis=1, inplace=True)

    # Standardize plant_name capitalization and remove leading/trailing white
    # space -- necesary b/c plant_name is part of many foreign keys.
    ferc1_steam_df['plant_name'] = ferc1_steam_df['plant_name'].str.strip()
    ferc1_steam_df['plant_name'] = ferc1_steam_df['plant_name'].str.title()

    # Take the messy free-form construction_type and plant_kind fields, and do
    # our best to map them to some canonical categories...
    # this is necessarily imperfect:

    ferc1_steam_df.type_const = \
        pudl.transform.pudl.cleanstrings(ferc1_steam_df.type_const,
                                         pc.ferc1_construction_type_strings,
                                         unmapped=np.nan)
    ferc1_steam_df.plant_kind = \
        pudl.transform.pudl.cleanstrings(ferc1_steam_df.plant_kind,
                                         pc.ferc1_plant_kind_strings,
                                         unmapped=np.nan)

    # Force the construction and installation years to be numeric values, and
    # set them to NA if they can't be converted. (table has some junk values)
    ferc1_steam_df['yr_const'] = pd.to_numeric(
        ferc1_steam_df['yr_const'],
        errors='coerce')
    ferc1_steam_df['yr_installed'] = pd.to_numeric(
        ferc1_steam_df['yr_installed'],
        errors='coerce')

    # Converting everything to per MW and MWh units...
    ferc1_steam_df['cost_per_mw'] = 1000 * ferc1_steam_df['cost_per_kw']
    ferc1_steam_df.drop('cost_per_kw', axis=1, inplace=True)
    ferc1_steam_df['net_generation_mwh'] = \
        ferc1_steam_df['net_generation'] / 1000
    ferc1_steam_df.drop('net_generation', axis=1, inplace=True)
    ferc1_steam_df['expns_per_mwh'] = 1000 * ferc1_steam_df['expns_kwh']
    ferc1_steam_df.drop('expns_kwh', axis=1, inplace=True)

    ferc1_steam_df.rename(columns={
        # FERC 1 DB Name      PUDL DB Name
        'yr_const': 'year_constructed',
        'type_const': 'construction_type',
        'asset_retire_cost': 'asset_retirement_cost',
        'yr_installed': 'year_installed',
        'tot_capacity': 'total_capacity_mw',
        'peak_demand': 'peak_demand_mw',
        'plnt_capability': 'plant_capability_mw',
        'when_limited': 'water_limited_mw',
        'when_not_limited': 'not_water_limited_mw',
        'avg_num_of_emp': 'avg_num_employees',
        'net_generation': 'net_generation_mwh',
        'cost_of_plant_to': 'total_cost_of_plant',
        'expns_steam_othr': 'expns_steam_other',
        'expns_engnr': 'expns_engineering',
        'tot_prdctn_expns': 'expns_production_total'},
        inplace=True)
    ferc1_steam_df.to_sql(name='plants_steam_ferc1',
                          con=pudl_engine, index=False, if_exists='append',
                          dtype={'respondent_id': sa.Integer,
                                 'report_year': sa.Integer,
                                 'construction_type': sa.String,
                                 'plant_kind': sa.String,
                                 'year_constructed': sa.Integer,
                                 'year_installed': sa.Integer})


def ingest_plants_hydro_ferc1(pudl_engine, ferc1_engine, ferc1_years):
    """
    Ingest f1_hydro table of FERC Form 1 DB into PUDL DB.

    Load data about hydroelectric generation resources from our cloned FERC 1
    database into the PUDL DB. Standardizes plant names (stripping whitespace
    and Using Title Case).  Also converts into our preferred units of MW and
    MWh.

    Args:
        pudl_engine (sqlalchemy.engine): Engine for connecting to the PUDL DB.
        ferc1_engine (sqlalchemy.engine): Engine for connecting to the FERC DB.
        ferc1_years (sequence of integers): Years for which we should extract
            fuel data from the FERC1 Database.

    Returns: Nothing.
    """
    f1_hydro = pudl.extract.ferc1.ferc1_meta.tables['f1_hydro']

    f1_hydro_select = sa.sql.select([f1_hydro]).\
        where(f1_hydro.c.plant_name != '').\
        where(f1_hydro.c.report_year.in_(ferc1_years))

    ferc1_hydro_df = pd.read_sql(f1_hydro_select, ferc1_engine)
    ferc1_hydro_df.drop(['spplmnt_num', 'row_number', 'row_seq', 'row_prvlg',
                         'report_prd'], axis=1, inplace=True)

    # Standardize plant_name capitalization and remove leading/trailing white
    # space -- necesary b/c plant_name is part of many foreign keys.
    ferc1_hydro_df['plant_name'] = ferc1_hydro_df['plant_name'].str.strip()
    ferc1_hydro_df['plant_name'] = ferc1_hydro_df['plant_name'].str.title()

    # Converting kWh to MWh
    ferc1_hydro_df['net_generation_mwh'] = \
        ferc1_hydro_df['net_generation'] / 1000.0
    ferc1_hydro_df.drop('net_generation', axis=1, inplace=True)
    # Converting cost per kW installed to cost per MW installed:
    ferc1_hydro_df['cost_per_mw'] = ferc1_hydro_df['cost_per_kw'] * 1000.0
    ferc1_hydro_df.drop('cost_per_kw', axis=1, inplace=True)
    # Converting kWh to MWh
    ferc1_hydro_df['expns_per_mwh'] = ferc1_hydro_df['expns_kwh'] * 1000.0
    ferc1_hydro_df.drop('expns_kwh', axis=1, inplace=True)

    ferc1_hydro_df['yr_const'] = pd.to_numeric(
        ferc1_hydro_df['yr_const'],
        errors='coerce')
    ferc1_hydro_df['yr_installed'] = pd.to_numeric(
        ferc1_hydro_df['yr_installed'],
        errors='coerce')
    ferc1_hydro_df.dropna(inplace=True)
    ferc1_hydro_df.rename(columns={
        # FERC1 DB          PUDL DB
        'project_no': 'project_number',
        'yr_const': 'year_constructed',
        'plant_const': 'plant_construction',
        'yr_installed': 'year_installed',
        'tot_capacity': 'total_capacity_mw',
        'peak_demand': 'peak_demand_mw',
        'plant_hours': 'plant_hours_connected_while_generating',
        'favorable_cond': 'net_capacity_favorable_conditions_mw',
        'adverse_cond': 'net_capacity_adverse_conditions_mw',
        'avg_num_of_emp': 'avg_num_employees',
        'cost_of_land': 'cost_land',
        'expns_engnr': 'expns_engineering',
        'expns_total': 'expns_production_total',
        'asset_retire_cost': 'asset_retirement_cost'
    }, inplace=True)

    ferc1_hydro_df.to_sql(name='plants_hydro_ferc1',
                          con=pudl_engine, index=False, if_exists='append',
                          dtype={'respondent_id': sa.Integer,
                                 'report_year': sa.Integer})


def ingest_plants_pumped_storage_ferc1(pudl_engine, ferc1_engine, ferc1_years):
    """
    Ingest f1_pumped_storage table of FERC Form 1 DB into PUDL DB.

    Load data about pumped hydro storage facilities from our cloned FERC 1
    database into the PUDL DB. Standardizes plant names (stripping whitespace
    and Using Title Case).  Also converts into our preferred units of MW and
    MWh.

    Args:
       pudl_engine (sqlalchemy.engine): Engine for connecting to the PUDL DB.
       ferc1_engine (sqlalchemy.engine): Engine for connecting to the FERC DB.
       ferc1_years (sequence of integers): Years for which we should extract
           fuel data from the FERC1 Database.

    Returns: Nothing.
    """
    f1_pumped_storage = \
        pudl.extract.ferc1.ferc1_meta.tables['f1_pumped_storage']

    # Removing the empty records.
    # This reduces the entries for 2015 from 272 records to 27.
    f1_pumped_storage_select = sa.sql.select([f1_pumped_storage]).\
        where(f1_pumped_storage.c.plant_name != '').\
        where(f1_pumped_storage.c.report_year.in_(ferc1_years))

    ferc1_pumped_storage_df = pd.read_sql(
        f1_pumped_storage_select, ferc1_engine)
    ferc1_pumped_storage_df.drop(['spplmnt_num', 'row_number', 'row_seq',
                                  'row_prvlg', 'report_prd'],
                                 axis=1, inplace=True)

    # Standardize plant_name capitalization and remove leading/trailing white
    # space -- necesary b/c plant_name is part of many foreign keys.
    ferc1_pumped_storage_df['plant_name'] = \
        ferc1_pumped_storage_df['plant_name'].str.strip()
    ferc1_pumped_storage_df['plant_name'] = \
        ferc1_pumped_storage_df['plant_name'].str.title()

    # Converting kWh to MWh
    ferc1_pumped_storage_df['net_generation_mwh'] = \
        ferc1_pumped_storage_df['net_generation'] / 1000.0
    ferc1_pumped_storage_df.drop('net_generation', axis=1, inplace=True)

    ferc1_pumped_storage_df['energy_used_for_pumping_mwh'] = \
        ferc1_pumped_storage_df['energy_used'] / 1000.0
    ferc1_pumped_storage_df.drop('energy_used', axis=1, inplace=True)

    ferc1_pumped_storage_df['net_load_mwh'] = \
        ferc1_pumped_storage_df['net_load'] / 1000.0
    ferc1_pumped_storage_df.drop('net_load', axis=1, inplace=True)

    # Converting cost per kW installed to cost per MW installed:
    ferc1_pumped_storage_df['cost_per_mw'] = \
        ferc1_pumped_storage_df['cost_per_kw'] * 1000.0
    ferc1_pumped_storage_df.drop('cost_per_kw', axis=1, inplace=True)

    ferc1_pumped_storage_df['expns_per_mwh'] = \
        ferc1_pumped_storage_df['expns_kwh'] * 1000.0
    ferc1_pumped_storage_df.drop('expns_kwh', axis=1, inplace=True)

    ferc1_pumped_storage_df['yr_const'] = pd.to_numeric(
        ferc1_pumped_storage_df['yr_const'],
        errors='coerce')
    ferc1_pumped_storage_df['yr_installed'] = pd.to_numeric(
        ferc1_pumped_storage_df['yr_installed'],
        errors='coerce')

    ferc1_pumped_storage_df.dropna(inplace=True)

    ferc1_pumped_storage_df.rename(columns={
        # FERC1 DB          PUDL DB
        'tot_capacity': 'total_capacity_mw',
        'project_no': 'project_number',
        'peak_demand': 'peak_demand_mw',
        'yr_const': 'year_constructed',
        'yr_installed': 'year_installed',
        'plant_hours': 'plant_hours_connected_while_generating',
        'plant_capability': 'plant_capability_mw',
        'avg_num_of_emp': 'avg_num_employees',
        'cost_wheels': 'cost_wheels_turbines_generators',
        'cost_electric': 'cost_equipment_electric',
        'cost_misc_eqpmnt': 'cost_equipment_misc',
        'cost_of_plant': 'cost_plant_total',
        'expns_water_pwr': 'expns_water_for_pwr',
        'expns_pump_strg': 'expns_pump_storage',
        'expns_misc_power': 'expns_generation_misc',
        'expns_misc_plnt': 'expns_misc_plant',
        'expns_producton': 'expns_production_before_pumping',
        'tot_prdctn_exns': 'expns_production_total'},
        inplace=True)

    ferc1_pumped_storage_df.to_sql(name='plants_pumped_storage_ferc1',
                                   con=pudl_engine, index=False,
                                   if_exists='append')


def ingest_accumulated_depreciation_ferc1(pudl_engine,
                                          ferc1_engine,
                                          ferc1_years):
    """
    Ingest f1_accumdepr_prvs table from FERC Form 1 DB into PUDL DB.

    Load data about accumulated depreciation from our cloned FERC Form 1
    database into the PUDL DB. This information is organized by FERC account,
    with each line of the FERC Form 1 having a different descriptive
    identifier like 'balance_end_of_year' or 'transmission'.

    Args:
       pudl_engine (sqlalchemy.engine): Engine for connecting to the PUDL DB.
       ferc1_engine (sqlalchemy.engine): Engine for connecting to the FERC DB.
       ferc1_years (sequence of integers): Years for which we should extract
           fuel data from the FERC1 Database.

    Returns: Nothing.
    """
    f1_accumdepr_prvsn = \
        pudl.extract.ferc1.ferc1_meta.tables['f1_accumdepr_prvsn']
    f1_accumdepr_prvsn_select = sa.sql.select([f1_accumdepr_prvsn]).\
        where(f1_accumdepr_prvsn.c.report_year.in_(ferc1_years))

    ferc1_apd_df = pd.read_sql(f1_accumdepr_prvsn_select, ferc1_engine)

    # Discard DataFrame columns that we aren't pulling into PUDL. For
    ferc1_apd_df.drop(['spplmnt_num', 'row_seq',
                       'row_prvlg', 'item', 'report_prd'],
                      axis=1, inplace=True)

    ferc1_acct_apd = pc.ferc_accumulated_depreciation.drop(
        ['ferc_account_description'], axis=1)
    ferc1_acct_apd.dropna(inplace=True)
    ferc1_acct_apd['row_number'] = ferc1_acct_apd['row_number'].astype(int)

    ferc1_accumdepr_prvsn_df = pd.merge(ferc1_apd_df, ferc1_acct_apd,
                                        how='left', on='row_number')
    ferc1_accumdepr_prvsn_df.drop('row_number', axis=1, inplace=True)

    ferc1_accumdepr_prvsn_df.rename(columns={
        # FERC1 DB   PUDL DB
        'total_cde': 'total'},
        inplace=True)

    ferc1_accumdepr_prvsn_df.\
        to_sql(name='accumulated_depreciation_ferc1',
               con=pudl_engine, index=False, if_exists='append',
               dtype={'respondent_id': sa.Integer,
                      'report_year': sa.Integer,
                      'line_id': sa.String,
                      'total': sa.Numeric(14, 2),
                      'electric_plant': sa.Numeric(14, 2),
                      'future_plant': sa.Numeric(14, 2),
                      'leased plant': sa.Numeric(14, 2)})


def ingest_plant_in_service_ferc1(pudl_engine, ferc1_engine, ferc1_years):
    """
    Ingest f1_plant_in_srvce table of FERC Form 1 DB into PUDL DB.

    Load data about the financial value of utility plant in service from our
    cloned FERC Form 1 database into the PUDL DB. This information is organized
    by FERC account, with each line of the FERC Form 1 having a different FERC
    account id (most are numeric and correspond to FERC's Uniform Electric
    System of Accounts). As of PUDL v0.1, this data is only valid from 2007
    onward, as the line numbers for several accounts are different in earlier
    years.

    Args:
        pudl_engine (sqlalchemy.engine): Engine for connecting to the PUDL DB.
        ferc1_engine (sqlalchemy.engine): Engine for connecting to the FERC DB.
        ferc1_years (sequence of integers): Years for which we should extract
            fuel data from the FERC1 Database.

       Returns: Nothing.
    """
    f1_plant_in_srvce = \
        pudl.extract.ferc1.ferc1_meta.tables['f1_plant_in_srvce']
    f1_plant_in_srvce_select = sa.sql.select([f1_plant_in_srvce]).\
        where(
            sa.sql.and_(
                f1_plant_in_srvce.c.report_year.in_(ferc1_years),
                # line_no mapping is invalid before 2007
                f1_plant_in_srvce.c.report_year >= 2007
            )
    )

    ferc1_pis_df = pd.read_sql(f1_plant_in_srvce_select, ferc1_engine)

    # Discard DataFrame columns that we aren't pulling into PUDL. For the
    # Plant In Service table, we need to hold on to the row_number because it
    # corresponds to a FERC account number.
    ferc1_pis_df.drop(['spplmnt_num', 'row_seq', 'row_prvlg', 'report_prd'],
                      axis=1, inplace=True)

    # Now we need to add a column to the DataFrame that has the FERC account
    # IDs corresponding to the row_number that's already in there...
    ferc_accts_df = pc.ferc_electric_plant_accounts.drop(
        ['ferc_account_description'], axis=1)
    ferc_accts_df.dropna(inplace=True)
    ferc_accts_df['row_number'] = ferc_accts_df['row_number'].astype(int)

    ferc1_pis_df = pd.merge(ferc1_pis_df, ferc_accts_df,
                            how='left', on='row_number')
    ferc1_pis_df.drop('row_number', axis=1, inplace=True)

    ferc1_pis_df.rename(columns={
        # FERC 1 DB Name  PUDL DB Name
        'begin_yr_bal': 'beginning_year_balance',
        'addition': 'additions',
        'yr_end_bal': 'year_end_balance'},
        inplace=True)
    ferc1_pis_df.to_sql(name='plant_in_service_ferc1',
                        con=pudl_engine, index=False, if_exists='append',
                        dtype={'respondent_id': sa.Integer,
                               'report_year': sa.Integer,
                               'ferc_account_id': sa.String,
                               'beginning_year_balance': sa.Numeric(14, 2),
                               'additions': sa.Numeric(14, 2),
                               'retirements': sa.Numeric(14, 2),
                               'adjustments': sa.Numeric(14, 2),
                               'transfers': sa.Numeric(14, 2),
                               'year_end_balance': sa.Numeric(14, 2)})


def ingest_plants_small_ferc1(pudl_engine, ferc1_engine, ferc1_years):
    """
    Ingest f1_gnrt_plant table from our cloned FERC Form 1 DB into PUDL DB.

    This FERC Form 1 table contains information about a large number of small
    plants, including many small hydroelectric and other renewable generation
    facilities. Unfortunately the data is not well standardized, and so the
    plants have been categorized manually, with the results of that
    categorization stored in an Excel spreadsheet. This function reads in the
    plant type data from the spreadsheet and merges it with the rest of the
    information from the FERC DB based on record number, FERC respondent ID,
    and report year. When possible the FERC license number for small hydro
    plants is also manually extracted from the data.

    This categorization will need to be renewed with each additional year of
    FERC data we pull in. As of v0.1 the small plants have been categorized
    for 2004-2015.

    Args:
        pudl_engine (sqlalchemy.engine): Engine for connecting to the PUDL DB.
        ferc1_engine (sqlalchemy.engine): Engine for connecting to the FERC DB.
        ferc1_years (sequence of integers): Years for which we should extract
            fuel data from the FERC1 Database.

    Returns: Nothing.
    """
    import os.path
    from sqlalchemy import or_

    assert min(ferc1_years) >= min(pc.working_years['ferc1']),\
        """Year {} is too early. Small plant data has not been categorized for
        before 2004.""".format(min(ferc1_years))
    assert max(ferc1_years) <= max(pc.working_years['ferc1']),\
        """Year {} is too recent. Small plant data has not been categorized for
        any year after 2015.""".format(max(ferc1_years))
    f1_small = pudl.extract.ferc1.ferc1_meta.tables['f1_gnrt_plant']
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

    # Standardize plant_name capitalization and remove leading/trailing white
    # space -- necesary b/c plant_name is part of many foreign keys.
    ferc1_small_df['plant_name'] = ferc1_small_df['plant_name'].str.strip()
    ferc1_small_df['plant_name'] = ferc1_small_df['plant_name'].str.title()

    ferc1_small_df['kind_of_fuel'] = ferc1_small_df['kind_of_fuel'].str.strip()
    ferc1_small_df['kind_of_fuel'] = ferc1_small_df['kind_of_fuel'].str.title()

    # Force the construction and installation years to be numeric values, and
    # set them to NA if they can't be converted. (table has some junk values)
    ferc1_small_df['yr_constructed'] = pd.to_numeric(
        ferc1_small_df['yr_constructed'],
        errors='coerce')
    # Convert from cents per mmbtu to dollars per mmbtu to be consistent
    # with the f1_fuel table data. Also, let's use a clearer name.
    ferc1_small_df['fuel_cost_per_mmbtu'] = ferc1_small_df['fuel_cost'] / 100.0
    ferc1_small_df.drop('fuel_cost', axis=1, inplace=True)

    # Create a single "record number" for the individual lines in the FERC
    # Form 1 that report different small plants, so that we can more easily
    # tell whether they are adjacent to each other in the reporting.
    ferc1_small_df['record_number'] = 46 * ferc1_small_df['spplmnt_num'] + \
        ferc1_small_df['row_number']

    # Unforunately the plant types were not able to be parsed automatically
    # in this table. It's been done manually for 2004-2015, and the results
    # get merged in in the following section.
    small_types_file = os.path.join(settings.PUDL_DIR,
                                    'results',
                                    'ferc1_small_plants',
                                    'small_plants_2004-2016.xlsx')
    small_types_df = pd.read_excel(small_types_file)

    # Only rows with plant_type set will give us novel information.
    small_types_df.dropna(subset=['plant_type', ], inplace=True)
    # We only need this small subset of the columns to extract the plant type.
    small_types_df = small_types_df[['report_year', 'respondent_id',
                                     'record_number', 'plant_name_clean',
                                     'plant_type', 'ferc_license']]

    # Munge the two dataframes together, keeping everything from the
    # frame we pulled out of the FERC1 DB, and supplementing it with the
    # plant_name_clean, plant_type, and ferc_license fields from our hand
    # made file.
    ferc1_small_df = pd.merge(ferc1_small_df,
                              small_types_df,
                              how='left',
                              on=['report_year',
                                  'respondent_id',
                                  'record_number'])

    # We don't need to pull these columns into PUDL, so drop them:
    ferc1_small_df.drop(['row_seq', 'row_prvlg', 'report_prd',
                         'row_number', 'spplmnt_num', 'record_number'],
                        axis=1, inplace=True)

    # Standardize plant_name capitalization and remove leading/trailing white
    # space, so that plant_name_clean matches formatting of plant_name
    ferc1_small_df['plant_name_clean'] = \
        ferc1_small_df['plant_name_clean'].str.strip()
    ferc1_small_df['plant_name_clean'] = \
        ferc1_small_df['plant_name_clean'].str.title()

    ferc1_small_df.rename(columns={
        # FERC 1 DB Name      PUDL DB Name
        'yr_constructed': 'year_constructed',
        'capacity_rating': 'total_capacity_mw',
        'net_demand': 'peak_demand_mw',
        'net_generation': 'net_generation_mwh',
        'plant_cost': 'total_cost_of_plant',
        'plant_cost_mw': 'cost_of_plant_per_mw',
        'operation': 'cost_of_operation',
        'expns_maint': 'expns_maintenance',
        'fuel_cost': 'fuel_cost_per_mmbtu'},
        inplace=True)
    ferc1_small_df.to_sql(name='plants_small_ferc1',
                          con=pudl_engine, index=False, if_exists='append',
                          dtype={'respondent_id': sa.Integer,
                                 'report_year': sa.Integer,
                                 'plant_name': sa.String,
                                 'plant_name_clean': sa.String,
                                 'plant_type': sa.String,
                                 'kind_of_fuel': sa.String,
                                 'ferc_license': sa.Integer,
                                 'year_constructed': sa.Integer,
                                 'total_capacity_mw': sa.Float,
                                 'peak_demand_mw': sa.Float,
                                 'net_generation_mwh': sa.Float,
                                 'total_cost_of_plant': sa.Numeric(14, 2),
                                 'cost_of_plant_per_mw': sa.Numeric(14, 2),
                                 'cost_of_operation': sa.Numeric(14, 2),
                                 'expns_fuel': sa.Numeric(14, 2),
                                 'expns_maintenance': sa.Numeric(14, 2),
                                 'fuel_cost_per_mmbtu': sa.Numeric(14, 2)})


def ingest_purchased_power_ferc1(pudl_engine, ferc1_engine, ferc1_years):
    """
    Ingest f1_purchased_pwr table from our cloned FERC Form 1 DB into PUDL DB.

    This function pulls FERC Form 1 data about inter-untility power purchases
    into the PUDL DB. This includes how much electricty was purchased, how
    much it cost, and who it was purchased from. Unfortunately the field
    describing which other utility the power was being bought from is poorly
    standardized, making it difficult to correlate with other data. It will
    need to be categorized by hand or with some fuzzy matching eventually.

    Args:
        pudl_engine (sqlalchemy.engine): Engine for connecting to the PUDL DB.
        ferc1_engine (sqlalchemy.engine): Engine for connecting to the FERC DB.
        ferc1_years (sequence of integers): Years for which we should extract
            fuel data from the FERC1 Database.

    Returns: Nothing.
    """
    f1_purchased_pwr = pudl.extract.ferc1.ferc1_meta.tables['f1_purchased_pwr']
    f1_purchased_pwr_select = sa.sql.select([f1_purchased_pwr]).\
        where(f1_purchased_pwr.c.report_year.in_(ferc1_years))

    ferc1_purchased_pwr_df = pd.read_sql(f1_purchased_pwr_select, ferc1_engine)

    ferc1_purchased_pwr_df.drop(['spplmnt_num', 'row_number', 'row_seq',
                                 'row_prvlg', 'report_prd'],
                                axis=1, inplace=True)
    ferc1_purchased_pwr_df.replace(to_replace='', value=np.nan, inplace=True)
    ferc1_purchased_pwr_df.dropna(subset=['sttstcl_clssfctn',
                                          'rtsched_trffnbr'], inplace=True)

    ferc1_purchased_pwr_df.rename(columns={
        # FERC 1 DB Name  PUDL DB Name
        'athrty_co_name': 'authority_company_name',
        'sttstcl_clssfctn': 'statistical_classification',
        'rtsched_trffnbr': 'rate_schedule_tariff_number',
        'avgmth_bill_dmnd': 'average_billing_demand',
        'avgmth_ncp_dmnd': 'average_monthly_ncp_demand',
        'avgmth_cp_dmnd': 'average_monthly_cp_demand',
        'mwh_recv': 'mwh_received',
        'mwh_delvd': 'mwh_delivered',
        'dmnd_charges': 'demand_charges',
        'erg_charges': 'energy_charges',
        'othr_charges': 'other_charges',
        'settlement_tot': 'settlement_total'},
        inplace=True)

    ferc1_purchased_pwr_df.to_sql(
        name='purchased_power_ferc1',
        con=pudl_engine, index=False,
        if_exists='append',
        dtype={'respondent_id': sa.Integer,
               'report_year': sa.Integer,
               'authority_company_name': sa.String,
               'statistical_classification': sa.String,
               'rate_schedule_tariff_number': sa.String,
               'average_billing_demand': sa.String,
               'average_monthly_ncp_demand': sa.String,
               'average_monthly_cp_demand': sa.String,
               'mwh_purchased': sa.Numeric(14, 2),
               'mwh_received': sa.Numeric(14, 2),
               'mwh_delivered': sa.Numeric(14, 2),
               'demand_charges': sa.Numeric(14, 2),
               'energy_charges': sa.Numeric(14, 2),
               'other_charges': sa.Numeric(14, 2),
               'settlement_total': sa.Numeric(14, 2)})


def ingest_stocks_eia923(pudl_engine, eia923_dfs, csvdir='', keep_csv=True):
    """Ingest data on fuel stocks from EIA Form 923."""
    pass


###############################################################################
###############################################################################
# BEGIN DATABASE INITIALIZATION
###############################################################################
###############################################################################


def ingest_eia860(pudl_engine,
                  eia860_years=pc.working_years['eia860'],
                  eia860_tables=pc.eia860_pudl_tables,
                  verbose=True, debug=False, testing=False,
                  csvdir=os.path.join(settings.PUDL_DIR,
                                      'results', 'csvdump'),
                  keep_csv=True):
    """Wrapper function that ingests all the EIA Form 860 tables."""
    # Prep for ingesting EIA860
    eia860_dfs = \
        pudl.extract.eia860.create_dfs_eia860(files=pc.files_eia860,
                                              eia860_years=eia860_years,
                                              verbose=verbose)
    # NOW START INGESTING EIA860 DATA:
    eia860_transform_functions = {
        'ownership_eia860': pudl.transform.eia860.ownership,
        'generators_eia860': pudl.transform.eia860.generators,
        'plants_eia860': pudl.transform.eia860.plants,
        'boiler_generator_assn_eia860': pudl.transform.eia860.boiler_generator_assn,
        'utilities_eia860': pudl.transform.eia860.utilities}
    eia860_transformed_dfs = {}

    for table in eia860_transform_functions.keys():
        if table in eia860_tables:
            if verbose:
                print("Ingesting {} from EIA 860 into PUDL.".format(table))
            eia860_transform_functions[table](eia860_dfs,
                                              eia860_transformed_dfs)

    load.eia860(eia860_transformed_dfs,
                pudl_engine,
                csvdir=csvdir,
                keep_csv=keep_csv)


def ingest_eia923(pudl_engine,
                  eia923_tables=pc.eia923_pudl_tables,
                  eia923_years=pc.working_years['eia923'],
                  verbose=True, debug=False, testing=False,
                  csvdir=os.path.join(settings.PUDL_DIR, 'results', 'csvdump'),
                  keep_csv=True):
    """Wrapper function that ingests all the EIA Form 923 tables."""
    # Prep for ingesting EIA923
    # Create excel objects
    eia923_xlsx = pudl.extract.eia923.get_eia923_xlsx(eia923_years)

    # Create DataFrames
    eia923_dfs = {}
    for page in pc.tab_map_eia923.columns:
        if (page == 'plant_frame'):
            eia923_dfs[page] = pudl.extract.eia923.get_eia923_plants(
                eia923_years, eia923_xlsx)
        else:
            eia923_dfs[page] = pudl.extract.eia923.\
                get_eia923_page(page, eia923_xlsx,
                                years=eia923_years,
                                verbose=verbose)

    # NOW START INGESTING EIA923 DATA:
    eia923_transform_functions = {
        'plants_eia923': pudl.transform.eia923.plants,
        'generation_fuel_eia923': pudl.transform.eia923.generation_fuel,
        'boilers_eia923': pudl.transform.eia923.boilers,
        'boiler_fuel_eia923': pudl.transform.eia923.boiler_fuel,
        'generation_eia923': pudl.transform.eia923.generation,
        'generators_eia923': pudl.transform.eia923.generators,
        'coalmine_eia923': pudl.transform.eia923.coalmine
    }
    eia923_transformed_dfs = {}

    for table in eia923_transform_functions.keys():
        if table in eia923_tables:
            if verbose:
                print("Transforming {} from EIA 923.".format(table))
            eia923_transform_functions[table](eia923_dfs,
                                              eia923_transformed_dfs)

    for key, value in eia923_transformed_dfs.items():
        if verbose:
            print("Loading {} from EIA 923 into PUDL.".format(key))
        load._csv_dump_load(value,
                            key,
                            pudl_engine,
                            csvdir=csvdir,
                            keep_csv=keep_csv)

    # FRC table requires the auto incremented id's from the coalmine table so
    # we need to load the coalmine table which requires the  pudl_engine, so
    # this function has differnt inputs than the other transform functions.
    pudl.transform.eia923.fuel_reciepts_costs(eia923_dfs,
                                              eia923_transformed_dfs,
                                              pudl_engine)

    load._csv_dump_load(eia923_transformed_dfs['fuel_receipts_costs_eia923'],
                        'fuel_receipts_costs_eia923',
                        pudl_engine,
                        csvdir=csvdir,
                        keep_csv=keep_csv)


def ingest_ferc1(pudl_engine,
                 ferc1_tables=pc.ferc1_pudl_tables,
                 ferc1_years=pc.working_years['ferc1'],
                 verbose=True, debug=False, testing=False):
    """Wrapper function that ingests all the FERC Form 1 tables."""
    # BEGIN INGESTING FERC FORM 1 DATA:
    # Note that ferc1.init_db() must already have been run... somewhere.
    ferc1_ingest_functions = {
        'f1_fuel': ingest_fuel_ferc1,
        'f1_steam': ingest_plants_steam_ferc1,
        'f1_gnrt_plant': ingest_plants_small_ferc1,
        'f1_hydro': ingest_plants_hydro_ferc1,
        'f1_pumped_storage': ingest_plants_pumped_storage_ferc1,
        'f1_plant_in_srvce': ingest_plant_in_service_ferc1,
        'f1_purchased_pwr': ingest_purchased_power_ferc1,
        'f1_accumdepr_prvsn': ingest_accumulated_depreciation_ferc1
    }

    ferc1_engine = pudl.extract.ferc1.connect_db(testing=testing)
    for table in ferc1_ingest_functions.keys():
        if table in ferc1_tables:
            if verbose:
                print("Ingesting {} from FERC Form 1 into PUDL.".format(table))
            ferc1_ingest_functions[table](pudl_engine,
                                          ferc1_engine,
                                          ferc1_years)


def init_db(ferc1_tables=pc.ferc1_pudl_tables,
            ferc1_years=pc.working_years['ferc1'],
            eia923_tables=pc.eia923_pudl_tables,
            eia923_years=pc.working_years['eia923'],
            eia860_tables=pc.eia860_pudl_tables,
            eia860_years=pc.working_years['eia860'],
            verbose=True, debug=False,
            pudl_testing=False,
            ferc1_testing=False,
            csvdir=os.path.join(settings.PUDL_DIR, 'results', 'csvdump'),
            keep_csv=True):
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
        eia923_years (list): The list of years from which to pull EIA 923
            data.
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
            assert(table in pc.ferc1_working_tables)
            assert(table in pc.ferc1_pudl_tables)

    if not debug:
        for table in eia923_tables:
            assert(table in pc.eia923_pudl_tables)

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

    ingest_eia860(pudl_engine,
                  eia860_tables=eia860_tables,
                  eia860_years=eia860_years,
                  verbose=verbose, debug=debug, testing=pudl_testing,
                  csvdir=csvdir, keep_csv=keep_csv)

    ingest_eia923(pudl_engine,
                  eia923_tables=eia923_tables,
                  eia923_years=eia923_years,
                  verbose=verbose, debug=debug, testing=pudl_testing,
                  csvdir=csvdir, keep_csv=keep_csv)

    ingest_ferc1(pudl_engine,
                 ferc1_tables=ferc1_tables,
                 ferc1_years=ferc1_years,
                 verbose=verbose, debug=debug, testing=ferc1_testing)
