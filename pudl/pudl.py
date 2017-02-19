import pandas as pd
import numpy as np
import os.path

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine
from sqlalchemy import Integer, String

from pudl import settings
from pudl.ferc1 import db_connect_ferc1, cleanstrings, ferc1_meta
from pudl.constants import ferc1_fuel_strings, us_states, prime_movers
from pudl.constants import ferc1_fuel_unit_strings, rto_iso

# Tables that hold constant values:
from pudl.models import Fuel, FuelUnit, Month, Quarter, PrimeMover, Year, RTOISO
from pudl.models import State

# Tables that hold "glue" connecting FERC1 & EIA923 to each other:
from pudl.models import Utility, UtilityFERC1, UtilityEIA923
from pudl.models import Plant, PlantFERC1, PlantEIA923
from pudl.models import UtilPlantAssn
from pudl.models import PUDLBase

from pudl.models_ferc1 import FuelFERC1, PlantSteamFERC1

"""
The Public Utility Data Liberation (PUDL) project integrates several different
public data sets into one well normalized database allowing easier access and
interaction between all of them.

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

def db_connect_pudl():
    """
    Performs database connection using database settings from settings.py.
    Returns sqlalchemy engine instance
    """
    return create_engine(URL(**settings.DB_PUDL))

def create_tables_pudl(engine):
    """"""
    PUDLBase.metadata.create_all(engine)

def drop_tables_pudl(engine):
    """"""
    PUDLBase.metadata.drop_all(engine)

def init_db():
    """
    Create the PUDL database and fill it up with data!

    Uses the metadata associated with Base, which is defined by all the
    objects & tables defined above.

    """
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.sql import select
    import pandas as pd

    pudl_engine = db_connect_pudl()

    # Wipe it out and start anew for testing purposes...
    drop_tables_pudl(pudl_engine)
    create_tables_pudl(pudl_engine)

    PUDL_Session = sessionmaker(bind=pudl_engine)
    pudl_session = PUDL_Session()

    # Populate tables with static data from above.
    pudl_session.add_all([Fuel(name=f) for f in ferc1_fuel_strings.keys()])
    pudl_session.add_all([FuelUnit(unit=u) for u in ferc1_fuel_unit_strings.keys()])
    pudl_session.add_all([Month(month=i+1) for i in range(12)])
    pudl_session.add_all([Quarter(q=i+1, end_month=3*(i+1)) for i in range(4)])
    pudl_session.add_all([PrimeMover(prime_mover=pm) for pm in prime_movers])
    pudl_session.add_all([RTOISO(abbr=k, name=v) for k,v in rto_iso.items()])
    pudl_session.add_all([Year(year=yr) for yr in range(1994,2017)])

    # States dictionary is defined outside this function, below.
    pudl_session.add_all([State(abbr=k, name=v) for k,v in us_states.items()])

    pudl_session.commit()
    pudl_session.close_all()

    # Populate the tables which define the mapping of EIA and FERC IDs to our
    # internal PUDL IDs, for both plants and utilities, so that we don't need
    # to use those poorly defined relationships any more.  These mappings were
    # largely determined by hand in an Excel spreadsheet, and so may be a
    # little bit imperfect. We're pulling that information in from the
    # "results" directory...


    map_eia923_ferc1_file = os.path.join(settings.PUDL_DIR,
                                         'results',
                                         'id_mapping',
                                         'mapping_eia923_ferc1.xlsx')

    plant_map = pd.read_excel(map_eia923_ferc1_file,'plants_output',
                              na_values='', keep_default_na=False,
                              converters={'plant_id':int,
                                          'plant_name':str,
                                          'respondent_id_ferc1':int,
                                          'respondent_name_ferc1':str,
                                          'plant_name_ferc1':str,
                                          'plant_id_eia923':int,
                                          'plant_name_eia923':str,
                                          'operator_name_eia923':str,
                                          'operator_id_eia923':int})

    utility_map = pd.read_excel(map_eia923_ferc1_file,'utilities_output',
                             na_values='', keep_default_na=False,
                             converters={'utility_id':int,
                                         'utility_name':str,
                                         'respondent_id_ferc1':int,
                                         'respondent_name_ferc1':str,
                                         'operator_id_eia923':int,
                                         'operator_name_eia923':str})

    plants = plant_map[['plant_id','plant_name']]
    plants = plants.drop_duplicates('plant_id')

    plants_eia923 = plant_map[['plant_id_eia923','plant_name_eia923','plant_id']]
    plants_eia923 = plants_eia923.drop_duplicates('plant_id_eia923')

    plants_ferc1 = plant_map[['plant_name_ferc1','respondent_id_ferc1','plant_id']]
    plants_ferc1 = plants_ferc1.drop_duplicates(['plant_name_ferc1','respondent_id_ferc1'])

    utilities = utility_map[['utility_id','utility_name']]
    utilities = utilities.drop_duplicates('utility_id')

    utilities_eia923 = utility_map[['operator_id_eia923','operator_name_eia923','utility_id']]
    utilities_eia923 = utilities_eia923.drop_duplicates('operator_id_eia923')

    utilities_ferc1 = utility_map[['respondent_id_ferc1','respondent_name_ferc1','utility_id']]
    utilities_ferc1 = utilities_ferc1.drop_duplicates('respondent_id_ferc1')

    # Now we need to create a table that indicates which plants are associated
    # with every utility.

    # These dataframes map our plant_id to FERC respondents and EIA
    # operators -- the equivalents of our "utilities"
    plants_respondents = plant_map[['plant_id','respondent_id_ferc1']]
    plants_operators = plant_map[['plant_id','operator_id_eia923']]

    # Here we treat the dataframes like database tables, and join on the
    # FERC respondent_id and EIA operator_id, respectively.
    utility_plant_ferc1 = utilities_ferc1.\
                            join(plants_respondents.\
                            set_index('respondent_id_ferc1'),
                                      on='respondent_id_ferc1')
    utility_plant_eia923 = utilities_eia923.join(plants_operators.set_index('operator_id_eia923'), on='operator_id_eia923')

    # Now we can concatenate the two dataframes, and get rid of all the  columns
    # except for plant_id and utility_id (which determine the  utility to plant
    # association), and get rid of any duplicates or lingering NaN values...
    utility_plant_assn = pd.concat([utility_plant_eia923,utility_plant_ferc1])
    utility_plant_assn = utility_plant_assn[['plant_id','utility_id']].\
                            dropna().\
                            drop_duplicates()

    # At this point there should be at most one row in each of these data
    # frames with NaN values after we drop_duplicates in each. This is because
    # there will be some plants and utilities that only exist in FERC, or only
    # exist in EIA, and while they will have PUDL IDs, they may not have
    # FERC/EIA info (and it'll get pulled in as NaN)

    for df in [plants_eia923, plants_ferc1, utilities_eia923, utilities_ferc1]:
        assert(df[pd.isnull(df).any(axis=1)].shape[0]<=1)
        df.dropna(inplace=True)

    # Before we start inserting records into the database, let's do some basic
    # sanity checks to ensure that it's (at least kind of) clean.
    # INSERT SANITY HERE

    # Any FERC respondent_id that appears in plants_ferc1 must also exist in
    # utils_ferc1:
    # INSERT MORE SANITY HERE

    plants.rename(columns={'plant_id':'id','plant_name':'name'},
                  inplace=True)
    plants.to_sql(name='plants',
                  con=pudl_engine, index=False, if_exists='append',
                  dtype={'id':Integer, 'name':String} )

    utilities.rename(columns={'utility_id':'id', 'utility_name':'name'},
                     inplace=True)
    utilities.to_sql(name='utilities',
                     con=pudl_engine, index=False, if_exists='append',
                     dtype={'id':Integer, 'name':String} )

    utilities_eia923.rename(columns={'operator_id_eia923':'operator_id',
                                     'operator_name_eia923':'operator_name',
                                     'utility_id':'util_id_pudl'},
                            inplace=True)
    utilities_eia923.to_sql(name='utilities_eia923',
                            con=pudl_engine, index=False, if_exists='append',
                            dtype={'operator_id':Integer,
                                   'operator_name':String,
                                   'util_id_pudl':Integer} )
    utilities_ferc1.rename(columns={'respondent_id_ferc1':'respondent_id',
                                    'respondent_name_ferc1':'respondent_name',
                                    'utility_id':'util_id_pudl'},
                           inplace=True)
    utilities_ferc1.to_sql(name='utilities_ferc1',
                           con=pudl_engine, index=False, if_exists='append',
                           dtype={'respondent_id':Integer,
                                  'respondent_name':String,
                                  'util_id_pudl':Integer} )

    plants_eia923.rename(columns={'plant_id_eia923':'plant_id',
                                  'plant_name_eia923':'plant_name',
                                  'plant_id':'plant_id_pudl'},
                         inplace=True)
    plants_eia923.to_sql(name='plants_eia923',
                         con=pudl_engine, index=False, if_exists='append',
                         dtype={'plant_id':Integer,
                                'plant_name':String,
                                'plant_id_pudl':Integer} )

    plants_ferc1.rename(columns={'respondent_id_ferc1':'respondent_id',
                                 'plant_name_ferc1':'plant_name',
                                 'plant_id':'plant_id_pudl'},
                        inplace=True)
    plants_ferc1.to_sql(name='plants_ferc1',
                        con=pudl_engine, index=False, if_exists='append',
                        dtype={'respondent_id':Integer,
                               'plant_name':String,
                               'plant_id_pudl':Integer} )

    utility_plant_assn.to_sql(name='util_plant_assn',
                              con=pudl_engine, index=False, if_exists='append',
                              dtype={'plant_id':Integer, 'utility_id':Integer})

    # Now we pour in some actual data!
    # FuelFERC1 pulls information from the f1_fuel table of the FERC DB.
    # Need:
    #  - handle to the ferc_f1 db
    #  - list of all the (utility_id, plant_id, fuel_type, year) combos we want
    #    to ingest.
    #  - Create a select statement that gets us the fields we need to populate.
    #  - Iterate across those results, adding them to the session.

    ferc1_engine = db_connect_ferc1()

    f1_fuel = ferc1_meta.tables['f1_fuel']
    f1_fuel_select = select([f1_fuel]).\
                         where(f1_fuel.c.fuel != '').\
                         where(f1_fuel.c.fuel_quantity > 0).\
                         where(f1_fuel.c.plant_name != '')

    ferc1_fuel_df = pd.read_sql(f1_fuel_select, ferc1_engine)

    # Delete the columns that we aren't going to insert:
    ferc1_fuel_df.drop(['spplmnt_num', 'row_number', 'row_seq', 'row_prvlg',
                       'report_prd'], axis=1, inplace=True)

    # Do a little cleanup of some of the fields
    ferc1_fuel_df.fuel = cleanstrings(ferc1_fuel_df.fuel,
                                      ferc1_fuel_strings,
                                      unmapped=np.nan)
    ferc1_fuel_df.fuel_unit = cleanstrings(ferc1_fuel_df.fuel_unit,
                                           ferc1_fuel_unit_strings,
                                           unmapped=np.nan)
    # drop any records w/ NA data...
    ferc1_fuel_df.dropna(inplace=True)

    ferc1_fuel_df.rename(columns={
                            'fuel_quantity':'fuel_qty_burned',
                            'fuel_avg_heat':'fuel_avg_mmbtu_per_unit',
                            'fuel_cost_burned':'fuel_cost_per_unit_burned',
                            'fuel_cost_delvd':'fuel_cost_per_unit_delivered',
                            'fuel_cost_btu':'fuel_cost_per_mmbtu',
                            'fuel_cost_kwh':'fuel_cost_per_kwh',
                            'fuel_generaton':'fuel_mmbtu_per_kwh'},
                         inplace=True)
    ferc1_fuel_df.to_sql(name='ferc1_fuel',
                         con=pudl_engine, index=False, if_exists='append',
                         dtype={'respondent_id':Integer,
                                'report_year':Integer} )

    f1_steam = ferc1_meta.tables['f1_steam']
    f1_steam_select = select([f1_steam]).\
                          where(f1_steam.c.net_generation > 0).\
                          where(f1_steam.c.plant_name != '')

    ferc1_steam_df = pd.read_sql(f1_steam_select, ferc1_engine)

    # Clean up the steam data here if we need to...
    # Many blank fields in steam table, need to be set to NA
    #ferc1_steam_df.replace(r'\s*', None, regex=True, inplace=True)
#
#    for rec in ferc1_steam_df.itertuples():
#        if not np.isnan(rec.yr_installed):
#            yr_installed = int(rec.yr_installed)
#        else:
#            yr_installed =
#        if not np.isnan(rec.yr_const): yr_const = int(rec.yr_const)
#        pudl_session.add(
#            PlantSteamFERC1(
#                respondent_id = int(rec.respondent_id),
#                plant_name = rec.plant_name,
#                report_year = int(rec.report_year),
#                plant_kind = rec.plant_kind,
#                type_const = rec.type_const,
#                year_constructed = yr_const,
#                year_installed = yr_installed,
#                total_capacity = rec.tot_capacity,
#                peak_demand = rec.peak_demand,
#                plant_hours = rec.plant_hours,
#                plant_capability = rec.plnt_capability,
#                when_not_limited = rec.when_not_limited,
#                when_limited = rec.when_limited,
#                avg_num_employees = rec.avg_num_of_emp,
#                net_generation = rec.net_generation,
#                cost_land = rec.cost_land,
#                cost_structure = rec.cost_structure,
#                cost_equipment = rec.cost_equipment,
#                cost_of_plant_total = rec.cost_of_plant_to,
#                cost_per_kw = rec.cost_per_kw,
#                expns_operations = rec.expns_operations,
#                expns_fuel = rec.expns_fuel,
#                expns_coolants = rec.expns_coolants,
#                expns_steam = rec.expns_steam,
#                expns_steam_other = rec.expns_steam_othr,
#                expns_transfer = rec.expns_transfer,
#                expns_electric = rec.expns_electric,
#                expns_misc_power = rec.expns_misc_power,
#                expns_rents = rec.expns_rents,
#                expns_allowances = rec.expns_allowances,
#                expns_engineering = rec.expns_engnr,
#                expns_structures = rec.expns_structures,
#                expns_boiler = rec.expns_boiler,
#                expns_plants = rec.expns_plants,
#                expns_misc_steam = rec.expns_misc_steam,
#                expns_production_total = rec.tot_prdctn_expns,
#                expns_kwh = rec.expns_kwh,
#                asset_retire_cost = rec.asset_retire_cost
#            )
#        )
