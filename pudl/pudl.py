from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine
import pandas as pd
import os.path

from pudl import settings
from pudl.constants import fuel_names, fuel_units, us_states, prime_movers
from pudl.constants import rto_iso

# The Declarative Base used to define our database:
from pudl.models import PUDLBase

# A couple of helpers:
from pudl.models import db_connect_pudl, create_tables_pudl, drop_tables_pudl

# Tables that hold constant values:
from pudl.models import Fuel, FuelUnit, Month, Quarter, PrimeMover, Year, RTOISO
from pudl.models import State

# Tables that hold "glue" connecting FERC1 & EIA923 to each other:
from pudl.models import Utility, UtilityFERC1, UtilityEIA923
from pudl.models import Plant, PlantFERC1, PlantEIA923
from pudl.models import UtilPlantAssn

# Tables that hold FERC1 data:
from pudl.models import FuelFERC1, PlantSteamFERC1

#import pudl.eia923
#import pudl.ferc1

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
def init_db(PUDLBase):
    """
    Create the PUDL database and fill it up with data!

    Uses the metadata associated with Base, which is defined by all the
    objects & tables defined above.

    """
    from sqlalchemy.orm import sessionmaker
    import pandas as pd

    pudl_engine = db_connect_pudl()

    # Wipe it out and start anew for testing purposes...
    drop_tables_pudl(pudl_engine)
    create_tables_pudl(pudl_engine)

    PUDL_Session = sessionmaker(bind=pudl_engine)
    pudl_session = PUDL_Session()

    # Populate tables with static data from above.
    pudl_session.add_all([Fuel(name=f) for f in fuel_names])
    pudl_session.add_all([FuelUnit(unit=u) for u in fuel_units])
    pudl_session.add_all([Month(month=i+1) for i in range(12)])
    pudl_session.add_all([Quarter(q=i+1, end_month=3*(i+1)) for i in range(4)])
    pudl_session.add_all([PrimeMover(prime_mover=pm) for pm in prime_movers])
    pudl_session.add_all([RTOISO(abbr=k, name=v) for k,v in rto_iso.items()])

    # States dictionary is defined outside this function, below.
    pudl_session.add_all([State(abbr=k, name=v) for k,v in us_states.items()])

    pudl_session.commit()

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

    util_map = pd.read_excel(map_eia923_ferc1_file,'utilities_output',
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

    utils = util_map[['utility_id','utility_name']]
    utils = utils.drop_duplicates('utility_id')

    utils_eia923 = util_map[['operator_id_eia923','operator_name_eia923','utility_id']]
    utils_eia923 = utils_eia923.drop_duplicates('operator_id_eia923')

    utils_ferc1 = util_map[['respondent_id_ferc1','respondent_name_ferc1','utility_id']]
    utils_ferc1 = utils_ferc1.drop_duplicates('respondent_id_ferc1')

    # Now we need to create a table that indicates which plants are associated
    # with every utility.

    # These dataframes map our plant_id to FERC respondents and EIA
    # operators -- the equivalents of our "utilities"
    plants_respondents = plant_map[['plant_id','respondent_id_ferc1']]
    plants_operators = plant_map[['plant_id','operator_id_eia923']]

    # Here we treat the dataframes like database tables, and join on the
    # FERC respondent_id and EIA operator_id, respectively.
    util_plant_ferc1 = utils_ferc1.join(plants_respondents.set_index('respondent_id_ferc1'), on='respondent_id_ferc1')
    util_plant_eia923 = utils_eia923.join(plants_operators.set_index('operator_id_eia923'), on='operator_id_eia923')

    # Now we can concatenate the two dataframes, and get rid of all the  columns
    # except for plant_id and utility_id (which determine the  utility to plant
    # association), and get rid of any duplicates or lingering NaN values...
    util_plant_assn = pd.concat([util_plant_eia923,util_plant_ferc1])
    util_plant_assn = util_plant_assn[['plant_id','utility_id']].dropna().drop_duplicates()

    # At this point there should be at most one row in each of these data
    # frames with NaN values after we drop_duplicates in each. This is because
    # there will be some plants and utilities that only exist in FERC, or only
    # exist in EIA, and while they will have PUDL IDs, they may not have
    # FERC/EIA info (and it'll get pulled in as NaN)

    for df in [plants_eia923, plants_ferc1, utils_eia923, utils_ferc1]:
        assert(df[pd.isnull(df).any(axis=1)].shape[0]<=1)
        df.dropna(inplace=True)

    # Before we start inserting records into the database, let's do some basic
    # sanity checks to ensure that it's (at least kind of) clean.
    # INSERT SANITY HERE

    # Any FERC respondent_id that appears in plants_ferc1 must also exist in
    # utils_ferc1:
    # INSERT MORE SANITY HERE

    for p in plants.itertuples():
        pudl_session.add(Plant(id = int(p.plant_id), name = p.plant_name))
    pudl_session.commit()

    for u in utils.itertuples():
        pudl_session.add(Utility(id = int(u.utility_id), name = u.utility_name))
    pudl_session.commit()

    for u in utils_eia923.itertuples():
        pudl_session.add(UtilityEIA923(operator_id   = int(u.operator_id_eia923),
                                       operator_name = u.operator_name_eia923,
                                       util_id_pudl  = int(u.utility_id)))
    pudl_session.commit()

    for u in utils_ferc1.itertuples():
        pudl_session.add(UtilityFERC1(respondent_id   = int(u.respondent_id_ferc1),
                                      respondent_name = u.respondent_name_ferc1,
                                      util_id_pudl    = int(u.utility_id)))
    pudl_session.commit()

    for p in plants_eia923.itertuples():
        pudl_session.add(PlantEIA923(plant_id      = int(p.plant_id_eia923),
                                     plant_name    = p.plant_name_eia923,
                                     plant_id_pudl = int(p.plant_id)))
    pudl_session.commit()

    for p in plants_ferc1.itertuples():
        pudl_session.add(PlantFERC1(respondent_id = int(p.respondent_id_ferc1),
                                    plant_name    = p.plant_name_ferc1,
                                    plant_id_pudl = int(p.plant_id)))
    pudl_session.commit()

    for assn in util_plant_assn.itertuples():
        pudl_session.add(UtilPlantAssn(plant_id   = int(assn.plant_id),
                                       utility_id = int(assn.utility_id)))
    pudl_session.commit()

    # Now we pour in some actual data!
    # FuelFERC1 pulls information from the f1_fuel table of the FERC DB.
    # Need:
    #  - handle to the ferc_f1 db
    #  - list of all the (utility_id, plant_id, fuel_type, year) combos we want
    #    to ingest.
    #  - Create a select statement that gets us the fields we need to populate.
    #  - Iterate across those results, adding them to the session.

    ferc1_engine = create_engine(URL(**settings.DB_FERC1))
    ferc1_fuel_df = pd.read_sql('SELECT respondent_id,\
                                        report_year,\
                                        plant_name,\
                                        fuel,\
                                        fuel_unit,\
                                        fuel_quantity,\
                                        fuel_avg_heat,\
                                        fuel_cost_delvd,\
                                        fuel_cost_burned,\
                                        fuel_cost_btu,\
                                        fuel_cost_kwh,\
                                        fuel_generaton,\
                                        report_prd\
                                 FROM f1_fuel', ferc1_engine)

    pudl_session.close_all()
