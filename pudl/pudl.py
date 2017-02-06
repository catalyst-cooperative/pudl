from sqlalchemy import Column, ForeignKey, Integer, String, Float
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
#from sqlalchemy.orm.collections import attribute_mapped_collection

#import eia923
import ferc1

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

Base = declarative_base()

###########################################################################
# Tables which represent static lists. E.g. all the US States.
###########################################################################

class State(Base):
    """
    A static list of US states.
    """
    __tablename__ = 'us_states'
    abbr = Column(String, primary_key=True)
    name = Column(String)

class Fuel(Base):
    """
    A static list of strings denoting possible fuel types.
    """
    __tablename__ = 'fuels'
    name = Column(String, primary_key=True)

class Year(Base):
    """A list of valid data years."""
    __tablename__ = 'years'
    year = Column(Integer, primary_key=True)

class Month(Base):
    """A list of valid data months."""
    __tablename__ = 'months'
    month = Column(Integer, primary_key=True)

class Quarter(Base):
    """A list of fiscal/calendar quarters."""
    __tablename__ = 'quarters'
    q = Column(Integer, primary_key=True) # 1, 2, 3, 4
    end_month = Column(Integer, nullable=False) # 3, 6, 9, 12

class RTOISO(Base):
    """A list of valid Regional Transmission Organizations and Independent
       System Operators."""
    __tablename__ = 'rto_iso'
    abbr = Column(String, primary_key=True)
    name = Column(String, nullable=False)

class FuelUnit(Base):
    """A list of strings denoting possible fuel units of measure."""
    __tablename__ = 'fuel_units'
    unit = Column(String, primary_key=True)

class PrimeMover(Base):
    """A list of strings denoting different types of prime movers."""
    __tablename__ = 'prime_movers'
    prime_mover = Column(String, primary_key="True")

###########################################################################
# "Glue" tables relating names & IDs from different data sources
###########################################################################

class UtilityFERC1(Base):
    """
    A FERC respondent -- typically this is a utility company.
    """
    __tablename__ = 'utilities_ferc1'
    respondent_id = Column(Integer, primary_key=True)
    respondent_name = Column(String, nullable=False)
    util_id_pudl = Column(Integer, ForeignKey('utilities.id'), nullable=False)

class PlantFERC1(Base):
    """
    A co-located collection of generation infrastructure. Sometimes broken out
    by type of plant, depending on the utility and history of the facility.
    FERC does not assign plant IDs -- the only identifying information we have
    is the name, and the respondent it is associated with.  The same plant may
    also be listed by multiple utilities (FERC respondents).
    """
    __tablename__ = 'plants_ferc1'
    respondent_id = Column(Integer,
                           ForeignKey('utilities_ferc1.respondent_id'),
                           primary_key=True)
    plant_name = Column(String, primary_key=True, nullable=False)
    plant_id_pudl = Column(Integer, ForeignKey('plants.id'), nullable=False)

class UtilityEIA923(Base):
    """
    An EIA operator, typically a utility company. EIA does assign unique IDs
    to each operator, as well as supplying a name.
    """
    __tablename__ = 'utilities_eia923'
    operator_id = Column(Integer, primary_key=True)
    operator_name = Column(String, nullable=False)
    util_id_pudl = Column(Integer,
                   ForeignKey('utilities.id'),
                   nullable=False)

class PlantEIA923(Base):
    """
    A plant listed in the EIA 923 form. A single plant typically has only a
    single operator.  However, plants may have multiple owners, and so the
    same plant may show up under multiple FERC respondents (utilities).
    """
    __tablename__ = 'plants_eia923'
    plant_id = Column(Integer, primary_key=True)
    plant_name = Column(String, nullable=False)
    plant_id_pudl = Column(Integer, ForeignKey('plants.id'), nullable=False)

class Utility(Base):
    """
    A general electric utility, constructed from FERC, EIA and other data. For
    now this object class is just glue, that allows us to correlate  the FERC
    respondents and EIA operators. In the future it could contain other useful
    information associated with the Utility.  Unfortunately there's not a one
    to one correspondence between FERC respondents and EIA operators, so
    there's some inherent ambiguity in this correspondence.
    """

    __tablename__ = 'utilities'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)

    utilities_eia923 = relationship("UtilityEIA923")
    utilities_ferc1 = relationship("UtilityFERC1")

class Plant(Base):
    """
    A co-located collection of electricity generating infrastructure.

    Plants are enumerated based on their appearing in at least one public data
    source, like the FERC Form 1, or EIA Form 923 reporting.  However, they
    may not appear in all data sources.  Additionally, plants may in some
    cases be broken down into smaller units in one data source than another.
    """
    __tablename__ = 'plants'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    #us_state = Column(String, ForeignKey('us_states.abbr'))
    #primary_fuel = Column(String, ForeignKey('fuels.name')) # or ENUM?
    #total_capacity = Column(Float)

    plants_ferc1 = relationship("PlantFERC1")
    plants_eia923 = relationship("PlantEIA923")

class UtilityPlant(Base):
    """
    Enumerates the existence of relationships between plants and utilities.

    In the future this table might also be used to describe the nature of the
    relationship between the utility and the plant -- for example listing the
    ownership share that the utility holds of the plant, and whether or not a
    given utility is the operator of the plant or not.
    """
    __tablename__ = 'plant_ownership'
    utility_id = Column(Integer, ForeignKey('utilities.id'), primary_key=True)
    plant_id = Column(Integer, ForeignKey('plants.id'), primary_key=True)
    #ownership_share = Column(Float, nullable=False)
    #operator = Column(Boolean)

###########################################################################
# Tables comprising data from the FERC f1_steam & f1_fuel tables
###########################################################################

class FuelFERC1(Base):
    """
    Annual fuel consumed by a given plant, as reported to FERC in Form 1. This
    information comes from the f1_fuel table in the FERC DB, which is
    populated from page 402 of the paper FERC For 1.
    """
    __tablename__ = 'fuel_ferc1'
    # Each year, for each fuel, there's one report for each plant, which may
    # be recorded multiple times for multiple utilities that have a stake in
    # the plant... Primary key fields: utility, plant, fuel and year.
    plant_id = Column(Integer, ForeignKey('plants.id'), primary_key=True)
    utility_id = Column(Integer, ForeignKey('utilities.id'), primary_key=True)
    fuel_type = Column(String, ForeignKey('fuels.name'), primary_key=True)
    year = Column(Integer, ForeignKey('years.year'), primary_key=True)
    fuel_unit = Column(String, ForeignKey('fuel_units.unit'), nullable=False)
    fuel_qty_burned = Column(Float, nullable=False)
    fuel_avg_heat = Column(Float, nullable=False)
    fuel_cost_burned = Column(Float, nullable=False)
    fuel_cost_delivered = Column(Float, nullable=False)
    fuel_cost_btu = Column(Float, nullable=False)
    fuel_cost_kwh = Column(Float, nullable=False)
    fuel_heat_kwh = Column(Float, nullable=False)

class ThermalPlantFERC1(Base):
    """
    A large thermal generating plant, as reported to FERC on Form 1.
    """
    __tablename__ = 'thermal_plant_ferc1'
    plant_id = Column(Integer, ForeignKey('plants.id'), primary_key=True)
    utility_id = Column(Integer, ForeignKey('utilities.id'), primary_key=True)
    year = Column(Integer, ForeignKey('years.year'), primary_key=True)

    #respondent_id
    #report_year
    #report_prd
    #spplmnt_num
    #row_number
    #row_seq
    #row_prvlg
    #plant_name
    plant_kind = Column(String)
    type_const = Column(String)
    year_constructed = Column(Integer)
    year_installed = Column(Integer)
    total_capacity = Column(Float)
    peak_demand = Column(Float)
    plant_hours = Column(Float)
    plant_capability = Column(Float)
    when_not_limited = Column(Float)
    when_limited = Column(Float)
    avg_num_employees = Column(Float)
    net_generation = Column(Float)
    cost_land = Column(Float)
    cost_structure = Column(Float)
    cost_equipment = Column(Float)
    cost_of_plant_total= Column(Float)
    cost_per_kw = Column(Float)
    expns_operations = Column(Float)
    expns_fuel = Column(Float)
    expns_coolants = Column(Float)
    expns_steam = Column(Float)
    expns_steam_other = Column(Float)
    expns_transfer = Column(Float)
    expns_electric = Column(Float)
    expns_misc_power = Column(Float)
    expns_rents = Column(Float)
    expns_allowances = Column(Float)
    expns_engineering = Column(Float)
    expns_structures = Column(Float)
    expns_boiler = Column(Float)
    expns_plants = Column(Float)
    expns_misc_steam = Column(Float)
    expns_production_total = Column(Float)
    expns_kwh = Column(Float)
    asset_retire_cost = Column(Float)

def init_db(Base):
    """
    Create the PUDL database and fill it up with data!

    Uses the metadata associated with Base, which is defined by all the
    objects & tables defined above.

    """
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    import pandas as pd

    engine = create_engine('postgresql://catalyst@localhost:5432/pudl_sandbox')

    # Wipe it out and start anew for testing purposes...
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    ######################################################################
    # Lists of static data to pull into the DB:
    ######################################################################

    fuel_names = ['coal', 'gas', 'oil']
    fuel_units = ['tons', 'mcf', 'bbls']

    prime_movers = ['steam_turbine',
                    'gas_turbine',
                    'hydro',
                    'internal_combustion',
                    'solar_pv',
                    'wind_turbine']

    rto_iso = { 'CAISO' :'California ISO',
                'ERCOT' :'Electric Reliability Council of Texas',
                'MISO'  :'Midcontinent ISO',
                'ISO-NE':'ISO New England',
                'NYISO' :'New York ISO',
                'PJM'   :'PJM Interconnection',
                'SPP'   :'Southwest Power Pool'
              }

    us_states = { 'AK':'Alaska',
                  'AL':'Alabama',
                  'AR':'Arkansas',
                  'AS':'American Samoa',
                  'AZ':'Arizona',
                  'CA':'California',
                  'CO':'Colorado',
                  'CT':'Connecticut',
                  'DC':'District of Columbia',
                  'DE':'Delaware',
                  'FL':'Florida',
                  'GA':'Georgia',
                  'GU':'Guam',
                  'HI':'Hawaii',
                  'IA':'Iowa',
                  'ID':'Idaho',
                  'IL':'Illinois',
                  'IN':'Indiana',
                  'KS':'Kansas',
                  'KY':'Kentucky',
                  'LA':'Louisiana',
                  'MA':'Massachusetts',
                  'MD':'Maryland',
                  'ME':'Maine',
                  'MI':'Michigan',
                  'MN':'Minnesota',
                  'MO':'Missouri',
                  'MP':'Northern Mariana Islands',
                  'MS':'Mississippi',
                  'MT':'Montana',
                  'NA':'National',
                  'NC':'North Carolina',
                  'ND':'North Dakota',
                  'NE':'Nebraska',
                  'NH':'New Hampshire',
                  'NJ':'New Jersey',
                  'NM':'New Mexico',
                  'NV':'Nevada',
                  'NY':'New York',
                  'OH':'Ohio',
                  'OK':'Oklahoma',
                  'OR':'Oregon',
                  'PA':'Pennsylvania',
                  'PR':'Puerto Rico',
                  'RI':'Rhode Island',
                  'SC':'South Carolina',
                  'SD':'South Dakota',
                  'TN':'Tennessee',
                  'TX':'Texas',
                  'UT':'Utah',
                  'VA':'Virginia',
                  'VI':'Virgin Islands',
                  'VT':'Vermont',
                  'WA':'Washington',
                  'WI':'Wisconsin',
                  'WV':'West Virginia',
                  'WY':'Wyoming'
                }

    # Populate tables with static data from above.
    session.add_all([Fuel(name=f) for f in fuel_names])
    session.add_all([FuelUnit(unit=u) for u in fuel_units])
    session.add_all([Month(month=i+1) for i in range(12)])
    session.add_all([Quarter(q=i+1, end_month=3*(i+1)) for i in range(4)])
    session.add_all([PrimeMover(prime_mover=pm) for pm in prime_movers])
    session.add_all([RTOISO(abbr=k, name=v) for k,v in rto_iso.items()])

    # States dictionary is defined outside this function, below.
    session.add_all([State(abbr=k, name=v) for k,v in us_states.items()])

    session.commit()

    # Populate the tables which define the mapping of EIA and FERC IDs to our
    # internal PUDL IDs, for both plants and utilities, so that we don't need
    # to use those poorly defined relationships any more.  These mappings were
    # largely determined by hand in an Excel spreadsheet, and so may be a
    # little bit imperfect. We're pulling that information in from the
    # "results" directory...

    map_eia923_ferc1_file = '../results/id_mapping/mapping_eia923_ferc1_test.xlsx'

    plant_map = pd.read_excel(map_eia923_ferc1_file,'plants_output',
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

    # Any FERC respondent_id that appears in plants_ferc1 must also exist in
    # utils_ferc1:

    for p in plants.itertuples():
        session.add(Plant(id = int(p.plant_id), name = p.plant_name))

    for u in utils.itertuples():
        session.add(Utility(id = int(u.utility_id), name = u.utility_name))

    for u in utils_eia923.itertuples():
        session.add(UtilityEIA923(operator_id   = int(u.operator_id_eia923),
                                  operator_name = u.operator_name_eia923,
                                  util_id_pudl  = int(u.utility_id)))

    for u in utils_ferc1.itertuples():
        session.add(UtilityFERC1(respondent_id   = int(u.respondent_id_ferc1),
                                 respondent_name = u.respondent_name_ferc1,
                                 util_id_pudl    = int(u.utility_id)))

    for p in plants_eia923.itertuples():
        session.add(PlantEIA923(plant_id      = int(p.plant_id_eia923),
                                plant_name    = p.plant_name_eia923,
                                plant_id_pudl = int(p.plant_id)))

    for p in plants_ferc1.itertuples():
        session.add(PlantFERC1(respondent_id = int(p.respondent_id_ferc1),
                               plant_name    = p.plant_name_ferc1,
                               plant_id_pudl = int(p.plant_id)))

    session.commit()

    # Now we pour in some actual data!
    # FuelFERC1 pulls information from the f1_fuel table of the FERC DB.
    # Need:
    #  - handle to the ferc_f1 db
    #  - list of all the (utility_id, plant_id, fuel_type, year) combos we want
    #    to ingest.
    #  - Create a select statement that gets us the fields we need to populate.
    #  - Iterate across those results, adding them to the session.

    session.close_all()

#class Boiler(Base):
#    __tablename__ = 'boiler'
#
#class Generator(Base):
#    __tablename__ = 'generator'
#
#class FuelDeliveryFERC1(Base):
#    __tablename__ = 'ferc_f1_fuel_delivery'
#
#class FuelDeliveryEIA923(Base):
#    __tablename__ = 'eia_f923_fuel_delivery'
#
#class FuelDelivery(Base):
#    __tablename__ = 'fuel_delivery'
#
#class PowerPlantUnit(Base):
#    __tablename__ = 'power_plant_unit'
