from sqlalchemy import Column, ForeignKey, Integer, String, Float
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine.url import URL
#from sqlalchemy.orm.collections import attribute_mapped_collection

import settings

PUDLBase = declarative_base()

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

###########################################################################
# Tables which represent static lists. E.g. all the US States.
###########################################################################

class State(PUDLBase):
    """
    A static list of US states.
    """
    __tablename__ = 'us_states'
    abbr = Column(String, primary_key=True)
    name = Column(String)

class Fuel(PUDLBase):
    """
    A static list of strings denoting possible fuel types.
    """
    __tablename__ = 'fuels'
    name = Column(String, primary_key=True)

class Year(PUDLBase):
    """A list of valid data years."""
    __tablename__ = 'years'
    year = Column(Integer, primary_key=True)

class Month(PUDLBase):
    """A list of valid data months."""
    __tablename__ = 'months'
    month = Column(Integer, primary_key=True)

class Quarter(PUDLBase):
    """A list of fiscal/calendar quarters."""
    __tablename__ = 'quarters'
    q = Column(Integer, primary_key=True) # 1, 2, 3, 4
    end_month = Column(Integer, nullable=False) # 3, 6, 9, 12

class RTOISO(PUDLBase):
    """A list of valid Regional Transmission Organizations and Independent
       System Operators."""
    __tablename__ = 'rto_iso'
    abbr = Column(String, primary_key=True)
    name = Column(String, nullable=False)

class FuelUnit(PUDLBase):
    """A list of strings denoting possible fuel units of measure."""
    __tablename__ = 'fuel_units'
    unit = Column(String, primary_key=True)

class PrimeMover(PUDLBase):
    """A list of strings denoting different types of prime movers."""
    __tablename__ = 'prime_movers'
    prime_mover = Column(String, primary_key="True")

###########################################################################
# "Glue" tables relating names & IDs from different data sources
###########################################################################

class UtilityFERC1(PUDLBase):
    """
    A FERC respondent -- typically this is a utility company.
    """
    __tablename__ = 'utilities_ferc1'
    respondent_id = Column(Integer, primary_key=True)
    respondent_name = Column(String, nullable=False)
    util_id_pudl = Column(Integer, ForeignKey('utilities.id'), nullable=False)

class PlantFERC1(PUDLBase):
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

class UtilityEIA923(PUDLBase):
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

class PlantEIA923(PUDLBase):
    """
    A plant listed in the EIA 923 form. A single plant typically has only a
    single operator.  However, plants may have multiple owners, and so the
    same plant may show up under multiple FERC respondents (utilities).
    """
    __tablename__ = 'plants_eia923'
    plant_id = Column(Integer, primary_key=True)
    plant_name = Column(String, nullable=False)
    plant_id_pudl = Column(Integer, ForeignKey('plants.id'), nullable=False)

class Utility(PUDLBase):
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

class Plant(PUDLBase):
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

class UtilPlantAssn(PUDLBase):
    "Enumerates existence of relationships between plants and utilities."

    __tablename__ = 'util_plant_assn'
    utility_id = Column(Integer, ForeignKey('utilities.id'), primary_key=True)
    plant_id = Column(Integer, ForeignKey('plants.id'), primary_key=True)

###########################################################################
# Tables comprising data from the FERC f1_steam & f1_fuel tables
###########################################################################

class FuelFERC1(PUDLBase):
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

class PlantSteamFERC1(PUDLBase):
    """
    A large thermal generating plant, as reported to FERC on Form 1.
    """
    __tablename__ = 'plants_steam_ferc1'
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

###########################################################################
# Classes we have not yet created...
###########################################################################
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
