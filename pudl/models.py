from sqlalchemy import Column, ForeignKey, Integer, String, Float
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base
#from sqlalchemy.orm.collections import attribute_mapped_collection

from pudl import settings, constants, pudl

PUDLBase = declarative_base()

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

class FERCAccount(PUDLBase):
    """
    Static list of all the FERC account numbers and descriptions.
    """
    __tablename__ = 'ferc_accounts'
    id = Column(String, primary_key=True)
    description = Column(String, nullable=False)

class CensusRegion(PUDLBase):
    """
    Static list of census regions used by EIA
    """
    __tablename__ = 'census_regions'
    abbr = Column(String, primary_key=True)
    name = Column(String, nullable=False)

class FuelTypeEIA923(PUDLBase):
    """
    Static list of fuel types used by EIA in Form 923,
    Enumerated on EIAForm923 Page 7
    """
    __tablename__ = 'fuel_type_eia923'
    abbr = Column(String, primary_key=True)
    fuel_type = Column(String, nullable=False)

class PrimeMoverEIA923(PUDLBase):
    """
    Static list of prime movers used by EIA in Form 923
    """
    __tablename__ = 'prime_mover_eia923'
    abbr = Column(String, primary_key = True)
    prime_mover = Column(String, nullable = True)

class FuelUnitEIA923(PUDLBase):
    """
    Static list of fuel units (physical unit labels) used by EIA in Form 923
    """
    __tablename__ = 'fuel_unit_eia923'
    abbr = Column(String, primary_key = True)
    unit = Column(String, nullable = True)

class FuelTypeAER(PUDLBase):
    """
    Static list of fuel types using AER codes, reported in EIA Form 923
    """
    __tablename__ = 'fuel_type_aer'
    abbr = Column(String, primary_key = True)
    fuel_type = Column(String, nullable = True)



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
