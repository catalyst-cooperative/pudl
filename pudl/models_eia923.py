from sqlalchemy import Column, ForeignKey, Integer, String, Float, Numeric
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base

#from sqlalchemy.orm.collections import attribute_mapped_collection

from pudl import settings, constants, models

PUDLBase = declarative_base()

###########################################################################
# EIA Form 923 tables which represent constants or metadata
# (e.g. fuel types and fuel units)
###########################################################################

class CoalMineInfoEIA923(models.PUDLBase):
#TODO figure out how to autoincrement a unique id and then insert that into a
#new field
    __tablename__ = 'coalmine_info_eia923'
    id = Column(Integer, autoincrement=True, primary_key=True) #surrogate key
    coalmine_name = Column(String) # TODO create FK as new table?
    coalmine_type = Column(String, ForeignKey('coalmine_type_eia923.abbr'))
    coalmine_state = Column(String, ForeignKey('coalmine_state_eia923.abbr'))
    # TODO check feasibility t add FK/constants or map to FIPS code used by EIA
    coalmine_county = Column(Integer)
    coalmine_msha_id = Column(Integer)
    # TODO check feasibility to add FK/constants or map to MSHA ID# used by EIA

class BoilersEIA923(models.PUDLBase):
    """
    List of Boiler IDs specific to each plant in EIA Form 923 Page 3
    """
    __tablename__ = 'boilers_eia923'
    boiler_id = Column(String, primary_key=True)

class GeneratorEIA923(PUDLBase):
    """
    List of Generator IDs specific to each plant in EIA Form 923 Page 4
    """
    __tablename__ = 'generators_eia923'
    generator_id = Column(String, primary_key=True)
    plant_id = Column(Integer, ForeignKey('plants_eia923.plant_id'),
        primary_key=True)


class FuelUnitEIA923(models.PUDLBase):
    """
    Static list of fuel units (physical unit labels) used by EIA in Form 923
    """
    __tablename__ = 'fuel_units_eia923'
    abbr = Column(String, primary_key = True)
    unit = Column(String)

class FuelTypeEIA923(models.PUDLBase):
    """
    Static list of fuel types used by EIA in Form 923,
    Enumerated on EIAForm923 Page 7
    """
    __tablename__ = 'fuel_type_eia923'
    abbr = Column(String, primary_key=True)
    fuel_type = Column(String, nullable=False)
    fuel_unit = Column(String, ForeignKey('fuel_units_eia923.abbr'))

class FuelGroupEIA923(models.PUDLBase):
    """
    EIA grouping of energy sources into fuel groups, used in EIA Form 923
    """
    __tablename__ = 'fuel_group_eia923'
    group = Column(String, primary_key = True)

class CombinedHeatPowerEIA923(PUDLBase):
    """
    Whether or not the plant is a combined heat & power facility (cogenerator)
    As reported in EIA Form 923 Page 7
    """
    __tablename__ = 'combined_heat_power_eia923'
    abbr = Column(String, primary_key=True)
    status = Column(String, nullable=False)

class RespondentFrequencyEIA923(PUDLBase):
    """
    Reporting frequency of plants, used by EIA in Form 923, Page 5:
    Fuel Receipts and Costs
    """
    __tablename__ = 'respondent_frequency_eia923'
    abbr = Column(String, primary_key=True)
    unit = Column(String, nullable=False)

class ContractTypeEIA923(models.PUDLBase):
    """
    Purchase type under which receipts occurred, reported in EIA Form 923
    """
    __tablename__ = 'contract_type_eia923'
    abbr = Column(String, primary_key = True)
    contract_type = Column(String, nullable = False)

class SectorEIA(PUDLBase):
    """
    EIA’s internal consolidated NAICS sectors
    """
    __tablename__ = 'sector_eia'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)

class FuelTypeAER(models.PUDLBase):
    """
    Static list of fuel types using AER codes, reported in EIA Form 923
    """
    __tablename__= 'fuel_type_aer_eia923'
    abbr = Column(String, primary_key=True)
    fuel_type= Column(String, nullable=False)

class PrimeMoverEIA923(models.PUDLBase):
    """
    Static list of prime movers used by EIA in Form 923
    """
    __tablename__ = 'prime_movers_eia923'
    abbr = Column(String, primary_key=True)
    prime_mover = Column(String, nullable=False)

class EnergySourceEIA923(models.PUDLBase):
    """
    Fuel code associated with fuel receipts in EIA Form 923
    """
    __tablename__ = 'energy_source_eia923'
    abbr = Column(String, primary_key = True)
    source = Column(String, nullable = False)

class CoalMineTypeEIA923(models.PUDLBase):
    """
    Type of coal mine, as used in EIA Form 923
    """
    __tablename__ = 'coalmine_type_eia923'
    abbr = Column(String, primary_key = True)
    name = Column(String, nullable = False)

class CoalMineStateEIA923(models.PUDLBase):
    """
    State and country abbreviations for coal mine locations, used in EIA Form923
    """
    __tablename__ = 'coalmine_state_eia923'
    abbr = Column(String, primary_key = True)
    state = Column(String, nullable=False)

class RegulatoryStatusEIA923(models.PUDLBase):
    """
    Regulatory status used in EIA Form 923
    """
    __tablename__ = 'regulatory_status_eia923'
    abbr = Column(String, primary_key = True)
    status = Column(String, nullable = False)

class NaturalGasTranspoServiceEIA923(models.PUDLBase):
    """
    Contract type for natural gas capacity service, used in EIA Form 923
    """
    __tablename__ = 'natural_gas_transpo_service_eia923'
    abbr = Column(String, primary_key = True)
    status = Column(String, nullable = False)

class TranspoModeEIA923(models.PUDLBase):
    """
    Mode used for longest & 2nd longest distance in EIA Form 923
    """
    __tablename__ = 'transpo_mode_eia923'
    abbr = Column(String, primary_key = True)
    mode = Column(String, nullable = False)



###########################################################################
# Tables which represent EIA Form 923 data. E.g. Fuel Receipts.
###########################################################################

class PlantInfoEIA923(PUDLBase):
    """
    Includes static information about each plant as reported on Page 1 of EIA
    Form 923
    """
    __tablename__ = 'plant_info_eia923'
    plant_id = Column(Integer, primary_key=True)
    combined_heat_power = Column(String,
        ForeignKey('combined_heat_power_eia923.abbr'), nullable=False)
    plant_state = Column(String, ForeignKey('us_states.abbr'), nullable=False)
    census_region = Column(String, ForeignKey('census_region.abbr'),
        nullable=False)
    nerc_region = Column(String, ForeignKey('nerc_region.abbr'), nullable=False)
    eia_sector = Column(String, ForeignKey('sector_eia.id'), nullable=False)
    naics_code = Column(Integer, nullable=False)
    operator_id = Column(String, ForeignKey('utilities_eia923.operator_id'),
        nullable=False)
    regulated = Column(String, ForeignKey('regulatory_status_eia923.abbr'))
    reporting_frequency = Column(String,
        ForeignKey('respondent_frequency_eia923.abbr'))

class GeneratorFuelEIA923(models.PUDLBase):
    """
    Annual fuel consumed by a given plant, as reported to EIA in Form 923 Page 1
    This information comes from the XXXXXX table in the XXXXX DB, which is
    populated from EIA Form 923 Page 1 Generation and Fuel Data.
    """
    __tablename__ = 'generator_fuel_eia923'

    # Each month, for each unique combination of prime mover and fuel type,
    # there is one report for each plant, which may be recorded multiple times
    # for multiple utilities that have a stake in the plant...
    # Primary key fields used previously:
    # plant, prime mover, fuel type, and year.
    id = Column(Integer, autoincrement=True, primary_key=True) #surrogate key
    plant_id = Column(Integer, ForeignKey('plants_eia923.plant_id'),
        nullable=False)
    year = Column(Integer, ForeignKey('years.year'), nullable=False)
    month = Column(Integer, ForeignKey('months.month'), nullable=False)
    nuclear_unit_id = Column(Integer)
    fuel_type = Column(String, nullable = False) #fuel_type & aer_fuel_type is a
        # many to many relationship
    aer_fuel_type = Column(String, ForeignKey('fuel_type_aer_eia923.abbr'),
        nullable=False)
#TODO: Ferc1 fuel table uses 'fuel_qty_burned' but EIA includes hydro data, etc.
    fuel_qty_consumed_total = Column(Float, nullable=False)
    fuel_qty_consumed_internal = Column(Float, nullable=False)
    fuel_mmbtu_per_unit = Column(Float, nullable=False)
    fuel_consumed_mmbtu_tot = Column(Float, nullable=False)
    fuel_consumed_for_electricity_mmbtu = Column(Float, nullable=False)
    net_gen_electricity = Column(Float, nullable=False)

class BoilerFuelDataEIA923(models.PUDLBase):
    """
    Monthly Boiler Fuel consumption and emissions Time Series, as reported to
    EIA in Form 923. This information comes from the XXXXXX table in the XXXXX
    DB, which is populated from EIA Form 923 Page 3 Generator Data.
    Sources: EIA-923 and EIA-860 Reports
    """
    __tablename__ = 'boiler_data_eia923'

    # Each month, for each unique combination of boiler id and prime mover and
    # fuel, there is one report for each boiler unit in each plant.
    # Primary key fields used previously:
    # plant, boiler, prime mover, fuel type, and year.

    id = Column(Integer, autoincrement=True, primary_key=True) #surrogate key
    plant_id = Column(Integer, ForeignKey('plants_eia923.plant_id'),
        nullable=False)
    prime_mover = Column(String, ForeignKey('prime_movers_eia923.abbr'),
        nullable=False)
    fuel_type = Column(String, ForeignKey('fuel_type_eia923.abbr'),
        nullable=False)
    boiler_id = Column(String, ForeignKey('boilers_eia923.boiler_id'),
        nullable=False)
    year = Column(Integer, ForeignKey('years.year'), nullable=False)
    month = Column(Integer, ForeignKey('months.month'), nullable=False)
    fuel_qty_consumed = Column(Float, nullable=False)
    fuel_mmbtu_per_unit = Column(Float, nullable=False)
    sulfur_content = Column(Float, nullable=False)
    ash_content = Column(Float, nullable=False)

class GeneratorDataEIA923(models.PUDLBase):
    """
    Monthly Generating Unit Net Generation Time Series by a given plant, as
    reported to EIA in Form 923. This information comes from the XXXXXX table in
    the XXXXX DB, which is populated from EIA Form 923 Page 4 Generator Data.
    Sources: EIA-923 and EIA-860 Reports
    """
    __tablename__ = 'generator_data_eia923'

    # Each month, for each unique combination of generator id and prime mover
    # and fuel,there is one report for each generator unit in each plant.
    # Primary key fields used previously:
    # plant, generator, prime mover, year, and month.
    id = Column(Integer, autoincrement=True, primary_key=True) # surrogate key
    plant_id = Column(Integer, ForeignKey('plants_eia923.plant_id'),
        nullable=False)
#TODO remove prime_mover since it's specific to generator_id?
    prime_mover = Column(String, ForeignKey('prime_movers_eia923.abbr'),
        nullable=False)
    generator_id = Column(String, nullable=False) # TODO generators table?  FK?
    year = Column(Integer, ForeignKey('years.year'), nullable=False)
    month = Column(Integer, ForeignKey('months.month'), nullable=False)
    net_generation_mwh = Column(Float, nullable=False)


class FuelReceiptsCostsEIA923(models.PUDLBase):
    """
    Fuel receipts and costs reported for individual fuel purchases;
    As reported on Page 5 of EIA Form 923
    Sources: EIA-923 and EIA-860 Reports
    """

    __tablename__ = 'fuel_receipts_costs_eia923'

    # Primary key fields used previously:
    # plant_id and fuel_type.

    fuel_receipt_id = Column(Integer, primary_key=True, autoincrement=True)
        # surrogate key
    plant_id = Column(Integer, ForeignKey('plants_eia923.plant_id'),
        nullable=False)#instead of plant name, which is field listed on Page 5?
    year = Column(Integer, ForeignKey('years.year'), nullable=False)
    month = Column(Integer, ForeignKey('months.month'), nullable=False)
    contract_type = Column(String,
    ForeignKey('contract_type_eia923.abbr'), nullable=False)
        # TODO use contract_type field? Or abbr?
    contract_expiration_date = Column(Integer, nullable=False)
    energy_source = Column(String, ForeignKey('energy_source_eia923.abbr'),
        nullable=False)
    fuel_group = Column(String, ForeignKey('fuel_group_eia923.group'),
        nullable=False)
#TODO: add FK for coalmine_msha_id
    coalmine_msha_id = Column(Integer)
    supplier =  Column(String, nullable=False) # TODO FK new table?
    qty = Column(Integer, nullable=False)
    average_heat_content = Column(Integer, nullable=False)
    average_sulfur_content = Column(Integer, nullable=False)
    average_ash_content = Column(Integer, nullable=False)
    average_mercury_content = Column(Integer, nullable=False)
    fuel_cost = Column(Integer) #null values exist in data
    primary_transportation_mode = Column(String,
        ForeignKey('transpo_mode_eia923.abbr'), nullable=False)
    secondary_transportation_mode = Column(String,
        ForeignKey('transpo_mode_eia923.abbr'), nullable=False)
    natural_gas_transportation_service = Column(String,
        ForeignKey('natural_gas_transpo_service_eia923.abbr'), nullable=False)
