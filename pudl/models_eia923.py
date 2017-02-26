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
# Tables which represent static lists. E.g. all the US States.
###########################################################################

class PlantInfo(models.PUDLBase):
    """
    Static information about each plant as reported on Page 1 of EIA Form 923
    """
    __tablename__ = 'plant_info_eia'

    plant_id = Column(Integer, ForeignKey('plants_eia23.plant_id'), primary_key = True)
#    combined_heat_power = Column(String, ForeignKey('????.???'), nullable=False)
#    plant_state = Column(String, ForeignKey(us_states.abbr), nullable=False)
    census_region = Column(String, ForeignKey('census_region.abbr'), nullable=False)
#    nerc_region = Column(String, ForeignKey(nerc_region.abbr), nullable=False)
#    eia_sector = Column(String, ForeignKey(eia_sector.number), nullable=False) #may need to rethink this
#    sector_name = Column(String, ForeignKey(eia_sector.name), nullable=False) #may need to rethink this
#    naics_code = Column(Integer, ForeignKey(naics_code.number), nullable=False)

##example class from pudl.py
class GeneratorFuelEIA923(models.PUDLBase):
    """
    Annual fuel consumed by a given plant, as reported to EIA in Form 923 Page 1.
    This information comes from the XXXXXX table in the XXXXX DB, which is
    populated from EIA Form 923 Page 1 Generation and Fuel Data.
    """
    __tablename__ = 'generator_fuel_eia923'

    # Each month, for each unique combination of prime mover and fuel type,
    # there is one report for each plant, which may be recorded multiple times
    # for multiple utilities that have a stake in the plant...
    # Primary key fields used previously:
    # plant, utility, prime mover, fuel type, and year.
    id = Column(Integer, autoincrement=True, primary_key=True)
    plant_id = Column(Integer, ForeignKey('plants_eia23.plant_id'))
    utility_id = Column(Integer, ForeignKey('utilities_eia923.operator_id'))
    prime_mover = Column(String, ForeignKey('prime_mover_eia923.prime_mover'))
    fuel_type = Column(String, ForeignKey('fuel_type_eia923.fuel_type'))
    year = Column(Integer, ForeignKey('years.year'))
    month = Column(Integer, ForeignKey('months.month'))
    nuclear_unit_id = Column(Integer)
    plant_name = Column(String, nullable=False) # TODO create class/FK
    operator_name = Column(String, nullable=False) # TODO create class/FK
    AER_fuel_type = Column(String, ForeignKey('fuel_type_aer_eia923.fuel_type'), nullable=False)
    fuel_unit = Column(String, ForeignKey('fuel_unit_eia923.unit'), nullable=False)
    quant_consumed_total = Column(Float, nullable=False)
    quant_consumed_internal = Column(Float, nullable=False)
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

    # Each month, for each unique combination of boiler id and prime mover and fuel,
    #there is one report for each boiler unit in each plant.
    #Primary key fields used previously:
    # plant, utility, boiler, prime mover, fuel type, and year.

    id = Column(Integer, autoincrement=True, primary_key=True)
    plant_id = Column(Integer, ForeignKey('plants_eia23.plant_id'))
    utility_id = Column(Integer, ForeignKey('utilities_eia923.operator_id'))
    prime_mover = Column(String, ForeignKey('prime_mover_eia923.prime_mover'))
    fuel_type = Column(String, ForeignKey('fuel_type_eia923.fuel_type'))
    generator_id = Column(String, ForeignKey('boilers.boiler')) #TODO this was generator field - was that correct? TODO: need to create class/FK
    year = Column(Integer, ForeignKey('years.year'))
    month = Column(Integer, ForeignKey('months.month'))
    fuel_unit = Column(String, ForeignKey('fuel_unit_eia923.unit'), nullable=False)
    plant_name = Column(String, ForeignKey('plants_eia923.plant_name'), nullable=False)
    operator_name = Column(String, ForeignKey('utilities_eia923.operator_name'),nullable=False)
    boiler_id = Column(String, nullable=False) # TODO: boilers table? FK?
    quant_consumed = Column(Float, nullable=False)
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

    # Each month, for each unique combination of generator id and prime mover and fuel,
    # there is one report for each generator unit in each plant.
    # Primary key fields: plant, utility, generator, and prime mover.
    plant_id = Column(Integer, ForeignKey('plants_eia23.plant_id'), primary_key=True)
    utility_id = Column(Integer, ForeignKey('utilities_eia923.operator_id'), primary_key=True)
    prime_mover = Column(String, ForeignKey('prime_mover_eia923.prime_mover'), primary_key=True)
    generator_id = Column(String, ForeignKey('generators.generator'), primary_key=True) #is this correct?
    year = Column(Integer, ForeignKey('years.year'), primary_key=True)
    month = Column(Integer, ForeignKey('months.month'), primary_key=True)
    plant_name = Column(String, ForeignKey('plants_eia923.plant_name'), nullable=False)
    operator_name = Column(String, ForeignKey('utilities_eia923.operator_name'), nullable=False)
    net_generation_mwh = Column(Float, nullable=False)

class FuelReceiptsCostsEIA923(models.PUDLBase):
    """
    Fuel receipts and costs reported for individual fuel purchases;
    As reported on Page 5 of EIA Form 923
    Sources: EIA-923 and EIA-860 Reports
    """

    __tablename__ = 'fuel_receipts_costs_eia923'
    __table_args__ = (ForeignKeyConstraint(
                        ['plant_id', 'fuel_type'],
                        ['plants_eia23.plant_id', 'fuel_type_eia923.fuel_type']),)

    fuel_receipt_id = Column(Integer, primary_key=True, autoincrement=True) #Create this field?
    plant_id = Column(Integer, ForeignKey('plants_eia23.plant_id'), primary_key=True)#instead of plant name, which is field listed on Page 5?
    year = Column(Integer, ForeignKey('years.year'), nullable=False)
    month = Column(Integer, ForeignKey('months.month'), nullable=False)
    contract_type = Column(String, ForeignKey('contract_type_eia923.contract_type'), nullable=False) #use contract_type field? Or abbr?
    contract_expiration_date = Column(Integer, nullable=False)
    energy_source = Column(String, ForeignKey('energy_source_eia923.source'), nullable=False)
    fuel_group = Column(String, ForeignKey('fuel_group_eia923.group'), nullable=False)
    coalmine_type = Column(String, ForeignKey('coalmine_type_eia923.name'))
    coalmine_state = Column(String, ForeignKey('coalmine_state_eia923.state'))
    coalmine_county = Column(Integer) # TODO check feasibility to add FK/constants or map to FIPS code used by EIA
    coalmine_msha_id = Column(Integer) # TODO check feasibility to add FK/constants or map to MSHA ID# used by EIA
    coalmine_name = Column(String) # TODO create FK as new table?
    supplier =  Column(String, nullable=False) # TODO FK new table?
    quantity = Column(Integer, nullable=False)
    average_heat_content = Column(Integer, nullable=False)
    average_sulfur_content = Column(Integer, nullable=False)
    average_ash_content = Column(Integer, nullable=False)
    average_mercury_content = Column(Integer, nullable=False)
    fuel_cost = Column(Integer) #null values exist in data
    regulated = Column(String, ForeignKey('regulatory_status_eia923.status'), nullable=False)
    operator_name = Column(String, ForeignKey('utilities_eia923.operator_name'), nullable=False) # TODO Operator Name or ID here?
    reporting_frequency = Column(String, ForeignKey('respondent_frequency_eia923.unit'), nullable=False)
    primary_transportation_mode = Column(String, ForeignKey('transpo_mode_eia923.mode'), nullable=False)
    secondary_transportation_mode = Column(String, ForeignKey('transpo_mode_eia923.mode'), nullable=False)
    natural_gas_transportation_service = Column(String, ForeignKey('natural_gas_transpo_service_eia923.status'), nullable=False)
