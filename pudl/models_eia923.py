from sqlalchemy import Column, ForeignKey, Integer, String, Float
from sqlalchemy.orm import relationship
#from sqlalchemy.orm.collections import attribute_mapped_collection

from pudl import settings, constants, models

###########################################################################
# Tables which represent static lists. E.g. all the US States.
###########################################################################

#class PlantInfo(models.PUDLBase):
#    __tablename__ = 'plant_info_eia'
#    plant_id = Column(Integer, ForeignKey('plants.id'), primary_key=True)
#    combined_heat_power = Column(String, ForeignKey('????.???'), nullable=False)
#    plant_state = Column(String, ForeignKey(us_states.abbr), nullable=False)
#    census_region = Column(String, ForeignKey(census_region.abbr), nullable=False)
#    nerc_region = Column(String, ForeignKey(nerc_region.abbr), nullable=False)
#    eia_sector = Column(String, ForeignKey(eia_sector.number), nullable=False) #may need to rethink this
#    sector_name = Column(String, ForeignKey(eia_sector.name), nullable=False) #may need to rethink this
#    naics_code = Column(Integer, ForeignKey(naics_code.number), nullable=False)

##example class from pudl.py
class GeneratorFuelEIA923(models.PUDLBase):
    """
    Annual fuel consumed by a given plant, as reported to EIA in Form 923. This
    information comes from the XXXXXX table in the XXXXX DB, which is
    populated from EIA Form 923 Page 1 Generation and Fuel Data.
    """
    __tablename__ = 'generator_fuel_eia923'
    # Each month, for each unique combination of prime mover and fuel type,
    #there is one report for each plant, which may be recorded multiple times
    #for multiple utilities that have a stake in the plant...
    #Primary key fields: plant, utility, prime mover, fuel type, and year.
    plant_id = Column(Integer, ForeignKey('plants.id'), primary_key=True)
    utility_id = Column(Integer, ForeignKey('utilities.id'), primary_key=True)
    prime_mover = Column(String, ForeignKey('prime_movers.prime_mover'), primary_key=True)
    fuel_type = Column(String, ForeignKey('fuels.name'), primary_key=True)
    year = Column(Integer, ForeignKey('years.year'), primary_key=True)
    month = Column(Integer, ForeignKey('months.month'), primary_key=True)
    nuclear_unit_id = Column(Integer, nullable=True)
    plant_name = Column(String, nullable=False)
    operator_name = Column(String, nullable=False)
    AER_fuel_type = Column(String, nullable=False)
    fuel_unit = Column(String, nullable=False)
    quant_consumed_total = Column(Float, nullable=False)
    quant_consumed_internal = Column(Float, nullable=False)
    fuel_mmbtu_per_unit = Column(Float, nullable=False)
    fuel_consumed_mmbtu_tot = Column(Float, nullable=False)
    fuel_consumed_for_electricity_mmbtu = Column(Float, nullable=False)
    net_gen_electricity = Column(Float, nullable=False)

class BoilerFuelDataEIA923(models.PUDLBase):
    """
    Monthly Boiler Fuel consumptino and emissions Time Series, as reported to
    EIA in Form 923. This information comes from the XXXXXX table in the XXXXX
    DB, which is populated from EIA Form 923 Page 3 Generator Data.
    """
    __tablename__ = 'boiler_data_eia923'
    # Each month, for each unique combination of boiler id and prime mover and fuel,
    #there is one report for each boiler unit in each plant.
    #Primary key fields: plant, utility, boiler, prime mover, fuel type, and year.

    # Should this be PUDL or EIA plant_id? Should we have both here?
    plant_id = Column(Integer, ForeignKey('plants.id'), primary_key=True)
    utility_id = Column(Integer, ForeignKey('utilities.id'), primary_key=True)
    prime_mover = Column(String, ForeignKey('prime_movers.prime_mover'), primary_key=True)
    generator_id = Column(String, ForeignKey('boilers.boiler'), primary_key=True) #is this correct?
    fuel_type = Column(String, ForeignKey('fuels.name'), primary_key=True)
    year = Column(Integer, ForeignKey('years.year'), primary_key=True)
    month = Column(Integer, ForeignKey('months.month'), primary_key=True)
    fuel_unit = Column(String, ForeignKey('fuel_units.unit'), nullable=False)
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
    """
    __tablename__ = 'generator_data_eia923'
    # Each month, for each unique combination of generator id and prime mover and fuel,
    #there is one report for each generator unit in each plant.
    #Primary key fields: plant, utility, generator, and prime mover.
    plant_id = Column(Integer, ForeignKey('plants.id'), primary_key=True)

    #field is operator ID (column F) in EIA923Page1
    utility_id = Column(Integer, ForeignKey('utilities.id'), primary_key=True)
    prime_mover = Column(String, ForeignKey('prime_movers.prime_mover'), primary_key=True)
    generator_id = Column(String, ForeignKey('generators.generator'), primary_key=True) #is this correct?
    year = Column(Integer, ForeignKey('years.year'), primary_key=True)
    month = Column(Integer, ForeignKey('months.month'), primary_key=True)
    plant_name = Column(String, ForeignKey('plants_eia923.plant_name'), nullable=False)
    operator_name = Column(String, ForeignKey('utilities_eia923.operator_name'), nullable=False)
    net_generation_mwh = Column(Float, nullable=False)

class FuelReceiptsCostsEIA923(models.PUDLBase):
    __tablename__ = 'fuel_receipts_costs_eia923'
    fuel_receipt_id = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(Integer, ForeignKey('years.year'), nullable=False)
    month = Column(Integer, ForeignKey('months.month'), nullable=False) #is this correct?
    plant_id = Column(Integer, ForeignKey('plants.id'), nullable=False)
    purchase_type = Column(String, nullable=False)
    contract_expiration_date = Column(Integer, nullable=False)
    energy_source = Column(String, nullable=False) # TODO add FK/constants
    fuel_group = Column(String, nullable=False) # TODO add FK/constants
    coalmine_type = Column(String, nullable=False) # TODO add FK/constants
    coalmine_state = Column(String, nullable=False) # TODO add FK/constants
    coalmine_county = Column(String, nullable=False) # TODO add FK/constants
    coalmine_msha_id = Column(Integer, nullable=False) # TODO add FK/new table
    coalmine_name = Column(String, nullable=False) # TODO add FK/new table?
    supplier =  Column(String, nullable=False) # TODO FK new table?
    quantity = Column(Integer, nullable=False)
    average_heat_content = Column(Integer, nullable=False)
    average_sulfur_content = Column(Integer, nullable=False)
    average_ash_content = Column(Integer, nullable=False)
    average_mercury_content = Column(Integer, nullable=False)
    fuel_cost = Column(Integer, nullable=False)
    regulated = Column(String, nullable=False) # TODO add FK/constants
    operator_name = Column(String, ForeignKey('utilities_eia923.operator_name'), nullable=False)
    reporting_frequency = Column(String, nullable=False)  #do we have constants for these?
    primary_transportation_mode = Column(String, nullable=False)  #do we have constants for these?
    secondary_transportation_mode = Column(String, nullable=False) #do we have constants for these?
    natural_gas_transportation_service = Column(String, nullable=False)  #do we have constants for these?
