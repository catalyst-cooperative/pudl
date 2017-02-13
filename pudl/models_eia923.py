from sqlalchemy import Column, ForeignKey, Integer, String, Float
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine.url import URL
#from sqlalchemy.orm.collections import attribute_mapped_collection

from pudl import settings, constants, models


###########################################################################
# Tables which represent static lists. E.g. all the US States.
###########################################################################

#class PlantInfo(Base):
#    __tablename__ = 'plant_info_eia'
#    plant_id = Column(Integer, ForeignKey('plants.id'), primary_key=True)
#    combined_heat_power = Column(String, ForeignKey('????.???'), nullable=False)
#    plant_state = Column(String, ForeignKey(us_states.abbr), nullable=False)
#    census_region = Column(String, ForeignKey(census_region.abbr), nullable=False)
#    nerc_region = Column(String, ForeignKey(nerc_region.abbr), nullable=False)
#    eia_sector = Column(String, ForeignKey(eia_sector.number), nullable=False) #may need to rethink this
#    sector_name = Column(String, ForeignKey(eia_sector.name), nullable=False) #may need to rethink this


##example class from pudl.py
class GeneratorFuelEIA923(Base):
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
    utility_id = Column(Integer, ForeignKey('utilities.id'), primary_key=True) #field is operator ID (column F) in EIA923Page1
    prime_mover = Column(String, ForeignKey('prime_movers.prime_mover'), primary_key=True)
    fuel_type = Column(String, ForeignKey('fuels.name'), primary_key=True)
    year = Column(Integer, ForeignKey('years.year'), primary_key=True)    
    nuclear_unit_id = Column(Integer, nullable=True)
    plant_name = Column(String, nullable=False)
    operator_name = Column(String, nullable=False)
    AER_fuel_type = Column(String, nullable=False)    
    fuel_unit = Column(String, nullable=False)    
    quant_consumed_total_jan = Column(Integer, nullable=False)
    quant_consumed_total_feb = Column(Integer, nullable=False)
    quant_consumed_total_mar = Column(Integer, nullable=False)
    quant_consumed_total_apr = Column(Integer, nullable=False)
    quant_consumed_total_may = Column(Integer, nullable=False)
    quant_consumed_total_jun = Column(Integer, nullable=False)
    quant_consumed_total_jul = Column(Integer, nullable=False)
    quant_consumed_total_aug = Column(Integer, nullable=False)
    quant_consumed_total_sep = Column(Integer, nullable=False)
    quant_consumed_total_oct = Column(Integer, nullable=False)
    quant_consumed_total_nov = Column(Integer, nullable=False)
    quant_consumed_total_dec = Column(Integer, nullable=False)  
    quant_consumed_internal_jan = Column(Integer, nullable=False)  
    quant_consumed_internal_feb = Column(Integer, nullable=False)  
    quant_consumed_internal_mar = Column(Integer, nullable=False)  
    quant_consumed_internal_apr = Column(Integer, nullable=False)  
    quant_consumed_internal_may = Column(Integer, nullable=False)  
    quant_consumed_internal_jun = Column(Integer, nullable=False)  
    quant_consumed_internal_jul = Column(Integer, nullable=False)  
    quant_consumed_internal_aug = Column(Integer, nullable=False)  
    quant_consumed_internal_sep = Column(Integer, nullable=False)  
    quant_consumed_internal_oct = Column(Integer, nullable=False)  
    quant_consumed_internal_nov = Column(Integer, nullable=False)  
    quant_consumed_internal_dec = Column(Integer, nullable=False)      
    fuel_mmbtu_per_unit_jan = Column(Float, nullable=False)
    fuel_mmbtu_per_unit_feb = Column(Float, nullable=False)
    fuel_mmbtu_per_unit_mar = Column(Float, nullable=False)
    fuel_mmbtu_per_unit_apr = Column(Float, nullable=False)
    fuel_mmbtu_per_unit_may = Column(Float, nullable=False)
    fuel_mmbtu_per_unit_jun = Column(Float, nullable=False)
    fuel_mmbtu_per_unit_jul = Column(Float, nullable=False)
    fuel_mmbtu_per_unit_aug = Column(Float, nullable=False)
    fuel_mmbtu_per_unit_sep = Column(Float, nullable=False)
    fuel_mmbtu_per_unit_oct = Column(Float, nullable=False)
    fuel_mmbtu_per_unit_nov = Column(Float, nullable=False)
    fuel_mmbtu_per_unit_dec = Column(Float, nullable=False)
    fuel_consumed_MMBtu_tot_jan = Column(Integer, nullable=False)
    fuel_consumed_MMBtu_tot_feb = Column(Integer, nullable=False) 
    fuel_consumed_MMBtu_tot_mar = Column(Integer, nullable=False) 
    fuel_consumed_MMBtu_tot_apr = Column(Integer, nullable=False) 
    fuel_consumed_MMBtu_tot_may = Column(Integer, nullable=False) 
    fuel_consumed_MMBtu_tot_jun = Column(Integer, nullable=False) 
    fuel_consumed_MMBtu_tot_jul = Column(Integer, nullable=False) 
    fuel_consumed_MMBtu_tot_aug = Column(Integer, nullable=False) 
    fuel_consumed_MMBtu_tot_sep = Column(Integer, nullable=False) 
    fuel_consumed_MMBtu_tot_oct = Column(Integer, nullable=False) 
    fuel_consumed_MMBtu_tot_nov = Column(Integer, nullable=False) 
    fuel_consumed_MMBtu_tot_dec = Column(Integer, nullable=False) 
    fuel_consumed_for_electricity_MMBtu_jan = Column(Integer, nullable=False)
    fuel_consumed_for_electricity_MMBtu_feb = Column(Integer, nullable=False)
    fuel_consumed_for_electricity_MMBtu_mar = Column(Integer, nullable=False)
    fuel_consumed_for_electricity_MMBtu_apr = Column(Integer, nullable=False)
    fuel_consumed_for_electricity_MMBtu_may = Column(Integer, nullable=False)
    fuel_consumed_for_electricity_MMBtu_jun = Column(Integer, nullable=False)
    fuel_consumed_for_electricity_MMBtu_jul = Column(Integer, nullable=False)
    fuel_consumed_for_electricity_MMBtu_aug = Column(Integer, nullable=False)
    fuel_consumed_for_electricity_MMBtu_sep = Column(Integer, nullable=False)
    fuel_consumed_for_electricity_MMBtu_oct = Column(Integer, nullable=False)
    fuel_consumed_for_electricity_MMBtu_nov = Column(Integer, nullable=False)
    fuel_consumed_for_electricity_MMBtu_dec = Column(Integer, nullable=False)
    net_gen_electricity_jan = Column(Integer, nullable=False)
    net_gen_electricity_feb = Column(Integer, nullable=False)
    net_gen_electricity_mar = Column(Integer, nullable=False)
    net_gen_electricity_apr = Column(Integer, nullable=False)
    net_gen_electricity_may = Column(Integer, nullable=False)
    net_gen_electricity_jun = Column(Integer, nullable=False)
    net_gen_electricity_jul = Column(Integer, nullable=False)
    net_gen_electricity_aug = Column(Integer, nullable=False)
    net_gen_electricity_sep = Column(Integer, nullable=False)
    net_gen_electricity_oct = Column(Integer, nullable=False)
    net_gen_electricity_nov = Column(Integer, nullable=False)
    net_gen_electricity_dec = Column(Integer, nullable=False)


                                                        
               