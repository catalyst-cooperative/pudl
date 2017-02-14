from sqlalchemy import Column, ForeignKey, Integer, String, Float
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine.url import URL

#from sqlalchemy.orm.collections import attribute_mapped_collection

from pudl import settings, constants, PUDLBase

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

class HydroFERC1(Base):
    """
    Annual data on hydro plants from FERC form 1
    """
    __tablename__ = 'hydro_ferc1'
    plant_id = Column(Integer, ForeignKey('plants.id'), primary_key=True)
    utility_id = Column(Integer, ForeignKey('utilities.id'), primary_key=True)
    year = Column(Integer, ForeignKey('years.year'), primary_key=True)
    #respondent_id
    #report_year ?? Don't we need to link the report_year to the year ??
    project_no = Column(Integer)
    plant_name = Column(String)
    plant_kind = Column(String)
    plant_const = Column(String)
    yr_const = Column(Integer)
    yr_installed = Column(Integer)
    tot_capacity = Column(Float)
    peak_demand = Column(Float)
    plant_hours = Column(Float)
    favorable_cond = Column(Float)
    adverse_cond = Column(Float)
    avg_num_of_emp = Column(Float)
    net_generation = Column(Float)
    cost_of_land = Column(Float)
    cost_structure = Column(Float)
    cost_facilities = Column(Float)
    cost_equipment = Column(Float)
    cost_roads = Column(Float)
    cost_plant_total = Column(Float)
    cost_per_kw = Column(Float)
    expns_operations = Column(Float)
    expns_water_pwr = Column(Float)
    expns_hydraulic = Column(Float)
    expns_electric = Column(Float)
    expns_generation = Column(Float)
    expns_rents = Column(Float)
    expns_engnr = Column(Float)
    expns_structures = Column(Float)
    expns_dams = Column(Float)
    expns_plant = Column(Float)
    expns_misc_plant = Column(Float)
    expns_total = Column(Float)
    expns_kwh = Column(Float)
    asset_retire_cost = Column(Float)
    #report_prd
