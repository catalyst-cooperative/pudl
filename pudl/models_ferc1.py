from sqlalchemy import Column, ForeignKey, Integer, String, Float
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy.orm import relationship

from pudl import settings, constants, models

###########################################################################
# Tables comprising data from the FERC f1_steam & f1_fuel tables
###########################################################################

class FuelFERC1(models.PUDLBase):
    """
    Annual fuel consumed by a given plant, as reported to FERC in Form 1. This
    information comes from the f1_fuel table in the FERC DB, which is
    populated from page 402 of the paper FERC For 1.
    """
    __tablename__ = 'fuel_ferc1'
    __table_args__ = (ForeignKeyConstraint(
                        ['respondent_id', 'plant_name'],
                        ['plants_ferc1.respondent_id', 'plants_ferc1.plant_name']),)
    # Each year, for each fuel, there's one report for each plant, which may
    # be recorded multiple times for multiple utilities that have a stake in
    # the plant... Primary key fields: utility, plant, fuel and year.

#    plant_id = Column(Integer, ForeignKey('plants.id'), primary_key=True)
#    utility_id = Column(Integer, ForeignKey('utilities.id'), primary_key=True)

    id = Column(Integer, autoincrement=True, primary_key=True)
    respondent_id = Column(Integer, nullable=False) # Also ForeignKeyConstraint
    plant_name = Column(String, nullable=False) # Also ForeignKeyConstraint
    report_year = Column(Integer, ForeignKey('years.year'), nullable=False)
    fuel = Column(String, ForeignKey('fuels.name'), nullable=False)
    fuel_unit = Column(String, ForeignKey('fuel_units.unit'), nullable=False)
    fuel_qty_burned = Column(Float, nullable=False)
    fuel_avg_mmbtu_per_unit = Column(Float, nullable=False)
    fuel_cost_per_unit_burned = Column(Float, nullable=False)
    fuel_cost_per_unit_delivered = Column(Float, nullable=False)
    fuel_cost_per_mmbtu = Column(Float, nullable=False)
    fuel_cost_per_kwh = Column(Float, nullable=False)
    fuel_mmbtu_per_kwh = Column(Float, nullable=False)

class PlantSteamFERC1(models.PUDLBase):
    """
    A large thermal generating plant, as reported to FERC on Form 1.
    """
    __tablename__ = 'plants_steam_ferc1'
    __table_args__ = (ForeignKeyConstraint(
                        ['respondent_id', 'plant_name'],
                        ['plants_ferc1.respondent_id', 'plants_ferc1.plant_name']),)
    id = Column(Integer, autoincrement=True, primary_key=True)
    respondent_id = Column(Integer, nullable=False)
    plant_name = Column(String, nullable=False)
    report_year = Column(Integer, ForeignKey('years.year'), nullable=False)
    plant_kind = Column(String) # FK New, needs cleaning
    type_const = Column(String) # FK New, needs cleaning
    year_constructed = Column(Integer) # Data?
    year_installed = Column(Integer) # Data?
    total_capacity = Column(Float) # Data!
    peak_demand = Column(Float) # Data
    plant_hours = Column(Float) # Data
    plant_capability = Column(Float) # Data
    when_not_limited = Column(Float) # Data
    when_limited = Column(Float) # Data
    avg_num_employees = Column(Float) # Data
    net_generation = Column(Float) # Data
    cost_land = Column(Float) # Data
    cost_structure = Column(Float) # Data
    cost_equipment = Column(Float) # Data
    cost_of_plant_total= Column(Float) # Data
    cost_per_kw = Column(Float) # Data
    expns_operations = Column(Float) # Data
    expns_fuel = Column(Float) # Data
    expns_coolants = Column(Float) # Data
    expns_steam = Column(Float) # Data
    expns_steam_other = Column(Float) # Data
    expns_transfer = Column(Float) # Data
    expns_electric = Column(Float) # Data
    expns_misc_power = Column(Float) # Data
    expns_rents = Column(Float) # Data
    expns_allowances = Column(Float) # Data
    expns_engineering = Column(Float) # Data
    expns_structures = Column(Float) # Data
    expns_boiler = Column(Float) # Data
    expns_plants = Column(Float) # Data
    expns_misc_steam = Column(Float) # Data
    expns_production_total = Column(Float) # Data
    expns_kwh = Column(Float) # Data
    asset_retire_cost = Column(Float) # Data

class HydroFERC1(models.PUDLBase):
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
