from sqlalchemy import Column, ForeignKey, Integer, String, Float, Numeric
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
    fuel_cost_per_mwh = Column(Float, nullable=False)
    fuel_mmbtu_per_mwh = Column(Float, nullable=False)

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
    total_capacity_mw = Column(Float) # Data!
    peak_demand_mw = Column(Float) # Data
    plant_hours = Column(Float) # Data
    plant_capability_mw = Column(Float) # Data
    water_limited_mw = Column(Float) # Data
    not_water_limited_mw = Column(Float) # Data
    avg_num_employees = Column(Float) # Data
    net_generation_mwh = Column(Float) # Data
    cost_land = Column(Numeric(14,2)) # Data
    cost_structure = Column(Numeric(14,2)) # Data
    cost_equipment = Column(Numeric(14,2)) # Data
    cost_of_plant_total= Column(Numeric(14,2)) # Data
    cost_per_mw = Column(Numeric(14,2)) # Data
    expns_operations = Column(Numeric(14,2)) # Data
    expns_fuel = Column(Numeric(14,2)) # Data
    expns_coolants = Column(Numeric(14,2)) # Data
    expns_steam = Column(Numeric(14,2)) # Data
    expns_steam_other = Column(Numeric(14,2)) # Data
    expns_transfer = Column(Numeric(14,2)) # Data
    expns_electric = Column(Numeric(14,2)) # Data
    expns_misc_power = Column(Numeric(14,2)) # Data
    expns_rents = Column(Numeric(14,2)) # Data
    expns_allowances = Column(Numeric(14,2)) # Data
    expns_engineering = Column(Numeric(14,2)) # Data
    expns_structures = Column(Numeric(14,2)) # Data
    expns_boiler = Column(Numeric(14,2)) # Data
    expns_plants = Column(Numeric(14,2)) # Data
    expns_misc_steam = Column(Numeric(14,2)) # Data
    expns_production_total = Column(Numeric(14,2)) # Data
    expns_per_mwh = Column(Numeric(14,2)) # Data
    asset_retire_cost = Column(Numeric(14,2)) # Data

class PlantInServiceFERC1(models.PUDLBase):
    """
    Balances and changes to FERC Electric Plant In Service accounts.

    This data comes from f1_plant_in_srvce in the ferc1 DB. It corresponds to
    the balances of the FERC Uniform System of Accounts for Electric Plant
    which is well documented here:

    https://www.law.cornell.edu/cfr/text/18/part-101

    Each FERC respondent reports starting & ending balances, and changes to
    the account balances, each year.
    """
    __tablename__ = 'plant_in_service_ferc1'
    respondent_id = Column(Integer,
                           ForeignKey('utilities_ferc1.respondent_id'),
                           primary_key=True)
    report_year = Column(Integer,
                         ForeignKey('years.year'),
                         primary_key=True)
    ferc_account_id = Column(String,
                             ForeignKey('ferc_accounts.id'),
                             primary_key=True)
    beginning_year_balance = Column(Numeric(14,2))
    additions = Column(Numeric(14,2))
    retirements = Column(Numeric(14,2))
    adjustments = Column(Numeric(14,2))
    transfers = Column(Numeric(14,2))
    year_end_balance = Column(Numeric(14,2))

class PlantSmallFERC1(models.PUDLBase):
    """
    Annual data on "small plants" imported from the f1_gnrt_plant table.

    Capacity and generation data related to a heterogenous collection of small
    plants that have to report to FERC. Includes many renewable energy
    facilities, as well as smaller thermal generation stations.
    """
    __tablename__ = 'plants_small_ferc1'
    __table_args__ = (ForeignKeyConstraint(
                        ['respondent_id', 'plant_name'],
                        ['plants_ferc1.respondent_id', 'plants_ferc1.plant_name']),)
    id = Column(Integer, autoincrement=True, primary_key=True)
    respondent_id = Column(Integer, nullable=False)
    plant_name = Column(String, nullable=False)
    report_year = Column(Integer, ForeignKey('years.year'), nullable=False)
    year_constructed = Column(Integer)
    total_capacity_mw = Column(Float)
    peak_demand_mw = Column(Float)
    net_generation_mwh = Column(Float)
    cost_of_plant_total = Column(Numeric(14,2))
    cost_of_plant_per_mw = Column(Numeric(14,2))
    cost_of_operation = Column(Numeric(14,2)) # No idea what this field is.
    expns_fuel = Column(Numeric(14,2))
    expns_maintenance = Column(Numeric(14,2))
    kind_of_fuel = Column(String)
    fuel_cost_per_mmbtu = Column(Numeric(14,2))

class PlantHydroFERC1(models.PUDLBase):
    """
    Annual data on hydro plants from FERC form 1
    Christina is working on this one.
    """
    __tablename__ = 'plants_hydro_ferc1'
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
