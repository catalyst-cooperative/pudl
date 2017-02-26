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

class PlantHydroFERC1(models.PUDLBase):
    """
    Annual data on hydro plants from FERC form 1
    """
    __tablename__ = 'plants_hydro_ferc1'
    __table_args__ = (ForeignKeyConstraint(
                        ['respondent_id', 'plant_name'],
                        ['plants_ferc1.respondent_id', 'plants_ferc1.plant_name']),)
    id = Column(Integer, autoincrement=True, primary_key=True)
    respondent_id = Column(Integer) #
    year = Column(Integer, ForeignKey('years.year'))
    project_number = Column(Integer)
    plant_name = Column(String)
    plant_kind = Column(String) #FK
    plant_construction = Column(String) #
    year_constructed = Column(Integer) #
    year_installed = Column(Integer) #
    total_capacity_mw = Column(Float) #name plate capacity
    peak_demand_mw = Column(Float) #Net Peak Demand on Plant-Megawatts (60 minutes)
    plant_hours_connected_while_generating = Column(Float) #Plant Hours Connect to Load
    net_capacity_favorable_conditions_mw = Column(Float) # Net Plant Capability (in megawatts) (a) Under Most Favorable Oper Conditions
    net_capacity_adverse_conditions = Column(Float) # Net Plant Capability (in megawatts) (b) Under the Most Adverse Oper Conditions
    avg_number_employees = Column(Float) #Average Number of Employees
    net_generation = Column(Float) #Net Generation, Exclusive of Plant Use - Kwh
    cost_land = Column(Float) #Land and Land Rights
    cost_structure = Column(Float) #Structures and Improvements
    cost_facilities = Column(Float) #Reservoirs, Dams, and Waterways
    cost_equipment = Column(Float) #Equipment Costs
    cost_roads = Column(Float) #Roads, Railroads, and Bridges
    asset_retire_cost = Column(Float) #Asset Retirement Costs
    cost_plant_total = Column(Float) #TOTAL cost (Total of 14 thru 19)
    cost_per_kw = Column(Float) #Cost per KW of Installed Capacity (line 20 / 5)
    expns_operations = Column(Float) #Operation Supervision and Engineering
    expns_water_pwr = Column(Float) #Water for Power
    expns_hydraulic = Column(Float) #Hydraulic Expenses
    expns_electric = Column(Float) #Electric Expenses
    expns_generation = Column(Float) #Misc Hydraulic Power Generation Expenses
    expns_rents = Column(Float) #Rents
    expns_engineering = Column(Float) #Maintenance Supervision and Engineering
    expns_structures = Column(Float) #Maintenance of Structures
    expns_dams = Column(Float) #Maintenance of Reservoirs, Dams, and Waterways
    expns_plant = Column(Float) #Maintenance of Electric Plant
    expns_misc_plant = Column(Float) #Maintenance of Misc Hydraulic Plant
    expns_production_total = Column(Float) #Total Production Expenses (total 23 thru 33)
    expns_kwh = Column(Float) #Expenses per net KWh
    #report_prd

# class PlantsPumpedHydro(models.PUDLBase):
#     """docstring for PlantsPumpedHydro"""
#     __tablename__ = 'plants_pummped_hydro_ferc1'
#     __table_args__ = (ForeignKeyConstraint(
#                         ['respondent_id', 'plant_name'],
#                         ['plants_ferc1.respondent_id', 'plants_ferc1.plant_name']),)
#     respondent_id = Column(Integer, nullable=False)
#     report_year = Colum(Integer, nullable=False)
#     #spplmnt_num       | integer               | not null  | plain    |              |
#     #row_number        | integer               | not null  | plain    |              |
#     #row_seq           | integer               |           | plain    |              |
#     #row_prvlg         | character varying(1)  |           | extended |              |
#     project_number = Column(Integer)
#     plant_name = Column(String, nullable=False)
#     plant_kind = Column(String)
#     year_constructed = Column(Integer)
#     year_installed = Column(Integer)
#     tot_capacity      | double precision      |           | plain    |              |
#     peak_demand       | double precision      |           | plain    |              |
#     plant_hours       | double precision      |           | plain    |              |
#     plant_capability  | double precision      |           | plain    |              |
#     avg_num_of_emp    | double precision      |           | plain    |              |
#     net_generation    | double precision      |           | plain    |              |
#     energy_used       | double precision      |           | plain    |              |
#     net_load          | double precision      |           | plain    |              |
#     cost_land         | double precision      |           | plain    |              |
#     cost_structures   | double precision      |           | plain    |              |
#     cost_facilties    | double precision      |           | plain    |              |
#     cost_wheels       | double precision      |           | plain    |              |
#     cost_electric     | double precision      |           | plain    |              |
#     cost_misc_eqpmnt  | double precision      |           | plain    |              |
#     cost_roads        | double precision      |           | plain    |              |
#     asset_retire_cost | double precision      |           | plain    |              |
#     cost_of_plant     | double precision      |           | plain    |              |
#     cost_per_kw       | double precision      |           | plain    |              |
#     expns_operations  | double precision      |           | plain    |              |
#     expns_water_pwr   | double precision      |           | plain    |              |
#     expns_pump_strg   | double precision      |           | plain    |              |
#     expns_electric    | double precision      |           | plain    |              |
#     expns_misc_power  | double precision      |           | plain    |              |
#     expns_rents       | double precision      |           | plain    |              |
#     expns_engneering  | double precision      |           | plain    |              |
#     expns_structures  | double precision      |           | plain    |              |
#     expns_dams        | double precision      |           | plain    |              |
#     expns_plant       | double precision      |           | plain    |              |
#     expns_misc_plnt   | double precision      |           | plain    |              |
#     expns_producton   | double precision      |           | plain    |              |
#     pumping_expenses  | double precision      |           | plain    |              |
#     tot_prdctn_exns   | double precision      |           | plain    |              |
#     expns_kwh         | double precision      |           | plain    |              |
#     #report_prd        | integer               | not null  | plain    |              |
