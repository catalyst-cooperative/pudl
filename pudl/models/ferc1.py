"""Database models for PUDL tables derived from FERC Form 1 data."""

from sqlalchemy import Column, ForeignKey, Integer, String, Float, Numeric
from sqlalchemy import ForeignKeyConstraint
import pudl.models.entities

###########################################################################
# Tables comprising data from the FERC f1_steam & f1_fuel tables
###########################################################################


class FuelFERC1(pudl.models.entities.PUDLBase):
    """
    Annual fuel consumed by plant, as reported to FERC in Form 1.

    This information comes from the f1_fuel table in the FERC DB, which is
    populated from page 402 of the paper FERC For 1.
    """

    __tablename__ = 'fuel_ferc1'
    __table_args__ = (ForeignKeyConstraint(
        ['respondent_id', 'plant_name'],
        ['plants_ferc.respondent_id', 'plants_ferc.plant_name']),)
    # Each year, for each fuel, there's one report for each plant, which may
    # be recorded multiple times for multiple utilities that have a stake in
    # the plant... Primary key fields: utility, plant, fuel and year.
    id = Column(Integer, autoincrement=True, primary_key=True)
    respondent_id = Column(Integer, nullable=False)
    plant_name = Column(String, nullable=False)  # Also ForeignKeyConstraint
    report_year = Column(Integer, nullable=False)
    # fuel_type_pudl field was ormerly fuels.name and was called 'fuel'
    fuel_type_pudl = Column(String)
    fuel_unit = Column(String, ForeignKey('fuel_units.unit'), nullable=False)
    fuel_qty_burned = Column(Float, nullable=False)
    fuel_avg_mmbtu_per_unit = Column(Float, nullable=False)
    fuel_cost_per_unit_burned = Column(Float, nullable=False)
    fuel_cost_per_unit_delivered = Column(Float, nullable=False)
    fuel_cost_per_mmbtu = Column(Float, nullable=False)
    fuel_cost_per_mwh = Column(Float, nullable=False)
    fuel_mmbtu_per_mwh = Column(Float, nullable=False)


class PlantSteamFERC1(pudl.models.entities.PUDLBase):
    """A large thermal generating plant, as reported to FERC on Form 1."""

    __tablename__ = 'plants_steam_ferc1'
    __table_args__ = (ForeignKeyConstraint(
        ['respondent_id', 'plant_name'],
        ['plants_ferc.respondent_id', 'plants_ferc.plant_name']),)

    id = Column(Integer, autoincrement=True, primary_key=True)
    respondent_id = Column(Integer, nullable=False)
    plant_name = Column(String, nullable=False)
    report_year = Column(Integer, nullable=False)
    plant_kind = Column(String)  # FK New, needs cleaning
    construction_type = Column(String)  # FK New, needs cleaning
    year_constructed = Column(Integer)
    year_installed = Column(Integer)
    total_capacity_mw = Column(Float)
    peak_demand_mw = Column(Float)
    plant_hours = Column(Float)
    plant_capability_mw = Column(Float)
    water_limited_mw = Column(Float)
    not_water_limited_mw = Column(Float)
    avg_num_employees = Column(Float)
    net_generation_mwh = Column(Float)
    cost_land = Column(Numeric(14, 2))
    cost_structure = Column(Numeric(14, 2))
    cost_equipment = Column(Numeric(14, 2))
    total_cost_of_plant = Column(Numeric(14, 2))
    cost_per_mw = Column(Numeric(14, 2))
    expns_operations = Column(Numeric(14, 2))
    expns_fuel = Column(Numeric(14, 2))
    expns_coolants = Column(Numeric(14, 2))
    expns_steam = Column(Numeric(14, 2))
    expns_steam_other = Column(Numeric(14, 2))
    expns_transfer = Column(Numeric(14, 2))
    expns_electric = Column(Numeric(14, 2))
    expns_misc_power = Column(Numeric(14, 2))
    expns_rents = Column(Numeric(14, 2))
    expns_allowances = Column(Numeric(14, 2))
    expns_engineering = Column(Numeric(14, 2))
    expns_structures = Column(Numeric(14, 2))
    expns_boiler = Column(Numeric(14, 2))
    expns_plants = Column(Numeric(14, 2))
    expns_misc_steam = Column(Numeric(14, 2))
    expns_production_total = Column(Numeric(14, 2))
    expns_per_mwh = Column(Numeric(14, 2))
    asset_retirement_cost = Column(Numeric(14, 2))


class PlantInServiceFERC1(pudl.models.entities.PUDLBase):
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
                           ForeignKey('utilities_ferc.respondent_id'),
                           primary_key=True)
    report_year = Column(Integer,
                         primary_key=True)
    ferc_account_id = Column(String,
                             ForeignKey('ferc_accounts.id'),
                             primary_key=True)
    beginning_year_balance = Column(Numeric(14, 2))
    additions = Column(Numeric(14, 2))
    retirements = Column(Numeric(14, 2))
    adjustments = Column(Numeric(14, 2))
    transfers = Column(Numeric(14, 2))
    year_end_balance = Column(Numeric(14, 2))


class AccumulatedDepreciationFERC1(pudl.models.entities.PUDLBase):
    """
    Balances and changes to FERC Accumulated Provision for Depreciation.

    This data comes from the f1_accumdepr_prvsn table in the ferc1 DB.
    """

    __tablename__ = 'accumulated_depreciation_ferc1'
    respondent_id = Column(Integer,
                           ForeignKey('utilities_ferc.respondent_id'),
                           primary_key=True)
    report_year = Column(Integer,
                         primary_key=True)
    line_id = Column(String,
                     ForeignKey('ferc_depreciation_lines.id'),
                     primary_key=True)
    total = Column(Numeric(14, 2))
    electric_plant = Column(Numeric(14, 2))
    future_plant = Column(Numeric(14, 2))
    leased_plant = Column(Numeric(14, 2))


class PurchasedPowerFERC1(pudl.models.entities.PUDLBase):
    """Utility power purchase data, from FERC1 DB f1_purchased_pwr table."""

    __tablename__ = 'purchased_power_ferc1'
    id = Column(Integer, autoincrement=True, primary_key=True)
    respondent_id = Column(Integer,
                           ForeignKey('utilities_ferc.respondent_id'),
                           nullable=False)
    report_year = Column(Integer, nullable=False)
    authority_company_name = Column(String)
    statistical_classification = Column(String)
    rate_schedule_tariff_number = Column(String)
    average_billing_demand = Column(String)
    average_monthly_ncp_demand = Column(String)
    average_monthly_cp_demand = Column(String)
    mwh_purchased = Column(Numeric(14, 2))
    mwh_received = Column(Numeric(14, 2))
    mwh_delivered = Column(Numeric(14, 2))
    demand_charges = Column(Numeric(14, 2))
    energy_charges = Column(Numeric(14, 2))
    other_charges = Column(Numeric(14, 2))
    settlement_total = Column(Numeric(14, 2))


class PlantSmallFERC1(pudl.models.entities.PUDLBase):
    """
    Annual data on "small plants" imported from the f1_gnrt_plant table.

    Capacity and generation data related to a heterogenous collection of small
    plants that have to report to FERC. Includes many renewable energy
    facilities, as well as smaller thermal generation stations.
    """

    __tablename__ = 'plants_small_ferc1'
    __table_args__ = (ForeignKeyConstraint(
        ['respondent_id', 'plant_name'],
        ['plants_ferc.respondent_id', 'plants_ferc.plant_name']),)

    id = Column(Integer, autoincrement=True, primary_key=True)
    respondent_id = Column(Integer, nullable=False)
    report_year = Column(Integer, nullable=False)
    plant_name = Column(String, nullable=False)
    plant_name_clean = Column(String)
    plant_type = Column(String)
    ferc_license = Column(Integer)
    year_constructed = Column(Integer)
    total_capacity_mw = Column(Float)
    peak_demand_mw = Column(Float)
    net_generation_mwh = Column(Float)
    total_cost_of_plant = Column(Numeric(14, 2))
    cost_of_plant_per_mw = Column(Numeric(14, 2))
    cost_of_operation = Column(Numeric(14, 2))  # No idea what this field is.
    expns_fuel = Column(Numeric(14, 2))
    expns_maintenance = Column(Numeric(14, 2))
    kind_of_fuel = Column(String)
    fuel_cost_per_mmbtu = Column(Numeric(14, 2))


class PlantHydroFERC1(pudl.models.entities.PUDLBase):
    """Annual data on hydro plants from FERC form 1."""

    __tablename__ = 'plants_hydro_ferc1'
    __table_args__ = (ForeignKeyConstraint(
        ['respondent_id', 'plant_name'],
        ['plants_ferc.respondent_id', 'plants_ferc.plant_name']),)

    id = Column(Integer, autoincrement=True, primary_key=True)
    respondent_id = Column(Integer, nullable=False)
    plant_name = Column(String, nullable=False)
    report_year = Column(Integer, nullable=False)
    project_number = Column(Integer)
    plant_kind = Column(String)  # FK
    plant_construction = Column(String)
    year_constructed = Column(Integer)
    year_installed = Column(Integer)
    # name plate capacity
    total_capacity_mw = Column(Float)
    # Net Peak Demand on Plant-Megawatts (60 minutes)
    peak_demand_mw = Column(Float)
    # Plant Hours Connect to Load
    plant_hours_connected_while_generating = Column(Float)
    # Net Plant Capability (in megawatts)
    # (a) Under Most Favorable Oper Conditions
    net_capacity_favorable_conditions_mw = Column(Float)
    # Net Plant Capability (in megawatts)
    # (b) Under the Most Adverse Oper Conditions
    net_capacity_adverse_conditions_mw = Column(Float)
    # Average Number of Employees
    avg_num_employees = Column(Float)
    # Net Generation, Exclusive of Plant Use - kWh in FERC, converted to MWh
    net_generation_mwh = Column(Float)
    # Land and Land Rights
    cost_land = Column(Numeric(14, 2))
    # Structures and Improvements
    cost_structure = Column(Numeric(14, 2))
    # Reservoirs, Dams, and Waterways
    cost_facilities = Column(Numeric(14, 2))
    # Equipment Costs
    cost_equipment = Column(Numeric(14, 2))
    # Roads, Railroads, and Bridges
    cost_roads = Column(Numeric(14, 2))
    # Asset Retirement Costs
    asset_retirement_cost = Column(Numeric(14, 2))
    # TOTAL cost (Total of 14 thru 19)
    cost_plant_total = Column(Numeric(14, 2))
    # Cost per KW of Installed Capacity (line 20 / 5)
    # Converted to MW installed...
    cost_per_mw = Column(Numeric(14, 2))
    # O peration Supervision and Engineering
    expns_operations = Column(Numeric(14, 2))
    # Water for Power
    expns_water_pwr = Column(Numeric(14, 2))
    # Hydraulic Expenses
    expns_hydraulic = Column(Numeric(14, 2))
    # Electric Expenses
    expns_electric = Column(Numeric(14, 2))
    # Misc Hydraulic Power Generation Expenses
    expns_generation = Column(Numeric(14, 2))
    # Rents
    expns_rents = Column(Numeric(14, 2))
    # Maintenance Supervision and Engineering
    expns_engineering = Column(Numeric(14, 2))
    # Maintenance of Structures
    expns_structures = Column(Numeric(14, 2))
    # Maintenance of Reservoirs, Dams, and Waterways
    expns_dams = Column(Numeric(14, 2))
    # Maintenance of Electric Plant
    expns_plant = Column(Numeric(14, 2))
    # Maintenance of Misc Hydraulic Plant
    expns_misc_plant = Column(Numeric(14, 2))
    # Total Production Expenses (total 23 thru 33)
    expns_production_total = Column(Numeric(14, 2))
    # Expenses per net KWh in FERC, converted to MWh in PUDL
    expns_per_mwh = Column(Numeric(14, 2))


class PlantsPumpedStorage(pudl.models.entities.PUDLBase):
    """Annual data on pumped storage from the f1_pumped_storage table."""

    __tablename__ = 'plants_pumped_storage_ferc1'
    __table_args__ = (ForeignKeyConstraint(
        ['respondent_id', 'plant_name'],
        ['plants_ferc.respondent_id', 'plants_ferc.plant_name']),)

    id = Column(Integer, autoincrement=True, primary_key=True)
    respondent_id = Column(Integer, nullable=False)
    report_year = Column(Integer, nullable=False)
    project_number = Column(Integer)
    plant_name = Column(String, nullable=False)
    plant_kind = Column(String)
    year_constructed = Column(Integer)
    year_installed = Column(Integer)
    # Total installed cap (Gen name plate Rating in MW)
    total_capacity_mw = Column(Float)
    # Net Peak Demaind on Plant-Megawatts (60 minutes)
    peak_demand_mw = Column(Float)
    # Plant Hours Connect to Load While Generating
    plant_hours_connected_while_generating = Column(Float)
    # Net Plant Capability (in megawatts)
    plant_capability_mw = Column(Float)
    # Average Number of Employees
    avg_num_employees = Column(Float)
    # Generation, Exclusive of Plant Use - Kwh, converted to mwh
    net_generation_mwh = Column(Float)
    # Energy Used for Pumping, converted from kwh to MWh
    energy_used_for_pumping_mwh = Column(Float)
    # Net Output for Load (line 9 - line 10) - Kwh, converted to MWh
    net_load_mwh = Column(Float)
    # Land and Land Rights
    cost_land = Column(Numeric(14, 2))
    # Structures and Improvements
    cost_structures = Column(Numeric(14, 2))
    # Reservoirs, Dams, and Waterways
    cost_facilties = Column(Numeric(14, 2))
    # Water Wheels, Turbines, and Generators
    cost_wheels_turbines_generators = Column(Numeric(14, 2))
    # Accessory Electric Equipment
    cost_equipment_electric = Column(Numeric(14, 2))
    # Miscellaneous Powerplant Equipment
    cost_equipment_misc = Column(Numeric(14, 2))
    # Roads, Railroads, and Bridges
    cost_roads = Column(Numeric(14, 2))
    # Asset Retirement Costs
    asset_retire_cost = Column(Numeric(14, 2))
    # Total cost (total 13 thru 20)
    cost_plant_total = Column(Numeric(14, 2))
    # Cost per KW of installed cap (line 21 / 4), converted to MW
    cost_per_mw = Column(Numeric(14, 2))
    # Operation Supervision and Engineering
    expns_operations = Column(Numeric(14, 2))
    # Water for Power
    expns_water_for_pwr = Column(Numeric(14, 2))
    # Pumped Storage Expenses
    expns_pump_storage = Column(Numeric(14, 2))
    # Electric Expenses
    expns_electric = Column(Numeric(14, 2))
    # Misc Pumped Storage Power generation Expenses
    expns_generation_misc = Column(Numeric(14, 2))
    # Rents
    expns_rents = Column(Numeric(14, 2))
    # Maintenance Supervision and Engineering
    expns_engneering = Column(Numeric(14, 2))
    # Maintenance of Structures
    expns_structures = Column(Numeric(14, 2))
    # Maintenance of Reservoirs, Dams, and Waterways
    expns_dams = Column(Numeric(14, 2))
    # Maintenance of Electric Plant
    expns_plant = Column(Numeric(14, 2))
    # Maintenance of Misc Pumped Storage Plant
    expns_misc_plant = Column(Numeric(14, 2))
    # Production Exp Before Pumping Exp (24 thru 34)
    expns_production_before_pumping = Column(Numeric(14, 2))
    # Pumping Expenses
    pumping_expenses = Column(Numeric(14, 2))
    # Total Production Exp (total 35 and 36)
    expns_production_total = Column(Numeric(14, 2))
    # Expenses per KWh (line 37 / 9), converted to mwh
    expns_per_mwh = Column(Numeric(14, 2))
