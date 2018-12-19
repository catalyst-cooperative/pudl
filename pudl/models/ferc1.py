"""Database models for PUDL tables derived from FERC Form 1 data."""

from sqlalchemy import Column, ForeignKey, Integer, String, Float, Numeric
from sqlalchemy import Enum
from sqlalchemy import ForeignKeyConstraint
import pudl.models.entities
import pudl.constants as pc

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
        ['utility_id_ferc1', 'plant_name'],
        ['plants_ferc.utility_id_ferc1', 'plants_ferc.plant_name']),)
    # Each year, for each fuel, there's one report for each plant, which may
    # be recorded multiple times for multiple utilities that have a stake in
    # the plant... Primary key fields: utility, plant, fuel and year.
    id = Column(
        Integer,
        autoincrement=True,
        primary_key=True,
        comment='PUDL issued surrogate key.'
    )
    plant_id_ferc1 = Column(
        Integer,
        comment='PUDL issued FERC Plant ID. Not stable between sessions.'
    )
    record_id = Column(
        String,
        nullable=False,
        comment="Identifier indicating original FERC Form 1 source record. format: {report_year}_{respondent_id}_{spplmnt_num}_{row_number}. Unique within each FERC Form 1 DB table."
    )
    utility_id_ferc1 = Column(
        Integer,
        nullable=False,
        comment="FERC assinged respondent_id, identifying the reporting entity. Stable from year to year."
    )
    plant_name = Column(
        String,
        nullable=False,
        comment="Name of the plant, as reported to FERC. This is a freeform string, not guaranteed to be consistent across references to the same plant"
    )
    report_year = Column(
        Integer,
        nullable=False,
        comment="Year in which the data was reported."
    )
    fuel_type_code_pudl = Column(
        Enum(*pc.ferc1_fuel_strings.keys(),
             name='ferc1_pudl_fuel_codes'),
        comment="PUDL assigned code indicating the general fuel type."
    )
    fuel_unit = Column(
        Enum(*pc.ferc1_fuel_unit_strings.keys(),
             name='ferc1_pudl_fuel_unit_codes'),
        comment="PUDL assigned code indicating reported fuel unit of measure."
    )
    fuel_qty_burned = Column(
        Float,
        nullable=False,
        comment="Quantity of fuel consumed in the report year, in terms of the reported fuel units."
    )
    fuel_mmbtu_per_unit = Column(
        Float,
        nullable=False,
        comment="Average heat content of fuel consumed in the report year, in mmBTU per reported fuel unit."
    )
    fuel_cost_per_unit_burned = Column(
        Float,
        nullable=False,
        comment="Average cost of fuel consumed in the report year, in nominal USD per reported fuel unit."
    )
    fuel_cost_per_unit_delivered = Column(
        Float,
        nullable=False,
        comment="Average cost of fuel delivered in the report year, in nominal USD per reported fuel unit."
    )
    fuel_cost_per_mmbtu = Column(
        Float,
        nullable=False,
        comment="Average cost of fuel consumed in the report year, in nominal USD per mmBTU of fuel heat content."
    )
    # Is this a useful number for any fuel that's not overwhelmingly dominant?
    fuel_cost_per_mwh = Column(
        Float,
        nullable=False,
        comment="Average cost of fuel burned per MWh of net generation in the report year."
    )
    # Is this a useful number for any fuel that's not overwhelmingly dominant?
    fuel_mmbtu_per_mwh = Column(
        Float,
        nullable=False,
        comment="Average heat content in mmBTU of fuel consumed per MWh of net generation in the report year."
    )


class PlantSteamFERC1(pudl.models.entities.PUDLBase):
    """A large thermal generating plant, as reported to FERC on Form 1."""

    __tablename__ = 'plants_steam_ferc1'
    __table_args__ = (ForeignKeyConstraint(
        ['utility_id_ferc1', 'plant_name'],
        ['plants_ferc.utility_id_ferc1', 'plants_ferc.plant_name']),)

    id = Column(
        Integer,
        autoincrement=True,
        primary_key=True,
        comment="PUDL generated surrogate key."
    )
    record_id = Column(
        String,
        nullable=False,
        comment="Identifier indicating original FERC Form 1 source record. format: {report_year}_{respondent_id}_{spplmnt_num}_{row_number}. Unique within each FERC Form 1 DB table."
    )
    utility_id_ferc1 = Column(
        Integer,
        nullable=False,
        comment="FERC assinged respondent_id, identifying the reporting entity. Stable from year to year."
    )
    plant_id_ferc1 = Column(
        Integer,
        comment='PUDL issued FERC Plant ID. Not stable between sessions.'
    )
    plant_name = Column(
        String,
        nullable=False,
        comment="Name of the plant, as reported to FERC. This is a freeform string, not guaranteed to be consistent across references to the same plant"
    )
    report_year = Column(
        Integer,
        nullable=False,
        comment="Year in which the data was reported."
    )
    plant_type = Column(
        Enum(*pc.ferc1_plant_kind_strings, name='ferc1_plant_kind'),
        comment="Simplified plant type, categorized by PUDL based on our best guess of what was intended based on freeform string reported to FERC. Unidentifiable types are null."
    )
    construction_type = Column(
        Enum(*pc.ferc1_construction_type_strings,
             name='ferc1_construction_type'),
        comment="Type of plant construction. Categorized by PUDL based on our best guess of intended value in FERC1 freeform strings."
    )
    construction_year = Column(
        Integer,
        comment="Year the plant's oldest still operational unit was built."
    )
    installation_year = Column(
        Integer,
        comment="Year the plant's most recently built unit was installed."
    )
    capacity_mw = Column(
        Float,
        comment="Total installed plant capacity in MW."
    )
    peak_demand_mw = Column(
        Float,
        comment="Net peak demand experienced by the plant in MW in report year."
    )
    plant_hours_connected_while_generating = Column(
        Float,
        comment="Total number hours the plant was generated and connected to load during report year."
    )
    plant_capability_mw = Column(
        Float,
        comment="Net continuous plant capability in MW"
    )
    water_limited_capacity_mw = Column(
        Float,
        comment="Plant capacity in MW when limited by condenser water."
    )
    not_water_limited_capacity_mw = Column(
        Float,
        comment="Plant capacity in MW when not limited by condenser water."
    )
    avg_num_employees = Column(
        Float,
        comment="Average number of plant employees during report year."
    )
    net_generation_mwh = Column(
        Float,
        comment="Net generation (exclusive of plant use) in MWh during report year."
    )
    capex_land = Column(
        Numeric(14, 2),
        comment="Capital expense for land and land rights."
    )
    capex_structure = Column(
        Numeric(14, 2),
        comment="Capital expense for structures and improvements."
    )
    capex_equipment = Column(
        Numeric(14, 2),
        comment="Capital expense for equipment."
    )
    capex_total = Column(
        Numeric(14, 2),
        comment="Total capital expenses."
    )
    capex_per_mw = Column(
        Numeric(14, 2),
        comment="Capital expenses per MW of installed plant capacity."
    )
    opex_operations = Column(
        Numeric(14, 2),
        comment="Production expenses: operations, supervision, and engineering."
    )
    opex_fuel = Column(
        Numeric(14, 2),
        comment="Total cost of fuel."
    )
    opex_coolants = Column(
        Numeric(14, 2),
        comment="Cost of coolants and water (nuclear plants only)"
    )
    opex_steam = Column(
        Numeric(14, 2),
        comment="Steam expenses."
    )
    opex_steam_other = Column(
        Numeric(14, 2),
        comment="Steam from other sources."
    )
    opex_transfer = Column(
        Numeric(14, 2),
        comment="Steam transferred (Credit)."
    )
    opex_electric = Column(
        Numeric(14, 2),
        comment="Electricity expenses."
    )
    opex_misc_power = Column(
        Numeric(14, 2),
        comment="Miscellaneous steam (or nuclear) expenses."
    )
    opex_rents = Column(
        Numeric(14, 2),
        comment="Rents."
    )
    opex_allowances = Column(
        Numeric(14, 2),
        comment="Allowances."
    )
    opex_engineering = Column(
        Numeric(14, 2),
        comment="Maintenance, supervision, and engineering."
    )
    opex_structures = Column(
        Numeric(14, 2),
        comment="Maintenance of structures."
    )
    opex_boiler = Column(
        Numeric(14, 2),
        comment="Maintenance of boiler (or reactor) plant."
    )
    opex_plants = Column(
        Numeric(14, 2),
        comment="Maintenance of electrical plant."
    )
    opex_misc_steam = Column(
        Numeric(14, 2),
        comment="Maintenance of miscellaneous steam (or nuclear) plant."
    )
    opex_production_total = Column(
        Numeric(14, 2),
        comment="Total operating epxenses."
    )
    opex_per_mwh = Column(
        Numeric(14, 2),
        comment="Total operating expenses per MWh of net generation."
    )
    asset_retirement_cost = Column(
        Numeric(14, 2),
        comment="Asset retirement cost."
    )


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
    utility_id_ferc1 = Column(Integer,
                              ForeignKey('utilities_ferc.utility_id_ferc1'),
                              primary_key=True)
    report_year = Column(Integer,
                         primary_key=True)
    ferc_account_id = Column(String,
                             ForeignKey('ferc_accounts.id'),
                             primary_key=True)
    record_id = Column(String, nullable=False)
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
    utility_id_ferc1 = Column(Integer,
                              ForeignKey('utilities_ferc.utility_id_ferc1'),
                              primary_key=True)
    report_year = Column(Integer,
                         primary_key=True)
    line_id = Column(String,
                     ForeignKey('ferc_depreciation_lines.id'),
                     primary_key=True)
    record_id = Column(String, nullable=False)
    total = Column(Numeric(14, 2))
    electric_plant = Column(Numeric(14, 2))
    future_plant = Column(Numeric(14, 2))
    leased_plant = Column(Numeric(14, 2))


class PurchasedPowerFERC1(pudl.models.entities.PUDLBase):
    """Utility power purchase data, from FERC1 DB f1_purchased_pwr table."""

    __tablename__ = 'purchased_power_ferc1'
    id = Column(Integer, autoincrement=True, primary_key=True)
    utility_id_ferc1 = Column(Integer,
                              ForeignKey('utilities_ferc.utility_id_ferc1'),
                              nullable=False)
    record_id = Column(String, nullable=False)
    report_year = Column(Integer, nullable=False)
    authority_company_name = Column(String)
    statistical_classification = Column(String)
    rate_schedule_tariff_num = Column(String)
    avg_billing_demand_mw = Column(String)
    avg_monthly_ncp_demand_mw = Column(String)
    avg_monthly_cp_demand_mw = Column(String)
    purchased_mwh = Column(Numeric(14, 2))
    received_mwh = Column(Numeric(14, 2))
    delivered_mwh = Column(Numeric(14, 2))
    demand_charges = Column(Numeric(14, 2))
    energy_charges = Column(Numeric(14, 2))
    other_charges = Column(Numeric(14, 2))
    total_settlement = Column(Numeric(14, 2))


class PlantSmallFERC1(pudl.models.entities.PUDLBase):
    """
    Annual data on "small plants" imported from the f1_gnrt_plant table.

    Capacity and generation data related to a heterogenous collection of small
    plants that have to report to FERC. Includes many renewable energy
    facilities, as well as smaller thermal generation stations.
    """

    __tablename__ = 'plants_small_ferc1'
    __table_args__ = (ForeignKeyConstraint(
        ['utility_id_ferc1', 'plant_name_original'],
        ['plants_ferc.utility_id_ferc1', 'plants_ferc.plant_name']),)

    id = Column(Integer, autoincrement=True, primary_key=True)
    record_id = Column(String, nullable=False)
    utility_id_ferc1 = Column(Integer, nullable=False)
    report_year = Column(Integer, nullable=False)
    plant_name_original = Column(String, nullable=False)
    plant_name = Column(String)
    plant_type = Column(String)
    ferc_license_id = Column(Integer)
    construction_year = Column(Integer)
    capacity_mw = Column(Float)
    peak_demand_mw = Column(Float)
    net_generation_mwh = Column(Float)
    total_cost_of_plant = Column(Numeric(14, 2))
    capex_per_mw = Column(Numeric(14, 2))
    opex_total = Column(Numeric(14, 2))  # No idea what this field is.
    opex_fuel = Column(Numeric(14, 2))
    opex_maintenance = Column(Numeric(14, 2))
    fuel_type = Column(String)
    fuel_cost_per_mmbtu = Column(Numeric(14, 2))


class PlantHydroFERC1(pudl.models.entities.PUDLBase):
    """Annual data on hydro plants from FERC form 1."""

    __tablename__ = 'plants_hydro_ferc1'
    __table_args__ = (ForeignKeyConstraint(
        ['utility_id_ferc1', 'plant_name'],
        ['plants_ferc.utility_id_ferc1', 'plants_ferc.plant_name']),)

    id = Column(Integer, autoincrement=True, primary_key=True)
    record_id = Column(String, nullable=False)
    utility_id_ferc1 = Column(Integer, nullable=False)
    plant_name = Column(String, nullable=False)
    report_year = Column(Integer, nullable=False)
    project_num = Column(Integer)
    plant_type = Column(String)  # FK
    plant_construction_type = Column(String)
    construction_year = Column(Integer)
    installation_year = Column(Integer)
    # name plate capacity
    capacity_mw = Column(Float)
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
    capex_land = Column(Numeric(14, 2))
    # Structures and Improvements
    capex_structure = Column(Numeric(14, 2))
    # Reservoirs, Dams, and Waterways
    capex_facilities = Column(Numeric(14, 2))
    # Equipment Costs
    capex_equipment = Column(Numeric(14, 2))
    # Roads, Railroads, and Bridges
    capex_roads = Column(Numeric(14, 2))
    # Asset Retirement Costs
    asset_retirement_cost = Column(Numeric(14, 2))
    # TOTAL cost (Total of 14 thru 19)
    capex_total = Column(Numeric(14, 2))
    # Cost per KW of Installed Capacity (line 20 / 5)
    # Converted to MW installed...
    capex_per_mw = Column(Numeric(14, 2))
    # O peration Supervision and Engineering
    opex_operations = Column(Numeric(14, 2))
    # Water for Power
    opex_water_pwr = Column(Numeric(14, 2))
    # Hydraulic Expenses
    opex_hydraulic = Column(Numeric(14, 2))
    # Electric Expenses
    opex_electric = Column(Numeric(14, 2))
    # Misc Hydraulic Power Generation Expenses
    opex_generation = Column(Numeric(14, 2))
    # Rents
    opex_rents = Column(Numeric(14, 2))
    # Maintenance Supervision and Engineering
    opex_engineering = Column(Numeric(14, 2))
    # Maintenance of Structures
    opex_structures = Column(Numeric(14, 2))
    # Maintenance of Reservoirs, Dams, and Waterways
    opex_dams = Column(Numeric(14, 2))
    # Maintenance of Electric Plant
    opex_plant = Column(Numeric(14, 2))
    # Maintenance of Misc Hydraulic Plant
    opex_misc_plant = Column(Numeric(14, 2))
    # Total Production Expenses (total 23 thru 33)
    opex_total = Column(Numeric(14, 2))
    # Expenses per net KWh in FERC, converted to MWh in PUDL
    opex_per_mwh = Column(Numeric(14, 2))


class PlantsPumpedStorage(pudl.models.entities.PUDLBase):
    """Annual data on pumped storage from the f1_pumped_storage table."""

    __tablename__ = 'plants_pumped_storage_ferc1'
    __table_args__ = (ForeignKeyConstraint(
        ['utility_id_ferc1', 'plant_name'],
        ['plants_ferc.utility_id_ferc1', 'plants_ferc.plant_name']),)

    id = Column(Integer, autoincrement=True, primary_key=True)
    utility_id_ferc1 = Column(Integer, nullable=False)
    record_id = Column(String, nullable=False)
    report_year = Column(Integer, nullable=False)
    project_num = Column(Integer)
    plant_name = Column(String, nullable=False)
    plant_kind = Column(String)
    construction_year = Column(Integer)
    installation_year = Column(Integer)
    # Total installed cap (Gen name plate Rating in MW)
    capacity_mw = Column(Float)
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
    capex_land = Column(Numeric(14, 2))
    # Structures and Improvements
    capex_structures = Column(Numeric(14, 2))
    # Reservoirs, Dams, and Waterways
    capex_facilities = Column(Numeric(14, 2))
    # Water Wheels, Turbines, and Generators
    capex_wheels_turbines_generators = Column(Numeric(14, 2))
    # Accessory Electric Equipment
    capex_equipment_electric = Column(Numeric(14, 2))
    # Miscellaneous Powerplant Equipment
    capex_equipment_misc = Column(Numeric(14, 2))
    # Roads, Railroads, and Bridges
    capex_roads = Column(Numeric(14, 2))
    # Asset Retirement Costs
    asset_retirement_cost = Column(Numeric(14, 2))
    # Total cost (total 13 thru 20)
    capex_plant_total = Column(Numeric(14, 2))
    # Cost per KW of installed cap (line 21 / 4), converted to MW
    capex_per_mw = Column(Numeric(14, 2))
    # Operation Supervision and Engineering
    opex_operations = Column(Numeric(14, 2))
    # Water for Power
    opex_water_for_power = Column(Numeric(14, 2))
    # Pumped Storage Expenses
    opex_pumped_storage = Column(Numeric(14, 2))
    # Electric Expenses
    opex_electric = Column(Numeric(14, 2))
    # Misc Pumped Storage Power generation Expenses
    opex_generation_misc = Column(Numeric(14, 2))
    # Rents
    opex_rents = Column(Numeric(14, 2))
    # Maintenance Supervision and Engineering
    opex_engineering = Column(Numeric(14, 2))
    # Maintenance of Structures
    opex_structures = Column(Numeric(14, 2))
    # Maintenance of Reservoirs, Dams, and Waterways
    opex_dams = Column(Numeric(14, 2))
    # Maintenance of Electric Plant
    opex_plant = Column(Numeric(14, 2))
    # Maintenance of Misc Pumped Storage Plant
    opex_plant_misc = Column(Numeric(14, 2))
    # Production Exp Before Pumping Exp (24 thru 34)
    opex_production_before_pumping = Column(Numeric(14, 2))
    # Pumping Expenses
    opex_pumping = Column(Numeric(14, 2))
    # Total Production Exp (total 35 and 36)
    opex_total = Column(Numeric(14, 2))
    # Expenses per KWh (line 37 / 9), converted to mwh
    opex_per_mwh = Column(Numeric(14, 2))
