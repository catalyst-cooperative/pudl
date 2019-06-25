"""Database models for PUDL tables derived from FERC Form 1 data."""

from sqlalchemy import Column, ForeignKey, Integer, String, Float, Numeric
from sqlalchemy import Enum
from sqlalchemy import ForeignKeyConstraint
import pudl.models.entities
import pudl.constants as pc

###########################################################################
# Tables comprising data from the FERC f1_steam & f1_fuel tables
###########################################################################

construction_type_enum = Enum(*pc.ferc1_const_type_strings.keys(),
                              name='ferc1_construction_type')

id_comment = "PUDL issued surrogate key."
record_id_comment = "Identifier indicating original FERC Form 1 source record. format: {report_year}_{respondent_id}_{spplmnt_num}_{row_number}. Unique within each FERC Form 1 DB table."
utility_id_ferc1_comment = "FERC assigned respondent_id, identifying the reporting entity. Stable from year to year."
report_year_comment = "Four-digit year in which the data was reported."
plant_id_ferc1_comment = "Algorithmically assigned PUDL FERC Plant ID. WARNING: NOT STABLE BETWEEN PUDL DB INITIALIZATIONS."
plant_name_comment = "Name of the plant, as reported to FERC. This is a freeform string, not guaranteed to be consistent across references to the same plant."
construction_type_comment = "Type of plant construction ('outdoor' or 'conventional'). Categorized by PUDL based on our best guess of intended value in FERC1 freeform strings."


class FuelFERC1(pudl.models.entities.PUDLBase):
    """
    Annual fuel consumed by plant, as reported to FERC in Form 1.

    This information comes from the f1_fuel table in the FERC DB, which is
    populated from page 402 of the paper FERC Form 1.
    """

    __tablename__ = 'fuel_ferc1'
    __table_args__ = (
        ForeignKeyConstraint(
            ['utility_id_ferc1', 'plant_name'],
            ['plants_ferc.utility_id_ferc1', 'plants_ferc.plant_name']),
        {'comment': "Annual fuel consumed by large thermal generating plants. As reported on page 402 of FERC Form 1."}
    )
    # Each year, for each fuel, there's one report for each plant, which may
    # be recorded multiple times for multiple utilities that have a stake in
    # the plant... Primary key fields: utility, plant, fuel and year.
    id = Column(Integer, autoincrement=True,
                primary_key=True, comment=id_comment)
    record_id = Column(String, nullable=False, comment=record_id_comment)
    utility_id_ferc1 = Column(Integer, nullable=False,
                              comment=utility_id_ferc1_comment)
    report_year = Column(Integer, nullable=False, comment=report_year_comment)

    plant_name = Column(String, nullable=False, comment=plant_name_comment)

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
    # fuel_cost_per_mwh = Column(
    #    Float,
    #    nullable=False,
    #    comment="Average cost of fuel burned per MWh of net generation in the report year. In plants burning multiple fuels, this may not be indicative of overall fuel cost per MWh."
    # )
    # Is this a useful number for any fuel that's not overwhelmingly dominant?
    # fuel_mmbtu_per_mwh = Column(
    #    Float,
    #    nullable=False,
    #    comment="Average heat content in mmBTU of fuel consumed per MWh of net generation in the report year. In plants burning multiple fuels this may not be indicative of overall plant heat rate."
    # )
#


class PlantSteamFERC1(pudl.models.entities.PUDLBase):
    """A large thermal generating plant, as reported to FERC on Form 1."""

    __tablename__ = 'plants_steam_ferc1'
    __table_args__ = (
        ForeignKeyConstraint(
            ['utility_id_ferc1', 'plant_name'],
            ['plants_ferc.utility_id_ferc1', 'plants_ferc.plant_name']),
        {'comment': "Large thermal generating plants, as reported on page 402 of FERC Form 1."}
    )

    id = Column(Integer, autoincrement=True,
                primary_key=True, comment=id_comment)
    record_id = Column(String, nullable=False, comment=record_id_comment)
    utility_id_ferc1 = Column(Integer, nullable=False,
                              comment=utility_id_ferc1_comment)
    report_year = Column(Integer, nullable=False, comment=report_year_comment)

    plant_id_ferc1 = Column(Integer, comment=plant_id_ferc1_comment)
    plant_name = Column(String, nullable=False, comment=plant_name_comment)

    plant_type = Column(
        Enum(*pc.ferc1_plant_kind_strings, name='ferc1_plant_kind'),
        comment="Simplified plant type, categorized by PUDL based on our best guess of what was intended based on freeform string reported to FERC. Unidentifiable types are null."
    )
    construction_type = Column(
        construction_type_enum,  # Enum, see top of this file
        comment=construction_type_comment
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
    capex_structures = Column(
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
    __table_args__ = ({'comment': "Balances and changes to FERC Electric Plant in Service accounts, as reported on FERC Form 1. Data originally from the f1_plant_in_srvce table in FERC's FoxPro database. Account numbers correspond to the FERC Uniform System of Accounts for Electric Plant, which is defined in Code of Federal Regulations (CFR) Title 18, Chapter I, Subchapter C, Part 101. (See e.g. https://www.law.cornell.edu/cfr/text/18/part-101). Each FERC respondent reports starting and ending balances for each account annually. Balances are organization wide, and are not broken down on a per-plant basis. End of year balance should equal beginning year balance plus the sum of additions, retirements, adjustments, and transfers."})
    utility_id_ferc1 = Column(
        Integer,
        ForeignKey('utilities_ferc.utility_id_ferc1'),
        primary_key=True,
        comment=utility_id_ferc1_comment
    )
    report_year = Column(
        Integer,
        primary_key=True,
        comment=report_year_comment
    )
    ferc_account_id = Column(
        String,
        ForeignKey('ferc_accounts.id'),
        primary_key=True,
        comment="Identification number associated with each FERC account, as described in the FERC Uniform System of Accounts for Electric Plant."
    )
    record_id = Column(String, nullable=False, comment=record_id_comment)
    beginning_year_balance = Column(
        Numeric(14, 2),
        comment="Balance in the associated account at the beginning of the year. Nominal USD."
    )
    additions = Column(
        Numeric(14, 2),
        comment="Change in account balance during the year due to additions. Nominal USD."
    )
    retirements = Column(
        Numeric(14, 2),
        comment="Change in account balance during the year due to retirements. Nominal USD."
    )
    adjustments = Column(
        Numeric(14, 2),
        comment="Change in account balance during the year due to adjustments. Nominal USD."
    )
    transfers = Column(
        Numeric(14, 2),
        comment="Change in account balance during the year due to transfers. Nominal USD."
    )
    year_end_balance = Column(
        Numeric(14, 2),
        comment="Balance in the associated account at the end of the year. Nominal USD."
    )


class AccumulatedDepreciationFERC1(pudl.models.entities.PUDLBase):
    """
    Balances and changes to FERC Accumulated Provision for Depreciation.

    This data comes from the f1_accumdepr_prvsn table in the ferc1 DB.
    """

    __tablename__ = 'accumulated_depreciation_ferc1'
    __table_args__ = (
        {'comment': "Balances and changes to FERC Accumulated Provision for Depreciation."})
    utility_id_ferc1 = Column(
        Integer,
        ForeignKey('utilities_ferc.utility_id_ferc1'),
        primary_key=True,
        comment="FERC assinged respondent_id, identifying the reporting entity. Stable from year to year."
    )
    report_year = Column(
        Integer,
        primary_key=True,
        comment="Four-digit year in which the data was reported."
    )
    record_id = Column(
        String,
        nullable=False,
        comment="Identifier indicating original FERC Form 1 source record. format: {report_year}_{respondent_id}_{spplmnt_num}_{row_number}. Unique within each FERC Form 1 DB table."
    )
    line_id = Column(
        String,
        ForeignKey('ferc_depreciation_lines.id'),
        primary_key=True,
        comment="Line numbers, and corresponding FERC account number from FERC Form 1, page 2019, Accumulated Provision for Depreciation of Electric Utility Plant (Account 108)."
    )
    total = Column(
        Numeric(14, 2),
        comment="Total of Electric Plant In Service, Electric Plant Held for Future Use, and Electric Plant Leased to Others. Nominal USD."
    )
    electric_plant = Column(
        Numeric(14, 2),
        comment="Electric Plant In Service. Nominal USD."
    )
    future_plant = Column(
        Numeric(14, 2),
        comment="Electric Plant Held for Future Use. Nominal USD."
    )
    leased_plant = Column(
        Numeric(14, 2),
        comment="Electric Plant Leased to Others. Nominal USD."
    )


class PurchasedPowerFERC1(pudl.models.entities.PUDLBase):
    """Utility power purchase data, from FERC1 DB f1_purchased_pwr table."""

    __tablename__ = 'purchased_power_ferc1'
    __table_args__ = ({"comment": "Purchased Power (Account 555) including power exchanges (i.e. transactions involving a balancing of debits and credits for energy, capacity, etc.) and any settlements for imbalanced exchanges. Reported on pages 326-327 of FERC Form 1. Extracted from the f1_purchased_pwr table in FERC's FoxPro database. "})

    id = Column(Integer, autoincrement=True,
                primary_key=True, comment=id_comment)
    record_id = Column(String, nullable=False, comment=record_id_comment)
    utility_id_ferc1 = Column(Integer, nullable=False,
                              comment=utility_id_ferc1_comment)
    report_year = Column(Integer, nullable=False, comment=report_year_comment)

    seller_name = Column(
        String,
        comment="Name of the seller, or the other party in an exchange transaction."
    )
    purchase_type = Column(
        Enum(*pc.ferc1_power_purchase_type.values(),
             name="ferc1_power_purchase_type"),
        comment="Categorization based on the original contractual terms and conditions of the service. Must be one of 'requirements', 'long_firm', 'intermediate_firm', 'short_firm', 'long_unit', 'intermediate_unit', 'electricity_exchange', 'other_service', or 'adjustment'. Requirements service is ongoing high reliability service, with load integrated into system resource planning. 'Long term' means 5+ years. 'Intermediate term' is 1-5 years. 'Short term' is less than 1 year. 'Firm' means not interruptible for economic reasons. 'unit' indicates service from a particular designated generating unit. 'exchange' is an in-kind transaction."
    )
    tariff = Column(
        String,
        comment="FERC Rate Schedule Number or Tariff. (Note: may be incomplete if originally reported on multiple lines.)"
    )
    billing_demand_mw = Column(
        Float,
        comment="Monthly average billing demand (for requirements purchases, and any transactions involving demand charges). In megawatts."
    )
    non_coincident_peak_demand_mw = Column(
        Float,
        comment="Average monthly non-coincident peak (NCP) demand (for requirements purhcases, and any transactions involving demand charges). Monthly NCP demand is the maximum metered hourly (60-minute integration) demand in a month. In megawatts."
    )
    coincident_peak_demand_mw = Column(
        Float,
        comment="Average monthly coincident peak (CP) demand (for requirements purchases, and any transactions involving demand charges). Monthly CP demand is the metered demand during the hour (60-minute integration) in which the supplier's system reaches its monthly peak. In megawatts."
    )
    purchased_mwh = Column(
        Float,
        comment="Megawatt-hours shown on bills rendered to the respondent."
    )
    received_mwh = Column(
        Float,
        comment="Gross megawatt-hours received in power exchanges and used as the basis for settlement."
    )
    delivered_mwh = Column(
        Float,
        comment="Gross megawatt-hours delivered in power exchanges and used as the basis for settlement."
    )
    demand_charges = Column(
        Numeric(14, 2),
        comment="Demand charges. Nominal USD."
    )
    energy_charges = Column(
        Numeric(14, 2),
        comment="Energy charges. Nominal USD."
    )
    other_charges = Column(
        Numeric(14, 2),
        comment="Other charges, including out-of-period adjustments. Nominal USD."
    )
    total_settlement = Column(
        Numeric(14, 2),
        comment="Sum of demand, energy, and other charges. For power exchanges, the settlement amount for the net receipt of energy. If more energy was delivered than received, this amount is negative. Nominal USD."
    )


class PlantSmallFERC1(pudl.models.entities.PUDLBase):
    """
    Annual data on "small plants" imported from the f1_gnrt_plant table.

    Capacity and generation data related to a heterogenous collection of small
    plants that have to report to FERC. Includes many renewable energy
    facilities, as well as smaller thermal generation stations.
    """

    __tablename__ = 'plants_small_ferc1'
    __table_args__ = (
        ForeignKeyConstraint(
            ['utility_id_ferc1', 'plant_name_original'],
            ['plants_ferc.utility_id_ferc1', 'plants_ferc.plant_name']),
        {"comment": "Generating plant statistics for small plants, as reported on FERC Form 1 pages 410-411, and extracted from the FERC FoxPro database table f1_gnrt_plant. Small generating plants are defined by having nameplate capacity of less than 25MW for steam plants, and less than 10MW for internal combustion, conventional hydro, and pumped storage plants."}
    )
    id = Column(Integer, autoincrement=True,
                primary_key=True, comment=id_comment)
    record_id = Column(String, nullable=False, comment=record_id_comment)
    utility_id_ferc1 = Column(Integer, nullable=False,
                              comment=utility_id_ferc1_comment)
    report_year = Column(Integer, nullable=False, comment=report_year_comment)

    plant_name_original = Column(
        String,
        nullable=False,
        comment="Original plant name in the FERC Form 1 FoxPro database.",
    )
    plant_name = Column(
        String,
        comment="PUDL assigned simplified plant name.",
    )
    plant_type = Column(
        String,
        comment="PUDL assigned plant type. This is a best guess based on the fuel type, plant name, and other attributes.",
    )
    ferc_license_id = Column(
        Integer,
        comment="FERC issued operating license ID for the facility, if available. This value is extracted from the original plant name where possible.",
    )
    construction_year = Column(
        Integer,
        comment="Original year of plant construction.",
    )
    capacity_mw = Column(
        Float,
        comment="Name plate capacity in megawatts.",
    )
    peak_demand_mw = Column(
        Float,
        comment="Net peak demand for 60 minutes. Note: in some cases peak demand for other time periods may have been reported instead, if hourly peak demand was unavailable.",
    )
    net_generation_mwh = Column(
        Float,
        comment="Net generation excluding plant use, in megawatt-hours.",
    )
    total_cost_of_plant = Column(
        Numeric(14, 2),
        comment="Total cost of plant. Nominal USD.",
    )
    capex_per_mw = Column(
        Numeric(14, 2),
        comment="Plant costs (including asset retirement costs) per megawatt. Nominal USD. ",
    )
    opex_total = Column(
        Numeric(14, 2),
        comment="Total plant operating expenses, excluding fuel. Nominal USD.",
    )
    opex_fuel = Column(
        Numeric(14, 2),
        comment="Production expenses: Fuel. Nominal USD.",
    )
    opex_maintenance = Column(
        Numeric(14, 2),
        comment="Production expenses: Maintenance. Nominal USD.",
    )
    fuel_type = Column(
        String,
        comment="Kind of fuel. Originally reported to FERC as a freeform string. Assigned a canonical value by PUDL based on our best guess.",
    )
    fuel_cost_per_mmbtu = Column(
        Numeric(14, 2),
        comment="Average fuel cost per mmBTU (if applicable). Nominal USD.",
    )


class PlantHydroFERC1(pudl.models.entities.PUDLBase):
    """Annual data on hydro plants from FERC form 1."""

    __tablename__ = 'plants_hydro_ferc1'
    __table_args__ = (
        ForeignKeyConstraint(
            ['utility_id_ferc1', 'plant_name'],
            ['plants_ferc.utility_id_ferc1', 'plants_ferc.plant_name']),
        {"comment": "Hydroelectric generating plant statistics for large plants. Large plants have an installed nameplate capacity of more than 10 MW. As reported on FERC Form 1, pages 406-407, and extracted from the f1_hydro table in FERC's FoxPro database."}
    )

    id = Column(Integer, autoincrement=True,
                primary_key=True, comment=id_comment)
    record_id = Column(String, nullable=False, comment=record_id_comment)
    utility_id_ferc1 = Column(Integer, nullable=False,
                              comment=utility_id_ferc1_comment)
    report_year = Column(Integer, nullable=False, comment=report_year_comment)

    plant_name = Column(
        String,
        nullable=False,
        comment=plant_name_comment
    )
    project_num = Column(
        Integer,
        comment="FERC Licensed Project Number."
    )
    plant_type = Column(
        String,
        comment="Kind of plant (Run-of-River or Storage)."
    )
    construction_type = Column(
        construction_type_enum,  # Enum, see top of this file
        comment=construction_type_comment
    )
    construction_year = Column(
        Integer,
        comment="Four digit year of the plant's original construction."
    )
    installation_year = Column(
        Integer,
        comment="Four digit year in which the last unit was installed."
    )
    capacity_mw = Column(
        Float,
        comment="Total installed (nameplate) capacity, in megawatts."
    )
    peak_demand_mw = Column(
        Float,
        comment="Net peak demand on the plant (60-minute integration), in megawatts."
    )
    plant_hours_connected_while_generating = Column(
        Float,
        comment="Hours the plant was connected to load while generating."
    )
    net_capacity_favorable_conditions_mw = Column(
        Float,
        comment="Net plant capability under the most favorable operating conditions, in megawatts."
    )
    net_capacity_adverse_conditions_mw = Column(
        Float,
        comment="Net plant capability under the least favorable operating conditions, in megawatts."
    )
    avg_num_employees = Column(
        Float,
        comment="Average number of employees."
    )
    net_generation_mwh = Column(
        Float,
        comment="Net generation, exclusive of plant use, in megawatt hours."
    )
    capex_land = Column(
        Numeric(14, 2),
        comment="Cost of plant: land and land rights. Nominal USD."
    )
    capex_structures = Column(
        Numeric(14, 2),
        comment="Cost of plant: structures and improvements. Nominal USD."
    )
    capex_facilities = Column(
        Numeric(14, 2),
        comment="Cost of plant: reservoirs, dams, and waterways. Nominal USD."
    )
    capex_equipment = Column(
        Numeric(14, 2),
        comment="Cost of plant: equipment. Nominal USD."
    )
    capex_roads = Column(
        Numeric(14, 2),
        comment="Cost of plant: roads, railroads, and bridges. Nominal USD."
    )
    asset_retirement_cost = Column(
        Numeric(14, 2),
        comment="Cost of plant: asset retirement costs. Nominal USD."
    )
    capex_total = Column(
        Numeric(14, 2),
        comment="Total cost of plant. Nominal USD."
    )
    capex_per_mw = Column(
        Numeric(14, 2),
        comment="Cost of plant per megawatt of installed (nameplate) capacity. Nominal USD."
    )
    opex_operations = Column(
        Numeric(14, 2),
        comment="Production expenses: operation, supervision, and engineering. Nominal USD."
    )
    opex_water_for_power = Column(
        Numeric(14, 2),
        comment="Production expenses: water for power. Nominal USD."
    )
    opex_hydraulic = Column(
        Numeric(14, 2),
        comment="Production expenses: hydraulic expenses. Nominal USD."
    )
    opex_electric = Column(
        Numeric(14, 2),
        comment="Production expenses: electric expenses. Nominal USD."
    )
    opex_generation_misc = Column(
        Numeric(14, 2),
        comment="Production expenses: miscellaneous hydraulic power generation expenses. Nominal USD."
    )
    opex_rents = Column(
        Numeric(14, 2),
        comment="Production expenses: rent. Nominal USD."
    )
    opex_engineering = Column(
        Numeric(14, 2),
        comment="Production expenses: maintenance, supervision, and engineering. Nominal USD."
    )
    opex_structures = Column(
        Numeric(14, 2),
        comment="Production expenses: maintenance of structures. Nominal USD."
    )
    opex_dams = Column(
        Numeric(14, 2),
        comment="Production expenses: maintenance of reservoirs, dams, and waterways. Nominal USD."
    )
    opex_plant = Column(
        Numeric(14, 2),
        comment="Production expenses: maintenance of electric plant. Nominal USD."
    )
    opex_misc_plant = Column(
        Numeric(14, 2),
        comment="Production expenses: maintenance of miscellaneous hydraulic plant. Nominal USD."
    )
    opex_total = Column(
        Numeric(14, 2),
        comment="Total production expenses. Nominal USD."
    )
    opex_per_mwh = Column(
        Numeric(14, 2),
        comment="Production expenses per net megawatt hour generated. Nominal USD."
    )


class PlantPumpedStorage(pudl.models.entities.PUDLBase):
    """Annual data on pumped storage from the f1_pumped_storage table."""

    __tablename__ = 'plants_pumped_storage_ferc1'
    __table_args__ = (
        ForeignKeyConstraint(
            ['utility_id_ferc1', 'plant_name'],
            ['plants_ferc.utility_id_ferc1', 'plants_ferc.plant_name']),
        {"comment": ""}
    )

    id = Column(Integer, autoincrement=True,
                primary_key=True, comment=id_comment)
    record_id = Column(String, nullable=False, comment=record_id_comment)
    utility_id_ferc1 = Column(Integer, nullable=False,
                              comment=utility_id_ferc1_comment)
    report_year = Column(Integer, nullable=False, comment=report_year_comment)

    plant_name = Column(
        String,
        nullable=False,
        comment=plant_name_comment
    )
    project_num = Column(
        Integer,
        comment="FERC Licensed Project Number."
    )
    construction_type = Column(
        construction_type_enum,  # Enum, see top of this file
        comment=construction_type_comment
    )
    construction_year = Column(
        Integer,
        comment="Four digit year of the plant's original construction."
    )
    installation_year = Column(
        Integer,
        comment="Four digit year in which the last unit was installed."
    )
    capacity_mw = Column(
        Float,
        comment="Total installed (nameplate) capacity, in megawatts."
    )
    peak_demand_mw = Column(
        Float,
        comment="Net peak demand on the plant (60-minute integration), in megawatts."
    )
    plant_hours_connected_while_generating = Column(
        Float,
        comment="Hours the plant was connected to load while generating."
    )
    plant_capability_mw = Column(
        Float,
        comment="Net plant capability in megawatts."
    )
    avg_num_employees = Column(
        Float,
        comment="Average number of employees."
    )
    net_generation_mwh = Column(
        Float,
        comment="Net generation, exclusive of plant use, in megawatt hours."
    )
    energy_used_for_pumping_mwh = Column(
        Float,
        comment="Energy used for pumping, in megawatt-hours."
    )
    net_load_mwh = Column(
        Float,
        comment="Net output for load (net generation - energy used for pumping) in megawatt-hours."
    )
    capex_land = Column(
        Numeric(14, 2),
        comment="Cost of plant: land and land rights. Nominal USD."
    )
    capex_structures = Column(
        Numeric(14, 2),
        comment="Cost of plant: structures and improvements. Nominal USD."
    )
    capex_facilities = Column(
        Numeric(14, 2),
        comment="Cost of plant: reservoirs, dams, and waterways. Nominal USD."
    )
    capex_wheels_turbines_generators = Column(
        Numeric(14, 2),
        comment="Cost of plant: water wheels, turbines, and generators. Nominal USD."
    )
    capex_equipment_electric = Column(
        Numeric(14, 2),
        comment="Cost of plant: accessory electric equipment. Nominal USD."
    )
    capex_equipment_misc = Column(
        Numeric(14, 2),
        comment="Cost of plant: miscellaneous power plant equipment. Nominal USD."
    )
    capex_roads = Column(
        Numeric(14, 2),
        comment="Cost of plant: roads, railroads, and bridges. Nominal USD."
    )
    asset_retirement_cost = Column(
        Numeric(14, 2),
        comment="Cost of plant: asset retirement costs. Nominal USD."
    )
    capex_total = Column(
        Numeric(14, 2),
        comment="Total cost of plant. Nominal USD."
    )
    capex_per_mw = Column(
        Numeric(14, 2),
        comment="Cost of plant per megawatt of installed (nameplate) capacity. Nominal USD."
    )
    opex_operations = Column(
        Numeric(14, 2),
        comment="Production expenses: operation, supervision, and engineering. Nominal USD."
    )
    opex_water_for_power = Column(
        Numeric(14, 2),
        comment="Production expenses: water for power. Nominal USD."
    )
    opex_pumped_storage = Column(
        Numeric(14, 2),
        comment="Production expenses: pumped storage. Nominal USD."
    )
    opex_electric = Column(
        Numeric(14, 2),
        comment="Production expenses: electric expenses. Nominal USD."
    )
    opex_generation_misc = Column(
        Numeric(14, 2),
        comment="Production expenses: miscellaneous pumped storage power generation expenses. Nominal USD."
    )
    opex_rents = Column(
        Numeric(14, 2),
        comment="Production expenses: rent. Nominal USD."
    )
    opex_engineering = Column(
        Numeric(14, 2),
        comment="Production expenses: maintenance, supervision, and engineering. Nominal USD."
    )
    opex_structures = Column(
        Numeric(14, 2),
        comment="Production expenses: maintenance of structures. Nominal USD."
    )
    opex_dams = Column(
        Numeric(14, 2),
        comment="Production expenses: maintenance of reservoirs, dams, and waterways. Nominal USD."
    )
    opex_plant = Column(
        Numeric(14, 2),
        comment="Production expenses: maintenance of electric plant. Nominal USD."
    )
    opex_misc_plant = Column(
        Numeric(14, 2),
        comment="Production expenses: maintenance of miscellaneous hydraulic plant. Nominal USD."
    )
    opex_production_before_pumping = Column(
        Numeric(14, 2),
        comment="Total production expenses before pumping. Nominal USD."
    )
    opex_pumping = Column(
        Numeric(14, 2),
        comment="Production expenses: We are here to PUMP YOU UP! Nominal USD."
    )
    opex_total = Column(
        Numeric(14, 2),
        comment="Total production expenses. Nominal USD."
    )
    opex_per_mwh = Column(
        Numeric(14, 2),
        comment="Production expenses per net megawatt hour generated. Nominal USD."
    )
