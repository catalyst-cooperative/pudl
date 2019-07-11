"""Database models for PUDL tables for ."""

from sqlalchemy import (Boolean, Column, Date, Float, ForeignKey, Integer,
                        String)
from sqlalchemy.ext.declarative import declarative_base

PUDLBase = declarative_base()


class DataSets(PUDLBase):
    """
    Active datasets ingest into PUDL.

    A compilation of the active or inactive datasets in the databaseself.
    """

    __tablename__ = 'datasets'
    datasource = Column(String, primary_key=True,
                        comment="The dataset from the list of datasets that can be pulled into PUDL.")
    active = Column(
        Boolean, comment="Whether or not the dataset has been pulled into PUDL by the extract transform load process.")


class UtilityEntityEIA(PUDLBase):
    """
    An EIA Utility, listed in 923 or 860.

    A compilation of all EIA utilities ids and static info.
    """

    __tablename__ = 'utilities_entity_eia'
    utility_id_eia = Column(Integer, primary_key=True, nullable=False,
                            comment="The EIA Utility Identification number.")
    utility_name = Column(String, comment="The name of the utility.")
    entity_type = Column(String, comment="Entity type of principle owner (C = Cooperative, I = Investor-Owned Utility, Q = Independent Power Producer, M = Municipally-Owned Utility, P = Political Subdivision, F = Federally-Owned Utility, S = State-Owned Utility, IND = Industrial, COM = Commercial")


class PlantEntityEIA(PUDLBase):
    """
    An EIA Plant, listed in 923 or 860.

    A compilation of all EIA plant ids and static info.
    """

    __tablename__ = 'plants_entity_eia'
    plant_id_eia = Column(Integer, primary_key=True, nullable=False,
                          comment="EIA Plant Identification number. One to five digit numeric.")
    plant_name = Column(String, comment="Plant name.")
    balancing_authority_code = Column(
        String, comment="The plant's balancing authority code.")
    balancing_authority_name = Column(
        String, comment="The plant's balancing authority name.")
    city = Column(String, comment="The plant's city.")
    county = Column(String, comment="The plant's county.")
    ferc_cogen_status = Column(
        String, comment="Indicates whether the plant has FERC qualifying facility cogenerator status.")
    ferc_exempt_wholesale_generator = Column(
        String, comment="Indicates whether the plant has FERC qualifying facility exempt wholesale generator status")
    ferc_small_power_producer = Column(
        String, comment="Indicates whether the plant has FERC qualifying facility small power producer status")
    grid_voltage_kv = Column(
        Float, comment="Plant's grid voltage at point of interconnection to transmission or distibution facilities")
    grid_voltage_2_kv = Column(
        Float, comment="Plant's grid voltage at point of interconnection to transmission or distibution facilities")
    grid_voltage_3_kv = Column(
        Float, comment="Plant's grid voltage at point of interconnection to transmission or distibution facilities")
    iso_rto_name = Column(
        String, comment="The name of the plant's ISO or RTO.")
    iso_rto_code = Column(
        String, comment="The code of the plant's ISO or RTO.")
    latitude = Column(Float, comment="The latitude of a plant's coordinates")
    longitude = Column(Float, comment="The longitude of a plant's coordinates")
    nerc_region = Column(
        String, comment="NERC region in which the plant is located")
    primary_purpose_naics_id = Column(
        Float, comment="North American Industry Classification System (NAICS) code that best describes the primary purpose of the reporting plant")
    sector_name = Column(
        String, comment="Plant-level sector name, designated by the primary purpose, regulatory status and plant-level combined heat and power status")
    sector_id = Column(
        Float, comment="Plant-level sector number, designated by the primary purpose, regulatory status and plant-level combined heat and power status")
    state = Column(String,  # pudl.models.glue.us_states_territories,  # ENUM
                   comment="Plant state. Two letter US state and territory abbreviations.")
    street_address = Column(String, comment="Plant street address")
    zip_code = Column(String, comment="Plant street address")
    timezone = Column(String, comment="IANA timezone name")


class GeneratorEntityEIA(PUDLBase):
    """
    An EIA Generator, listed in 923 or 860.

    A compilation of all EIA plant ids and static info.
    """

    __tablename__ = 'generators_entity_eia'
    plant_id_eia = Column(Integer,
                          ForeignKey('plants_entity_eia.plant_id_eia'),
                          primary_key=True, nullable=False, comment="EIA Plant Identification number. One to five digit numeric.")
    generator_id = Column(String, primary_key=True, nullable=False,
                          comment="Generator identification number")
    # TODO: Add static plant info
    # ForeignKey('prime_movers_eia923.abbr'),
    prime_mover_code = Column(
        String, comment="EIA assigned code for the prime mover (i.e. the engine, turbine, water wheel, or similar machine that drives an electric generator)")
    duct_burners = Column(
        Boolean, comment="Indicates whether the unit has duct-burners for supplementary firing of the turbine exhaust gas")
    operating_date = Column(
        Date, comment="Date the generator began commercial operation")
    topping_bottoming_code = Column(
        String, comment="If the generator is associated with a combined heat and power system, indicates whether the generator is part of a topping cycle or a bottoming cycle")  # ENUM
    solid_fuel_gasification = Column(
        Boolean, comment="Indicates whether the generator is part of a solid fuel gasification system")
    pulverized_coal_tech = Column(
        Boolean, comment="Indicates whether the generator uses pulverized coal technology")
    fluidized_bed_tech = Column(
        Boolean, comment="Indicates whether the generator uses fluidized bed technology")
    subcritical_tech = Column(
        Boolean, comment="Indicates whether the generator uses subcritical technology")
    supercritical_tech = Column(
        Boolean, comment="Indicates whether the generator uses supercritical technology")
    ultrasupercritical_tech = Column(
        Boolean, comment="Indicates whether the generator uses ultra-supercritical technology")
    stoker_tech = Column(
        Boolean, comment="Indicates whether the generator uses stoker technology")
    other_combustion_tech = Column(
        Boolean, comment="Indicates whether the generator uses other combustion technologies")
    heat_bypass_recovery = Column(
        Boolean, comment="Can this generator operate while bypassing the heat recovery steam generator?")
    rto_iso_lmp_node_id = Column(
        String, comment="The designation used to identify the price node in RTO/ISO Locational Marginal Price reports")
    rto_iso_location_wholesale_reporting_id = Column(
        String, comment="The designation used to report ths specific location of the wholesale sales transactions to FERC for the Electric Quarterly Report")
    associated_combined_heat_power = Column(
        Boolean, comment="Indicates whether the generator is associated with a combined heat and power system")
    original_planned_operating_date = Column(
        Date, comment="The date the generator was originally scheduled to be operational")
    operating_switch = Column(
        String, comment="Indicates whether the fuel switching generator can switch when operating")
    previously_canceled = Column(
        Boolean, comment="Indicates whether the generator was previously reported as indefinitely postponed or canceled")


class BoilerEntityEIA(PUDLBase):
    """
    An EIA Boiler, listed in 923 or 860.

    A compilation of all EIA plant ids and static info.
    """

    __tablename__ = 'boilers_entity_eia'
    plant_id_eia = Column(Integer,
                          ForeignKey('plants_entity_eia.plant_id_eia'),
                          primary_key=True, nullable=False, comment="EIA Plant Identification number. One to five digit numeric.")
    boiler_id = Column(String, primary_key=True, nullable=False,
                       comment="The EIA-assigned boiler identification code. Alphanumeric.")
    # TODO: Add static boiler info (if necessary?)
    prime_mover_code = Column(
        String, comment="Code for the type of prime mover (e.g. CT, CG)")  # from EIA 923, CT, CG


class RegionEntityIPM(PUDLBase):
    """A region in EPA's Integrated Planning Model."""

    __tablename__ = 'regions_entity_ipm'

    region_id_ipm = Column(String, primary_key=True, nullable=False)
