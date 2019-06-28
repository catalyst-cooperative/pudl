"""Database models for PUDL tables for ."""

from sqlalchemy import Column, ForeignKey, Integer, Float, String, Boolean, Date
from sqlalchemy.ext.declarative import declarative_base

PUDLBase = declarative_base()


class DataSets(PUDLBase):
    """
    Active datasets ingest into PUDL.

    A compilation of the active or inactive datasets in the databaseself.
    """

    __tablename__ = 'datasets'
    datasource = Column(String, primary_key=True)
    active = Column(Boolean)


class UtilityEntityEIA(PUDLBase):
    """
    An EIA Utility, listed in 923 or 860.

    A compilation of all EIA utilities ids and static info.
    """

    __tablename__ = 'utilities_entity_eia'
    utility_id_eia = Column(Integer, primary_key=True, nullable=False)
    utility_name = Column(String)
    entity_type = Column(String)


class PlantEntityEIA(PUDLBase):
    """
    An EIA Plant, listed in 923 or 860.

    A compilation of all EIA plant ids and static info.
    """

    __tablename__ = 'plants_entity_eia'
    plant_id_eia = Column(Integer, primary_key=True, nullable=False)
    plant_name = Column(String)
    balancing_authority_code = Column(String)
    balancing_authority_name = Column(String)
    city = Column(String)
    county = Column(String)
    ferc_cogen_status = Column(String)
    ferc_exempt_wholesale_generator = Column(String)
    ferc_small_power_producer = Column(String)
    grid_voltage_kv = Column(Float)
    grid_voltage_2_kv = Column(Float)
    grid_voltage_3_kv = Column(Float)
    iso_rto_name = Column(String)
    iso_rto_code = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    nerc_region = Column(String)
    primary_purpose_naics_id = Column(Float)
    sector_name = Column(String)
    sector_id = Column(Float)
    state = Column(String,  # pudl.models.glue.us_states_territories,  # ENUM
                   comment="Two letter US state and territory abbreviations.")
    street_address = Column(String)
    zip_code = Column(String)
    timezone = Column(String, comment="IANA timezone name")


class GeneratorEntityEIA(PUDLBase):
    """
    An EIA Generator, listed in 923 or 860.

    A compilation of all EIA plant ids and static info.
    """

    __tablename__ = 'generators_entity_eia'
    plant_id_eia = Column(Integer,
                          ForeignKey('plants_entity_eia.plant_id_eia'),
                          primary_key=True, nullable=False)
    generator_id = Column(String, primary_key=True, nullable=False)
    # TODO: Add static plant info
    # ForeignKey('prime_movers_eia923.abbr'),
    prime_mover_code = Column(String)
    duct_burners = Column(Boolean)
    operating_date = Column(Date)
    topping_bottoming_code = Column(String)  # ENUM
    solid_fuel_gasification = Column(Boolean)
    pulverized_coal_tech = Column(Boolean)
    fluidized_bed_tech = Column(Boolean)
    subcritical_tech = Column(Boolean)
    supercritical_tech = Column(Boolean)
    ultrasupercritical_tech = Column(Boolean)
    stoker_tech = Column(Boolean)
    other_combustion_tech = Column(Boolean)
    heat_bypass_recovery = Column(Boolean)
    rto_iso_lmp_node_id = Column(String)
    rto_iso_location_wholesale_reporting_id = Column(String)
    associated_combined_heat_power = Column(Boolean)
    original_planned_operating_date = Column(Date)
    operating_switch = Column(String)
    previously_canceled = Column(Boolean)


class BoilerEntityEIA(PUDLBase):
    """
    An EIA Boiler, listed in 923 or 860.

    A compilation of all EIA plant ids and static info.
    """

    __tablename__ = 'boilers_entity_eia'
    plant_id_eia = Column(Integer,
                          ForeignKey('plants_entity_eia.plant_id_eia'),
                          primary_key=True, nullable=False)
    boiler_id = Column(String, primary_key=True, nullable=False)
    # TODO: Add static boiler info (if necessary?)
    prime_mover_code = Column(String)


class RegionEntityIPM(PUDLBase):
    """
    A region in EPA's Integrated Planning Model.
    """

    __tablename__ = 'regions_entity_ipm'

    region_id_ipm = Column(String, primary_key=True, nullable=False)
