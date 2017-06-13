"""Database models for PUDL tables derived from EIA Form 860 Data."""

from sqlalchemy import Boolean, Integer, String, Float, Numeric, Date
from sqlalchemy import Column, ForeignKey, ForeignKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
import pudl.models


class BoilerGeneratorAssnEIA860(pudl.models.PUDLBase):
    """Information pertaining to boiler_generator pairs listed in EIA 860."""

    __tablename__ = 'boiler_generator_assn_eia860'
    id = Column(Integer, autoincrement=True, primary_key=True)  # surrogate key
    # Integer, ForeignKey('utilities_eia923.operator_id'))
    operator_id = Column(Integer)
    plant_id = Column(Integer)  # , ForeignKey('plants_eia923.plant_id'))
    boiler_id = Column(String)
    generator_id = Column(String)


class UtilityEIA860(pudl.models.PUDLBase):
    """Information on utilities reporting information on form EIA860."""

    __tablename__ = 'utility_eia860'
    id = Column(Integer, autoincrement=True, primary_key=True)
    operator_id = Column(Integer)
    street_address = Column(String)
    city = Column(String)
    state = Column(String)
    zip_code = Column(Integer)
    plants_reported_owner = Column(String)
    plants_reported_operator = Column(String)
    plants_reported_asset_manager = Column(String)
    plants_reported_other_relationship = Column(String)
    entity_type = Column(String)


class PlantEIA860(pudl.models.PUDLBase):
    """Plant-specific information reported on form EIA860."""

    __tablename__ = 'plant_eia860'
    id = Column(Integer, autoincrement=True, primary_key=True)
    operator_id = Column(Integer)
    plant_id = Column(Integer)
    street_address = Column(String)
    city = Column(String)
    state = Column(String)
    zip_code = Column(Integer)
    county = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    nerc_region = Column(String)
    balancing_authority_code = Column(String)
    balancing_authority_name = Column(String)
    water_source = Column(String)
    primary_purpose_naics = Column(Integer)
    regulatory_status = Column(String)
    sector = Column(Integer)
    sector_name = Column(String)
    net_metering = Column(String)
    ferc_cogen_status = Column(String)
    ferc_cogen_docket_no = Column(String)
    ferc_small_power_producer = Column(String)
    ferc_small_power_producer_docket_no = Column(String)
    ferc_exempt_wholesale_generator = Column(String)
    ferc_exempt_wholesale_generator_docket_no = Column(String)
    ash_impoundment = Column(String)
    ash_impoundment_lined = Column(String)
    ash_impoundment_status = Column(String)
    transmission_distribution_owner = Column(String)
    transmission_distribution_owner_id = Column(Integer)
    transmission_distribution_owner_state = Column(String)
    grid_voltage_kv = Column(Float)
    grid_voltage_2kv = Column(Float)
    grid_voltage_3kv = Column(Float)
    natural_gas_pipeline = Column(String)


class OwnershipEIA860(pudl.models.PUDLBase):
    """The schedule of generator ownership shares from EIA860."""

    __tablename__ = 'ownership_eia860'
    id = Column(Integer, autoincrement=True, primary_key=True)
    operator_id = Column(Integer)
    plant_id = Column(Integer)
    state = Column(String)
    generator_id = Column(String)
    status = Column(String)
    owner_address = Column(String)
    owner_city = Column(String)
    owner_state = Column(String)
    owner_zip_code = Column(Integer)
    owner_id = Column(Integer)
    percent_owned = Column(Float)


class GeneratorEIA860(pudl.models.PUDLBase):
    """Generator-level data reported in form EIA860."""

    __tablename__ = 'generator_eia860'
    id = Column(Integer, autoincrement=True, primary_key=True)
    operator_id = Column(Integer)
    plant_id = Column(Integer)
    state = Column(String)
    county = Column(String)
    generator_id = Column(String)
    technology = Column(String)
    prime_mover = Column(String)
    unit_code = Column(String)
    ownership = Column(String)
    duct_burners = Column(String)
    heat_bypass_recovery = Column(String)
    rto_iso_lmp_node = Column(String)
    rto_iso_location_wholesale_reporting = Column(String)
    nameplate_capacity_mw = Column(Float)
    nameplate_power_factor = Column(Float)
    summer_capacity_mw = Column(Float)
    winter_capacity_mw = Column(Float)
    minimum_load_mw = Column(Float)
    uprate_derate_during_year = Column(String)
    month_uprate_derate_completed = Column(String)
    year_uprate_derate_completed = Column(String)
    status = Column(String)
    syncronized_transmission_grid = Column(String)
    operating_month = Column(Integer)
    operating_year = Column(Integer)
    planned_retirement_month = Column(Integer)
    planned_retirement_year = Column(Integer)
    associated_combined_heat_power = Column(String)
    sector_name = Column(String)
    sector = Column(Integer)
    topping_bottoming = Column(String)
    energy_source_1 = Column(String)
    energy_source_2 = Column(String)
    energy_source_3 = Column(String)
    energy_source_4 = Column(String)
    energy_source_5 = Column(String)
    energy_source_6 = Column(String)
    startup_source_1 = Column(String)
    startup_source_2 = Column(String)
    startup_source_3 = Column(String)
    startup_source_4 = Column(String)
    solid_fuel_gasification = Column(String)
    carbon_capture = Column(String)
    turbines_inverters_hydrokinetics = Column(String)
    time_cold_shutdown_full_load = Column(String)
    fluidized_bed_tech = Column(String)
    pulverized_coal_tech = Column(String)
    stoker_tech = Column(String)
    other_combustion_tech = Column(String)
    subcritical_tech = Column(String)
    supercritical_tech = Column(String)
    ultrasupercritical_tech = Column(String)
    planned_net_summer_capacity_uprate = Column(Float)
    planned_net_winter_capacity_uprate = Column(Float)
    planned_uprate_month = Column(Integer)
    planned_uprate_year = Column(Integer)
    planned_net_summer_capacity_derate = Column(Float)
    planned_net_winter_capacity_derate = Column(Float)
    planned_derate_month = Column(Integer)
    planned_derate_year = Column(Integer)
    planned_new_prime_mover = Column(String)
    planned_energy_source_1 = Column(String)
    planned_new_nameplate_capacity_mw = Column(Float)
    planned_repower_month = Column(Integer)
    planned_repower_year = Column(Integer)
    other_planned_modifications = Column(String)
    other_modifications_month = Column(Integer)
    other_modifications_year = Column(Integer)
    cofire_fuels = Column(String)
    switch_oil_gas = Column(String)
