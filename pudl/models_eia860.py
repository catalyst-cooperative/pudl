"""Database models for PUDL tables derived from EIA Form 860 Data."""

from sqlalchemy import Boolean, Integer, String, Float, Numeric, Date
from sqlalchemy import Column, ForeignKey, ForeignKeyConstraint
import pudl.models


class BoilerGeneratorAssnEIA860(pudl.models.PUDLBase):
    """Information pertaining to boiler_generator pairs listed in EIA 860."""

    __tablename__ = 'boiler_generator_assn_eia860'
    id = Column(Integer, autoincrement=True, primary_key=True)
    operator_id = Column(Integer, nullable=False)  # FK?
    plant_id = Column(Integer, nullable=False)  # FK?
    boiler_id = Column(String, nullable=False)  # FK?
    generator_id = Column(String, nullable=False)  # FK?


class UtilitiesEIA860(pudl.models.PUDLBase):
    """Information on utilities reporting information on form EIA860."""

    __tablename__ = 'utilities_eia860'
    id = Column(Integer, autoincrement=True, primary_key=True)
    report_year = Column(Integer, nullable=False)
    operator_id = Column(Integer, nullable=False)  # FK
    operator_name = Column(String, nullable=False)  # FK
    street_address = Column(String)
    city = Column(String)
    state = Column(String)
    zip_code = Column(String)
    plants_reported_owner = Column(String)
    plants_reported_operator = Column(String)
    plants_reported_asset_manager = Column(String)
    plants_reported_other_relationship = Column(String)
    entity_type = Column(String)


class PlantsEIA860(pudl.models.PUDLBase):
    """Plant-specific information reported on form EIA860."""

    __tablename__ = 'plants_eia860'
    id = Column(Integer, autoincrement=True, primary_key=True)
    report_year = Column(Integer, nullable=False)
    operator_id = Column(Integer)  # FK
    operator_name = Column(String)  # FK
    plant_id = Column(Integer, nullable=False)  # FK
    plant_name = Column(String)  # FK
    street_address = Column(String)
    city = Column(String)
    county = Column(String)
    state = Column(String)
    zip_code = Column(String)
    water_source = Column(String)
    nerc_region = Column(String)
    primary_purpose_naics = Column(Integer)
    transmission_distribution_owner = Column(String)
    transmission_distribution_owner_id = Column(String)
    transmission_distribution_owner_state = Column(String)
    regulatory_status = Column(String)
    sector_name = Column(String)
    sector = Column(Float)
    ferc_cogen_status = Column(String)
    ferc_cogen_docket_no = Column(String)
    net_metering = Column(String)
    ferc_small_power_producer = Column(String)
    ferc_small_power_producer_docket_no = Column(String)
    ferc_exempt_wholesale_generator = Column(String)
    ferc_exempt_wholesale_generator_docket_no = Column(String)
    iso_rto = Column(String)
    iso_rto_code = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    balancing_authority_code = Column(String)
    balancing_authority_name = Column(String)
    grid_voltage_kv = Column(Float)
    grid_voltage_2_kv = Column(Float)
    grid_voltage_3_kv = Column(Float)
    ash_impoundment = Column(String)
    ash_impoundment_lined = Column(String)
    ash_impoundment_status = Column(String)
    natural_gas_pipeline = Column(String)


class OwnershipEIA860(pudl.models.PUDLBase):
    """The schedule of generator ownership shares from EIA860."""

    __tablename__ = 'ownership_eia860'
    id = Column(Integer, autoincrement=True, primary_key=True)
    report_year = Column(Integer, nullable=False)
    operator_id = Column(Integer, nullable=False)  # FK
    operator_name = Column(String, nullable=False)  # FK
    plant_id = Column(Integer, nullable=False)  # FK
    plant_name = Column(String, nullable=False)  # FK
    state = Column(String)  # FK?
    generator_id = Column(String, nullable=False)  # FK
    status = Column(String)
    ownership_id = Column(Integer)
    owner_name = Column(String)
    owner_state = Column(String)
    owner_city = Column(String)
    owner_street_address = Column(String)
    owner_zip = Column(String)
    percent_owned = Column(Float)


class GeneratorsEIA860(pudl.models.PUDLBase):
    """Generator-level data reported in form EIA860."""

    __tablename__ = 'generators_eia860'
    id = Column(Integer, autoincrement=True, primary_key=True)
    report_year = Column(Integer, nullable=False)
    operator_id = Column(Integer)  # FK
    operator_name = Column(String)  # FK
    plant_id = Column(Integer, ForeignKey('plants_eia.plant_id'))
    plant_name = Column(String)  # FK
    state = Column(String)  # FK
    county = Column(String)  # FK
    generator_id = Column(String)
    prime_mover = Column(String)  # FK?
    unit_code = Column(String)
    status = Column(String)
    ownership = Column(String)
    duct_burners = Column(String)
    nameplate_capacity_mw = Column(Float)
    summer_capacity_mw = Column(Float)
    winter_capacity_mw = Column(Float)
    operating_date = Column(Date)
    energy_source_1 = Column(String)
    energy_source_2 = Column(String)
    energy_source_3 = Column(String)
    energy_source_4 = Column(String)
    energy_source_5 = Column(String)
    energy_source_6 = Column(String)
    multiple_fuels = Column(String)
    deliver_power_transgrid = Column(String)
    syncronized_transmission_grid = Column(String)
    turbines = Column(Integer)
    cogenerator = Column(String)
    sector_name = Column(String)
    sector = Column(Integer)
    topping_bottoming = Column(String)
    planned_modifications = Column(String)
    planned_net_summer_capacity_uprate = Column(Float)
    planned_net_winter_capacity_uprate = Column(Float)
    planned_uprate_date = Column(Date)
    planned_net_summer_capacity_derate = Column(Float)
    planned_net_winter_capacity_derate = Column(Float)
    planned_derate_date = Column(Date)
    planned_new_prime_mover = Column(String)
    planned_energy_source_1 = Column(String)
    planned_repower_date = Column(Date)
    other_planned_modifications = Column(String)
    other_modifications_date = Column(Date)
    planned_retirement_date = Column(Date)
    solid_fuel_gasification = Column(String)
    pulverized_coal_tech = Column(String)
    fluidized_bed_tech = Column(String)
    subcritical_tech = Column(String)
    supercritical_tech = Column(String)
    ultrasupercritical_tech = Column(String)
    carbon_capture = Column(String)
    startup_source_1 = Column(String)
    startup_source_2 = Column(String)
    startup_source_3 = Column(String)
    startup_source_4 = Column(String)
    technology = Column(String)
    turbines_inverters_hydrokinetics = Column(String)
    time_cold_shutdown_full_load = Column(String)
    stoker_tech = Column(String)
    other_combustion_tech = Column(String)
    planned_new_nameplate_capacity_mw = Column(Float)
    cofire_fuels = Column(String)
    switch_oil_gas = Column(String)
    heat_bypass_recovery = Column(String)
    rto_iso_lmp_node = Column(String)
    rto_iso_location_wholesale_reporting = Column(String)
    nameplate_power_factor = Column(Float)
    minimum_load_mw = Column(Float)
    uprate_derate_during_year = Column(String)
    uprate_derate_completed_date = Column(Date)
    associated_combined_heat_power = Column(String)
    original_planned_operating_date = Column(Date)
    current_planned_operating_date = Column(Date)
    summer_estimated_capability = Column(Float)
    winter_estimated_capability = Column(Float)
    operating_switch = Column(String)
    previously_canceled = Column(String)
    retirement_date = Column(Date)
