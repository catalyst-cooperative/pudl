"""Database models for PUDL tables derived from EIA Data."""

from sqlalchemy import Column, ForeignKey, Integer, String, Date, Float, Boolean
from sqlalchemy import ForeignKeyConstraint
import pudl.models.entities


class UtilityAnnualEIA(pudl.models.entities.PUDLBase):
    """
    EIA Plants per year, listed in 923 or 860.

    A compliation of yearly plant info.
    """

    __tablename__ = 'utilities_annual_eia'

    id = Column(Integer, autoincrement=True, primary_key=True)
    utility_id_eia = Column(Integer,
                            ForeignKey('utilities_entity_eia.utility_id_eia'),
                            nullable=False)
    report_date = Column(Date, nullable=False)
    # TODO: Add utility info that varies per year
    plants_reported_owner = Column(String)
    plants_reported_operator = Column(String)
    plants_reported_asset_manager = Column(String)
    plants_reported_other_relationship = Column(String)


class PlantAnnualEIA(pudl.models.entities.PUDLBase):
    """
    EIA Plants per year, listed in 923 or 860.

    A compliation of yearly plant info.
    """

    __tablename__ = 'plants_annual_eia'

    id = Column(Integer, autoincrement=True, primary_key=True)
    plant_id_eia = Column(Integer,
                          ForeignKey('plants_entity_eia.plant_id_eia'),
                          nullable=False)
    report_date = Column(Date, nullable=False)
    ash_impoundment = Column(String)
    ash_impoundment_lined = Column(String)
    ash_impoundment_status = Column(String)
    energy_storage = Column(String)
    ferc_cogen_docket_no = Column(String)
    ferc_exempt_wholesale_generator_docket_no = Column(String)
    ferc_small_power_producer_docket_no = Column(String)
    liquefied_natural_gas_storage = Column(String)
    natural_gas_local_distribution_company = Column(String)
    natural_gas_storage = Column(String)
    natural_gas_pipeline_name_1 = Column(String)
    natural_gas_pipeline_name_2 = Column(String)
    natural_gas_pipeline_name_3 = Column(String)
    net_metering = Column(String)
    pipeline_notes = Column(String)
    regulatory_status_code = Column(String)
    transmission_distribution_owner_id = Column(String)
    transmission_distribution_owner_name = Column(String)
    transmission_distribution_owner_state = Column(String)
    utility_id_eia = Column(Integer)
    water_source = Column(String)


class GeneratorAnnualEIA(pudl.models.entities.PUDLBase):
    """
    EIA generators per year, listed in 923 or 860.

    A compilation of EIA generators ids and yearly info.
    """

    __tablename__ = 'generators_annual_eia'
    __table_args__ = (ForeignKeyConstraint(
        ['plant_id_eia', 'generator_id'],
        ['generators_entity_eia.plant_id_eia',
         'generators_entity_eia.generator_id']),)

    id = Column(Integer, autoincrement=True, primary_key=True)
    plant_id_eia = Column(Integer, nullable=False)
    generator_id = Column(String, nullable=False)
    report_date = Column(Date, nullable=False)
    # TODO: Add static plant info
    operational_status_code = Column(String)
    ownership_code = Column(String)
    capacity_mw = Column(Float)
    summer_capacity_mw = Column(Float)
    winter_capacity_mw = Column(Float)
    energy_source_code_1 = Column(String)
    energy_source_code_2 = Column(String)
    energy_source_code_3 = Column(String)
    energy_source_code_4 = Column(String)
    energy_source_code_5 = Column(String)
    energy_source_code_6 = Column(String)
    fuel_type_code_pudl = Column(String)
    multiple_fuels = Column(Boolean)
    deliver_power_transgrid = Column(Boolean)
    syncronized_transmission_grid = Column(Boolean)
    turbines_num = Column(Integer)
    # sector_name = Column(String)
    # sector_id = Column(Integer)
    planned_modifications = Column(Boolean)
    planned_net_summer_capacity_uprate_mw = Column(Float)
    planned_net_winter_capacity_uprate_mw = Column(Float)
    planned_uprate_date = Column(Date)
    planned_net_summer_capacity_derate_mw = Column(Float)
    planned_net_winter_capacity_derate_mw = Column(Float)
    planned_derate_date = Column(Date)
    planned_new_prime_mover_code = Column(String)
    planned_energy_source_code_1 = Column(String)
    planned_repower_date = Column(Date)
    other_planned_modifications = Column(Boolean)
    other_modifications_date = Column(Date)
    planned_retirement_date = Column(Date)
    carbon_capture = Column(Boolean)
    startup_source_code_1 = Column(String)
    startup_source_code_2 = Column(String)
    startup_source_code_3 = Column(String)
    startup_source_code_4 = Column(String)
    technology_description = Column(String)
    turbines_inverters_hydrokinetics = Column(String)
    time_cold_shutdown_full_load_code = Column(String)
    planned_new_capacity_mw = Column(Float)
    cofire_fuels = Column(Boolean)
    switch_oil_gas = Column(Boolean)
    nameplate_power_factor = Column(Float)
    minimum_load_mw = Column(Float)
    uprate_derate_during_year = Column(Boolean)
    uprate_derate_completed_date = Column(Date)
    current_planned_operating_date = Column(Date)
    summer_estimated_capability_mw = Column(Float)
    winter_estimated_capability_mw = Column(Float)
    operating_switch = Column(String)
    retirement_date = Column(Date)


class BoilerGeneratorAssociationEIA(pudl.models.entities.PUDLBase):
    """
    EIA boiler generator associations.

    Compiled from 860 and 923.
    """

    __tablename__ = 'boiler_generator_assn_eia'
    __table_args__ = (ForeignKeyConstraint(
        ['plant_id_eia', 'generator_id'],
        ['generators_entity_eia.plant_id_eia',
         'generators_entity_eia.generator_id']),)

    id = Column(Integer, autoincrement=True, primary_key=True)
    plant_id_eia = Column(Integer, nullable=False)
    report_date = Column(Date, nullable=False)
    generator_id = Column(String)
    boiler_id = Column(String)
    unit_id_eia = Column(String)
    unit_id_pudl = Column(Integer, nullable=False)
    bga_source = Column(String)
