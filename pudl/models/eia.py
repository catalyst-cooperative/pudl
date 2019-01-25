"""Database models for PUDL tables derived from EIA Data."""

from sqlalchemy import Column, ForeignKey, Integer, String, Date
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
    # TODO: Add plant info that varies per year
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
    #utility_name = Column(String)
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
