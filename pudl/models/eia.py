"""Database models for PUDL tables derived from EIA Data."""

from sqlalchemy import Column, ForeignKey, Integer, String, Float
from sqlalchemy import Boolean, Integer, String, Float, Numeric, Date
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
import pudl.models.entities


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
    unit_code = Column(String)
    unit_id_pudl = Column(Integer, nullable=False)
    bga_source = Column(String)
