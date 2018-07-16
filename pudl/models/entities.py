"""Database models for PUDL tables for ."""

from sqlalchemy import Column, ForeignKey, Integer, String, Boolean
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


class PlantEntityEIA(PUDLBase):
    """
    An EIA Plant, listed in 923 or 860.

    A compilation of all EIA plant ids and static info.
    """

    __tablename__ = 'plants_entity_eia'
    plant_id_eia = Column(Integer, primary_key=True, nullable=False)
    # TODO: Add static plant info


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
