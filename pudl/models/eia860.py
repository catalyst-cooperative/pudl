"""Database models for PUDL tables derived from EIA Form 860 Data."""

from sqlalchemy import Boolean, Integer, String, Float, Date
from sqlalchemy import Column, ForeignKey, ForeignKeyConstraint
import pudl.models.entities


class BoilerGeneratorAssnEIA860(pudl.models.entities.PUDLBase):
    """Information pertaining to boiler_generator pairs listed in EIA 860."""

    __tablename__ = 'boiler_generator_assn_eia860'
    __table_args__ = (ForeignKeyConstraint(
        ['plant_id_eia', 'generator_id'],
        ['generators_entity_eia.plant_id_eia',
         'generators_entity_eia.generator_id']),)

    id = Column(Integer, autoincrement=True, primary_key=True)
    report_date = Column(Date, nullable=False)
    plant_id_eia = Column(Integer, nullable=False)  # FK?
    boiler_id = Column(String, nullable=False)  # FK?
    generator_id = Column(String, nullable=False)


class UtilitiesEIA860(pudl.models.entities.PUDLBase):
    """Information on utilities reporting information on form EIA860."""

    __tablename__ = 'utilities_eia860'
    id = Column(Integer, autoincrement=True, primary_key=True)
    report_date = Column(Date, nullable=False)
    utility_id_eia = Column(Integer,
                            ForeignKey('utilities_entity_eia.utility_id_eia'),
                            nullable=False)


class PlantsEIA860(pudl.models.entities.PUDLBase):
    """Plant-specific information reported on form EIA860."""

    __tablename__ = 'plants_eia860'
    id = Column(Integer, autoincrement=True, primary_key=True)
    report_date = Column(Date, nullable=False)
    utility_name = Column(String)  # FK
    plant_id_eia = Column(Integer, nullable=False)  # FK


class OwnershipEIA860(pudl.models.entities.PUDLBase):
    """The schedule of generator ownership shares from EIA860."""

    __tablename__ = 'ownership_eia860'
    __table_args__ = (ForeignKeyConstraint(
        ['plant_id_eia', 'generator_id'],
        ['generators_entity_eia.plant_id_eia',
         'generators_entity_eia.generator_id']),)

    id = Column(Integer, autoincrement=True, primary_key=True)
    report_date = Column(Date, nullable=False)
    utility_id_eia = Column(Integer,
                            ForeignKey('utilities_entity_eia.utility_id_eia'),
                            nullable=False)
    plant_id_eia = Column(Integer, nullable=False)  # FK
    generator_id = Column(String, nullable=False,)
    operational_status_code = Column(String)
    owner_utility_id_eia = Column(Integer, nullable=False)  # FK utility_id_eia
    owner_name = Column(String)
    owner_state = Column(
        pudl.models.glue.us_states_canada_prov_terr,  # ENUM
        comment="Two letter US state and territory abbreviations."
    )
    owner_city = Column(String)
    owner_street_address = Column(String)
    owner_zip_code = Column(String)
    fraction_owned = Column(Float)


class GeneratorsEIA860(pudl.models.entities.PUDLBase):
    """Generator-level data reported in form EIA860."""

    __tablename__ = 'generators_eia860'
    __table_args__ = (ForeignKeyConstraint(
        ['plant_id_eia', 'generator_id'],
        ['generators_entity_eia.plant_id_eia',
         'generators_entity_eia.generator_id']),)

    id = Column(Integer, autoincrement=True, primary_key=True)
    report_date = Column(Date, nullable=False)
    utility_name = Column(String)  # FK
    plant_id_eia = Column(Integer)
    generator_id = Column(String)
    unit_id_eia = Column(String)
