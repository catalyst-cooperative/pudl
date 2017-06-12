"""Database models for PUDL tables derived from EIA Form 860 Data."""

from sqlalchemy import Boolean, Integer, String, Float, Numeric, Date
from sqlalchemy import Column, ForeignKey, ForeignKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
import pudl.models


class BoilerGeneratorAssnEIA860(pudl.models.PUDLBase):
    """Information pertaining to individual coal mines listed in EIA 923."""

    __tablename__ = 'boiler_generator_assn_eia860'
    id = Column(Integer, autoincrement=True, primary_key=True)  # surrogate key
    # Integer, ForeignKey('utilities_eia923.operator_id'))
    operator_id = Column(Integer)
    plant_id = Column(Integer)  # , ForeignKey('plants_eia923.plant_id'))
    boiler_id = Column(String)
    generator_id = Column(String)
