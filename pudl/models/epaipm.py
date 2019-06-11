"""Database models for PUDL tables derived from EPA IPM Data."""

from sqlalchemy import Boolean, Integer, String, Float, Date
from sqlalchemy import Column, ForeignKey, ForeignKeyConstraint
import pudl.models.entities


class TransmissionSingleIPM(pudl.models.entities.PUDLBase):
    """
    Transmission limits between individual IPM regions
    """

    __tablename__ = 'transmission_single_ipm'

    id = Column(Integer, autoincrement=True, primary_key=True)
    region_from = Column(
        String,
        ForeignKey('regions_entity_ipm.region_id_ipm'),
        nullable=False
    )
    region_to = Column(
        String,
        ForeignKey('regions_entity_ipm.region_id_ipm'),
        nullable=False
    )
    firm_capacity_mw = Column(Float)
    nonfirm_capacity_mw = Column(Float)
    tariff_mills_kwh = Column(Float)
