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
        nullable=False,
        comment='Name of the IPM region sending electricity'
    )
    region_to = Column(
        String,
        ForeignKey('regions_entity_ipm.region_id_ipm'),
        nullable=False,
        comment='Name of the IPM region receiving electricity'
    )
    firm_ttc_mw = Column(
        Float,
        comment='Transfer capacity with N-1 lines (used for reserve margins)'
    )
    nonfirm_ttc_mw = Column(
        Float,
        comment='Transfer capacity with N-0 lines (used for energy sales)'
    )
    tariff_mills_kwh = Column(
        Float,
        comment='Cost to transfer electricity between regions'
    )


class LoadCurveIPM(pudl.models.entities.PUDLBase):

    __tablename__ = 'load_curves_ipm'

    id = Column(Integer, autoincrement=True, primary_key=True)
    region = Column(
        String,
        ForeignKey('regions_entity_ipm.region_id_ipm'),
        nullable=False,
        comment='Name of the IPM region'
    )
    month = Column(
        Integer,
        nullable=False,
        comment='Month of the year'
    )
    day_of_year = Column(
        Integer,
        nullable=False,
        comment='Day of the year'
    )
    hour = Column(
        Integer,
        nullable=False,
        comment='Hour of the day (0-23). Original IPM values were 1-24.'
    )
    time_index = Column(
        Integer,
        nullable=False,
        comment='8760 index hour of the year'
    )
    load_mw = Column(
        Float,
        nullable=False,
        comment='Load (MW) in an hour of the day for the IPM region'
    )
