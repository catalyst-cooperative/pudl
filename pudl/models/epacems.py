"""Database models for PUDL tables derived from EPA CEMS Data."""

from sqlalchemy import (
    Boolean,
    Integer,
    String,
    Float,
    Numeric,
    Date,
    Column,
    ForeignKey,
    ForeignKeyConstraint,
    UniqueConstraint,
    CheckConstraint,
)
import pudl.models.entities


class HourlyEmissions(pudl.models.entities.PUDLBase):
    """Hourly emissions data by month as reported to EPA CEMS."""
    # TODO: Add these constraints and indexes *after* loading all the data in.
    # UniqueConstraint("orispl_code", "unitid", "op_date", "op_hour")
    # indexes on: orispl_code, op_date, orispl_code (each individually)
    __tablename__ = "hourly_emissions_epacems"
    __table_args__ = {'prefixes': ['UNLOGGED']}
    id = Column(Integer, autoincrement=True, primary_key=True)  # surrogate key
    state = Column(String, nullable=False)
    facility_name = Column(String, nullable=False)
    # TODO: Link to EIA plant ID
    orispl_code = Column(Integer, nullable=False)
    unitid = Column(String, nullable=False)
    # TODO: parse these as better dates or intervals
    op_date = Column(String, nullable=False)
    op_hour = Column(Integer, nullable=False)
    op_time = Column(Float)
    gload_mw = Column(Float)
    sload_1000_lbs = Column(Float)
    so2_mass_lbs = Column(Float)
    so2_mass_measure_flg = Column(String)
    so2_rate_lbs_mmbtu = Column(Float)
    so2_rate_measure_flg = Column(String)
    nox_rate_lbs_mmbtu = Column(Float)
    nox_rate_measure_flg = Column(String)
    nox_mass_lbs = Column(Float)
    nox_mass_measure_flg = Column(String)
    co2_mass_tons = Column(Float)
    co2_mass_measure_flg = Column(String)
    co2_rate_tons_mmbtu = Column(Float)
    co2_rate_measure_flg = Column(String)
    heat_input_mmbtu = Column(Float)
    # TODO: I think these are the same as orispl_code and unitid, but check.
    # These are currently being dropped in extract/epacems.py
    # fac_id = Column(Integer)
    # unit_id = Column(Integer)
    heatrate = Column(Float)
    bad_heatrate = Column(Boolean, nullable=False)
