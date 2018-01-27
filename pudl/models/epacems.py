"""Database models for PUDL tables derived from EPA CEMS Data."""

from sqlalchemy import Boolean, Integer, String, Float, Numeric, Date
from sqlalchemy import Column, ForeignKey, ForeignKeyConstraint
import pudl.models


class HourlyEmissions(pudl.models.PUDLBase):
    """Hourly emissions data by month as reported to EPA CEMS."""

    __tablename__ = 'hourly_emissions_epacems'
    id = Column(Integer, autoincrement=True, primary_key=True)  # surrogate key
    state = Column(String)
    facility_name = Column(String)
    orispl_code = Column(Integer)
    unitid = Column(String)
    op_date = Column(String)
    op_hour = Column(Integer)
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
    fac_id = Column(Integer)
    unit_id = Column(Integer)
