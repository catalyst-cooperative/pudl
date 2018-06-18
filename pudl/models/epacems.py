"""Database models for PUDL tables derived from EPA CEMS Data."""

from sqlalchemy import Integer, String, Float, DateTime, Column
import pudl.models.entities


class HourlyEmissions(pudl.models.entities.PUDLBase):
    """Hourly emissions data by month as reported to EPA CEMS."""
    # TODO: Add these constraints and indexes *after* loading all the data in.
    # UniqueConstraint("orispl_code", "unitid", "op_date", "op_hour")
    # indexes on: orispl_code, op_datetime, orispl_code, and the date part of
    # op_datetime (each individually)
    # Command to create an index for the date part of op_datetime
    # CREATE INDEX op_date_idx (op_datetime::date)

    __tablename__ = "hourly_emissions_epacems"
    __table_args__ = {'prefixes': ['UNLOGGED']}
    id = Column(Integer, autoincrement=True, primary_key=True)  # surrogate key
    state = Column(String, nullable=False)
    facility_name = Column(String, nullable=False)
    # TODO: Link to EIA plant ID
    orispl_code = Column(Integer, nullable=False)
    unitid = Column(String, nullable=False)
    op_datetime = Column(DateTime, nullable=False)
    # TODO: Make a view that multiplies op_time and gload_mw to get gload_mwh
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
    # TODO: Make a view that divides heat_input_mmbtu / gload_mwh to get heatrate
    #   And also has a bad_heatrate flag.
    # heatrate = Column(Float)
    # bad_heatrate = Column(Boolean, nullable=False)
    # TODO: I think these are the same as orispl_code and unitid, but check.
    # These are currently being dropped in extract/epacems.py
    # fac_id = Column(Integer)
    # unit_id = Column(Integer)
