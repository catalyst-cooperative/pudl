"""Database models for PUDL tables derived from EPA CEMS Data."""

from sqlalchemy import Integer, String, Float, DateTime, Column, Enum, Interval, Date
from sqlalchemy.dialects.postgresql import TSRANGE
import pudl.models.entities

# Three types of Enum here, one for things that are sort of measured, one for
# things that are only calculated, and a special case for NOx rate and mass.
# Measured:
# - so2_mass_measure_flg
# - co2_mass_measure_flg
# Calculated:
# - so2_rate_measure_flg
# - co2_rate_measure_flg
# NOx:
# - nox_rate_measure_flg
# - nox_mass_measure_flg

measurement_flag_enum = Enum(
    "LME",
    "Measured",
    "Measured and Substitute",
    "Other",
    "Substitute",
    "Unknown Code",
    "",
    name="measurement_flag_enum",
)
calculated_flag_enum = Enum("Calculated", "", name="calculated_flag_enum")

nox_enum = Enum(
    "Calculated",
    "LME",
    "Measured",
    "Measured and Substitute",
    "Other",
    "Substitute",
    "Undetermined",
    "Unknown Code",
    "",
    name="nox_rate_enum",
)


class HourlyEmissions(pudl.models.entities.PUDLBase):
    """Hourly emissions data by month as reported to EPA CEMS."""

    # TODO(low priority):
    # - Make a view that divides heat_input_mmbtu / gload_mwh to get heatrate
    #   And also has a bad_heatrate flag.
    # - Make a view that multiplies op_time and gload_mw to get gload_mwh
    __tablename__ = "hourly_emissions_epacems"
    __table_args__ = {"prefixes": ["UNLOGGED"]}
    id = Column(Integer, autoincrement=True, primary_key=True)  # surrogate key
    state = Column(String, nullable=False)
    facility_name = Column(String, nullable=False)
    # TODO: Link to EIA plant ID
    orispl_code = Column(Integer, nullable=False)
    unitid = Column(String, nullable=False)
    operating_date = Column(Date, nullable=False)
    operating_datetime = Column(DateTime, nullable=False)
    operating_interval = Column(Interval)
    gross_load_mw = Column(Float)
    steam_load_1000_lbs = Column(Float)
    so2_mass_lbs = Column(Float)
    so2_mass_measure_flg = Column(measurement_flag_enum)
    so2_rate_lbs_mmbtu = Column(Float)
    so2_rate_measure_flg = Column(calculated_flag_enum)
    nox_rate_lbs_mmbtu = Column(Float)
    nox_rate_measure_flg = Column(nox_enum)
    nox_mass_lbs = Column(Float)
    nox_mass_measure_flg = Column(nox_enum)
    co2_mass_tons = Column(Float)
    co2_mass_measure_flg = Column(measurement_flag_enum)
    co2_rate_tons_mmbtu = Column(Float)
    co2_rate_measure_flg = Column(calculated_flag_enum)
    heat_input_mmbtu = Column(Float)
    fac_id = Column(Integer)
    unit_id = Column(Integer)
