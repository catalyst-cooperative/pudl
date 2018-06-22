"""Database models for PUDL tables derived from EPA CEMS Data."""

from sqlalchemy import Integer, SmallInteger, String, REAL, DateTime, Column, Enum, Interval, Date
from sqlalchemy.dialects.postgresql import TSRANGE
import pudl.models.entities
import pudl.constants as pc

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

ENUM_FLAG_MEASUREMENT = Enum(
    "LME",
    "Measured",
    "Measured and Substitute",
    "Other",
    "Substitute",
    "Undetermined",
    "Unknown Code",
    "",
    name="enum_measurement_flag",
)
# ENUM_FLAG_CALCULATED = Enum("Calculated", "", name="enum_calculated_flag")

ENUM_NOX = Enum(
    "Calculated",
    "LME",
    "Measured",
    "Measured and Substitute",
    "Not Applicable",
    "Other",
    "Substitute",
    "Undetermined",
    "Unknown Code",
    "",
    name="enum_nox",
)

ENUM_STATES = Enum(*pc.cems_states.keys(), name="enum_states")


class HourlyEmissions(pudl.models.entities.PUDLBase):
    """Hourly emissions data by month as reported to EPA CEMS."""

    # TODO(low priority):
    # - Make a view that divides heat_input_mmbtu / gload_mwh to get heatrate
    #   And also has a bad_heatrate flag.
    # - Make a view that multiplies op_time and gload_mw to get gload_mwh
    # - And has an operating_date
    __tablename__ = "hourly_emissions_epacems"
    __table_args__ = {"prefixes": ["UNLOGGED"]}
    id = Column(Integer, autoincrement=True, primary_key=True)  # surrogate key
    state = Column(ENUM_STATES, nullable=False)
    facility_name = Column(String, nullable=False)
    # TODO: Link to EIA plant ID
    orispl_code = Column(Integer, nullable=False)
    unitid = Column(String, nullable=False)
    # operating_date = Column(Date, nullable=False)
    operating_datetime = Column(DateTime, nullable=False)
    operating_interval = Column(Interval)
    gross_load_mw = Column(REAL)
    steam_load_1000_lbs = Column(REAL)
    so2_mass_lbs = Column(REAL)
    so2_mass_measure_flg = Column(ENUM_FLAG_MEASUREMENT)
    # so2_rate_lbs_mmbtu = Column(REAL)
    # so2_rate_measure_flg = Column(ENUM_FLAG_CALCULATED)
    nox_rate_lbs_mmbtu = Column(REAL)
    nox_rate_measure_flg = Column(ENUM_NOX)
    nox_mass_lbs = Column(REAL)
    nox_mass_measure_flg = Column(ENUM_NOX)
    co2_mass_tons = Column(REAL)
    co2_mass_measure_flg = Column(ENUM_FLAG_MEASUREMENT)
    # co2_rate_tons_mmbtu = Column(REAL)
    # co2_rate_measure_flg = Column(ENUM_FLAG_CALCULATED)
    heat_input_mmbtu = Column(REAL)
    fac_id = Column(SmallInteger)  # max value is 8421
    unit_id = Column(Integer)

DROP_VIEWS = ["DROP VIEW IF EXISTS hourly_emissions_epacems_view"]
CREATE_VIEWS = ["""
    CREATE VIEW hourly_emissions_epacems_view AS
    SELECT
        id,
        state,
        facility_name,
        orispl_code,
        unitid,
        operating_datetime,
        operating_datetime::date AS operating_date,
        operating_interval,
        gross_load_mw,
        steam_load_1000_lbs,
        so2_mass_lbs,
        so2_mass_measure_flg,
        so2_mass_lbs / heat_input_mmbtu AS so2_rate_lbs_mmbtu,
        nox_rate_lbs_mmbtu,
        nox_rate_measure_flg,
        nox_mass_lbs,
        nox_mass_measure_flg,
        co2_mass_tons,
        co2_mass_measure_flg,
        co2_mass_tons / heat_input_mmbtu AS co2_rate_tons_mmbtu,
        heat_input_mmbtu,
        fac_id,
        unit_id
    FROM hourly_emissions_epacems
    """,
    ]
