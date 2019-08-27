"""Database models for PUDL tables derived from EPA CEMS Data."""

import sqlalchemy as sa
from sqlalchemy import Integer, SmallInteger, String
from sqlalchemy import REAL, TIMESTAMP, Column, Enum
import pudl.models.entities

# Three types of Enum here, one for things that are sort of measured, one for
# things that are only calculated, and a special case for NOx rate and mass.
# Measured:
# - so2_mass_measurement_code
# - co2_mass_measurement_code
# Calculated:
# - so2_rate_measure_flg
# - co2_rate_measure_flg
# NOx:
# - nox_rate_measurement_code
# - nox_mass_measurement_code

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


class HourlyEmissions(pudl.models.entities.PUDLBase):
    """Hourly emissions data by month as reported to EPA CEMS."""

    # TODO(low priority):
    # - Make a view that divides heat_content_mmbtu / gload_mwh to get heatrate
    #   And also has a bad_heatrate flag.
    # - Make a view that multiplies op_time and gload_mw to get gload_mwh
    __tablename__ = "hourly_emissions_epacems"
    id = Column(Integer, autoincrement=True, primary_key=True,
                comment="PUDL-issued surrogate key.")  # surrogate key
    state = Column(
        pudl.models.glue.us_states_lower48,  # ENUM
        comment="State the plant is located in."
    )
    # plant_name = Column(String, nullable=False, comment="Name of the plant")
    # TODO: Set up foreign-key link to EIA plant ID
    plant_id_eia = Column(Integer, nullable=False,
                          comment="EIA Plant Identification number. One to five digit numeric.")
    unitid = Column(String, nullable=False,
                    comment="Facility-specific unit id (e.g. Unit 4)")
    # SQLA recommends TIMESTAMP over DateTime when dealing with timezones
    operating_datetime_utc = Column(TIMESTAMP(
        timezone=True), nullable=False, comment="Date and time measurement began.")
    operating_time_hours = Column(
        REAL, comment="Length of time interval measured.")
    gross_load_mw = Column(
        REAL, nullable=False, comment="Power delivered during time interval measured.")
    steam_load_1000_lbs = Column(
        REAL, comment="Total steam pressure produced by a unit or source in any calendar year (or other specified time period) produced by combusting a given heat input of fuel.")
    so2_mass_lbs = Column(REAL, comment="Sulfur dioxide emissions in pounds.")
    so2_mass_measurement_code = Column(
        ENUM_FLAG_MEASUREMENT, comment="Identifies whether the reported value of emissions was measured, calculated, or measured and substitute.")
    # so2_rate_lbs_mmbtu = Column(REAL)
    # so2_rate_measure_flg = Column(ENUM_FLAG_CALCULATED)
    nox_rate_lbs_mmbtu = Column(
        REAL, comment="The average rate at which NOx was emitted during a given time period.")
    nox_rate_measurement_code = Column(
        ENUM_NOX, comment="Identifies whether the reported value of emissions was measured, calculated, or measured and substitute.")
    nox_mass_lbs = Column(REAL, comment="Nitrogen oxide emissions in pounds.")
    nox_mass_measurement_code = Column(
        ENUM_NOX, comment="Identifies whether the reported value of emissions was measured, calculated, or measured and substitute.")
    co2_mass_tons = Column(
        REAL, comment="Carbon dioxide emissions in short tons.")
    co2_mass_measurement_code = Column(
        ENUM_FLAG_MEASUREMENT, comment="Identifies whether the reported value of emissions was measured, calculated, or measured and substitute.")
    # co2_rate_tons_mmbtu = Column(REAL)
    # co2_rate_measure_flg = Column(ENUM_FLAG_CALCULATED)
    heat_content_mmbtu = Column(
        REAL, nullable=False, comment="The measure of utilization that is calculated by multiplying the quantity of fuel by the fuel's heat content.")
    # max value is 8421
    facility_id = Column(
        SmallInteger, comment="The unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration.")
    unit_id_epa = Column(
        Integer, comment="Unique EPA identifier for each unit at a facility.")


DROP_VIEWS = ["DROP VIEW IF EXISTS hourly_emissions_epacems_view"]
CREATE_VIEWS = ["""
    CREATE VIEW hourly_emissions_epacems_view AS
    SELECT
        id,
        plant_id_eia,
        unitid,
        operating_datetime_utc,
        operating_datetime_utc::date AS operating_date,
        operating_time_hours,
        gross_load_mw,
        steam_load_1000_lbs,
        so2_mass_lbs,
        so2_mass_measurement_code,
        so2_mass_lbs / heat_content_mmbtu AS so2_rate_lbs_mmbtu,
        nox_rate_lbs_mmbtu,
        nox_rate_measurement_code,
        nox_mass_lbs,
        nox_mass_measurement_code,
        co2_mass_tons,
        co2_mass_measurement_code,
        co2_mass_tons / heat_content_mmbtu AS co2_rate_tons_mmbtu,
        heat_content_mmbtu,
        facility_id,
        unit_id_epa
    FROM hourly_emissions_epacems
    """,
                ]


def finalize(engine):
    """Finalizes the EPA CEMS table.

    This function does a few things after all the data have been written because
    it's faster to do these after the fact.

    1. Add individual indexes for operating_datetime_utc, plant_id_eia, and
       the date part of operating_datetime_utc,
    2. Add a unique index for the combination of operating_datetime_utc,
       plant_id_eia, and unitid.

    args:
        engine (sqlalchemy engine)
    """

    # List of indexes and constraints we need to create later, after loading
    # See https://stackoverflow.com/a/41254430
    # index names follow SQLAlchemy's convention ix_tablename_columnname, but
    # this doesn't matter
    indexes_to_create = [
        sa.Index("ix_hourly_emissions_epacems_operating_datetime_utc",
                 HourlyEmissions.operating_datetime_utc),
        sa.Index("ix_hourly_emissions_epacems_plant_id_eia",
                 HourlyEmissions.plant_id_eia),
        # The name that follows the pattern would be
        # ix_hourly_emissions_epacems_plant_id_eia_unitid_operating_datetime_utc
        # But that's too long.
        sa.Index("ix_plant_id_eia_unitid_operating_datetime_utc",
                 HourlyEmissions.plant_id_eia,
                 HourlyEmissions.unitid,
                 HourlyEmissions.operating_datetime_utc,
                 unique=True),
    ]
    for index in indexes_to_create:
        try:
            index.create(engine)
        except sa.exc.ProgrammingError as e:
            from warnings import warn
            warn(f"Failed to add index/constraint '{index.name}'\n" +
                 "Details:\n" + str(e))
