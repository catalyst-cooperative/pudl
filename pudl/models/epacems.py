"""Database models for PUDL tables derived from EPA CEMS Data."""

import sqlalchemy as sa
from sqlalchemy import Integer, SmallInteger, String
from sqlalchemy import REAL, DateTime, Column, Enum
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
    id = Column(Integer, autoincrement=True, primary_key=True)  # surrogate key
    # state = Column(
    #     pudl.models.glue.us_states_lower48,  # ENUM
    #     comment="Two letter US state and territory abbreviations."
    # )
    # plant_name = Column(String, nullable=False)
    # TODO: Set up foreign-key link to EIA plant ID
    plant_id_eia = Column(Integer, nullable=False)
    unitid = Column(String, nullable=False)
    operating_datetime = Column(DateTime, nullable=False)
    operating_time_hours = Column(REAL)
    gross_load_mw = Column(REAL, nullable=False)
    steam_load_1000_lbs = Column(REAL)
    so2_mass_lbs = Column(REAL)
    so2_mass_measurement_code = Column(ENUM_FLAG_MEASUREMENT)
    # so2_rate_lbs_mmbtu = Column(REAL)
    # so2_rate_measure_flg = Column(ENUM_FLAG_CALCULATED)
    nox_rate_lbs_mmbtu = Column(REAL)
    nox_rate_measurement_code = Column(ENUM_NOX)
    nox_mass_lbs = Column(REAL)
    nox_mass_measurement_code = Column(ENUM_NOX)
    co2_mass_tons = Column(REAL)
    co2_mass_measurement_code = Column(ENUM_FLAG_MEASUREMENT)
    # co2_rate_tons_mmbtu = Column(REAL)
    # co2_rate_measure_flg = Column(ENUM_FLAG_CALCULATED)
    heat_content_mmbtu = Column(REAL, nullable=False)
    facility_id = Column(SmallInteger)  # max value is 8421
    unit_id_epa = Column(Integer)


DROP_VIEWS = ["DROP VIEW IF EXISTS hourly_emissions_epacems_view"]
CREATE_VIEWS = ["""
    CREATE VIEW hourly_emissions_epacems_view AS
    SELECT
        id,
        plant_id_eia,
        unitid,
        operating_datetime,
        operating_datetime::date AS operating_date,
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
    """Finalize the EPA CEMS table

    args: engine (sqlalchemy engine)

    This function does a few things after all the data have been written because
    it's faster to do these after the fact.
    1. Add individual indexes for operating_datetime, plant_id_eia, and
       the date part of operating_datetime,
    2. Add a unique index for the combination of operating_datetime,
       plant_id_eia, and unitid.
    """

    # List of indexes and constraints we need to create later, after loading
    # See https://stackoverflow.com/a/41254430
    # index names follow SQLAlchemy's convention ix_tablename_columnname, but
    # this doesn't matter
    indexes_to_create = [
        sa.Index("ix_hourly_emissions_epacems_operating_datetime",
                 HourlyEmissions.operating_datetime),
        sa.Index("ix_hourly_emissions_epacems_plant_id_eia",
                 HourlyEmissions.plant_id_eia),
        sa.Index("ix_hourly_emissions_epacems_opperating_date_part",
                 sa.cast(HourlyEmissions.operating_datetime, sa.Date)),
        # The name that follows the pattern would be
        # ix_hourly_emissions_epacems_plant_id_eia_unitid_operating_datetime
        # But that's too long.
        sa.Index("ix_plant_id_eia_unitid_operating_datetime",
                 HourlyEmissions.plant_id_eia,
                 HourlyEmissions.unitid,
                 HourlyEmissions.operating_datetime,
                 unique=True),
    ]
    for index in indexes_to_create:
        try:
            index.create(engine)
        except sa.exc.ProgrammingError as e:
            from warnings import warn
            warn(f"Failed to add index/constraint '{index.name}'\n" +
                 "Details:\n" + str(e))
