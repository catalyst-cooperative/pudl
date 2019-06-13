"""Database models for PUDL tables derived from EIA Form 860 Data."""

from sqlalchemy import Boolean, Integer, String, Float, Date
from sqlalchemy import Column, ForeignKey, ForeignKeyConstraint
import pudl.models.entities


class UtilityEIA860(pudl.models.entities.PUDLBase):
    """
    EIA Plants per year, listed in 923 or 860.

    A compliation of yearly plant info.
    """

    __tablename__ = 'utilities_eia860'

    id = Column(Integer, autoincrement=True, primary_key=True,
                comment="PUDL issued surrogate key.")
    utility_id_eia = Column(Integer,
                            ForeignKey('utilities_entity_eia.utility_id_eia'),
                            nullable=False, comment="EIA-assigned identification number for the company that is responsible for the day-to-day operations of the generator.")
    report_date = Column(Date, nullable=False, comment="Date reported.")
    street_address = Column(String)
    city = Column(String)
    state = Column(String,  # ENUM
                   # pudl.models.glue.us_states_canada_prov_terr,  # ENUM
                   comment="Two letter US state and territory abbreviations.")
    zip_code = Column(String)
    plants_reported_owner = Column(
        String, comment="Is the reporting entity an owner of power plants reported on Schedule 2 of the form?")
    plants_reported_operator = Column(
        String, comment="Is the reporting entity an operator of power plants reported on Schedule 2 of the form?")
    plants_reported_asset_manager = Column(
        String, comment="Is the reporting entity an asset manager of power plants reported on Schedule 2 of the form?")
    plants_reported_other_relationship = Column(
        String, comment="Does the reporting entity have any other relationship to the power plants reported on Schedule 2 of the form?")


class PlantEIA860(pudl.models.entities.PUDLBase):
    """
    EIA Plants per year, listed in 923 or 860.

    A compliation of yearly plant info.
    """

    __tablename__ = 'plants_eia860'

    id = Column(Integer, autoincrement=True, primary_key=True,
                comment="PUDL issued surrogate key.")
    plant_id_eia = Column(Integer,
                          ForeignKey('plants_entity_eia.plant_id_eia'),
                          nullable=False, comment="EIA Plant Identification number. One to five digit numeric.")
    report_date = Column(Date, nullable=False, comment="Date reported.")
    ash_impoundment = Column(
        String, comment="Is there an ash impoundment (e.g. pond, reservoir) at the plant?")
    ash_impoundment_lined = Column(
        String, comment="If there is an ash impoundment at the plant, is the impoundment lined?")
    ash_impoundment_status = Column(
        String, comment="If there is an ash impoundment at the plant, the ash impoundment status as of December 31 of the reporting year.")
    energy_storage = Column(
        String, comment="Indicates if the facility has energy storage capabilities.")
    ferc_cogen_docket_no = Column(
        String, comment="The docket number relating to the FERC qualifying facility cogenerator status.")
    ferc_exempt_wholesale_generator_docket_no = Column(
        String, comment="The docket number relating to the FERC qualifying facility exempt wholesale generator status.")
    ferc_small_power_producer_docket_no = Column(
        String, comment="The docket number relating to the FERC qualifying facility small power producer status.")
    liquefied_natural_gas_storage = Column(
        String, comment="Indicates if the facility have the capability to store the natural gas in the form of liquefied natural gas.")
    natural_gas_local_distribution_company = Column(
        String, comment="Names of Local Distribution Company (LDC), connected to natural gas burning power plants.")
    natural_gas_storage = Column(
        String, comment="Indicates if the facility have on-site storage of natural gas.")
    natural_gas_pipeline_name_1 = Column(
        String, comment="The name of the owner or operator of natural gas pipeline that connects directly to this facility or that connects to a lateral pipeline owned by this facility.")
    natural_gas_pipeline_name_2 = Column(
        String, comment="The name of the owner or operator of natural gas pipeline that connects directly to this facility or that connects to a lateral pipeline owned by this facility.")
    natural_gas_pipeline_name_3 = Column(
        String, comment="The name of the owner or operator of natural gas pipeline that connects directly to this facility or that connects to a lateral pipeline owned by this facility.")
    net_metering = Column(
        String, comment="Did this plant have a net metering agreement in effect during the reporting year?  (Only displayed for facilities that report the sun or wind as an energy source).")
    pipeline_notes = Column(
        String, comment="Additional owner or operator of natural gas pipeline.")
    regulatory_status_code = Column(
        String, comment="Indicates whether the plant is regulated or non-regulated.")
    transmission_distribution_owner_id = Column(
        String, comment="EIA-assigned code for owner of transmission/distribution system to which the plant is interconnected.")
    transmission_distribution_owner_name = Column(
        String, comment="Name of the owner of the transmission or distribution system to which the plant is interconnected.")
    transmission_distribution_owner_state = Column(
        String, comment="State location for owner of transmission/distribution system to which the plant is interconnected.")
    utility_id_eia = Column(
        Integer, comment="EIA-assigned identification number for the company that is responsible for the day-to-day operations of the generator.")
    water_source = Column(
        String, comment="Name of water source associater with the plant.")


class GeneratorEIA860(pudl.models.entities.PUDLBase):
    """
    EIA generators per year, listed in 923 or 860.

    A compilation of EIA generators ids and yearly info.
    """

    __tablename__ = 'generators_eia860'
    __table_args__ = (ForeignKeyConstraint(
        ['plant_id_eia', 'generator_id'],
        ['generators_entity_eia.plant_id_eia',
         'generators_entity_eia.generator_id']),)

    id = Column(Integer, autoincrement=True, primary_key=True,
                comment="PUDL issued surrogate key.")
    plant_id_eia = Column(Integer, nullable=False,
                          comment="EIA Plant Identification number. One to five digit numeric.")
    generator_id = Column(String, nullable=False,
                          comment="Generator identification number.")
    report_date = Column(Date, nullable=False, comment="Date reported.")
    operational_status_code = Column(
        String, comment="The operating status of the generator.")
    operational_status = Column(
        String, comment="The operating status of the generator. This is based on which tab the generator was listed in in EIA 860.")
    ownership_code = Column(
        String, comment="Identifies the ownership for each generator.")
    capacity_mw = Column(
        Float, comment="The highest value on the generator nameplate in megawatts rounded to the nearest tenth.")
    summer_capacity_mw = Column(Float, comment="The net summer capacity.")
    winter_capacity_mw = Column(Float, comment="The net winter capacity.")
    energy_source_code_1 = Column(
        String, comment="The code representing the most predominant type of energy that fuels the generator.")
    energy_source_code_2 = Column(
        String, comment="The code representing the second most predominant type of energy that fuels the generator")
    energy_source_code_3 = Column(
        String, comment="The code representing the third most predominant type of energy that fuels the generator")
    energy_source_code_4 = Column(
        String, comment="The code representing the fourth most predominant type of energy that fuels the generator")
    energy_source_code_5 = Column(
        String, comment="The code representing the fifth most predominant type of energy that fuels the generator")
    energy_source_code_6 = Column(
        String, comment="The code representing the sixth most predominant type of energy that fuels the generator")
    fuel_type_code_pudl = Column(
        String, comment="Standardized fuel codes in PUDL.")
    multiple_fuels = Column(
        Boolean, comment="Can the generator burn multiple fuels?")
    deliver_power_transgrid = Column(
        Boolean, comment="Indicate whether the generator can deliver power to the transmission grid.")
    syncronized_transmission_grid = Column(
        Boolean, comment="Indicates whether standby generators (SB status) can be synchronized to the grid.")
    turbines_num = Column(
        Integer, comment="Number of wind turbines, or hydrokinetic buoys.")
    # sector_name = Column(String)
    # sector_id = Column(Integer)
    planned_modifications = Column(
        Boolean, comment="Indicates whether there are any planned capacity uprates/derates, repowering, other modifications, or generator retirements scheduled for the next 5 years.")
    planned_net_summer_capacity_uprate_mw = Column(
        Float, comment="Increase in summer capacity expected to be realized from the modification to the equipment.")
    planned_net_winter_capacity_uprate_mw = Column(
        Float, comment="Increase in winter capacity expected to be realized from the uprate modification to the equipment.")
    planned_uprate_date = Column(
        Date, comment="Planned effective date that the generator is scheduled to enter operation after the uprate modification.")
    planned_net_summer_capacity_derate_mw = Column(
        Float, comment="Decrease in summer capacity expected to be realized from the derate modification to the equipment.")
    planned_net_winter_capacity_derate_mw = Column(
        Float, comment="Decrease in winter capacity expected to be realized from the derate modification to the equipment.")
    planned_derate_date = Column(
        Date, comment="Planned effective month that the generator is scheduled to enter operation after the derate modification.")
    planned_new_prime_mover_code = Column(
        String, comment="New prime mover for the planned repowered generator.")
    planned_energy_source_code_1 = Column(
        String, comment="New energy source code for the planned repowered generator.")
    planned_repower_date = Column(
        Date, comment="Planned effective date that the generator is scheduled to enter operation after the repowering is complete.")
    other_planned_modifications = Column(
        Boolean, comment="Indicates whether there are there other modifications planned for the generator.")
    other_modifications_date = Column(
        Date, comment="Planned effective date that the generator is scheduled to enter commercial operation after any other planned modification is complete.")
    planned_retirement_date = Column(
        Date, comment="Planned effective date of the scheduled retirement of the generator.")
    carbon_capture = Column(
        Boolean, comment="Indicates whether the generator uses carbon capture technology.")
    startup_source_code_1 = Column(
        String, comment="The code representing the first, second, third or fourth start-up and flame stabilization energy source used by the combustion unit(s) associated with this generator.")
    startup_source_code_2 = Column(
        String, comment="The code representing the first, second, third or fourth start-up and flame stabilization energy source used by the combustion unit(s) associated with this generator.")
    startup_source_code_3 = Column(
        String, comment="The code representing the first, second, third or fourth start-up and flame stabilization energy source used by the combustion unit(s) associated with this generator.")
    startup_source_code_4 = Column(
        String, comment="The code representing the first, second, third or fourth start-up and flame stabilization energy source used by the combustion unit(s) associated with this generator.")
    technology_description = Column(
        String, comment="High level description of the technology used by the generator to produce electricity.")
    turbines_inverters_hydrokinetics = Column(
        String, comment="Number of wind turbines, or hydrokinetic buoys.")
    time_cold_shutdown_full_load_code = Column(
        String, comment="The minimum amount of time required to bring the unit to full load from shutdown.")
    planned_new_capacity_mw = Column(
        Float, comment="The expected new namplate capacity for the generator.")
    cofire_fuels = Column(Boolean, comment="Can the generator co-fire fuels?.")
    switch_oil_gas = Column(
        Boolean, comment="Indicates whether the generator switch between oil and natural gas.")
    nameplate_power_factor = Column(
        Float, comment="The nameplate power factor of the generator.")
    minimum_load_mw = Column(
        Float, comment="The minimum load at which the generator can operate at continuosuly.")
    uprate_derate_during_year = Column(
        Boolean, comment="Was an uprate or derate completed on this generator during the reporting year?")
    uprate_derate_completed_date = Column(
        Date, comment="The date when the uprate or derate was completed.")
    current_planned_operating_date = Column(
        Date, comment="The most recently updated effective date on which the generator is scheduled to start operation")
    summer_estimated_capability_mw = Column(
        Float, comment="EIA estimated summer capacity (in MWh).")
    winter_estimated_capability_mw = Column(
        Float, comment="EIA estimated winter capacity (in MWh).")
    operating_switch = Column(
        String, comment="Indicates whether the proposed unit will be able to switch fuels while operating.")
    retirement_date = Column(
        Date, comment="Date of the scheduled or effected retirement of the generator.")


class BoilerGeneratorAssociationEIA860(pudl.models.entities.PUDLBase):
    """
    EIA boiler generator associations.

    Compiled from 860 and 923.
    """

    __tablename__ = 'boiler_generator_assn_eia860'
    __table_args__ = (ForeignKeyConstraint(
        ['plant_id_eia', 'generator_id'],
        ['generators_entity_eia.plant_id_eia',
         'generators_entity_eia.generator_id']),)

    id = Column(Integer, autoincrement=True, primary_key=True,
                comment="PUDL issued surrogate key.")
    plant_id_eia = Column(Integer, nullable=False,
                          comment="EIA Plant Identification number. One to five digit numeric.")
    report_date = Column(Date, nullable=False)
    generator_id = Column(String)
    boiler_id = Column(String)
    unit_id_eia = Column(String)
    unit_id_pudl = Column(Integer, nullable=False)
    bga_source = Column(String)


class OwnershipEIA860(pudl.models.entities.PUDLBase):
    """The schedule of generator ownership shares from EIA860."""

    __tablename__ = 'ownership_eia860'
    __table_args__ = (ForeignKeyConstraint(
        ['plant_id_eia', 'generator_id'],
        ['generators_entity_eia.plant_id_eia',
         'generators_entity_eia.generator_id']),)

    id = Column(Integer, autoincrement=True, primary_key=True,
                comment="PUDL issued surrogate key.")
    report_date = Column(Date, nullable=False, comment="Date reported.")
    utility_id_eia = Column(Integer,
                            ForeignKey('utilities_entity_eia.utility_id_eia'),
                            nullable=False, comment="EIA-assigned identification number for the company that is responsible for the day-to-day operations of the generator.")
    plant_id_eia = Column(Integer, nullable=False,
                          comment="EIA Plant Identification number. One to five digit numeric.")  # FK
    generator_id = Column(String, nullable=False,
                          comment="Generator identification number.")
    operational_status_code = Column(
        String, comment="The operating status of the generator.")
    owner_utility_id_eia = Column(
        Integer, nullable=False, comment="EIA-assigned owner's identification number.")  # FK utility_id_eia
    owner_name = Column(String, comment="Name of owner.")
    owner_state = Column(
        pudl.models.glue.us_states_canada_prov_terr,  # ENUM
        comment="Two letter US state and territory abbreviations."
    )
    owner_city = Column(String, comment="City of owner.")
    owner_street_address = Column(String, comment="Steet address of owner.")
    owner_zip_code = Column(String, comment="Zip code of owner.")
    fraction_owned = Column(Float, comment="Percent of generator ownership.")
