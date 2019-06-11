"""Database models for PUDL tables derived from EIA Form 923 Data."""

from sqlalchemy import Integer, String, Float, Date, Enum
from sqlalchemy import Column, ForeignKey, ForeignKeyConstraint
import pudl.models.entities
import pudl.models.glue
import pudl.constants

firm_interrupt = Enum('firm', 'interruptible', name='firm_interrupt')

###########################################################################
# EIA Form 923 tables which represent constants or metadata
# (e.g. fuel types and fuel units)
###########################################################################


class CoalMineEIA923(pudl.models.entities.PUDLBase):
    """Information pertaining to individual coal mines listed in EIA 923."""

    __tablename__ = 'coalmine_eia923'
    id = Column(Integer, primary_key=True)  # surrogate key
    mine_name = Column(String)
    mine_type_code = Column(
        Enum(*pudl.constants.coalmine_type_eia923.keys(),
             name='coalmine_type_eia923'),
        comment="Type of mine. P: Preparation plant, U: Underground, S: Surface, SU: Mostly Surface with some Underground, US: Mostly Underground with some Surface."
    )
    state = Column(
        Enum(*pudl.constants.coalmine_country_eia923.values(),
             *pudl.constants.us_states.keys(),
             name="coalmine_location_eia923"),
        comment="Two letter US state abbreviations and three letter ISO-3166-1 country codes for international mines."
    )
    county_id_fips = Column(Integer)
    mine_id_msha = Column(Integer)


class FuelTypeEIA923(pudl.models.entities.PUDLBase):
    """
    Static list of fuel types used by EIA in Form 923.

    Enumerated on Page 7 of EIA Form 923.
    """

    __tablename__ = 'fuel_type_eia923'
    abbr = Column(String, primary_key=True)
    fuel_type = Column(String, nullable=False)
    fuel_unit = Column(
        Enum(*pudl.constants.fuel_units_eia923,
             name="fuel_units_eia923"),
        comment="The type of physical units fuel consumption is reported in. All consumption is reported in either short tons for solids, thousands of cubic feet for gases, or barrels for liquids. "
    )


class FuelTypeAER(pudl.models.entities.PUDLBase):
    """Static list of fuel types using AER codes, reported in EIA Form 923."""

    __tablename__ = 'fuel_type_aer_eia923'
    abbr = Column(String, primary_key=True)
    fuel_type = Column(String, nullable=False)


class PrimeMoverEIA923(pudl.models.entities.PUDLBase):
    """Static list of prime movers used by EIA in Form 923."""

    __tablename__ = 'prime_movers_eia923'
    abbr = Column(String, primary_key=True)
    prime_mover = Column(String, nullable=False)


class EnergySourceEIA923(pudl.models.entities.PUDLBase):
    """Fuel code associated with fuel receipts in EIA Form 923."""

    __tablename__ = 'energy_source_eia923'
    abbr = Column(String, primary_key=True)
    source = Column(String, nullable=False)


class NaturalGasTransportEIA923(pudl.models.entities.PUDLBase):
    """Contract type for natural gas capacity service."""

    __tablename__ = 'natural_gas_transport_eia923'
    abbr = Column(String, primary_key=True)
    status = Column(String, nullable=False)


class TransportModeEIA923(pudl.models.entities.PUDLBase):
    """Mode used for longest & 2nd longest distance in EIA Form 923."""

    __tablename__ = 'transport_modes_eia923'
    abbr = Column(String, primary_key=True)
    mode = Column(String, nullable=False)


###########################################################################
# Tables which represent EIA Form 923 data. E.g. Fuel Receipts.
###########################################################################
class GenerationFuelEIA923(pudl.models.entities.PUDLBase):
    """
    Monthly fuel consumption and electricity generation by plant.

    Reported on Page 1 of EIA Form 923.
    """

    __tablename__ = 'generation_fuel_eia923'

    id = Column(Integer, autoincrement=True, primary_key=True, comment="PUDL issued surrogate key.")  # surrogate key
    plant_id_eia = Column(Integer,
                          ForeignKey('plants_entity_eia.plant_id_eia'),
                          nullable=False, comment="EIA Plant Identification number. One to five digit numeric.")
    report_date = Column(Date, nullable=False, comment="Date reported.")
    # TODO: Should nuclear_unit_id really be here? It's kind of part of the
    # plant_id... but also unit_id.  Seems weird but necessary to uniquely
    # identify the records as reported.
    nuclear_unit_id = Column(Integer, comment="For nuclear plants only, the unit number .One digit numeric. Nuclear plants are the only type of plants for which data are shown explicitly at the generating unit level.")
    fuel_type = Column(String,
                       ForeignKey('fuel_type_eia923.abbr'),
                       nullable=False, comment="The fuel code reported to EIA. Two or three letter alphanumeric.")
    fuel_type_code_pudl = Column(String, comment="Standardized fuel codes in PUDL.")
    fuel_type_code_aer = Column(
        String, ForeignKey('fuel_type_aer_eia923.abbr'), comment="A partial aggregation of the reported fuel type codes into larger categories used by EIA in, for example, the Annual Energy Review (AER).Two or three letter alphanumeric.")
    prime_mover_code = Column(String,
                              ForeignKey('prime_movers_eia923.abbr'),
                              nullable=False, comment="Type of prime mover.")
    fuel_consumed_units = Column(Float, comment="Consumption of the fuel type in physical units. Note: this is the total quantity consumed for both electricity and, in the case of combined heat and power plants, process steam production.")
    fuel_consumed_for_electricity_units = Column(Float, comment="Consumption for electric generation of the fuel type in physical units.")
    fuel_mmbtu_per_unit = Column(Float, comment="Heat content of the fuel in millions of Btus per physical unit.")
    fuel_consumed_mmbtu = Column(Float, comment="Total consumption of fuel in physical units, year to date. Note: this is the total quantity consumed for both electricity and, in the case of combined heat and power plants, process steam production.")
    fuel_consumed_for_electricity_mmbtu = Column(Float, comment="Total consumption of fuel to produce electricity, in physical units, year to date.")
    net_generation_mwh = Column(Float, comment="Net generation, year to date in megawatthours (MWh). This is total electrical output net of station service.  In the case of combined heat and power plants, this value is intended to include internal consumption of electricity for the purposes of a production process, as well as power put on the grid.")


class BoilerFuelEIA923(pudl.models.entities.PUDLBase):
    """Monthly fuel consumption by boiler reported on Page 3 of EIA 923."""

    __tablename__ = 'boiler_fuel_eia923'
    # __table_args__ = (ForeignKeyConstraint(
    #    ['plant_id_eia', 'boiler_id'],
    #    ['boilers_entity_eia.plant_id_eia',
    #     'boilers_entity_eia.boiler_id']),)

    # Each month, for each unique combination of boiler id and prime mover and
    # fuel, there is one report for each boiler unit in each plant.

    id = Column(Integer, autoincrement=True, primary_key=True,
                comment="PUDL issued surrogate key.")  # surrogate key
    plant_id_eia = Column(Integer, nullable=False,
                          comment="EIA Plant Identification number. \
                          One to five digit numeric.")
    boiler_id = Column(String, nullable=False, comment=
                       "Boiler identification code. Alphanumeric.")
    # prime_mover_code = Column(String, ForeignKey('prime_movers_eia923.abbr'),
    #                          nullable=False, comment="Type of prime mover.")
    fuel_type_code = Column(String, ForeignKey('fuel_type_eia923.abbr'),
                            nullable=False, comment="The fuel code reported \
                            to EIA. Two or three letter alphanumeric.")
    fuel_type_code_pudl = Column(String, comment="Standardized fuel codes \
                                 in PUDL.")
    report_date = Column(Date, nullable=False, comment="Date reported.")
    fuel_consumed_units = Column(Float, comment="Consumption of the fuel type \
                                 in physical units. Note: this is the total \
                                 quantity consumed for both electricity and, \
                                 in the case of combined heat and power \
                                 plants, process steam production.")
    fuel_mmbtu_per_unit = Column(Float, comment="Heat content of the fuel in \
                                 millions of Btus per physical unit.")
    sulfur_content_pct = Column(Float, comment="Sulfur content percentage by \
                                weight to the nearest 0.01 percent.")
    ash_content_pct = Column(Float, comment="Ash content percentage by weight \
                             to the nearest 0.1 percent.")


class GenerationEIA923(pudl.models.entities.PUDLBase):
    """Monthly electricity generation by generator from EIA923 Page 4."""

    __tablename__ = 'generation_eia923'
    __table_args__ = (ForeignKeyConstraint(
        ['plant_id_eia', 'generator_id'],
        ['generators_entity_eia.plant_id_eia',
         'generators_entity_eia.generator_id']),)

    # Each month, for each unique combination of generator id and prime mover
    # and fuel,there is one report for each generator unit in each plant.
    id = Column(Integer, autoincrement=True, primary_key=True, comment="PUDL issued surrogate key.")  # surrogate key
    plant_id_eia = Column(Integer, nullable=False, comment="EIA Plant Identification number. One to five digit numeric.")
    # TODO remove prime_mover since it's specific to generator_id?
    # prime_mover_code = Column(String, ForeignKey('prime_movers_eia923.abbr'),
    #                          nullable=False, comment="Type of prime mover.")
    # TODO: Add FK constraint refering to (plant_id, generator_id) in the
    # generators_eia923 table.  Or at least give it a shot.
    generator_id = Column(String, nullable=False, comment="Generator identification code. Alphanumeric")
    report_date = Column(Date, nullable=False, comment="Date reported.")
    net_generation_mwh = Column(Float, comment="Net generation for specified period in megawatthours (MWh).")


class FuelReceiptsCostsEIA923(pudl.models.entities.PUDLBase):
    """Fuel receipts & costs by plant &purchase from Page 5 of EIA Form 923."""

    __tablename__ = 'fuel_receipts_costs_eia923'

    # surrogate key
    fuel_receipt_id = Column(Integer, primary_key=True, autoincrement=True,
                             comment="PUDL fuel receipt identification number.")
    plant_id_eia = Column(Integer,
                          ForeignKey('plants_entity_eia.plant_id_eia'),
                          nullable=False,
                          comment="EIA Plant Identification number. One to \
                          five digit numeric.")
    report_date = Column(Date, nullable=False, comment="Date reported.")
    contract_type_code = Column(
        Enum(*pudl.constants.contract_type_eia923.keys(),
             name="contract_type_eia923"),
        comment="Purchase type under which receipts occurred in the reporting \
        month. C: Contract, NC: New Contract, S: Spot Purchase, T: Tolling \
        Agreement."
    )
    contract_expiration_date = Column(Date, comment="Date contract expires. \
                                      Format:  MMYY.")
    energy_source_code = Column(
        String, ForeignKey('energy_source_eia923.abbr'), comment="The fuel \
                               code associated with the fuel receipt. \
                               Two or three character alphanumeric.")
    fuel_type_code_pudl = Column(String, comment="Standardized fuel codes in \
                                 PUDL.")
    fuel_group_code = Column(
        Enum(*pudl.constants.fuel_group_eia923, name="fuel_group_eia923"),
        comment="Groups the energy sources into fuel groups that are located \
                in the Electric Power Monthly:  Coal, Natural Gas, Petroleum, \
                Petroleum Coke.")
    fuel_group_code_simple = Column(String, comment="Simplified grouping of \
                                    fuel_group_code, with Coal and Petroluem \
                                    Coke as well as Natural Gas and Other Gas \
                                    grouped together.")
    mine_id_pudl = Column(Integer, ForeignKey('coalmine_eia923.id'),
                          comment="PUDL mine identification number.")
    supplier_name = Column(String, nullable=False, comment="Company that sold \
                           the fuel to the plant or, in the case of Natural \
                           Gas, pipline owner.")
    fuel_qty_units = Column(Float, nullable=False, comment="Quanity of fuel \
                            received in tons, barrel, or Mcf.")
    heat_content_mmbtu_per_unit = Column(Float, nullable=False,
                                         comment="Heat content of the fuel in \
                                         millions of Btus per physical unit to \
                                         the nearest 0.01 percent.")
    sulfur_content_pct = Column(Float, nullable=False, comment="Sulfur content percentage by weight to the nearest 0.01 percent.")
    ash_content_pct = Column(Float, nullable=False, comment="Ash content percentage by weight to the nearest 0.1 percent.")
    mercury_content_ppm = Column(Float, comment="Mercury content in parts per million (ppm) to the nearest 0.001 ppm.")
    fuel_cost_per_mmbtu = Column(Float, comment="All costs incurred in the purchase and delivery of the fuel to the plant in cents per million Btu(MMBtu) to the nearest 0.1 cent.")
    primary_transportation_mode_code = Column(
        String,
        ForeignKey('transport_modes_eia923.abbr'),
        comment="Transportation mode for the longest distance transported.")
    secondary_transportation_mode_code = Column(
        String,
        ForeignKey('transport_modes_eia923.abbr'),
        comment="Transportation mode for the second longest distance transported.")
    natural_gas_transport_code = Column(firm_interrupt, comment="Contract type for natural gas transportation service.")  # Enum
    natural_gas_delivery_contract_type_code = Column(firm_interrupt, comment="Contract type for natrual gas delivery service:")  # Enum
    moisture_content_pct = Column(Float)
    chlorine_content_ppm = Column(Float)
