"""
The Public Utility Data Liberation (PUDL) project core module.

The PUDL project integrates several different public data sets into one well
normalized database allowing easier access and interaction between all of them.
This module defines database tables using the SQLAlchemy Object Relational
Mapper (ORM) and initializes the database from several sources:

 - US Energy Information Agency (EIA):
   - Form 860 (eia860)
   - Form 861 (eia861)
   - Form 923 (eia923)
 - US Federal Energy Regulatory Commission (FERC):
   - Form 1 (ferc1)
   - Form 714 (ferc714)
 - US Environmental Protection Agency (EPA):
   - Air Market Program Data (epaampd)
   - Greenhouse Gas Reporting Program (epaghgrp)
"""

import pandas as pd
import numpy as np
import postgres_copy
import os.path

from sqlalchemy.sql import select
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine
from sqlalchemy import Integer, String, Numeric, Boolean, Float

from pudl import settings
from pudl.ferc1 import db_connect_ferc1, cleanstrings, ferc1_meta
from pudl.eia923 import get_eia923_page, yearly_to_monthly_eia923
from pudl.eia923 import get_eia923_file, get_eia923_xlsx, cleanstringsEIA923
from pudl.constants import ferc1_fuel_strings, us_states, prime_movers
from pudl.constants import ferc1_fuel_unit_strings, rto_iso
from pudl.constants import ferc1_plant_kind_strings, ferc1_type_const_strings
from pudl.constants import ferc1_default_tables, ferc1_pudl_tables
from pudl.constants import ferc1_working_tables
from pudl.constants import ferc_electric_plant_accounts
from pudl.constants import ferc_accumulated_depreciation
from pudl.constants import month_dict_eia923

# Tables that hold constant values:
from pudl.models import Fuel, FuelUnit, Month, Quarter, PrimeMover, Year
from pudl.models import State, RTOISO
from pudl.constants import census_region, nerc_region
from pudl.constants import fuel_type_aer_eia923, respondent_frequency_eia923

# EIA specific lists that will get moved over to models_eia923.py
from pudl.constants import sector_eia, contract_type_eia923
from pudl.constants import fuel_type_eia923, prime_movers_eia923
from pudl.constants import fuel_units_eia923, energy_source_eia923
from pudl.constants import fuel_group_eia923, aer_fuel_type_strings
from pudl.constants import coalmine_type_eia923, coalmine_state_eia923
from pudl.constants import natural_gas_transport_eia923
from pudl.constants import transport_modes_eia923
from pudl.constants import eia923_pudl_tables

# Tables that hold constant values:
from pudl.models import Fuel, FuelUnit, Month, Quarter, PrimeMover, Year
from pudl.models import State, RTOISO, CensusRegion, NERCRegion

# EIA specific lists stored in models_eia923.py
from pudl.models_eia923 import SectorEIA, ContractTypeEIA923
from pudl.models_eia923 import EnergySourceEIA923
from pudl.models_eia923 import CoalMineTypeEIA923, CoalMineStateEIA923
from pudl.models_eia923 import NaturalGasTransportEIA923
from pudl.models_eia923 import TransportModeEIA923
from pudl.models_eia923 import RespondentFrequencyEIA923
from pudl.models_eia923 import PrimeMoverEIA923, FuelTypeAER
from pudl.models_eia923 import FuelTypeEIA923, AERFuelCategoryEIA923
from pudl.models_eia923 import FuelGroupEIA923, FuelUnitEIA923
from pudl.models_eia923 import PlantInfoEIA923, BoilersEIA923
from pudl.models_eia923 import BoilerFuelEIA923

# Tables that hold "glue" connecting FERC1 & EIA923 to each other:
from pudl.models import Utility, UtilityFERC1, UtilityEIA923
from pudl.models import Plant, PlantFERC1, PlantEIA923
from pudl.models import UtilPlantAssn

# The declarative_base object that contains our PUDL DB MetaData
from pudl.models import PUDLBase


###############################################################################
###############################################################################
# DATABASE CONNECTION & HELPER FUNCTIONS
###############################################################################
###############################################################################


def db_connect_pudl(testing=False):
    """Connect to the PUDL database using global settings from settings.py."""
    if(testing):
        return create_engine(URL(**settings.DB_PUDL_TEST))
    else:
        return create_engine(URL(**settings.DB_PUDL))


def create_tables_pudl(engine):
    """Create the tables associated with the PUDL Database."""
    PUDLBase.metadata.create_all(engine)


def drop_tables_pudl(engine):
    """Drop all the tables associated with the PUDL Database and start over."""
    PUDLBase.metadata.drop_all(engine)

###############################################################################
###############################################################################
#   OTHER HELPER FUNCTIONS
###############################################################################
###############################################################################


def fix_int_na(col, float_na=np.nan, int_na=-1, str_na=''):
    """
    Convert a dataframe column from float to string for CSV export.

    Numpy doesn't have a real NA value for integers. When pandas stores integer
    data which has NA values, it thus upcasts integers to floating point
    values, using np.nan values for NA. However, in order to dump some of our
    dataframes to CSV files that are suitable for loading into postgres
    directly, we need to write out integer formatted numbers, with empty
    strings as the NA value. This function replaces np.nan values with a
    sentiel value, converts the column to integers, and then to strings,
    finally replacing the sentinel value with the desired NA string.

    Args:
        col (pandas.Series): The DataFrame column that needs to be
            reformatted for output.
        float_na (float): The floating point value to be interpreted as NA and
            replaced in col.
        int_na (int): Sentinel value to substitute for float_na prior to
            conversion of the column to integers.
        str_na (str): String value to substitute for int_na after the column
            has been converted to strings.

    Returns:
        str_col (pandas.Series): a column containing the same values and lack
            of values as the col argument, but stored as strings that are
            compatible with the postgresql COPY FROM command.
    """
    return(col.replace(float_na, int_na).
           astype(int).
           astype(str).
           replace(str(int_na), str_na))


def csv_dump_load(df, table_name, engine, csvdir='', keep_csv=True):
    """
    Write a dataframe to CSV and load it into postgresql using COPY FROM.

    The fastest way to load a bunch of records is using the database's native
    text file copy function.  This function dumps a given dataframe out to a
    CSV file, and then loads it into the specified table using a sqlalchemy
    wrapper around the postgresql COPY FROM command, called postgres_copy.

    Args:
        df (pandas.DataFrame): The DataFrame which is to be dumped to CSV and
            loaded into the database. All DataFrame columns must have exactly
            the same names as the database fields they are meant to populate,
            and all column data types must be directly compatible with the
            database fields they are meant to populate. Do any cleanup before
            you call this function.
        table_name (str): The exact name of the database table which the
            DataFrame df is going to be used to populate. It will be used both
            to look up an SQLAlchemy table object in the PUDLBase metadata
            object, and to name the CSV file.
        engine (sqlalchemy.engine): SQLAlchemy database engine, which will be
            used to pull the CSV output into the database.
        csvdir (str): Path to the directory into which the CSV files should be
            output (and saved, if they are being kept).
        keep_csv (bool): True if the CSV output should be saved after the data
            has been loaded into the database. False if they should be deleted.

    Returns: Nothing.
    """
    import postgres_copy
    import os

    csvfile = os.path.join(csvdir, table_name + '.csv')
    df.to_csv(csvfile, index=False)
    tbl = PUDLBase.metadata.tables[table_name]
    postgres_copy.copy_from(open(csvfile, 'r'), tbl, engine,
                            columns=tuple(df.columns),
                            format='csv', header=True, delimiter=',')
    if not keep_csv:
        os.remove(csvfile)

###############################################################################
###############################################################################
#   BEGIN INGESTING STATIC & INFRASTRUCTURE TABLES
###############################################################################
###############################################################################


def ingest_static_tables(engine):
    """
    Populate static PUDL tables with constants for use as foreign keys.

    There are many values specified within the data that are essentially
    constant, but which we need to store for data validation purposes, for use
    as foreign keys.  E.g. the list of valid EIA fuel type codes, or the
    possible state and country codes indicating a coal delivery's location of
    origin. For now these values are primarily stored in a large collection of
    lists, dictionaries, and dataframes which are specified in the
    pudl.constants module.  This function uses those data structures to
    populate a bunch of small infrastructural tables within the PUDL DB.

    Args:
        engine (sqlalchemy.engine): A database engine with which to connect to
            to the PUDL DB.

    Returns: Nothing.
    """
    from sqlalchemy.orm import sessionmaker

    PUDL_Session = sessionmaker(bind=engine)
    pudl_session = PUDL_Session()

    # Populate tables with static data from above.
    pudl_session.add_all([Fuel(name=f) for f in ferc1_fuel_strings.keys()])
    pudl_session.add_all([FuelUnit(unit=u) for u in
                          ferc1_fuel_unit_strings.keys()])
    pudl_session.add_all([Month(month=i + 1) for i in range(12)])
    pudl_session.add_all(
        [Quarter(q=i + 1, end_month=3 * (i + 1)) for i in range(4)])
    pudl_session.add_all([PrimeMover(prime_mover=pm) for pm in prime_movers])
    pudl_session.add_all([RTOISO(abbr=k, name=v) for k, v in rto_iso.items()])
    pudl_session.add_all([Year(year=yr) for yr in range(1994, 2017)])
    pudl_session.add_all(
        [CensusRegion(abbr=k, name=v) for k, v in census_region.items()])
    pudl_session.add_all(
        [NERCRegion(abbr=k, name=v) for k, v in nerc_region.items()])
    pudl_session.add_all(
        [RespondentFrequencyEIA923(abbr=k, unit=v)
         for k, v in respondent_frequency_eia923.items()])
    pudl_session.add_all(
        [SectorEIA(id=k, name=v) for k, v in sector_eia.items()])
    pudl_session.add_all(
        [ContractTypeEIA923(abbr=k, contract_type=v)
         for k, v in contract_type_eia923.items()])
    pudl_session.add_all(
        [FuelTypeEIA923(abbr=k, fuel_type=v)
         for k, v in fuel_type_eia923.items()])
    pudl_session.add_all(
        [PrimeMoverEIA923(abbr=k, prime_mover=v)
         for k, v in prime_movers_eia923.items()])
    pudl_session.add_all(
        [FuelUnitEIA923(abbr=k, unit=v)
         for k, v in fuel_units_eia923.items()])
    pudl_session.add_all(
        [FuelTypeAER(abbr=k, fuel_type=v)
         for k, v in fuel_type_aer_eia923.items()])
    pudl_session.add_all(
        [FuelGroupEIA923(group=gr) for gr in fuel_group_eia923])
    pudl_session.add_all(
        [EnergySourceEIA923(abbr=k, source=v)
         for k, v in energy_source_eia923.items()])
    pudl_session.add_all(
        [CoalMineTypeEIA923(abbr=k, name=v)
         for k, v in coalmine_type_eia923.items()])
    pudl_session.add_all(
        [CoalMineStateEIA923(abbr=k, state=v)
         for k, v in coalmine_state_eia923.items()])
    pudl_session.add_all(
        [CoalMineStateEIA923(abbr=k, state=v)
         for k, v in us_states.items()])  # is this right way to add these?
    pudl_session.add_all(
        [TransportModeEIA923(abbr=k, mode=v)
         for k, v in transport_modes_eia923.items()])
    pudl_session.add_all(
        [NaturalGasTransportEIA923(abbr=k, status=v)
         for k, v in natural_gas_transport_eia923.items()])
    pudl_session.add_all(
        [AERFuelCategoryEIA923(name=k)
         for k in aer_fuel_type_strings.keys()])

    # States dictionary is defined outside this function, below.
    pudl_session.add_all([State(abbr=k, name=v) for k, v in us_states.items()])

    # Commit the changes to the DB and close down the session.
    pudl_session.commit()
    pudl_session.close_all()

    # We aren't bringing row_number in to the PUDL DB:
    ferc_accts_df = ferc_electric_plant_accounts.drop('row_number', axis=1)
    # Get rid of excessive whitespace introduced to break long lines (ugh)
    ferc_accts_df.ferc_account_description = \
        ferc_accts_df.ferc_account_description.str.replace('\s+', ' ')

    ferc_accts_df.rename(columns={'ferc_account_id': 'id',
                                  'ferc_account_description': 'description'},
                         inplace=True)

    ferc_accts_df.to_sql('ferc_accounts',
                         con=engine, index=False, if_exists='append',
                         dtype={'id': String,
                                'description': String})

    ferc_depreciation_lines_df = \
        ferc_accumulated_depreciation.drop('row_number', axis=1)

    ferc_depreciation_lines_df.\
        rename(columns={'line_id': 'id',
                        'ferc_account_description': 'description'},
               inplace=True)

    ferc_depreciation_lines_df.\
        to_sql('ferc_depreciation_lines',
               con=engine, index=False, if_exists='append',
               dtype={'id': String,
                      'description': String})


def ingest_glue_tables(engine):
    """
    Populate the tables which relate the EIA & FERC datasets to each other.

    We have compiled a bunch of information which can be used to map individual
    utilities and plants listed in both the EIA and FERC data sets to each
    other, allowing disparate data reported in the two sources to be related
    to each other. That data is primarily stored in the plant_output and
    utility_output tabs of results/id_mapping/mapping_eia923_ferc1.xlsx in the
    repository. There are a total of seven relations described in this data:

        - utilities: Unique id and name for each utility for use across the
          PUDL DB.
        - plants: Unique id and name for each plant for use across the PUDL DB.
        - utilities_eia923: EIA operator ids and names attached to a PUDL
          utility id.
        - plants_eia923: EIA plant ids and names attached to a PUDL plant id.
        - utilities_ferc1: FERC respondent ids & names attached to a PUDL
          utility id.
        - plants_ferc1: A combination of FERC plant names and respondent ids,
          associated with a PUDL plant ID. This is necessary because FERC does
          not provide plant ids, so the unique plant identifier is a
          combination of the respondent id and plant name.
        - util_plant_assn: An association table which describes which plants
          have relationships with what utilities. If a record exists in this
          table then combination of PUDL utility id & PUDL plant id does have
          an association of some kind. The nature of that association is
          somewhat fluid, and more scrutiny will likely be required for use in
          analysis.

    Presently, the 'glue' tables are a very basic piece of infrastructure for
    the PUDL DB, because they contain the primary key fields for utilities and
    plants. It may make sense to revise this going forward, as the
    relationships between data from different sources are looser than we had
    originally anticipated.
    """
    map_eia923_ferc1_file = os.path.join(settings.PUDL_DIR,
                                         'results',
                                         'id_mapping',
                                         'mapping_eia923_ferc1.xlsx')

    plant_map = pd.read_excel(map_eia923_ferc1_file, 'plants_output',
                              na_values='', keep_default_na=False,
                              converters={'plant_id': int,
                                          'plant_name': str,
                                          'respondent_id_ferc1': int,
                                          'respondent_name_ferc1': str,
                                          'plant_name_ferc1': str,
                                          'plant_id_eia923': int,
                                          'plant_name_eia923': str,
                                          'operator_name_eia923': str,
                                          'operator_id_eia923': int})

    utility_map = pd.read_excel(map_eia923_ferc1_file, 'utilities_output',
                                na_values='', keep_default_na=False,
                                converters={'utility_id': int,
                                            'utility_name': str,
                                            'respondent_id_ferc1': int,
                                            'respondent_name_ferc1': str,
                                            'operator_id_eia923': int,
                                            'operator_name_eia923': str})

    # We need to standardize plant names -- same capitalization and no leading
    # or trailing white space... since this field is being used as a key in
    # many cases. This also needs to be done any time plant_name is pulled in
    # from other tables.
    plant_map['plant_name_ferc1'] = plant_map['plant_name_ferc1'].str.strip()
    plant_map['plant_name_ferc1'] = plant_map['plant_name_ferc1'].str.title()

    plants = plant_map[['plant_id', 'plant_name']]
    plants = plants.drop_duplicates('plant_id')

    plants_eia923 = plant_map[['plant_id_eia923',
                               'plant_name_eia923',
                               'plant_id']]
    plants_eia923 = plants_eia923.drop_duplicates('plant_id_eia923')
    plants_ferc1 = plant_map[['plant_name_ferc1',
                              'respondent_id_ferc1',
                              'plant_id']]
    plants_ferc1 = plants_ferc1.drop_duplicates(['plant_name_ferc1',
                                                 'respondent_id_ferc1'])
    utilities = utility_map[['utility_id', 'utility_name']]
    utilities = utilities.drop_duplicates('utility_id')
    utilities_eia923 = utility_map[['operator_id_eia923',
                                    'operator_name_eia923',
                                    'utility_id']]
    utilities_eia923 = utilities_eia923.drop_duplicates('operator_id_eia923')

    utilities_ferc1 = utility_map[['respondent_id_ferc1',
                                   'respondent_name_ferc1',
                                   'utility_id']]
    utilities_ferc1 = utilities_ferc1.drop_duplicates('respondent_id_ferc1')

    # Now we need to create a table that indicates which plants are associated
    # with every utility.

    # These dataframes map our plant_id to FERC respondents and EIA
    # operators -- the equivalents of our "utilities"
    plants_respondents = plant_map[['plant_id', 'respondent_id_ferc1']]
    plants_operators = plant_map[['plant_id', 'operator_id_eia923']]

    # Here we treat the dataframes like database tables, and join on the
    # FERC respondent_id and EIA operator_id, respectively.
    utility_plant_ferc1 = utilities_ferc1.\
        join(plants_respondents.
             set_index('respondent_id_ferc1'),
             on='respondent_id_ferc1')

    utility_plant_eia923 = utilities_eia923.join(
        plants_operators.set_index('operator_id_eia923'),
        on='operator_id_eia923')

    # Now we can concatenate the two dataframes, and get rid of all the columns
    # except for plant_id and utility_id (which determine the  utility to plant
    # association), and get rid of any duplicates or lingering NaN values...
    utility_plant_assn = pd.concat([utility_plant_eia923, utility_plant_ferc1])
    utility_plant_assn = utility_plant_assn[['plant_id', 'utility_id']].\
        dropna().drop_duplicates()

    # At this point there should be at most one row in each of these data
    # frames with NaN values after we drop_duplicates in each. This is because
    # there will be some plants and utilities that only exist in FERC, or only
    # exist in EIA, and while they will have PUDL IDs, they may not have
    # FERC/EIA info (and it'll get pulled in as NaN)

    for df in [plants_eia923, plants_ferc1, utilities_eia923, utilities_ferc1]:
        assert df[pd.isnull(df).any(axis=1)].shape[0] <= 1
        df.dropna(inplace=True)

    # Before we start inserting records into the database, let's do some basic
    # sanity checks to ensure that it's (at least kind of) clean.
    # INSERT SANITY HERE

    # Any FERC respondent_id that appears in plants_ferc1 must also exist in
    # utils_ferc1:
    # INSERT MORE SANITY HERE

    plants.rename(columns={'plant_id': 'id', 'plant_name': 'name'},
                  inplace=True)
    plants.to_sql(name='plants',
                  con=engine, index=False, if_exists='append',
                  dtype={'id': Integer, 'name': String})

    utilities.rename(columns={'utility_id': 'id', 'utility_name': 'name'},
                     inplace=True)
    utilities.to_sql(name='utilities',
                     con=engine, index=False, if_exists='append',
                     dtype={'id': Integer, 'name': String})

    utilities_eia923.rename(columns={'operator_id_eia923': 'operator_id',
                                     'operator_name_eia923': 'operator_name',
                                     'utility_id': 'util_id_pudl'},
                            inplace=True)
    utilities_eia923.to_sql(name='utilities_eia923',
                            con=engine, index=False, if_exists='append',
                            dtype={'operator_id': Integer,
                                   'operator_name': String,
                                   'util_id_pudl': Integer})

    utilities_ferc1.rename(columns={'respondent_id_ferc1': 'respondent_id',
                                    'respondent_name_ferc1': 'respondent_name',
                                    'utility_id': 'util_id_pudl'},
                           inplace=True)
    utilities_ferc1.to_sql(name='utilities_ferc1',
                           con=engine, index=False, if_exists='append',
                           dtype={'respondent_id': Integer,
                                  'respondent_name': String,
                                  'util_id_pudl': Integer})

    plants_eia923.rename(columns={'plant_id_eia923': 'plant_id',
                                  'plant_name_eia923': 'plant_name',
                                  'plant_id': 'plant_id_pudl'},
                         inplace=True)
    plants_eia923.to_sql(name='plants_eia923',
                         con=engine, index=False, if_exists='append',
                         dtype={'plant_id': Integer,
                                'plant_name': String,
                                'plant_id_pudl': Integer})

    plants_ferc1.rename(columns={'respondent_id_ferc1': 'respondent_id',
                                 'plant_name_ferc1': 'plant_name',
                                 'plant_id': 'plant_id_pudl'},
                        inplace=True)
    plants_ferc1.to_sql(name='plants_ferc1',
                        con=engine, index=False, if_exists='append',
                        dtype={'respondent_id': Integer,
                               'plant_name': String,
                               'plant_id_pudl': Integer})

    utility_plant_assn.to_sql(name='util_plant_assn',
                              con=engine, index=False, if_exists='append',
                              dtype={'plant_id': Integer,
                                     'utility_id': Integer})


###############################################################################
###############################################################################
# BEGIN FERC 1 INGEST FUNCTIONS
###############################################################################
###############################################################################


def ingest_fuel_ferc1(pudl_engine, ferc1_engine, ferc1_years):
    """
    Clean & ingest f1_fuel table from FERC Form 1 DB into the PUDL DB.

    Read data from the f1_fuel table of our cloned FERC Form 1 database for
    the years specified by ferc1_years, clean it up and re-organize it for
    loading into the PUDL DB. This process includes converting some columns
    to be in terms of our preferred units, like MWh and mmbtu instead of kWh
    and btu. Plant names are also standardized (stripped & Title Case). Fuel
    and fuel unit strings are also standardized using our cleanstrings()
    function and string cleaning dictionaries found in pudl.constants.

    Args:
        pudl_engine (sqlalchemy.engine): Engine for connecting to the PUDL DB.
        ferc1_engine (sqlalchemy.engine): Engine for connecting to the FERC DB.
        ferc1_years (sequence of integers): Years for which we should extract
            fuel data from the FERC1 Database.

    Returns: Nothing.
    """
    # Grab the f1_fuel SQLAlchemy Table object from the metadata object.
    f1_fuel = ferc1_meta.tables['f1_fuel']
    # Generate a SELECT statement that pulls all fields of the f1_fuel table,
    # but only gets records with plant names, and non-zero fuel amounts:
    f1_fuel_select = select([f1_fuel]).\
        where(f1_fuel.c.fuel != '').\
        where(f1_fuel.c.fuel_quantity > 0).\
        where(f1_fuel.c.plant_name != '').\
        where(f1_fuel.c.report_year.in_(ferc1_years))
    # Use the above SELECT to pull those records into a DataFrame:
    ferc1_fuel_df = pd.read_sql(f1_fuel_select, ferc1_engine)

    # Discard DataFrame columns that we aren't pulling into PUDL:
    ferc1_fuel_df.drop(['spplmnt_num', 'row_number', 'row_seq', 'row_prvlg',
                        'report_prd'], axis=1, inplace=True)

    # Standardize plant_name capitalization and remove leading/trailing white
    # space -- necesary b/c plant_name is part of many foreign keys.
    ferc1_fuel_df['plant_name'] = ferc1_fuel_df['plant_name'].str.strip()
    ferc1_fuel_df['plant_name'] = ferc1_fuel_df['plant_name'].str.title()

    # Take the messy free-form fuel & fuel_unit fields, and do our best to
    # map them to some canonical categories... this is necessarily imperfect:
    ferc1_fuel_df.fuel = cleanstrings(ferc1_fuel_df.fuel,
                                      ferc1_fuel_strings,
                                      unmapped=np.nan)
    ferc1_fuel_df.fuel_unit = cleanstrings(ferc1_fuel_df.fuel_unit,
                                           ferc1_fuel_unit_strings,
                                           unmapped=np.nan)

    # Convert to MW/MWh units across the board.
    ferc1_fuel_df['fuel_cost_per_mwh'] = 1000 * ferc1_fuel_df['fuel_cost_kwh']
    ferc1_fuel_df.drop('fuel_cost_kwh', axis=1, inplace=True)
    # Here we are converting from BTU/kWh to 1e6 BTU/MWh
    ferc1_fuel_df['fuel_mmbtu_per_mwh'] = (1e3 / 1e6) * \
        ferc1_fuel_df['fuel_generaton']
    ferc1_fuel_df.drop('fuel_generaton', axis=1, inplace=True)
    # Convert from BTU/unit of fuel to 1e6 BTU/unit.
    ferc1_fuel_df['fuel_avg_mmbtu_per_unit'] = \
        ferc1_fuel_df['fuel_avg_heat'] / 1e6
    ferc1_fuel_df.drop('fuel_avg_heat', axis=1, inplace=True)

    # Drop any records that are missing data. This is a blunt instrument, to
    # be sure. In some cases we lose data here, because some utilities have
    # (for example) a "Total" line w/ only fuel_mmbtu_per_kwh on it. Grr.
    ferc1_fuel_df.dropna(inplace=True)

    # Make sure that the DataFrame column names (which were imported from the
    # f1_fuel table) match their corresponding field names in the PUDL DB.
    ferc1_fuel_df.rename(columns={
        # FERC 1 DB Name      PUDL DB Name
        'fuel_quantity': 'fuel_qty_burned',
        'fuel_cost_burned': 'fuel_cost_per_unit_burned',
        'fuel_cost_delvd': 'fuel_cost_per_unit_delivered',
                           'fuel_cost_btu': 'fuel_cost_per_mmbtu'},
                         inplace=True)
    ferc1_fuel_df.to_sql(name='fuel_ferc1',
                         con=pudl_engine, index=False, if_exists='append',
                         dtype={'respondent_id': Integer,
                                'report_year': Integer})


def ingest_plants_steam_ferc1(pudl_engine, ferc1_engine, ferc1_years):
    """
    Clean f1_steam table of the FERC Form 1 DB and pull into the PUDL DB.

    Load data about large thermal plants from our cloned FERC Form 1 DB into
    the PUDL DB. Clean up and reorganize some of the information along the way.
    This includes converting to our preferred units of MWh and MW, as well as
    standardizing the strings describing the kind of plant and construction.

    Args:
        pudl_engine (sqlalchemy.engine): Engine for connecting to the PUDL DB.
        ferc1_engine (sqlalchemy.engine): Engine for connecting to the FERC DB.
        ferc1_years (sequence of integers): Years for which we should extract
            fuel data from the FERC1 Database.

    Returns: Nothing.
    """
    f1_steam = ferc1_meta.tables['f1_steam']
    f1_steam_select = select([f1_steam]).\
        where(f1_steam.c.net_generation > 0).\
        where(f1_steam.c.plant_name != '').\
        where(f1_steam.c.report_year.in_(ferc1_years))

    ferc1_steam_df = pd.read_sql(f1_steam_select, ferc1_engine)
    # Discard DataFrame columns that we aren't pulling into PUDL:
    ferc1_steam_df.drop(['spplmnt_num', 'row_number', 'row_seq', 'row_prvlg',
                         'report_prd'], axis=1, inplace=True)

    # Standardize plant_name capitalization and remove leading/trailing white
    # space -- necesary b/c plant_name is part of many foreign keys.
    ferc1_steam_df['plant_name'] = ferc1_steam_df['plant_name'].str.strip()
    ferc1_steam_df['plant_name'] = ferc1_steam_df['plant_name'].str.title()

    # Take the messy free-form type_const and plant_kind fields, and do our
    # best to map them to some canonical categories...
    # this is necessarily imperfect:

    ferc1_steam_df.type_const = cleanstrings(ferc1_steam_df.type_const,
                                             ferc1_type_const_strings,
                                             unmapped=np.nan)
    ferc1_steam_df.plant_kind = cleanstrings(ferc1_steam_df.plant_kind,
                                             ferc1_plant_kind_strings,
                                             unmapped=np.nan)

    # Force the construction and installation years to be numeric values, and
    # set them to NA if they can't be converted. (table has some junk values)
    ferc1_steam_df['yr_const'] = pd.to_numeric(
        ferc1_steam_df['yr_const'],
        errors='coerce')
    ferc1_steam_df['yr_installed'] = pd.to_numeric(
        ferc1_steam_df['yr_installed'],
        errors='coerce')

    # Converting everything to per MW and MWh units...
    ferc1_steam_df['cost_per_mw'] = 1000 * ferc1_steam_df['cost_per_kw']
    ferc1_steam_df.drop('cost_per_kw', axis=1, inplace=True)
    ferc1_steam_df['net_generation_mwh'] = \
        ferc1_steam_df['net_generation'] / 1000
    ferc1_steam_df.drop('net_generation', axis=1, inplace=True)
    ferc1_steam_df['expns_per_mwh'] = 1000 * ferc1_steam_df['expns_kwh']
    ferc1_steam_df.drop('expns_kwh', axis=1, inplace=True)

    ferc1_steam_df.rename(columns={
        # FERC 1 DB Name      PUDL DB Name
        'yr_const': 'year_constructed',
        'yr_installed': 'year_installed',
        'tot_capacity': 'total_capacity_mw',
        'peak_demand': 'peak_demand_mw',
        'plnt_capability': 'plant_capability_mw',
        'when_limited': 'water_limited_mw',
        'when_not_limited': 'not_water_limited_mw',
        'avg_num_of_emp': 'avg_num_employees',
        'net_generation': 'net_generation_mwh',
        'cost_of_plant_to': 'cost_of_plant_total',
        'expns_steam_othr': 'expns_steam_other',
        'expns_engnr': 'expns_engineering',
        'tot_prdctn_expns': 'expns_production_total'},
        inplace=True)
    ferc1_steam_df.to_sql(name='plants_steam_ferc1',
                          con=pudl_engine, index=False, if_exists='append',
                          dtype={'respondent_id': Integer,
                                 'report_year': Integer,
                                 'type_const': String,
                                 'plant_kind': String,
                                 'year_constructed': Integer,
                                 'year_installed': Integer})


def ingest_plants_hydro_ferc1(pudl_engine, ferc1_engine, ferc1_years):
    """
    Ingest f1_hydro table of FERC Form 1 DB into PUDL DB.

    Load data about hydroelectric generation resources from our cloned FERC 1
    database into the PUDL DB. Standardizes plant names (stripping whitespace
    and Using Title Case).  Also converts into our preferred units of MW and
    MWh.

    Args:
        pudl_engine (sqlalchemy.engine): Engine for connecting to the PUDL DB.
        ferc1_engine (sqlalchemy.engine): Engine for connecting to the FERC DB.
        ferc1_years (sequence of integers): Years for which we should extract
            fuel data from the FERC1 Database.

    Returns: Nothing.
    """
    f1_hydro = ferc1_meta.tables['f1_hydro']

    f1_hydro_select = select([f1_hydro]).\
        where(f1_hydro.c.plant_name != '').\
        where(f1_hydro.c.report_year.in_(ferc1_years))

    ferc1_hydro_df = pd.read_sql(f1_hydro_select, ferc1_engine)
    ferc1_hydro_df.drop(['spplmnt_num', 'row_number', 'row_seq', 'row_prvlg',
                         'report_prd'], axis=1, inplace=True)

    # Standardize plant_name capitalization and remove leading/trailing white
    # space -- necesary b/c plant_name is part of many foreign keys.
    ferc1_hydro_df['plant_name'] = ferc1_hydro_df['plant_name'].str.strip()
    ferc1_hydro_df['plant_name'] = ferc1_hydro_df['plant_name'].str.title()

    # Converting kWh to MWh
    ferc1_hydro_df['net_generation_mwh'] = \
        ferc1_hydro_df['net_generation'] / 1000.0
    ferc1_hydro_df.drop('net_generation', axis=1, inplace=True)
    # Converting cost per kW installed to cost per MW installed:
    ferc1_hydro_df['cost_per_mw'] = ferc1_hydro_df['cost_per_kw'] * 1000.0
    ferc1_hydro_df.drop('cost_per_kw', axis=1, inplace=True)
    # Converting kWh to MWh
    ferc1_hydro_df['expns_per_mwh'] = ferc1_hydro_df['expns_kwh'] * 1000.0
    ferc1_hydro_df.drop('expns_kwh', axis=1, inplace=True)

    ferc1_hydro_df['yr_const'] = pd.to_numeric(
        ferc1_hydro_df['yr_const'],
        errors='coerce')
    ferc1_hydro_df['yr_installed'] = pd.to_numeric(
        ferc1_hydro_df['yr_installed'],
        errors='coerce')
    ferc1_hydro_df.dropna(inplace=True)
    ferc1_hydro_df.rename(columns={
        # FERC1 DB          PUDL DB
        'project_no': 'project_number',
        'yr_const': 'year_constructed',
        'plant_const': 'plant_construction',
        'yr_installed': 'year_installed',
        'tot_capacity': 'total_capacity_mw',
        'peak_demand': 'peak_demand_mw',
        'plant_hours': 'plant_hours_connected_while_generating',
        'favorable_cond': 'net_capacity_favorable_conditions_mw',
        'adverse_cond': 'net_capacity_adverse_conditions_mw',
        'avg_num_of_emp': 'avg_number_employees',
        'cost_of_land': 'cost_land',
        'expns_engnr': 'expns_engineering',
        'expns_total': 'expns_production_total'
    }, inplace=True)

    ferc1_hydro_df.to_sql(name='plants_hydro_ferc1',
                          con=pudl_engine, index=False, if_exists='append',
                          dtype={'respondent_id': Integer,
                                 'report_year': Integer})


def ingest_plants_pumped_storage_ferc1(pudl_engine, ferc1_engine, ferc1_years):
    """
    Ingest f1_pumped_storage table of FERC Form 1 DB into PUDL DB.

    Load data about pumped hydro storage facilities from our cloned FERC 1
    database into the PUDL DB. Standardizes plant names (stripping whitespace
    and Using Title Case).  Also converts into our preferred units of MW and
    MWh.

    Args:
       pudl_engine (sqlalchemy.engine): Engine for connecting to the PUDL DB.
       ferc1_engine (sqlalchemy.engine): Engine for connecting to the FERC DB.
       ferc1_years (sequence of integers): Years for which we should extract
           fuel data from the FERC1 Database.

    Returns: Nothing.
    """
    f1_pumped_storage = ferc1_meta.tables['f1_pumped_storage']

    # Removing the empty records.
    # This reduces the entries for 2015 from 272 records to 27.
    f1_pumped_storage_select = select([f1_pumped_storage]).\
        where(f1_pumped_storage.c.plant_name != '').\
        where(f1_pumped_storage.c.report_year.in_(ferc1_years))

    ferc1_pumped_storage_df = pd.read_sql(
        f1_pumped_storage_select, ferc1_engine)
    ferc1_pumped_storage_df.drop(['spplmnt_num', 'row_number', 'row_seq',
                                  'row_prvlg', 'report_prd'],
                                 axis=1, inplace=True)

    # Standardize plant_name capitalization and remove leading/trailing white
    # space -- necesary b/c plant_name is part of many foreign keys.
    ferc1_pumped_storage_df['plant_name'] = \
        ferc1_pumped_storage_df['plant_name'].str.strip()
    ferc1_pumped_storage_df['plant_name'] = \
        ferc1_pumped_storage_df['plant_name'].str.title()

    # Converting kWh to MWh
    ferc1_pumped_storage_df['net_generation_mwh'] = \
        ferc1_pumped_storage_df['net_generation'] / 1000.0
    ferc1_pumped_storage_df.drop('net_generation', axis=1, inplace=True)

    ferc1_pumped_storage_df['energy_used_for_pumping_mwh'] = \
        ferc1_pumped_storage_df['energy_used'] / 1000.0
    ferc1_pumped_storage_df.drop('energy_used', axis=1, inplace=True)

    ferc1_pumped_storage_df['net_load_mwh'] = \
        ferc1_pumped_storage_df['net_load'] / 1000.0
    ferc1_pumped_storage_df.drop('net_load', axis=1, inplace=True)

    # Converting cost per kW installed to cost per MW installed:
    ferc1_pumped_storage_df['cost_per_mw'] = \
        ferc1_pumped_storage_df['cost_per_kw'] * 1000.0
    ferc1_pumped_storage_df.drop('cost_per_kw', axis=1, inplace=True)

    ferc1_pumped_storage_df['expns_per_mwh'] = \
        ferc1_pumped_storage_df['expns_kwh'] * 1000.0
    ferc1_pumped_storage_df.drop('expns_kwh', axis=1, inplace=True)

    ferc1_pumped_storage_df['yr_const'] = pd.to_numeric(
        ferc1_pumped_storage_df['yr_const'],
        errors='coerce')
    ferc1_pumped_storage_df['yr_installed'] = pd.to_numeric(
        ferc1_pumped_storage_df['yr_installed'],
        errors='coerce')

    ferc1_pumped_storage_df.dropna(inplace=True)

    ferc1_pumped_storage_df.rename(columns={
        # FERC1 DB          PUDL DB
        'tot_capacity': 'total_capacity_mw',
        'project_no': 'project_number',
        'peak_demand': 'peak_demand_mw',
        'yr_const': 'year_constructed',
        'yr_installed': 'year_installed',
        'plant_hours': 'plant_hours_connected_while_generating',
        'plant_capability': 'plant_capability_mw',
        'avg_num_of_emp': 'avg_number_employees',
        'cost_wheels': 'cost_wheels_turbines_generators',
        'cost_electric': 'cost_equipment_electric',
        'cost_misc_eqpmnt': 'cost_equipment_misc',
        'cost_of_plant': 'cost_plant_total',
        'expns_water_pwr': 'expns_water_for_pwr',
        'expns_pump_strg': 'expns_pump_storage',
        'expns_misc_power': 'expns_generation_misc',
        'expns_misc_plnt': 'expns_misc_plant',
        'expns_producton': 'expns_producton_before_pumping',
        'tot_prdctn_exns': 'expns_producton_total'},
        inplace=True)

    ferc1_pumped_storage_df.to_sql(name='plants_pumped_storage_ferc1',
                                   con=pudl_engine, index=False,
                                   if_exists='append')


def ingest_accumulated_depreciation_ferc1(pudl_engine,
                                          ferc1_engine,
                                          ferc1_years):
    """
    Ingest f1_accumdepr_prvs table from FERC Form 1 DB into PUDL DB.

    Load data about accumulated depreciation from our cloned FERC Form 1
    database into the PUDL DB. This information is organized by FERC account,
    with each line of the FERC Form 1 having a different descriptive
    identifier like 'balance_end_of_year' or 'transmission'.

    Args:
       pudl_engine (sqlalchemy.engine): Engine for connecting to the PUDL DB.
       ferc1_engine (sqlalchemy.engine): Engine for connecting to the FERC DB.
       ferc1_years (sequence of integers): Years for which we should extract
           fuel data from the FERC1 Database.

    Returns: Nothing.
    """
    f1_accumdepr_prvsn = ferc1_meta.tables['f1_accumdepr_prvsn']
    f1_accumdepr_prvsn_select = select([f1_accumdepr_prvsn]).\
        where(f1_accumdepr_prvsn.c.report_year.in_(ferc1_years))

    ferc1_apd_df = pd.read_sql(f1_accumdepr_prvsn_select, ferc1_engine)

    # Discard DataFrame columns that we aren't pulling into PUDL. For
    ferc1_apd_df.drop(['spplmnt_num', 'row_seq',
                       'row_prvlg', 'item', 'report_prd'],
                      axis=1, inplace=True)

    ferc1_acct_apd = ferc_accumulated_depreciation.drop(
        ['ferc_account_description'], axis=1)
    ferc1_acct_apd.dropna(inplace=True)
    ferc1_acct_apd['row_number'] = ferc1_acct_apd['row_number'].astype(int)

    ferc1_accumdepr_prvsn_df = pd.merge(ferc1_apd_df, ferc1_acct_apd,
                                        how='left', on='row_number')
    ferc1_accumdepr_prvsn_df.drop('row_number', axis=1, inplace=True)

    ferc1_accumdepr_prvsn_df.\
        to_sql(name='accumulated_depreciation_ferc1',
               con=pudl_engine, index=False, if_exists='append',
               dtype={'respondent_id': Integer,
                      'report_year': Integer,
                      'line_id': String,
                      'total_cde': Numeric(14, 2),
                      'electric_plant': Numeric(14, 2),
                      'future_plant': Numeric(14, 2),
                      'leased plant': Numeric(14, 2)})


def ingest_plant_in_service_ferc1(pudl_engine, ferc1_engine, ferc1_years):
    """
    Ingest f1_plant_in_srvce table of FERC Form 1 DB into PUDL DB.

    Load data about the financial value of utility plant in service from our
    cloned FERC Form 1 database into the PUDL DB. This information is organized
    by FERC account, with each line of the FERC Form 1 having a different FERC
    account id (most are numeric and correspond to FERC's Uniform Electric
    System of Accounts). As of PUDL v0.1, this data is only valid from 2007
    onward, as the line numbers for several accounts are different in earlier
    years.

    Args:
        pudl_engine (sqlalchemy.engine): Engine for connecting to the PUDL DB.
        ferc1_engine (sqlalchemy.engine): Engine for connecting to the FERC DB.
        ferc1_years (sequence of integers): Years for which we should extract
            fuel data from the FERC1 Database.

       Returns: Nothing.
    """
    min_yr = min(ferc1_years)
    assert min_yr >= 2007,\
        """Invalid year requested: {}. FERC Form 1 Plant In Service data is
        currently only valid for years 2007 and later.""".format(min_yr)
    f1_plant_in_srvce = ferc1_meta.tables['f1_plant_in_srvce']
    f1_plant_in_srvce_select = select([f1_plant_in_srvce]).\
        where(f1_plant_in_srvce.c.report_year.in_(ferc1_years))

    ferc1_pis_df = pd.read_sql(f1_plant_in_srvce_select, ferc1_engine)

    # Discard DataFrame columns that we aren't pulling into PUDL. For the
    # Plant In Service table, we need to hold on to the row_number because it
    # corresponds to a FERC account number.
    ferc1_pis_df.drop(['spplmnt_num', 'row_seq', 'row_prvlg', 'report_prd'],
                      axis=1, inplace=True)

    # Now we need to add a column to the DataFrame that has the FERC account
    # IDs corresponding to the row_number that's already in there...
    ferc_accts_df = ferc_electric_plant_accounts.drop(
        ['ferc_account_description'], axis=1)
    ferc_accts_df.dropna(inplace=True)
    ferc_accts_df['row_number'] = ferc_accts_df['row_number'].astype(int)

    ferc1_pis_df = pd.merge(ferc1_pis_df, ferc_accts_df,
                            how='left', on='row_number')
    ferc1_pis_df.drop('row_number', axis=1, inplace=True)

    ferc1_pis_df.rename(columns={
        # FERC 1 DB Name  PUDL DB Name
        'begin_yr_bal': 'beginning_year_balance',
        'addition': 'additions',
        'yr_end_bal': 'year_end_balance'},
        inplace=True)
    ferc1_pis_df.to_sql(name='plant_in_service_ferc1',
                        con=pudl_engine, index=False, if_exists='append',
                        dtype={'respondent_id': Integer,
                               'report_year': Integer,
                               'ferc_account_id': String,
                               'beginning_year_balance': Numeric(14, 2),
                               'additions': Numeric(14, 2),
                               'retirements': Numeric(14, 2),
                               'adjustments': Numeric(14, 2),
                               'transfers': Numeric(14, 2),
                               'year_end_balance': Numeric(14, 2)})


def ingest_plants_small_ferc1(pudl_engine, ferc1_engine, ferc1_years):
    """
    Ingest f1_gnrt_plant table from our cloned FERC Form 1 DB into PUDL DB.

    This FERC Form 1 table contains information about a large number of small
    plants, including many small hydroelectric and other renewable generation
    facilities. Unfortunately the data is not well standardized, and so the
    plants have been categorized manually, with the results of that
    categorization stored in an Excel spreadsheet. This function reads in the
    plant type data from the spreadsheet and merges it with the rest of the
    information from the FERC DB based on record number, FERC respondent ID,
    and report year. When possible the FERC license number for small hydro
    plants is also manually extracted from the data.

    This categorization will need to be renewed with each additional year of
    FERC data we pull in. As of v0.1 the small plants have been categorized
    for 2004-2015.

    Args:
        pudl_engine (sqlalchemy.engine): Engine for connecting to the PUDL DB.
        ferc1_engine (sqlalchemy.engine): Engine for connecting to the FERC DB.
        ferc1_years (sequence of integers): Years for which we should extract
            fuel data from the FERC1 Database.

    Returns: Nothing.
    """
    import os.path
    from sqlalchemy import or_

    assert min(ferc_years) >= 2004,\
        """Year {} is too early. Small plant data has not been categorized for
        before 2004.""".format(min(ferc_years))
    assert max(ferc_years) <= 2015,\
        """Year {} is too recent. Small plant data has not been categorized for
        any year 2015.""".format(max(ferc_years))
    f1_small = ferc1_meta.tables['f1_gnrt_plant']
    f1_small_select = select([f1_small, ]).\
        where(f1_small.c.report_year.in_(ferc1_years)).\
        where(f1_small.c.plant_name != '').\
        where(or_((f1_small.c.capacity_rating != 0),
                  (f1_small.c.net_demand != 0),
                  (f1_small.c.net_generation != 0),
                  (f1_small.c.plant_cost != 0),
                  (f1_small.c.plant_cost_mw != 0),
                  (f1_small.c.operation != 0),
                  (f1_small.c.expns_fuel != 0),
                  (f1_small.c.expns_maint != 0),
                  (f1_small.c.fuel_cost != 0)))

    ferc1_small_df = pd.read_sql(f1_small_select, ferc1_engine)

    # Standardize plant_name capitalization and remove leading/trailing white
    # space -- necesary b/c plant_name is part of many foreign keys.
    ferc1_small_df['plant_name'] = ferc1_small_df['plant_name'].str.strip()
    ferc1_small_df['plant_name'] = ferc1_small_df['plant_name'].str.title()

    ferc1_small_df['kind_of_fuel'] = ferc1_small_df['kind_of_fuel'].str.strip()
    ferc1_small_df['kind_of_fuel'] = ferc1_small_df['kind_of_fuel'].str.title()

    # Force the construction and installation years to be numeric values, and
    # set them to NA if they can't be converted. (table has some junk values)
    ferc1_small_df['yr_constructed'] = pd.to_numeric(
        ferc1_small_df['yr_constructed'],
        errors='coerce')
    # Convert from cents per mmbtu to dollars per mmbtu to be consistent
    # with the f1_fuel table data. Also, let's use a clearer name.
    ferc1_small_df['fuel_cost_per_mmbtu'] = ferc1_small_df['fuel_cost'] / 100.0
    ferc1_small_df.drop('fuel_cost', axis=1, inplace=True)

    # Create a single "record number" for the individual lines in the FERC
    # Form 1 that report different small plants, so that we can more easily
    # tell whether they are adjacent to each other in the reporting.
    ferc1_small_df['record_number'] = 46 * ferc1_small_df['spplmnt_num'] + \
        ferc1_small_df['row_number']

    # Unforunately the plant types were not able to be parsed automatically
    # in this table. It's been done manually for 2004-2015, and the results
    # get merged in in the following section.
    small_types_file = os.path.join(settings.PUDL_DIR,
                                    'results',
                                    'ferc1_small_plants',
                                    'small_plants_2004-2015.xlsx')
    small_types_df = pd.read_excel(small_types_file)

    # Only rows with plant_type set will give us novel information.
    small_types_df.dropna(subset=['plant_type', ], inplace=True)
    # We only need this small subset of the columns to extract the plant type.
    small_types_df = small_types_df[['report_year', 'respondent_id',
                                     'record_number', 'plant_name_clean',
                                     'plant_type', 'ferc_license']]

    # Munge the two dataframes together, keeping everything from the
    # frame we pulled out of the FERC1 DB, and supplementing it with the
    # plant_name_clean, plant_type, and ferc_license fields from our hand
    # made file.
    ferc1_small_df = pd.merge(ferc1_small_df,
                              small_types_df,
                              how='left',
                              on=['report_year',
                                  'respondent_id',
                                  'record_number'])

    # We don't need to pull these columns into PUDL, so drop them:
    ferc1_small_df.drop(['row_seq', 'row_prvlg', 'report_prd',
                         'row_number', 'spplmnt_num', 'record_number'],
                        axis=1, inplace=True)

    # Standardize plant_name capitalization and remove leading/trailing white
    # space, so that plant_name_clean matches formatting of plant_name
    ferc1_small_df['plant_name_clean'] = \
        ferc1_small_df['plant_name_clean'].str.strip()
    ferc1_small_df['plant_name_clean'] = \
        ferc1_small_df['plant_name_clean'].str.title()

    ferc1_small_df.rename(columns={
        # FERC 1 DB Name      PUDL DB Name
        'yr_constructed': 'year_constructed',
        'capacity_rating': 'total_capacity_mw',
        'net_demand': 'peak_demand_mw',
        'net_generation': 'net_generation_mwh',
        'plant_cost': 'cost_of_plant_total',
        'plant_cost_mw': 'cost_of_plant_per_mw',
        'operation': 'cost_of_operation',
        'expns_maint': 'expns_maintenance',
        'fuel_cost': 'fuel_cost_per_mmbtu'},
        inplace=True)
    ferc1_small_df.to_sql(name='plants_small_ferc1',
                          con=pudl_engine, index=False, if_exists='append',
                          dtype={'respondent_id': Integer,
                                 'report_year': Integer,
                                 'plant_name': String,
                                 'plant_name_clean': String,
                                 'plant_type': String,
                                 'kind_of_fuel': String,
                                 'ferc_license': Integer,
                                 'year_constructed': Integer,
                                 'total_capacity_mw': Float,
                                 'peak_demand_mw': Float,
                                 'net_generation_mwh': Float,
                                 'cost_of_plant_total': Numeric(14, 2),
                                 'cost_of_plant_per_mw': Numeric(14, 2),
                                 'cost_of_operation': Numeric(14, 2),
                                 'expns_fuel': Numeric(14, 2),
                                 'expns_maintenance': Numeric(14, 2),
                                 'fuel_cost_per_mmbtu': Numeric(14, 2)})


def ingest_purchased_power_ferc1(pudl_engine, ferc1_engine, ferc1_years):
    """
    Ingest f1_purchased_pwr table from our cloned FERC Form 1 DB into PUDL DB.

    This function pulls FERC Form 1 data about inter-untility power purchases
    into the PUDL DB. This includes how much electricty was purchased, how
    much it cost, and who it was purchased from. Unfortunately the field
    describing which other utility the power was being bought from is poorly
    standardized, making it difficult to correlate with other data. It will
    need to be categorized by hand or with some fuzzy matching eventually.

    Args:
        pudl_engine (sqlalchemy.engine): Engine for connecting to the PUDL DB.
        ferc1_engine (sqlalchemy.engine): Engine for connecting to the FERC DB.
        ferc1_years (sequence of integers): Years for which we should extract
            fuel data from the FERC1 Database.

    Returns: Nothing.
    """

    f1_purchased_pwr = ferc1_meta.tables['f1_purchased_pwr']
    f1_purchased_pwr_select = select([f1_purchased_pwr]).\
        where(f1_purchased_pwr.c.report_year.in_(ferc1_years))

    ferc1_purchased_pwr_df = pd.read_sql(f1_purchased_pwr_select, ferc1_engine)

    ferc1_purchased_pwr_df.drop(['spplmnt_num', 'row_number', 'row_seq',
                                 'row_prvlg', 'report_prd'],
                                axis=1, inplace=True)
    ferc1_purchased_pwr_df.replace(to_replace='', value=np.nan, inplace=True)
    ferc1_purchased_pwr_df.dropna(subset=['sttstcl_clssfctn',
                                          'rtsched_trffnbr'], inplace=True)

    ferc1_purchased_pwr_df.rename(columns={
        # FERC 1 DB Name  PUDL DB Name
        'athrty_co_name': 'authority_company_name',
        'sttstcl_clssfctn': 'statistical_classification',
        'rtsched_trffnbr': 'rate_schedule_tariff_number',
        'avgmth_bill_dmnd': 'average_billing_demand',
        'avgmth_ncp_dmnd': 'average_monthly_ncp_demand',
        'avgmth_cp_dmnd': 'average_monthly_cp_demand',
        'mwh_recv': 'mwh_received',
        'mwh_delvd': 'mwh_delivered',
        'dmnd_charges': 'demand_charges',
        'erg_charges': 'energy_charges',
        'othr_charges': 'other_charges',
        'settlement_tot': 'settlement_total'},
        inplace=True)

    ferc1_purchased_pwr_df.to_sql(name='purchased_power_ferc1',
                                  con=pudl_engine, index=False,
                                  if_exists='append',
                                  dtype={'respondent_id': Integer,
                                         'report_year': Integer,
                                         'authority_company_name': String,
                                         'statistical_classification': String,
                                         'rate_schedule_tariff_number': String,
                                         'average_billing_demand': String,
                                         'average_monthly_ncp_demand': String,
                                         'average_monthly_cp_demand': String,
                                         'mwh_purchased': Numeric(14, 2),
                                         'mwh_received': Numeric(14, 2),
                                         'mwh_delivered': Numeric(14, 2),
                                         'demand_charges': Numeric(14, 2),
                                         'energy_charges': Numeric(14, 2),
                                         'other_charges': Numeric(14, 2),
                                         'settlement_total': Numeric(14, 2)})

###############################################################################
###############################################################################
# BEGIN EIA923 INGEST FUNCTIONS
###############################################################################
###############################################################################


def ingest_plant_info_eia923(pudl_engine, eia923_dfs,
                             csvdir='', keep_csv=True):
    """
    Ingest data describing static attributes of plants from EIA Form 923.

    Much of the static plant information is reported repeatedly, and scattered
    across several different pages of EIA 923. This function tries to bring it
    together into one unified, unduplicated table.
    """
    # From 'plant_frame'
    plant_frame_cols = ['plant_id',
                        'plant_state',
                        'combined_heat_power',
                        'eia_sector',
                        'naics_code',
                        'reporting_frequency']

    plant_frame_df = eia923_dfs['plant_frame'][plant_frame_cols]

    # From 'generation_fuel' to merge by plant_id
    gen_fuel_cols = ['plant_id',
                     'census_region',
                     'nerc_region']

    gen_fuel_df = eia923_dfs['generation_fuel'][gen_fuel_cols]

    # Remove "State fuel-level increment" records... which don't pertain to
    # any particular plant (they have plant_id == operator_id == 99999)
    gen_fuel_df = gen_fuel_df[gen_fuel_df.plant_id != 99999]

    # because there ought to be one entry for each plant in each year's worth
    # of data, we're dropping duplicates by plant_id in the two data frames
    # which we're combining. TODO: populate a table that lists plant operators
    # by year... nominally plant_ownership_eia923
    plant_info_df = pd.merge(plant_frame_df.drop_duplicates('plant_id'),
                             gen_fuel_df.drop_duplicates('plant_id'),
                             how='outer', on='plant_id')

    # Since this is a plain Yes/No variable -- just make it a real Boolean.
    plant_info_df.combined_heat_power.replace({'N': False, 'Y': True},
                                              inplace=True)

    # Output into the DB:
    plant_info_df.to_sql(name='plant_info_eia923',
                         con=pudl_engine, index=False, if_exists='append',
                         dtype={'eia_sector': Integer,
                                'naics_code': Integer,
                                'combined_heat_power': Boolean})


def ingest_generation_fuel_eia923(pudl_engine, eia923_dfs,
                                  csvdir='', keep_csv=True):
    """
    Ingest generation and fuel data from Page 1 of EIA Form 923 into PUDL DB.

    Page 1 of EIA 923 (in recent years) reports generation and fuel consumption
    on a monthly, per-plant basis.

    Populates the generation_fuel_eia923 table.
    """
    # This needs to be a copy of what we're passed in so we can edit it.
    gf_df = eia923_dfs['generation_fuel'].copy()

    # Drop fields we're not inserting into the generation_fuel_eia923 table.
    cols_to_drop = ['combined_heat_power',
                    'plant_name',
                    'operator_name',
                    'operator_id',
                    'plant_state',
                    'census_region',
                    'nerc_region',
                    'naics_code',
                    'eia_sector',
                    'sector_name',
                    'fuel_unit',
                    'total_fuel_consumption_quantity',
                    'electric_fuel_consumption_quantity',
                    'total_fuel_consumption_mmbtu',
                    'elec_fuel_consumption_mmbtu',
                    'net_generation_megawatthours']
    gf_df.drop(cols_to_drop, axis=1, inplace=True)

    # Convert the EIA923 DataFrame from yearly to monthly records.
    gf_df = yearly_to_monthly_eia923(gf_df, month_dict_eia923)
    # Replace the EIA923 NA value ('.') with a real NA value.
    gf_df.replace(to_replace='^\.$', value=np.nan, regex=True, inplace=True)
    # Remove "State fuel-level increment" records... which don't pertain to
    # any particular plant (they have plant_id == operator_id == 99999)
    gf_df = gf_df[gf_df.plant_id != 99999]

    # Take a float field and make it an integer, with the empty String
    # as the NA value... for postgres loading.
    gf_df['nuclear_unit_id'] = fix_int_na(gf_df['nuclear_unit_id'],
                                          float_na=np.nan,
                                          int_na=-1,
                                          str_na='')

    # # map AER fuel types to simplified PUDL categories
    gf_df['aer_fuel_category'] = cleanstringsEIA923(gf_df.aer_fuel_type,
                                                    aer_fuel_type_strings)

    # Write the dataframe out to a csv file and load it directly
    csv_dump_load(gf_df, 'generation_fuel_eia923', pudl_engine,
                  csvdir=csvdir, keep_csv=keep_csv)


def ingest_boiler_fuel_eia923(pudl_engine, eia923_dfs,
                              csvdir='', keep_csv=True):
    """
    Ingest data on fuel consumption by boiler from EIA Form 923.

    Populates the boilers_eia923 table and the boiler_fuel_eia923 table.
    """
    # Populate 'boilers_eia923' table
    boiler_cols = ['plant_id',
                   'boiler_id',
                   'prime_mover']

    boilers_df = eia923_dfs['boiler_fuel'][boiler_cols]
    boilers_df = boilers_df.drop_duplicates(
        subset=['plant_id', 'boiler_id'])

    # drop null values from foreign key fields
    boilers_df.dropna(subset=['boiler_id', 'plant_id'], inplace=True)

    # Write the dataframe out to a csv file and load it directly
    csv_dump_load(boilers_df, 'boilers_eia923', pudl_engine,
                  csvdir=csvdir, keep_csv=keep_csv)

    # Populate 'boiler_fuel_eia923' table
    # This needs to be a copy of what we're passed in so we can edit it.
    bf_df = eia923_dfs['boiler_fuel'].copy()

    # Drop fields we're not inserting into the boiler_fuel_eia923 table.
    cols_to_drop = ['combined_heat_power',
                    'plant_name',
                    'operator_name',
                    'operator_id',
                    'plant_state',
                    'census_region',
                    'nerc_region',
                    'naics_code',
                    'eia_sector',
                    'sector_name',
                    'fuel_unit',
                    'total_fuel_consumption_quantity']
    bf_df.drop(cols_to_drop, axis=1, inplace=True)

    bf_df.dropna(subset=['boiler_id', 'plant_id'], inplace=True)

    # Convert the EIA923 DataFrame from yearly to monthly records.
    bf_df = yearly_to_monthly_eia923(bf_df, month_dict_eia923)
    # Replace the EIA923 NA value ('.') with a real NA value.
    bf_df.replace(to_replace='^\.$', value=np.nan, regex=True, inplace=True)

    # Write the dataframe out to a csv file and load it directly
    csv_dump_load(bf_df, 'boiler_fuel_eia923', pudl_engine,
                  csvdir=csvdir, keep_csv=keep_csv)


def ingest_generator_eia923(pudl_engine, eia923_dfs,
                            csvdir='', keep_csv=True):
    """
    Ingest data on electricity production by generator from EIA Form 923.

    This function populates two tables in the PUDL DB.  One describing the
    generators (generators_eia923) and another containing records of reported
    generation (generation_eia923).
    """
    # This needs to be a copy of what we're passed in so we can edit it.
    g_df = eia923_dfs['generator'].copy()

    # Populating the 'generators_eia923' table
    generator_cols = ['plant_id',
                      'generator_id',
                      'prime_mover']

    generators_df = eia923_dfs['generator'][generator_cols]
    generators_df = generators_df.drop_duplicates(
        subset=['plant_id', 'generator_id'])

    # drop null values from foreign key fields
    generators_df.dropna(subset=['generator_id', 'plant_id'], inplace=True)

    # Write the dataframe out to a csv file and load it directly
    csv_dump_load(generators_df, 'generators_eia923', pudl_engine,
                  csvdir=csvdir, keep_csv=keep_csv)

    # Populating the generation_eia923 table:
    # This needs to be a copy of what we're passed in so we can edit it.
    g_df = eia923_dfs['generator'].copy()

    # Drop fields we're not inserting into the generation_eia923_fuel_eia923
    # table.
    cols_to_drop = ['combined_heat_power',
                    'plant_name',
                    'operator_name',
                    'operator_id',
                    'plant_state',
                    'census_region',
                    'nerc_region',
                    'naics_code',
                    'eia_sector',
                    'sector_name',
                    'net_generation_mwh_year_to_date']

    g_df.drop(cols_to_drop, axis=1, inplace=True)

    # Convert the EIA923 DataFrame from yearly to monthly records.
    g_df = yearly_to_monthly_eia923(g_df, month_dict_eia923)
    # Replace the EIA923 NA value ('.') with a real NA value.
    g_df.replace(to_replace='^\.$', value=np.nan, regex=True, inplace=True)

    # Write the dataframe out to a csv file and load it directly
    csv_dump_load(g_df, 'generation_eia923', pudl_engine,
                  csvdir=csvdir, keep_csv=keep_csv)


def ingest_fuel_receipts_costs_eia923(pudl_engine, eia923_dfs,
                                      csvdir='', keep_csv=True):
    """
    Ingest data on fuel purchases and costs from EIA Form 923.

    Populates the coalmine_info_eia923, energy_source_eia923, and
    fuel_receipts_costs_eia923 tables.
    """
    # Populate 'coalmine_info_eia923' table
    coalmine_cols = ['coalmine_name',
                     'coalmine_type',
                     'coalmine_state',
                     'coalmine_county',
                     'coalmine_msha_id']

    coalmine_df = eia923_dfs['fuel_receipts_costs'].copy()
    coalmine_df = coalmine_df[coalmine_cols]

    # Map codes to a few standard values:
    coalmine_df['coalmine_type'].replace(
        {'[pP]': 'P', 'U/S': 'US', 'S/U': 'SU'},
        inplace=True, regex=True)

    # TODO: Not sure which fields of duplicates need to be dropped here
    coalmine_df = coalmine_df.drop_duplicates(subset=['coalmine_name',
                                                      'coalmine_msha_id'])

    # drop null values from foreign key fields
    coalmine_df.dropna(subset=['coalmine_name', ], inplace=True)

    # Take a float field and make it an integer, with the empty String
    # as the NA value... for postgres loading. Yes, this is janky.
    coalmine_df['coalmine_msha_id'] = \
        fix_int_na(coalmine_df['coalmine_msha_id'],
                   float_na=np.nan,
                   int_na=-1,
                   str_na='')

    # Write the dataframe out to a csv file and load it directly
    csv_dump_load(coalmine_df, 'coalmine_info_eia923', pudl_engine,
                  csvdir=csvdir, keep_csv=keep_csv)

    frc_df = eia923_dfs['fuel_receipts_costs'].copy()

    # Drop fields we're not inserting into the fuel_receipts_costs_eia923
    # table.
    cols_to_drop = ['plant_name',
                    'plant_state',
                    'operator_name',
                    'operator_id',
                    'coalmine_msha_id',
                    'coalmine_type',
                    'coalmine_state',
                    'coalmine_county',
                    'coalmine_name',
                    'regulated',
                    'reporting_frequency']

    frc_df.drop(cols_to_drop, axis=1, inplace=True)

    # Replace the EIA923 NA value ('.') with a real NA value.
    frc_df.replace(to_replace='^\.$', value=np.nan, regex=True, inplace=True)

    # Standardize case on transportaion codes -- all upper case!
    frc_df['primary_transportation_mode'] = \
        frc_df['primary_transportation_mode'].str.upper()
    frc_df['secondary_transportation_mode'] = \
        frc_df['secondary_transportation_mode'].str.upper()

    frc_df['contract_expiration_date'] = \
        fix_int_na(frc_df['contract_expiration_date'],
                   float_na=np.nan,
                   int_na=-1,
                   str_na='')
    # Write the dataframe out to a csv file and load it directly
    csv_dump_load(frc_df, 'fuel_receipts_costs_eia923', pudl_engine,
                  csvdir=csvdir, keep_csv=keep_csv)


def ingest_stocks_eia923(pudl_engine, eia923_dfs, csvdir='', keep_csv=True):
    """Ingest data on fuel stocks from EIA Form 923."""
    pass


###############################################################################
###############################################################################
# BEGIN DATABASE INITIALIZATION
###############################################################################
###############################################################################


def init_db(ferc1_tables=ferc1_pudl_tables,
            ferc1_years=range(2007, 2016),
            eia923_tables=eia923_pudl_tables,
            eia923_years=range(2011, 2016),
            verbose=True, debug=False, testing=False,
            csvdir=os.path.join(settings.PUDL_DIR, 'results', 'csvdump'),
            keep_csv=True):
    """
    Create the PUDL database and fill it up with data.

    Args:
        ferc1_tables (list): The list of tables that will be created and
            ingested. By default only known to be working tables are ingested.
            That list of tables is defined in pudl.constants.
        ferc1_years (list): The list of years from which to pull FERC Form 1
            data.
        eia923_tables (list): The list of tables that will be created and
            ingested. By default only known to be working tables are ingested.
            That list of tables is defined in pudl.constants.
        eia923_years (list): The list of years from which to pull EIA 923
            data.
        debug (bool): You can tell init_db to ingest whatever list of tables
            you want, but if your desired table is not in the list of known to
            be working tables, you need to set debug=True (otherwise init_db
            won't let you).
    """
    # Make sure that the tables we're being asked to ingest can actually be
    # pulled into both the FERC Form 1 DB, and the PUDL DB...
    if not debug:
        for table in ferc1_tables:
            assert(table in ferc1_working_tables)
            assert(table in ferc1_pudl_tables)

    if not debug:
        for table in eia923_tables:
            assert(table in eia923_pudl_tables)

    # Connect to the PUDL DB, wipe out & re-create tables:
    pudl_engine = db_connect_pudl(testing=testing)
    drop_tables_pudl(pudl_engine)
    create_tables_pudl(pudl_engine)
    # Populate all the static tables:
    if verbose:
        print("Ingesting static PUDL tables...")
    ingest_static_tables(pudl_engine)
    # Populate tables that relate FERC1 & EIA923 data to each other.
    if verbose:
        print("Sniffing EIA923/FERC1 glue tables...")
    ingest_glue_tables(pudl_engine)

    # BEGIN INGESTING FERC FORM 1 DATA:
    ferc1_ingest_functions = {
        'f1_fuel': ingest_fuel_ferc1,
        'f1_steam': ingest_plants_steam_ferc1,
        'f1_gnrt_plant': ingest_plants_small_ferc1,
        'f1_hydro': ingest_plants_hydro_ferc1,
        'f1_pumped_storage': ingest_plants_pumped_storage_ferc1,
        'f1_plant_in_srvce': ingest_plant_in_service_ferc1,
        'f1_purchased_pwr': ingest_purchased_power_ferc1,
        'f1_accumdepr_prvsn': ingest_accumulated_depreciation_ferc1}

    ferc1_engine = db_connect_ferc1(testing=testing)
    for table in ferc1_ingest_functions.keys():
        if table in ferc1_tables:
            if verbose:
                print("Ingesting {} from FERC Form 1 into PUDL.".format(table))
            ferc1_ingest_functions[table](pudl_engine,
                                          ferc1_engine,
                                          ferc1_years)

    eia923_xlsx = get_eia923_xlsx(eia923_years)
    eia923_dfs = {}

    if 'plant_info_eia923' in eia923_tables:
        for page in ['plant_frame', 'generation_fuel']:
            eia923_dfs[page] = get_eia923_page(page,
                                               eia923_xlsx,
                                               years=eia923_years,
                                               verbose=verbose)
    if ('generation_fuel_eia923' in eia923_tables) \
            and ('generation_fuel' not in eia923_dfs.keys()):
        eia923_dfs['generation_fuel'] = get_eia923_page('generation_fuel',
                                                        eia923_xlsx,
                                                        years=eia923_years,
                                                        verbose=verbose)

    if 'fuel_stocks_eia923' in eia923_tables:
        pass  # no DB table defined for fuel stocks yet.

    if 'boiler_fuel_eia923' in eia923_tables:
        eia923_dfs['boiler_fuel'] = get_eia923_page('boiler_fuel',
                                                    eia923_xlsx,
                                                    years=eia923_years,
                                                    verbose=verbose)
    if 'generation_eia923' in eia923_tables:
        eia923_dfs['generator'] = get_eia923_page('generator',
                                                  eia923_xlsx,
                                                  years=eia923_years,
                                                  verbose=verbose)
    if 'fuel_receipts_costs_eia923' in eia923_tables:
        eia923_dfs['fuel_receipts_costs'] = \
            get_eia923_page('fuel_receipts_costs',
                            eia923_xlsx,
                            years=eia923_years,
                            verbose=verbose)

    # NOW START INGESTING EIA923 DATA:
    eia923_ingest_functions = {
        'plant_info_eia923': ingest_plant_info_eia923,
        'generation_fuel_eia923': ingest_generation_fuel_eia923,
        'boiler_fuel_eia923': ingest_boiler_fuel_eia923,
        'generation_eia923': ingest_generator_eia923,
        'fuel_receipts_costs_eia923': ingest_fuel_receipts_costs_eia923,
        'stocks_eia923': ingest_stocks_eia923
        #    'operator_info_eia923': ingest_operator_info_eia923
    }

    for table in eia923_ingest_functions.keys():
        if table in eia923_tables:
            if verbose:
                print("Ingesting {} from EIA 923 into PUDL.".format(table))
            eia923_ingest_functions[table](pudl_engine, eia923_dfs,
                                           csvdir=csvdir, keep_csv=keep_csv)
