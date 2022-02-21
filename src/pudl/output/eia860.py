"""Functions for pulling data primarily from the EIA's Form 860."""

import logging
from collections import defaultdict

import pandas as pd
import sqlalchemy as sa

import pudl
from pudl.metadata.fields import apply_pudl_dtypes

logger = logging.getLogger(__name__)


def utilities_eia860(pudl_engine, start_date=None, end_date=None):
    """Pull all fields from the EIA860 Utilities table.

    Args:
        pudl_engine (sqlalchemy.engine.Engine): SQLAlchemy connection engine
            for the PUDL DB.
        start_date (date-like): date-like object, including a string of the
            form 'YYYY-MM-DD' which will be used to specify the date range of
            records to be pulled.  Dates are inclusive.
        end_date (date-like): date-like object, including a string of the
            form 'YYYY-MM-DD' which will be used to specify the date range of
            records to be pulled.  Dates are inclusive.

    Returns:
        pandas.DataFrame: A DataFrame containing all the fields of the EIA 860
        Utilities table.

    """
    pt = pudl.output.pudltabl.get_table_meta(pudl_engine)
    # grab the entity table
    utils_eia_tbl = pt['utilities_entity_eia']
    utils_eia_select = sa.sql.select(utils_eia_tbl)
    utils_eia_df = pd.read_sql(utils_eia_select, pudl_engine)

    # grab the annual eia entity table
    utils_eia860_tbl = pt['utilities_eia860']
    utils_eia860_select = sa.sql.select(utils_eia860_tbl)

    if start_date is not None:
        start_date = pd.to_datetime(start_date)
        utils_eia860_select = utils_eia860_select.where(
            utils_eia860_tbl.c.report_date >= start_date
        )
    if end_date is not None:
        end_date = pd.to_datetime(end_date)
        utils_eia860_select = utils_eia860_select.where(
            utils_eia860_tbl.c.report_date <= end_date
        )
    utils_eia860_df = pd.read_sql(utils_eia860_select, pudl_engine)

    # grab the glue table for the utility_id_pudl
    utils_g_eia_tbl = pt['utilities_eia']
    utils_g_eia_select = sa.sql.select(
        utils_g_eia_tbl.c.utility_id_eia,
        utils_g_eia_tbl.c.utility_id_pudl,
    )
    utils_g_eia_df = pd.read_sql(utils_g_eia_select, pudl_engine)

    out_df = pd.merge(utils_eia_df, utils_eia860_df,
                      how='left', on=['utility_id_eia'])
    out_df = pd.merge(out_df, utils_g_eia_df,
                      how='left', on=['utility_id_eia'])
    out_df = (
        out_df.assign(report_date=lambda x: pd.to_datetime(x.report_date))
        .dropna(subset=["report_date", "utility_id_eia"])
        .pipe(apply_pudl_dtypes, group="eia")
    )
    first_cols = [
        'report_date',
        'utility_id_eia',
        'utility_id_pudl',
        'utility_name_eia',
    ]

    out_df = pudl.helpers.organize_cols(out_df, first_cols)
    return out_df


def plants_eia860(pudl_engine, start_date=None, end_date=None):
    """Pull all fields from the EIA Plants tables.

    Args:
        pudl_engine (sqlalchemy.engine.Engine): SQLAlchemy connection engine
            for the PUDL DB.
        start_date (date-like): date-like object, including a string of the
            form 'YYYY-MM-DD' which will be used to specify the date range of
            records to be pulled.  Dates are inclusive.
        end_date (date-like): date-like object, including a string of the
            form 'YYYY-MM-DD' which will be used to specify the date range of
            records to be pulled.  Dates are inclusive.

    Returns:
        pandas.DataFrame: A DataFrame containing all the fields of the EIA 860
        Plants table.

    """
    pt = pudl.output.pudltabl.get_table_meta(pudl_engine)
    # grab the entity table
    plants_eia_tbl = pt['plants_entity_eia']
    plants_eia_select = sa.sql.select(plants_eia_tbl)
    plants_eia_df = pd.read_sql(plants_eia_select, pudl_engine)

    # grab the annual table select
    plants_eia860_tbl = pt['plants_eia860']
    plants_eia860_select = sa.sql.select(plants_eia860_tbl)
    if start_date is not None:
        start_date = pd.to_datetime(start_date)
        plants_eia860_select = plants_eia860_select.where(
            plants_eia860_tbl.c.report_date >= start_date
        )
    if end_date is not None:
        end_date = pd.to_datetime(end_date)
        plants_eia860_select = plants_eia860_select.where(
            plants_eia860_tbl.c.report_date <= end_date
        )
    plants_eia860_df = (
        pd.read_sql(plants_eia860_select, pudl_engine)
        .assign(report_date=lambda x: pd.to_datetime(x.report_date))
    )

    # plant glue table
    plants_g_eia_tbl = pt['plants_eia']
    plants_g_eia_select = sa.sql.select(
        plants_g_eia_tbl.c.plant_id_eia,
        plants_g_eia_tbl.c.plant_id_pudl,
    )
    plants_g_eia_df = pd.read_sql(plants_g_eia_select, pudl_engine)

    out_df = pd.merge(
        plants_eia_df, plants_eia860_df, how='left', on=['plant_id_eia'])
    out_df = pd.merge(out_df, plants_g_eia_df, how='left', on=['plant_id_eia'])

    utils_eia_tbl = pt['utilities_eia']
    utils_eia_select = sa.sql.select(utils_eia_tbl)
    utils_eia_df = pd.read_sql(utils_eia_select, pudl_engine)

    out_df = (
        pd.merge(out_df, utils_eia_df, how='left', on=['utility_id_eia'])
        .dropna(subset=["report_date", "plant_id_eia"])
        .pipe(apply_pudl_dtypes, group="eia")
    )
    return out_df


def plants_utils_eia860(pudl_engine, start_date=None, end_date=None):
    """Create a dataframe of plant and utility IDs and names from EIA 860.

    Returns a pandas dataframe with the following columns:
    - report_date (in which data was reported)
    - plant_name_eia (from EIA entity)
    - plant_id_eia (from EIA entity)
    - plant_id_pudl
    - utility_id_eia (from EIA860)
    - utility_name_eia (from EIA860)
    - utility_id_pudl

    Args:
        pudl_engine (sqlalchemy.engine.Engine): SQLAlchemy connection engine
            for the PUDL DB.
        start_date (date-like): date-like object, including a string of the
            form 'YYYY-MM-DD' which will be used to specify the date range of
            records to be pulled.  Dates are inclusive.
        end_date (date-like): date-like object, including a string of the
            form 'YYYY-MM-DD' which will be used to specify the date range of
            records to be pulled.  Dates are inclusive.

    Returns:
        pandas.DataFrame: A DataFrame containing plant and utility IDs and
        names from EIA 860.

    """
    # Contains the one-to-one mapping of EIA plants to their operators
    plants_eia = (
        plants_eia860(pudl_engine, start_date=start_date, end_date=end_date)
        .drop(['utility_id_pudl', 'city', 'state',  # Avoid dupes in merge
               'zip_code', 'street_address', 'utility_name_eia'],
              axis='columns')
        .dropna(subset=["utility_id_eia"])  # Drop unmergable records
    )
    utils_eia = utilities_eia860(pudl_engine,
                                 start_date=start_date,
                                 end_date=end_date)

    # to avoid duplicate columns on the merge...
    out_df = pd.merge(plants_eia, utils_eia,
                      how='left', on=['report_date', 'utility_id_eia'])

    out_df = (
        out_df.loc[:, ['report_date',
                       'plant_id_eia',
                       'plant_name_eia',
                       'plant_id_pudl',
                       'utility_id_eia',
                       'utility_name_eia',
                       'utility_id_pudl']
                   ]
        .dropna(subset=["report_date", "plant_id_eia", "utility_id_eia"])
        .pipe(apply_pudl_dtypes, group="eia")
    )
    return out_df


def generators_eia860(
    pudl_engine: sa.engine.Engine,
    start_date=None,
    end_date=None,
    unit_ids: bool = False,
    fill_tech_desc: bool = True,
) -> pd.DataFrame:
    """Pull all fields reported in the generators_eia860 table.

    Merge in other useful fields including the latitude & longitude of the
    plant that the generators are part of, canonical plant & operator names and
    the PUDL IDs of the plant and operator, for merging with other PUDL data
    sources.

    Fill in data for adjacent years if requested, but never fill in earlier
    than the earliest working year of data for EIA923, and never add more than
    one year on after the reported data (since there should at most be a one
    year lag between EIA923 and EIA860 reporting)

    This also fills the ``technology_description`` field according to matching
    ``energy_source_code_1`` values. It will only do so if the ``energy_source_code_1``
    is consistent throughout years for a given plant.

    Args:
        pudl_engine: SQLAlchemy connection engine for the PUDL DB.
        start_date (date-like): date-like object, including a string of the
            form 'YYYY-MM-DD' which will be used to specify the date range of
            records to be pulled.  Dates are inclusive.
        end_date (date-like): date-like object, including a string of the
            form 'YYYY-MM-DD' which will be used to specify the date range of
            records to be pulled.  Dates are inclusive.
        unit_ids: If True, use several heuristics to assign
            individual generators to functional units. EXPERIMENTAL.
        fill_tech_desc: If True, backfill the technology_description
            field to years earlier than 2013 based on plant and
            energy_source_code_1 and fill in technologies with only one matching code.

    Returns:
        A DataFrame containing all the fields of the EIA 860 Generators table.

    """
    pt = pudl.output.pudltabl.get_table_meta(pudl_engine)
    # Almost all the info we need will come from here.
    gens_eia860_tbl = pt['generators_eia860']
    gens_eia860_select = sa.sql.select(gens_eia860_tbl)
    # To get plant age
    generators_entity_eia_tbl = pt['generators_entity_eia']
    generators_entity_eia_select = sa.sql.select(generators_entity_eia_tbl)
    # To get the Lat/Lon coordinates
    plants_entity_eia_tbl = pt['plants_entity_eia']
    plants_entity_eia_select = sa.sql.select(plants_entity_eia_tbl)

    if start_date is not None:
        start_date = pd.to_datetime(start_date)
        gens_eia860_select = gens_eia860_select.where(
            gens_eia860_tbl.c.report_date >= start_date
        )

    if end_date is not None:
        end_date = pd.to_datetime(end_date)
        gens_eia860_select = gens_eia860_select.where(
            gens_eia860_tbl.c.report_date <= end_date
        )

    gens_eia860 = pd.read_sql(gens_eia860_select, pudl_engine)
    generators_entity_eia_df = pd.read_sql(
        generators_entity_eia_select, pudl_engine)

    plants_entity_eia_df = pd.read_sql(plants_entity_eia_select, pudl_engine)

    out_df = pd.merge(gens_eia860, plants_entity_eia_df,
                      how='left', on=['plant_id_eia'])
    out_df = pd.merge(out_df, generators_entity_eia_df,
                      how='left', on=['plant_id_eia', 'generator_id'])

    out_df.report_date = pd.to_datetime(out_df.report_date)

    # Bring in some generic plant & utility information:
    pu_eia = (
        plants_utils_eia860(
            pudl_engine, start_date=start_date, end_date=end_date)
        .drop(["plant_name_eia", "utility_id_eia"], axis="columns")
    )
    out_df = pd.merge(out_df, pu_eia,
                      on=['report_date', 'plant_id_eia'],
                      how="left")

    # Merge in the unit_id_pudl assigned to each generator in the BGA process
    # Pull the BGA table and make it unit-generator only:
    out_df = pd.merge(
        out_df,
        boiler_generator_assn_eia860(
            pudl_engine, start_date=start_date, end_date=end_date
        )[[
            "report_date",
            "plant_id_eia",
            "generator_id",
            "unit_id_pudl",
            "bga_source",
        ]].drop_duplicates(),
        on=["report_date", "plant_id_eia", "generator_id"],
        how="left",
        validate="m:1",
    )

    # In order to be able to differentiate between single and multi-fuel
    # plants, we need to count how many different simple energy sources there
    # are associated with plant's generators. This allows us to do the simple
    # lumping of an entire plant's fuel & generation if its primary fuels
    # are homogeneous, and split out fuel & generation by fuel if it is
    # hetereogeneous.
    ft_count = out_df[['plant_id_eia', 'fuel_type_code_pudl', 'report_date']].\
        drop_duplicates().groupby(['plant_id_eia', 'report_date']).count()
    ft_count = ft_count.reset_index()
    ft_count = ft_count.rename(
        columns={'fuel_type_code_pudl': 'fuel_type_count'})
    out_df = (
        pd.merge(out_df, ft_count, how='left',
                 on=['plant_id_eia', 'report_date'])
        .dropna(subset=["report_date", "plant_id_eia", "generator_id"])
        .pipe(apply_pudl_dtypes, group="eia")
    )
    # Augment those base unit_id_pudl values using heuristics, see below.
    if unit_ids:
        logger.info("Assigning pudl unit ids")
        out_df = assign_unit_ids(out_df)

    if fill_tech_desc:
        logger.info("Filling technology type")
        out_df = fill_generator_technology_description(out_df)

    first_cols = [
        'report_date',
        'plant_id_eia',
        'plant_id_pudl',
        'plant_name_eia',
        'utility_id_eia',
        'utility_id_pudl',
        'utility_name_eia',
        'generator_id',
    ]

    # Re-arrange the columns for easier readability:
    out_df = (
        pudl.helpers.organize_cols(out_df, first_cols)
        .sort_values(['report_date', 'plant_id_eia', 'generator_id'])
        .pipe(apply_pudl_dtypes, group="eia")
    )

    return out_df


def fill_generator_technology_description(gens_df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill in missing ``technology_description`` based on generator and energy source.

    Prior to 2014, the EIA 860 did not report ``technology_description``. This
    function backfills those early years within groups defined by ``plant_id_eia``,
    ``generator_id`` and ``energy_source_code_1``. Some remaining missing values are
    then filled in using the consistent, unique mappings that are observed between
    ``energy_source_code_1`` and ``technology_type`` across all years and generators.

    As a result, more than 95% of all generator records end up having a
    ``technology_description`` associated with them.

    Parameters:
        gens_df: A generators_eia860 dataframe containing at least the columns
            ``report_date``, ``plant_id_eia``, ``generator_id``,
            ``energy_source_code_1``, and ``technology_description``.

    Returns:
        A copy of the input dataframe, with ``technology_description`` filled in.

    """
    nrows_orig = len(gens_df)
    out_df = gens_df.copy()

    # Backfill within generator-energy_source groups:
    out_df["technology_description"] = (
        out_df
        .sort_values("report_date")
        .groupby(["plant_id_eia", "generator_id", "energy_source_code_1"])
        .technology_description.bfill()
    )

    # Fill in remaining missing technology_descriptions with unique correspondences
    # between energy_source_code_1 where possible. Use a default value of pd.NA
    # for any technology_description that isn't uniquely identified by energy source
    static_fuels = defaultdict(
        lambda: pd.NA,
        gens_df.dropna(subset=['technology_description'])
        .drop_duplicates(subset=['energy_source_code_1', 'technology_description'])
        .drop_duplicates(subset=['energy_source_code_1'], keep=False)
        .set_index('energy_source_code_1')
        ['technology_description'].to_dict()
    )

    out_df.loc[
        out_df.technology_description.isna(),
        "technology_description"
    ] = (out_df.energy_source_code_1.map(static_fuels))

    assert len(out_df) == nrows_orig

    # Assert that at least 95 percent of tech desc rows are filled in
    pct_val = 0.95
    if out_df.technology_description.count() \
        / out_df.technology_description.size \
            < pct_val:
        raise AssertionError(
            f"technology_description filling no longer covering {pct_val:.0%}")

    return out_df


def boiler_generator_assn_eia860(pudl_engine, start_date=None, end_date=None):
    """Pull all fields from the EIA 860 boiler generator association table.

    Args:
        pudl_engine (sqlalchemy.engine.Engine): SQLAlchemy connection engine
            for the PUDL DB.
        start_date (date-like): date-like object, including a string of the
            form 'YYYY-MM-DD' which will be used to specify the date range of
            records to be pulled.  Dates are inclusive.
        end_date (date-like): date-like object, including a string of the
            form 'YYYY-MM-DD' which will be used to specify the date range of
            records to be pulled.  Dates are inclusive.

    Returns:
        pandas.DataFrame: A DataFrame containing all the fields from the EIA
        860 boiler generator association table.

    """
    pt = pudl.output.pudltabl.get_table_meta(pudl_engine)
    bga_eia860_tbl = pt['boiler_generator_assn_eia860']
    bga_eia860_select = sa.sql.select(bga_eia860_tbl)

    if start_date is not None:
        start_date = pd.to_datetime(start_date)
        bga_eia860_select = bga_eia860_select.where(
            bga_eia860_tbl.c.report_date >= start_date
        )
    if end_date is not None:
        end_date = pd.to_datetime(end_date)
        bga_eia860_select = bga_eia860_select.where(
            bga_eia860_tbl.c.report_date <= end_date
        )
    out_df = (
        pd.read_sql(bga_eia860_select, pudl_engine)
        .assign(report_date=lambda x: pd.to_datetime(x.report_date))
    )
    return out_df


def ownership_eia860(pudl_engine, start_date=None, end_date=None):
    """Pull a useful set of fields related to ownership_eia860 table.

    Args:
        pudl_engine (sqlalchemy.engine.Engine): SQLAlchemy connection engine
            for the PUDL DB.
        start_date (date-like): date-like object, including a string of the
            form 'YYYY-MM-DD' which will be used to specify the date range of
            records to be pulled.  Dates are inclusive.
        end_date (date-like): date-like object, including a string of the
            form 'YYYY-MM-DD' which will be used to specify the date range of
            records to be pulled.  Dates are inclusive.

    Returns:
        pandas.DataFrame: A DataFrame containing a useful set of fields related
        to the EIA 860 Ownership table.

    """
    pt = pudl.output.pudltabl.get_table_meta(pudl_engine)
    own_eia860_tbl = pt["ownership_eia860"]
    own_eia860_select = sa.sql.select(own_eia860_tbl)

    if start_date is not None:
        start_date = pd.to_datetime(start_date)
        own_eia860_select = own_eia860_select.where(
            own_eia860_tbl.c.report_date >= start_date
        )
    if end_date is not None:
        end_date = pd.to_datetime(end_date)
        own_eia860_select = own_eia860_select.where(
            own_eia860_tbl.c.report_date <= end_date
        )
    own_eia860_df = (
        pd.read_sql(own_eia860_select, pudl_engine)
        .assign(report_date=lambda x: pd.to_datetime(x["report_date"]))
    )

    pu_eia = (
        plants_utils_eia860(
            pudl_engine, start_date=start_date, end_date=end_date)
        .loc[:, ['plant_id_eia', 'plant_id_pudl', 'plant_name_eia',
                 'utility_name_eia', 'utility_id_pudl', 'report_date']]
    )

    out_df = (
        pd.merge(own_eia860_df, pu_eia,
                 how='left', on=['report_date', 'plant_id_eia'])
        .dropna(subset=[
            "report_date",
            "plant_id_eia",
            "generator_id",
            "owner_utility_id_eia",
        ])
        .pipe(apply_pudl_dtypes, group="eia")
    )

    first_cols = [
        'report_date',
        'plant_id_eia',
        'plant_id_pudl',
        'plant_name_eia',
        'utility_id_eia',
        'utility_id_pudl',
        'utility_name_eia',
        'generator_id',
        'owner_utility_id_eia',
        'owner_name',
    ]

    # Re-arrange the columns for easier readability:
    out_df = (
        pudl.helpers.organize_cols(out_df, first_cols)
    )

    return out_df


################################################################################
# Temporary integration of more complete unit_id_pudl assignments
# Eventually this should go into the boiler-generator-association process
# and these IDs should probably live in the BGA table with the other
# unit_id_pudl values derived from the BGA table and other heuristics.
################################################################################
def assign_unit_ids(gens_df):
    """
    Group generators into operational units using various heuristics.

    Splits a few columns off from the big generator dataframe and uses several
    heuristic functions to fill in missing unit_id_pudl values beyond those that
    are generated in the boiler generator association process. Then merges the
    new unit ID values back in to the generators dataframe.

    Args:
        gens_df (pandas.DataFrame): An EIA generator table. Must contain at
            least the columns: report_date, plant_id_eia, generator_id,
            unit_id_pudl, bga_source, fuel_type_code_pudl, prime_mover_code,

    Returns:
        pandas.DataFrame: Returned dataframe should only vary from the input in
        that some NA values in the ``unit_id_pudl`` and ``bga_source`` columns
        have been filled in with real values.

    Raises:
        ValueError: If the input dataframe is missing required columns.
        ValueError: If any generator is associated with more than one unit_id_pudl.
        AssertionError: If row or column indices are changed.
        AssertionError: If pre-existing unit_id_pudl or bga_source values are altered.
        AssertionError: If contents of any other columns are altered at all.

    """
    required_cols = [
        "plant_id_eia",
        "generator_id",
        "report_date",
        "unit_id_pudl",
        "bga_source",
        "fuel_type_code_pudl",
        "prime_mover_code",
    ]
    if not set(required_cols).issubset(gens_df.columns):
        missing_cols = set(required_cols).difference(gens_df.columns)
        errstr = f"Input DataFrame missing required columns: {missing_cols}."
        raise ValueError(errstr)

    unit_ids = (
        gens_df.loc[:, required_cols]
        # Forward and back fill preexisting Unit IDs:
        .pipe(fill_unit_ids)
        # Assign Unit IDs to the CT+CA CC generators:
        .pipe(assign_cc_unit_ids)
        # For whole-combined cycle (CC) and single-shaft combined cycle (CS)
        # units, we give each generator their own unit ID. We do the same for
        # internal combustion and simple-cycle gas combustion turbines.
        .pipe(
            assign_single_gen_unit_ids,
            prime_mover_codes=["CC", "CS", "GT", "IC"]
        )
        # Nuclear units don't report in boiler_fuel_eia923 or generation_eia923
        # Their fuel consumption is reported as mmbtu in generation_fuel_eia923
        # Their net generation also only shows up in generation_fuel_eia923
        # The generation_fuel_eia923 table records a "nuclear_unit_id" which
        # appears to be the same as the associated generator_id. However, we
        # can't use that as a unit_id_pudl since it might have a collision with
        # other already assigned unit_id_pudl values in the same plant for
        # generators with other fuel types. Thus we still need to assign them
        # a fuel-and-prime-mover based unit ID here. For now ALL nuclear plants
        # use steam turbines.
        .pipe(
            assign_single_gen_unit_ids,
            prime_mover_codes=["ST"],
            fuel_type_code_pudl="nuclear",
            label_prefix="nuclear",
        )
        # In these next 4 assignments, we lump together all steam turbine (ST)
        # generators that have a consistent simplified fuel_type_code_pudl
        # across all years within a given plant into the same unit, since we
        # won't be able to distinguish them in the generation_fuel_eia923
        # table. This will lump together solid fuels like BIT, LIG, SUB, PC etc.
        # under "coal".  There are a few cases in which a generator has truly
        # changed its fuel type, e.g. coal-to-gas conversions but these are
        # rare and insubstantial. They will not be assigned a Unit ID in this
        # process. Non-fuel steam generation is also left out (geothermal &
        # solar thermal)
        .pipe(
            assign_prime_fuel_unit_ids,
            prime_mover_code="ST",
            fuel_type_code_pudl="coal"
        )
        .pipe(
            assign_prime_fuel_unit_ids,
            prime_mover_code="ST",
            fuel_type_code_pudl="oil"
        )
        .pipe(
            assign_prime_fuel_unit_ids,
            prime_mover_code="ST",
            fuel_type_code_pudl="gas"
        )
        .pipe(
            assign_prime_fuel_unit_ids,
            prime_mover_code="ST",
            fuel_type_code_pudl="waste"
        )
        # Retain only the merge keys and output columns
        .loc[:, [
            "plant_id_eia",  # Merge key
            "generator_id",  # Merge key
            "report_date",   # Merge key
            "unit_id_pudl",  # Output column
            "bga_source"     # Output column
        ]]
    )
    # Check that each generator is only ever associated with a single unit,
    # at least within the codes that we've just assigned -- the Unit IDs that
    # are based on the EIA boiler-generator-association or other matching
    # methods could legitimately specify different units for generators over
    # time -- which could impact the forward-/back-filled IDs as well:
    old_codes = list(gens_df.bga_source.unique()) + ["bfill_units", "ffill_units"]
    gens_have_unique_unit = (
        unit_ids[~unit_ids.bga_source.isin(old_codes)]
        .groupby(["plant_id_eia", "generator_id"])["unit_id_pudl"]
        .nunique() <= 1  # nunique() == 0 when there are only NA values.
    ).all()
    if not gens_have_unique_unit:
        errstr = "Some generators are associated with more than one unit_id_pudl."
        raise ValueError(errstr)

    # Use natural composite primary key as the index
    gens_idx = ["plant_id_eia", "generator_id", "report_date"]
    unit_ids = unit_ids.set_index(gens_idx).sort_index()
    gens_df = gens_df.set_index(gens_idx).sort_index()

    # Check that our input DataFrame and unit IDs have identical row indices
    # This is a dumb hack b/c set_index() doesn't preserve index data types
    # under some circumstances, and so we have "object" and "int64" types
    # being used for plant_id_eia at this point, fml. Really this should just
    # be assert_index_equal() for the two df indices:
    pd.testing.assert_frame_equal(
        unit_ids.reset_index()[gens_idx],
        gens_df.reset_index()[gens_idx]
    )
    # Verify that anywhere out_df has a unit_id_pudl, it's identical in unit_ids
    pd.testing.assert_series_equal(
        gens_df.unit_id_pudl.dropna(),
        unit_ids.unit_id_pudl.loc[gens_df.unit_id_pudl.dropna().index]
    )
    # Verify that anywhere out_df has a bga_source, it's identical in unit_ids
    pd.testing.assert_series_equal(
        gens_df.bga_source.dropna(),
        unit_ids.bga_source.loc[gens_df.bga_source.dropna().index]
    )
    # We know that the indices are identical
    # We know that we aren't going to overwrite anything that isn't NA
    # Thus we should be able to just assign these values straight across.
    unit_cols = ["unit_id_pudl", "bga_source"]
    gens_df.loc[:, unit_cols] = unit_ids[unit_cols]

    return gens_df.reset_index()


def fill_unit_ids(gens_df):
    """
    Back and forward fill Unit IDs for each plant / gen combination.

    This routine assumes that the mapping of generators to units is constant
    over time, and extends those mappings into years where no boilers have
    been reported -- since in the BGA we can only connect generators to each
    other if they are both connected to a boiler.

    Prior to 2014, combined cycle units didn't report any "boilers" but in
    latter years, they have been given "boilers" that correspond to their
    generators, so that all of their fuel consumption is recorded alongside
    that of other types of generators.

    The bga_source field is set to "bfill_units" for those that were backfilled,
    and "ffill_units" for those that were forward filled.

    Note: We could back/forward fill the boiler IDs prior to the BGA process and
    we ought to get consistent units across all the years that are the same as
    what we fill in here. We could also back/forward fill boiler IDs and Unit
    IDs after the fact, and we *should* get the same result. this will address
    many currently "boilerless" CCNG units that use generator ID as boiler ID in
    the latter years. We could try and apply this more generally, but in cases
    of generator IDs that haven't been used as boiler IDs, it would break the
    foreign key relationship with the boiler table, unless we added them there
    too, which seems like too much deep muddling.

    Args:
        gens_df (pandas.DataFrame): An generators_eia860 dataframe, which must
            contain columns: report_date, plant_id_eia, generator_id,
            unit_id_pudl, bga_source.

    Returns:
        pandas.DataFrame: with the same columns as the input dataframe, but
        having some NA values filled in for both the unit_id_pudl and bga_source
        columns.

    """
    # forward and backward fill the unit IDs
    gen_ids = ["plant_id_eia", "generator_id"]
    gens_df = gens_df.sort_values(["report_date", "plant_id_eia", "generator_id"])

    bfill_units = gens_df.groupby(gen_ids)["unit_id_pudl"].bfill()
    bfill_idx = (bfill_units.notnull()) & (gens_df.unit_id_pudl.isnull())
    gens_df.loc[bfill_idx, "bga_source"] = "bfill_units"
    gens_df.loc[bfill_idx, "unit_id_pudl"] = bfill_units.loc[bfill_idx]

    ffill_units = gens_df.groupby(gen_ids)["unit_id_pudl"].ffill()
    ffill_idx = (ffill_units.notnull()) & (gens_df.unit_id_pudl.isnull())
    gens_df.loc[ffill_idx, "bga_source"] = "ffill_units"
    gens_df.loc[ffill_idx, "unit_id_pudl"] = ffill_units.loc[ffill_idx]
    gens_df["bga_source"] = gens_df["bga_source"].astype(pd.StringDtype())

    return gens_df


def max_unit_id_by_plant(gens_df):
    """
    Identify the largest unit ID associated with each plant so we don't overlap.

    The PUDL Unit IDs are sequentially assigned integers. To assign a new ID, we
    need to know the largest existing Unit ID within a plant. This function
    calculates that largest existing ID, or uses zero, if no Unit IDs are set
    within the plant.

    Note that this calculation depends on having all of the pre-existing
    generators and units still available in the dataframe!

    Args:
        gens_df (pandas.DataFrame): A generators_eia860 dataframe containing at
            least the columns plant_id_eia and unit_id_pudl.

    Returns:
        pandas.DataFrame: Having two columns: plant_id_eia and max_unit_id_pudl
        in which each row should be unique.

    """
    return (
        gens_df[["plant_id_eia", "unit_id_pudl"]]
        .drop_duplicates()
        .groupby("plant_id_eia").agg({"unit_id_pudl": max})
        .fillna(0)
        .rename(columns={"unit_id_pudl": "max_unit_id_pudl"})
        .reset_index()
    )


def _append_masked_units(gens_df, row_mask, unit_ids, on):
    """
    Replace rows with new PUDL Unit IDs in the original dataframe.

    Merges the newly assigned Unit IDs found in ``unit_ids`` into the
    ``gens_df`` dataframe, but only for those rows which are selected by the
    boolean ``row_mask``. Merges using the column or columns specified by
    ``on``. This operation should only result in changes to the values of
    ``unit_id_pudl`` and ``bga_source`` in the output dataframe. All of
    ``gens_df``, ``unit_ids`` and ``row_mask`` must be similarly indexed for
    this to work.

    Args:
        gens_df (pandas.DataFrame): a gens_eia860 based dataframe.
        row_mask (boolean mask): A boolean array indicating which records
            in ``gens_df`` should be replaced using values from ``unit_ids``.
        unit_ids (pandas.DataFrame): A dataframe containing newly assigned
            ``unit_id_pudl`` values to be integrated into ``gens_df``.
        on (str or list): Column or list of columns to merge on.

    Returns:
        pandas.DataFrame:

    """
    return gens_df.loc[~row_mask].append(
        gens_df.loc[row_mask]
        .drop(["unit_id_pudl", "bga_source"], axis="columns")
        .merge(
            unit_ids,
            on=on,
            how="left",
            validate="many_to_one",
        )
    )


def assign_single_gen_unit_ids(
    gens_df,
    prime_mover_codes,
    fuel_type_code_pudl=None,
    label_prefix="single"
):
    """
    Assign a unique PUDL Unit ID to each generator of a given prime mover type.

    Calculate the maximum pre-existing PUDL Unit ID within each plant, and
    assign each as of yet unidentified distinct generator within each plant
    with an incrementing integer unit_id_pudl, beginning with 1 + the previous
    maximum unit_id_pudl found in that plant. Mark that generator with a label
    in the bga_source column consisting of label_prefix + the prime mover code.

    If fuel_type_code_pudl is not None, then only assign new Unit IDs to those
    generators having the specified fuel type code, and use that fuel type code
    as the label prefix, e.g. "coal_st" for a coal-fired steam turbine.

    Only generators having NA unit_id_pudl will be assigned a new ID.

    Args:
        gens_df (pandas.DataFrame): A collection of EIA generator records.
            Must include the ``plant_id_eia``, ``generator_id`` and
            ``prime_mover_code`` and ``unit_id_pudl`` columns.
        prime_mover_codes (list): List of prime mover codes for which we are
            attempting to assign simple Unit IDs.
        fuel_type_code_pudl (str, None): If not None, then limit the records
            assigned a unit_id to those that have the specified
            fuel_type_code_pudl (e.g. "coal", "gas", "oil", "nuclear")
        label_prefix (str): String to use in labeling records as to how their
            unit_id_pudl was set. Will be concatenated with the prime mover
            code.

    Returns:
        pandas.DataFrame: A new dataframe with the same rows and columns as
        were passed in, but with the unit_id_pudl and bga_source columns updated
        to reflect the newly assigned Unit IDs.

    """
    if fuel_type_code_pudl is not None:
        # Need to make this only apply to consistent inter-year fuel types.
        fuel_type_mask = gens_df.fuel_type_code_pudl == fuel_type_code_pudl
    else:
        fuel_type_mask = True

    # Only alter the rows lacking Unit IDs and matching our target rows
    row_mask = (
        (gens_df.prime_mover_code.isin(prime_mover_codes))
        & (gens_df.unit_id_pudl.isnull())
        & (fuel_type_mask)
    )
    # We only need a few columns to make these assignments.
    cols = ["plant_id_eia", "generator_id", "unit_id_pudl", "prime_mover_code"]

    logger.info(
        "Selected %s %s records lacking Unit IDs from %s records overall. ",
        row_mask.sum(), prime_mover_codes, len(gens_df)
    )

    unit_ids = (
        gens_df.loc[row_mask, cols]
        .drop_duplicates()
        .merge(
            max_unit_id_by_plant(gens_df),
            on="plant_id_eia",
            how="left",
            validate="many_to_one",
        )
        # Assign new unit_id_pudl values based on number of distinct generators:
        .assign(
            unit_id_pudl=lambda x: (
                x.groupby("plant_id_eia")["generator_id"]
                .cumcount() + x.max_unit_id_pudl + 1
            ),
            bga_source=lambda x: label_prefix + "_" + x.prime_mover_code.str.lower(),
        )
        .drop(["max_unit_id_pudl", "prime_mover_code"], axis="columns")
    )
    # Split original dataframe based on row_mask, and merge in the new IDs and
    # labels only on the subset of the dataframe matching our row_mask:
    return _append_masked_units(
        gens_df, row_mask, unit_ids, on=["plant_id_eia", "generator_id"]
    )


def assign_cc_unit_ids(gens_df):
    """
    Assign PUDL Unit IDs for combined cycle generation units.

    This applies only to combined cycle units reported as a combination of CT
    and CA prime movers. All CT and CA generators within a plant that do not
    already have a unit_id_pudl assigned will be given the same unit ID. The
    ``bga_source`` column is set to one of several flags indicating what type
    of arrangement was found:

    * ``orphan_ct`` (zero CA gens, 1+ CT gens)
    * ``orphan_ca`` (zero CT gens, 1+ CA gens)
    * ``one_ct_one_ca_inferred`` (1 CT, 1 CA)
    * ``one_ct_many_ca_inferred`` (1 CT, 1+ CA)
    * ``many_ct_one_ca_inferred`` (1+ CT, 1 CA)
    * ``many_ct_many_ca_inferred`` (1+ CT, 1+ CA)

    Orphaned generators are still assigned a ``unit_id_pudl`` so that they can
    potentially be associated with other generators in the same unit across
    years. It's likely that these orphans are a result of mislabled or missing
    generators. Note that as generators are added or removed over time, the
    flags associated with each generator may change, even though it remains
    part of the same inferred unit.

    Returns:
        pandas.DataFrame

    """
    # Calculate the largest preexisting unit_id_pudl within each plant
    max_unit_ids = max_unit_id_by_plant(gens_df)

    cc_missing_units = gens_df[
        (gens_df.unit_id_pudl.isna())
        & gens_df.prime_mover_code.isin(["CT", "CA"])
    ]
    # On a per-plant, per-year basis, count up the number of CT and CA generators.
    # Only look at those which don't already have a unit ID assigned:
    cc_pm_counts = (
        cc_missing_units
        .groupby(["plant_id_eia", "report_date"])["prime_mover_code"]
        .value_counts().unstack(fill_value=0).astype(int).reset_index()
    )
    cc_pm_counts.columns.name = None

    # Bring the max unit ID and PM counts into the DF so we can select and
    # assign based on them. We're using the cc_missing_units and a temporary
    # dataframe here to avoid interference from the CT & CA generators
    # that do already have unit IDs assigned to them in gens_df.
    tmp_df = (
        cc_missing_units
        .merge(
            max_unit_ids,
            on="plant_id_eia",
            how="left",
            validate="many_to_one",
        )
        .merge(
            cc_pm_counts,
            on=["plant_id_eia", "report_date"],
            how="left",
            validate="many_to_one",
        )
    )

    # Assign the new Unit IDs.
    # All CA and CT units get assigned to the same unit within a plant:
    tmp_df["unit_id_pudl"] = tmp_df["max_unit_id_pudl"] + 1

    # Assign the orphan flags
    tmp_df.loc[tmp_df.CA == 0, "bga_source"] = "orphan_ct"
    tmp_df.loc[tmp_df.CT == 0, "bga_source"] = "orphan_ca"
    # The orphan flags should only have been applied to generators that had
    # at least one prime mover of the orphaned type. Just checking...
    assert (tmp_df.loc[tmp_df.bga_source == "orphan_ct", "CT"] > 0).all()
    assert (tmp_df.loc[tmp_df.bga_source == "orphan_ca", "CA"] > 0).all()

    # Assign flags for various arrangements of CA and CT generators
    tmp_df.loc[((tmp_df.CT == 1) & (tmp_df.CA == 1)),
               "bga_source"] = "one_ct_one_ca_inferred"
    tmp_df.loc[((tmp_df.CT == 1) & (tmp_df.CA > 1)),
               "bga_source"] = "one_ct_many_ca_inferred"
    tmp_df.loc[((tmp_df.CT > 1) & (tmp_df.CA == 1)),
               "bga_source"] = "many_ct_one_ca_inferred"
    tmp_df.loc[((tmp_df.CT > 1) & (tmp_df.CA > 1)),
               "bga_source"] = "many_ct_many_ca_inferred"

    # Align the indices of the two dataframes so we can assign directly
    tmp_df = tmp_df.set_index(["plant_id_eia", "generator_id", "report_date"])
    out_df = gens_df.set_index(["plant_id_eia", "generator_id", "report_date"])
    out_df.loc[tmp_df.index, ["unit_id_pudl", "bga_source"]
               ] = tmp_df[["unit_id_pudl", "bga_source"]]

    return out_df.reset_index()


def assign_prime_fuel_unit_ids(gens_df, prime_mover_code, fuel_type_code_pudl):
    """
    Assign a PUDL Unit ID to all generators with a given prime mover and fuel.

    Within each plant, assign a Unit ID to all generators that don't have one,
    and that share the same `fuel_type_code_pudl` and `prime_mover_code`. This
    is especially useful for differentiating between different types of steam
    turbine generators, as there are so many different kinds of steam turbines,
    and the only characteristic we have to differentiate between them in this
    context is the fuel they consume. E.g. nuclear, geothermal, solar thermal,
    natural gas, diesel, and coal can all run steam turbines, but it doesn't
    make sense to lump those turbines together into a single unit just because
    they are located at the same plant.

    This routine only assigns a PUDL Unit ID to generators that have a
    consistently reported value of `fuel_type_code_pudl` across all of the years
    of data in `gens_df`. This consistency is important because otherwise the
    prime-fuel based unit assignment could put the same generator into different
    units in different years, which is currently not compatible with our concept
    of "units."

    Args:
        gens_df (pandas.DataFrame): A collection of EIA generator records.
            Must include the ``plant_id_eia``, ``generator_id`` and
            ``prime_mover_code`` and ``unit_id_pudl`` columns.
        prime_mover_code (str): List of prime mover codes for which we are
            attempting to assign simple Unit IDs.
        fuel_type_code_pudl (str): If not None, then limit the records
            assigned a unit_id to those that have the specified
            fuel_type_code_pudl (e.g. "coal", "gas", "oil", "nuclear")

    Returns:
        pandas.DataFrame:

    """
    # Find generators with a consistent fuel_type_code_pudl across all years.
    consistent_fuel = (
        gens_df.groupby(["plant_id_eia", "generator_id"])["fuel_type_code_pudl"]
        .transform(lambda x: x.nunique())
    ) == 1
    # This mask defines the generators generators we are going to alter:
    row_mask = (
        (gens_df.prime_mover_code == prime_mover_code)
        & (gens_df.unit_id_pudl.isna())
        & (gens_df.fuel_type_code_pudl == fuel_type_code_pudl)
        & (consistent_fuel)
    )

    # We only need a few columns to make these assignments.
    cols = ["plant_id_eia", "generator_id", "unit_id_pudl"]

    logger.info(
        "Selected %s %s records lacking Unit IDs burning %s from %s records overall.",
        row_mask.sum(), prime_mover_code, fuel_type_code_pudl, len(gens_df)
    )

    unit_ids = (
        gens_df.loc[row_mask, cols]
        .drop_duplicates()
        .merge(
            max_unit_id_by_plant(gens_df),
            on="plant_id_eia",
            how="left",
            validate="many_to_one",
        )
        # Assign all selected generators within each plant the next PUDL Unit ID.
        .assign(
            unit_id_pudl=lambda x: x.max_unit_id_pudl + 1,
            bga_source=lambda x: fuel_type_code_pudl + "_" + prime_mover_code.lower(),
        )
        .drop(["max_unit_id_pudl"], axis="columns")
    )

    # Split original dataframe based on row_mask, and merge in the new IDs and
    # labels only on the subset of the dataframe matching our row_mask:
    out_df = _append_masked_units(
        gens_df, row_mask, unit_ids, on=["plant_id_eia", "generator_id"]
    )

    # Find generators with inconsistent fuel_type_code_pudl so we can label them
    inconsistent_fuel = (
        out_df.groupby(["plant_id_eia", "generator_id"])["fuel_type_code_pudl"]
        .transform(lambda x: x.nunique())
    ) > 1

    inconsistent_fuel_mask = (
        (out_df.prime_mover_code == prime_mover_code)
        & (out_df.unit_id_pudl.isna())
        & (out_df.fuel_type_code_pudl == fuel_type_code_pudl)
        & (inconsistent_fuel)
    )
    out_df.loc[inconsistent_fuel_mask, "bga_source"] = (
        "inconsistent_" + fuel_type_code_pudl + "_" + prime_mover_code.lower()
    )
    return out_df
