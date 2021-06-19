"""Functions for pulling data primarily from the EIA's Form 860."""

import logging

import pandas as pd
import sqlalchemy as sa

import pudl

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
    utils_eia_select = sa.sql.select([utils_eia_tbl])
    utils_eia_df = pd.read_sql(utils_eia_select, pudl_engine)

    # grab the annual eia entity table
    utils_eia860_tbl = pt['utilities_eia860']
    utils_eia860_select = sa.sql.select([utils_eia860_tbl])

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
    utils_g_eia_select = sa.sql.select([
        utils_g_eia_tbl.c.utility_id_eia,
        utils_g_eia_tbl.c.utility_id_pudl,
    ])
    utils_g_eia_df = pd.read_sql(utils_g_eia_select, pudl_engine)

    out_df = pd.merge(utils_eia_df, utils_eia860_df,
                      how='left', on=['utility_id_eia'])
    out_df = pd.merge(out_df, utils_g_eia_df,
                      how='left', on=['utility_id_eia'])
    out_df = (
        out_df.assign(report_date=lambda x: pd.to_datetime(x.report_date))
        .dropna(subset=["report_date", "utility_id_eia"])
        .astype({
            "utility_id_eia": "Int64",
            "utility_id_pudl": "Int64",
        })
        .drop(['id'], axis='columns')
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
    plants_eia_select = sa.sql.select([plants_eia_tbl])
    plants_eia_df = pd.read_sql(plants_eia_select, pudl_engine)

    # grab the annual table select
    plants_eia860_tbl = pt['plants_eia860']
    plants_eia860_select = sa.sql.select([plants_eia860_tbl])
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
    plants_g_eia_select = sa.sql.select([
        plants_g_eia_tbl.c.plant_id_eia,
        plants_g_eia_tbl.c.plant_id_pudl,
    ])
    plants_g_eia_df = pd.read_sql(plants_g_eia_select, pudl_engine)

    out_df = pd.merge(
        plants_eia_df, plants_eia860_df, how='left', on=['plant_id_eia'])
    out_df = pd.merge(out_df, plants_g_eia_df, how='left', on=['plant_id_eia'])

    utils_eia_tbl = pt['utilities_eia']
    utils_eia_select = sa.sql.select([utils_eia_tbl])
    utils_eia_df = pd.read_sql(utils_eia_select, pudl_engine)

    out_df = (
        pd.merge(out_df, utils_eia_df, how='left', on=['utility_id_eia'])
        .drop(['id'], axis='columns')
        .dropna(subset=["report_date", "plant_id_eia"])
        .astype({
            "plant_id_eia": "Int64",
            "plant_id_pudl": "Int64",
            "utility_id_eia": "Int64",
            "utility_id_pudl": "Int64",
        })
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
        .astype({
            "plant_id_eia": "Int64",
            "plant_id_pudl": "Int64",
            "utility_id_eia": "Int64",
            "utility_id_pudl": "Int64",
        })
    )
    return out_df


def generators_eia860(pudl_engine, start_date=None, end_date=None):
    """Pull all fields reported in the generators_eia860 table.

    Merge in other useful fields including the latitude & longitude of the
    plant that the generators are part of, canonical plant & operator names and
    the PUDL IDs of the plant and operator, for merging with other PUDL data
    sources.

    Fill in data for adjacent years if requested, but never fill in earlier
    than the earliest working year of data for EIA923, and never add more than
    one year on after the reported data (since there should at most be a one
    year lag between EIA923 and EIA860 reporting)

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
        Generators table.
    """
    pt = pudl.output.pudltabl.get_table_meta(pudl_engine)
    # Almost all the info we need will come from here.
    gens_eia860_tbl = pt['generators_eia860']
    gens_eia860_select = sa.sql.select([gens_eia860_tbl, ])
    # To get plant age
    generators_entity_eia_tbl = pt['generators_entity_eia']
    generators_entity_eia_select = sa.sql.select([
        generators_entity_eia_tbl,
        # generators_entity_eia_tbl.c.report_date
    ])
    # To get the Lat/Lon coordinates
    plants_entity_eia_tbl = pt['plants_entity_eia']
    plants_entity_eia_select = sa.sql.select([
        plants_entity_eia_tbl,
    ])

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

    # Drop a few extraneous fields...
    out_df = out_df.drop(['id'], axis='columns')

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
        .astype({
            "plant_id_eia": "Int64",
            "plant_id_pudl": "Int64",
            "unit_id_pudl": "Int64",
            "utility_id_eia": "Int64",
            "utility_id_pudl": "Int64",
        })
    )
    # Augment those base unit_id_pudl values using heuristics, see below.
    # Should this be optional? It takes a minute. Maybe we can speed it up.
    out_df = assign_unit_ids(out_df)

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
    )

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
    bga_eia860_select = sa.sql.select([bga_eia860_tbl])

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
        .drop(['id'], axis='columns')
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
    # breakpoint()
    pt = pudl.output.pudltabl.get_table_meta(pudl_engine)
    own_eia860_tbl = pt["ownership_eia860"]
    own_eia860_select = sa.sql.select([own_eia860_tbl])

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
        .drop(['id'], axis='columns')
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
        .astype({
            "plant_id_eia": "Int64",
            "plant_id_pudl": "Int64",
            "utility_id_eia": "Int64",
            "utility_id_pudl": "Int64",
        })
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
    new columns back in.

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
        Error: If row or column indices are not equivalent before & after
        Error: If pre-existing unit_id_pudl or bga_source values are altered.
        Error: If contents of any other columns are altered at all
        Error: If any generator is associated with more than one unit_id_pudl

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
        .pipe(
            assign_simple_unit_ids,
            prime_mover_codes=["CC", "CS", "GT", "IC"]
        )
        .pipe(assign_unique_combined_cycle_unit_ids)
        .pipe(assign_complex_combined_cycle_unit_ids)
        .pipe(flag_orphan_combined_cycle_gens, orphan_code="CA")
        .pipe(flag_orphan_combined_cycle_gens, orphan_code="CT")
        .pipe(
            assign_simple_unit_ids,
            prime_mover_codes=["ST"],
            fuel_type_code_pudl="nuclear"
        )
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
    pd.testing.assert_index_equal(
        unit_ids.index,
        gens_df.index,
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

    Returns:
        pd.DataFrame: with columns: report_date, plant_id_eia, generator_id,
            unit_id_pudl, prime_mover_code, unit_id_source, unit_id_new

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

    This calculation depends on having all of the generators and units of all
    kinds still available in the dataframe!
    """
    return (
        gens_df[["plant_id_eia", "unit_id_pudl"]]
        .drop_duplicates()
        .groupby("plant_id_eia").agg({"unit_id_pudl": max})
        .fillna(0)
        .rename(columns={"unit_id_pudl": "max_unit_id_pudl"})
        .reset_index()
    )


def assign_simple_unit_ids(
    gens_df,
    prime_mover_codes,
    fuel_type_code_pudl=None,
    label_prefix="single"
):
    """
    Assign unique PUDL Unit IDs to all generators of a given prime mover type.

    Calculate the maximum pre-existing PUDL Unit ID within each plant, and
    assign each as of yet unidentified distinct generator within each plant
    with an incrementing integer unit_id_pudl, beginning with 1 + the previous
    maximum unit_id_pudl found in that plant. Mark that generator with the
    given label.

    Only generators with an NA unit_id_pudl will be assigned a new ID

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
        # If we're selecting based on fuel as well as prime mover, use that
        # in the label
        logger.info("Using fuel type %s as label_prefix.", fuel_type_code_pudl)
        label_prefix = fuel_type_code_pudl
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
    out_df = gens_df.loc[~row_mask].append(
        gens_df.loc[row_mask]
        .drop(["unit_id_pudl", "bga_source"], axis="columns")
        .merge(
            unit_ids,
            on=["plant_id_eia", "generator_id"],
            how="left",
            validate="many_to_one",
        )
    )

    return out_df


def assign_unique_combined_cycle_unit_ids(gens_df):
    """
    Assign unit IDs to combined cycle plants with a unique CT or CA generator.

    Within each plant, for any generator that has no Unit ID, count the number
    of generators of each prime mover type. Assign any collection of generators
    having only a single CT or a single CA a unit ID, and flag it "unique_cc".
    Assign collection of generators having multiple CTs and multiple CAs unit
    IDs and flag them "multi_cc"

    """
    pm_cols = ["plant_id_eia", "generator_id", "prime_mover_code"]
    # Get rid of the annual dependence, and only keep gens with missing PMs
    df = gens_df[gens_df.unit_id_pudl.isna()][pm_cols].drop_duplicates()
    # Count up the number of each kind of prime mover on a per-plant ID basis:
    pm_counts = (
        df.groupby(["plant_id_eia", "prime_mover_code"])
        .size()
        .unstack(fill_value=0)
    )
    # Plant IDs with at least one CT and one CA
    # and either a single CA or a single CT.
    # This makes the combined cycle unit grouping unambiguous.
    cc_plant_ids = pm_counts[
        (pm_counts["CA"] > 0)
        & (pm_counts["CT"] > 0)
        & ((pm_counts["CT"] == 1) | (pm_counts["CA"] == 1))
    ].index
    row_mask = (
        gens_df.plant_id_eia.isin(cc_plant_ids)
        & gens_df.unit_id_pudl.isnull()
        & gens_df.prime_mover_code.isin(["CA", "CT"])
    )

    # Create Unit IDs for CA/CT generators that are being grouped together.
    # Note that generator_id is not involved here because all of the
    # selected generators within each plant are going to get the same Unit ID
    unit_ids = (
        gens_df.loc[row_mask, ["plant_id_eia", "unit_id_pudl"]]
        .drop_duplicates()
        .merge(
            max_unit_id_by_plant(gens_df),
            on="plant_id_eia",
            how="left",
            validate="many_to_one",
        )
        # Assign new unit_id_pudl values by incrementing beyond the previous max
        .assign(
            unit_id_pudl=lambda x: x.max_unit_id_pudl + 1,
            bga_source=lambda x: "agg_unique_cc",
        )
        .drop("max_unit_id_pudl", axis="columns")
    )

    out_df = gens_df.loc[~row_mask].append(
        gens_df.loc[row_mask]
        .drop(["unit_id_pudl", "bga_source"], axis="columns")
        .merge(
            unit_ids,
            on="plant_id_eia",
            how="left",
            validate="many_to_one",
        )
    )
    return out_df


def assign_complex_combined_cycle_unit_ids(gens_df):
    """Assign unit IDs to combined cycle plants that must aggregate CT/CA PMs."""
    pm_cols = ["plant_id_eia", "generator_id", "prime_mover_code"]
    # Get rid of the annual dependence, and only keep gens with missing PMs
    df = gens_df[gens_df.unit_id_pudl.isna()][pm_cols].drop_duplicates()
    # Plant IDs with more than one CT and more than one CA
    cc_plant_ids = (
        df.groupby(["plant_id_eia", "prime_mover_code"])
        .size()
        .unstack(fill_value=0)
        .query("CA > 1 & CT > 1")
    ).index
    row_mask = (
        gens_df.plant_id_eia.isin(cc_plant_ids)
        & gens_df.unit_id_pudl.isnull()
        & gens_df.prime_mover_code.isin(["CA", "CT"])
    )

    # Create Unit IDs for CA/CT generators that are being grouped together.
    # Note that generator_id is not involved here because all of the
    # selected generators within each plant are going to get the same Unit ID
    unit_ids = (
        gens_df.loc[row_mask, ["plant_id_eia", "unit_id_pudl"]]
        .drop_duplicates()
        .merge(
            max_unit_id_by_plant(gens_df),
            on="plant_id_eia",
            how="left",
            validate="many_to_one",
        )
        # Assign new unit_id_pudl values by incrementing beyond the previous max
        .assign(
            unit_id_pudl=lambda x: x.max_unit_id_pudl + 1,
            bga_source=lambda x: "agg_complex_cc",
        )
        .drop("max_unit_id_pudl", axis="columns")
    )

    out_df = gens_df.loc[~row_mask].append(
        gens_df.loc[row_mask]
        .drop(["unit_id_pudl", "bga_source"], axis="columns")
        .merge(
            unit_ids,
            on="plant_id_eia",
            how="left",
            validate="many_to_one",
        )
    )
    return out_df


def flag_orphan_combined_cycle_gens(gens_df, orphan_code):
    """
    Flag CA/CT generators without corresponding CT/CAs as orphans.

    Args:
        gens_df (pandas.DataFrame): Must contain the columns plant_id_eia,
            generator_id, and prime mover code. The column bga_code will
            be created or assigned in matching rows.
        orphan_code (str): Prime mover code indicating which type of generator
            is being flagged as orphaned. Must be either "CA" or "CT".

    Returns:
        pandas.DataFrame

    Raises:
        ValueError: if orphan_code is not CA or CT.

    """
    if orphan_code == "CA":
        missing_code = "CT"
    elif orphan_code == "CT":
        missing_code = "CA"
    else:
        raise ValueError(
            f"orphan must be either 'CA' or 'CT', but we got {orphan_code}."
        )

    orphan_plant_ids = (
        gens_df[gens_df.unit_id_pudl.isnull()]
        .drop_duplicates(subset=["plant_id_eia", "generator_id", "prime_mover_code"])
        .groupby(["plant_id_eia", "prime_mover_code"])
        .size()
        .unstack(fill_value=0)
        .query(f"{orphan_code} > 0 & {missing_code} == 0")
    ).index
    row_mask = (
        (gens_df.plant_id_eia.isin(orphan_plant_ids))
        & (gens_df.unit_id_pudl.isnull())
        & (gens_df.prime_mover_code == orphan_code)
    )
    out_df = gens_df.loc[~row_mask].append(
        gens_df.loc[row_mask]
        .assign(bga_source=f"orphan_{orphan_code.lower()}")
    )
    return out_df


def assign_prime_fuel_unit_ids(gens_df, prime_mover_code, fuel_type_code_pudl):
    """
    Assign a PUDL Unit ID to all generators with a given prime mover and fuel.

    This only assigns a PUDL Unit ID to generators that don't already have one,
    and only to generators that have a consistent `fuel_type_code_pudl` across
    all of the years of data in `gens_df`. This is a simplified fuel code that
    only looks at primary fuels like "coal", "oil", or "gas". All generators
    within a plant that share the same `prime_mover_code` and
    `fuel_type_code_pudl` across all years of data will be assigned a
    `unit_id_pudl` value that is distinct from all pre-existing unit IDs.

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
    single_fuel = (
        gens_df.groupby(["plant_id_eia", "generator_id"])["fuel_type_code_pudl"]
        .transform(lambda x: x.nunique())
    ) == 1

    # This mask defines the generators generators we are going to alter:
    row_mask = (
        (gens_df.prime_mover_code == prime_mover_code)
        & (gens_df.unit_id_pudl.isna())
        & (gens_df.fuel_type_code_pudl == fuel_type_code_pudl)
        & (single_fuel)
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
    out_df = gens_df.loc[~row_mask].append(
        gens_df.loc[row_mask]
        .drop(["unit_id_pudl", "bga_source"], axis="columns")
        .merge(
            unit_ids,
            on=["plant_id_eia", "generator_id"],
            how="left",
            validate="many_to_one",
        )
    )

    return out_df
