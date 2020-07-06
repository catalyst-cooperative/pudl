"""Functions for pulling data primarily from the EIA's Form 860."""

# import datetime
import pandas as pd
import sqlalchemy as sa

import pudl


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
        .astype({"utility_id_pudl": "Int64"})
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
    utils_eia_select = sa.sql.select([
        utils_eia_tbl.c.utility_id_eia,
        utils_eia_tbl.c.utility_id_pudl,
    ])
    utils_eia_df = pd.read_sql(utils_eia_select, pudl_engine)

    out_df = (
        pd.merge(out_df, utils_eia_df, how='left', on=['utility_id_eia', ])
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
               'zip_code', 'street_address'], axis='columns')
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
    # pudl_settings = pudl.workspace.setup.get_defaults()
    # pudl_engine = sa.create_engine(pudl_settings["pudl_db"])

    pt = pudl.output.pudltabl.get_table_meta(pudl_engine)
    # Almost all the info we need will come from here.
    gens_eia860_tbl = pt['generators_eia860']
    gens_eia860_select = sa.sql.select([gens_eia860_tbl, ])
    # To get plant age
    generators_entity_eia_tbl = pt['generators_entity_eia']
    generators_entity_eia_select = sa.sql.select([
        generators_entity_eia_tbl.c.plant_id_eia,
        generators_entity_eia_tbl.c.generator_id,
        generators_entity_eia_tbl.c.operating_date,
        # generators_entity_eia_tbl.c.report_date
    ])
    # To get the Lat/Lon coordinates
    plants_entity_eia_tbl = pt['plants_entity_eia']
    plants_entity_eia_select = sa.sql.select([
        plants_entity_eia_tbl.c.plant_id_eia,
        plants_entity_eia_tbl.c.plant_name_eia,
        plants_entity_eia_tbl.c.latitude,
        plants_entity_eia_tbl.c.longitude,
        plants_entity_eia_tbl.c.state,
        plants_entity_eia_tbl.c.balancing_authority_code_eia,
        plants_entity_eia_tbl.c.balancing_authority_name_eia,
        plants_entity_eia_tbl.c.iso_rto_code,
        plants_entity_eia_tbl.c.city,
        plants_entity_eia_tbl.c.county,
        plants_entity_eia_tbl.c.nerc_region,
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

    # breakpoint()
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
    # ,'plant_name_eia', 'utility_id_eia'])

    # Drop a few extraneous fields...
    out_df = out_df.drop(['id'], axis='columns')

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
