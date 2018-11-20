"""Functions for pulling data primarily from the EIA's Form 860."""

import sqlalchemy as sa
import pandas as pd

import pudl
import pudl.constants as pc
import pudl.models.entities

# Shorthand for easier table referecnes:
pt = pudl.models.entities.PUDLBase.metadata.tables


def utilities_eia860(start_date=None, end_date=None, testing=False):
    """Pull all fields from the EIA860 Utilities table."""
    pudl_engine = pudl.init.connect_db(testing=testing)
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

    utils_eia_tbl = pt['utilities_eia']
    utils_eia_select = sa.sql.select([
        utils_eia_tbl.c.utility_id_eia,
        utils_eia_tbl.c.utility_id_pudl,
    ])
    utils_eia_df = pd.read_sql(utils_eia_select, pudl_engine)

    out_df = pd.merge(utils_eia860_df, utils_eia_df,
                      how='left', on=['utility_id_eia', ])

    out_df = out_df.drop(['id'], axis=1)
    first_cols = [
        'report_date',
        'utility_id_eia',
        'utility_id_pudl',
        'utility_name',
    ]

    out_df = pudl.helpers.organize_cols(out_df, first_cols)
    out_df = pudl.helpers.extend_annual(
        out_df, start_date=start_date, end_date=end_date)
    return out_df


def plants_eia860(start_date=None, end_date=None, testing=False):
    """Pull all fields from the EIA860 Plants table."""
    pudl_engine = pudl.init.connect_db(testing=testing)
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
    plants_eia860_df = pd.read_sql(plants_eia860_select, pudl_engine)

    plants_eia_tbl = pt['plants_eia']
    plants_eia_select = sa.sql.select([
        plants_eia_tbl.c.plant_id_eia,
        plants_eia_tbl.c.plant_id_pudl,
    ])
    plants_eia_df = pd.read_sql(plants_eia_select, pudl_engine)

    out_df = pd.merge(plants_eia860_df, plants_eia_df,
                      how='left', on=['plant_id_eia', ])

    utils_eia_tbl = pt['utilities_eia']
    utils_eia_select = sa.sql.select([
        utils_eia_tbl.c.utility_id_eia,
        utils_eia_tbl.c.utility_id_pudl,
    ])
    utils_eia_df = pd.read_sql(utils_eia_select, pudl_engine)

    out_df = pd.merge(out_df, utils_eia_df,
                      how='left', on=['utility_id_eia', ])

    out_df = out_df.drop(['id'], axis=1)
    first_cols = [
        'report_date',
        'utility_id_eia',
        'utility_id_pudl',
        'utility_name',
        'plant_id_eia',
        'plant_id_pudl',
        'plant_name',
    ]

    out_df = pudl.helpers.organize_cols(out_df, first_cols)
    out_df = pudl.helpers.extend_annual(out_df,
                                        start_date=start_date,
                                        end_date=end_date)
    return out_df


def plants_utils_eia860(start_date=None, end_date=None, testing=False):
    """
    Create a dataframe of plant and utility IDs and names from EIA.

    Returns a pandas dataframe with the following columns:
    - report_date (in which data was reported)
    - plant_name (from EIA860)
    - plant_id_eia (from EIA860)
    - plant_id_pudl
    - utility_id_eia (from EIA860)
    - utility_name (from EIA860)
    - utility_id_pudl

    Note: EIA 860 data has only been integrated for 2011-2016. If earlier or
          later years are requested, they will be filled in with data from the
          first or last years.
    """
    # Contains the one-to-one mapping of EIA plants to their operators, but
    # we only have the 860 data integrated for 2011 forward right now.
    plants_eia = plants_eia860(start_date=start_date,
                               end_date=end_date,
                               testing=testing)
    utils_eia = utilities_eia860(start_date=start_date,
                                 end_date=end_date,
                                 testing=testing)
    # to avoid duplicate columns on the merge...
    plants_eia = plants_eia.drop(['utility_id_pudl', 'utility_name'], axis=1)
    out_df = pd.merge(plants_eia, utils_eia,
                      how='left', on=['report_date', 'utility_id_eia'])

    cols_to_keep = ['report_date',
                    'plant_id_eia',
                    'plant_name',
                    'plant_id_pudl',
                    'utility_id_eia',
                    'utility_name',
                    'utility_id_pudl']

    out_df = out_df[cols_to_keep]
    out_df = out_df.dropna()
    out_df.plant_id_pudl = out_df.plant_id_pudl.astype(int)
    out_df.utility_id_pudl = out_df.utility_id_pudl.astype(int)
    return out_df


def generators_eia860(start_date=None, end_date=None, testing=False):
    """
    Pull all fields reported in the generators_eia860 table.

    Merge in other useful fields including the latitude & longitude of the
    plant that the generators are part of, canonical plant & operator names and
    the PUDL IDs of the plant and operator, for merging with other PUDL data
    sources.

    Fill in data for adjacent years if requested, but never fill in earlier
    than the earliest working year of data for EIA923, and never add more than
    one year on after the reported data (since there should at most be a one
    year lag between EIA923 and EIA860 reporting)

    Args:
        start_date (date): the earliest EIA 860 data to retrieve or synthesize
        end_date (date): the latest EIA 860 data to retrieve or synthesize
        testing (bool): Connect to the live PUDL DB or the testing DB?

    Returns:
        A pandas dataframe.

    """
    pudl_engine = pudl.init.connect_db(testing=testing)
    # Almost all the info we need will come from here.
    gens_eia860_tbl = pt['generators_eia860']
    gens_eia860_select = sa.sql.select([gens_eia860_tbl, ])
    # To get the Lat/Lon coordinates
    plants_eia860_tbl = pt['plants_eia860']
    plants_eia860_select = sa.sql.select([
        plants_eia860_tbl.c.report_date,
        plants_eia860_tbl.c.plant_id_eia,
        plants_eia860_tbl.c.latitude,
        plants_eia860_tbl.c.longitude,
        plants_eia860_tbl.c.balancing_authority_code,
        plants_eia860_tbl.c.balancing_authority_name,
        plants_eia860_tbl.c.iso_rto_name,
        plants_eia860_tbl.c.iso_rto_code,
    ])

    if start_date is not None:
        start_date = pd.to_datetime(start_date)
        # We don't want to get too crazy with the date extensions...
        # start_date shouldn't go back before the earliest working year of
        # EIA 923
        eia923_start_date = \
            pd.to_datetime('{}-01-01'.format(
                min(pc.working_years['eia923'])))
        assert start_date >= eia923_start_date
        gens_eia860_select = gens_eia860_select.where(
            gens_eia860_tbl.c.report_date >= start_date
        )
        plants_eia860_select = plants_eia860_select.where(
            plants_eia860_tbl.c.report_date >= start_date
        )

    if end_date is not None:
        end_date = pd.to_datetime(end_date)
        # end_date shouldn't be more than one year ahead of the most recent
        # year for which we have EIA 860 data:
        eia860_end_date = \
            pd.to_datetime('{}-12-31'.format(
                max(pc.working_years['eia860'])))
        assert end_date <= eia860_end_date + pd.DateOffset(years=1)
        gens_eia860_select = gens_eia860_select.where(
            gens_eia860_tbl.c.report_date <= end_date
        )
        plants_eia860_select = plants_eia860_select.where(
            plants_eia860_tbl.c.report_date <= end_date
        )

    gens_eia860 = pd.read_sql(gens_eia860_select, pudl_engine)
    # Canonical sources for these fields are elsewhere. We will merge them in.
    gens_eia860 = gens_eia860.drop(['utility_id_eia',
                                    'utility_name',
                                    'plant_name'], axis=1)
    plants_eia860_df = pd.read_sql(plants_eia860_select, pudl_engine)
    out_df = pd.merge(gens_eia860, plants_eia860_df,
                      how='left', on=['report_date', 'plant_id_eia'])
    out_df.report_date = pd.to_datetime(out_df.report_date)

    # Bring in some generic plant & utility information:
    pu_eia = plants_utils_eia860(start_date=start_date,
                                 end_date=end_date,
                                 testing=testing)
    out_df = pd.merge(out_df, pu_eia, on=['report_date', 'plant_id_eia'])

    # Drop a few extraneous fields...
    cols_to_drop = ['id', ]
    out_df = out_df.drop(cols_to_drop, axis=1)

    # In order to be able to differentiate betweet single and multi-fuel
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
    out_df = pd.merge(out_df, ft_count, how='left',
                      on=['plant_id_eia', 'report_date'])

    first_cols = [
        'report_date',
        'plant_id_eia',
        'plant_id_pudl',
        'plant_name',
        'utility_id_eia',
        'utility_id_pudl',
        'utility_name',
        'generator_id',
    ]

    # Re-arrange the columns for easier readability:
    out_df = pudl.helpers.organize_cols(out_df, first_cols)
    out_df = pudl.helpers.extend_annual(
        out_df, start_date=start_date, end_date=end_date)
    out_df = out_df.sort_values(['report_date',
                                 'plant_id_eia',
                                 'generator_id'])

    return out_df


def boiler_generator_assn_eia860(start_date=None, end_date=None,
                                 testing=False):
    """Pull all fields from the EIA 860 boiler generator association table."""
    pudl_engine = pudl.init.connect_db(testing=testing)
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
    bga_eia860_df = pd.read_sql(bga_eia860_select, pudl_engine)
    out_df = pudl.helpers.extend_annual(bga_eia860_df,
                                        start_date=start_date, end_date=end_date)
    return out_df


def ownership_eia860(start_date=None, end_date=None, testing=False):
    """
    Pull a useful set of fields related to ownership_eia860 table.

    Args:
    -----
        start_date (date): date of the earliest data to retrieve
        end_date (date): date of the latest data to retrieve
        testing (bool): True if we're connecting to the pudl_test DB, False
            if we're connecting to the live PUDL DB. False by default.
    Returns:
    --------
        out_df (pandas dataframe)

    """
    pudl_engine = pudl.init.connect_db(testing=testing)
    o_eia860_tbl = pt['ownership_eia860']
    o_eia860_select = sa.sql.select([o_eia860_tbl, ])
    o_df = pd.read_sql(o_eia860_select, pudl_engine)

    pu_eia = plants_utils_eia860(start_date=start_date,
                                 end_date=end_date,
                                 testing=testing)
    pu_eia = pu_eia[['plant_id_eia', 'plant_id_pudl', 'utility_id_pudl',
                     'report_date']]

    o_df['report_date'] = pd.to_datetime(o_df.report_date)
    out_df = pd.merge(o_df, pu_eia,
                      how='left', on=['report_date', 'plant_id_eia'])

    out_df = out_df.drop(['id'], axis=1)

    out_df = out_df.dropna(subset=[
        'plant_id_eia',
        'plant_id_pudl',
        'utility_id_eia',
        'utility_id_pudl',
        'generator_id',
        'owner_utility_id_eia',
    ])

    first_cols = [
        'report_date',
        'plant_id_eia',
        'plant_id_pudl',
        'plant_name',
        'utility_id_eia',
        'utility_id_pudl',
        'utility_name',
        'generator_id',
        'owner_utility_id_eia',
        'owner_name',
    ]

    # Re-arrange the columns for easier readability:
    out_df = pudl.helpers.organize_cols(out_df, first_cols)
    out_df['plant_id_pudl'] = out_df.plant_id_pudl.astype(int)
    out_df['utility_id_pudl'] = out_df.utility_id_pudl.astype(int)
    out_df = pudl.helpers.extend_annual(
        out_df, start_date=start_date, end_date=end_date)

    return out_df
