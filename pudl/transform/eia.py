"""Routines specific to cleaning up EIA Form 923 data."""

import logging
import numpy as np
import networkx as nx
import pandas as pd
import pudl
import pudl.constants as pc

logger = logging.getLogger(__name__)


def _occurrence_consistency(entity_id, compiled_df, col,
                            cols_to_consit, strictness=.7):
    """
    Find the occurence of plants & the consistency of records

    We need to determine how consistent a reported value is in the records
    across all of the years or tables that the value is being reported, so we
    want to compile two key

    Args:
        entity_id (int): the earliest EIA 860 data to retrieve or synthesize
        compiled_df (dataframe): the latest EIA 860 data to retrieve or synthesize
        col (string): Connect to the live PUDL DB or the testing DB?
        cols_to_consit

    Returns:
        A pandas dataframe.
    """
    # select only the colums you want and drop the NaNs
    col_df = compiled_df[entity_id + ['report_date',
                                      col, 'table']].copy().dropna()
    if len(col_df) == 0:
        col_df[f'{col}_consistent'] = np.NaN
        col_df['occurences'] = np.NaN
        col_df = col_df.drop(columns=['table'])
        return col_df
    # determine how many times each plant occurs
    occur = (
        col_df.
        groupby(by=cols_to_consit).
        agg({'table': "count"}).
        reset_index().
        rename(columns={'table': 'occurences'})
    )

    col_df = col_df.merge(occur, on=cols_to_consit)

    consist_df = (
        col_df.
        groupby(by=cols_to_consit + [col]).
        agg({'table': 'count'}).
        reset_index().
        rename(columns={'table': 'consistency'})
        #rename(columns={'table': f'{col}_consistent'})
    )

    col_df = col_df.merge(consist_df, how='outer').drop(columns=['table'])
    # change all of the fully consistent records to True
    col_df[f'{col}_consistent'] = (col_df['consistency'] /
                                   col_df['occurences'] > strictness)

    # col_df.loc[:, f'{col}_consistent'] = (col_df[f'{col}_consistent'] /
    #                                      col_df['occurences'] > strictness)
    return col_df


def _lat_long(dirty_df, clean_df, entity_id_df, entity_id,
              col, cols_to_consit, round_to=2):
    """Special case haveresting for lat/long """
    # grab the dirty plant records, round and get a new consistency
    ll_df = dirty_df.round(decimals=round_to)
    ll_df['table'] = 'special_case'
    ll_df = _occurrence_consistency(entity_id, ll_df, col, cols_to_consit)
    # grab the clean plants
    ll_clean_df = clean_df.dropna()
    # find the new clean plant records
    ll_df = ll_df[ll_df[f'{col}_consistent'] ==
                  True].drop_duplicates(subset=entity_id)
    # add the newly cleaned records
    ll_clean_df = ll_clean_df.append(ll_df,)
    # merge onto the plants df w/ all plant ids
    ll_clean_df = entity_id_df.merge(ll_clean_df, how='outer')
    return ll_clean_df


def _add_timezone(plants_entity):
    """Add plants' IANA timezones from lat/lon
    param: plants_entity Plant entity table, including columns named "latitude",
        "longitude", and optionally "state"
    Returns the same table, with a "timezone" column added. Timezone may be
    missing if lat/lon is missing or invalid.
    """
    plants_entity["timezone"] = plants_entity.apply(
        lambda row: pudl.helpers.find_timezone(
            lng=row["longitude"], lat=row["latitude"],
            state=row["state"], strict=False
        ),
        axis=1,
    )
    return plants_entity


def _add_additional_epacems_plants(plants_entity):
    """Add the info for plants that have IDs in the CEMS data but not EIA data

    param: plants_entity Plant entity table that will be appended to
    returns: The same plants_entity table, with the addition of some missing EPA
    CEMS plants
    Note that a side effect will be resetting the index on plants_entity, if one
    exists. If that's a problem, modify the code below.

    The columns loaded are plant_id_eia, plant_name, state, latitude, and longitude

    Note that some of these plants disappear from the CEMS before the earliest
    EIA data PUDL processes, so if PUDL eventually ingests older data, these
    may be redundant.
    The set of additional plants is every plant that appears in the hourly CEMS
    data (1995-2017) that never appears in the EIA 923 or 860 data (2009-2017
    for EIA 923, 2011-2017 for EIA 860).
    """
    # Add the plant IDs that are missing and update the values for the others
    # The data we're reading is a CSV in pudl/metadata/
    # SQL would call this whole process an upsert
    # See also: https://github.com/pandas-dev/pandas/issues/22812
    cems_df = pd.read_csv(
        pc.epacems_additional_plant_info_file,
        index_col=["plant_id_eia"],
        usecols=["plant_id_eia", "plant_name",
                 "state", "latitude", "longitude"],
    )
    plants_entity = plants_entity.set_index("plant_id_eia")
    cems_unmatched = cems_df.loc[~cems_df.index.isin(plants_entity.index)]
    # update will replace columns and index values that add rows or affect
    # non-matching columns. It also requires an index, so we set and reset the
    # index as necessary. Also, it only works in-place, so we can't chain.
    plants_entity.update(cems_df, overwrite=True)
    return plants_entity.append(cems_unmatched).reset_index()


def _harvesting(entity,
                eia_transformed_dfs,
                entities_dfs,
                debug=False):
    """
    Compiling entities.

    For each entity (plants, generators, boilers, utilties), this function
    goes and finds all the harvestable columns from any table that they show up
    in. It then determines how consistent the records are and keeps the values
    that are mostly consistent. Then we compile those consistent records intro
    one normalized table.

    Args:
        entity (str) : plants, generators, boilers, utilties
        eia_transformed_dfs (dict) : dictionary of tbl names (keys) and
            transformed dfs (values)
        debug (bool)
    Returns:
        eia_transformed_dfs (dict)
        entity_dfs (dict): dictionary of entity table names (keys) and entiy
            dfs (values)
    """
    # we know these columns must be in the dfs
    entity_id = pc.entities[entity][0]
    base_cols = pc.entities[entity][0] + ['report_date']
    static_cols = pc.entities[entity][1]
    annual_cols = pc.entities[entity][2]

    logger.debug("    compiling plants for entity tables from:")
    # empty list for dfs to be added to for each table below
    dfs = []
    # for each df in the dtc of transformed dfs
    for table_name, transformed_df in eia_transformed_dfs.items():
        # inside of main() we are going to be adding items into
        # eia_transformed_dfs with the name 'annaul'. We don't want to harvert
        # from our newly harvested tables.
        if 'annual' not in table_name:
            # if the if contains the desired columns the grab those columns
            if set(base_cols).issubset(transformed_df.columns):
                logger.debug(f"        {table_name}...")
                # create a copy of the df to muck with
                df = transformed_df.copy()
                # we know these columns must be in the dfs
                cols = []
                # check whether the columns are in the specific table
                for column in static_cols + annual_cols:
                    if column in df.columns:
                        cols.append(column)
                df = df[(base_cols + cols)]
                # add a column with the table name so we know its origin
                df['table'] = table_name
                dfs.append(df)

                # remove the static columns, with an exception
                if entity == 'plants' and table_name in ('ownership_eia860',
                                                         'utilities_eia860'):
                    cols.remove('utility_id_eia')
                transformed_df = transformed_df.drop(columns=cols)
                eia_transformed_dfs[table_name] = transformed_df

    # add those records to the compliation
    compiled_df = pd.concat(dfs, axis=0, ignore_index=True, sort=True)
    # strip the month and day from the date so we can have annual records
    compiled_df['report_date'] = compiled_df['report_date'].dt.year
    # convert the year back into a date_time object
    year = compiled_df['report_date']
    compiled_df['report_date'] = \
        pd.to_datetime({'year': year,
                        'month': 1,
                        'day': 1})

    logger.debug('    Casting harvested IDs to correct data types')
    # most columns become objects (ack!), so assign types
    compiled_df = \
        compiled_df.astype(pc.entities[entity][3])

    # compile annual ids
    annual_id_df = compiled_df[
        ['report_date'] + entity_id].copy().drop_duplicates()
    annual_id_df.sort_values(['report_date'] + entity_id,
                             inplace=True, ascending=False)

    # create the annual and entity dfs
    entity_id_df = annual_id_df.drop(
        ['report_date'], axis=1).drop_duplicates(subset=entity_id)

    entity_df = entity_id_df.copy()
    annual_df = annual_id_df.copy()
    special_case_cols = {'latitude': [_lat_long, 1],
                         'longitude': [_lat_long, 1]}
    consistency = pd.DataFrame(columns=['column', 'consistent_ratio',
                                        'wrongos', 'total'])
    col_dfs = {}
    # determine how many times each of the columns occur
    for col in static_cols + annual_cols:
        if col in annual_cols:
            cols_to_consit = entity_id + ['report_date']
        if col in static_cols:
            cols_to_consit = entity_id

        col_df = _occurrence_consistency(
            entity_id, compiled_df, col, cols_to_consit, strictness=.7)

        # pull the correct values out of the df and merge w/ the plant ids
        col_correct_df = (
            col_df[col_df[f'{col}_consistent']].
            drop_duplicates(subset=(cols_to_consit + [f'{col}_consistent']))
        )

        # we need this to be an empty df w/ columns bc we are going to use it
        if col_correct_df.empty:
            col_correct_df = pd.DataFrame(columns=col_df.columns)

        if col in static_cols:
            clean_df = entity_id_df.merge(
                col_correct_df, on=entity_id, how='left')
            clean_df = clean_df[entity_id + [col]]
            entity_df = entity_df.merge(clean_df, on=entity_id)

        if col in annual_cols:
            clean_df = annual_id_df.merge(
                col_correct_df, on=(entity_id + ['report_date']), how='left')
            clean_df = clean_df[entity_id + ['report_date', col]]
            annual_df = annual_df.merge(
                clean_df, on=(entity_id + ['report_date']))

        # get the still dirty records by using the cleaned ids w/null values
        # we need the plants that have no 'correct' value so
        # we can't just use the col_df records when the consistency is not True
        dirty_df = col_df.merge(
            clean_df[clean_df[col].isnull()][entity_id])

        if col in special_case_cols.keys():
            clean_df = special_case_cols[col][0](
                dirty_df, clean_df, entity_id_df, entity_id, col,
                cols_to_consit, special_case_cols[col][1])

        if debug:
            col_dfs[col] = col_df
        # this next section is used to print and test whether the harvested
        # records are consistent enough
        total = len(col_df.drop_duplicates(subset=cols_to_consit))
        # if the total is 0, the ratio will error, so assign null values.
        if total == 0:
            ratio = np.NaN
            wrongos = np.NaN
            logger.debug(f"       Zero records found for {col}")
        if total > 0:
            ratio = (
                len(col_df[(col_df[f'{col}_consistent'])].
                    drop_duplicates(subset=cols_to_consit)) / total
            )
            wrongos = (1 - ratio) * total
            logger.debug(
                f"       Ratio: {ratio:.3}  Wrongos: {wrongos:.5}  Total: {total}   {col}")
            # the following assertions are here to ensure that the harvesting
            # process is producing enough consistent records. When every year
            # is being imported the lowest consistency ratio should be .97,
            # with the exception of the latitude and longitude, which has a
            # ratio of ~.94. The ratios are better with less years imported.
            if col in ('latitude', 'longitude'):
                if ratio < .92:
                    raise AssertionError(
                        f'Harvesting of {col} is too inconsistent.')
            elif ratio < .95:
                raise AssertionError(
                    f'Harvesting of {col} is too inconsistent at {ratio:.3}.')
        # add to a small df to be used in order to print out the ratio of
        # consistent records
        consistency = consistency.append({'column': col,
                                          'consistent_ratio': ratio,
                                          'wrongos': wrongos,
                                          'total': total}, ignore_index=True)
    mcs = consistency['consistent_ratio'].mean()
    logger.info(
        f"Average consistency of static {entity} values is {mcs:.2%}")

    if entity == "plants":
        entity_df = _add_additional_epacems_plants(entity_df)
        entity_df = _add_timezone(entity_df)

    eia_transformed_dfs[f'{entity}_annual_eia'] = annual_df
    entities_dfs[f'{entity}_entity_eia'] = entity_df
    if debug:
        return entities_dfs, eia_transformed_dfs, col_dfs
    return entities_dfs, eia_transformed_dfs


def _boiler_generator_assn(eia_transformed_dfs,
                           eia923_years=pc.working_years['eia923'],
                           eia860_years=pc.working_years['eia860'],
                           debug=False):
    """
    Create more complete boiler generator associations.

    Creates a unique unit_id_pudl for each collection of boilers and generators
    within a plant that have ever been associated with each other, based on
    the boiler generator associations reported in EIA860. Unfortunately, this
    information is not complete for years before 2014, as the gas turbine
    portion of combined cycle power plants in those earlier years were not
    reporting their fuel consumption, or existence as part of the plants.

    For years 2014 and on, EIA860 contains a unit_id_eia value, allowing the
    combined cycle plant compoents to be associated with each other. For many
    plants not listed in the reported boiler generator associations, it is
    nonetheless possible to associate boilers and generators on a one-to-one
    basis, as they use identical strings to describe the units.

    In the end, between the reported BGA table, the string matching, and the
    unit_id_eia values, it's possible to create a nearly complete mapping of
    the generation units, at least for 2014 and later.

    Args:
    -----
        eia_transformed_dfs (dict): a dictionary of post-transform dataframes
            representing the EIA database tables.
        eia923_years (list-like): a list of the years of EIA 923 data that
            should be used to infer the boiler-generator associations. By
            default it is all the working years of data.
        eia860_years (list-like): a list of the years of EIA 860 data that
            should be used to infer the boiler-generator associations. By
            default it is all the working years of data.
        debug (bool): If True, include columns in the returned dataframe
            indicating by what method the individual boiler generator
            associations were inferred.

    Returns:
    --------
        eia_transformed_dfs (dict): Returns the same dictionary of dataframes
            that was passed in, and adds a new dataframe to it representing
            the boiler-generator associations as records containing
            plant_id_eia, generator_id, boiler_id, and unit_id_pudl

    """
    # if you're not ingesting both 860 and 923, the bga is not compilable
    if not (eia860_years and eia923_years):
        return
    # compile and scrub all the parts
    logger.info("Inferring complete EIA boiler-generator associations.")
    bga_eia860 = eia_transformed_dfs['boiler_generator_assn_eia860'].copy()
    bga_eia860 = _restrict_years(bga_eia860, eia923_years, eia860_years)
    bga_eia860['generator_id'] = bga_eia860.generator_id.astype(str)
    bga_eia860['boiler_id'] = bga_eia860.boiler_id.astype(str)
    # bga_eia860 = bga_eia860.drop(['utility_id_eia'], axis=1)

    gen_eia923 = eia_transformed_dfs['generation_eia923'].copy()
    gen_eia923 = _restrict_years(gen_eia923, eia923_years, eia860_years)
    gen_eia923['generator_id'] = gen_eia923.generator_id.astype(str)
    gen_eia923 = gen_eia923.set_index(pd.DatetimeIndex(gen_eia923.report_date))

    gen_eia923_gb = gen_eia923.groupby(
        [pd.Grouper(freq='AS'), 'plant_id_eia', 'generator_id'])
    gen_eia923 = gen_eia923_gb['net_generation_mwh'].sum().reset_index()
    gen_eia923['missing_from_923'] = False

    # compile all of the generators
    gens_eia860 = eia_transformed_dfs['generators_eia860'].copy()
    gens_eia860 = _restrict_years(gens_eia860, eia923_years, eia860_years)
    gens_eia860['generator_id'] = gens_eia860.generator_id.astype(str)
    gens = pd.merge(gen_eia923, gens_eia860,
                    on=['plant_id_eia', 'report_date', 'generator_id'],
                    how='outer')

    gens = gens[['plant_id_eia',
                 'report_date',
                 'generator_id',
                 'unit_id_eia',
                 'net_generation_mwh',
                 'missing_from_923']].drop_duplicates()

    gens['generator_id'] = gens['generator_id'].astype(str)

    # create the beginning of a bga compilation w/ the generators as the
    # background
    bga_compiled_1 = pd.merge(gens, bga_eia860,
                              on=['plant_id_eia', 'generator_id',
                                  'report_date'],
                              how='outer')

    # Create a set of bga's that are linked, directly from bga8
    bga_assn = bga_compiled_1[bga_compiled_1['boiler_id'].notnull()].copy()
    bga_assn['bga_source'] = 'eia860_org'

    # Create a set of bga's that were not linked directly through bga8
    bga_unassn = bga_compiled_1[bga_compiled_1['boiler_id'].isnull()].copy()
    bga_unassn = bga_unassn.drop(['boiler_id'], axis=1)

    # Side note: there are only 6 generators that appear in bga8 that don't
    # apear in gens9 or gens8 (must uncomment-out the og_tag creation above)
    # bga_compiled_1[bga_compiled_1['og_tag'].isnull()]

    bf_eia923 = eia_transformed_dfs['boiler_fuel_eia923'].copy()
    bf_eia923 = _restrict_years(bf_eia923, eia923_years, eia860_years)
    bf_eia923['boiler_id'] = bf_eia923.boiler_id.astype(str)
    bf_eia923['total_heat_content_mmbtu'] = bf_eia923['fuel_consumed_units'] * \
        bf_eia923['fuel_mmbtu_per_unit']
    bf_eia923 = bf_eia923.set_index(pd.DatetimeIndex(bf_eia923.report_date))
    bf_eia923_gb = bf_eia923.groupby(
        [pd.Grouper(freq='AS'), 'plant_id_eia', 'boiler_id'])
    bf_eia923 = bf_eia923_gb.agg({
        'total_heat_content_mmbtu': pudl.helpers.sum_na,
    }).reset_index()

    bf_eia923.drop_duplicates(
        subset=['plant_id_eia', 'report_date', 'boiler_id'], inplace=True)

    # Create a list of boilers that were not in bga8
    bf9_bga = bf_eia923.merge(bga_compiled_1,
                              on=['plant_id_eia', 'boiler_id', 'report_date'],
                              how='outer',
                              indicator=True)
    bf9_not_in_bga = bf9_bga[bf9_bga['_merge'] == 'left_only']
    bf9_not_in_bga = bf9_not_in_bga.drop(['_merge'], axis=1)

    # Match the unassociated generators with unassociated boilers
    # This method is assuming that some the strings of the generators and the
    # boilers are the same
    bga_unassn = bga_unassn.merge(bf9_not_in_bga[['plant_id_eia',
                                                  'boiler_id',
                                                  'report_date']],
                                  how='left',
                                  left_on=['report_date',
                                           'plant_id_eia',
                                           'generator_id'],
                                  right_on=['report_date',
                                            'plant_id_eia',
                                            'boiler_id'])
    bga_unassn.sort_values(['report_date', 'plant_id_eia'], inplace=True)
    bga_unassn['bga_source'] = None
    bga_unassn.loc[bga_unassn.boiler_id.notnull(),
                   'bga_source'] = 'string_assn'

    bga_compiled_2 = bga_assn.append(bga_unassn)
    bga_compiled_2.sort_values(['plant_id_eia', 'report_date'], inplace=True)
    bga_compiled_2['missing_from_923'].fillna(value=True, inplace=True)

    # Connect the gens and boilers in units
    bga_compiled_units = bga_compiled_2.loc[
        bga_compiled_2['unit_id_eia'].notnull()]
    bga_gen_units = bga_compiled_units.drop(['boiler_id'], axis=1)
    bga_boil_units = bga_compiled_units[['plant_id_eia',
                                         'report_date',
                                         'boiler_id',
                                         'unit_id_eia']].copy()
    bga_boil_units.dropna(subset=['boiler_id'], inplace=True)

    # merge the units with the boilers
    bga_unit_compilation = bga_gen_units.merge(bga_boil_units,
                                               how='outer',
                                               on=['plant_id_eia',
                                                   'report_date',
                                                   'unit_id_eia'],
                                               indicator=True)
    # label the bga_source
    bga_unit_compilation. \
        loc[bga_unit_compilation['bga_source'].isnull(),
            'bga_source'] = 'unit_connection'
    bga_unit_compilation.drop(['_merge'], axis=1, inplace=True)
    bga_non_units = bga_compiled_2[bga_compiled_2['unit_id_eia'].isnull()]

    # combine the unit compilation and the non units
    bga_compiled_3 = bga_non_units.append(bga_unit_compilation)

    # resort the records and the columns
    bga_compiled_3.sort_values(['plant_id_eia', 'report_date'], inplace=True)
    bga_compiled_3 = bga_compiled_3[['plant_id_eia',
                                     'report_date',
                                     'generator_id',
                                     'boiler_id',
                                     'unit_id_eia',
                                     'bga_source',
                                     'net_generation_mwh',
                                     'missing_from_923']]

    # label plants that have 'bad' generator records (generators that have MWhs
    # in gens9 but don't have connected boilers) create a df with just the bad
    # plants by searching for the 'bad' generators
    bad_plants = bga_compiled_3[(bga_compiled_3['boiler_id'].isnull()) &
                                (bga_compiled_3['net_generation_mwh'] > 0)].\
        drop_duplicates(subset=['plant_id_eia', 'report_date'])
    bad_plants = bad_plants[['plant_id_eia', 'report_date']]

    # merge the 'bad' plants back into the larger frame
    bga_compiled_3 = bga_compiled_3.merge(bad_plants,
                                          how='outer',
                                          on=['plant_id_eia', 'report_date'],
                                          indicator=True)

    # use the indicator to create labels
    bga_compiled_3['plant_w_bad_generator'] = \
        np.where(bga_compiled_3._merge == 'both', True, False)
    # Note: At least one gen has reported MWh in 923, but could not be
    # programmatically mapped to a boiler

    # we don't need this one anymore
    bga_compiled_3 = bga_compiled_3.drop(['_merge'], axis=1)

    # create a label for generators that are unmapped but in 923
    bga_compiled_3['unmapped_but_in_923'] = \
        np.where((bga_compiled_3.boiler_id.isnull()) &
                 ~bga_compiled_3.missing_from_923 &
                 (bga_compiled_3.net_generation_mwh == 0),
                 True,
                 False)

    # create a label for generators that are unmapped
    bga_compiled_3['unmapped'] = np.where(bga_compiled_3.boiler_id.isnull(),
                                          True,
                                          False)
    bga_out = bga_compiled_3.drop('net_generation_mwh', axis=1)
    bga_out.loc[bga_out.unit_id_eia.isnull(), 'unit_id_eia'] = None

    bga_for_nx = bga_out[['plant_id_eia', 'report_date', 'generator_id',
                          'boiler_id', 'unit_id_eia']]
    # If there's no boiler... there's no boiler-generator association
    bga_for_nx = bga_for_nx.dropna(subset=['boiler_id']).drop_duplicates()

    # Need boiler & generator specific ID strings, or they look like
    # the same node to NX
    bga_for_nx['generators'] = 'p' + bga_for_nx.plant_id_eia.astype(str) + \
                               '_g' + bga_for_nx.generator_id.astype(str)
    bga_for_nx['boilers'] = 'p' + bga_for_nx.plant_id_eia.astype(str) + \
                            '_b' + bga_for_nx.boiler_id.astype(str)

    # dataframe to accumulate the unit_ids in
    bga_w_units = pd.DataFrame()
    # We want to start our unit_id counter anew for each plant:
    for pid in bga_for_nx.plant_id_eia.unique():
        bga_byplant = bga_for_nx[bga_for_nx.plant_id_eia == pid].copy()

        # Create a graph from the dataframe of boilers and generators. It's a
        # multi-graph, meaning the same nodes can be connected by more than one
        # edge -- this allows us to preserve multiple years worth of boiler
        # generator association information for later inspection if need be:
        bga_graph = nx.from_pandas_edgelist(bga_byplant,
                                            source='generators',
                                            target='boilers',
                                            edge_attr=True,
                                            create_using=nx.MultiGraph())

        # Each connected sub-graph is a generation unit:
        gen_units = [bga_graph.subgraph(c).copy()
                     for c in nx.connected_components(bga_graph)]

        # Assign a unit_id to each subgraph, and extract edges into a dataframe
        for unit_id, unit in zip(range(len(gen_units)), gen_units):
            # All the boiler-generator association graphs should be bi-partite,
            # meaning generators only connect to boilers, and boilers only
            # connect to generators.
            if not nx.algorithms.bipartite.is_bipartite(unit):
                raise AssertionError(
                    f"Non-bipartite generation unit graph found."
                    f"plant_id_eia={pid}, unit_id_pudl={unit_id}."
                )
            nx.set_edge_attributes(
                unit, name='unit_id_pudl', values=unit_id + 1)
            new_unit_df = nx.to_pandas_edgelist(unit)
            bga_w_units = bga_w_units.append(new_unit_df)

    bga_w_units = bga_w_units.sort_values(['plant_id_eia', 'unit_id_pudl',
                                           'generator_id', 'boiler_id'])
    bga_w_units = bga_w_units.drop(['source', 'target'], axis=1)

    # Check whether the PUDL unit_id values we've inferred conflict with
    # the unit_id_eia values that were reported to EIA. Are there any PUDL
    # unit_id values that have more than 1 EIA unit_id_eia within them?
    bga_unit_id_eia_counts = \
        bga_w_units.groupby(['plant_id_eia', 'unit_id_pudl'])['unit_id_eia'].\
        nunique().to_frame().reset_index()
    bga_unit_id_eia_counts = bga_unit_id_eia_counts.rename(
        columns={'unit_id_eia': 'unit_id_eia_count'})
    bga_unit_id_eia_counts = pd.merge(bga_w_units, bga_unit_id_eia_counts,
                                      on=['plant_id_eia', 'unit_id_pudl'])
    too_many_codes = \
        bga_unit_id_eia_counts[bga_unit_id_eia_counts.unit_id_eia_count > 1]
    too_many_codes = \
        too_many_codes[~too_many_codes.unit_id_eia.isnull()].\
        groupby(['plant_id_eia', 'unit_id_pudl'])['unit_id_eia'].unique()
    for row in too_many_codes.iteritems():
        logger.warning(f"Multiple EIA unit codes:"
                       f"plant_id_eia={row[0][0]}, "
                       f"unit_id_pudl={row[0][1]}, "
                       f"unit_id_eia={row[1]}")
    bga_w_units = bga_w_units.drop('unit_id_eia', axis=1)

    # These assertions test that all boilers and generators ended up in the
    # same unit_id across all the years of reporting:
    pgu_gb = bga_w_units.groupby(
        ['plant_id_eia', 'generator_id'])['unit_id_pudl']
    if not (pgu_gb.nunique() == 1).all():
        raise AssertionError("Inconsistent inter-annual BGA assignment!")
    pbu_gb = bga_w_units.groupby(
        ['plant_id_eia', 'boiler_id'])['unit_id_pudl']
    if not (pbu_gb.nunique() == 1).all():
        raise AssertionError("Inconsistent inter-annual BGA assignment!")

    bga_w_units = bga_w_units.drop('report_date', axis=1)
    bga_w_units = bga_w_units[['plant_id_eia', 'unit_id_pudl',
                               'generator_id', 'boiler_id']].drop_duplicates()
    bga_out = pd.merge(bga_out, bga_w_units, how='left',
                       on=['plant_id_eia', 'generator_id', 'boiler_id'])
    bga_out['unit_id_pudl'] = (
        bga_out['unit_id_pudl'].
        fillna(value=0).
        astype(int)
    )

    if not debug:
        bga_out = bga_out[~bga_out.missing_from_923 &
                          ~bga_out.plant_w_bad_generator &
                          ~bga_out.unmapped_but_in_923 &
                          ~bga_out.unmapped]

        bga_out = bga_out.drop(['missing_from_923',
                                'plant_w_bad_generator',
                                'unmapped_but_in_923',
                                'unmapped'], axis=1)
        bga_out = bga_out.drop_duplicates(subset=['report_date',
                                                  'plant_id_eia',
                                                  'boiler_id',
                                                  'generator_id'])

    eia_transformed_dfs['boiler_generator_assn_eia860'] = bga_out

    return eia_transformed_dfs


def _restrict_years(df,
                    eia923_years=pc.working_years['eia923'],
                    eia860_years=pc.working_years['eia860']):
    """Restrict eia years for boiler generator association."""
    bga_years = set(eia860_years) & set(eia923_years)
    df = df[df.report_date.dt.year.isin(bga_years)]
    return df


def transform(eia_transformed_dfs,
              eia923_years=pc.working_years['eia923'],
              eia860_years=pc.working_years['eia860'],
              debug=False):
    """Create dfs for EIA Entity tables."""
    if not eia923_years and not eia860_years:
        logger.info('Not ingesting EIA')
        return None
    # create the empty entities df to fill up
    entities_dfs = {}

    # for each of the entities, harvest the static and annual columns.
    # the order of the entities matter! the
    for entity in pc.entities.keys():
        logger.info(f"Harvesting IDs & consistently static attributes "
                    f"for EIA {entity}")
        _harvesting(entity, eia_transformed_dfs, entities_dfs,
                    debug=debug)

    _boiler_generator_assn(eia_transformed_dfs,
                           eia923_years=eia923_years,
                           eia860_years=eia860_years,
                           debug=debug)

    # get rid of the original annual dfs in the transformed dict
    remove = ['generators', 'plants', 'utilities']
    for entity in remove:
        eia_transformed_dfs[f'{entity}_eia860'] = \
            eia_transformed_dfs.pop(f'{entity}_annual_eia',
                                    f'{entity}_annual_eia')
    # remove the boilers annual table bc it has no columns
    eia_transformed_dfs.pop('boilers_annual_eia',)

    return entities_dfs, eia_transformed_dfs
