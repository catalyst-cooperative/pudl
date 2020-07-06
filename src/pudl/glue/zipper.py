"""
A module for inferring associations between related data based on correlations.

Often, two data sets contain interesting related information, but do not share
a common key which can be used to connect the data (e.g. the FERC and EIA
datasets related to plant operating expenses). However, in some cases, there
are quantities which are reported in both sets of data (e.g. net electricity
generation and heat content of fuel consumed). The correlations between those
reported data can potentially be used to associate the other independent
information in the two datasets with each other, if the search space for is
sufficiently constrained. This module provides a generalized tool for creating
these associations when there's a shared set of IDs in both data sets that
narrows the number of potential matches down enough that an exhausive attempt
at correlating the series is computationally tractable.

How does this work in the particular case of FERC & EIA data?

We want to extract the plant-level non-fuel operating costs from the FERC
plant tables, especially the plants_steam_ferc1 table. The data we have at the
FERC plant level and also in the EIA data (at either the plant or generator
level) includes:

* Net electricity generated (generator)
* Heat content of fuel consumed, by fuel (oil, gas, coal) (boiler/generator)
* Cost of fuel consumed, by fuel (oil, gas, coal) (EIA plant)

There are a couple of pre-filters we can (and probably need to) apply before we
jump into the brute force combinatorics:

* We can use the PUDL plant IDs to narrow down the search space, by only
  attempting to match FERC plants & EIA generators that have been associated
  with the same PUDL plant.
* Within each PUDL plant ID, we can often categorize individual FERC plants
  as coal or gas plants, based on their fuel consumption (e.g. if more than
  95% of all fuel is of one type). Similarly, individual EIA generators are
  typically clearly either gas or coal fired. There's no point in trying to
  match gas generators to coal plants, or coal generators to gas plants -- any
  combination of generators that does so can be automatically eliminated.

So the inputs into the datazipper are going to be two dataframes, one for
FERC and one for EIA, with the following fields:

FERC:
* respondent_id
* plant_name
* plant_id_pudl
* report_year
* net_generation_mwh
* gas_total_heat_mmbtu
* oil_total_heat_mmbtu
* coal_total_heat_mmbtu
* gas_total_cost
* oil_total_cost
* coal_total_cost

EIA:
* plant_id_eia
* generator_id
* plant_id_pudl
* report_year
* net_generation_mwh (by generator)
* gas_total_heat_mmbtu (by generator via boiler)
* oil_total_heat_mmbtu (by generator via boiler)
* coal_total_heat_mmbtu (by generator via boiler)
* gas_total_cost (by generator via plant)
* oil_total_cost (by generator via plant)
* coal_total_cost (by generator via plant)

The output is a set of IDs that allow the FERC plants & EIA generators tables
to be joined together.

"""
import itertools
# Useful high-level external modules.
import logging
import random

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def partition(collection):
    """
    Generate all possible partitions of a collection of items.

    Recursively generate all the possible groupings of the individual items in
    the collection, such that all items appear, and appear only once.

    We use this funciton to generate different sets of potential plant and
    sub-plant generation infrastructure within the collection of generators
    associated with each plant_id_pudl -- both in the FERC and EIA data, so
    that we can find the association between the two data sets which results
    in the highest correlation between some variables reported in each one.
    Namely: net generation, fuel heat content, and fuel costs.

    Args:
        collection (list of items): the set to partition.
    Returns:
        A list of all valid set partitions.

    """
    if len(collection) == 1:
        yield [collection]
        return

    first = collection[0]
    for smaller in partition(collection[1:]):
        # insert `first` in each of the subpartition's subsets
        for n, subset in enumerate(smaller):
            yield smaller[:n] + [[first] + subset] + smaller[n + 1:]
        # put `first` in its own subset
        yield [[first]] + smaller


def partition_k(collection, k):
    """Generate all partitions of a set having k elements."""
    for part in partition(collection):
        if len(part) == k:
            yield part


def random_chunk(li, min_chunk=1, max_chunk=3):
    """Chunk a list of items into a list of lists containing the same items.

    Takes a list, and creates a generator that returns sub-lists containing
    at least min_chunk items and at most max_chunk items from the original
    list. Used here to create groupings of individual generators that get
    aggregated into either FERC plants or PUDL plants for synthetic data.
    """
    it = iter(li)
    while True:
        nxt = list(itertools.islice(
            # This is not a cryptographic application so random is fiiiine.
            it, random.randint(min_chunk, max_chunk)))  # nosec: B311
        if nxt:
            yield nxt
        else:
            break


def zippertestdata(gens=50, max_group_size=6, samples=10,
                   noise=(0.10, 0.10, 0.10)):
    """Generate a test dataset for the datazipper, with known solutions.

    Args:
        gens (int): number of actual atomic units (analogous to generators)
            which may not fully enumerated in either dataset.
        max_group_size (int): Maximum number of atomic units which should
            be allowed to aggregate in the FERC groups.
        samples (int): How many samples should be available in each shared data
            series?
        noise (array): An array-like collection of numbers indicating the
            amount of noise (dispersion) to add between the two synthetic
            datasets. Larger numbers will result in lower correlations. The
            length of the noise array determines how many data series are
            created in the two synthetic datasets.

    Returns:
        eia_df (pd.DataFrame): Synthetic test data representing the EIA data
            to be used in connecting EIA and FERC plant data. For now it
            assumes that we have annual data at the generator level for all
            of the variables we're using to correlate.
        ferc_df (pd.DataFrame): Synthetic data representing the FERC data to
            be used in connecting the EIA and FERC plant data. Does not have
            individual generator level information. Rather, the dependent
            variables are grouped by ferc_plant_id, each of which is a sum
            of several original generator level data series. The ferc_plant_id
            values indicate which original generators went into creating the
            data series, allowing us to easily check whether they've been
            correctly matched.
    """
    from string import ascii_lowercase, ascii_uppercase

    # Make sure we've got enough plant IDs to work with:
    rpt = 1
    while(len(ascii_lowercase)**rpt < gens):
        rpt = rpt + 1

    # Generate the list of atomic generator IDs for both FERC (upper case) and
    # EIA (lower case) Using the same IDs across both datasets will make it
    # easy for us to tell whether we've correctly inferred the connections
    # between them.
    gen_ids_ferc = [''.join(s) for s in
                    itertools.product(ascii_uppercase, repeat=rpt)]
    gen_ids_ferc = gen_ids_ferc[:gens]
    gen_ids_eia = [''.join(s) for s in
                   itertools.product(ascii_lowercase, repeat=rpt)]
    gen_ids_eia = gen_ids_eia[:gens]

    # make some dummy years to use as the independent (time) variable:
    years = np.arange(2000, 2000 + samples)

    # Set up some empty Data Frames to receive the synthetic data:
    eia_df = pd.DataFrame(columns=[['year', 'eia_gen_id']])
    ferc_df = pd.DataFrame(columns=[['year', 'ferc_gen_id']])

    # Now we create several pairs of synthetic data by atomic generator, which
    # will exist in both datasets, but apply some noise to one of them, so the
    # correlation between them isn't perfect.

    for ferc_gen_id, eia_gen_id in zip(gen_ids_ferc, gen_ids_eia):
        # Create a new set of FERC and EIA records, 'samples' long, with
        # years as the independent variable.
        eia_new = pd.DataFrame(columns=['year', 'eia_gen_id'])
        ferc_new = pd.DataFrame(columns=['year', 'ferc_gen_id'])

        eia_new['year'] = years
        eia_new['eia_gen_id'] = eia_gen_id
        ferc_new['year'] = years
        ferc_new['ferc_gen_id'] = ferc_gen_id
        for n, noise_scale in enumerate(noise):
            series_label = f'series{n}'
            # Create a pair of logarithmically distributed correlated
            # randomized data series:
            eia_data = 10**(np.random.uniform(low=3, high=9, size=samples))
            ferc_data = eia_data * np.random.normal(loc=1,
                                                    scale=noise_scale,
                                                    size=samples)
            eia_new[series_label] = eia_data
            ferc_new[series_label] = ferc_data

        # Add the new set of records (samples years for each ID)
        eia_df = eia_df.append(eia_new)
        ferc_df = ferc_df.append(ferc_new)

    # Now we're going to group the "true" data together into groups which are
    # the same in both datasets -- these are analogous to the PUDL Plant ID
    # groups. Here we're just randomly chunking the list of all generator IDs
    # into little pieces:

    eia_groups = [group for group in random_chunk(gen_ids_eia,
                                                  min_chunk=1,
                                                  max_chunk=max_group_size)]

    ferc_groups = [[gid.upper() for gid in group] for group in eia_groups]

    # Then within each of these groups, we need to randomly aggregate the data
    # series on the FERC side, to represent the non-atomic FERC plants, which
    # are made up of more than a single generator, but which are still
    # contained within the PUDL ID group:
    ferc_plant_groups = []
    for group in ferc_groups:
        ferc_plant_groups.append([g for g in
                                  random_chunk(group,
                                               min_chunk=1,
                                               max_chunk=max_group_size)])

    for pudl_plant_id in np.arange(0, len(ferc_plant_groups)):
        # set the pudl_plant_id on every record whose ID is in this group.
        for ferc_plant in ferc_plant_groups[pudl_plant_id]:
            # set ferc_plant_id on every record whose ID is in this sub-group.
            ferc_plant_id = '_'.join(ferc_plant)
            ferc_plant_mask = ferc_df.ferc_gen_id.isin(ferc_plant)
            ferc_df.loc[ferc_plant_mask, 'ferc_plant_id'] = ferc_plant_id
            ferc_df.loc[ferc_plant_mask, 'pudl_plant_id'] = pudl_plant_id

    # Fix the type of the pudl_plant_id... getting upcast to float
    ferc_df.pudl_plant_id = ferc_df.pudl_plant_id.astype(int)

    # Assign a numerical pudl_plant_id to each EIA generator, enabling us to
    # compare the same small groups of generators on both the FERC and EIA
    # sides.  This is the only thing that makes the search space workable with
    # so few data points in each series to correlate.
    for pudl_plant_id in np.arange(0, len(eia_groups)):
        eia_group_mask = eia_df.eia_gen_id.isin(eia_groups[pudl_plant_id])
        eia_df.loc[eia_group_mask, 'pudl_plant_id'] = pudl_plant_id

    # Fix the type of the pudl_plant_id... getting upcast to float
    eia_df.pudl_plant_id = eia_df.pudl_plant_id.astype(int)

    # Sum the dependent data series by PUDL plant, FERC plant, and year,
    # creating our synthetic lumped dataset for the algorithm to untangle.
    ferc_gb = ferc_df.groupby(['pudl_plant_id', 'ferc_plant_id', 'year'])
    ferc_df = ferc_gb.agg(sum).reset_index()
    return eia_df, ferc_df


def aggregate_by_pudl_plant(eia_df, ferc_df):
    """Create all possible candidate aggregations of EIA test data.

    The two input dataframes (eia_df and ferc_df) each contain several
    columns of corresponding synthetic data, with some amaount of noise added
    in to keep them from being identical. However, within the two dataframes,
    this data is aggregated differently.  Both dataframes have pudl_plant_id
    values, and the same data can be found within each pudl_plant_id.

    In eia_df, within each pudl_plant_id group there will be some number of
    individual generator units, each with a data value reported for each of the
    data columns, and its own unique alphabetical generator ID.

    In ferc_df, the full granularity is not available -- some lumping of the
    original generators has already been done, and the data associated with
    those lumps are the sums of the data which was originally associated with
    the individual generators which make up the lumps.

    This function generates all the possible lumpings of the fine-grained
    EIA data which have the same number of elements as the FERC data within the
    same pudl_plant_id group, and aggregates the data series within each of
    those possible lumpings so that the data associated with each possible
    collection of generators can be compared with the (already lumped) data
    associated with the FERC plants.

    The function returns a dataframe which contains all of the data from both
    datasets, with many copies of the FERC data, and a different candidate
    aggregation of the EIA data associated with each one. This can be a very
    big dataframe, if there are lots of generators, and lots of plant entities
    within some of the pudl_plant_id groups.
    """
    import re

    # Create a DataFrame where we will accumulate the tests cases:
    eia_test_df = pd.DataFrame(columns=eia_df.columns)
    eia_pudl_ids = eia_df.pudl_plant_id.unique()
    ferc_pudl_ids = ferc_df.pudl_plant_id.unique()
    diff_ids = set(eia_pudl_ids).symmetric_difference(set(ferc_pudl_ids))
    if diff_ids:
        raise ValueError(
            f"EIA and FERC1 PUDL ID sets are not identical."
            f"Symmetric difference: {diff_ids}"
        )

    pudl_plant_ids = eia_pudl_ids

    for pudl_plant_id in pudl_plant_ids:
        # Grab a single PUDL plant from FERC:
        ferc_pudl_plant = ferc_df[ferc_df['pudl_plant_id'] == pudl_plant_id]
        eia_pudl_plant = eia_df[eia_df['pudl_plant_id'] == pudl_plant_id]

        # Count how many FERC plants there are within this PUDL ID.
        ferc_plant_ids = ferc_pudl_plant.ferc_plant_id.unique()
        ferc_plant_n = len(ferc_plant_ids)

        # Enumerate all the EIA generator set coverages with the same number of
        # elements as there are FERC plants.
        eia_test_groups = \
            partition_k(list(eia_pudl_plant.eia_gen_id.unique()), ferc_plant_n)

        # Create new records from the EIA dataframe with the data series
        # aggregated according to each of the candidate generator groupings.
        test_group_id = 0
        for group in eia_test_groups:
            new_eia_grouping = eia_pudl_plant.copy()
            new_eia_grouping['test_group_id'] = test_group_id

            for subgroup in group:
                eia_subgroup_id = '_'.join(subgroup)
                eia_subgroup_mask = new_eia_grouping.eia_gen_id.isin(subgroup)
                new_eia_grouping.loc[eia_subgroup_mask, 'eia_gen_subgroup'] = \
                    eia_subgroup_id

            eia_test_df = eia_test_df.append(new_eia_grouping)
            test_group_id = test_group_id + 1

    eia_test_df['test_group_id'] = eia_test_df['test_group_id'].astype(int)
    eia_test_df = eia_test_df.groupby(['pudl_plant_id',
                                       'test_group_id',
                                       'eia_gen_subgroup',
                                       'year']).agg(sum)

    # Within each (pudl_plant_id, test_group_id) pairing, we'll have a list of
    # N generator subgroups. We need to calculate the correlations with FERC
    # Form 1 for each possible generator subgroup ordering We can generate the
    # list of all possible combinations of FERC plant and EIA subgroups using
    # the itertools.product() function... but what do we do with that
    # information?
    eia_test_df = eia_test_df.reset_index()
    eia_test_df = eia_test_df.rename(
        columns=lambda x: re.sub('(series[0-9]*$)', r'\1_eia', x))
    ferc_df = ferc_df.reset_index()
    ferc_df = ferc_df.drop('index', axis=1)
    ferc_df = ferc_df.rename(
        columns=lambda x: re.sub('(series[0-9]*$)', r'\1_ferc', x))
    both_df = eia_test_df.merge(ferc_df, on=['pudl_plant_id', 'year'])

    return both_df


def correlate_by_generators(agg_df, eia_cols, ferc_cols, corr_cols):
    """Calculate EIA vs. FERC correlations for several data series.

    Given a dataframe output by aggregate_by_pudl_plant(), and lists of
    corresponding columns from the input EIA and FERC datasets, and for the
    output dataframe, calculate the correlations between every possible
    candidate lumping of EIA generators, and the existing FERC generating
    units for which data was supplied.

    The shared variables are indicated with eia_cols and ferc_cols
    which contain the names of columns that exist in both of the
    two data sources, which need to be correlated. E.g.
    'net_generation_mwh_eia' and 'net_generation_mwh_ferc'.

    Returns a dataframe containing the per-variable correlations,
    and a bunch of ID fields for grouping and joining on later.
    """
    index_cols = ['pudl_plant_id',
                  'ferc_plant_id',
                  'test_group_id',
                  'eia_gen_subgroup']

    gb = agg_df.groupby(index_cols)

    # We'll accumulate the various correlation results in this DF
    corrs = agg_df[index_cols].drop_duplicates()
    for eia_var, ferc_var, corr_var in zip(eia_cols, ferc_cols, corr_cols):
        # Calculate correlations between the two variables
        newcorr = gb[[eia_var, ferc_var]].corr().reset_index()
        # Need to eliminate extraneous correlation matrix elements.
        newcorr = newcorr.drop(ferc_var, axis=1)
        newcorr = newcorr[newcorr['level_4'] == ferc_var]
        newcorr = newcorr.drop('level_4', axis=1)
        newcorr = newcorr.rename(columns={eia_var: corr_var})
        corrs = corrs.merge(newcorr, on=index_cols)

    return corrs


def score_all(df, corr_cols):
    """
    Score candidate ensembles of EIA generators based on match to FERC.

    Given a datafram output from correlate_by_generators() above, containing
    correlations between potential EIA generator lumpings and the original
    FERC sub-plant groupings, generate all the possible ensembles of lumped
    EIA generators which have the same number of elements as the FERC
    plants we're trying to replicate, with all possible mappings between the
    EIA generator groups and the FERC generator groups.

    Then, calculate the mean correlations for all the data series for each
    entire candidate ensemble. Return a dataframe of winners, with the EIA
    and FERC plant IDs, the candidate_id, and the mean correlation of the
    entire candidate ensemble across all of the data series used to determine
    the mapping between the two data sources.

    Potential improvements:

    * Might need to be able to use a more general scoring function, rather
      than just taking the mean of all the correlations across all the data
      columns within a given candidate grouping. Is there a way to pass in
      an arbitrary number of columns associated with a group in a groupby
      object for array application? Doing two rounds of aggretation and
      mean() calculation seems dumb.

    * The generation of all the candidate groupings of generator ensembles
      is hella kludgy, and also slow -- it seems like there must be a less
      iterative, more vectorized way of doing the same thing. Depending on
      the number of permutations that need to be generated and tested in the
      real data, this may or may not be functional from a speed perspective.

    * Just for scale, assuming 100 generators, 10 data series, and 100
      samples in each data series, as we change the maximum group size
      (which determines both how large a PUDL plant can be, and how large the
      lumpings within a PUDL plant can be), the time to complete the tests
      increased as follows:

      * 5 => 20 seconds
      * 6 => 60 seconds
      * 7 => 150 seconds
      * 8 => 5000 seconds

    * Can this whole process be made more general, so that it can be used
      to zip together other more arbitrary datasets based on shared data
      fields? What would that look like? What additional parameters would
      we need to pass in?

    * Is there a good reason to keep this chain of functions separate, or
      should they be concatenated into one longer function? Are there more
      sensible ways to break the pipeline up?

    """
    candidates = {}
    # Iterate through each PUDL Plant ID
    for ppid in df.pudl_plant_id.unique():
        combo_list = []
        # Create a miniature dataframe for just this PUDL Plant ID:
        ppid_df = df[df.pudl_plant_id == ppid].copy()
        # For each test group that exists within this PUDL Plant ID:
        for tgid in ppid_df.test_group_id.unique():
            # Create yet another subset dataframe... jsut for this test group:
            tg_df = ppid_df[ppid_df.test_group_id == tgid].copy()
            ferc_ids = tg_df.ferc_plant_id.unique()
            ferc_combos = itertools.combinations(ferc_ids, len(ferc_ids))
            eia_ids = tg_df.eia_gen_subgroup.unique()
            eia_permus = itertools.permutations(eia_ids, len(eia_ids))
            combos = list(itertools.product(ferc_combos, eia_permus))
            combo_list = combo_list + combos

        # Re-organize these lists of tuples into binary mappings... ugh.
        y = []
        for x in combo_list:
            y = y + [[z for z in zip(x[0], x[1])], ]

        # Now we've got a dictionary with pudl plant IDs as the keys,
        # and lists of all possible candidate FERC/EIA mappings as the values.
        candidates[ppid] = y

    candidates_df = pd.DataFrame(columns=df.columns)
    candidates_df['candidate_id'] = []
    candidates_df.drop(['test_group_id', ], axis=1, inplace=True)

    for ppid in candidates:
        cid = 0
        for c in candidates[ppid]:
            candidate = pd.DataFrame(columns=candidates_df.columns)
            for mapping in c:
                newrow = df.loc[(df['ferc_plant_id'] == mapping[0]) &
                                (df['eia_gen_subgroup'] == mapping[1])]
                candidate = candidate.append(newrow)
            candidate['candidate_id'] = cid
            candidates_df = candidates_df.append(candidate)
            cid = cid + 1

    logger.info(
        f"{len(candidates_df)} candidate generator ensembles identified.")

    candidates_df.candidate_id = candidates_df.candidate_id.astype(int)
    candidates_df = candidates_df.drop('test_group_id', axis=1)
    cand_gb = candidates_df.groupby(['pudl_plant_id', 'candidate_id'])

    cand_mean_corrs = cand_gb[corr_cols].mean()
    scored = pd.DataFrame(cand_mean_corrs.mean(axis=1),
                          columns=['mean_corr', ])

    idx = scored.groupby(['pudl_plant_id', ])['mean_corr'].\
        transform(max) == scored['mean_corr']

    winners = scored[idx].reset_index()
    winners = winners.merge(candidates_df,
                            how='left',
                            on=['pudl_plant_id', 'candidate_id'])
    winners = winners.drop(corr_cols, axis=1).\
        drop_duplicates(['eia_gen_subgroup', ])
    winners['success'] = \
        winners.eia_gen_subgroup == winners.ferc_plant_id.str.lower()

    return winners


def correlation_merge():
    """Merge two datasets based on specified shared data series."""
    # What fields do we need in the data frames to be merged? What's the
    # output that we're expecting?
