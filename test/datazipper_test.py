"""Tests excercising FERC/EIA correlation merge for use with PyTest."""
import logging

import pytest

from pudl import analysis

logger = logging.getLogger(__name__)


@pytest.mark.skip(reason="Data zipper not ready for prime time.")
def test_datazipper(gens=100, max_group_size=5, n_series=10, n_samples=100):
    """Do a test run of the FERC/EIA correlation merge."""
    # These values will determine how much noise is added to each of the
    # syntetic data series which exist in both data sources, and whose
    # correlations are used to connect the two data sources.  It also
    # determines how many of those data series there are. Because we're
    # just testing for structural correctness, we'll give it a very easy set
    # of data to work with (high correlations, and planty of data points)
    test_noise = n_series * [0.1, ]

    # Now we create the two synthetic datasets. The FERC data will be
    # arbitrarily pre-lumped (with "true" generating units combined into
    # plant-level data) while the EIA data will remain fine grained (with
    # every data series available for every generator)
    logger.info('Generating synthetic EIA & FERC test data.')
    logger.info("""    generators = {}
    max_group_size = {}
    n_series = {}
    n_samples = {}""".format(gens, max_group_size, n_series, n_samples))
    eia_df, ferc_df = analysis.zippertestdata(gens=gens,
                                              max_group_size=max_group_size,
                                              noise=test_noise,
                                              samples=n_samples)

    n_pudl_plants = len(ferc_df.pudl_plant_id.unique())
    logger.info('{} synthetic PUDL plants created.'.format(n_pudl_plants))

    # Now we aggregate the synthetic EIA data to create all the possible
    # lumpings that we might want to use in comparing to the FERC data:
    logger.info('Aggregating synthetic EIA data.')
    agg_df = analysis.aggregate_by_pudl_plant(eia_df, ferc_df)
    n_eia_groups = len(agg_df.eia_gen_subgroup.unique())
    logger.info('{} EIA subgroupings created.'.format(n_eia_groups))

    eia_cols = ['series{}_eia'.format(n) for n in range(n_series)]
    ferc_cols = ['series{}_ferc'.format(n) for n in range(n_series)]
    corr_cols = ['series{}_corr'.format(n) for n in range(n_series)]

    # Now that we have all the possible lumpings of EIA data, we can
    # calculate the correlations between each of them and their potential
    # matches within each PUDL ID in the FERC data.
    logger.info('Calculating candidate plant grouping correlations.')
    corr_df = analysis.correlate_by_generators(agg_df, eia_cols,
                                               ferc_cols, corr_cols)

    # Finally, we generate all possible ensembles of lumped EIA plants that
    # make up a complete PUDL plant, and score each of them to see which
    # candidate is the best match.
    logger.info('Scoring cantidate ensembles based on mean correlations.')
    winners = analysis.score_all(corr_df, corr_cols)
    assert len(winners.success is True) / len(winners) == 1.0
