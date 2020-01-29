"""Validate post-ETL EIA 860 data and the associated derived outputs."""

import logging

from scipy import stats

from pudl import helpers

logger = logging.getLogger(__name__)


def test_own_eia860(pudl_out_eia, live_pudl_db):
    """Sanity checks for EIA 860 generator ownership data."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    logger.info('Reading EIA 860 generator ownership data...')
    own_out = pudl_out_eia.own_eia860()

    if (own_out.fraction_owned > 1.0).any():
        raise AssertionError(
            "Generators with ownership fractions > 1.0 found. Bad data?"
        )

    if (own_out.fraction_owned < 0.0).any():
        raise AssertionError(
            "Generators with ownership fractions < 0.0 found. Bad data?"
        )

    # Verify that the reported ownership fractions add up to something very
    # close to 1.0 (i.e. that the full ownership of each generator is
    # specified by the EIA860 data)
    own_gb = own_out.groupby(['report_date', 'plant_id_eia', 'generator_id'])
    own_sum = own_gb['fraction_owned'].agg(helpers.sum_na).reset_index()
    logger.info(
        f"{len(own_sum[own_sum.fraction_owned.isnull()])} generator-years have no ownership data.")

    own_sum = own_sum.dropna()
    logger.info(
        f"{len(own_sum[own_sum.fraction_owned < 0.98])} generator-years have incomplete ownership data.")
    if not max(own_sum['fraction_owned'] < 1.02):
        raise AssertionError(
            "Plants with more than 100% ownership found..."
        )
    # There might be a few generators with incomplete ownership but virtually
    # everything should be pretty fully described. If not, let us know. The
    # 0.1 threshold means 0.1% -- i.e. less than 1 in 1000 is partial.
    if stats.percentileofscore(own_sum.fraction_owned, 0.98) >= 0.1:
        raise AssertionError(
            "Found too many generators with partial ownership data."
        )
