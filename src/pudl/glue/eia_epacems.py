"""
Define and fill in gaps of the EPA-EIA crosswalk.

This module defines functions that read in the EPA-EIA crosswalk
and fill in as many of the eia id gaps as possible. Eventually, EPA
is going to come out with a crosswalk file containing fewer gaps.
Until then, this module reads and cleans the current crosswalk.
"""
import importlib
import logging

import pandas as pd

import pudl

logger = logging.getLogger(__name__)


def grab_n_clean_epa_orignal():
    """Retrieve the original EPA-EIA crosswalk file."""
    logger.info("grabbing original crosswalk")
    eia_epacems_crosswalk_csv = (
        importlib.resources.open_text(
            'pudl.package_data.glue',
            'epa_eia_crosswalk_from_epa.csv')
    )
    eia_epacems_crosswalk = (
        pd.read_csv(eia_epacems_crosswalk_csv)
        .pipe(pudl.helpers.simplify_columns)
        .rename(columns={
            'oris_code': 'plant_id_epa',
            'eia_oris': 'plant_id_eia',
            'unit_id': 'epa_point_source_unit',
            'facility_name': 'plant_name_eia',
            'unit_type': 'prime_mover_code'})
        .drop([
            'fuel_type_primary',
            'edat_mw_cap',
            'way_gen_id_matched',
            'unit_op_status_date',
            'notes',
            'annual_heat_input',
            'op_status'], axis=1)
    )
    return eia_epacems_crosswalk


def split_into_w_and_wo_eia_ids(df):
    """Separate into two dataframes with and without eia ids."""
    logger.info("separating matched from missing")
    matched_plant_id_eia = df[df['plant_id_eia'].notna()]
    missing_plant_id_eia = (
        df[df['plant_id_eia'].isna()].drop('plant_id_eia', axis=1))
    return matched_plant_id_eia, missing_plant_id_eia


def test_plant_name_strings(missing_ids, eia_plants):
    """Fill in missing EIA ids based on plant name strings matches."""
    logger.info("running plant name match")
    eia_plants = eia_plants.filter(['plant_id_eia', 'plant_name_eia']).copy()
    missing_merge = (
        pd.merge(
            missing_ids,
            eia_plants,
            on='plant_name_eia',
            how='left')
        .drop_duplicates(subset='index')
    )
    return split_into_w_and_wo_eia_ids(missing_merge)


def test_plant_id_gen_id_pairs(missing_ids, eia_gens):
    """Look for plant_id and generator_id parings that match between EIA and EPA."""
    logger.info("running plant id and plant gen match")
    eia_gen = eia_gens.filter(['plant_id_eia', 'generator_id']).copy()
    missing_merge = (
        pd.merge(
            missing_ids,
            eia_gen,
            left_on=['plant_id_epa', 'generator_id'],
            right_on=['plant_id_eia', 'generator_id'],
            how='left')
        .drop_duplicates(subset='index')
    )
    return split_into_w_and_wo_eia_ids(missing_merge)


def test_plant_ids(missing_ids, eia_plants):
    """Look for plant id matches between EIA and EPA."""
    logger.info("running plant id match")
    eia_plants = eia_plants.filter(['plant_id_eia', 'plant_name_eia']).copy()
    missing_merge = (
        pd.merge(
            missing_ids,
            eia_plants,
            left_on='plant_id_epa',
            right_on='plant_id_eia',
            how='left',
            suffixes=['_epa', '_eia'])
        .drop_duplicates(subset='index')
    )
    return split_into_w_and_wo_eia_ids(missing_merge)


def find_test_combine_id_matches(eia_plants, eia_gens):
    """Run all EIA id matching tests on the crosswalk to fill in the gaps."""
    # Make sure the original crosswalk has an index field
    crosswalk = grab_n_clean_epa_orignal().reset_index()
    matched_ids_1, missing_ids = split_into_w_and_wo_eia_ids(crosswalk)
    matched_ids_2, missing_ids = test_plant_name_strings(
        missing_ids, eia_plants)
    matched_ids_3, missing_ids = test_plant_id_gen_id_pairs(
        missing_ids, eia_gens)
    matched_ids_4, missing_ids = test_plant_ids(missing_ids, eia_plants)

    clean_crosswalk = (
        pd.concat([
            matched_ids_1,
            matched_ids_2,
            matched_ids_3,
            matched_ids_4,
            missing_ids])
        .drop(['index', 'plant_name_eia_eia', 'plant_name_eia_epa'], axis=1)
    )
    return clean_crosswalk
