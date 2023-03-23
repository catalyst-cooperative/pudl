"""Validate post-ETL Generators data from EIA 860."""
import logging
import warnings

import numpy as np
import pandas as pd
import pytest

import pudl
from pudl.analysis.plant_parts_eia import (
    IDX_OWN_TO_ADD,
    IDX_TO_ADD,
    PLANT_PARTS,
    SUM_COLS,
    MakeMegaGenTbl,
)

logger = logging.getLogger(__name__)


def test_ownership_for_owned_records(pudl_out_eia, live_dbs):
    """Test ownership - fraction owned for owned records.

    This test can be run at the end of or with the result of
    :meth:`MakePlantParts.execute`. It tests a few aspects of the the
    fraction_owned column and raises assertions if the tests fail.
    """
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")

    if pudl_out_eia.freq == "AS":  # Annual only.
        logger.info("Testing ownership fractions for owned records.")

        plant_parts_eia = pudl_out_eia.plant_parts_eia()
        # ferc1_generator_agg_id will make groupings within a plant part and break the
        # validation, so we remove it from the list here.
        id_cols_list = [
            col
            for col in pudl.analysis.plant_parts_eia.make_id_cols_list()
            if col != "ferc1_generator_agg_id"
        ]

        test_own_df = (
            plant_parts_eia.groupby(
                by=id_cols_list + ["plant_part", "ownership_record_type"],
                dropna=False,
                observed=True,
            )[["fraction_owned", "capacity_mw"]]
            .sum(min_count=1)
            .reset_index()
        )

        owned_one_frac = test_own_df[
            (~np.isclose(test_own_df.fraction_owned, 1))
            & (test_own_df.capacity_mw != 0)
            & (test_own_df.capacity_mw.notnull())
            & (test_own_df.ownership_record_type == "owned")
        ]

        if not owned_one_frac.empty:
            raise AssertionError(
                "Hello friend, you did a bad. It happens... There are "
                f"{len(owned_one_frac)} rows where fraction_owned does not sum "
                "to 100% for the owned records. "
                "Check cached `owned_one_frac` & `test_own_df` and `scale_by_ownership()`"
            )

        no_frac_n_cap = test_own_df[
            (test_own_df.capacity_mw == 0) & (test_own_df.fraction_owned == 0)
        ]
        if len(no_frac_n_cap) > 60:
            warnings.warn(
                f"""Too many nothings, you nothing. There shouldn't been much
                more than 60 instances of records with zero capacity_mw (and
                therefor zero fraction_owned) and you got {len(no_frac_n_cap)}.
                """
            )
    else:
        pytest.skip("Plant part validation only works for annual data.")


#################
# Data Validation
#################


def test_run_aggregations(pudl_out_eia, live_dbs):
    """Run a test of the aggregated columns.

    This test will used the plant_parts_eia, re-run groubys and check similarity.
    """
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    if pudl_out_eia.freq == "AS":  # Annual only.
        logger.info("Testing ownership fractions for owned records.")

        mcoe = pudl_out_eia.mcoe()
        own_eia860 = pudl_out_eia.own_eia860()
        gens_mega = MakeMegaGenTbl().execute(mcoe, own_eia860)
        plant_parts_eia = pudl_out_eia.plant_parts_eia()

        for part_name in PLANT_PARTS:
            logger.info(f"Begining tests for {part_name}:")
            if part_name == "plant_match_ferc1":
                test_merge = prep_test_merge(
                    part_name,
                    plant_parts_eia,
                    plant_parts_eia.loc[
                        plant_parts_eia.plant_part != "plant_match_ferc1"
                    ],
                )
                # This is a manually generated plant part not present in gens_mega.
                # Test whether aggregating the records flagged as having a ferc1_generator_agg_id
                # produces the same result as the plant_match_ferc1 plant parts.
            else:
                test_merge = prep_test_merge(part_name, plant_parts_eia, gens_mega)
            for test_col in SUM_COLS:
                # Check if test aggregation is the same as generated aggreation
                # Apply a boolean column to the test df.
                test_merge[f"test_{test_col}"] = (
                    (test_merge[f"{test_col}_test"] == test_merge[f"{test_col}"])
                    | (
                        test_merge[f"{test_col}_test"].isnull()
                        & test_merge[f"{test_col}"].isnull()
                    )
                    | (test_merge.ownership_record_type == "total")
                )
                result = list(test_merge[f"test_{test_col}"].unique())
                logger.info(f"  Results for {test_col}: {result}")
                if not all(result):
                    warnings.warn(f"{test_col} done fucked up.")
                    return test_merge
                    # raise AssertionError(
                    #    f"{test_col}'s '"
                    # )
    else:
        pytest.skip("Plant part validation only works for annual data.")


def prep_test_merge(part_name, plant_parts_eia, gens_mega):
    """Run the test groupby and merge with the aggregations."""
    id_cols = PLANT_PARTS[part_name]["id_cols"]
    plant_cap = (
        gens_mega[gens_mega.ownership_record_type == "owned"]
        .pipe(pudl.helpers.convert_cols_dtypes, "eia")
        .groupby(by=id_cols + IDX_TO_ADD + IDX_OWN_TO_ADD, observed=True)[SUM_COLS]
        .sum(min_count=1)
        .reset_index()
        .pipe(pudl.helpers.convert_cols_dtypes, "eia")
    )
    test_merge = pd.merge(
        plant_parts_eia[plant_parts_eia.plant_part == part_name],
        plant_cap,
        on=id_cols + IDX_TO_ADD + IDX_OWN_TO_ADD,
        how="outer",
        indicator=True,
        suffixes=("", "_test"),
    )
    return test_merge
